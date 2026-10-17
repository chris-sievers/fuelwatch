import asyncio
import sqlite3
import threading
from datetime import UTC, date, datetime, timedelta
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from fuelwatch import scheduler as sch
from fuelwatch.collectors import portwatch as pw
from fuelwatch.collectors.ais_tracker import SERIES_SHIPS
from fuelwatch.http import PacedClient, ProviderError, make_client, redact
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Collector
from fuelwatch.scheduler import Scheduler, backoff
from fuelwatch.store import BackfillState, Store

NOW = datetime(2026, 10, 17, 12, tzinfo=UTC)  # a Saturday


def day(y, m, d):
    return datetime(y, m, d, tzinfo=UTC)


@pytest.fixture(autouse=True)
def small_fakes_are_plausible(monkeypatch):
    from fuelwatch import backfill

    monkeypatch.setattr(backfill, "MIN_POINTS_PER_SERIES", 0)


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "fuelwatch.db")
    yield s
    s.close()


@pytest.fixture
async def client():
    async with make_client() as c:
        yield c


class Fake:
    """A fetch that records its calls and returns or raises."""

    def __init__(self, result=None, error=None):
        self.result = result if result is not None else []
        self.error = error
        self.calls: list[date | None] = []

    async def __call__(self, client, since=None):
        self.calls.append(since)
        await asyncio.sleep(0)
        if self.error is not None:
            raise self.error
        return list(self.result)


def like(name, fetch, **kw):
    """A fake collector with the real collector's series (and so schedules)."""
    real = REGISTRY.collector(name)
    return Collector(
        name,
        real.series,
        fetch,
        revision_window=kw.get("revision_window", real.revision_window),
        full_refresh_every=kw.get("full_refresh_every", real.full_refresh_every),
    )


def brent(d):
    return Point("eia.spot.brent", d, 80.0)


# --- one poll -------------------------------------------------------------------------


async def test_one_failing_collector_does_not_stop_others(store, client):
    redact.add("sekrit-key-123")
    good = Fake([brent(day(2026, 10, 13))])
    bad_provider = Fake(error=ProviderError("http_status", "503 from x?api_key=sekrit-key-123"))
    bad_internal = Fake(error=RuntimeError("boom sekrit-key-123 /etc/passwd"))
    s = Scheduler(
        store,
        client=client,
        collectors=[
            like("portwatch", bad_provider),
            like("eia", good),
            like("eu_oil_bulletin", bad_internal),
        ],
        ais=None,
        now=lambda: NOW,
    )
    results = {r.collector: r for r in await s.poll_all()}
    assert results["eia"].rows == 1 and results["eia"].error is None
    assert store.latest("eia.spot.brent")[0].value == 80.0

    st = {x.collector: x for x in store.all_status()}
    assert st["eia"].error_class is None and st["eia"].last_success_at is not None
    assert st["portwatch"].error_class == "http_status"
    assert "sekrit" not in st["portwatch"].error_message
    assert st["eu_oil_bulletin"].error_class == "internal"
    assert st["eu_oil_bulletin"].error_message == "RuntimeError"
    assert s.failures == {"portwatch": 1, "eia": 0, "eu_oil_bulletin": 1}


async def test_success_clears_failure(store, client):
    f = Fake(error=ProviderError("transport", "ConnectTimeout"))
    c = like("eia", f)
    s = Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW)
    await s.poll(c)
    assert store.get_status("eia").error_class == "transport"
    f.error = None
    await s.poll(c)
    assert store.get_status("eia").error_class is None
    assert s.failures["eia"] == 0


async def test_eia_since_is_revision_window_before_newest(store, client):
    store.upsert_points([brent(day(2026, 10, 13))])
    f = Fake()
    c = like("eia", f)
    s = Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW)
    await s.poll(c)
    assert f.calls == [date(2026, 10, 13) - timedelta(days=14)]


async def test_eu_since_is_newest_monday(store, client):
    store.upsert_points([Point("eu_pump_petrol_de", day(2026, 10, 12), 1.8)])
    f = Fake()
    c = like("eu_oil_bulletin", f)
    await Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW).poll(c)
    assert f.calls == [date(2026, 10, 12)]


@respx.mock
async def test_portwatch_refetches_35_day_window(store, client):
    store.set_backfill("portwatch", BackfillState.DONE, None, 1.0, at=NOW)
    store.upsert_points([Point("portwatch.hormuz.all", day(2026, 10, 11), 40.0)])
    seen = []

    def respond(request):
        seen.append(parse_qs(request.url.query.decode())["where"][0])
        return httpx.Response(200, json={"features": []})

    respx.get(pw.QUERY_URL).mock(side_effect=respond)
    c = REGISTRY.collector("portwatch")
    unpaced = PacedClient(client)
    s = Scheduler(store, client=unpaced, collectors=[c], ais=None, now=lambda: NOW)
    r = await s.poll(c)
    assert r.error is None
    assert r.since == date(2026, 10, 11) - pw.REVISION_WINDOW == date(2026, 9, 6)
    assert seen == [
        pw.query_params(date(2026, 9, 6))["where"],
    ]
    # Every run, not only the first.
    await s.poll(c)
    assert len(seen) == 2 and seen[1] == seen[0]


async def test_portwatch_window_without_data_counts_from_today(store, client):
    store.set_backfill("portwatch", BackfillState.DONE, None, 1.0, at=NOW)
    f = Fake()
    c = like("portwatch", f)
    await Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW).poll(c)
    assert f.calls == [NOW.date() - timedelta(days=35)]


async def test_portwatch_periodic_full_refresh(store, client):
    store.set_backfill("portwatch", BackfillState.DONE, None, 1.0, at=NOW - timedelta(days=29))
    f = Fake()
    c = like("portwatch", f)
    s = Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW)
    await s.poll(c)
    assert f.calls == [None]
    assert store.get_backfill("portwatch").updated_at > NOW - timedelta(days=1)
    await s.poll(c)
    assert f.calls[1] is not None


# --- cadence and backoff ----------------------------------------------------------------


def test_backoff_doubles_to_cap():
    assert backoff(1) == timedelta(minutes=10)
    assert backoff(2) == timedelta(minutes=20)
    assert backoff(4) == timedelta(minutes=80)
    assert backoff(20) == timedelta(hours=6)
    big = timedelta(days=10**6)
    assert backoff(10**6, timedelta(minutes=10), big) == timedelta(minutes=10 * 2**20)


async def test_next_delay_backs_off_and_resets(store, client):
    f = Fake(error=ProviderError("http_status", "500"))
    c = like("eu_oil_bulletin", f)
    s = Scheduler(store, client=client, collectors=[c], ais=None, now=lambda: NOW, jitter=0)
    delays = []
    for _ in range(3):
        await s.poll(c)
        delays.append(await s.next_delay(c))
    assert delays == [timedelta(minutes=10), timedelta(minutes=20), timedelta(minutes=40)]
    f.error = None
    await s.poll(c)
    assert await s.next_delay(c) > timedelta(minutes=40)


def _sched(store, now, name="eia"):
    c = like(name, Fake())
    return c, Scheduler(store, client=None, collectors=[c], ais=None, now=lambda: now, jitter=0)


def test_cadence_idle_capped_at_next_release(store):
    # Sat noon: next EIA release is Mon Oct 19 22:00 (retail), beyond 6 h.
    c, s = _sched(store, NOW)
    latest = {sp.id: day(2026, 10, 13) for sp in c.series}
    assert s.cadence(c, latest) == timedelta(hours=6)
    # Mon 20:00: the retail release at 22:00 comes first.
    c, s = _sched(store, datetime(2026, 10, 19, 20, tzinfo=UTC))
    assert s.cadence(c, latest) == timedelta(hours=2, minutes=5)


def test_cadence_hourly_while_release_pending(store):
    now = datetime(2026, 10, 19, 23, tzinfo=UTC)  # retail out at 22:00
    c, s = _sched(store, now)
    latest = {sp.id: day(2026, 10, 13) for sp in c.series}
    latest |= {sp.id: day(2026, 10, 12) for sp in c.series if ".retail." in sp.id}
    assert s.cadence(c, latest) == timedelta(hours=1)
    latest |= {sp.id: day(2026, 10, 19) for sp in c.series if ".retail." in sp.id}
    assert s.cadence(c, latest) == timedelta(hours=6)


def test_cadence_stops_hot_polling_after_window(store):
    # PortWatch release Tue Oct 13 never arrived; four days on, back to idle.
    now = datetime(2026, 10, 17, 14, tzinfo=UTC)
    c, s = _sched(store, now, "portwatch")
    latest = {sp.id: day(2026, 10, 4) for sp in c.series}
    assert s.cadence(c, latest) == timedelta(hours=6)


async def test_first_delay_respects_last_run(store):
    c, s = _sched(store, NOW)
    assert await s.first_delay(c) == timedelta(0)
    store.record_success("eia", at=NOW - timedelta(hours=2))
    store.upsert_points([Point(sp.id, day(2026, 10, 13), 1.0) for sp in c.series])
    assert await s.first_delay(c) == timedelta(hours=4)


# --- the loop ----------------------------------------------------------------------------


def fast(store, client, collectors, **kw):
    tiny = timedelta(milliseconds=10)
    return Scheduler(
        store,
        client=client,
        collectors=collectors,
        ais=kw.pop("ais", None),
        idle_interval=tiny,
        hot_interval=tiny,
        retry_base=tiny,
        retry_max=timedelta(milliseconds=40),
        min_delay=tiny,
        backfill_check=0.01,
        jitter=0,
        **kw,
    )


async def until(cond, timeout=5.0):
    """Wait for a condition, yielding to the loop; the timeout only bounds a
    broken test."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.001)


async def test_loop_isolation_and_clean_stop(store, client):
    good = Fake([brent(day(2026, 10, 13))])
    bad = Fake(error=RuntimeError("x"))
    s = fast(store, client, [like("eia", good), like("portwatch", bad)], run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: len(good.calls) >= 3 and len(bad.calls) >= 3)
    assert s.failures["portwatch"] >= 3 and s.failures["eia"] == 0
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    n = len(good.calls)
    for _ in range(10):
        await asyncio.sleep(0)
    assert len(good.calls) == n
    assert not client.is_closed  # the caller's client is left open


async def test_run_closes_its_own_client(store):
    s = fast(store, None, [like("eia", Fake())], run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: s._raw_client is not None)
    raw = s._raw_client
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert raw.is_closed


async def test_polling_waits_for_backfill(store, client):
    f = Fake([brent(day(2026, 10, 13))])
    s = fast(store, client, [like("eia", f)], run_backfill=True)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: len(f.calls) >= 2)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert f.calls[0] is None  # the backfill's five-year fetch came first
    assert all(c is not None for c in f.calls[1:])
    assert store.get_backfill("eia").state == BackfillState.DONE


class Hanging:
    """A fetch that never returns until released."""

    def __init__(self):
        self.calls = []
        self.release = asyncio.Event()

    async def __call__(self, client, since=None):
        self.calls.append(since)
        await self.release.wait()
        return []


async def test_polls_wait_while_backfill_runs_and_stop_is_prompt(store, client):
    h = Hanging()
    s = fast(store, client, [like("eia", h)], run_backfill=True)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: h.calls == [None])
    for _ in range(50):
        await asyncio.sleep(0)
    assert h.calls == [None]  # no poll while the backfill is running
    stop.set()
    await asyncio.wait_for(task, timeout=1)  # well inside SHUTDOWN_GRACE


async def test_failed_backfill_unblocks_polls(store, client):
    f = Fake(error=ProviderError("http_status", "503 from api.eia.gov"))
    s = fast(store, client, [like("eia", f)], run_backfill=True)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: any(c is not None for c in f.calls))  # a poll ran
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert f.calls[0] is None
    st = store.get_status("eia")
    assert st.backfill.state == BackfillState.NOT_STARTED  # failed, not "running"
    assert st.error_class == "http_status"


class LockedStore:
    """Wraps a Store; chosen methods raise 'database is locked'."""

    def __init__(self, store, failing):
        self._store = store
        self.failing = set(failing)

    def __getattr__(self, name):
        attr = getattr(self._store, name)
        if name in self.failing:

            def locked(*a, **kw):
                raise sqlite3.OperationalError("database is locked")

            return locked
        return attr


async def test_loops_survive_a_failing_store(store, client):
    locked = LockedStore(
        store,
        {"upsert_points", "record_failure", "record_success", "series_latest", "get_status"},
    )
    f = Fake([brent(day(2026, 10, 13))])
    s = fast(locked, client, [like("eia", f)], run_backfill=True)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    # The backfill attempt fails on the store, then polls keep failing and coming.
    await until(lambda: f.calls and s.failures["eia"] >= 3)
    locked.failing.clear()  # the store recovers
    await until(lambda: store.latest("eia.spot.brent"))
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert not task.exception()


async def test_shutdown_waits_for_writes_in_flight(store, client):
    started, release = threading.Event(), threading.Event()
    real = store.upsert_points
    finished = []

    def slow_upsert(points, *a, **kw):
        started.set()
        release.wait(timeout=5)
        n = real(points, *a, **kw)
        finished.append(n)
        return n

    store.upsert_points = slow_upsert
    s = fast(store, client, [like("eia", Fake([brent(day(2026, 10, 13))]))], run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await asyncio.to_thread(started.wait, 5)
    stop.set()
    for _ in range(20):
        await asyncio.sleep(0)
    assert not task.done()  # still draining the write
    release.set()
    await asyncio.wait_for(task, timeout=sch.SHUTDOWN_GRACE + 5)
    assert finished == [1]
    assert s.io.running == 0


def test_empty_selection_means_none(store):
    assert Scheduler(store, collectors=[], ais=None).collectors == ()
    assert len(Scheduler(store, ais=None).collectors) == 3


# --- AIS ------------------------------------------------------------------------------


class FakeAis:
    def __init__(self, crashes=0):
        self.crashes = crashes
        self.runs = 0
        self.status = type("S", (), {"error_class": None, "message": None})()

    async def run(self, sink, stop):
        self.runs += 1
        if self.runs <= self.crashes:
            raise RuntimeError("bug")
        await sink([Point(SERIES_SHIPS, datetime(2026, 10, 17, 12, 10, tzinfo=UTC), 31.0)])
        await stop.wait()


async def test_ais_stream_writes_through_sink(store, client, monkeypatch):
    monkeypatch.setattr(sch, "AIS_RESTART_START", 0.001)
    ais = FakeAis(crashes=1)
    s = fast(store, client, [], ais=ais, run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await until(lambda: store.latest(SERIES_SHIPS))
    stop.set()
    await asyncio.wait_for(task, timeout=3)
    assert ais.runs == 2  # restarted after the crash
    assert store.latest(SERIES_SHIPS)[0].value == 31.0
    assert store.get_status("aisstream").error_class is None


async def test_ais_restart_delay_resets_after_a_healthy_run(store, client, monkeypatch):
    waits = []

    async def fake_wait(stop, seconds):
        waits.append(seconds)
        await asyncio.sleep(0)
        return stop.is_set()

    monkeypatch.setattr(sch.backfill, "wait_or_stop", fake_wait)
    clock = {"t": 0.0}
    durations = iter([0, 0, 0, 1000, 0])  # three quick crashes, one long run, one more

    class Crashy(FakeAis):
        async def run(self, sink, stop):
            self.runs += 1
            clock["t"] += next(durations, 0)
            if self.runs >= 6:
                stop.set()
                return
            raise RuntimeError("bug")

    s = fast(store, client, [], ais=Crashy(), run_backfill=False, monotonic=lambda: clock["t"])
    stop = asyncio.Event()
    await asyncio.wait_for(s._ais_loop(stop), timeout=2)
    restarts = [w for w in waits if w != sch.AIS_STATUS_CHECK]
    assert restarts == [1.0, 2.0, 4.0, 1.0, 2.0]


def test_ais_off_without_key(store, monkeypatch):
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    assert Scheduler(store).ais is None
    monkeypatch.setenv("AISSTREAM_API_KEY", "k-for-test")
    assert Scheduler(store).ais is not None
