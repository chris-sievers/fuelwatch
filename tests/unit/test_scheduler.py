import asyncio
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


async def test_loop_isolation_and_clean_stop(store, client):
    good = Fake([brent(day(2026, 10, 13))])
    bad = Fake(error=RuntimeError("x"))
    s = fast(store, client, [like("eia", good), like("portwatch", bad)], run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await asyncio.sleep(0.3)
    assert len(good.calls) >= 3
    assert len(bad.calls) >= 2
    # Backoff: the failing collector polls less often than the good one.
    assert len(bad.calls) < len(good.calls)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    n = len(good.calls)
    await asyncio.sleep(0.05)
    assert len(good.calls) == n
    assert not client.is_closed  # the caller's client is left open


async def test_run_closes_its_own_client(store):
    s = fast(store, None, [like("eia", Fake())], run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await asyncio.sleep(0.05)
    raw = s._raw_client
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert raw.is_closed


async def test_polling_waits_for_backfill(store, client):
    f = Fake([brent(day(2026, 10, 13))])
    s = fast(store, client, [like("eia", f)], run_backfill=True)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, timeout=2)
    assert f.calls[0] is None  # the backfill's five-year fetch came first
    assert all(c is not None for c in f.calls[1:]) and len(f.calls) >= 2
    assert store.get_backfill("eia").state == BackfillState.DONE


async def test_stop_while_backfill_pending_returns_quickly(store, client):
    s = fast(store, client, [like("eia", Fake())], run_backfill=False)
    s.run_backfill = True  # loops wait for a backfill nobody runs
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)


# --- AIS ------------------------------------------------------------------------------


class FakeAis:
    def __init__(self, crash_first=False):
        self.crash_first = crash_first
        self.runs = 0
        self.status = type("S", (), {"error_class": None, "message": None})()

    async def run(self, sink, stop):
        self.runs += 1
        if self.crash_first and self.runs == 1:
            raise RuntimeError("bug")
        await sink([Point(SERIES_SHIPS, datetime(2026, 10, 17, 12, 10, tzinfo=UTC), 31.0)])
        await stop.wait()


async def test_ais_stream_writes_through_sink(store, client, monkeypatch):
    monkeypatch.setattr(sch, "AIS_RESTART_START", 0.01)
    ais = FakeAis(crash_first=True)
    s = fast(store, client, [], ais=ais, run_backfill=False)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    for _ in range(100):
        await asyncio.sleep(0.02)
        if store.latest(SERIES_SHIPS):
            break
    stop.set()
    await asyncio.wait_for(task, timeout=3)
    assert ais.runs == 2  # restarted after the crash
    assert store.latest(SERIES_SHIPS)[0].value == 31.0
    assert store.get_status("aisstream").error_class is None


def test_ais_off_without_key(store, monkeypatch):
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    assert Scheduler(store).ais is None
    monkeypatch.setenv("AISSTREAM_API_KEY", "k-for-test")
    assert Scheduler(store).ais is not None
