import asyncio
import sqlite3
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from fuelwatch import backfill
from fuelwatch.__main__ import main as cli_main
from fuelwatch.collectors import eia, eu_oil_bulletin, portwatch
from fuelwatch.http import PacedClient, ProviderError, make_client
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Collector
from fuelwatch.store import BackfillState, Store

NOW = datetime(2026, 10, 17, 12, tzinfo=UTC)
FIX = Path(__file__).parent.parent / "fixtures"


@pytest.fixture(autouse=True)
def small_fakes_are_plausible(monkeypatch):
    """The fakes return a handful of points; the threshold has its own tests."""
    monkeypatch.setattr(backfill, "MIN_POINTS_PER_SERIES", 0)
    monkeypatch.setattr(backfill, "REQUIRE_EVERY_SERIES", False)


async def until(cond, timeout=5.0):
    """Wait for a condition, yielding to the loop; deterministic outcome,
    the timeout only bounds a broken test."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met")
        await asyncio.sleep(0.001)


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "fuelwatch.db")
    yield s
    s.close()


@pytest.fixture
async def client():
    async with make_client() as c:
        yield c


def daily(series, n, start=date(2021, 10, 20)):
    return [
        Point(series, datetime.combine(start + timedelta(days=i), datetime.min.time(), UTC), 1.0)
        for i in range(n)
    ]


class Fake:
    def __init__(self, points=(), error=None):
        self.points = list(points)
        self.error = error
        self.calls = []

    async def __call__(self, client, since=None):
        self.calls.append(since)
        if self.error is not None:
            raise self.error
        if since is None:
            return list(self.points)
        return [p for p in self.points if p.period_end.date() >= since]


def coll(name, fetch):
    return Collector(name, REGISTRY.collector(name).series, fetch)


async def test_backfill_all_and_mark_done(store, client):
    a = Fake(daily("eia.spot.brent", 100))
    b = Fake(daily("portwatch.hormuz.all", 50))
    results = await backfill.run(
        store, client, collectors=[coll("eia", a), coll("portwatch", b)], now=lambda: NOW
    )
    assert {r.collector: r.rows for r in results.values()} == {"eia": 100, "portwatch": 50}
    assert a.calls == [None] and b.calls == [None]
    for name in ("eia", "portwatch"):
        bf = store.get_backfill(name)
        assert bf.state == BackfillState.DONE and bf.progress == 1.0
        assert store.get_status(name).last_success_at is not None
    assert len(store.points("eia.spot.brent")) == 100
    assert backfill.summary(store, [coll("eia", a), coll("portwatch", b)]).state == "done"


async def test_done_collectors_are_skipped(store, client):
    store.set_backfill("eia", BackfillState.DONE, None, 1.0)
    a = Fake(daily("eia.spot.brent", 10))
    b = Fake(daily("portwatch.hormuz.all", 10))
    results = await backfill.run(store, client, collectors=[coll("eia", a), coll("portwatch", b)])
    assert a.calls == []
    assert results["eia"].skipped
    assert b.calls == [None]


async def test_resume_after_interruption(store, client, monkeypatch):
    monkeypatch.setattr(backfill, "CHUNK", 100)
    done_first = Fake(daily("eu_pump_petrol_de", 20))
    big = Fake(daily("eia.spot.brent", 1000))
    collectors = [coll("eu_oil_bulletin", done_first), coll("eia", big)]

    # Kill the run after the third chunk of the second collector is written.
    real_upsert = store.upsert_points
    writes = {"n": 0}
    killed = asyncio.Event()

    def upsert(points, *a, **kw):
        n = real_upsert(points, *a, **kw)
        if points and points[0].series == "eia.spot.brent":
            writes["n"] += 1
            if writes["n"] == 3:
                killed.set()
                time.sleep(0.05)  # let the cancel land while this chunk is in flight
        return n

    monkeypatch.setattr(store, "upsert_points", upsert)
    task = asyncio.create_task(backfill.run(store, client, collectors=collectors, now=lambda: NOW))
    await killed.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    bf = store.get_backfill("eia")
    assert bf.state == BackfillState.RUNNING
    assert bf.cursor is not None
    cursor = date.fromisoformat(bf.cursor)
    assert date(2021, 10, 20) + timedelta(days=199) <= cursor <= date(2021, 10, 20) + timedelta(299)
    assert 0 < bf.progress < 1
    assert backfill.summary(store, collectors).state == "backfilling"
    assert store.get_backfill("eu_oil_bulletin").state == BackfillState.DONE

    # Restart: the done collector is not fetched again; the other resumes.
    results = await backfill.run(store, client, collectors=collectors, now=lambda: NOW)
    assert done_first.calls == [None]
    assert big.calls == [None, cursor]
    assert results["eia"].state == BackfillState.DONE
    assert len(store.points("eia.spot.brent")) == 1000
    assert store.get_backfill("eia").progress == 1.0


async def test_failure_is_recorded_retried_and_resumable(store, client):
    f = Fake(error=ProviderError("http_status", "429 from api.eia.gov"))
    ok = Fake(daily("portwatch.hormuz.all", 5))
    results = await backfill.run(
        store,
        client,
        collectors=[coll("eia", f), coll("portwatch", ok)],
        max_attempts=3,
        retry_start=0.001,
    )
    assert len(f.calls) == 3
    assert results["eia"].error == "429 from api.eia.gov"
    assert results["portwatch"].state == BackfillState.DONE  # not blocked by eia
    st = store.get_status("eia")
    assert st.error_class == "http_status"
    # Failed, not "running": nothing is running, and a later run retries.
    assert st.backfill.state == BackfillState.NOT_STARTED
    s = backfill.summary(store, [coll("eia", f), coll("portwatch", ok)])
    assert s.state == "backfilling" and s.errors == {"eia": "429 from api.eia.gov"}


async def test_internal_error_message_is_type_only(store, client):
    f = Fake(error=ValueError("raw text with secrets"))
    results = await backfill.run(store, client, collectors=[coll("eia", f)], max_attempts=1)
    assert results["eia"].error == "ValueError"
    assert store.get_status("eia").error_message == "ValueError"


async def test_stop_ends_retry_wait(store, client):
    f = Fake(error=ProviderError("transport", "ConnectError"))
    stop = asyncio.Event()
    task = asyncio.create_task(
        backfill.run(store, client, collectors=[coll("eia", f)], stop=stop, retry_start=60)
    )
    await until(lambda: f.calls)
    stop.set()
    await asyncio.wait_for(task, timeout=1)


# --- the event loop keeps running ------------------------------------------------------
#
# Deterministic: the blocking work waits (in its thread) for a tick of the
# event loop. If the work ran on the loop thread, the loop could not tick,
# the wait would time out and the test would fail.


class LoopProbe:
    def __init__(self):
        self.ticked = threading.Event()
        self.saw_tick: list[bool] = []
        self._task = None

    async def __aenter__(self):
        async def tick():
            while True:
                await asyncio.sleep(0)
                self.ticked.set()

        self._task = asyncio.create_task(tick())
        return self

    async def __aexit__(self, *exc):
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    def blocking(self, fn):
        """Wrap a blocking function: it records whether the loop ticked
        while it ran."""

        def wrapper(*a, **kw):
            self.ticked.clear()
            self.saw_tick.append(self.ticked.wait(timeout=5))
            return fn(*a, **kw)

        return wrapper


@respx.mock
async def test_loop_runs_during_eu_parse(store, client, monkeypatch):
    content = (FIX / "eu_oil_bulletin" / "history_with_taxes_trimmed.xlsx").read_bytes()
    respx.get(eu_oil_bulletin.HISTORY_URL).mock(
        return_value=httpx.Response(200, content=content, headers={"content-type": "x/xlsx"})
    )
    async with LoopProbe() as probe:
        monkeypatch.setattr(eu_oil_bulletin, "read_rows", probe.blocking(eu_oil_bulletin.read_rows))
        c = REGISTRY.collector("eu_oil_bulletin")
        results = await backfill.run(store, PacedClient(client), collectors=[c], now=lambda: NOW)
    assert results["eu_oil_bulletin"].state == BackfillState.DONE
    assert probe.saw_tick == [True]


async def test_loop_runs_during_writes(store, client, monkeypatch):
    many = [p for s in ("eia.spot.brent", "eia.spot.wti") for p in daily(s, 1830)]
    c = coll("eia", Fake(many))
    async with LoopProbe() as probe:
        monkeypatch.setattr(store, "upsert_points", probe.blocking(store.upsert_points))
        results = await backfill.run(store, client, collectors=[c], now=lambda: NOW)
    assert results["eia"].rows == len(many)
    assert len(probe.saw_tick) == 2 and all(probe.saw_tick)  # two 2,000-point chunks


@respx.mock
async def test_loop_runs_during_portwatch_parse(store, client, monkeypatch):
    import json

    page = json.loads((FIX / "portwatch" / "recent_page1.json").read_text())
    page["exceededTransferLimit"] = False
    respx.get(portwatch.QUERY_URL).mock(return_value=httpx.Response(200, json=page))
    async with LoopProbe() as probe:
        monkeypatch.setattr(portwatch, "parse_page", probe.blocking(portwatch.parse_page))
        monkeypatch.setattr(portwatch, "to_points", probe.blocking(portwatch.to_points))
        c = REGISTRY.collector("portwatch")
        results = await backfill.run(store, PacedClient(client), collectors=[c], now=lambda: NOW)
    assert results["portwatch"].rows > 0
    assert probe.saw_tick == [True, True]


# --- plausibility and store failures -----------------------------------------------------


async def test_empty_full_backfill_is_a_failure(store, client, monkeypatch):
    monkeypatch.setattr(backfill, "MIN_POINTS_PER_SERIES", 52)
    f = Fake([])
    results = await backfill.run(
        store, client, collectors=[coll("eia", f)], max_attempts=2, retry_start=0.001
    )
    assert len(f.calls) == 2  # retried, not marked done
    assert results["eia"].error.startswith("eia: backfill returned 0 points for 23 series")
    st = store.get_status("eia")
    assert st.error_class == "implausible"
    assert st.backfill.state == BackfillState.NOT_STARTED


async def test_implausibly_small_backfill_is_a_failure(store, client, monkeypatch):
    monkeypatch.setattr(backfill, "MIN_POINTS_PER_SERIES", 52)
    c = coll("eu_oil_bulletin", Fake(daily("eu_pump_petrol_de", 519)))  # need 52 x 10
    r = await backfill.backfill_collector(store, client, c)
    assert r.error is not None and "expected at least 520" in r.error
    ok = coll("eu_oil_bulletin", Fake(daily("eu_pump_petrol_de", 520)))
    assert (await backfill.backfill_collector(store, client, ok)).state == BackfillState.DONE


async def test_empty_resume_is_a_failure(store, client):
    store.set_backfill("eia", BackfillState.RUNNING, "2026-10-01", 0.9)
    r = await backfill.backfill_collector(store, client, coll("eia", Fake([])))
    assert r.error == "eia: no points since 2026-10-01"
    assert store.get_backfill("eia").cursor == "2026-10-01"


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


async def test_backfill_survives_a_failing_store(store, client):
    locked = LockedStore(store, {"get_backfill", "set_backfill", "record_failure"})
    f = Fake(daily("eia.spot.brent", 5))
    results = await backfill.run(
        locked, client, collectors=[coll("eia", f)], max_attempts=2, retry_start=0.001
    )
    assert results["eia"].error == "OperationalError"
    # The store recovers: the next run completes.
    locked.failing.clear()
    results = await backfill.run(locked, client, collectors=[coll("eia", f)])
    assert results["eia"].state == BackfillState.DONE


async def test_failed_write_does_not_leave_state_running(store, client):
    locked = LockedStore(store, {"upsert_points"})
    r = await backfill.backfill_collector(
        locked, client, coll("eia", Fake(daily("eia.spot.brent", 5)))
    )
    assert r.error == "OperationalError"
    st = store.get_status("eia")
    assert st.backfill.state == BackfillState.NOT_STARTED
    assert st.error_class == "internal" and st.error_message == "OperationalError"


async def test_empty_selection_means_none(store, client):
    f = Fake(daily("eia.spot.brent", 5))
    assert await backfill.run(store, client, collectors=[]) == {}
    assert backfill.summary(store, []).collectors == {}
    assert f.calls == []


# --- pacing ------------------------------------------------------------------------------


class FakeTime:
    def __init__(self):
        self.t = 0.0
        self.slept: list[float] = []

    def clock(self):
        return self.t

    async def sleep(self, s):
        self.slept.append(s)
        self.t += s
        await asyncio.sleep(0)


@respx.mock
async def test_paced_client_spaces_and_counts(client):
    respx.get("https://api.eia.gov/v2/x").mock(return_value=httpx.Response(200, json={}))
    respx.get("https://example.org/y").mock(return_value=httpx.Response(200, json={}))
    ft = FakeTime()
    p = PacedClient(client, per_host={"api.eia.gov": 0.5}, sleep=ft.sleep, clock=ft.clock)
    for _ in range(3):
        await p.get("https://api.eia.gov/v2/x")
    await p.get("https://example.org/y")
    assert ft.slept == [0.5, 0.5]
    assert p.requests == {"api.eia.gov": 3, "example.org": 1}


@respx.mock
async def test_paced_client_retries_rate_limit(client):
    route = respx.get("https://api.eia.gov/v2/x").mock(
        side_effect=[
            httpx.Response(429, headers={"retry-after": "0"}),
            httpx.Response(503),
            httpx.Response(200, json={"ok": 1}),
        ]
    )
    ft = FakeTime()
    p = PacedClient(client, retry_delay=7, sleep=ft.sleep, clock=ft.clock)
    resp = await p.get("https://api.eia.gov/v2/x")
    assert resp.status_code == 200 and route.call_count == 3
    assert p.rate_limited == {"api.eia.gov": 2}
    assert ft.slept == [7]  # Retry-After: 0 retries at once, then the default delay


@respx.mock
async def test_paced_client_gives_up(client):
    respx.get("https://api.eia.gov/v2/x").mock(return_value=httpx.Response(429))
    ft = FakeTime()
    p = PacedClient(client, retries=2, sleep=ft.sleep, clock=ft.clock)
    assert (await p.get("https://api.eia.gov/v2/x")).status_code == 429
    assert p.requests == {"api.eia.gov": 3}


@respx.mock
async def test_spacing_stays_bounded_through_a_run_of_503s(client, caplog):
    respx.get("https://api.eia.gov/v2/x").mock(
        return_value=httpx.Response(503, headers={"retry-after": "1e12"})
    )
    ft = FakeTime()
    p = PacedClient(client, per_host={"api.eia.gov": 0.5}, sleep=ft.sleep, clock=ft.clock)
    for _ in range(20):  # 20 failed polls, 4 attempts each
        await p.get("https://api.eia.gov/v2/x")
    assert p.spacing("api.eia.gov") == 60.0
    assert max(ft.slept) == 600.0  # Retry-After clamped to max_retry_delay
    assert "retrying in 600 s" in caplog.text and "1000000000000" not in caplog.text


@respx.mock
async def test_spacing_doubles_once_per_get_and_recovers(client):
    route = respx.get("https://api.eia.gov/v2/x")
    route.side_effect = [httpx.Response(503)] * 4 + [httpx.Response(200, json={})]
    ft = FakeTime()
    p = PacedClient(client, per_host={"api.eia.gov": 0.5}, sleep=ft.sleep, clock=ft.clock)
    await p.get("https://api.eia.gov/v2/x")  # four 503s
    assert p.spacing("api.eia.gov") == 1.0  # doubled once (at least 1 s), not four times
    await p.get("https://api.eia.gov/v2/x")  # success
    assert p.spacing("api.eia.gov") == 0.5


async def test_stop_interrupts_a_long_wait(client):
    from fuelwatch.http import Stopped

    stop = asyncio.Event()
    p = PacedClient(client, per_host={"api.eia.gov": 3600.0}, stop=stop)
    p._next["api.eia.gov"] = p._clock() + 3600.0  # the host's next slot is an hour away
    task = asyncio.create_task(p.get("https://api.eia.gov/v2/x"))
    await asyncio.sleep(0)
    stop.set()
    with pytest.raises(Stopped):
        await asyncio.wait_for(task, timeout=1)
    assert p.requests == {}


async def test_pacing_never_sleeps_holding_the_lock(client):
    stop = asyncio.Event()
    p = PacedClient(client, per_host={"a.example": 3600.0}, stop=stop)
    p._next["a.example"] = p._clock() + 3600.0
    slow = asyncio.create_task(p._pace("a.example"))
    await asyncio.sleep(0)
    # Another host is not held up by the sleeping one.
    await asyncio.wait_for(p._pace("b.example"), timeout=1)
    stop.set()
    with pytest.raises(Exception):  # noqa: B017 - Stopped
        await slow


# --- the CLI -----------------------------------------------------------------------------


def test_cli_backfill_and_collect_once(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FW_DATA_DIR", str(tmp_path))
    calls = []

    def fake(series):
        async def fetch(client, since=None):
            calls.append((series, since))
            return daily(series, 10, start=date(2026, 10, 1))

        return fetch

    monkeypatch.setattr(eia, "fetch", fake("eia.spot.brent"))
    monkeypatch.setattr(portwatch, "fetch", fake("portwatch.suez.all"))

    async def broken(client, since=None):
        raise ProviderError("bad_xlsx", "response is not a readable XLSX workbook")

    monkeypatch.setattr(eu_oil_bulletin, "fetch", broken)
    monkeypatch.setattr(backfill, "RETRY_START", 0.001)

    assert cli_main(["backfill", "--attempts", "1"]) == 1
    out = capsys.readouterr().out
    assert "eia" in out and "10 rows in 1/23 series" in out
    assert "ERROR response is not a readable XLSX workbook" in out
    assert ("eia.spot.brent", None) in calls

    assert cli_main(["backfill", "--only", "eia"]) == 0
    assert "already done" in capsys.readouterr().out

    calls.clear()
    assert cli_main(["collect", "--once", "--only", "eia", "--only", "portwatch"]) == 0
    out = capsys.readouterr().out
    assert sorted(calls) == [
        ("eia.spot.brent", date(2026, 10, 10) - timedelta(days=14)),
        ("portwatch.suez.all", date(2026, 10, 10) - timedelta(days=35)),
    ]
    assert "10 points" in out


def test_cli_unknown_collector_is_an_error(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FW_DATA_DIR", str(tmp_path))
    for argv in (["backfill", "--only", "eai"], ["collect", "--once", "--only", "eai"]):
        with pytest.raises(SystemExit) as ei:
            cli_main(argv)
        assert ei.value.code == 2
        err = capsys.readouterr().err
        assert "invalid choice: 'eai'" in err and "eia" in err and "portwatch" in err


# --- second review round -----------------------------------------------------------------


def _every_eia_series(n=60):
    return [p for s in REGISTRY.collector("eia").series for p in daily(s.id, n)]


async def test_partial_outage_is_a_failure(store, client, monkeypatch):
    """EIA's spot route answers but retail, stocks and refinery come back
    empty: enough points on average, but 19 series have none."""
    monkeypatch.setattr(backfill, "MIN_POINTS_PER_SERIES", 52)
    monkeypatch.setattr(backfill, "REQUIRE_EVERY_SERIES", True)
    spot = [
        p
        for s in ("brent", "wti", "nyh_gasoline", "nyh_ulsd")
        for p in daily(f"eia.spot.{s}", 1250)
    ]
    assert len(spot) >= 52 * 23  # the average alone would pass
    r = await backfill.backfill_collector(store, client, coll("eia", Fake(spot)))
    assert r.error is not None
    assert "no points for 19 of 23 series" in r.error and "eia.refinery.utilization" in r.error
    assert store.get_backfill("eia").state == BackfillState.NOT_STARTED
    assert store.get_status("eia").error_class == "implausible"

    ok = await backfill.backfill_collector(store, client, coll("eia", Fake(_every_eia_series())))
    assert ok.state == BackfillState.DONE


async def test_failed_record_failure_still_resets_state(store, client):
    locked = LockedStore(store, {"record_failure"})
    r = await backfill.backfill_collector(
        locked, client, coll("eia", Fake(error=ProviderError("http_status", "503")))
    )
    assert r.error == "503"
    assert store.get_backfill("eia").state == BackfillState.NOT_STARTED


def test_failure_log_traceback_once_per_streak(caplog):
    import logging

    fl = backfill.FailureLog(logging.getLogger("fuelwatch.test_failure_log"))
    caplog.set_level(logging.DEBUG, logger="fuelwatch.test_failure_log")

    def boom():
        try:
            raise TypeError("bad parser")
        except TypeError as e:
            return e

    fl.report("eia", "eia poll", boom())
    fl.report("eia", "eia poll", boom())
    fl.report("eia", "eia poll", ProviderError("http_status", "503"))
    fl.recovered("eia")
    fl.report("eia", "eia poll", boom())
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    # exc_text once RedactingFilter (installed by the CLI tests) has formatted it
    assert [bool(r.exc_info or r.exc_text) for r in errors] == [True, False, True]
    assert "(repeat)" in errors[1].getMessage()
    assert [r.levelno for r in caplog.records].count(logging.WARNING) == 1


async def test_drain_timeout_holds_with_executor_saturated():
    from concurrent.futures import ThreadPoolExecutor

    from fuelwatch.threads import ThreadCalls

    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(pool)
    release = threading.Event()
    calls = ThreadCalls()
    task = asyncio.create_task(calls(release.wait, 5))
    await until(lambda: calls.running == 1)
    t0 = loop.time()
    assert await calls.drain(timeout=0.05) is False  # the only worker is busy
    assert loop.time() - t0 < 1
    release.set()
    await task
    assert await calls.drain(timeout=1) is True
