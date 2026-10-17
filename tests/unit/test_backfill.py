import asyncio
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
    assert st.backfill.state == BackfillState.RUNNING


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
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)


# --- the event loop keeps running ------------------------------------------------------


async def _max_gap(coro, interval=0.01):
    gaps = []
    done = False

    async def ticker():
        last = time.monotonic()
        while not done:
            await asyncio.sleep(interval)
            t = time.monotonic()
            gaps.append(t - last)
            last = t

    t = asyncio.create_task(ticker())
    try:
        result = await coro
    finally:
        done = True
        await t
    return result, max(gaps), len(gaps)


@respx.mock
async def test_loop_responsive_during_slow_eu_parse(store, client, monkeypatch):
    content = (FIX / "eu_oil_bulletin" / "history_with_taxes_trimmed.xlsx").read_bytes()
    respx.get(eu_oil_bulletin.HISTORY_URL).mock(
        return_value=httpx.Response(200, content=content, headers={"content-type": "x/xlsx"})
    )
    real = eu_oil_bulletin.read_rows

    def slow_read_rows(*a, **kw):
        time.sleep(0.5)  # a slow, blocking parse
        return real(*a, **kw)

    monkeypatch.setattr(eu_oil_bulletin, "read_rows", slow_read_rows)
    c = REGISTRY.collector("eu_oil_bulletin")
    results, gap, ticks = await _max_gap(
        backfill.run(store, PacedClient(client), collectors=[c], now=lambda: NOW)
    )
    assert results["eu_oil_bulletin"].state == BackfillState.DONE
    assert ticks >= 20
    assert gap < 0.2


async def test_loop_responsive_during_large_write(store, client):
    many = [p for s in ("eia.spot.brent", "eia.spot.wti") for p in daily(s, 1830)]
    many *= 10  # 36,600 points, repeated periods
    c = coll("eia", Fake(many))
    results, gap, _ = await _max_gap(backfill.run(store, client, collectors=[c], now=lambda: NOW))
    assert results["eia"].rows == len(many)
    assert gap < 0.2


@respx.mock
async def test_loop_responsive_during_real_portwatch_parse(store, client):
    import json

    page = json.loads((FIX / "portwatch" / "recent_page1.json").read_text())
    page["exceededTransferLimit"] = False
    respx.get(portwatch.QUERY_URL).mock(return_value=httpx.Response(200, json=page))
    c = REGISTRY.collector("portwatch")
    results, gap, _ = await _max_gap(
        backfill.run(store, PacedClient(client), collectors=[c], now=lambda: NOW)
    )
    assert results["portwatch"].rows > 0
    assert gap < 0.2


# --- pacing ------------------------------------------------------------------------------


@respx.mock
async def test_paced_client_spaces_and_counts(client):
    respx.get("https://api.eia.gov/v2/x").mock(return_value=httpx.Response(200, json={}))
    respx.get("https://example.org/y").mock(return_value=httpx.Response(200, json={}))
    p = PacedClient(client, per_host={"api.eia.gov": 0.1})
    t0 = time.monotonic()
    for _ in range(3):
        await p.get("https://api.eia.gov/v2/x")
    await p.get("https://example.org/y")
    assert time.monotonic() - t0 >= 0.2
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
    slept = []

    async def sleep(s):
        slept.append(s)

    p = PacedClient(client, retry_delay=7, sleep=sleep)
    resp = await p.get("https://api.eia.gov/v2/x")
    assert resp.status_code == 200 and route.call_count == 3
    assert p.rate_limited == {"api.eia.gov": 2}
    assert 0 in slept and 7 in slept  # Retry-After, then the default delay


@respx.mock
async def test_paced_client_gives_up(client):
    respx.get("https://api.eia.gov/v2/x").mock(return_value=httpx.Response(429))

    async def sleep(s):
        pass

    p = PacedClient(client, retries=2, sleep=sleep)
    assert (await p.get("https://api.eia.gov/v2/x")).status_code == 429
    assert p.requests == {"api.eia.gov": 3}


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
