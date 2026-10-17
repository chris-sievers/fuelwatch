"""The background five-year backfill.

For each polled collector whose backfill is not done: fetch with
``since=None`` (the collector's own five-year start), or from the saved
cursor after a restart; write the points oldest first in chunks; then mark
it done. Collectors run one after another, so each provider sees one
paced stream of requests.

State lives in the store's backfill fields, per collector:

- ``state``: not_started, running or done;
- ``cursor``: ISO date of the newest period written so far. Points are
  written in period order, so everything before the cursor is stored, and a
  restart resumes with ``since=cursor`` (inclusive; upserts are idempotent);
- ``progress``: how much of the five-year span the cursor has covered, 0 to 1.

Nothing here blocks the event loop: the collectors parse in worker threads
and every store call goes through ``asyncio.to_thread``.

Provider limits: the whole backfill is about 6 EIA requests (at most 5,000
rows each, against about 9,000 requests an hour), about 10 PortWatch pages
spaced two seconds apart (IMF asks for paced automated download), and one
4.5 MB EU history workbook.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

from fuelwatch.http import PacedClient, ProviderError, make_client
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Collector
from fuelwatch.store import BackfillState, Store

log = logging.getLogger(__name__)

SPAN = timedelta(days=5 * 366)  # what progress is measured against
CHUNK = 2000  # points per upsert transaction
RETRY_START = 60.0  # seconds before retrying a failed collector, doubling
RETRY_MAX = 6 * 3600.0

# Minimum seconds between requests to one host.
PACING = {
    "api.eia.gov": 0.5,
    "services9.arcgis.com": 2.0,
    "energy.ec.europa.eu": 1.0,
}


@dataclass(frozen=True)
class Result:
    collector: str
    state: BackfillState
    rows: int = 0  # points written by this run
    skipped: bool = False  # already done
    error: str | None = None  # sanitized


def paced(client: httpx.AsyncClient | PacedClient) -> PacedClient:
    return client if isinstance(client, PacedClient) else PacedClient(client, per_host=PACING)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _progress(cursor: date, today: date) -> float:
    start = today - SPAN
    frac = (cursor - start).days / max(1, (today - start).days)
    return min(1.0, max(0.0, frac))


def _sorted(points: list[Point]) -> list[Point]:
    return sorted(points, key=lambda p: (p.period_end, p.series))


async def backfill_collector(
    store: Store,
    client,
    collector: Collector,
    *,
    now: Callable[[], datetime] = _utcnow,
    stop: asyncio.Event | None = None,
) -> Result:
    """Backfill one collector, or skip it if done. Never raises for a
    provider or store failure: the error is recorded in status and
    returned, and the state stays resumable."""
    name = collector.name
    bf = await asyncio.to_thread(store.get_backfill, name)
    if bf.state == BackfillState.DONE:
        return Result(name, bf.state, skipped=True)

    since = date.fromisoformat(bf.cursor) if bf.cursor else None
    today = now().date()
    start_progress = _progress(since, today) if since else 0.0
    await asyncio.to_thread(
        store.set_backfill, name, BackfillState.RUNNING, bf.cursor, start_progress
    )
    log.info("backfill %s: fetching %s", name, f"since {since}" if since else "five years")
    written = 0
    try:
        points = await collector.fetch(client, since)
        points = await asyncio.to_thread(_sorted, points)
        for i in range(0, len(points), CHUNK):
            if stop is not None and stop.is_set():
                return Result(name, BackfillState.RUNNING, written)
            chunk = points[i : i + CHUNK]
            await asyncio.to_thread(store.upsert_points, chunk)
            written += len(chunk)
            cursor = chunk[-1].period_end.date()
            await asyncio.to_thread(
                store.set_backfill,
                name,
                BackfillState.RUNNING,
                cursor.isoformat(),
                max(start_progress, _progress(cursor, today)),
            )
        final_cursor = points[-1].period_end.date().isoformat() if points else bf.cursor
        await asyncio.to_thread(store.set_backfill, name, BackfillState.DONE, final_cursor, 1.0)
        await asyncio.to_thread(store.record_success, name)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # recorded sanitized, never re-raised
        await asyncio.to_thread(store.record_failure, name, e)
        status = await asyncio.to_thread(store.get_status, name)
        msg = status.error_message if status else type(e).__name__
        if isinstance(e, ProviderError):
            log.warning("backfill %s failed: %s", name, msg)
        else:
            log.error("backfill %s: internal error %s", name, type(e).__name__)
            log.debug("backfill %s traceback", name, exc_info=True)
        return Result(name, BackfillState.RUNNING, written, error=msg)
    log.info("backfill %s: done, %d points", name, written)
    return Result(name, BackfillState.DONE, written)


async def wait_or_stop(stop: asyncio.Event | None, seconds: float) -> bool:
    """Sleep up to `seconds`; True if `stop` was set."""
    if stop is None:
        await asyncio.sleep(seconds)
        return False
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
        return True
    except TimeoutError:
        return False


async def run(
    store: Store,
    client: httpx.AsyncClient | PacedClient | None = None,
    *,
    collectors: Iterable[Collector] | None = None,
    stop: asyncio.Event | None = None,
    max_attempts: int | None = None,
    retry_start: float = RETRY_START,
    retry_max: float = RETRY_MAX,
    now: Callable[[], datetime] = _utcnow,
) -> dict[str, Result]:
    """Backfill every polled collector that is not done, retrying failures
    with a doubling delay. Returns when all are done, `stop` is set, or a
    collector has failed `max_attempts` times (None: keep retrying, as in
    the app)."""
    collectors = [c for c in (collectors or REGISTRY.polled) if c.polled]
    own = client is None
    raw = make_client() if own else client
    http = paced(raw)
    results: dict[str, Result] = {}
    attempts: dict[str, int] = {}
    delay = retry_start
    try:
        pending = list(collectors)
        while pending and not (stop is not None and stop.is_set()):
            failed = []
            for c in pending:
                if stop is not None and stop.is_set():
                    break
                r = await backfill_collector(store, http, c, now=now, stop=stop)
                results[c.name] = r
                if r.error is not None:
                    attempts[c.name] = attempts.get(c.name, 0) + 1
                    if max_attempts is None or attempts[c.name] < max_attempts:
                        failed.append(c)
            pending = failed
            if pending:
                wait = delay * random.uniform(0.9, 1.1)
                log.info("backfill: retrying %s in %.0f s", [c.name for c in pending], wait)
                if await wait_or_stop(stop, wait):
                    break
                delay = min(delay * 2, retry_max)
    finally:
        if own:
            await raw.aclose()
    return results


@dataclass(frozen=True)
class Summary:
    """Overall backfill state for /api/status."""

    state: str  # "backfilling", "done" or "not_started"
    progress: float  # mean over polled collectors, 0 to 1
    collectors: dict[str, BackfillState]


def summary(store: Store, collectors: Iterable[Collector] | None = None) -> Summary:
    """Synchronous; call it through asyncio.to_thread."""
    names = [c.name for c in (collectors or REGISTRY.polled) if c.polled]
    states = {n: store.get_backfill(n) for n in names}
    progress = sum(1.0 if b.state == BackfillState.DONE else b.progress for b in states.values())
    progress /= max(1, len(states))
    if all(b.state == BackfillState.DONE for b in states.values()):
        overall = "done"
    elif all(b.state == BackfillState.NOT_STARTED for b in states.values()):
        overall = "not_started"
    else:
        overall = "backfilling"
    return Summary(overall, round(progress, 4), {n: b.state for n, b in states.items()})


async def reset(store: Store, names: Iterable[str]) -> None:
    """Mark collectors not started, so the next run backfills them again."""
    for n in names:
        await asyncio.to_thread(store.set_backfill, n, BackfillState.NOT_STARTED, None, 0.0)
