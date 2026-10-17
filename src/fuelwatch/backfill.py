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

A failed attempt records its sanitized reason in status (``record_failure``)
and sets the state back to not_started, keeping cursor and progress, so the
state never sits at "running" while nothing runs; ``run`` retries it later
with a doubling delay. The scheduler starts polling a collector whose
backfill has failed, so recent data keeps arriving while history waits.

A full fetch that returns fewer than MIN_POINTS_PER_SERIES points per
registered series on average is treated as a failure, not done. Five years
of the sparsest series (weekly) is about 260 points per series, so less
than one year's worth means an outage or a truncated answer, and marking it
done would leave the history missing for good (polls only fetch recent
weeks). A resumed fetch must return at least one point (the cursor's day).

Nothing here blocks the event loop: the collectors parse in worker threads
and every store call goes through a worker thread (``ThreadCalls``).

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

from fuelwatch.http import PacedClient, ProviderError, Stopped, make_client, redact
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Collector
from fuelwatch.store import BackfillState, Store
from fuelwatch.threads import ThreadCalls

log = logging.getLogger(__name__)

SPAN = timedelta(days=5 * 366)  # what progress is measured against
CHUNK = 2000  # points per upsert transaction
RETRY_START = 60.0  # seconds before retrying a failed collector, doubling
RETRY_MAX = 6 * 3600.0
MIN_POINTS_PER_SERIES = 52  # one year of weekly data; see the module docstring

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


def check_plausible(collector: Collector, points: list[Point], since: date | None) -> None:
    """Raise ProviderError unless the fetch looks like real history."""
    if since is not None:
        if not points:
            raise ProviderError("empty", f"{collector.name}: no points since {since}")
        return
    need = MIN_POINTS_PER_SERIES * len(collector.series)
    if len(points) < need:
        raise ProviderError(
            "implausible",
            f"{collector.name}: backfill returned {len(points)} points for "
            f"{len(collector.series)} series, expected at least {need}",
        )


async def unless_stopped(aw, stop: asyncio.Event | None):
    """Await `aw`, but cancel it and raise Stopped as soon as `stop` is set,
    so shutdown never waits on a slow provider."""
    if stop is None:
        return await aw
    if stop.is_set():
        if asyncio.iscoroutine(aw):
            aw.close()
        raise Stopped()
    task = asyncio.ensure_future(aw)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopper.cancel()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, stopper, return_exceptions=True)
    if task.cancelled():
        raise Stopped()
    return task.result()


def _message(e: BaseException) -> str:
    """What may be logged or returned for an exception: a ProviderError's
    sanitized text, or just the type name for anything else."""
    return str(e) if isinstance(e, ProviderError) else type(e).__name__


async def backfill_collector(
    store: Store,
    client,
    collector: Collector,
    *,
    now: Callable[[], datetime] = _utcnow,
    stop: asyncio.Event | None = None,
    io: ThreadCalls | None = None,
) -> Result:
    """Backfill one collector, or skip it if done. Never raises except on
    cancellation: any failure, the store's included, is logged sanitized,
    recorded when the store allows, and returned."""
    io = io or ThreadCalls()
    name = collector.name
    written = 0
    cursor: str | None = None
    progress = 0.0
    try:
        bf = await io(store.get_backfill, name)
        if bf.state == BackfillState.DONE:
            return Result(name, bf.state, skipped=True)
        cursor, progress = bf.cursor, bf.progress
        since = date.fromisoformat(cursor) if cursor else None
        today = now().date()
        start_progress = _progress(since, today) if since else 0.0
        await io(store.set_backfill, name, BackfillState.RUNNING, cursor, start_progress)
        log.info("backfill %s: fetching %s", name, f"since {since}" if since else "five years")
        points = await unless_stopped(collector.fetch(client, since), stop)
        check_plausible(collector, points, since)
        points = await io(_sorted, points)
        for i in range(0, len(points), CHUNK):
            if stop is not None and stop.is_set():
                return Result(name, BackfillState.RUNNING, written)
            chunk = points[i : i + CHUNK]
            await io(store.upsert_points, chunk)
            written += len(chunk)
            cursor = chunk[-1].period_end.date().isoformat()
            progress = max(start_progress, _progress(date.fromisoformat(cursor), today))
            await io(store.set_backfill, name, BackfillState.RUNNING, cursor, progress)
        await io(store.set_backfill, name, BackfillState.DONE, cursor, 1.0)
        await io(store.record_success, name)
    except asyncio.CancelledError:
        raise
    except Stopped:
        return Result(name, BackfillState.RUNNING, written)  # resumes on the next start
    except Exception as e:
        msg = _message(e)
        if isinstance(e, ProviderError):
            log.warning("backfill %s failed: %s", name, msg)
        else:
            log.error("backfill %s: internal error %s", name, msg)
            log.debug("backfill %s traceback", name, exc_info=True)
        await _record_failed(store, io, name, e, cursor, progress)
        return Result(name, BackfillState.NOT_STARTED, written, error=redact(msg))
    log.info("backfill %s: done, %d points", name, written)
    return Result(name, BackfillState.DONE, written)


async def _record_failed(store, io, name, error, cursor, progress) -> None:
    """Best effort: the store may be what failed."""
    try:
        await io(store.record_failure, name, error)
        await io(store.set_backfill, name, BackfillState.NOT_STARTED, cursor, progress)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 - logged; the retry loop carries on
        log.error("backfill %s: could not record the failure: %s", name, type(e).__name__)


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


def _select(collectors: Iterable[Collector] | None) -> list[Collector]:
    """None means every polled collector; an empty list means none."""
    return [c for c in (REGISTRY.polled if collectors is None else collectors) if c.polled]


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
    io: ThreadCalls | None = None,
    on_result: Callable[[Result], None] | None = None,
) -> dict[str, Result]:
    """Backfill every collector given (None: every polled one) that is not
    done, retrying failures with a doubling delay. Returns when all are
    done, `stop` is set, or a collector has failed `max_attempts` times
    (None: keep retrying, as in the app). `on_result` is called after each
    attempt."""
    collectors = _select(collectors)
    io = io or ThreadCalls()
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
                try:
                    r = await backfill_collector(store, http, c, now=now, stop=stop, io=io)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - backfill_collector should not raise
                    r = Result(c.name, BackfillState.NOT_STARTED, error=_message(e))
                results[c.name] = r
                if on_result is not None:
                    on_result(r)
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
    progress: float  # mean over the collectors, 0 to 1
    collectors: dict[str, BackfillState]
    errors: dict[str, str]  # collector -> sanitized reason its last attempt failed


def summary(store: Store, collectors: Iterable[Collector] | None = None) -> Summary:
    """Synchronous; call it through asyncio.to_thread."""
    names = [c.name for c in _select(collectors)]
    statuses = {n: store.get_status(n) for n in names}
    states = {n: store.get_backfill(n) for n in names}
    progress = sum(1.0 if b.state == BackfillState.DONE else b.progress for b in states.values())
    progress /= max(1, len(states))
    errors = {
        n: st.error_message or st.error_class
        for n, st in statuses.items()
        if st is not None and st.error_class and states[n].state != BackfillState.DONE
    }
    if all(b.state == BackfillState.DONE for b in states.values()):
        overall = "done"
    elif all(b.state == BackfillState.NOT_STARTED for b in states.values()) and not errors:
        overall = "not_started"
    else:
        overall = "backfilling"
    return Summary(overall, round(progress, 4), {n: b.state for n, b in states.items()}, errors)


async def reset(store: Store, names: Iterable[str]) -> None:
    """Mark collectors not started, so the next run backfills them again."""
    for n in names:
        await asyncio.to_thread(store.set_backfill, n, BackfillState.NOT_STARTED, None, 0.0)
