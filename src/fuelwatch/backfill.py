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

Plausibility is judged after the points are written, so good history is
never thrown away:

- a full fetch with no points at all is a failure (not_started, as above);
- if every registered series now has data and the fetch averaged at least
  MIN_POINTS_PER_SERIES points per series, the collector is done;
- otherwise it is **partial**: the history that came back is kept, polls
  run normally, the missing series are named in status, and ``run`` retries
  a full fetch with a delay doubling from PARTIAL_RETRY_START to
  PARTIAL_RETRY_MAX (24 h: it is a heavy refetch). A series that a provider
  really retired keeps the collector partial until an operator runs
  ``fuelwatch backfill --only NAME --accept-missing``, which marks it done
  and keeps the missing list. Freshness still shows those series as
  no_data, so the dashboard and alerts keep pointing at them.

Five years of the sparsest series (weekly) is about 260 points per series,
so an average below one year's worth (52) means a truncated answer. A
resumed fetch must return at least one point (the cursor's day).

The store's backfill_state column allows only not_started, running and
done (a CHECK constraint; widening it would need a table rebuild, which the
additive-only migrations forbid, and would break a rolled-back image that
reads the row). So "partial" is state done plus a JSON cursor, which the
store treats as opaque text: ``{"partial": {"attempt": n, "missing": [...],
"points": n, "need": n}}``, or ``{"accepted_missing": [...]}`` once accepted.
Read it through ``summary()`` or ``partial_info()``.

Nothing here blocks the event loop: the collectors parse in worker threads
and every store call goes through a worker thread (``ThreadCalls``).

Provider limits: the whole backfill is about 6 EIA requests (at most 5,000
rows each, against about 9,000 requests an hour), about 10 PortWatch pages
spaced two seconds apart (IMF asks for paced automated download), and one
4.5 MB EU history workbook.
"""

from __future__ import annotations

import asyncio
import json
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
from fuelwatch.threads import ThreadCalls, is_store_closed

log = logging.getLogger(__name__)

SPAN = timedelta(days=5 * 366)  # what progress is measured against
CHUNK = 2000  # points per upsert transaction
RETRY_START = 60.0  # seconds before retrying a failed collector, doubling
RETRY_MAX = 6 * 3600.0
MIN_POINTS_PER_SERIES = 52  # one year of weekly data; see the module docstring
PARTIAL_RETRY_START = 3600.0  # seconds before re-fetching a partial collector, doubling
PARTIAL_RETRY_MAX = 24 * 3600.0

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
    skipped: bool = False  # already done (or partial and not due for a retry)
    error: str | None = None  # sanitized
    missing: tuple[str, ...] = ()  # partial: registered series with no data
    retry_in: float | None = None  # partial: seconds until the next full fetch

    @property
    def partial(self) -> bool:
        return self.state == BackfillState.DONE and bool(self.missing or self.retry_in)


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


def partial_info(cursor: str | None) -> dict | None:
    """The JSON marker a done collector's cursor holds when its backfill is
    partial or its missing series were accepted; None otherwise."""
    if not cursor or not cursor.startswith("{"):
        return None
    try:
        info = json.loads(cursor)
    except ValueError:
        return None
    return info if isinstance(info, dict) else None


def partial_delay(attempt: int) -> float:
    return min(PARTIAL_RETRY_START * 2 ** min(max(0, attempt - 1), 20), PARTIAL_RETRY_MAX)


def _names(ids: Iterable[str], limit: int = 10) -> str:
    ids = list(ids)
    shown = ", ".join(ids[:limit])
    return shown + (f" and {len(ids) - limit} more" if len(ids) > limit else "")


class FailureLog:
    """Logs failures without flooding: a provider error is a warning with
    its sanitized message; an internal error logs its (redacted) traceback
    at ERROR the first time in a row for a key, then the type only, until
    ``recovered(key)``. One per Scheduler (or per ``run``), so its state
    never leaks between instances."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or log
        self._seen: set[str] = set()

    def report(self, key: str, what: str, e: BaseException) -> None:
        if is_store_closed(e):
            self._log.debug("%s: store closed, stopping", what)
        elif isinstance(e, ProviderError):
            self._log.warning("%s failed: %s", what, _message(e))
        elif key in self._seen:
            self._log.error("%s: internal error %s (repeat)", what, type(e).__name__)
        else:
            self._seen.add(key)
            self._log.error("%s: internal error %s", what, type(e).__name__, exc_info=e)

    def recovered(self, key: str) -> None:
        self._seen.discard(key)


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
    flog: FailureLog | None = None,
) -> Result:
    """Backfill one collector, skip it if done, or re-fetch it if it is
    partial and due. Never raises except on cancellation: any failure, the
    store's included, is logged sanitized, recorded when the store allows,
    and returned."""
    io = io or ThreadCalls()
    flog = flog or FailureLog()
    name = collector.name
    written = 0
    cursor: str | None = None
    progress = 0.0
    retry: dict | None = None  # the partial marker, when re-fetching a partial collector
    try:
        bf = await io(store.get_backfill, name)
        info = partial_info(bf.cursor)
        if bf.state == BackfillState.DONE:
            if info is None or "partial" not in info:
                return Result(name, bf.state, skipped=True)
            retry = info["partial"]
            due = (bf.updated_at or now()) + timedelta(seconds=partial_delay(retry["attempt"]))
            if now() < due:
                left = (due - now()).total_seconds()
                return Result(
                    name, bf.state, skipped=True, missing=tuple(retry["missing"]), retry_in=left
                )
            since = None
        else:
            cursor, progress = bf.cursor, bf.progress
            since = date.fromisoformat(cursor) if cursor and info is None else None
        today = now().date()
        start_progress = _progress(since, today) if since else 0.0
        if retry is None:
            await io(store.set_backfill, name, BackfillState.RUNNING, cursor, start_progress)
        what = f"since {since}" if since else "five years"
        log.info("backfill %s: fetching %s%s", name, what, " (partial retry)" if retry else "")
        points = await unless_stopped(collector.fetch(client, since), stop)
        if not points:
            if since is not None:
                raise ProviderError("empty", f"{name}: no points since {since}")
            if retry is None:
                raise ProviderError("empty", f"{name}: backfill returned no points")
        points = await io(_sorted, points)
        for i in range(0, len(points), CHUNK):
            if stop is not None and stop.is_set():
                return Result(name, BackfillState.RUNNING, written)
            chunk = points[i : i + CHUNK]
            await io(store.upsert_points, chunk)
            written += len(chunk)
            if retry is None:  # a partial re-fetch leaves the done state alone
                cursor = chunk[-1].period_end.date().isoformat()
                progress = max(start_progress, _progress(date.fromisoformat(cursor), today))
                await io(store.set_backfill, name, BackfillState.RUNNING, cursor, progress)
        # Judge only now, with the points safely stored.
        stored = await io(store.series_latest)
        ids = [s.id for s in collector.series]
        fetched = {p.series for p in points}
        missing = tuple(sorted(i for i in ids if i not in fetched and i not in stored))
        need = MIN_POINTS_PER_SERIES * len(ids)
        short = since is None and len(points) < need
        if missing or short:
            return await _record_partial(
                store, io, flog, collector, missing, len(points), need, retry, written
            )
        await io(store.set_backfill, name, BackfillState.DONE, cursor, 1.0)
        await io(store.record_success, name)
    except asyncio.CancelledError:
        raise
    except Stopped:
        return Result(name, BackfillState.RUNNING, written)  # resumes on the next start
    except Exception as e:  # noqa: BLE001 - logged by FailureLog, recorded, returned
        if is_store_closed(e):
            log.debug("backfill %s: store closed, stopping", name)
            return Result(name, BackfillState.RUNNING, written)
        msg = _message(e)
        flog.report(f"backfill:{name}", f"backfill {name}", e)
        if retry is not None:  # keep the history and the partial marker; try again later
            nxt = dict(retry, attempt=retry["attempt"] + 1)
            await _best_effort(
                io,
                name,
                "state",
                store.set_backfill,
                name,
                BackfillState.DONE,
                json.dumps({"partial": nxt}),
                bf.progress,
            )
            await _best_effort(io, name, "failure", store.record_failure, name, e)
            return Result(
                name,
                BackfillState.DONE,
                written,
                error=redact(msg),
                missing=tuple(nxt["missing"]),
                retry_in=partial_delay(nxt["attempt"]),
            )
        await _record_failed(store, io, name, e, cursor, progress)
        return Result(name, BackfillState.NOT_STARTED, written, error=redact(msg))
    flog.recovered(f"backfill:{name}")
    log.info("backfill %s: done, %d points", name, written)
    return Result(name, BackfillState.DONE, written)


async def _record_partial(store, io, flog, collector, missing, points, need, retry, written):
    name = collector.name
    attempt = retry["attempt"] + 1 if retry else 1
    marker = {
        "partial": {"attempt": attempt, "missing": list(missing), "points": points, "need": need}
    }
    present = len(collector.series) - len(missing)
    delay = partial_delay(attempt)
    parts = [f"{name}: partial backfill, history for {present} of {len(collector.series)} series"]
    if missing:
        parts.append(f"no data for {_names(missing)}")
    if points < need:
        parts.append(f"only {points} points (expected at least {need})")
    parts.append(
        f"full re-fetch in {delay / 3600:.0f} h; if a series is retired, run "
        f"'fuelwatch backfill --only {name} --accept-missing'"
    )
    err = ProviderError("partial", "; ".join(parts))
    flog.report(f"backfill:{name}", f"backfill {name}", err)
    await io(
        store.set_backfill,
        name,
        BackfillState.DONE,
        json.dumps(marker),
        present / max(1, len(collector.series)),
    )
    await io(store.record_failure, name, err)
    return Result(name, BackfillState.DONE, written, missing=missing, retry_in=delay)


async def _best_effort(io, name, what, fn, *args) -> None:
    try:
        await io(fn, *args)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 - logged; the retry loop carries on
        if not is_store_closed(e):
            log.error("backfill %s: could not record the %s: %s", name, what, type(e).__name__)


async def _record_failed(store, io, name, error, cursor, progress) -> None:
    """Best effort: the store may be what failed. The state goes first, so
    a failing record_failure cannot leave it at "running"."""
    await _best_effort(
        io, name, "state", store.set_backfill, name, BackfillState.NOT_STARTED, cursor, progress
    )
    await _best_effort(io, name, "failure", store.record_failure, name, error)


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
    flog: FailureLog | None = None,
    on_result: Callable[[Result], None] | None = None,
) -> dict[str, Result]:
    """Backfill every collector given (None: every polled one) that is not
    done. Failures are retried after `retry_start` doubling to `retry_max`;
    a collector fails for good after `max_attempts` (None: never, as in the
    app). A partial collector is re-fetched on its own schedule (up to 24 h
    apart) while `max_attempts` is None; with `max_attempts` set (the CLI),
    a partial result ends its run. Returns when nothing is left to do or
    `stop` is set. `on_result` is called after each attempt."""
    collectors = _select(collectors)
    io = io or ThreadCalls()
    flog = flog or FailureLog()
    own = client is None
    raw = make_client() if own else client
    http = paced(raw)
    loop = asyncio.get_running_loop()
    results: dict[str, Result] = {}
    attempts: dict[str, int] = {}
    pending = {c.name: c for c in collectors}
    due = dict.fromkeys(pending, loop.time())
    try:
        while pending and not (stop is not None and stop.is_set()):
            for name in [n for n in pending if due[n] <= loop.time()]:
                if stop is not None and stop.is_set():
                    break
                c = pending[name]
                try:
                    r = await backfill_collector(
                        store, http, c, now=now, stop=stop, io=io, flog=flog
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - backfill_collector should not raise
                    r = Result(name, BackfillState.NOT_STARTED, error=_message(e))
                results[name] = r
                if on_result is not None:
                    on_result(r)
                if r.partial:
                    if max_attempts is not None or r.retry_in is None:
                        del pending[name]
                    else:
                        due[name] = loop.time() + r.retry_in * random.uniform(1.0, 1.1)
                elif r.error is not None:
                    attempts[name] = attempts.get(name, 0) + 1
                    if max_attempts is not None and attempts[name] >= max_attempts:
                        del pending[name]
                    else:
                        delay = min(retry_start * 2 ** min(attempts[name] - 1, 20), retry_max)
                        due[name] = loop.time() + delay * random.uniform(0.9, 1.1)
                        log.info("backfill %s: retrying in %.0f s", name, due[name] - loop.time())
                elif r.state == BackfillState.DONE:
                    del pending[name]
                else:  # interrupted (stop, or the store closed): resumes on the next start
                    del pending[name]
            if pending:
                wait = max(0.0, min(due[n] for n in pending) - loop.time())
                if wait and await wait_or_stop(stop, wait):
                    break
    finally:
        if own:
            await raw.aclose()
    return results


async def accept_missing(store: Store, name: str) -> tuple[str, ...]:
    """Mark a partial collector done, keeping its missing series on record.
    Raises ValueError if it is not partial."""
    bf = await asyncio.to_thread(store.get_backfill, name)
    info = partial_info(bf.cursor)
    if bf.state != BackfillState.DONE or info is None or "partial" not in info:
        raise ValueError(f"{name} is not a partial backfill (state {bf.state.value})")
    missing = tuple(info["partial"]["missing"])
    marker = json.dumps({"accepted_missing": list(missing)})
    await asyncio.to_thread(store.set_backfill, name, BackfillState.DONE, marker, 1.0)
    log.warning("backfill %s: accepted with no data for %s", name, _names(missing) or "none")
    return missing


@dataclass(frozen=True)
class Summary:
    """Overall backfill state for /api/status."""

    state: str  # "not_started", "backfilling", "partial" or "done"
    progress: float  # mean over the collectors, 0 to 1
    collectors: dict[str, str]  # not_started, running, partial or done
    errors: dict[str, str]  # collector -> sanitized reason its last attempt failed
    missing: dict[str, tuple[str, ...]]  # partial collector -> series with no data
    accepted_missing: dict[str, tuple[str, ...]]  # done, with these series accepted as absent


def summary(store: Store, collectors: Iterable[Collector] | None = None) -> Summary:
    """Synchronous; call it through asyncio.to_thread."""
    names = [c.name for c in _select(collectors)]
    statuses = {n: store.get_status(n) for n in names}
    states = {n: store.get_backfill(n) for n in names}
    view: dict[str, str] = {}
    missing: dict[str, tuple[str, ...]] = {}
    accepted: dict[str, tuple[str, ...]] = {}
    progress = 0.0
    for n, b in states.items():
        info = partial_info(b.cursor) if b.state == BackfillState.DONE else None
        if info and "partial" in info:
            view[n] = "partial"
            missing[n] = tuple(info["partial"]["missing"])
            progress += b.progress
            continue
        if info and "accepted_missing" in info:
            accepted[n] = tuple(info["accepted_missing"])
        view[n] = b.state.value
        progress += 1.0 if b.state == BackfillState.DONE else b.progress
    progress /= max(1, len(states))
    errors = {
        n: st.error_message or st.error_class
        for n, st in statuses.items()
        if st is not None and st.error_class and view[n] != "done"
    }
    values = set(view.values())
    if values <= {"done"}:
        overall = "done"
    elif values == {"not_started"} and not errors:
        overall = "not_started"
    elif values & {"not_started", "running"}:
        overall = "backfilling"
    else:
        overall = "partial"
    return Summary(overall, round(progress, 4), view, errors, missing, accepted)


async def reset(store: Store, names: Iterable[str]) -> None:
    """Mark collectors not started, so the next run backfills them again."""
    for n in names:
        await asyncio.to_thread(store.set_backfill, n, BackfillState.NOT_STARTED, None, 0.0)
