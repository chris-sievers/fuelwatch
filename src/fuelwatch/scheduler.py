"""The collection scheduler: polls each polled collector near its
publication times, runs the AIS stream when it has a key, and starts the
background backfill.

Entry point for the app::

    stop = asyncio.Event()
    sched = Scheduler(store)          # sched.ais: the AIS collector or None
    task = asyncio.create_task(sched.run(stop))
    ...
    stop.set(); await task            # then store.close() is safe

or ``await scheduler.run(store, stop)``.

Polling cadence, per collector:

- Idle: every IDLE_INTERVAL (6 h), so revisions and off-schedule releases
  land within a few hours at four polls a day.
- At each expected release time (from the registry), plus a few minutes.
- Hot: while a release is due but its period is not stored yet, every
  HOT_INTERVAL (1 h), for up to HOT_WINDOW (3 days) after the release time.
  A late or holiday-slipped release is picked up within the hour.
- After a failure: RETRY_BASE (10 min) doubling to RETRY_MAX (6 h), with
  jitter; a success resets it.

That is at most about 4 requests per EIA poll, one PortWatch page, and one
small EU file, so even the hot rate is a few dozen requests a day per
provider. A collector does not poll until its backfill is done, so a
first start fetches each provider once, not twice.

Each poll re-fetches from ``revision_window`` before the newest stored
period: 14 days for EIA, PortWatch's 35-day REVISION_WINDOW (and a full
five-year refresh every 28 days), and the newest stored Monday for the EU
bulletin (which keeps it on the small latest-prices file).

Collectors run as separate tasks: one raising never stops the others, and
every loop survives any exception (a failing store included) by logging it
sanitized and backing off. Each run records success or failure in the
status table; the store keeps only a ProviderError's class and sanitized
message, or 'internal' and the exception type for anything else.

A collector polls once its backfill is done, or once its backfill attempt
has failed (the backfill keeps retrying in the background), or if the
backfill task has ended.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

from fuelwatch import backfill
from fuelwatch.collectors.aisstream import AisStreamCollector
from fuelwatch.http import PacedClient, ProviderError, Stopped, make_client, redact
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Collector, Registry, next_release, pending_release
from fuelwatch.store import BackfillState, Store
from fuelwatch.threads import ThreadCalls, is_store_closed

log = logging.getLogger(__name__)

IDLE_INTERVAL = timedelta(hours=6)
HOT_INTERVAL = timedelta(hours=1)
HOT_WINDOW = timedelta(days=3)
RELEASE_LAG = timedelta(minutes=5)  # poll this long after a scheduled release time
MIN_DELAY = timedelta(minutes=1)
RETRY_BASE = timedelta(minutes=10)
RETRY_MAX = timedelta(hours=6)
BACKFILL_CHECK = 30.0  # seconds between checks while waiting for a backfill
AIS_STATUS_CHECK = 60.0
AIS_RESTART_START = 1.0
AIS_RESTART_MAX = 300.0
SHUTDOWN_GRACE = 5.0  # seconds for loops to finish before they are cancelled
DRAIN_TIMEOUT = 10.0  # seconds to wait for store writes still in worker threads

AIS_NAME = "aisstream"


class _Auto:
    pass


AUTO = _Auto()


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class PollResult:
    collector: str
    since: date | None
    rows: int = 0
    error: str | None = None  # sanitized, as stored in status


def backoff(failures: int, base: timedelta = RETRY_BASE, cap: timedelta = RETRY_MAX) -> timedelta:
    """Delay after `failures` consecutive failures (1 or more). The exponent
    is capped so a long outage cannot overflow timedelta."""
    return min(base * 2 ** min(max(0, failures - 1), 20), cap)


def _message(e: BaseException) -> str:
    return redact(str(e)) if isinstance(e, ProviderError) else type(e).__name__


def _log_failure(what: str, e: BaseException) -> None:
    if is_store_closed(e):
        log.debug("%s: store closed, stopping", what)
    elif isinstance(e, ProviderError):
        log.warning("%s failed: %s", what, _message(e))
    else:
        log.error("%s: internal error %s", what, type(e).__name__)
        log.debug("%s traceback", what, exc_info=e)  # redacted by RedactingFilter


class Scheduler:
    def __init__(
        self,
        store: Store,
        *,
        client: httpx.AsyncClient | PacedClient | None = None,
        registry: Registry = REGISTRY,
        collectors: Iterable[Collector] | None = None,
        ais: AisStreamCollector | None | _Auto = AUTO,
        run_backfill: bool = True,
        now: Callable[[], datetime] = _utcnow,
        monotonic: Callable[[], float] = time.monotonic,
        idle_interval: timedelta = IDLE_INTERVAL,
        hot_interval: timedelta = HOT_INTERVAL,
        retry_base: timedelta = RETRY_BASE,
        retry_max: timedelta = RETRY_MAX,
        min_delay: timedelta = MIN_DELAY,
        backfill_check: float = BACKFILL_CHECK,
        jitter: float = 0.1,
    ) -> None:
        self.store = store
        self.registry = registry
        # None: every polled collector. An empty list means none.
        chosen = registry.polled if collectors is None else collectors
        self.collectors = tuple(c for c in chosen if c.polled)
        self.ais = AisStreamCollector.from_env() if isinstance(ais, _Auto) else ais
        self.run_backfill = run_backfill
        self.now = now
        self.monotonic = monotonic
        self.idle_interval = idle_interval
        self.hot_interval = hot_interval
        self.retry_base = retry_base
        self.retry_max = retry_max
        self.min_delay = min_delay
        self.backfill_check = backfill_check
        self.jitter = jitter
        self._own_client = client is None
        self._raw_client = client
        self.http: PacedClient | None = backfill.paced(client) if client is not None else None
        self.failures: dict[str, int] = {c.name: 0 for c in self.collectors}
        self.io = ThreadCalls()  # every store call; drained at shutdown
        self._backfill_task: asyncio.Task | None = None
        self._backfill_failed: set[str] = set()

    # --- one poll ------------------------------------------------------------------

    def _http(self) -> PacedClient:
        if self.http is None:
            self._raw_client = make_client()
            self.http = backfill.paced(self._raw_client)
        return self.http

    async def _latest(self, c: Collector) -> dict[str, datetime | None]:
        latest = await self.io(self.store.series_latest)
        return {s.id: (latest[s.id].period_end if s.id in latest else None) for s in c.series}

    def since_for(self, c: Collector, latest: dict[str, datetime | None]) -> date:
        today = self.now().date()
        newest = max((t for t in latest.values() if t is not None), default=None)
        anchor = min(today, newest.date()) if newest else today
        return anchor - c.revision_window

    async def poll(self, c: Collector) -> PollResult:
        """Run one incremental fetch and write it. Records the outcome in
        status when the store allows; never raises except on cancellation
        or a closed store."""
        since: date | None = None
        try:
            since = self.since_for(c, await self._latest(c))
            full = False
            if c.full_refresh_every is not None:
                bf = await self.io(self.store.get_backfill, c.name)
                full = (
                    bf.state == BackfillState.DONE
                    and bf.updated_at is not None
                    and self.now() - bf.updated_at >= c.full_refresh_every
                )
            if full:
                since = None
                log.info("%s: periodic full refresh", c.name)
            http = self._http()
            points: list[Point] = await backfill.unless_stopped(c.fetch(http, since), http.stop)
            n = await self.io(self.store.upsert_points, points)
            if full:
                await self.io(self.store.set_backfill, c.name, BackfillState.DONE, None, 1.0)
            await self.io(self.store.record_success, c.name)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # one collector must not stop the others
            if is_store_closed(e):
                raise  # shutting down: the loop ends quietly
            self.failures[c.name] = self.failures.get(c.name, 0) + 1
            if not isinstance(e, Stopped):
                _log_failure(f"{c.name} poll", e)
                await self._record_failure(c.name, e)
            return PollResult(c.name, since, error=_message(e))
        self.failures[c.name] = 0
        log.info("%s: %d points since %s", c.name, n, since)
        return PollResult(c.name, since, n)

    async def _record_failure(self, name: str, e: BaseException) -> None:
        """Best effort: the store may be what failed."""
        if is_store_closed(e):
            return
        try:
            await self.io(self.store.record_failure, name, e)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - logged; the caller backs off
            if is_store_closed(err):
                return
            log.error("%s: could not record the failure: %s", name, type(err).__name__)

    async def poll_all(self) -> list[PollResult]:
        """Poll every collector once, concurrently (``collect --once``)."""
        try:
            return list(await asyncio.gather(*(self.poll(c) for c in self.collectors)))
        finally:
            await self.aclose()

    # --- cadence -------------------------------------------------------------------

    def _jitter(self, d: timedelta) -> timedelta:
        return d * random.uniform(1 - self.jitter, 1 + self.jitter) if self.jitter else d

    def _backoff(self, failures: int) -> timedelta:
        return self._jitter(backoff(failures, self.retry_base, self.retry_max))

    async def next_delay(self, c: Collector) -> timedelta:
        failures = self.failures.get(c.name, 0)
        if failures:
            return self._backoff(failures)
        return self.cadence(c, await self._latest(c))

    def cadence(self, c: Collector, latest: dict[str, datetime | None]) -> timedelta:
        now = self.now()
        delay = self.idle_interval
        upcoming = []
        for s in c.series:
            sched = self.registry.get(s.id).schedule
            r = pending_release(sched, latest.get(s.id), now)
            if r is not None and now - r.at < HOT_WINDOW:
                delay = min(delay, self.hot_interval)
            nxt = next_release(sched, now)
            if nxt is not None:
                upcoming.append(nxt + RELEASE_LAG)
        if upcoming:
            delay = min(delay, min(upcoming) - now)
        return max(delay, self.min_delay)

    async def first_delay(self, c: Collector) -> timedelta:
        """After a restart, wait out the rest of the interval since the last
        run instead of polling at once, so a crash loop cannot hammer a
        provider."""
        status = await self.io(self.store.get_status, c.name)
        delay = await self.next_delay(c)
        if status is None or status.last_run_at is None:
            return timedelta(0)
        if status.error_class is not None:
            return min(delay, backoff(1, self.retry_base, self.retry_max))
        due = status.last_run_at + delay
        return max(timedelta(0), due - self.now())

    # --- loops ---------------------------------------------------------------------

    def _backfill_result(self, r: backfill.Result) -> None:
        if r.error is not None:
            self._backfill_failed.add(r.collector)

    async def _wait_backfill(self, c: Collector, stop: asyncio.Event) -> bool:
        """True once `c` may poll: its backfill is done or has failed, or the
        backfill task is not running. False if `stop` was set."""
        while not stop.is_set():
            if c.name in self._backfill_failed:
                return True
            task = self._backfill_task
            if task is None or task.done():
                return True
            try:
                bf = await self.io(self.store.get_backfill, c.name)
                if bf.state == BackfillState.DONE:
                    return True
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - keep waiting; the store may recover
                _log_failure(f"{c.name} backfill check", e)
            if await backfill.wait_or_stop(stop, self.backfill_check):
                return False
        return False

    async def _collector_loop(self, c: Collector, stop: asyncio.Event) -> None:
        if self.run_backfill and not await self._wait_backfill(c, stop):
            return
        guard_failures = 0
        try:
            delay = await self.first_delay(c)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - poll after the first backoff step
            _log_failure(f"{c.name} first delay", e)
            delay = self._backoff(1)
        while not stop.is_set():
            if await backfill.wait_or_stop(stop, delay.total_seconds()):
                return
            try:
                await self.poll(c)
                delay = await self.next_delay(c)
                guard_failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - a loop must never die
                if is_store_closed(e):
                    log.debug("%s: store closed, loop stopping", c.name)
                    return
                guard_failures += 1
                _log_failure(f"{c.name} loop", e)
                delay = self.retry_max  # cannot raise, unlike anything computed
                with contextlib.suppress(Exception):
                    delay = self._backoff(max(guard_failures, self.failures.get(c.name, 0)))
            log.debug("%s: next poll in %s", c.name, delay)

    async def _ais_loop(self, stop: asyncio.Event) -> None:
        ais = self.ais
        assert ais is not None

        async def sink(points: list[Point]) -> None:
            try:
                await self.io(self.store.upsert_points, points)
                await self.io(self.store.record_success, AIS_NAME)
            except Exception as e:
                await self._record_failure(AIS_NAME, e)
                raise

        async def mirror_status() -> None:
            # The stream reports connection errors on ais.status; copy each
            # new one into the status table.
            last = None
            while not await backfill.wait_or_stop(stop, AIS_STATUS_CHECK):
                st = ais.status
                cur = (st.error_class, st.message) if st.error_class else None
                if cur is not None and cur != last:
                    await self._record_failure(
                        AIS_NAME, ProviderError(st.error_class, st.message or "")
                    )
                last = cur

        watcher = asyncio.create_task(mirror_status())
        delay = AIS_RESTART_START
        try:
            while not stop.is_set():
                started = self.monotonic()
                try:
                    await ais.run(sink, stop)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - keep the rest of the app alive
                    _log_failure("AIS stream", e)
                    await self._record_failure(AIS_NAME, e)
                    if self.monotonic() - started >= AIS_RESTART_MAX:
                        delay = AIS_RESTART_START  # it ran healthily; start the backoff over
                    if await backfill.wait_or_stop(stop, delay):
                        break
                    delay = min(delay * 2, AIS_RESTART_MAX)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    async def run(self, stop: asyncio.Event) -> None:
        """Run until `stop` is set, then shut down: loops finish at their
        next wait (provider waits end at once), anything still busy after
        SHUTDOWN_GRACE is cancelled, store calls already running in worker
        threads are waited for (up to DRAIN_TIMEOUT), and a client this
        scheduler created is closed. The store can be closed after this."""
        http = self._http()
        http.stop = stop
        tasks: list[asyncio.Task] = []

        def watch(task: asyncio.Task) -> asyncio.Task:
            def done(t: asyncio.Task) -> None:
                if t.cancelled():
                    return
                if t.exception() is not None:
                    log.error("task %s died: %s", t.get_name(), type(t.exception()).__name__)
                elif not stop.is_set():
                    log.warning("task %s ended before stop", t.get_name())

            task.add_done_callback(done)
            tasks.append(task)
            return task

        if self.run_backfill:
            self._backfill_task = watch(
                asyncio.create_task(
                    backfill.run(
                        self.store,
                        http,
                        collectors=self.collectors,
                        stop=stop,
                        io=self.io,
                        on_result=self._backfill_result,
                    ),
                    name="backfill",
                )
            )
        for c in self.collectors:
            watch(asyncio.create_task(self._collector_loop(c, stop), name=f"poll:{c.name}"))
        if self.ais is not None:
            watch(asyncio.create_task(self._ais_loop(stop), name="ais"))
        else:
            log.info("live AIS off: %s not set", "AISSTREAM_API_KEY")
        try:
            await stop.wait()
            if tasks:
                _, still = await asyncio.wait(tasks, timeout=SHUTDOWN_GRACE)
                for t in still:
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not await self.io.drain(DRAIN_TIMEOUT):
                log.warning("store calls still running after %.0f s", DRAIN_TIMEOUT)
            await self.aclose()

    async def aclose(self) -> None:
        if self._own_client and self._raw_client is not None:
            await self._raw_client.aclose()
            self._raw_client = None
            self.http = None


async def run(
    store: Store,
    stop: asyncio.Event,
    *,
    client: httpx.AsyncClient | None = None,
    now: Callable[[], datetime] = _utcnow,
    **kwargs,
) -> None:
    """Collect until `stop` is set. See Scheduler for the keyword options."""
    await Scheduler(store, client=client, now=now, **kwargs).run(stop)
