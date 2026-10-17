"""The HTTP app: a read-only JSON API over the store, plus /metrics on its
own port. Run it with ``python -m fuelwatch serve`` (one Uvicorn worker:
the collectors live in this process and AISStream allows one websocket).

Lifespan
    Start: open the Store, load the event markers, start the metrics
    server, start the Scheduler as a background task. Shutdown: set
    ``stop``, await the scheduler (it drains its store calls), stop the
    metrics server, then close the store, in that order.

Routes (all GET)
    /api/summary         calc.summary: tiles, sentence, calculated numbers
    /api/series          every registered series: latest and previous value,
                         freshness, source and attribution
    /api/data/{series}   chart data; ?start=&end= ISO dates, at most five
                         years; 10-minute AIS series roll up to days for
                         ranges over 7 days
    /api/events          event markers from src/fuelwatch/events.json
    /api/status          one row per source; backfill from backfill.summary()
    /api/version         FW_COMMIT and FW_BUILD_TIME
    /healthz             "ok"
    /, /about            placeholders until the dashboard (milestone 3)

Caching (in process)
    /api/series, /api/data and /api/status: 60 s. /api/summary: until the scheduler's
    collection count changes, and at most an hour. /api/events: loaded once.

Every response, errors included, carries the spec's Content-Security-Policy,
X-Content-Type-Options and Referrer-Policy (an ASGI wrapper outside
Starlette's error middleware, so even a 500 has them). A closed store is a
503. Error bodies never include exception text.

Settings (environment)
    FW_API_SPOT_DATA   0 (default) or 1: serve the four EIA spot series
                       (Refinitiv data) through /api/data
    FW_METRICS_HOST    default 127.0.0.1
    FW_METRICS_PORT    default 9201
    FW_COMMIT, FW_BUILD_TIME   shown by /api/version, default "unknown"
    FW_HOST, FW_PORT   used by ``serve``: default 127.0.0.1 and 8080
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException

from fuelwatch.api import events as events_mod
from fuelwatch.api import metrics as metrics_mod
from fuelwatch.api import views
from fuelwatch.api.cache import SERIES_TTL, SUMMARY_MAX_AGE, Cache
from fuelwatch.registry import REGISTRY, Registry
from fuelwatch.store import Store, StoreClosedError

log = logging.getLogger(__name__)

CSP = (
    "default-src 'self'; img-src 'self' data:; object-src 'none'; "
    "base-uri 'none'; frame-ancestors 'none'"
)
SECURITY_HEADERS = [
    (b"content-security-policy", CSP.encode()),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
]
_HEADER_NAMES = {name for name, _ in SECURITY_HEADERS}

DEFAULT_METRICS_HOST = "127.0.0.1"
DEFAULT_METRICS_PORT = 9201
_VERSION_RE = re.compile(r"^[A-Za-z0-9._:+-]{1,64}$")


class _Auto:
    pass


AUTO = _Auto()


def _utcnow() -> datetime:
    return datetime.now(UTC)


# --- settings ---------------------------------------------------------------------


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip()
    if raw == "":
        return default
    if raw not in ("0", "1"):
        raise ValueError(f"{name} must be 0 or 1, got {raw!r}")
    return raw == "1"


def env_port(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if raw == "":
        return default
    if not raw.isdigit() or not 0 <= int(raw) <= 65535:
        raise ValueError(f"{name} must be a port number, got {raw!r}")
    return int(raw)


def _version_value(name: str) -> str:
    raw = os.environ.get(name, "").strip()
    return raw if _VERSION_RE.match(raw) else "unknown"


# --- security headers -------------------------------------------------------------


class SecurityHeaders:
    """Pure ASGI wrapper: sets the security headers on every HTTP response,
    replacing any the app set. It sits outside Starlette's
    ServerErrorMiddleware, so a 500 gets them too."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                headers = [
                    (k, v) for k, v in message.get("headers", []) if k.lower() not in _HEADER_NAMES
                ]
                message = {**message, "headers": headers + SECURITY_HEADERS}
            await send(message)

        return await self.app(scope, receive, send_with_headers)


class _App(FastAPI):
    def build_middleware_stack(self):
        return SecurityHeaders(super().build_middleware_stack())


# --- placeholders -----------------------------------------------------------------

_PAGE = """<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title></head>
<body>
<h1>{title}</h1>
<p>{text}</p>
<p>The JSON API is live: <a href="/api/summary">summary</a>, <a href="/api/series">series</a>,
<a href="/api/status">status</a>.</p>
</body>
</html>
"""


# --- the app ----------------------------------------------------------------------


def create_app(
    *,
    store_factory: Callable[[], Store] = Store,
    scheduler_factory: Callable | None | _Auto = AUTO,
    registry: Registry = REGISTRY,
    clock: Callable[[], datetime] = _utcnow,
    spot_data: bool | None = None,
    metrics_host: str | None = None,
    metrics_port: int | None | _Auto = AUTO,
    events_path: Path = events_mod.PATH,
) -> FastAPI:
    """Build the app. Defaults come from the environment; tests pass
    ``scheduler_factory=None`` and ``metrics_port=None`` to run without the
    collectors or the metrics server."""
    if isinstance(scheduler_factory, _Auto):
        from fuelwatch.scheduler import Scheduler

        scheduler_factory = Scheduler
    if spot_data is None:
        spot_data = env_flag("FW_API_SPOT_DATA")
    if isinstance(metrics_port, _Auto):
        metrics_port = env_port("FW_METRICS_PORT", DEFAULT_METRICS_PORT)
    if metrics_host is None:
        metrics_host = os.environ.get("FW_METRICS_HOST", "").strip() or DEFAULT_METRICS_HOST
    version = {"commit": _version_value("FW_COMMIT"), "build_time": _version_value("FW_BUILD_TIME")}
    event_list = events_mod.load(events_path)  # a bad file stops the app here

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state = app.state
        state.store = store_factory()
        state.stop = asyncio.Event()
        state.scheduler = None
        task = None
        server = None
        try:
            if metrics_port is not None:
                server = metrics_mod.serve(state.metrics_registry, metrics_host, metrics_port)
                state.metrics_server = server
                log.info("metrics on %s:%d", metrics_host, server.server_port)
            if scheduler_factory is not None:
                state.scheduler = scheduler_factory(state.store)
                task = asyncio.create_task(state.scheduler.run(state.stop), name="scheduler")
            yield
        finally:
            state.stop.set()
            if task is not None:
                try:
                    await task
                except Exception as e:  # noqa: BLE001 - shut down the rest regardless
                    log.error("scheduler ended with %s", type(e).__name__)
            if server is not None:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
            state.store.close()
            log.info("shutdown complete")

    app = _App(
        title="Fuelwatch",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.store = None
    app.state.scheduler = None
    app.state.cache = Cache(lambda: clock().timestamp())
    app.state.metrics_registry = metrics_mod.make_registry(
        metrics_mod.AppMetrics(
            store=lambda: app.state.store,
            registry=registry,
            ais=lambda: getattr(app.state.scheduler, "ais", None),
            now=clock,
        )
    )
    cache: Cache = app.state.cache

    def store_of() -> Store:
        store = app.state.store
        if store is None:
            raise StoreClosedError()
        return store

    def ais():
        return getattr(app.state.scheduler, "ais", None)

    def collections() -> int:
        return getattr(app.state.scheduler, "collections", 0)

    # errors

    @app.exception_handler(StoreClosedError)
    async def store_closed(request: Request, exc: StoreClosedError):
        return JSONResponse({"detail": "the data store is not available"}, status_code=503)

    @app.exception_handler(RequestValidationError)
    async def bad_request(request: Request, exc: RequestValidationError):
        # The location and reason only; never the input that was sent.
        problems = [
            {"loc": [str(x) for x in err.get("loc", ())], "msg": str(err.get("msg", "invalid"))}
            for err in exc.errors()
        ]
        return JSONResponse({"detail": problems}, status_code=400)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    @app.exception_handler(Exception)
    async def internal_error(request: Request, exc: Exception):
        log.error("%s %s: internal error %s", request.method, request.url.path, type(exc).__name__)
        return JSONResponse({"detail": "internal error"}, status_code=500)

    # routes

    @app.get("/healthz", response_class=PlainTextResponse)
    async def healthz():
        return "ok"

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return _PAGE.format(title="Fuelwatch", text="The dashboard arrives in milestone 3.")

    @app.get("/about", response_class=HTMLResponse)
    async def about():
        return _PAGE.format(
            title="About Fuelwatch",
            text="The glossary and source notes arrive with the dashboard in milestone 3.",
        )

    @app.get("/api/version")
    async def api_version():
        return version

    @app.get("/api/events")
    async def api_events():
        return views.events(event_list)

    @app.get("/api/summary")
    async def api_summary():
        tag = collections()
        hit = cache.get(("summary",), SUMMARY_MAX_AGE, tag)
        if hit is None:
            hit = await asyncio.to_thread(views.summary, store_of(), clock())
            cache.put(("summary",), hit, tag)
        return hit

    @app.get("/api/series")
    async def api_series():
        hit = cache.get(("series",), SERIES_TTL)
        if hit is None:
            hit = await asyncio.to_thread(
                views.series, store_of(), registry, clock(), spot_data=spot_data
            )
            cache.put(("series",), hit)
        return hit

    @app.get("/api/data/{series_id}")
    async def api_data(
        series_id: str,
        start: str | None = Query(None, max_length=10),
        end: str | None = Query(None, max_length=10),
    ):
        try:
            entry = registry.get(series_id)
        except KeyError:
            raise HTTPException(404, "unknown series") from None
        if not spot_data and entry.id in views.SPOT_SERIES:
            raise HTTPException(
                403,
                "this series is third-party (Refinitiv) data and is not served by the JSON API; "
                "its latest value is in /api/series",
            )
        now = clock()
        try:
            lo, hi = views.data_range(start, end, now.date())
        except views.BadRange as e:
            raise HTTPException(400, str(e)) from None
        key = ("data", entry.id, lo, hi)
        hit = cache.get(key, SERIES_TTL)
        if hit is None:
            hit = await asyncio.to_thread(views.data, store_of(), entry, lo, hi, now)
            cache.put(key, hit)
        return hit

    @app.get("/api/status")
    async def api_status():
        hit = cache.get(("status",), SERIES_TTL)
        if hit is None:
            hit = await asyncio.to_thread(views.status, store_of(), registry, clock(), ais())
            cache.put(("status",), hit)
        return hit

    return app
