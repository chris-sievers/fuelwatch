"""AISStream live collector: one long-lived websocket for the Strait of Hormuz.

AIS is a stream, not a polled fetch, so this collector has its own shape
next to the polled `fetch()` collectors:

    SERIES: list[SeriesSpec]
    class AisStreamCollector:
        @classmethod from_env() -> AisStreamCollector | None   # None: live AIS off
        async def run(sink, stop=None) -> None   # until `stop` is set
        def snapshot() -> tuple[ShipPosition, ...]   # read-only, for the map
        status -> AisStatus

`sink` is an async callable that receives the four points for each
completed 10-minute slot. Slots the stream did not fully cover produce no
call at all, never zeros. Parsing is a pure function, `parse_frame`, so unit
tests run against the hand-built fixtures in tests/fixtures/aisstream/.

Docs: https://aisstream.io/documentation and the message schema at
https://github.com/aisstream/ais-message-models/blob/master/type-definition.yaml
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from fuelwatch.collectors.ais_tracker import (
    HORMUZ_BBOX,
    SERIES_CROSSINGS,
    SERIES_SHIPS,
    SERIES_TANKER_CROSSINGS,
    SERIES_TANKERS,
    SLOT,
    ShipPosition,
    Tracker,
    slot_start,
)
from fuelwatch.http import ProviderError, redact
from fuelwatch.models import Frequency, Point, SeriesSpec

log = logging.getLogger(__name__)

URL = "wss://stream.aisstream.io/v0/stream"
ENV_KEY = "AISSTREAM_API_KEY"
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]
MAX_FRAME_BYTES = 16 * 1024  # real messages are about 1 KB
SILENCE_TIMEOUT = 120.0  # Hormuz is never quiet for two minutes; treat it as a dead socket
BACKOFF_START = 1.0
BACKOFF_MAX = 300.0
STABLE_AFTER = 60.0  # a connection that lasted this long resets the backoff

_GRACE = timedelta(hours=1)
_SCHEDULE = "every 10 minutes while connected"
SERIES = [
    SeriesSpec(SERIES_SHIPS, "Ships broadcasting AIS in the strait", "ships", "AISStream",
               Frequency.TEN_MINUTES, _SCHEDULE, _GRACE),
    SeriesSpec(SERIES_TANKERS, "Tankers broadcasting AIS in the strait", "ships", "AISStream",
               Frequency.TEN_MINUTES, _SCHEDULE, _GRACE),
    SeriesSpec(SERIES_CROSSINGS, "Ships crossing the strait (live AIS)", "ships", "AISStream",
               Frequency.TEN_MINUTES, _SCHEDULE, _GRACE),
    SeriesSpec(SERIES_TANKER_CROSSINGS, "Tankers crossing the strait (live AIS)", "ships",
               "AISStream", Frequency.TEN_MINUTES, _SCHEDULE, _GRACE),
]


# --- pure parsing ----------------------------------------------------------------------


@dataclass(frozen=True)
class PositionMsg:
    mmsi: object
    lat: object
    lon: object
    sog: object
    cog: object
    heading: object


@dataclass(frozen=True)
class StaticMsg:
    mmsi: object
    ship_type: object
    name: object


@dataclass(frozen=True)
class Confirmation:
    pass


@dataclass(frozen=True)
class ErrorMsg:
    text: str


Parsed = PositionMsg | StaticMsg | Confirmation | ErrorMsg


def subscription(api_key: str, bbox=HORMUZ_BBOX) -> str:
    return json.dumps(
        {
            "APIKey": api_key,
            "BoundingBoxes": [bbox.to_aisstream()],
            "FilterMessageTypes": MESSAGE_TYPES,
        }
    )


def parse_frame(raw: bytes | str) -> Parsed | None:
    """Decode one websocket frame. Returns None for anything that is not a
    well-formed message we use. Field values are passed through unvalidated;
    the tracker validates them."""
    if len(raw) > MAX_FRAME_BYTES:
        return None
    try:
        doc = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict):
        return None
    if "error" in doc:  # AISStream reports a bad key or subscription this way
        return ErrorMsg(str(doc["error"])[:200])
    mtype = doc.get("MessageType")
    message = doc.get("Message")
    if mtype == "SubscriptionConfirmation":
        return Confirmation()
    if mtype not in MESSAGE_TYPES or not isinstance(message, dict):
        return None
    body = message.get(mtype)
    if not isinstance(body, dict) or body.get("Valid") is False:
        return None
    if mtype == "PositionReport":
        return PositionMsg(
            mmsi=body.get("UserID"),
            lat=body.get("Latitude"),
            lon=body.get("Longitude"),
            sog=body.get("Sog"),
            cog=body.get("Cog"),
            heading=body.get("TrueHeading"),
        )
    return StaticMsg(mmsi=body.get("UserID"), ship_type=body.get("Type"), name=body.get("Name"))


def apply(tracker: Tracker, msg: Parsed | None, now: datetime) -> bool:
    if isinstance(msg, PositionMsg):
        return tracker.position(msg.mmsi, msg.lat, msg.lon, now, msg.sog, msg.cog, msg.heading)
    if isinstance(msg, StaticMsg):
        return tracker.static(msg.mmsi, msg.ship_type, msg.name, now)
    return False


# --- the websocket client --------------------------------------------------------------


@dataclass
class AisStatus:
    connected: bool = False
    error_class: str | None = None
    message: str | None = None
    reconnects: int = 0


def _utcnow() -> datetime:
    return datetime.now(UTC)


Sink = Callable[[list[Point]], Awaitable[None]]


class AisStreamCollector:
    def __init__(
        self,
        api_key: str,
        *,
        tracker: Tracker | None = None,
        url: str = URL,
        connect=ws_connect,
        clock: Callable[[], datetime] = _utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        redact.add(api_key)
        self._key = api_key
        self.tracker = tracker if tracker is not None else Tracker()
        self._url = url
        self._connect = connect
        self._clock = clock
        self._sleep = sleep
        self.status = AisStatus()

    @classmethod
    def from_env(cls, **kw) -> AisStreamCollector | None:
        """None when AISSTREAM_API_KEY is unset: live AIS is off (as on beta)."""
        key = os.environ.get(ENV_KEY, "").strip()
        return cls(key, **kw) if key else None

    def snapshot(self) -> tuple[ShipPosition, ...]:
        return self.tracker.snapshot(self._clock())

    async def run(self, sink: Sink, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        tasks = [
            asyncio.create_task(self._tick(sink, stop)),
            asyncio.create_task(self._stream(stop)),
            asyncio.create_task(stop.wait()),
        ]
        try:
            # The stream loop only returns once stop is set; a quiet socket can
            # sit in recv(), so stop.wait() ends the run promptly instead.
            await asyncio.wait(tasks[1:], return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.tracker.disconnected(self._clock())
            self.status.connected = False

    async def _tick(self, sink: Sink, stop: asyncio.Event) -> None:
        while not stop.is_set():
            now = self._clock()
            boundary = slot_start(now) + SLOT
            await self._sleep(max(0.0, (boundary - now).total_seconds()))
            points = self.tracker.close_slot(boundary)
            if points:
                try:
                    await sink(points)
                except Exception:
                    log.exception("AIS sink failed for slot %s", boundary.isoformat())

    async def _stream(self, stop: asyncio.Event) -> None:
        delay = BACKOFF_START
        while not stop.is_set():
            started = self._clock()
            try:
                await self._session(stop)
            except ProviderError as e:
                self._fail(e.error_class, str(e))
            except InvalidStatus as e:
                self._fail("http_status", f"handshake rejected: {e.response.status_code}")
            except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError) as e:
                self._fail("transport", f"{type(e).__name__}: {e}")
            if stop.is_set():
                break
            if (self._clock() - started).total_seconds() >= STABLE_AFTER:
                delay = BACKOFF_START
            self.status.reconnects += 1
            await self._sleep(delay * random.uniform(0.8, 1.2))
            delay = min(delay * 2, BACKOFF_MAX)

    def _fail(self, error_class: str, message: str) -> None:
        self.tracker.disconnected(self._clock())
        self.status.connected = False
        self.status.error_class = error_class
        self.status.message = redact(message)[:300]
        log.warning("AISStream %s: %s", error_class, self.status.message)

    async def _session(self, stop: asyncio.Event) -> None:
        async with self._connect(
            self._url, compression="deflate", max_size=2**20, open_timeout=10
        ) as ws:
            # AISStream closes the socket if the subscription is not sent within 3 s.
            await ws.send(subscription(self._key, self.tracker.bbox))
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=SILENCE_TIMEOUT)
                now = self._clock()
                msg = parse_frame(raw)
                if isinstance(msg, ErrorMsg):
                    raise ProviderError("provider_error", msg.text)
                if msg is None:
                    continue
                if not self.tracker.live:
                    self.tracker.connected(now)
                    self.status.connected = True
                    self.status.error_class = self.status.message = None
                apply(self.tracker, msg, now)
            await ws.close()
