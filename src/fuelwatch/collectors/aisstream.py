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
call at all, never zeros.

Liveness comes from the websocket protocol, not from traffic. Hormuz is
often quiet for many minutes, so silence is normal and never triggers a
reconnect: the websockets keepalive pings every PING_INTERVAL seconds and
closes the connection if a pong takes longer than PING_TIMEOUT. A quiet slot
on a healthy connection is a real zero. Because a dead socket is only
noticed when a ping fails, each slot is also confirmed with a ping sent at
its end: the slot's points go to the sink only if that pong comes back, so a
connection that died late in a slot never yields zeros for it.

A pong proves the socket, not the feed: AISStream could stop sending data
on a socket that still answers pings. So the subscription also covers a
wide box (Persian Gulf and Gulf of Oman, FEED_BBOX) that always has traffic,
and a slot counts as covered only if at least FEED_MIN_MESSAGES data frames
arrived during it. A slot that fails this check is not emitted and status
shows error_class "feed_stalled". The tracker still counts only Hormuz-box
ships; `apply` keeps wide-box static data out of it too.

Parsing is a pure function, `parse_frame`, so unit
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
import traceback
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
    BBox,
    ShipPosition,
    Tracker,
    finite,
    slot_start,
)
from fuelwatch.http import ProviderError, redact
from fuelwatch.models import Frequency, Point, SeriesSpec

log = logging.getLogger(__name__)

URL = "wss://stream.aisstream.io/v0/stream"
ENV_KEY = "AISSTREAM_API_KEY"
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]
MAX_FRAME_BYTES = 16 * 1024  # real messages are about 1 KB
PING_INTERVAL = 20.0  # websockets keepalive: send a ping this often
PING_TIMEOUT = 20.0  # and drop the connection if its pong takes longer than this
# Persian Gulf plus Gulf of Oman: 100-400 messages a slot against 2-7 at Hormuz,
# so its traffic is the evidence that the feed, not just the socket, is live.
FEED_BBOX = BBox(south=22.0, west=48.0, north=30.5, east=60.5)
FEED_MIN_MESSAGES = 1  # data frames per slot needed for the slot to count as covered
BACKOFF_START = 1.0
BACKOFF_MAX = 300.0
STABLE_AFTER = 60.0  # a connection that lasted this long resets the backoff

_GRACE = timedelta(hours=1)
_SCHEDULE = "every 10 minutes while connected"
SERIES = [
    SeriesSpec(
        SERIES_SHIPS,
        "Ships broadcasting AIS in the strait",
        "ships",
        "AISStream",
        Frequency.TEN_MINUTES,
        _SCHEDULE,
        _GRACE,
    ),
    SeriesSpec(
        SERIES_TANKERS,
        "Tankers broadcasting AIS in the strait",
        "ships",
        "AISStream",
        Frequency.TEN_MINUTES,
        _SCHEDULE,
        _GRACE,
    ),
    SeriesSpec(
        SERIES_CROSSINGS,
        "Ships crossing the strait (live AIS)",
        "ships",
        "AISStream",
        Frequency.TEN_MINUTES,
        _SCHEDULE,
        _GRACE,
    ),
    SeriesSpec(
        SERIES_TANKER_CROSSINGS,
        "Tankers crossing the strait (live AIS)",
        "ships",
        "AISStream",
        Frequency.TEN_MINUTES,
        _SCHEDULE,
        _GRACE,
    ),
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
    lat: object = None  # the ship's last position, from MetaData (untrusted)
    lon: object = None


@dataclass(frozen=True)
class Confirmation:
    pass


@dataclass(frozen=True)
class ErrorMsg:
    text: str


Parsed = PositionMsg | StaticMsg | Confirmation | ErrorMsg


def subscription(api_key: str, bbox=HORMUZ_BBOX, feed_bbox=FEED_BBOX) -> str:
    return json.dumps(
        {
            "APIKey": api_key,
            "BoundingBoxes": [bbox.to_aisstream(), feed_bbox.to_aisstream()],
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
    except (ValueError, UnicodeDecodeError, RecursionError):
        return None  # RecursionError: nesting deeper than the decoder allows
    if not isinstance(doc, dict):
        return None
    if "error" in doc:  # AISStream reports a bad key or subscription this way
        err = doc["error"]
        return ErrorMsg(err[:200] if isinstance(err, str) else "unreadable error message")
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
    meta = doc.get("MetaData")
    meta = meta if isinstance(meta, dict) else {}
    return StaticMsg(
        mmsi=body.get("UserID"),
        ship_type=body.get("Type"),
        name=body.get("Name"),
        lat=meta.get("latitude"),
        lon=meta.get("longitude"),
    )


def apply(tracker: Tracker, msg: Parsed | None, now: datetime) -> bool:
    """Feed one message to the tracker. Positions outside its box are dropped
    by the tracker. Static data with a usable MetaData position is kept only
    if that position is in the box, even for a tracked ship; without one, only
    for a ship already tracked. So wide-box ships never enter the tracker's
    table (or its 5,000-ship cap), and the tracker never lets static data
    renew a ship (see Tracker.static)."""
    if isinstance(msg, PositionMsg):
        return tracker.position(msg.mmsi, msg.lat, msg.lon, now, msg.sog, msg.cog, msg.heading)
    if isinstance(msg, StaticMsg):
        lat, lon = finite(msg.lat), finite(msg.lon)
        if lat is not None and lon is not None:
            if not tracker.bbox.contains(lat, lon):
                return False
        elif not tracker.knows(msg.mmsi):
            return False
        return tracker.static(msg.mmsi, msg.ship_type, msg.name, now)
    return False


# --- the websocket client --------------------------------------------------------------


@dataclass
class AisStatus:
    connected: bool = False
    error_class: str | None = None
    message: str | None = None
    reconnects: int = 0
    last_message_at: datetime | None = None  # last AIS data frame


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
        feed_bbox: BBox = FEED_BBOX,
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
        self._ws = None  # the socket of the current live session, for slot-end pings
        self._feed_bbox = feed_bbox
        self._feed: dict[datetime, int] = {}  # data frames received, per slot start
        self._last_closed: datetime | None = None  # the latest slot end the ticker closed

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
            # The loops only return once stop is set; a quiet socket can sit in
            # recv(), so stop.wait() ends the run promptly instead. The ticker
            # is watched too: if either loop dies on an unexpected error, run()
            # ends with that error (below) rather than limping on with no
            # slots, so the caller's restart loop sees it.
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._ws = None
            self.tracker.disconnected(self._clock())
            self.status.connected = False
        for task in done:
            exc = None if task.cancelled() else task.exception()
            if exc is not None:
                name = "ticker" if task is tasks[0] else "stream"
                # Status gets the type name only, as the store does for internal
                # errors: the text may hold anything. The details go to the log.
                self.status.error_class = "internal"
                self.status.message = f"{name} failed: {type(exc).__name__}"
                details = redact("".join(traceback.format_exception(exc)))
                log.error("AISStream %s\n%s", self.status.message, details)
                raise exc

    async def _tick(self, sink: Sink, stop: asyncio.Event) -> None:
        while not stop.is_set():
            boundary = slot_start(self._clock()) + SLOT
            if self._last_closed is not None and boundary <= self._last_closed:
                # The wall clock stepped back: never close a slot twice.
                boundary = self._last_closed + SLOT
            # Sleep until the clock really reads the boundary; a sleep can end
            # early by the clock's reckoning if the clock was stepped meanwhile.
            while (now := self._clock()) < boundary:
                await self._sleep((boundary - now).total_seconds())
            self._last_closed = boundary
            # Count first, so frames arriving while the ping is in flight
            # belong to the next slot; then confirm the socket was alive.
            points = self.tracker.close_slot(boundary)
            start = boundary - SLOT
            feed = self._feed.pop(start, 0)
            for old in [k for k in self._feed if k < start]:
                del self._feed[old]
            if points and not await self._alive_through(boundary):
                log.info("AIS slot %s dropped: no pong at slot end", boundary.isoformat())
                points = []
            if points and feed < FEED_MIN_MESSAGES:
                self._stalled(boundary, feed)
                points = []
            elif points and self.status.error_class == "feed_stalled":
                self.status.error_class = self.status.message = None
            if points:
                try:
                    await sink(points)
                except Exception:
                    log.exception("AIS sink failed for slot %s", boundary.isoformat())

    async def _alive_through(self, boundary: datetime) -> bool:
        """True if the live socket answers a ping sent after `boundary`. The
        pong proves every frame the server sent before it has arrived, so the
        connection was healthy up to the end of the slot."""
        ws = self._ws
        if ws is None or not self.tracker.live:
            return False
        try:
            # One deadline for sending the ping and getting the pong: sending
            # can block too (a closing socket, a paused transport).
            async with asyncio.timeout(PING_TIMEOUT):
                await (await ws.ping())
        except (ConnectionClosed, TimeoutError):
            return False
        # A failure or reconnect noticed meanwhile still sinks the slot.
        return self._ws is ws and self.tracker.live

    def _stalled(self, boundary: datetime, frames: int) -> None:
        self.status.error_class = "feed_stalled"
        self.status.message = (
            f"socket up, but {frames} AIS messages from the Persian Gulf and Gulf of Oman "
            f"in the 10 minutes to {boundary:%H:%M} UTC; slot not stored"
        )
        log.warning("AISStream feed_stalled: %s", self.status.message)

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
        self._ws = None
        self.tracker.disconnected(self._clock())
        self.status.connected = False
        self.status.error_class = error_class
        self.status.message = redact(message)[:300]
        log.warning("AISStream %s: %s", error_class, self.status.message)

    async def _session(self, stop: asyncio.Event) -> None:
        async with self._connect(
            self._url,
            compression="deflate",
            max_size=2**20,
            open_timeout=10,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
        ) as ws:
            # AISStream closes the socket if the subscription is not sent within 3 s.
            await ws.send(subscription(self._key, self.tracker.bbox, self._feed_bbox))
            try:
                while not stop.is_set():
                    # No timeout here: silence is normal at Hormuz. A dead
                    # socket fails the keepalive ping and recv() raises
                    # ConnectionClosed.
                    raw = await ws.recv()
                    now = self._clock()
                    try:
                        msg = parse_frame(raw)
                    except Exception as e:  # noqa: BLE001 - one bad frame must not end the socket
                        log.warning("AISStream frame skipped: %s", type(e).__name__)
                        continue
                    if isinstance(msg, ErrorMsg):
                        raise ProviderError("provider_error", msg.text)
                    if msg is None:
                        continue
                    if isinstance(msg, (PositionMsg, StaticMsg)):
                        self.status.last_message_at = now
                        slot = slot_start(now)
                        self._feed[slot] = self._feed.get(slot, 0) + 1
                    if not self.tracker.live:
                        # The subscription confirmation (or the first data
                        # frame) shows the subscription was accepted.
                        self.tracker.connected(now)
                        self._ws = ws
                        self.status.connected = True
                        self.status.error_class = self.status.message = None
                    try:
                        apply(self.tracker, msg, now)
                    except Exception as e:  # noqa: BLE001 - as above
                        log.warning("AISStream message skipped: %s", type(e).__name__)
            finally:
                self._ws = None
            await ws.close()
