import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from websockets.exceptions import ConnectionClosedError

from fuelwatch.collectors import aisstream
from fuelwatch.collectors.ais_tracker import SERIES_SHIPS, SERIES_TANKERS, Tracker
from fuelwatch.collectors.aisstream import (
    BACKOFF_MAX,
    PING_INTERVAL,
    PING_TIMEOUT,
    SERIES,
    AisStreamCollector,
    Confirmation,
    ErrorMsg,
    PositionMsg,
    StaticMsg,
    apply,
    parse_frame,
    subscription,
)
from fuelwatch.models import Frequency

FIX = Path(__file__).parent.parent / "fixtures" / "aisstream"
T0 = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
KEY = "planted-fake-aisstream-key"


def fixture(name: str) -> bytes:
    return (FIX / name).read_bytes()  # AISStream sends binary frames of UTF-8 JSON


# --- parsing ---------------------------------------------------------------------------


def test_parse_position_report_fixture():
    msg = parse_frame(fixture("position_report.json"))
    assert msg == PositionMsg(mmsi=636019825, lat=26.6, lon=56.25, sog=12.3, cog=112.5, heading=111)


def test_parse_ship_static_data_fixture():
    msg = parse_frame(fixture("ship_static_data.json"))
    assert msg == StaticMsg(
        mmsi=636019825, ship_type=80, name="EXAMPLE TANKER@@@@@@", lat=26.6, lon=56.25
    )


def test_parse_confirmation_and_text_frames():
    assert parse_frame(fixture("subscription_confirmation.json")) == Confirmation()
    assert isinstance(parse_frame(fixture("position_report.json").decode()), PositionMsg)


def test_fixtures_end_to_end_make_a_tanker_on_the_map():
    t = Tracker()
    assert apply(t, parse_frame(fixture("position_report.json")), T0)
    assert apply(t, parse_frame(fixture("ship_static_data.json")), T0)
    (ship,) = t.snapshot(T0)
    assert ship.mmsi == "636019825" and ship.tanker and ship.name == "EXAMPLE TANKER"


def test_error_frame_is_reported():
    assert parse_frame(b'{"error": "Api Key Is Not Valid"}') == ErrorMsg("Api Key Is Not Valid")


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"not json",
        b"[]",
        b"null",
        b"\xff\xfe",
        b'{"MessageType": "PositionReport"}',
        b'{"MessageType": "PositionReport", "Message": {"PositionReport": []}}',
        b'{"MessageType": "PositionReport", "Message": {"ShipStaticData": {}}}',
        b'{"MessageType": "AidsToNavigationReport", "Message": {"AidsToNavigationReport": {}}}',
        b'{"MessageType": "PositionReport", "Message": {"PositionReport": {"Valid": false}}}',
        b'{"MessageType": "PositionReport", "Message": {"PositionReport": {}}, "x": "'
        + b"a" * 20_000
        + b'"}',
    ],
)
def test_malformed_or_oversized_frames_ignored(raw):
    assert parse_frame(raw) is None


def test_subscription_message_shape():
    sub = json.loads(subscription(KEY))
    assert sub == {
        "APIKey": KEY,
        # Hormuz, plus the Persian Gulf and Gulf of Oman as the feed heartbeat
        "BoundingBoxes": [[[25.9, 55.9], [27.0, 57.0]], [[22.0, 48.0], [30.5, 60.5]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


def test_series_registry():
    assert len(SERIES) == 4
    assert all(s.frequency is Frequency.TEN_MINUTES for s in SERIES)
    assert all(s.grace == timedelta(hours=1) for s in SERIES)


def test_from_env(monkeypatch):
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    assert AisStreamCollector.from_env() is None
    monkeypatch.setenv("AISSTREAM_API_KEY", KEY)
    assert isinstance(AisStreamCollector.from_env(), AisStreamCollector)


# --- websocket client with a fake socket -----------------------------------------------


class FakeWS:
    def __init__(self, frames, end_exc=None, pong=True):
        self.frames = list(frames)
        self.end_exc = end_exc or ConnectionClosedError(None, None)
        self.sent = []
        self.pong = pong  # False: pings are never answered (a dead peer)
        self.pings = 0

    async def ping(self):
        self.pings += 1
        waiter = asyncio.get_running_loop().create_future()
        if self.pong:
            waiter.set_result(0.0)
        return waiter

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def send(self, data):
        self.sent.append(data)

    async def recv(self):
        await asyncio.sleep(0)
        if self.frames:
            return self.frames.pop(0)
        raise self.end_exc

    async def close(self):
        pass


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


async def test_session_marks_live_then_disconnected_and_backs_off():
    sockets = [
        FakeWS(
            [
                fixture("subscription_confirmation.json"),
                fixture("position_report.json"),
                fixture("ship_static_data.json"),
            ]
        )
    ]
    stop = asyncio.Event()
    delays = []
    clock = Clock(T0 + timedelta(seconds=5))

    def connect(url, **kw):
        assert kw["compression"] == "deflate"
        # liveness is the websocket keepalive, not message silence
        assert kw["ping_interval"] == PING_INTERVAL and kw["ping_timeout"] == PING_TIMEOUT
        if sockets:
            return sockets.pop(0)
        raise OSError("connection refused")

    async def sleep(d):
        delays.append(d)
        if len(delays) >= 6:
            stop.set()

    c = AisStreamCollector(KEY, connect=connect, clock=clock, sleep=sleep)
    seen_live = []
    orig = c.tracker.connected
    c.tracker.connected = lambda now: (seen_live.append(now), orig(now))
    await c._stream(stop)

    assert seen_live == [clock.t]
    assert not c.tracker.live
    assert len(c.tracker.snapshot(clock.t)) == 1
    assert c.status.error_class == "transport" and not c.status.connected
    # exponential backoff with jitter, capped
    assert delays[1] > delays[0] * 1.3 and all(d <= BACKOFF_MAX * 1.2 for d in delays)


async def test_provider_error_frame_is_redacted(caplog):
    caplog.set_level(logging.DEBUG)
    sockets = [FakeWS([json.dumps({"error": f"bad key {KEY}"}).encode()])]
    stop = asyncio.Event()

    async def sleep(d):
        stop.set()

    c = AisStreamCollector(KEY, connect=lambda u, **k: sockets.pop(0), sleep=sleep)
    await c._stream(stop)
    assert c.status.error_class == "provider_error"
    assert KEY not in c.status.message
    assert KEY not in caplog.text
    assert not c.tracker.live  # an error before any data never marks the stream live


async def test_ticker_emits_only_covered_slots_and_never_zeros():
    clock = Clock(T0 + timedelta(minutes=3))
    t = Tracker()
    stop = asyncio.Event()
    emitted = []

    async def sink(points):
        emitted.append(points)

    async def sleep(d):
        clock.t += timedelta(seconds=d)
        boundary = clock.t
        if boundary == T0 + timedelta(minutes=10):  # first, partial slot: connected at 12:03
            t.position("636019825", 26.6, 56.35, boundary + timedelta(seconds=1))
        elif boundary == T0 + timedelta(minutes=30):
            t.disconnected(T0 + timedelta(minutes=21))  # down 12:21 to 12:35
            t.connected(T0 + timedelta(minutes=35))
            t.position("636019825", 26.6, 56.35, T0 + timedelta(minutes=36))
        elif boundary >= T0 + timedelta(minutes=60):
            stop.set()

    t.connected(clock.t)
    c = AisStreamCollector(KEY, tracker=t, clock=clock, sleep=sleep)
    c._ws = FakeWS([])  # a healthy socket answers the slot-end ping
    c._feed = {T0 + timedelta(minutes=m): 50 for m in range(0, 60, 10)}  # the feed is busy
    await c._tick(sink, stop)

    ends = [pts[0].period_end for pts in emitted]
    # 12:10 partial (started 12:03); 12:30 and 12:40 overlap the outage; 12:50 onwards covered
    assert ends == [
        T0 + timedelta(minutes=20),
        T0 + timedelta(minutes=50),
        T0 + timedelta(minutes=60),
    ]
    first = {p.series: p.value for p in emitted[0]}
    assert first[SERIES_SHIPS] == 1 and first[SERIES_TANKERS] == 0


def test_key_is_registered_for_redaction():
    AisStreamCollector("another-planted-key")
    assert "another-planted-key" not in aisstream.redact("x another-planted-key y")


async def test_run_stops_promptly_while_socket_is_quiet():
    class QuietWS(FakeWS):
        async def recv(self):
            if self.frames:
                return self.frames.pop(0)
            await asyncio.Event().wait()  # a socket with nothing to say

    ws = QuietWS([fixture("position_report.json")])
    stop = asyncio.Event()
    c = AisStreamCollector(KEY, connect=lambda u, **k: ws)

    async def stop_soon():
        await asyncio.sleep(0.05)
        stop.set()

    async def sink(points):
        pass

    await asyncio.wait_for(asyncio.gather(c.run(sink, stop), stop_soon()), timeout=2)
    assert json.loads(ws.sent[0])["APIKey"] == KEY
    assert not c.tracker.live and not c.status.connected
    assert len(c.snapshot()) == 1  # positions stay on the map until they expire


# --- liveness: keepalive pings and feed traffic, not Hormuz silence --------------------

GULF = (29.0, 49.5)  # in the Persian Gulf feed box, far outside the Hormuz box
GULF_MMSI = 422000001


def pos_frame(mmsi, lat, lon) -> bytes:
    doc = json.loads(fixture("position_report.json"))
    doc["MetaData"].update(MMSI=mmsi, MMSI_String=mmsi, latitude=lat, longitude=lon)
    doc["Message"]["PositionReport"].update(UserID=mmsi, Latitude=lat, Longitude=lon)
    return json.dumps(doc).encode()


def static_frame(mmsi, lat, lon, ship_type=80) -> bytes:
    doc = json.loads(fixture("ship_static_data.json"))
    doc["MetaData"].update(MMSI=mmsi, MMSI_String=mmsi, latitude=lat, longitude=lon)
    doc["Message"]["ShipStaticData"].update(UserID=mmsi, Type=ship_type)
    return json.dumps(doc).encode()


class QuietWS(FakeWS):
    """Delivers frames as the test pushes them and stays silent in between,
    like a quiet but healthy socket. `kill()` makes it fail the way
    websockets does after a keepalive ping times out."""

    def __init__(self, frames, **kw):
        super().__init__(frames, **kw)
        self.wake = asyncio.Event()
        self.dead = False

    def push(self, *frames):
        self.frames.extend(frames)
        self.wake.set()

    def kill(self):
        self.dead = True
        self.wake.set()

    async def recv(self):
        await asyncio.sleep(0)
        while not self.frames:
            if self.dead:
                raise ConnectionClosedError(None, None)
            self.wake.clear()
            await self.wake.wait()
        return self.frames.pop(0)


async def settle():
    for _ in range(20):
        await asyncio.sleep(0)  # let the stream task take what was pushed


async def gulf_traffic(ws, clock, boundary):
    """Mid-slot traffic from the wide box only: the feed is alive, Hormuz is quiet."""
    clock.t = boundary - timedelta(minutes=5)
    ws.push(pos_frame(GULF_MMSI, *GULF), static_frame(GULF_MMSI, *GULF))
    await settle()


def scripted_run(sockets, clock, on_tick, *, stop_after):
    """A collector whose ticker sleeps move the fake clock to each boundary.
    `on_tick(t)` runs just before the clock reaches boundary t and may move
    the clock within the slot; reconnect backoff sleeps just yield. Stops
    after `stop_after` sink calls."""
    stop = asyncio.Event()
    emitted = []
    connects = []

    def connect(url, **kw):
        assert kw["ping_interval"] == PING_INTERVAL and kw["ping_timeout"] == PING_TIMEOUT
        ws = sockets.pop(0)
        connects.append(ws)
        return ws

    async def sleep(d):
        await settle()
        if d < 30:  # reconnect backoff
            clock.t += timedelta(seconds=d)
            return
        target = clock.t + timedelta(seconds=d)
        await on_tick(target)
        clock.t = target

    async def sink(points):
        emitted.append(points)
        if len(emitted) >= stop_after:
            stop.set()

    c = AisStreamCollector(KEY, connect=connect, clock=clock, sleep=sleep)
    return c, sink, stop, emitted, connects


async def test_quiet_hormuz_with_live_feed_emits_real_zeros():
    # Confirmed at 11:58; then only Persian Gulf traffic, nothing at Hormuz.
    ws = QuietWS([fixture("subscription_confirmation.json")])
    clock = Clock(T0 - timedelta(minutes=2))

    async def on_tick(boundary):
        if boundary > T0:
            await gulf_traffic(ws, clock, boundary)

    c, sink, stop, emitted, connects = scripted_run([ws], clock, on_tick, stop_after=3)
    await asyncio.wait_for(c.run(sink, stop), timeout=5)

    assert [pts[0].period_end for pts in emitted] == [
        T0 + timedelta(minutes=10),
        T0 + timedelta(minutes=20),
        T0 + timedelta(minutes=30),
    ]
    assert all(p.value == 0 for pts in emitted for p in pts)  # real zeros, all four series
    assert len(connects) == 1 and c.status.reconnects == 0  # silence never reconnects
    assert ws.pings == 3  # each emitted slot was confirmed by a pong
    assert c.status.error_class is None
    # the Gulf ship never reached the Hormuz tracker
    assert len(c.tracker) == 0 and c.snapshot() == ()


async def test_healthy_socket_with_stalled_feed_emits_nothing_and_says_so():
    # The socket answers every ping, but AISStream sends no data at all
    # for 12:00-12:20. Then the feed comes back.
    ws = QuietWS([fixture("subscription_confirmation.json")])
    clock = Clock(T0 - timedelta(minutes=2))
    seen = {}
    box = []

    async def on_tick(boundary):
        if box:
            seen[boundary] = (box[0].status.error_class, box[0].status.message)
        if boundary == T0 + timedelta(minutes=30):
            await gulf_traffic(ws, clock, boundary)

    c, sink, stop, emitted, connects = scripted_run([ws], clock, on_tick, stop_after=1)
    box.append(c)
    await asyncio.wait_for(c.run(sink, stop), timeout=5)

    # 12:10 and 12:20 had a healthy socket but no feed: no points, not zeros.
    assert [pts[0].period_end for pts in emitted] == [T0 + timedelta(minutes=30)]
    error_class, message = seen[T0 + timedelta(minutes=30)]  # status after the 12:20 slot
    assert error_class == "feed_stalled"
    assert "0 AIS messages" in message and "12:20 UTC" in message
    assert c.status.error_class is None and c.status.message is None  # cleared on recovery
    assert len(connects) == 1 and c.status.reconnects == 0


def test_wide_box_ships_never_enter_the_hormuz_tracker():
    t = Tracker(max_ships=3)
    assert apply(t, parse_frame(pos_frame(636019825, 26.6, 56.35)), T0)  # a Hormuz ship
    for i in range(10):  # a flood of Gulf ships, positions and static data
        mmsi = GULF_MMSI + i
        assert not apply(t, parse_frame(pos_frame(mmsi, *GULF)), T0)
        assert not apply(t, parse_frame(static_frame(mmsi, *GULF)), T0)
    assert len(t) == 1  # no wide-box ship in the table or against the cap
    assert [s.mmsi for s in t.snapshot(T0)] == ["636019825"]
    # static data still lands for a tracked ship, wherever MetaData puts it ...
    assert apply(t, parse_frame(static_frame(636019825, *GULF)), T0)
    assert t.snapshot(T0)[0].tanker
    # ... and for an untracked ship whose MetaData position is in the box
    assert apply(t, parse_frame(static_frame(422000999, 26.5, 56.5)), T0)
    # static data with a missing or junk position is dropped for unknown ships
    assert not apply(t, StaticMsg(422000998, 80, "X", lat=None, lon=None), T0)
    assert not apply(t, StaticMsg(422000997, 80, "X", lat=float("nan"), lon=56.5), T0)
    assert not apply(t, StaticMsg(422000996, 80, "X", lat=True, lon=56.5), T0)


def test_wide_box_ships_never_reach_hormuz_counts():
    t = Tracker()
    t.connected(T0 - timedelta(minutes=1))
    for i in range(50):
        apply(t, parse_frame(pos_frame(GULF_MMSI + i, *GULF)), T0 + timedelta(minutes=1))
    apply(t, parse_frame(pos_frame(636019825, 26.6, 56.35)), T0 + timedelta(minutes=2))
    counts = {p.series: p.value for p in t.close_slot(T0 + timedelta(minutes=10))}
    assert counts[SERIES_SHIPS] == 1


async def test_failed_keepalive_reconnects_and_drops_the_slot():
    ws1 = QuietWS([fixture("subscription_confirmation.json")])
    ws2 = QuietWS([fixture("subscription_confirmation.json")])
    clock = Clock(T0 - timedelta(minutes=2))
    live = [ws1]

    async def on_tick(boundary):
        if boundary == T0 + timedelta(minutes=20):
            clock.t = T0 + timedelta(minutes=15)  # keepalive ping times out at 12:15
            ws1.kill()
            live[0] = ws2
            await settle()  # fail, back off, reconnect, confirm
        if boundary > T0:
            await gulf_traffic(live[0], clock, boundary)

    c, sink, stop, emitted, connects = scripted_run([ws1, ws2], clock, on_tick, stop_after=2)
    await asyncio.wait_for(c.run(sink, stop), timeout=5)

    # 12:10 covered by ws1; 12:20 overlaps the 12:15 outage, so no points
    # (not zeros); 12:30 covered by ws2.
    assert [pts[0].period_end for pts in emitted] == [
        T0 + timedelta(minutes=10),
        T0 + timedelta(minutes=30),
    ]
    assert connects == [ws1, ws2] and c.status.reconnects == 1


async def tick_one_slot(ws_factory):
    """Run the ticker over the 12:00-12:10 slot, which the tracker covered and
    the feed had traffic in; only the slot-end ping decides it."""
    t = Tracker()
    t.connected(T0 - timedelta(minutes=1))
    t.position("636019825", 26.6, 56.35, T0 + timedelta(minutes=1))
    clock = Clock(T0 + timedelta(minutes=5))
    stop = asyncio.Event()
    emitted = []

    async def sleep(d):
        clock.t += timedelta(seconds=d)
        if clock.t >= T0 + timedelta(minutes=10):
            stop.set()

    async def sink(points):
        emitted.append(points)

    c = AisStreamCollector(KEY, tracker=t, clock=clock, sleep=sleep)
    c._feed = {T0: 50}
    c._ws = ws_factory(c)
    await asyncio.wait_for(c._tick(sink, stop), timeout=2)
    return c, emitted


async def test_answered_slot_end_ping_emits_the_slot():
    c, emitted = await tick_one_slot(lambda c: FakeWS([]))
    assert len(emitted) == 1 and c._ws.pings == 1


async def test_unanswered_slot_end_ping_drops_the_slot(monkeypatch):
    # The peer is gone but the keepalive has not noticed yet: no pong.
    monkeypatch.setattr(aisstream, "PING_TIMEOUT", 0.01)
    c, emitted = await tick_one_slot(lambda c: FakeWS([], pong=False))
    assert emitted == [] and c._ws.pings == 1


async def test_ping_send_that_blocks_is_under_the_same_timeout(monkeypatch):
    # Sending the ping itself can block (paused transport, closing socket).
    monkeypatch.setattr(aisstream, "PING_TIMEOUT", 0.01)

    class BlockedWS(FakeWS):
        async def ping(self):
            await asyncio.Event().wait()

    _, emitted = await tick_one_slot(lambda c: BlockedWS([]))
    assert emitted == []


async def test_pong_waiter_raising_connection_closed_drops_the_slot():
    class ClosingWS(FakeWS):
        async def ping(self):
            waiter = asyncio.get_running_loop().create_future()
            waiter.set_exception(ConnectionClosedError(None, None))
            return waiter

    _, emitted = await tick_one_slot(lambda c: ClosingWS([]))
    assert emitted == []


async def test_reconnect_while_ping_in_flight_drops_the_slot():
    # The old socket dies and a new session is already live when the pong
    # resolves: the pong is not evidence for the slot, so it is dropped.
    class SwappedWS(FakeWS):
        def __init__(self, c):
            super().__init__([])
            self.c = c

        async def ping(self):
            waiter = asyncio.get_running_loop().create_future()
            now = self.c._clock()
            self.c.tracker.disconnected(now)
            self.c.tracker.connected(now)  # outage starts after the slot end
            self.c._ws = FakeWS([])
            waiter.set_result(0.0)
            return waiter

    c, emitted = await tick_one_slot(SwappedWS)
    assert c.tracker.live  # only the socket identity check catches this
    assert emitted == []


async def test_ticker_crash_ends_run_with_the_error():
    class BrokenPingWS(QuietWS):
        async def ping(self):
            raise RuntimeError("websockets changed")

    ws = BrokenPingWS([fixture("subscription_confirmation.json")])
    clock = Clock(T0 - timedelta(minutes=2))

    async def on_tick(boundary):
        if boundary > T0:
            await gulf_traffic(ws, clock, boundary)

    c, sink, stop, _, _ = scripted_run([ws], clock, on_tick, stop_after=1)
    with pytest.raises(RuntimeError, match="websockets changed"):
        await asyncio.wait_for(c.run(sink, stop), timeout=5)
    assert c.status.error_class == "internal"
    assert c.status.message.startswith("ticker failed: RuntimeError")
    assert not c.status.connected and not c.tracker.live


async def test_no_live_socket_means_no_slot():
    # Even if the tracker thinks it is live, a slot needs a socket to confirm it.
    _, emitted = await tick_one_slot(lambda c: None)
    assert emitted == []
