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
    assert msg == PositionMsg(mmsi=636019825, lat=26.6, lon=56.25, sog=12.3, cog=112.5,
                              heading=111)


def test_parse_ship_static_data_fixture():
    msg = parse_frame(fixture("ship_static_data.json"))
    assert msg == StaticMsg(mmsi=636019825, ship_type=80, name="EXAMPLE TANKER@@@@@@")


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


@pytest.mark.parametrize("raw", [
    b"", b"not json", b"[]", b"null", b"\xff\xfe", b'{"MessageType": "PositionReport"}',
    b'{"MessageType": "PositionReport", "Message": {"PositionReport": []}}',
    b'{"MessageType": "PositionReport", "Message": {"ShipStaticData": {}}}',
    b'{"MessageType": "AidsToNavigationReport", "Message": {"AidsToNavigationReport": {}}}',
    b'{"MessageType": "PositionReport", "Message": {"PositionReport": {"Valid": false}}}',
    b'{"MessageType": "PositionReport", "Message": {"PositionReport": {}}, "x": "'
    + b"a" * 20_000 + b'"}',
])
def test_malformed_or_oversized_frames_ignored(raw):
    assert parse_frame(raw) is None


def test_subscription_message_shape():
    sub = json.loads(subscription(KEY))
    assert sub == {
        "APIKey": KEY,
        "BoundingBoxes": [[[25.9, 55.9], [27.0, 57.0]]],
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
    def __init__(self, frames, end_exc=None):
        self.frames = list(frames)
        self.end_exc = end_exc or ConnectionClosedError(None, None)
        self.sent = []

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
    sockets = [FakeWS([fixture("subscription_confirmation.json"),
                       fixture("position_report.json"),
                       fixture("ship_static_data.json")])]
    stop = asyncio.Event()
    delays = []
    clock = Clock(T0 + timedelta(seconds=5))

    def connect(url, **kw):
        assert kw["compression"] == "deflate"
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
        elif boundary == T0 + timedelta(minutes=20):
            t.disconnected(boundary + timedelta(minutes=1))  # down 12:21 to 12:35
        elif boundary == T0 + timedelta(minutes=30):
            t.connected(boundary + timedelta(minutes=5))
            t.position("636019825", 26.6, 56.35, boundary + timedelta(minutes=6))
        elif boundary >= T0 + timedelta(minutes=60):
            stop.set()

    t.connected(clock.t)
    c = AisStreamCollector(KEY, tracker=t, clock=clock, sleep=sleep)
    await c._tick(sink, stop)

    ends = [pts[0].period_end for pts in emitted]
    # 12:10 partial (started 12:03); 12:30 and 12:40 overlap the outage; 12:50 onwards covered
    assert ends == [T0 + timedelta(minutes=20), T0 + timedelta(minutes=50),
                    T0 + timedelta(minutes=60)]
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
