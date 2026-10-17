"""Live AISStream contract test: a short session confirms the message shape.

Runs with FW_CONTRACT=1 and AISSTREAM_API_KEY set; skips otherwise.
Set FW_RECORD_FIXTURES=1 to save the first frame of each type to
tests/fixtures/aisstream/recorded_<type>.json for re-recording fixtures.

Docs: https://aisstream.io/documentation and
https://github.com/aisstream/ais-message-models/blob/master/type-definition.yaml
"""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from websockets.asyncio.client import connect

from fuelwatch.collectors.ais_tracker import HORMUZ_BBOX, Tracker
from fuelwatch.collectors.aisstream import (
    MESSAGE_TYPES,
    URL,
    ErrorMsg,
    PositionMsg,
    StaticMsg,
    apply,
    parse_frame,
)
from fuelwatch.http import redact

# Class A ships send static data (AIS message 5, ShipStaticData) only every
# 6 minutes, so the session runs until both types have arrived, capped a
# little above that interval.
SESSION_SECONDS = 420
# Hormuz traffic may be thin or jammed, so a busy box (Strait of Dover)
# guarantees enough frames to check the shape.
DOVER = [[50.8, 1.0], [51.2, 1.8]]
FIXTURES = Path(__file__).parent.parent / "fixtures" / "aisstream"

POSITION_FIELDS = {
    "UserID": int,
    "Valid": bool,
    "Latitude": (int, float),
    "Longitude": (int, float),
    "Sog": (int, float),
    "Cog": (int, float),
    "TrueHeading": int,
}
STATIC_FIELDS = {"UserID": int, "Valid": bool, "Name": str, "Type": int}


def _check_fields(body: dict, fields: dict) -> None:
    for name, kind in fields.items():
        assert name in body, f"missing field {name}; got {sorted(body)}"
        assert isinstance(body[name], kind), f"{name} is {type(body[name]).__name__}"


async def _session(key: str) -> dict:
    found: dict[str, dict] = {}
    raw_first: dict[str, bytes | str] = {}
    other_types: set[str] = set()
    tracker = Tracker()
    hormuz_positions = 0
    deadline = time.monotonic() + SESSION_SECONDS
    async with connect(URL, compression="deflate", open_timeout=10) as ws:
        await ws.send(
            json.dumps(
                {
                    "APIKey": key,
                    "BoundingBoxes": [HORMUZ_BBOX.to_aisstream(), DOVER],
                    "FilterMessageTypes": MESSAGE_TYPES,
                }
            )
        )
        while time.monotonic() < deadline and not set(MESSAGE_TYPES) <= found.keys():
            try:
                raw = await asyncio.wait_for(ws.recv(), deadline - time.monotonic())
            except TimeoutError:
                break
            msg = parse_frame(raw)
            assert not isinstance(msg, ErrorMsg), f"provider error: {redact(msg.text)}"
            doc = json.loads(raw)
            mtype = doc.get("MessageType")
            if mtype not in MESSAGE_TYPES:
                # The server acknowledges each subscription before data flows.
                if mtype != "SubscriptionConfirmation":
                    other_types.add(str(mtype))
                continue
            assert {"MessageType", "MetaData", "Message"} <= doc.keys(), sorted(doc)
            assert isinstance(doc["Message"].get(mtype), dict)
            if mtype not in found:
                found[mtype] = doc
                raw_first[mtype] = raw
            if isinstance(msg, PositionMsg) and HORMUZ_BBOX.contains(msg.lat, msg.lon):
                hormuz_positions += apply(tracker, msg, datetime.now(UTC))
            elif isinstance(msg, StaticMsg):
                apply(tracker, msg, datetime.now(UTC))
    return {
        "found": found,
        "raw": raw_first,
        "other": other_types,
        "hormuz": hormuz_positions,
        "tracker": tracker,
    }


@pytest.mark.contract
async def test_aisstream_message_shape():
    key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    if not key:
        pytest.skip("AISSTREAM_API_KEY is not set; no AISStream key on this machine")
    redact.add(key)

    result = await _session(key)
    found = result["found"]

    assert "PositionReport" in found, f"no PositionReport in {SESSION_SECONDS}s"
    pos_doc = found["PositionReport"]
    pos = pos_doc["Message"]["PositionReport"]
    _check_fields(pos, POSITION_FIELDS)
    assert isinstance(parse_frame(result["raw"]["PositionReport"]), PositionMsg)
    meta_mmsi = pos_doc["MetaData"].get("MMSI")
    assert meta_mmsi is None or int(meta_mmsi) == pos["UserID"]

    assert "ShipStaticData" in found, f"no ShipStaticData in {SESSION_SECONDS}s"
    static = found["ShipStaticData"]["Message"]["ShipStaticData"]
    _check_fields(static, STATIC_FIELDS)
    assert isinstance(parse_frame(result["raw"]["ShipStaticData"]), StaticMsg)

    assert not result["other"], f"unsubscribed message types arrived: {result['other']}"

    print(
        f"Hormuz positions accepted: {result['hormuz']}; "
        f"ships tracked: {len(result['tracker'].snapshot(datetime.now(UTC)))}; "
        f"MetaData keys: {sorted(pos_doc['MetaData'])}"
    )

    if os.environ.get("FW_RECORD_FIXTURES") == "1":
        for mtype, raw in result["raw"].items():
            text = raw.decode() if isinstance(raw, bytes) else raw
            (FIXTURES / f"recorded_{mtype}.json").write_text(
                json.dumps(json.loads(text), indent=2) + "\n"
            )
