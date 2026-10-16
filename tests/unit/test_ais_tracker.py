from datetime import UTC, datetime, timedelta

import pytest

from fuelwatch.collectors.ais_tracker import (
    HORMUZ_BBOX,
    HORMUZ_LINE,
    NAME_MAX,
    SERIES_CROSSINGS,
    SERIES_SHIPS,
    SERIES_TANKER_CROSSINGS,
    SERIES_TANKERS,
    Tracker,
    clean_name,
    is_tanker,
    segments_cross,
    valid_mmsi,
)

T0 = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)  # a slot boundary
MIN = timedelta(minutes=1)

# West and east of the crossing line at 26.6N (the line is at about 56.381E there).
WEST = (26.6, 56.35)
EAST = (26.6, 56.42)


def live_tracker(**kw) -> Tracker:
    t = Tracker(**kw)
    t.connected(T0 - MIN)
    return t


def values(points):
    return {p.series: p.value for p in points}


# --- tanker typing ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "code,expected",
    [
        (79, False),
        (80, True),
        (84, True),
        (89, True),
        (90, False),
        (70, False),
        (0, False),
        (None, False),
    ],
)
def test_tanker_types_are_80_to_89(code, expected):
    assert is_tanker(code) is expected


def test_counts_all_ships_and_tankers_seen_in_slot():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + MIN)
    t.static(636019825, 81, "TANKER ONE", T0 + MIN)
    t.position("538001234", 26.2, 56.8, T0 + 2 * MIN)
    t.static(538001234, 70, "CARGO ONE", T0 + 2 * MIN)
    t.position("477000001", 26.3, 56.7, T0 + 3 * MIN)  # no static data yet: counted, untyped
    v = values(t.close_slot(T0 + 10 * MIN))
    assert v[SERIES_SHIPS] == 3
    assert v[SERIES_TANKERS] == 1


def test_points_use_slot_end_as_period_end():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + MIN)
    pts = t.close_slot(T0 + 10 * MIN)
    assert {p.period_end for p in pts} == {T0 + 10 * MIN}
    assert len(pts) == 4


def test_ship_heard_only_in_earlier_slot_is_not_counted_now():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + MIN)
    t.close_slot(T0 + 10 * MIN)
    v = values(t.close_slot(T0 + 20 * MIN))
    assert v[SERIES_SHIPS] == 0  # live for the whole slot, genuinely none heard


def test_slot_end_must_be_aligned():
    with pytest.raises(ValueError):
        live_tracker().close_slot(T0 + 5 * MIN)


# --- line crossings --------------------------------------------------------------------


def test_segment_crossing_geometry():
    assert segments_cross(WEST, EAST, HORMUZ_LINE)
    assert segments_cross(EAST, WEST, HORMUZ_LINE)
    assert not segments_cross(WEST, (26.7, 56.30), HORMUZ_LINE)
    # passes north of Larak, beyond the end of the line
    assert not segments_cross((26.95, 56.30), (26.95, 56.45), HORMUZ_LINE)


def test_crossing_counted_for_all_and_tankers():
    t = live_tracker()
    t.static("636019825", 84, "TANKER", T0)
    t.position("636019825", *WEST, T0 + MIN)
    t.position("636019825", *EAST, T0 + 9 * MIN)
    t.position("538001234", *EAST, T0 + 2 * MIN)
    t.position("538001234", *WEST, T0 + 8 * MIN)
    v = values(t.close_slot(T0 + 10 * MIN))
    assert v[SERIES_CROSSINGS] == 2
    assert v[SERIES_TANKER_CROSSINGS] == 1


def test_crossing_lands_in_slot_when_it_happened():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + 8 * MIN)
    t.position("636019825", *EAST, T0 + 12 * MIN)
    assert values(t.close_slot(T0 + 10 * MIN))[SERIES_CROSSINGS] == 0
    assert values(t.close_slot(T0 + 20 * MIN))[SERIES_CROSSINGS] == 1


def test_jitter_across_line_counts_once():
    t = live_tracker()
    near_w, near_e = (26.6, 56.375), (26.6, 56.387)  # about 0.6 nm apart
    for i in range(8):
        t.position("636019825", *(near_w if i % 2 == 0 else near_e), T0 + i * MIN)
    assert values(t.close_slot(T0 + 10 * MIN))[SERIES_CROSSINGS] == 1


def test_implausible_jump_is_not_a_crossing():
    t = live_tracker()
    t.position("636019825", 26.6, 56.0, T0 + MIN)
    t.position("636019825", 26.6, 56.9, T0 + MIN + timedelta(seconds=10))  # ~48 nm in 10 s
    assert values(t.close_slot(T0 + 10 * MIN))[SERIES_CROSSINGS] == 0


def test_no_crossing_across_a_gap_longer_than_expiry():
    t = Tracker()
    t.connected(T0 - 60 * MIN)
    t.position("636019825", *WEST, T0 - 45 * MIN)
    t.position("636019825", *EAST, T0 + MIN)
    assert values(t.close_slot(T0 + 10 * MIN))[SERIES_CROSSINGS] == 0


# --- no zeros while disconnected -------------------------------------------------------


def test_never_connected_emits_nothing():
    assert Tracker().close_slot(T0 + 10 * MIN) == []


def test_partial_first_slot_emits_nothing():
    t = Tracker()
    t.connected(T0 + 3 * MIN)
    t.position("636019825", *WEST, T0 + 4 * MIN)
    assert t.close_slot(T0 + 10 * MIN) == []
    t.position("636019825", *WEST, T0 + 11 * MIN)
    assert values(t.close_slot(T0 + 20 * MIN))[SERIES_SHIPS] == 1


def test_disconnected_slot_emits_nothing_not_zeros():
    t = live_tracker()
    t.disconnected(T0 + 2 * MIN)
    assert t.close_slot(T0 + 10 * MIN) == []
    assert t.close_slot(T0 + 20 * MIN) == []


def test_brief_outage_inside_slot_drops_that_slot_only():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + MIN)
    t.disconnected(T0 + 4 * MIN)
    t.connected(T0 + 5 * MIN)
    t.position("636019825", *WEST, T0 + 6 * MIN)
    assert t.close_slot(T0 + 10 * MIN) == []
    t.position("636019825", *WEST, T0 + 11 * MIN)
    assert values(t.close_slot(T0 + 20 * MIN))[SERIES_SHIPS] == 1


def test_disconnect_just_after_slot_end_keeps_completed_slot():
    t = live_tracker()
    t.position("636019825", *WEST, T0 + MIN)
    t.disconnected(T0 + 10 * MIN + timedelta(seconds=1))
    assert values(t.close_slot(T0 + 10 * MIN))[SERIES_SHIPS] == 1


# --- expiry and cap --------------------------------------------------------------------


def test_snapshot_expires_after_30_minutes():
    t = live_tracker()
    t.position("636019825", *WEST, T0)
    assert len(t.snapshot(T0 + 29 * MIN)) == 1
    assert t.snapshot(T0 + 30 * MIN) == ()
    t.position("538001234", *EAST, T0 + 31 * MIN)
    assert len(t) == 1  # the stale ship is gone, not just hidden


def test_snapshot_is_read_only_copy():
    t = live_tracker()
    t.position("636019825", *WEST, T0, sog=12.3, cog=112.5, heading=111)
    t.static("636019825", 80, "EXAMPLE TANKER@@@@@@", T0)
    (ship,) = t.snapshot(T0 + MIN)
    assert (ship.mmsi, ship.lat, ship.lon, ship.tanker, ship.name) == (
        "636019825",
        26.6,
        56.35,
        True,
        "EXAMPLE TANKER",
    )
    with pytest.raises(AttributeError):
        ship.lat = 0.0  # frozen
    assert isinstance(t.snapshot(T0 + MIN), tuple)


def test_static_only_ships_are_not_on_the_map():
    t = live_tracker()
    t.static("636019825", 80, "NO POSITION", T0)
    assert t.snapshot(T0 + MIN) == ()


def test_cap_of_5000_ships_evicts_least_recently_heard():
    t = live_tracker()
    for i in range(5000):
        t.position(f"{201000000 + i}", *WEST, T0 + timedelta(milliseconds=i))
    t.position("201000000", *WEST, T0 + MIN)  # refresh the oldest
    t.position("775999999", *WEST, T0 + MIN)  # 5001st ship
    assert len(t) == 5000
    mmsis = {s.mmsi for s in t.snapshot(T0 + 2 * MIN)}
    assert "201000000" in mmsis and "775999999" in mmsis
    assert "201000001" not in mmsis


# --- untrusted input -------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        "12345678",
        "1234567890",
        "000000000",
        "123456789",
        "111234567",
        "800123456",
        "970123456",
        "992351000",
        "999999999",
        "776000000",
        "20100000a",
        "２０１０００００１",
        True,
        None,
        2.01e8,
        [201000001],
        {"m": 1},
        -201000001,
    ],
)
def test_invalid_mmsis_dropped(bad):
    t = live_tracker()
    assert valid_mmsi(bad) is None
    assert t.position(bad, *WEST, T0) is False
    assert t.static(bad, 80, "X", T0) is False
    assert len(t) == 0


@pytest.mark.parametrize("good", ["201000001", 201000001, "636019825", 775999999])
def test_valid_mmsis_accepted(good):
    assert valid_mmsi(good) == str(good)


@pytest.mark.parametrize(
    "lat,lon",
    [
        (25.0, 56.3),  # south of the box (Fujairah anchorage)
        (27.2, 56.3),  # Bandar Abbas
        (26.5, 55.0),  # inside the Gulf
        (26.5, 58.0),  # Gulf of Oman
        (91.0, 181.0),  # AIS "not available"
        (0.0, 0.0),  # null island
        (float("nan"), 56.3),
        (26.5, float("inf")),
        ("26.5", "56.3"),
        (None, None),
        (True, True),
    ],
)
def test_out_of_area_or_malformed_positions_dropped(lat, lon):
    t = live_tracker()
    assert t.position("636019825", lat, lon, T0) is False
    assert t.snapshot(T0) == ()


def test_markup_in_names_is_kept_as_short_plain_text():
    t = live_tracker()
    evil = '<script>alert("x")</script><img src=x onerror=alert(1)>\u202e\x00\n'
    t.position("636019825", *WEST, T0)
    t.static("636019825", 80, evil, T0)
    (ship,) = t.snapshot(T0)
    assert isinstance(ship.name, str)
    assert len(ship.name) <= NAME_MAX
    assert all(" " <= c <= "_" for c in ship.name)  # AIS 6-bit printable only
    assert ship.name == '<SCRIPT>ALERT("X")</'  # inert text; the renderer escapes it


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("EXAMPLE TANKER@@@@@@", "EXAMPLE TANKER"),
        ("  padded   ", "PADDED"),
        ("x" * 10_000, "X" * NAME_MAX),
        ("\u0000\u0007bell", "BELL"),
        (12345, ""),
        (None, ""),
    ],
)
def test_clean_name(raw, expected):
    assert clean_name(raw) == expected


def test_bad_static_fields_ignored():
    t = live_tracker()
    t.position("636019825", *WEST, T0)
    t.static("636019825", "80", None, T0)
    t.static("636019825", True, ["A"], T0)
    t.static("636019825", 999, {"x": 1}, T0)
    (ship,) = t.snapshot(T0)
    assert ship.ship_type is None and ship.name == "" and not ship.tanker


def test_flood_of_ships_stays_bounded():
    t = live_tracker()
    for i in range(50_000):
        t.position(
            f"{201000000 + i}",
            26.0 + (i % 100) / 100,
            56.0 + (i % 97) / 100,
            T0 + timedelta(microseconds=i),
        )
        t.static(f"{201000000 + i}", 80, "<b>FLOOD</b>" * 100, T0 + timedelta(microseconds=i))
    assert len(t) == 5000
    snap = t.snapshot(T0 + MIN)
    assert len(snap) == 5000
    assert all(len(s.name) <= NAME_MAX for s in snap)
    v = values(t.close_slot(T0 + 10 * MIN))
    assert v[SERIES_SHIPS] == 5000 and v[SERIES_TANKERS] == 5000


def test_bbox_corners_order_for_aisstream():
    assert HORMUZ_BBOX.to_aisstream() == [[25.9, 55.9], [27.0, 57.0]]
    assert HORMUZ_BBOX.contains(*HORMUZ_LINE.a) and HORMUZ_BBOX.contains(*HORMUZ_LINE.b)
