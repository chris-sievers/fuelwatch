"""Pure, in-memory AIS tracker for the Strait of Hormuz.

No I/O and no clock: every method takes the time it should use, so tests
drive it with hand-built timestamps. The websocket client in
`aisstream.py` feeds it; the future map reads `snapshot()`.

Everything here treats its input as untrusted (anyone with a transmitter
can put anything on AIS):

* MMSIs must be 9-digit ship-station identities (MID 201-775); others are
  dropped.
* Positions outside the bounding box, or not finite, are dropped.
* Ship names are reduced to the AIS 6-bit printable set, at most 20
  characters, and stored as plain text. They are never markup; whoever
  renders them must insert them as text.
* The table holds at most `max_ships` ships; the least recently heard ship
  is evicted first, and ships unheard for `expiry` disappear.

Counting rules, per 10-minute slot [start, end):

* ships / tankers: distinct ships with an accepted in-area position
  received during the slot. Tankers are AIS ship types 80-89.
* crossings / tanker crossings: distinct ships whose track crossed the
  crossing line during the slot.
* A slot is emitted only if the stream was live for the whole slot. Any
  outage, or a start part-way through, means no points for that slot:
  never zeros.
"""

from __future__ import annotations

import math
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from fuelwatch.models import Point

SLOT = timedelta(minutes=10)
MAX_SHIPS = 5000
EXPIRY = timedelta(minutes=30)
NAME_MAX = 20  # AIS ship names are 20 six-bit characters
TANKER_TYPES = range(80, 90)
# A track segment implying more than this is a jump (spoofing, a bad fix or
# a reused MMSI), not a transit, so it never counts as a crossing.
MAX_PLAUSIBLE_KNOTS = 60.0
# Ignore a second crossing by the same ship within this window, so a ship
# drifting back and forth across the line is counted once.
CROSSING_COOLDOWN = timedelta(hours=1)

SERIES_SHIPS = "hormuz_ais_ships"
SERIES_TANKERS = "hormuz_ais_tankers"
SERIES_CROSSINGS = "hormuz_ais_crossings"
SERIES_TANKER_CROSSINGS = "hormuz_ais_tanker_crossings"


@dataclass(frozen=True)
class BBox:
    south: float
    west: float
    north: float
    east: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.south <= lat <= self.north and self.west <= lon <= self.east

    def to_aisstream(self) -> list[list[float]]:
        """AISStream wants [[lat, lon], [lat, lon]] corner pairs."""
        return [[self.south, self.west], [self.north, self.east]]


@dataclass(frozen=True)
class Line:
    """A segment between two (lat, lon) points."""

    a: tuple[float, float]
    b: tuple[float, float]


# Strait of Hormuz, from west of the Musandam peninsula / Qeshm to the Gulf of
# Oman approach. Excludes the Bandar Abbas anchorages (north of 27.0N) and the
# Fujairah/Khor Fakkan anchorages (south of 25.9N).
HORMUZ_BBOX = BBox(south=25.9, west=55.9, north=27.0, east=57.0)

# The narrows: Ras Musandam (Oman) to the south shore of Larak Island (Iran).
# Both traffic separation lanes pass between these points, so every transit
# crosses it once. Approximate; check against charted lanes with live data.
HORMUZ_LINE = Line(a=(26.39, 56.40), b=(26.83, 56.36))


@dataclass(frozen=True)
class ShipPosition:
    """One ship as the map sees it. Immutable; `name` is plain text."""

    mmsi: str
    lat: float
    lon: float
    sog: float | None
    cog: float | None
    heading: int | None
    ship_type: int | None
    tanker: bool
    name: str
    seen_at: datetime


@dataclass
class _Ship:
    last_heard: datetime
    lat: float | None = None
    lon: float | None = None
    pos_at: datetime | None = None
    sog: float | None = None
    cog: float | None = None
    heading: int | None = None
    ship_type: int | None = None
    name: str = ""
    last_crossing: datetime | None = None


# --- validation of untrusted fields ------------------------------------------------


def valid_mmsi(value: object) -> str | None:
    """Return the MMSI as a 9-digit string if it is a ship station, else None.

    Ship-station MMSIs are MIDxxxxxx with a maritime identification digit
    block of 201-775. That rules out coast stations (00x), group calls (0x),
    SAR aircraft (111), handhelds (8x), SARTs/EPIRBs/MOB (97x), aids to
    navigation (99x) and placeholders like 000000000 or 123456789.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        s = str(value)
    elif isinstance(value, str):
        s = value.strip()
    else:
        return None
    if len(s) != 9 or not s.isascii() or not s.isdigit():
        return None
    if not 201 <= int(s[:3]) <= 775:
        return None
    return s


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    f = float(value)
    return f if math.isfinite(f) else None


def clean_name(value: object) -> str:
    """Reduce an AIS name to plain text: AIS 6-bit printable characters only
    (space to underscore, upper case), trailing '@' padding and spaces
    removed, at most 20 characters."""
    if not isinstance(value, str):
        return ""
    kept = "".join(c for c in value[: NAME_MAX * 4].upper() if " " <= c <= "_")
    return kept.rstrip("@ ").strip()[:NAME_MAX].rstrip()


def is_tanker(ship_type: int | None) -> bool:
    return ship_type is not None and ship_type in TANKER_TYPES


# --- geometry ------------------------------------------------------------------------


def _orient(p: tuple[float, float], q: tuple[float, float], r: tuple[float, float]) -> float:
    return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])


def segments_cross(p1: tuple[float, float], p2: tuple[float, float], line: Line) -> bool:
    """True if the track p1->p2 properly crosses the line (touching an end
    point or running along it does not count). Planar lat/lon is fine at
    this scale."""
    d1 = _orient(line.a, line.b, p1)
    d2 = _orient(line.a, line.b, p2)
    d3 = _orient(p1, p2, line.a)
    d4 = _orient(p1, p2, line.b)
    return d1 * d2 < 0 and d3 * d4 < 0


def _nm(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    """Great-circle distance in nautical miles."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*p1, *p2))
    h = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * 3440.065 * math.asin(min(1.0, math.sqrt(h)))


def slot_start(t: datetime) -> datetime:
    return t.replace(minute=t.minute - t.minute % 10, second=0, microsecond=0)


# --- the tracker ---------------------------------------------------------------------


class Tracker:
    def __init__(
        self,
        bbox: BBox = HORMUZ_BBOX,
        line: Line | None = HORMUZ_LINE,
        max_ships: int = MAX_SHIPS,
        expiry: timedelta = EXPIRY,
    ) -> None:
        self.bbox = bbox
        self.line = line
        self.max_ships = max_ships
        self.expiry = expiry
        self._ships: OrderedDict[str, _Ship] = OrderedDict()  # least recently heard first
        self._crossings: dict[datetime, dict[str, None]] = {}
        self._live_since: datetime | None = None
        self._first_live: datetime | None = None
        self._outages: deque[tuple[datetime, datetime | None]] = deque(maxlen=1000)

    # connection state

    @property
    def live(self) -> bool:
        return self._live_since is not None

    def connected(self, now: datetime) -> None:
        """The stream is delivering data."""
        if self._live_since is not None:
            return
        self._live_since = now
        if self._first_live is None:
            self._first_live = now
        if self._outages and self._outages[-1][1] is None:
            self._outages[-1] = (self._outages[-1][0], now)

    def disconnected(self, now: datetime) -> None:
        if self._live_since is None:
            return
        self._live_since = None
        self._outages.append((now, None))

    def _covered(self, start: datetime, end: datetime) -> bool:
        if self._first_live is None or self._first_live > start:
            return False
        for down, up in self._outages:
            if down < end and (up is None or up > start):
                return False
        return True

    # ingest

    def _touch(self, mmsi: str, now: datetime) -> _Ship:
        ship = self._ships.get(mmsi)
        if ship is None:
            self._expire(now)
            while len(self._ships) >= self.max_ships:
                self._ships.popitem(last=False)
            ship = _Ship(last_heard=now)
            self._ships[mmsi] = ship
        else:
            ship.last_heard = now
            self._ships.move_to_end(mmsi)
        return ship

    def _expire(self, now: datetime) -> None:
        cutoff = now - self.expiry
        while self._ships:
            mmsi, ship = next(iter(self._ships.items()))
            if ship.last_heard > cutoff:
                break
            del self._ships[mmsi]

    def position(
        self,
        mmsi: object,
        lat: object,
        lon: object,
        now: datetime,
        sog: object = None,
        cog: object = None,
        heading: object = None,
    ) -> bool:
        """Apply a position report. Returns False if it was dropped."""
        key = valid_mmsi(mmsi)
        flat, flon = _finite(lat), _finite(lon)
        if key is None or flat is None or flon is None or not self.bbox.contains(flat, flon):
            return False
        ship = self._touch(key, now)
        prev = (ship.lat, ship.lon, ship.pos_at)
        ship.lat, ship.lon, ship.pos_at = flat, flon, now
        s = _finite(sog)
        ship.sog = s if s is not None and 0 <= s < 102.3 else None  # 102.3 = not available
        c = _finite(cog)
        ship.cog = c if c is not None and 0 <= c < 360 else None  # 360 = not available
        ship.heading = heading if isinstance(heading, int) and 0 <= heading < 360 else None
        if prev[2] is not None:
            self._maybe_cross(key, ship, (prev[0], prev[1]), prev[2], now)
        return True

    def static(self, mmsi: object, ship_type: object, name: object, now: datetime) -> bool:
        """Apply ship static data. Returns False if it was dropped."""
        key = valid_mmsi(mmsi)
        if key is None:
            return False
        ship = self._touch(key, now)
        if isinstance(ship_type, int) and not isinstance(ship_type, bool) and 0 <= ship_type <= 99:
            ship.ship_type = ship_type
        cleaned = clean_name(name)
        if cleaned:
            ship.name = cleaned
        return True

    def _maybe_cross(
        self, key: str, ship: _Ship, before: tuple, before_at: datetime, now: datetime
    ) -> None:
        if self.line is None or now - before_at > self.expiry:
            return
        after = (ship.lat, ship.lon)
        if not segments_cross(before, after, self.line):
            return
        hours = (now - before_at).total_seconds() / 3600
        if hours <= 0 or _nm(before, after) / hours > MAX_PLAUSIBLE_KNOTS:
            return
        if ship.last_crossing is not None and now - ship.last_crossing < CROSSING_COOLDOWN:
            return
        ship.last_crossing = now
        bucket = self._crossings.setdefault(slot_start(now), {})
        if len(bucket) < self.max_ships:
            bucket[key] = None

    # output

    def close_slot(self, slot_end: datetime) -> list[Point]:
        """Points for the slot ending at `slot_end`, or [] if the stream was
        not live for all of it. Call once per slot, at or just after its end."""
        if slot_start(slot_end) != slot_end:
            raise ValueError("slot_end must fall on a 10-minute boundary")
        start = slot_end - SLOT
        crossed = self._crossings.pop(start, {})
        for old in [s for s in self._crossings if s < start]:
            del self._crossings[old]
        if not self._covered(start, slot_end):
            return []
        ships = tankers = 0
        for ship in self._ships.values():
            if ship.pos_at is not None and start <= ship.pos_at:
                ships += 1
                tankers += is_tanker(ship.ship_type)
        tanker_crossings = 0
        for mmsi in crossed:
            ship = self._ships.get(mmsi)
            tanker_crossings += ship is not None and is_tanker(ship.ship_type)
        return [
            Point(SERIES_SHIPS, slot_end, float(ships)),
            Point(SERIES_TANKERS, slot_end, float(tankers)),
            Point(SERIES_CROSSINGS, slot_end, float(len(crossed))),
            Point(SERIES_TANKER_CROSSINGS, slot_end, float(tanker_crossings)),
        ]

    def snapshot(self, now: datetime) -> tuple[ShipPosition, ...]:
        """Ships with a position heard within the expiry window."""
        self._expire(now)
        cutoff = now - self.expiry
        return tuple(
            ShipPosition(
                mmsi=mmsi,
                lat=s.lat,
                lon=s.lon,
                sog=s.sog,
                cog=s.cog,
                heading=s.heading,
                ship_type=s.ship_type,
                tanker=is_tanker(s.ship_type),
                name=s.name,
                seen_at=s.pos_at,
            )
            for mmsi, s in self._ships.items()
            if s.pos_at is not None and s.pos_at > cutoff
        )

    def __len__(self) -> int:
        return len(self._ships)
