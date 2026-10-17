"""Demo mode: FW_DEMO=1 runs the app on made-up data, for self-hosters
trying Fuelwatch and for the UI tests.

The app seeds a throwaway store (a temporary directory, removed on
shutdown) with deterministic synthetic data for every registered series and
runs no collector, so no API key is needed and nothing calls a provider.
The page shows a "Demo data" banner.

The data is plausible in shape but clearly not real. It includes what the
UI must handle:

- a jump the day after the reference date ("the war began"), with several
  zero-traffic days at Hormuz;
- the two newest PortWatch days marked provisional;
- a late source: the EU bulletin stops two releases short of what is due;
- holiday gaps: EIA spot prices skip weekends and US federal holidays;
- live AIS counts every 10 minutes for the last seven days.

Each series ends where its publication schedule says the newest release
would leave it at ``now``, so everything else reads as on time.

FW_DEMO_NOW (ISO instant, e.g. 2026-10-17T12:00:00Z) fixes "now" for both
the data and the app's clock, so screenshots are repeatable. Without it the
data is built up to the current time when the app starts; a demo left
running for days goes late, as a real copy would with no collectors.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import shutil
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from fuelwatch import calc
from fuelwatch.collectors import eu_oil_bulletin, portwatch
from fuelwatch.collectors.ais_tracker import (
    SERIES_CROSSINGS,
    SERIES_SHIPS,
    SERIES_TANKER_CROSSINGS,
    SERIES_TANKERS,
)
from fuelwatch.collectors.aisstream import AisStatus
from fuelwatch.models import Point
from fuelwatch.registry import REGISTRY, Registry, is_business_day, releases
from fuelwatch.store import BackfillState, Store

ENV_DEMO = "FW_DEMO"
ENV_NOW = "FW_DEMO_NOW"
HISTORY_DAYS = 6 * 365  # five years of charts plus a year for the typical ranges
AIS_DAYS = 7
LATE_COLLECTOR = eu_oil_bulletin.SOURCE  # the demo's late source, by SeriesSpec.source
SLOT = timedelta(minutes=10)

# Demo event markers: plainly labelled as made up, with placeholder links.
EVENTS: tuple[dict, ...] = (
    {
        "date": "2025-11-20",
        "label": "Demo marker: talks break down",
        "url": "https://example.org/demo/1",
    },
    {
        "date": "2026-02-28",
        "label": "Demo marker: fighting begins",
        "url": "https://example.org/demo/2",
    },
    {
        "date": "2026-04-15",
        "label": "Demo marker: first escorted convoy",
        "url": "https://example.org/demo/3",
    },
    {
        "date": "2026-07-01",
        "label": "Demo marker: emergency stocks released",
        "url": "https://example.org/demo/4",
    },
)


def parse_now(raw: str | None) -> datetime | None:
    """FW_DEMO_NOW as an aware UTC datetime, or None when unset."""
    if raw is None or not raw.strip():
        return None
    try:
        t = datetime.fromisoformat(raw.strip())
    except ValueError:
        raise ValueError(f"{ENV_NOW} must be an ISO date and time, got {raw!r}") from None
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(UTC)


def _midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


class _Noise:
    """Deterministic mean-reverting noise per name: the same inputs always
    give the same series."""

    def __init__(self, name: str, start: date, days: int, sigma: float, pull: float = 0.08):
        rnd = random.Random(f"fuelwatch-demo:{name}")
        x, out = 0.0, []
        for _ in range(days + 1):
            x += rnd.gauss(0, sigma) - pull * x
            out.append(x)
        self.start = start
        self.values = out

    def __call__(self, d: date) -> float:
        i = (d - self.start).days
        return self.values[min(max(i, 0), len(self.values) - 1)]


class _World:
    """The made-up drivers every series is drawn from."""

    def __init__(self, start: date, end: date, war: date) -> None:
        n = (end - start).days + 7
        self.start, self.war = start, war
        self.n_brent = _Noise("brent", start, n, 0.9)
        self.n_gap = _Noise("gap", start, n, 0.25)
        self.n_gas = _Noise("gas", start, n, 0.02)
        self.n_dsl = _Noise("dsl", start, n, 0.02)
        self.n_ships = _Noise("ships", start, n, 4.0, pull=0.5)

    def k(self, d: date) -> int:
        """Days since the war began, negative before."""
        return (d - self.war).days

    def shock(self, d: date, size: float, rise: float, fade: float, keep: float) -> float:
        """0 before the war; rises to `size` over `rise` days, then fades to
        `keep` of it over `fade` days."""
        k = self.k(d)
        if k < 0:
            return 0.0
        up = 1 - math.exp(-k / rise)
        return size * up * (keep + (1 - keep) * math.exp(-k / fade))

    def season(self, d: date, peak_doy: int) -> float:
        return math.cos(2 * math.pi * (d.timetuple().tm_yday - peak_doy) / 365.25)

    def brent(self, d: date) -> float:
        t = (d - self.start).days
        return 71 + 5 * math.sin(t / 70) + self.n_brent(d) + self.shock(d, 38, 4, 150, 0.6)

    def gap(self, d: date) -> float:
        return 3.8 + self.n_gap(d) + self.shock(d, 6, 8, 200, 0.7)

    def nyh_gasoline(self, d: date) -> float:
        return self.brent(d) / 42 + 0.50 + 0.12 * self.season(d, 170) + self.n_gas(d)

    def nyh_diesel(self, d: date) -> float:
        return self.brent(d) / 42 + 0.80 + 0.08 * self.season(d, 20) + self.n_dsl(d)


_REGION_OFFSETS = {
    "us": 0.0,
    "padd1": -0.04,
    "padd2": -0.09,
    "padd3": -0.33,
    "padd4": 0.06,
    "padd5": 0.78,
    "ca": 1.37,
}
_EU_BASE = {"eu27": 0.0, "de": 0.08, "fr": 0.12, "it": 0.17, "es": -0.12}
# Chokepoint: (ships a day before, after the war, tanker share)
_CHOKEPOINTS = {
    "hormuz": (104.0, 0.0, 0.46),
    "bab_el_mandeb": (31.0, 21.0, 0.32),
    "suez": (37.0, 27.0, 0.30),
    "cape_of_good_hope": (82.0, 108.0, 0.36),
    "malacca": (228.0, 234.0, 0.34),
}


def _hormuz_ships(w: _World, d: date) -> float:
    """Before: about 104 a day. Then a near stop with zero days, recovering
    to about a quarter of normal."""
    k = w.k(d)
    if k < 0:
        return 104 + w.n_ships(d)
    if k < 16:
        return 0.0 if k % 5 else 6.0 + (k % 3)
    return max(0.0, 8 + 20 * (1 - math.exp(-(k - 16) / 60)) + w.n_ships(d) / 2)


def _ship_count(w: _World, key: str, d: date) -> float:
    if key == "hormuz":
        return round(_hormuz_ships(w, d))
    before, after, _ = _CHOKEPOINTS[key]
    k = w.k(d)
    level = before if k < 0 else before + (after - before) * (1 - math.exp(-k / 20))
    wobble = 0.05 * level * math.sin((d - w.start).days * 0.9 + len(key))
    return round(max(0.0, level + wobble + w.n_ships(d) * level / 100))


def _due(entry, now: datetime) -> list:
    return [r for r in releases(entry.schedule, now, weeks_back=4, weeks_ahead=0) if r.at <= now]


def _last_period(entry, now: datetime) -> date:
    due = _due(entry, now)
    if entry.spec.source == LATE_COLLECTOR:
        return due[-3].period  # two releases missed: late
    return due[-1].period


def _value(w: _World, sid: str, d: date) -> float | None:
    if sid == calc.BRENT:
        return round(w.brent(d), 2)
    if sid == calc.WTI:
        return round(w.brent(d) - w.gap(d), 2)
    if sid == "eia.spot.nyh_gasoline":
        return round(w.nyh_gasoline(d), 3)
    if sid == "eia.spot.nyh_ulsd":
        return round(w.nyh_diesel(d), 3)
    if sid.startswith("eia.retail."):
        _, _, fuel, region = sid.split(".")
        lag = d - timedelta(days=10)
        if fuel == "gasoline":
            return round(w.nyh_gasoline(lag) + 1.02 + _REGION_OFFSETS[region], 3)
        return round(w.nyh_diesel(lag) + 1.20 + 0.8 * _REGION_OFFSETS[region], 3)
    if sid == calc.CRUDE_STOCKS:
        return round(432 + 16 * w.season(d, 120) + 3 * w.n_brent(d) - w.shock(d, 34, 50, 999, 1), 1)
    if sid == "eia.stocks.gasoline":
        return round(231 + 11 * w.season(d, 40) + w.n_brent(d) - w.shock(d, 14, 40, 999, 1), 1)
    if sid == "eia.stocks.distillate":
        return round(127 + 8 * w.season(d, 300) + w.n_gap(d) * 3 - w.shock(d, 15, 40, 999, 1), 1)
    if sid == "eia.stocks.spr":
        return round(398 + 0.4 * w.n_gap(d) - w.shock(d, 42, 90, 999, 1), 1)
    if sid == "eia.refinery.utilization":
        return round(min(96.5, 89.5 + 4 * w.season(d, 200) + w.n_gap(d)), 1)
    if sid.startswith("portwatch."):
        _, key, measure = sid.split(".")
        ships = _ship_count(w, key, d)
        tankers = round(ships * _CHOKEPOINTS[key][2])
        if measure == portwatch.ALL_SHIPS.suffix:
            return float(ships)
        if measure == portwatch.TANKERS.suffix:
            return float(tankers)
        return float(round(tankers * (104_000 + 9_000 * math.sin((d - w.start).days))))
    if sid.startswith("eu_pump_"):
        _, _, fuel, area = sid.split("_")
        lag = d - timedelta(days=14)
        crude = (w.brent(lag) - 71) / 159 * 1.1  # € a litre, roughly
        base = 1.66 if fuel == "petrol" else 1.58
        return round(base + _EU_BASE[area] + crude, 3)
    return None


def _days(start: date, end: date, keep: Callable[[date], bool]) -> list[date]:
    out, d = [], start
    while d <= end:
        if keep(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _ais_points(now: datetime) -> list[Point]:
    rnd = random.Random("fuelwatch-demo:ais")
    end = datetime.fromtimestamp((int(now.timestamp()) // 600) * 600, UTC)
    t = end - timedelta(days=AIS_DAYS) + SLOT
    pts = []
    while t <= end:
        hour = t.hour + t.minute / 60
        ships = round(27 + 5 * math.sin(2 * math.pi * hour / 24) + rnd.gauss(0, 2))
        tankers = round(ships * 0.3 + rnd.gauss(0, 1))
        cross = 1 if rnd.random() < 0.18 else 0
        tcross = cross if rnd.random() < 0.4 else 0
        pts += [
            Point(SERIES_SHIPS, t, float(max(ships, 0))),
            Point(SERIES_TANKERS, t, float(max(tankers, 0))),
            Point(SERIES_CROSSINGS, t, float(cross)),
            Point(SERIES_TANKER_CROSSINGS, t, float(tcross)),
        ]
        t += SLOT
    return pts


def seed(store: Store, now: datetime, registry: Registry = REGISTRY) -> int:
    """Fill `store` with demo data up to `now`; returns the point count."""
    settings = store.settings()
    start = now.date() - timedelta(days=HISTORY_DAYS)
    world = _World(start, now.date(), settings.reference_date + timedelta(days=1))
    pts: list[Point] = []
    for entry in registry:
        sid = entry.id
        if entry.collector == "aisstream":
            continue
        last = _last_period(entry, now)
        if sid.startswith("eia.spot."):
            days = _days(start, last, is_business_day)  # weekends and holidays are gaps
        elif entry.spec.frequency.value == "daily":
            days = _days(start, last, lambda d: True)
        else:
            weekday = last.weekday()  # weekly: the same weekday as the newest period
            days = _days(start, last, lambda d, wd=weekday: d.weekday() == wd)
        newest = {last, last - timedelta(days=1)}
        for d in days:
            v = _value(world, sid, d)
            if v is None:
                continue
            prov = entry.collector == "portwatch" and d in newest
            pts.append(Point(sid, _midnight(d), v, prov))
    pts += _ais_points(now)
    store.upsert_points(pts, fetched_at=now)
    checked = now - timedelta(minutes=25)
    for c in registry.polled:
        store.set_backfill(c.name, BackfillState.DONE, None, 1.0, at=checked)
        store.record_success(c.name, at=checked)
    return len(pts)


class DemoStore(Store):
    """A Store in its own temporary directory, removed on close."""

    def __init__(self, now: datetime, registry: Registry = REGISTRY) -> None:
        self._tmp = Path(tempfile.mkdtemp(prefix="fuelwatch-demo-"))
        super().__init__(self._tmp / "demo.db")
        seed(self, now, registry)

    def close(self) -> None:
        try:
            super().close()
        finally:
            shutil.rmtree(self._tmp, ignore_errors=True)


class DemoAis:
    """Stands in for the live AIS collector: connected, nothing to run."""

    def __init__(self, now: datetime) -> None:
        self.status = AisStatus(connected=True, last_message_at=now)


class DemoScheduler:
    """Runs no collector; waits for shutdown. Exposes the fake live AIS."""

    collections = 0

    def __init__(self, store: Store, now: datetime) -> None:
        self.store = store
        self.ais = DemoAis(now)

    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


def enabled() -> bool:
    raw = os.environ.get(ENV_DEMO, "").strip()
    if raw not in ("", "0", "1"):
        raise ValueError(f"{ENV_DEMO} must be 0 or 1, got {raw!r}")
    return raw == "1"
