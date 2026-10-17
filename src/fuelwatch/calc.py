"""Calculated numbers for the dashboard: pure functions over a Store.

Every function takes the store plus an explicit ``now`` (timezone-aware UTC;
nothing newer than ``now`` is used) and, where a number depends on them, the
settings. Nothing reads the clock or the settings table itself, so the same
inputs always give the same answer.

Every result carries
    ``as_of``: the period date of the newest point used;
    ``settings``: the settings the number depends on, for the label beside it;
    ``reason``: None when the number is available, otherwise a plain-English
    reason and ``value`` is None. Missing data is never reported as zero.

Time model (see the spec's Data rules)
    Points sit at UTC midnight of their period date, which is the provider's
    own date and is never converted: the trading day for EIA spot prices, the
    Monday survey date for EIA retail and the EU bulletin, the week-ending
    Friday for EIA stocks, the day for PortWatch. "The value on a date" is the
    last observation on or before it (``Store.value_on``), which lines daily
    and weekly series up on the reference date even when it is a weekend, a
    holiday or falls between two weekly period dates.

PortWatch
    The two newest days of each publication are stored provisional and are
    left out of every "vs normal" number (``settled_only``). Zeros are real
    data and are averaged like any other value.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from fuelwatch.collectors import eia, eu_oil_bulletin, portwatch
from fuelwatch.models import Point
from fuelwatch.store import Settings, Store

# --- series and constants -------------------------------------------------------------

BRENT = "eia.spot.brent"
WTI = "eia.spot.wti"
CRUDE_STOCKS = "eia.stocks.crude_commercial"
HORMUZ = portwatch.series_id(portwatch.CHOKEPOINTS[0], portwatch.ALL_SHIPS)
CHOKEPOINT_SERIES: dict[str, str] = {
    cp.key: portwatch.series_id(cp, portwatch.ALL_SHIPS) for cp in portwatch.CHOKEPOINTS
}
REGIONS = ("us", "padd1", "padd2", "padd3", "padd4", "padd5", "ca")
REGION_NAMES = {
    "us": "US",
    "padd1": "East Coast",
    "padd2": "Midwest",
    "padd3": "Gulf Coast",
    "padd4": "Rocky Mountain",
    "padd5": "West Coast",
    "ca": "California",
}
WEEKLY_STOCKS = (
    "eia.stocks.crude_commercial",
    "eia.stocks.gasoline",
    "eia.stocks.distillate",
    "eia.stocks.spr",
)


def gasoline(region: str = "us") -> str:
    return f"eia.retail.gasoline.{region}"


def diesel(region: str = "us") -> str:
    return f"eia.retail.diesel.{region}"


GASOLINE_US = gasoline("us")
DIESEL_US = diesel("us")

UNITS: dict[str, str] = {
    s.id: s.unit for s in (*eia.SERIES, *portwatch.SERIES, *eu_oil_bulletin.SERIES)
}

GALLONS_PER_BARREL = 42
CRUDE_LAG = timedelta(days=14)  # Brent this far before the pump price date
CRUDE_MAX_AGE = timedelta(days=7)  # an older Brent reading than this is a gap
RECENT_DAYS = 7  # the "now" side of percent of normal
RECENT_MIN_DAYS = 5  # days with data needed in the 7-day window
BASELINE_MIN_SHARE = 0.5  # share of baseline days that must have data
RANGE_YEARS = 5
RANGE_MIN_YEARS = 3  # years with data for the week needed for a typical range
SPARKLINE_DAYS = 90
FLAT_PERCENT = 0.5  # a change smaller than this reads as "little changed"
_LOOKBACK = timedelta(days=120)  # first try for "newest points before now"


# --- result types ---------------------------------------------------------------------


@dataclass(frozen=True)
class DatedValue:
    day: date
    value: float


@dataclass(frozen=True, kw_only=True)
class Result:
    """A number, or None with a reason. ``estimate`` marks a rough figure."""

    value: float | None
    unit: str
    as_of: date | None
    settings: dict[str, object] = field(default_factory=dict)
    reason: str | None = None
    estimate: bool = False

    @property
    def available(self) -> bool:
        return self.value is not None


@dataclass(frozen=True, kw_only=True)
class Change(Result):
    """``value`` is current minus base, in the series unit. ``percent`` is
    None when the base is zero (a percent change from zero has no meaning)."""

    percent: float | None = None
    current: DatedValue | None = None
    base: DatedValue | None = None


@dataclass(frozen=True, kw_only=True)
class PercentOfNormal(Result):
    """``value`` is the recent average as a percent of the baseline average."""

    recent_average: float | None = None
    recent_start: date | None = None
    recent_end: date | None = None
    recent_days: int = 0
    baseline_average: float | None = None
    baseline_start: date | None = None
    baseline_end: date | None = None
    baseline_days_with_data: int = 0


@dataclass(frozen=True, kw_only=True)
class SeriesResult(Result):
    """A derived series, oldest first; ``value`` is its newest value."""

    points: tuple[DatedValue, ...] = ()


@dataclass(frozen=True, kw_only=True)
class TankCost(Result):
    """``value`` is the cost to fill now; ``then`` the cost on the reference date."""

    price: DatedValue | None = None
    then: float | None = None
    then_as_of: date | None = None
    change: float | None = None
    percent: float | None = None


@dataclass(frozen=True, kw_only=True)
class CrudeShare(Result):
    """``value`` is crude's share of the pump price, in percent. Always an estimate."""

    pump: DatedValue | None = None
    brent: DatedValue | None = None
    brent_target: date | None = None
    estimate: bool = True


@dataclass(frozen=True, kw_only=True)
class TypicalRange(Result):
    """``value`` is the reading the range is for; ``position`` is "below",
    "within" or "above" the range (None when the range is not available)."""

    low: float | None = None
    high: float | None = None
    week: int | None = None
    years: tuple[int, ...] = ()
    position: str | None = None


@dataclass(frozen=True)
class RangePoint:
    day: date
    value: float
    low: float | None
    high: float | None


@dataclass(frozen=True, kw_only=True)
class Tile(Result):
    key: str
    label: str
    since_last: Change
    since_war: Change
    sparkline: tuple[DatedValue, ...] = ()
    typical_range: TypicalRange | None = None


@dataclass(frozen=True)
class Summary:
    text: str
    tiles: tuple[Tile, ...]
    as_of: date | None
    settings: dict[str, object]


# --- helpers --------------------------------------------------------------------------


def _check_now(now: datetime) -> None:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("now must be a timezone-aware UTC datetime")


def _dv(p: Point) -> DatedValue:
    return DatedValue(p.period_end.date(), p.value)


def _ref(settings: Settings) -> dict[str, object]:
    return {"reference_date": settings.reference_date}


def _recent(
    store: Store, series: str, n: int, now: datetime, *, settled_only: bool = False
) -> list[DatedValue]:
    """The newest n points on or before now, oldest first."""
    pts = store.points(series, now - _LOOKBACK, now, settled_only=settled_only)
    if len(pts) < n:
        pts = store.points(series, None, now, settled_only=settled_only)
    return [_dv(p) for p in pts[-n:]]


def _on(store: Store, series: str, on: date | datetime) -> DatedValue | None:
    p = store.value_on(series, on)
    return _dv(p) if p else None


def _change(
    cur: DatedValue | None,
    base: DatedValue | None,
    *,
    unit: str,
    settings: dict[str, object],
    no_current: str,
    no_base: str,
) -> Change:
    if cur is None:
        return Change(value=None, unit=unit, as_of=None, settings=settings, reason=no_current)
    if base is None:
        return Change(
            value=None, unit=unit, as_of=cur.day, settings=settings, reason=no_base, current=cur
        )
    diff = cur.value - base.value
    pct = None if base.value == 0 else diff / abs(base.value) * 100
    return Change(
        value=diff,
        percent=pct,
        unit=unit,
        as_of=cur.day,
        settings=settings,
        current=cur,
        base=base,
    )


def _no_data(series: str) -> str:
    return f"no data yet for {series}"


# --- change since the war began, change since the last reading -------------------------


def change_since_war(
    store: Store, series: str, *, now: datetime, settings: Settings, settled_only: bool = False
) -> Change:
    """Latest value on or before ``now`` against the last observation on or
    before the reference date."""
    _check_now(now)
    cur = _recent(store, series, 1, now, settled_only=settled_only)
    base = _on(store, series, settings.reference_date)
    return _change(
        cur[-1] if cur else None,
        base,
        unit=UNITS.get(series, ""),
        settings=_ref(settings),
        no_current=_no_data(series),
        no_base=f"no reading on or before the reference date {settings.reference_date}",
    )


def change_since_last(
    store: Store, series: str, *, now: datetime, settled_only: bool = False
) -> Change:
    """Latest value on or before ``now`` against the reading before it."""
    _check_now(now)
    pts = _recent(store, series, 2, now, settled_only=settled_only)
    return _change(
        pts[-1] if pts else None,
        pts[-2] if len(pts) == 2 else None,
        unit=UNITS.get(series, ""),
        settings={},
        no_current=_no_data(series),
        no_base="only one reading so far",
    )


# --- percent of normal (Hormuz and the other chokepoints) ------------------------------


def _days(store: Store, series: str, start: date | None, end: date | datetime) -> dict[date, float]:
    return {
        p.period_end.date(): p.value for p in store.points(series, start, end, settled_only=True)
    }


def _baseline(
    store: Store, series: str, settings: Settings
) -> tuple[float | None, date, date, int, str | None]:
    """Average of the settled days in the ``baseline_days`` days ending on
    (and including) the reference date."""
    end = settings.reference_date
    start = end - timedelta(days=settings.baseline_days - 1)
    vals = list(_days(store, series, start, end).values())
    need = math.ceil(settings.baseline_days * BASELINE_MIN_SHARE)
    if len(vals) < need:
        why = (
            f"only {len(vals)} of the {settings.baseline_days} days before the reference "
            f"date have data (need {need})"
        )
        return None, start, end, len(vals), why
    avg = sum(vals) / len(vals)
    if avg == 0:
        return None, start, end, len(vals), "the normal level is zero"
    return avg, start, end, len(vals), None


def _trailing(days: dict[date, float], end: date) -> tuple[float | None, int]:
    vals = [v for i in range(RECENT_DAYS) if (v := days.get(end - timedelta(days=i))) is not None]
    if len(vals) < RECENT_MIN_DAYS:
        return None, len(vals)
    return sum(vals) / len(vals), len(vals)


def _pon_settings(settings: Settings) -> dict[str, object]:
    return {"reference_date": settings.reference_date, "baseline_days": settings.baseline_days}


def percent_of_normal(
    store: Store, series: str = HORMUZ, *, now: datetime, settings: Settings
) -> PercentOfNormal:
    """The 7-day average of the newest settled days (the 7 calendar days
    ending on the newest settled day, gaps left out) as a percent of the
    ``baseline_days`` average up to the reference date. Provisional days are
    excluded; zeros count."""
    _check_now(now)
    unit, cfg = "% of normal", _pon_settings(settings)
    base, b_start, b_end, b_n, why = _baseline(store, series, settings)
    newest = _recent(store, series, 1, now, settled_only=True)
    common = {
        "unit": unit,
        "settings": cfg,
        "baseline_average": base,
        "baseline_start": b_start,
        "baseline_end": b_end,
        "baseline_days_with_data": b_n,
    }
    if not newest:
        return PercentOfNormal(value=None, as_of=None, reason=_no_data(series), **common)
    end = newest[-1].day
    r_start = end - timedelta(days=RECENT_DAYS - 1)
    recent, n = _trailing(_days(store, series, r_start, end), end)
    common |= {
        "recent_average": recent,
        "recent_start": r_start,
        "recent_end": end,
        "recent_days": n,
    }
    if recent is None:
        why = f"only {n} of the last {RECENT_DAYS} settled days have data"
    if why:
        return PercentOfNormal(value=None, as_of=end, reason=why, **common)
    return PercentOfNormal(value=recent / base * 100, as_of=end, **common)


def percent_of_normal_series(
    store: Store,
    series: str = HORMUZ,
    *,
    now: datetime,
    settings: Settings,
    start: date | None = None,
) -> SeriesResult:
    """For each settled day from ``start`` (default: 90 days before the
    newest settled day), the trailing 7-day average as a percent of normal.
    For the Hormuz chart line and the small chokepoint charts."""
    _check_now(now)
    unit, cfg = "% of normal", _pon_settings(settings)
    base, *_, why = _baseline(store, series, settings)
    if base is None:
        return SeriesResult(value=None, unit=unit, as_of=None, settings=cfg, reason=why)
    newest = _recent(store, series, 1, now, settled_only=True)
    if not newest:
        return SeriesResult(
            value=None, unit=unit, as_of=None, settings=cfg, reason=_no_data(series)
        )
    end = newest[-1].day
    if start is None:
        start = end - timedelta(days=SPARKLINE_DAYS - 1)
    days = _days(store, series, start - timedelta(days=RECENT_DAYS - 1), end)
    out = []
    for d in sorted(days):
        if d < start:
            continue
        avg, _ = _trailing(days, d)
        if avg is not None:
            out.append(DatedValue(d, avg / base * 100))
    if not out:
        return SeriesResult(
            value=None, unit=unit, as_of=end, settings=cfg, reason="not enough recent days"
        )
    return SeriesResult(value=out[-1].value, unit=unit, as_of=end, settings=cfg, points=tuple(out))


def chokepoints_vs_normal(
    store: Store, *, now: datetime, settings: Settings, start: date | None = None
) -> dict[str, SeriesResult]:
    """Percent-of-normal series for every tracked chokepoint, all ships."""
    return {
        key: percent_of_normal_series(store, sid, now=now, settings=settings, start=start)
        for key, sid in CHOKEPOINT_SERIES.items()
    }


# --- cost to fill a tank --------------------------------------------------------------


def tank_cost(
    store: Store, series: str = GASOLINE_US, *, now: datetime, settings: Settings
) -> TankCost:
    """Pump price times ``tank_gallons``, now and on the reference date."""
    _check_now(now)
    g = settings.tank_gallons
    cfg = {"tank_gallons": g, "reference_date": settings.reference_date}
    cur = _recent(store, series, 1, now)
    if not cur:
        return TankCost(value=None, unit="$", as_of=None, settings=cfg, reason=_no_data(series))
    price = cur[-1]
    then = _on(store, series, settings.reference_date)
    common = {
        "value": price.value * g,
        "unit": "$",
        "as_of": price.day,
        "settings": cfg,
        "price": price,
    }
    if then is None:
        return TankCost(**common)
    diff = (price.value - then.value) * g
    return TankCost(
        **common,
        then=then.value * g,
        then_as_of=then.day,
        change=diff,
        percent=diff / (then.value * g) * 100,
    )


# --- paired series: Brent-WTI gap and diesel premium ----------------------------------


def _paired(
    store: Store,
    a: str,
    b: str,
    start: date | datetime | None,
    end: date | datetime,
    fn: Callable[[float, float], float],
) -> list[DatedValue]:
    """fn(a, b) on each date both series have a reading; other dates are gaps."""
    bv = {p.period_end.date(): p.value for p in store.points(b, start, end)}
    return [
        DatedValue(d, fn(p.value, bv[d]))
        for p in store.points(a, start, end)
        if (d := p.period_end.date()) in bv
    ]


def _latest_paired(
    store: Store,
    a: str,
    b: str,
    now: datetime,
    fn: Callable[[float, float], float],
    unit: str,
) -> Result:
    _check_now(now)
    pts = _paired(store, a, b, now - _LOOKBACK, now, fn) or _paired(store, a, b, None, now, fn)
    if not pts:
        return Result(value=None, unit=unit, as_of=None, reason=f"no date with both {a} and {b}")
    return Result(value=pts[-1].value, unit=unit, as_of=pts[-1].day)


def _gap(brent: float, wti: float) -> float:
    return brent - wti


def brent_wti_gap_series(
    store: Store, *, start: date | datetime | None = None, end: date | datetime
) -> list[DatedValue]:
    """Brent minus WTI in $/bbl on each trading day both have a price."""
    return _paired(store, BRENT, WTI, start, end, _gap)


def brent_wti_gap(store: Store, *, now: datetime) -> Result:
    """The newest Brent minus WTI, $/bbl, from the newest day both have a price."""
    return _latest_paired(store, BRENT, WTI, now, _gap, "$/bbl")


def _premium(dsl: float, gas: float) -> float:
    return (dsl - gas) * 100


def diesel_premium_series(
    store: Store, region: str = "us", *, start: date | datetime | None = None, end: date | datetime
) -> list[DatedValue]:
    """Retail diesel minus regular gasoline, cents per gallon, per survey date."""
    return _paired(store, diesel(region), gasoline(region), start, end, _premium)


def diesel_premium(store: Store, region: str = "us", *, now: datetime) -> Result:
    """The newest retail diesel minus gasoline, cents per gallon."""
    return _latest_paired(store, diesel(region), gasoline(region), now, _premium, "cents/gal")


def diesel_premiums(store: Store, *, now: datetime) -> dict[str, Result]:
    return {r: diesel_premium(store, r, now=now) for r in REGIONS}


# --- crude's share of the pump price --------------------------------------------------


def crude_share(store: Store, pump_series: str = GASOLINE_US, *, now: datetime) -> CrudeShare:
    """Brent on the day two weeks before the newest pump price date (the
    last trading day on or before it), divided by 42 gallons per barrel, as a
    percent of that pump price. A Brent reading more than a week older than
    the target day counts as missing."""
    _check_now(now)
    unit = "%"
    cur = _recent(store, pump_series, 1, now)
    if not cur:
        return CrudeShare(value=None, unit=unit, as_of=None, reason=_no_data(pump_series))
    pump = cur[-1]
    target = pump.day - CRUDE_LAG
    brent = _on(store, BRENT, target)
    common = {"unit": unit, "as_of": pump.day, "pump": pump, "brent": brent, "brent_target": target}
    if brent is None or target - brent.day > CRUDE_MAX_AGE:
        return CrudeShare(
            value=None, reason=f"no Brent price in the week up to {target.isoformat()}", **common
        )
    return CrudeShare(value=brent.value / GALLONS_PER_BARREL / pump.value * 100, **common)


# --- typical range --------------------------------------------------------------------


def week_of_year(d: date) -> tuple[int, int]:
    """(ISO year, ISO week) of a period date, with week 53 folded into 52.
    EIA stocks are dated by the week-ending Friday, and an ISO week runs
    Monday to Sunday, so each Friday falls in exactly one ISO week."""
    year, week, _ = d.isocalendar()
    return year, min(week, 52)


def _by_week(points: Iterable[Point]) -> dict[tuple[int, int], list[float]]:
    out: dict[tuple[int, int], list[float]] = defaultdict(list)
    for p in points:
        out[week_of_year(p.period_end.date())].append(p.value)
    return out


def _range_for(
    weeks: dict[tuple[int, int], list[float]], d: date
) -> tuple[float | None, float | None, int, tuple[int, ...]]:
    year, week = week_of_year(d)
    years = tuple(y for y in range(year - RANGE_YEARS, year) if weeks.get((y, week)))
    if len(years) < RANGE_MIN_YEARS:
        return None, None, week, years
    vals = [v for y in years for v in weeks[(y, week)]]
    return min(vals), max(vals), week, years


def _range_start(d: date) -> date:
    # Dec 1 of the ISO year before the oldest year counted: its week 1 can
    # start in late December.
    return date(d.isocalendar()[0] - RANGE_YEARS - 1, 12, 1)


def typical_range(
    store: Store, series: str = CRUDE_STOCKS, *, now: datetime, on: date | None = None
) -> TypicalRange:
    """Lowest and highest value for the same week of the year over the five
    previous ISO years, for the newest reading (or the reading on ``on``).
    A year with no reading for that week is skipped; at least
    ``RANGE_MIN_YEARS`` years are needed."""
    _check_now(now)
    unit = UNITS.get(series, "")
    cfg = {"years": RANGE_YEARS}
    if on is None:
        cur = _recent(store, series, 1, now)
        p = cur[-1] if cur else None
    else:
        p = _on(store, series, min(on, now.date()))
    if p is None:
        return TypicalRange(
            value=None, unit=unit, as_of=None, settings=cfg, reason=_no_data(series)
        )
    weeks = _by_week(store.points(series, _range_start(p.day), p.day - timedelta(days=1)))
    low, high, week, years = _range_for(weeks, p.day)
    common = {
        "value": p.value,
        "unit": unit,
        "as_of": p.day,
        "settings": cfg,
        "week": week,
        "years": years,
    }
    if low is None:
        why = (
            f"only {len(years)} of the previous {RANGE_YEARS} years have data for week {week} "
            f"(need {RANGE_MIN_YEARS})"
        )
        return TypicalRange(reason=why, **common)
    pos = "below" if p.value < low else "above" if p.value > high else "within"
    return TypicalRange(low=low, high=high, position=pos, **common)


def typical_range_series(
    store: Store, series: str, *, start: date, end: date | datetime
) -> list[RangePoint]:
    """Each reading from start to end with its typical range, for the band
    behind the stored-oil charts. low and high are None where the range is
    not available."""
    pts = store.points(series, _range_start(start), end)
    weeks = _by_week(pts)
    out = []
    for p in pts:
        d = p.period_end.date()
        if d < start:
            continue
        low, high, _, _ = _range_for(weeks, d)
        out.append(RangePoint(d, p.value, low, high))
    return out


# --- headline tiles and the summary sentence ------------------------------------------


def _sparkline(store: Store, series: str, as_of: date | None) -> tuple[DatedValue, ...]:
    if as_of is None:
        return ()
    start = as_of - timedelta(days=SPARKLINE_DAYS - 1)
    return tuple(_dv(p) for p in store.points(series, start, as_of))


def _series_tile(
    store: Store, key: str, label: str, series: str, *, now: datetime, settings: Settings
) -> Tile:
    war = change_since_war(store, series, now=now, settings=settings)
    last = change_since_last(store, series, now=now)
    cur = war.current or last.current
    return Tile(
        key=key,
        label=label,
        value=cur.value if cur else None,
        unit=UNITS.get(series, ""),
        as_of=cur.day if cur else None,
        settings=_ref(settings),
        reason=None if cur else _no_data(series),
        since_last=last,
        since_war=war,
        sparkline=_sparkline(store, series, cur.day if cur else None),
    )


def _scale(c: Change, k: float, unit: str, settings: dict[str, object]) -> Change:
    def s(x: DatedValue | None) -> DatedValue | None:
        return DatedValue(x.day, x.value * k) if x else None

    return Change(
        value=None if c.value is None else c.value * k,
        percent=c.percent,
        unit=unit,
        as_of=c.as_of,
        settings=settings,
        reason=c.reason,
        current=s(c.current),
        base=s(c.base),
    )


def _tank_tile(store: Store, *, now: datetime, settings: Settings) -> Tile:
    g = settings.tank_gallons
    cfg = {"tank_gallons": g, "reference_date": settings.reference_date}
    t = _series_tile(store, "tank", "", GASOLINE_US, now=now, settings=settings)
    return Tile(
        key="tank",
        label=f"Cost to fill a {g:g}-gallon tank",
        value=None if t.value is None else t.value * g,
        unit="$",
        as_of=t.as_of,
        settings=cfg,
        reason=t.reason,
        since_last=_scale(t.since_last, g, "$", {"tank_gallons": g}),
        since_war=_scale(t.since_war, g, "$", cfg),
        sparkline=tuple(DatedValue(p.day, p.value * g) for p in t.sparkline),
    )


def _hormuz_tile(store: Store, *, now: datetime, settings: Settings) -> Tile:
    cfg = _pon_settings(settings)
    unit = "% of normal"
    cur = percent_of_normal(store, HORMUZ, now=now, settings=settings)
    ser = percent_of_normal_series(store, HORMUZ, now=now, settings=settings)
    pts = ser.points
    now_dv = DatedValue(cur.as_of, cur.value) if cur.value is not None else None
    prev = pts[-2] if now_dv and len(pts) >= 2 and pts[-1].day == now_dv.day else None
    base = None
    if cur.baseline_average is not None:
        ref = settings.reference_date
        avg, _ = _trailing(_days(store, HORMUZ, ref - timedelta(days=RECENT_DAYS - 1), ref), ref)
        if avg is not None:
            base = DatedValue(ref, avg / cur.baseline_average * 100)
    no_cur = cur.reason or _no_data(HORMUZ)
    return Tile(
        key="hormuz",
        label="Ships crossing Hormuz vs normal",
        value=cur.value,
        unit=unit,
        as_of=cur.as_of,
        settings=cfg,
        reason=cur.reason,
        since_last=_change(
            now_dv, prev, unit=unit, settings=cfg, no_current=no_cur, no_base="only one day so far"
        ),
        since_war=_change(
            now_dv,
            base,
            unit=unit,
            settings=cfg,
            no_current=no_cur,
            no_base="not enough days around the reference date",
        ),
        sparkline=pts,
    )


def _stocks_tile(store: Store, *, now: datetime, settings: Settings) -> Tile:
    t = _series_tile(
        store, "crude_stocks", "US crude stocks", CRUDE_STOCKS, now=now, settings=settings
    )
    rng = typical_range(store, CRUDE_STOCKS, now=now)
    return Tile(
        key=t.key,
        label=t.label,
        value=t.value,
        unit=t.unit,
        as_of=t.as_of,
        settings=t.settings | rng.settings,
        reason=t.reason,
        since_last=t.since_last,
        since_war=t.since_war,
        sparkline=t.sparkline,
        typical_range=rng,
    )


def tiles(store: Store, *, now: datetime, settings: Settings) -> tuple[Tile, ...]:
    """The six headline tiles, in page order."""
    _check_now(now)
    return (
        _series_tile(store, "brent", "Brent crude", BRENT, now=now, settings=settings),
        _series_tile(store, "gasoline", "US gasoline", GASOLINE_US, now=now, settings=settings),
        _series_tile(store, "diesel", "US diesel", DIESEL_US, now=now, settings=settings),
        _tank_tile(store, now=now, settings=settings),
        _hormuz_tile(store, now=now, settings=settings),
        _stocks_tile(store, now=now, settings=settings),
    )


def _money(x: float) -> str:
    return f"${x:,.2f}"


def _cents(x: float) -> str:
    """A pump price change: '41 cents', '1 cent', or '$1.05' from a dollar up."""
    x = abs(x)
    if round(x, 2) >= 1:
        return _money(x)
    c = round(x * 100)
    return f"{c} cent" if c == 1 else f"{c} cents"


def _pct(c: Change) -> str:
    return f"{abs(c.percent):.0f}%"


def _price_change(c: Change) -> str:
    return _cents(c.value)


def summary_text(tiles_: Iterable[Tile]) -> str:
    """One plain-English sentence from fixed templates. Clauses whose data is
    missing are left out; with nothing to say it says so. "Since the war
    began" is said once, on the first change mentioned."""
    by = {t.key: t for t in tiles_}
    clauses: list[str] = []
    since = " since the war began"

    def direction(c: Change, amount: Callable[[Change], str]) -> str:
        nonlocal since
        if c.value is None or c.percent is None:
            return ""
        s, since = since, ""
        if abs(c.percent) < FLAT_PERCENT:
            return f", little changed{s}"
        return f", {'up' if c.value > 0 else 'down'} {amount(c)}{s}"

    if (t := by.get("brent")) and t.value is not None:
        clauses.append(f"Brent crude is {_money(t.value)} a barrel{direction(t.since_war, _pct)}")
    if (t := by.get("gasoline")) and t.value is not None:
        d = direction(t.since_war, _price_change)
        clauses.append(f"US gasoline is {_money(t.value)} a gallon{d}")
    if (t := by.get("hormuz")) and t.value is not None:
        if t.value == 0:
            clauses.append("no ships are crossing the Strait of Hormuz")
        else:
            clauses.append(f"ships crossing the Strait of Hormuz are at {t.value:.0f}% of normal")
    if (t := by.get("crude_stocks")) and t.typical_range and t.typical_range.position:
        clauses.append(
            f"US crude stocks are {t.typical_range.position} the typical range "
            "for this time of year"
        )
    if not clauses:
        return "Not enough data has come in yet to sum things up."
    if len(clauses) > 1:
        clauses[-1] = "and " + clauses[-1]
    text = "; ".join(clauses) + "."
    return text[0].upper() + text[1:]


def summary(store: Store, *, now: datetime, settings: Settings) -> Summary:
    """The headline strip: the summary sentence and its six tiles."""
    ts = tiles(store, now=now, settings=settings)
    dates = [t.as_of for t in ts if t.as_of]
    return Summary(
        text=summary_text(ts),
        tiles=ts,
        as_of=max(dates) if dates else None,
        settings={
            "reference_date": settings.reference_date,
            "baseline_days": settings.baseline_days,
            "tank_gallons": settings.tank_gallons,
        },
    )


__all__ = [
    "BRENT",
    "CHOKEPOINT_SERIES",
    "CRUDE_STOCKS",
    "DIESEL_US",
    "GASOLINE_US",
    "HORMUZ",
    "REGIONS",
    "REGION_NAMES",
    "WTI",
    "Change",
    "CrudeShare",
    "DatedValue",
    "PercentOfNormal",
    "RangePoint",
    "Result",
    "SeriesResult",
    "Summary",
    "TankCost",
    "Tile",
    "TypicalRange",
    "brent_wti_gap",
    "brent_wti_gap_series",
    "change_since_last",
    "change_since_war",
    "chokepoints_vs_normal",
    "crude_share",
    "diesel_premium",
    "diesel_premium_series",
    "diesel_premiums",
    "percent_of_normal",
    "percent_of_normal_series",
    "summary",
    "summary_text",
    "tank_cost",
    "tiles",
    "typical_range",
    "typical_range_series",
    "week_of_year",
]
