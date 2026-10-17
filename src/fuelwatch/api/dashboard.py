"""/api/dashboard: everything the page draws, in one bounded response.

One request per time range instead of one per chart (the page has about
twenty). ``range`` is one of RANGES: since the war began, one year, or five
years, so a request never covers more than the five years /api/data allows.
The response is cached for 60 seconds per range.

This is the page's own feed, not a dataset: it carries the chart points the
page shows, including the four EIA spot series (Refinitiv data), which the
owner decided may be charted (terms-review.md, Decision 2026-10-17). Bulk
download of those series is what FW_API_SPOT_DATA governs, at /api/data.

Points are compact ``[date, value]`` pairs (``[date, value, provisional]``
for PortWatch days), oldest first. Every label here is plain language; the
registry's own names keep their jargon for the JSON API.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from fuelwatch import calc
from fuelwatch.api import views
from fuelwatch.api.attribution import credit
from fuelwatch.collectors import eu_oil_bulletin, portwatch
from fuelwatch.collectors.ais_tracker import SERIES_SHIPS, SERIES_TANKERS
from fuelwatch.registry import Entry, FreshnessStatus, Registry
from fuelwatch.store import Store

RANGES = ("war", "1y", "5y")
DEFAULT_RANGE = "1y"
FIVE_YEARS = timedelta(days=views.MAX_SPAN_DAYS - 1)
AIS_LIVE_FOR = timedelta(hours=1)  # a live count older than this is not "now"

NYH_GASOLINE = "eia.spot.nyh_gasoline"
NYH_DIESEL = "eia.spot.nyh_ulsd"
STOCKS = (
    (calc.CRUDE_STOCKS, "Crude oil", "Not counting the emergency reserve"),
    ("eia.stocks.gasoline", "Gasoline", None),
    ("eia.stocks.distillate", "Diesel and heating oil", None),
    ("eia.stocks.spr", "Emergency reserve", "Crude oil the government holds for emergencies"),
)
REFINERY = "eia.refinery.utilization"
OTHER_CHOKEPOINTS = tuple(cp for cp in portwatch.CHOKEPOINTS if cp.key != "hormuz")
PLACE_NAMES = {
    "hormuz": "Strait of Hormuz",
    "bab_el_mandeb": "Bab el-Mandeb (Red Sea)",
    "suez": "Suez Canal",
    "cape_of_good_hope": "Cape of Good Hope",
    "malacca": "Strait of Malacca",
}
EU_AREAS = tuple(eu_oil_bulletin.AREAS)  # eu27, de, fr, it, es
EU_NAMES = {key: name for key, (_, _, name) in eu_oil_bulletin.AREAS.items()}


def _how_often(entry: Entry) -> str:
    """How often a series is published, in words, for "Weekly, data through Oct 14"."""
    if entry.collector == "aisstream":
        return "Live, every 10 minutes"
    if entry.spec.frequency.value == "daily":
        return "Daily figures, published weekly"
    return "Weekly"


def label(sid: str) -> str:
    """The plain-language name the page uses for a series."""
    if sid == calc.BRENT:
        return "Brent crude"
    if sid == calc.WTI:
        return "WTI crude"
    if sid == NYH_GASOLINE:
        return "What stations pay for gasoline"
    if sid == NYH_DIESEL:
        return "What stations pay for diesel"
    if sid.startswith("eia.retail."):
        _, _, fuel, region = sid.split(".")
        return f"{'Gasoline' if fuel == 'gasoline' else 'Diesel'}, {calc.REGION_NAMES[region]}"
    for s, name, _ in STOCKS:
        if sid == s:
            return f"Stored {name.lower()}" if s != "eia.stocks.spr" else name
    if sid == REFINERY:
        return "Refineries in use"
    if sid.startswith("portwatch."):
        _, key, measure = sid.split(".")
        what = {"all": "Ships crossing", "tankers": "Tankers crossing"}.get(
            measure, "Tanker cargo crossing (estimate)"
        )
        return f"{what}, {PLACE_NAMES[key]}"
    if sid.startswith("eu_pump_"):
        _, _, fuel, area = sid.split("_")
        return f"{'Petrol' if fuel == 'petrol' else 'Diesel'}, {EU_NAMES[area]}"
    if sid == SERIES_SHIPS:
        return "Ships broadcasting their position in the strait"
    if sid == SERIES_TANKERS:
        return "Tankers broadcasting their position in the strait"
    return "Ships crossing the strait (live count)"


def range_bounds(key: str, now: datetime, reference: date) -> tuple[date, date]:
    end = now.date()
    if key == "war":
        start = min(reference, end)
    elif key == "1y":
        start = end - timedelta(days=365)
    else:
        start = end - FIVE_YEARS
    return max(start, end - FIVE_YEARS), end


def _pairs(points) -> list[list]:
    return [[p.period_end.date().isoformat(), p.value] for p in points]


def _dv(values) -> list[list]:
    return [[v.day.isoformat(), v.value] for v in values]


def _change(c: calc.Change) -> dict:
    return {
        "value": c.value,
        "percent": c.percent,
        "base_date": c.base.day.isoformat() if c.base else None,
    }


def _seven_day(days: dict[date, float], start: date, end: date) -> list[list]:
    """Trailing 7-day average of ships crossing, on each day with one."""
    out = []
    d = start
    while d <= end:
        vals = [v for i in range(7) if (v := days.get(d - timedelta(days=i))) is not None]
        if d in days and len(vals) >= calc.RECENT_MIN_DAYS:
            out.append([d.isoformat(), sum(vals) / len(vals)])
        d += timedelta(days=1)
    return out


def _hormuz(store: Store, start: date, end: date | datetime, now: datetime, settings) -> dict:
    sid_all = calc.HORMUZ
    sid_t = portwatch.series_id(portwatch.CHOKEPOINTS[0], portwatch.TANKERS)
    pts = store.points(sid_all, start, end)
    tankers = store.points(sid_t, start, end)
    settled = {
        p.period_end.date(): p.value
        for p in store.points(sid_all, start - timedelta(days=7), end, settled_only=True)
    }
    last_settled = max(settled) if settled else None
    avg7 = _seven_day(settled, start, last_settled) if last_settled else []
    # The normal band: the lowest and highest 7-day average over the
    # baseline window, and its average (what "100% of normal" means).
    ref = settings.reference_date
    b_start = ref - timedelta(days=settings.baseline_days - 1)
    base_days = {
        p.period_end.date(): p.value
        for p in store.points(
            sid_all, b_start - timedelta(days=7), calc._cap(ref, now), settled_only=True
        )
    }
    band = [v for _, v in _seven_day(base_days, b_start, ref)]
    pon = calc.percent_of_normal(store, sid_all, now=now, settings=settings)
    return {
        "all": [[p.period_end.date().isoformat(), p.value, p.provisional] for p in pts],
        "tankers": [[p.period_end.date().isoformat(), p.value, p.provisional] for p in tankers],
        "seven_day": avg7,
        "normal": {
            "average": pon.baseline_average,
            "low": min(band) if band else None,
            "high": max(band) if band else None,
            "start": b_start.isoformat(),
            "end": ref.isoformat(),
        },
        "percent_of_normal": pon.value,
        "as_of": pon.as_of.isoformat() if pon.as_of else None,
    }


def _ais(store: Store, now: datetime, ais) -> dict:
    out: dict = {"enabled": ais is not None, "connected": bool(ais and ais.status.connected)}
    for key, sid in (("ships", SERIES_SHIPS), ("tankers", SERIES_TANKERS)):
        p = store.latest(sid, 1)
        out[key] = {"at": views.iso_instant(p[0].period_end), "value": p[0].value} if p else None
    p = store.latest(SERIES_SHIPS, 1)
    out["live"] = bool(out["connected"] and p and now - p[0].period_end <= AIS_LIVE_FOR)
    return out


def _regions(store: Store, now: datetime) -> list[dict]:
    rows = []
    for r in calc.REGIONS:
        row = {"key": r, "name": calc.REGION_NAMES[r]}
        for fuel, sid in (("gasoline", calc.gasoline(r)), ("diesel", calc.diesel(r))):
            c = calc.change_since_last(store, sid, now=now)
            cur = c.current
            row[fuel] = {
                "value": cur.value if cur else None,
                "as_of": cur.day.isoformat() if cur else None,
                "since_last": _change(c),
            }
        rows.append(row)
    return rows


def _published(registry: Registry, store: Store, now: datetime) -> dict[str, str | None]:
    latest = store.series_latest()
    out = {}
    for c in registry.collectors:
        entries = registry.for_collector(c.name)
        have = [(latest[e.id].period_end, e) for e in entries if e.id in latest]
        if not have:
            out[c.name] = None
            continue
        t, e = max(have, key=lambda x: x[0])
        rel = views.release_date(e, t, now)
        out[c.name] = rel.isoformat() if rel else None
    return out


def _meta(registry: Registry, series: dict, now: datetime) -> dict:
    out = {}
    for row in series["series"]:
        entry = registry.get(row["id"])
        out[row["id"]] = {
            "label": label(row["id"]),
            "unit": row["unit"],
            "how_often": _how_often(entry),
            "as_of": row["as_of"],
            "late": row["status"] == FreshnessStatus.LATE.value,
            "late_after": row["late_after"],
            "attribution": row["attribution"],
            "credit": credit(entry.spec.source, spot=row["id"] in views.SPOT_SERIES),
        }
    return out


def build(
    store: Store,
    registry: Registry,
    now: datetime,
    *,
    range_key: str,
    events: list[dict],
    ais=None,
    demo: bool = False,
) -> dict:
    """The /api/dashboard body. Synchronous: run it in a worker thread."""
    settings = store.settings()
    start, end_day = range_bounds(range_key, now, settings.reference_date)
    end = calc._cap(end_day, now)
    series = views.series(store, registry, now, spot_data=True)
    status = views.status(store, registry, now, ais)
    published = _published(registry, store, now)
    late_by = {}
    for row in series["series"]:
        if row["status"] == FreshnessStatus.LATE.value:
            c = row["collector"]
            late_by[c] = min(late_by.get(c, row["late_after"]), row["late_after"])
    for row in status["sources"]:
        row["published"] = published.get(row["collector"])
        row["credit"] = credit(row["source"])
        row["late_after"] = late_by.get(row["collector"]) if row["status"] == "late" else None
    stocks = []
    for sid, name, note in STOCKS:
        rng = calc.typical_range(store, sid, now=now)
        stocks.append(
            {
                "id": sid,
                "name": name,
                "note": note,
                "position": rng.position,
                "low": rng.low,
                "high": rng.high,
                "points": [
                    [p.day.isoformat(), p.value, p.low, p.high]
                    for p in calc.typical_range_series(store, sid, start=start, end=end)
                ],
            }
        )
    refinery = store.latest(REFINERY, 2)
    share = calc.crude_share(store, calc.GASOLINE_US, now=now)
    chokepoints = []
    for cp in OTHER_CHOKEPOINTS:
        sid = portwatch.series_id(cp, portwatch.ALL_SHIPS)
        ser = calc.percent_of_normal_series(store, sid, now=now, settings=settings, start=start)
        chokepoints.append(
            {
                "id": sid,
                "name": PLACE_NAMES[cp.key],
                "value": ser.value,
                "as_of": ser.as_of.isoformat() if ser.as_of else None,
                "points": _dv(ser.points),
            }
        )
    return {
        "generated_at": views.iso_instant(now),
        "demo": demo,
        "range": {"key": range_key, "start": start.isoformat(), "end": end_day.isoformat()},
        "settings": {
            "reference_date": settings.reference_date.isoformat(),
            "baseline_days": settings.baseline_days,
            "tank_gallons": settings.tank_gallons,
        },
        "summary": views.summary(store, now),
        "series": _meta(registry, series, now),
        "status": status,
        "events": events,
        "crude": {
            "brent": _pairs(store.points(calc.BRENT, start, end)),
            "wti": _pairs(store.points(calc.WTI, start, end)),
            "gap": _dv(calc.brent_wti_gap_series(store, start=start, end=end)),
        },
        "pump": {
            "gasoline": _pairs(store.points(calc.GASOLINE_US, start, end)),
            "diesel": _pairs(store.points(calc.DIESEL_US, start, end)),
            "stations_gasoline": _pairs(store.points(NYH_GASOLINE, start, end)),
            "stations_diesel": _pairs(store.points(NYH_DIESEL, start, end)),
            "regions": _regions(store, now),
            "crude_share": {
                "value": share.value,
                "as_of": share.as_of.isoformat() if share.as_of else None,
                "pump": share.pump.value if share.pump else None,
                "brent": share.brent.value if share.brent else None,
                "brent_date": share.brent.day.isoformat() if share.brent else None,
            },
        },
        "eu": {
            fuel: {
                area: _pairs(store.points(eu_oil_bulletin.series_id(fuel, area), start, end))
                for area in EU_AREAS
            }
            for fuel in eu_oil_bulletin.FUELS
        },
        "hormuz": _hormuz(store, start, end, now, settings),
        "chokepoints": chokepoints,
        "ais": _ais(store, now, ais),
        "stocks": stocks,
        "refinery": {
            "value": refinery[-1].value if refinery else None,
            "previous": refinery[-2].value if len(refinery) == 2 else None,
            "as_of": refinery[-1].period_end.date().isoformat() if refinery else None,
        },
    }
