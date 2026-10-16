"""EIA collector: spot prices, retail pump prices and the Weekly Petroleum
Status Report, from EIA API v2 (https://api.eia.gov/v2/).

Series are fetched by EIA series ID through the `series` facet of each
route. Each row's `period` is stored as UTC midnight of that date:

- spot prices: the trading day;
- retail prices: the Monday the survey prices are collected (EIA's weekly
  period for this route is the survey date, not a week-ending date);
- WPSR: the week-ending Friday.

EIA leaves a holiday out of the data rather than sending a placeholder, so
gaps are missing rows. Empty, null, non-numeric and zero values are also
treated as gaps; nothing is ever filled with zero.
"""

import logging
import math
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

from fuelwatch.http import ProviderError, get_json, redact
from fuelwatch.models import Frequency, Point, SeriesSpec

log = logging.getLogger(__name__)

BASE_URL = "https://api.eia.gov/v2/"
SOURCE = "EIA"
PAGE_SIZE = 5000  # EIA's maximum rows per JSON response
BACKFILL = timedelta(days=5 * 366 + 14)  # five years plus a little for week-of-year ranges


@dataclass(frozen=True)
class EiaSeries:
    spec: SeriesSpec
    eia_id: str
    eia_units: str  # the `units` value EIA sends; anything else is a format change
    scale: float = 1.0  # multiply EIA's value by this to get spec.unit


@dataclass(frozen=True)
class Group:
    """One API route and frequency, fetched in a single paged request."""

    route: str
    frequency: Frequency
    series: tuple[EiaSeries, ...]


_SPOT_SCHEDULE = "weekly release of daily data, usually Wednesday"
_RETAIL_SCHEDULE = "weekly, Monday (a day later after a federal holiday)"
_WPSR_SCHEDULE = "weekly, Wednesday (a day later after a federal holiday); week ending Friday"
_GRACE = timedelta(days=2)


def _spot(sid: str, name: str, unit: str, eia_id: str, eia_units: str) -> EiaSeries:
    spec = SeriesSpec(sid, name, unit, SOURCE, Frequency.DAILY, _SPOT_SCHEDULE, _GRACE)
    return EiaSeries(spec, eia_id, eia_units)


def _retail(sid: str, name: str, eia_id: str) -> EiaSeries:
    spec = SeriesSpec(sid, name, "$/gal", SOURCE, Frequency.WEEKLY, _RETAIL_SCHEDULE, _GRACE)
    return EiaSeries(spec, eia_id, "$/GAL")


def _wpsr(sid: str, name: str, unit: str, eia_id: str, eia_units: str, scale=1.0) -> EiaSeries:
    spec = SeriesSpec(sid, name, unit, SOURCE, Frequency.WEEKLY, _WPSR_SCHEDULE, _GRACE)
    return EiaSeries(spec, eia_id, eia_units, scale)


_REGIONS = [
    ("us", "US", "NUS"),
    ("padd1", "East Coast (PADD 1)", "R10"),
    ("padd2", "Midwest (PADD 2)", "R20"),
    ("padd3", "Gulf Coast (PADD 3)", "R30"),
    ("padd4", "Rocky Mountain (PADD 4)", "R40"),
    ("padd5", "West Coast (PADD 5)", "R50"),
    ("ca", "California", "SCA"),
]

GROUPS: tuple[Group, ...] = (
    Group(
        "petroleum/pri/spt",
        Frequency.DAILY,
        (
            _spot("eia.spot.brent", "Brent crude spot", "$/bbl", "RBRTE", "$/BBL"),
            _spot("eia.spot.wti", "WTI crude spot, Cushing", "$/bbl", "RWTC", "$/BBL"),
            _spot(
                "eia.spot.nyh_gasoline",
                "NY Harbor conventional regular gasoline spot",
                "$/gal",
                "EER_EPMRU_PF4_Y35NY_DPG",
                "$/GAL",
            ),
            _spot(
                "eia.spot.nyh_ulsd",
                "NY Harbor ultra-low sulfur diesel spot",
                "$/gal",
                "EER_EPD2DXL0_PF4_Y35NY_DPG",
                "$/GAL",
            ),
        ),
    ),
    Group(
        "petroleum/pri/gnd",
        Frequency.WEEKLY,
        tuple(
            _retail(
                f"eia.retail.gasoline.{key}",
                f"Regular gasoline retail, {label}",
                f"EMM_EPMR_PTE_{code}_DPG",
            )
            for key, label, code in _REGIONS
        )
        + tuple(
            _retail(
                f"eia.retail.diesel.{key}",
                f"On-highway diesel retail, {label}",
                f"EMD_EPD2D_PTE_{code}_DPG",
            )
            for key, label, code in _REGIONS
        ),
    ),
    Group(
        "petroleum/stoc/wstk",
        Frequency.WEEKLY,
        (
            _wpsr(
                "eia.stocks.crude_commercial",
                "US commercial crude stocks (excl. SPR)",
                "million bbl",
                "WCESTUS1",
                "MBBL",
                0.001,
            ),
            _wpsr(
                "eia.stocks.gasoline",
                "US total motor gasoline stocks",
                "million bbl",
                "WGTSTUS1",
                "MBBL",
                0.001,
            ),
            _wpsr(
                "eia.stocks.distillate",
                "US distillate fuel oil stocks",
                "million bbl",
                "WDISTUS1",
                "MBBL",
                0.001,
            ),
            _wpsr(
                "eia.stocks.spr",
                "US Strategic Petroleum Reserve crude stocks",
                "million bbl",
                "WCSSTUS1",
                "MBBL",
                0.001,
            ),
        ),
    ),
    Group(
        "petroleum/pnp/wiup",
        Frequency.WEEKLY,
        (
            _wpsr(
                "eia.refinery.utilization",
                "US refinery utilization, operable capacity",
                "%",
                "WPULEUS3",
                "%",
            ),
        ),
    ),
)

SERIES: list[SeriesSpec] = [s.spec for g in GROUPS for s in g.series]


def parse_period(text: str) -> datetime:
    """EIA daily and weekly periods are YYYY-MM-DD; store as UTC midnight."""
    try:
        d = date.fromisoformat(text)
    except (TypeError, ValueError):
        raise ProviderError("schema", f"EIA period not YYYY-MM-DD: {text!r}") from None
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def parse_value(raw: object) -> float | None:
    """A number, or None for a gap. Zero is a gap: none of these series can
    truly be zero, so a zero can only be a placeholder."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        v = float(raw)  # EIA sends numbers as strings
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v == 0:
        return None
    return v


def parse_response(doc: object, group: Group) -> list[Point]:
    """Turn one decoded EIA v2 data response into points for `group`.

    Raises ProviderError("schema") if the response is not the expected shape
    or a series arrives in unexpected units; rows for series outside the
    group are ignored."""
    if not isinstance(doc, dict) or not isinstance(doc.get("response"), dict):
        raise ProviderError("schema", f"EIA {group.route}: no 'response' object")
    resp = doc["response"]
    freq = resp.get("frequency")
    if freq is not None and freq != group.frequency.value:
        raise ProviderError(
            "schema", f"EIA {group.route}: frequency {freq!r}, expected {group.frequency.value!r}"
        )
    rows = resp.get("data")
    if not isinstance(rows, list):
        raise ProviderError("schema", f"EIA {group.route}: 'data' is not a list")

    by_id = {s.eia_id: s for s in group.series}
    points: list[Point] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ProviderError("schema", f"EIA {group.route}: row is not an object")
        s = by_id.get(row.get("series"))
        if s is None:
            continue
        if row.get("units") != s.eia_units:
            raise ProviderError(
                "schema", f"EIA {s.eia_id}: units {row.get('units')!r}, expected {s.eia_units!r}"
            )
        period = parse_period(row.get("period"))
        value = parse_value(row.get("value"))
        if value is None:
            log.info(
                "EIA %s %s: no value (%r), leaving a gap", s.eia_id, period.date(), row.get("value")
            )
            continue
        points.append(Point(s.spec.id, period, round(value * s.scale, 6)))
    return points


def total_rows(doc: object) -> int:
    try:
        return int(doc["response"]["total"])  # type: ignore[index]
    except (KeyError, TypeError, ValueError):
        return 0


def api_key() -> str:
    key = os.environ.get("EIA_API_KEY", "").strip()
    if not key:
        raise ProviderError("config", "EIA_API_KEY is not set")
    redact.add(key)
    return key


def query_params(group: Group, key: str, start: date, offset: int = 0) -> dict:
    return {
        "api_key": key,
        "frequency": group.frequency.value,
        "data[0]": "value",
        "facets[series][]": [s.eia_id for s in group.series],
        "start": start.isoformat(),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": offset,
        "length": PAGE_SIZE,
    }


async def fetch_group(
    client: httpx.AsyncClient, group: Group, key: str, start: date
) -> list[Point]:
    url = f"{BASE_URL}{group.route}/data/"
    points: list[Point] = []
    offset = 0
    while True:
        doc = await get_json(client, url, **query_params(group, key, start, offset))
        page = parse_response(doc, group)
        points.extend(page)
        n = len(doc["response"]["data"])  # parse_response has checked the shape
        offset += n
        if n == 0 or offset >= total_rows(doc):
            return points


async def fetch(client: httpx.AsyncClient, since: date | None = None) -> list[Point]:
    key = api_key()
    start = since or (datetime.now(UTC).date() - BACKFILL)
    points: list[Point] = []
    for group in GROUPS:
        points.extend(await fetch_group(client, group, key, start))
    return points
