"""IMF PortWatch collector: ships crossing five chokepoints per day.

Source: the `Daily_Chokepoints_Data` ArcGIS FeatureServer, table layer 0
(no key). One row per chokepoint per day, with a `date` (esriFieldTypeDateOnly,
sent as "YYYY-MM-DD") and counts per vessel type:

- `n_total`: all ships crossing that day (sum of the per-type counts);
- `n_tanker`: tankers;
- `capacity_tanker`: estimated tanker trade volume in metric tons, i.e. each
  tanker's estimated payload share times its deadweight tonnage. It is cargo
  carried, not ship size: a tanker in ballast adds about zero, so a day can
  have tankers but zero tanker tonnage.

Chokepoints are matched on `portid` and the `portname` is checked, so a
renumbering fails loudly instead of mislabeling a series.

PortWatch publishes once a week (Tuesday about 13:00 UTC, observed) with data
through the previous Sunday. The two newest days of each publication are
marked provisional. Each point is stored at UTC midnight of its day. A day
with no row, or a null count, is a gap; zeros that PortWatch itself reports
are kept, because they are real (Hormuz had zero crossings on several days
in 2026).

Revisions are not limited to the newest days: archived responses show
changes to days several weeks old. Overwriting periods on each fetch picks
them up only for the range that is re-fetched, so callers should re-fetch
at least REVISION_WINDOW on every run.
"""

import logging
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx

from fuelwatch.http import ProviderError, get_json
from fuelwatch.models import Frequency, Point, SeriesSpec

log = logging.getLogger(__name__)

SERVICE_URL = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/ArcGIS/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer"
)
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
QUERY_URL = f"{LAYER_URL}/query"
SOURCE = "IMF PortWatch"

PAGE_SIZE = 1000  # the layer's maxRecordCount
MAX_PAGES = 100  # five years of five chokepoints is about 10 pages
BACKFILL = timedelta(days=5 * 366)
PROVISIONAL_DAYS = 2
REVISION_WINDOW = timedelta(days=35)  # suggested minimum re-fetch on each run

_SCHEDULE = "weekly, usually Tuesday about 13:00 UTC; data through the previous Sunday"
_GRACE = timedelta(days=2)


@dataclass(frozen=True)
class Chokepoint:
    key: str  # used in series IDs
    portid: str  # PortWatch's ID
    portname: str  # PortWatch's name, checked on every row


CHOKEPOINTS: tuple[Chokepoint, ...] = (
    Chokepoint("hormuz", "chokepoint6", "Strait of Hormuz"),
    Chokepoint("bab_el_mandeb", "chokepoint4", "Bab el-Mandeb Strait"),
    Chokepoint("suez", "chokepoint1", "Suez Canal"),
    Chokepoint("cape_of_good_hope", "chokepoint7", "Cape of Good Hope"),
    Chokepoint("malacca", "chokepoint5", "Malacca Strait"),
)
_BY_PORTID = {c.portid: c for c in CHOKEPOINTS}


@dataclass(frozen=True)
class Measure:
    suffix: str
    field: str
    label: str
    unit: str


ALL_SHIPS = Measure("all", "n_total", "ships crossing per day", "ships/day")
TANKERS = Measure("tankers", "n_tanker", "tankers crossing per day", "ships/day")
TANKER_TONNAGE = Measure(
    "tanker_tonnage", "capacity_tanker", "tanker cargo crossing per day (estimated)", "t/day"
)

MEASURES: dict[str, tuple[Measure, ...]] = {
    "hormuz": (ALL_SHIPS, TANKERS, TANKER_TONNAGE),
    "bab_el_mandeb": (ALL_SHIPS, TANKERS),
    "suez": (ALL_SHIPS, TANKERS),
    "cape_of_good_hope": (ALL_SHIPS, TANKERS),
    "malacca": (ALL_SHIPS, TANKERS),
}

OUT_FIELDS = ("date", "portid", "portname", "n_total", "n_tanker", "capacity_tanker")


def series_id(cp: Chokepoint, m: Measure) -> str:
    return f"portwatch.{cp.key}.{m.suffix}"


SERIES: list[SeriesSpec] = [
    SeriesSpec(
        series_id(cp, m),
        f"{cp.portname}: {m.label}",
        m.unit,
        SOURCE,
        Frequency.DAILY,
        _SCHEDULE,
        _GRACE,
    )
    for cp in CHOKEPOINTS
    for m in MEASURES[cp.key]
]


@dataclass(frozen=True)
class Row:
    """One validated chokepoint-day from the table."""

    day: date
    chokepoint: Chokepoint
    values: dict[str, float | None]  # field name -> value, None for a gap


def parse_day(raw: object) -> date:
    """`date` is DateOnly ("YYYY-MM-DD"). Older versions of this service
    used esriFieldTypeDate (epoch milliseconds at UTC midnight); accept both."""
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
    elif isinstance(raw, int) and not isinstance(raw, bool):
        return datetime.fromtimestamp(raw / 1000, UTC).date()
    raise ProviderError("schema", f"PortWatch date not YYYY-MM-DD: {raw!r}")


def parse_count(raw: object) -> float | None:
    """A non-negative number, or None for a gap. Zero is kept: PortWatch
    reports real zero-crossing days."""
    if raw is None or isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    if not math.isfinite(raw) or raw < 0:
        return None
    return float(raw)


def check_error(doc: object) -> dict:
    """ArcGIS reports query errors as HTTP 200 with an `error` object."""
    if not isinstance(doc, dict):
        raise ProviderError("schema", "PortWatch: response is not an object")
    err = doc.get("error")
    if err is not None:
        code = err.get("code") if isinstance(err, dict) else None
        msg = err.get("message") if isinstance(err, dict) else None
        raise ProviderError("provider_error", f"PortWatch query error {code}: {msg}")
    return doc


def parse_page(doc: object) -> list[Row]:
    """Turn one decoded query response into rows for the tracked chokepoints.

    Raises ProviderError on an ArcGIS error, a missing `features` list, a
    bad date, or a tracked `portid` whose name changed. Other chokepoints are
    ignored."""
    doc = check_error(doc)
    feats = doc.get("features")
    if not isinstance(feats, list):
        raise ProviderError("schema", "PortWatch: 'features' is not a list")
    rows: list[Row] = []
    for f in feats:
        attrs = f.get("attributes") if isinstance(f, dict) else None
        if not isinstance(attrs, dict):
            raise ProviderError("schema", "PortWatch: feature without 'attributes'")
        cp = _BY_PORTID.get(attrs.get("portid"))
        if cp is None:
            continue
        name = attrs.get("portname")
        if name != cp.portname:
            raise ProviderError(
                "schema", f"PortWatch {cp.portid}: name {name!r}, expected {cp.portname!r}"
            )
        day = parse_day(attrs.get("date"))
        values = {m.field: parse_count(attrs.get(m.field)) for m in MEASURES[cp.key]}
        rows.append(Row(day, cp, values))
    return rows


def to_points(rows: list[Row]) -> list[Point]:
    """Points for a whole publication (all pages). The two newest days of the
    publication are provisional. Null values and missing days are gaps; a
    repeated chokepoint-day (possible if a publish lands mid-paging) keeps
    the last row."""
    if not rows:
        return []
    newest = max(r.day for r in rows)
    cutoff = newest - timedelta(days=PROVISIONAL_DAYS - 1)
    latest: dict[tuple[str, date], Row] = {}
    for r in rows:
        latest[(r.chokepoint.portid, r.day)] = r
    points: list[Point] = []
    for (_, day), r in sorted(latest.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        period = datetime(day.year, day.month, day.day, tzinfo=UTC)
        for m in MEASURES[r.chokepoint.key]:
            v = r.values[m.field]
            if v is None:
                log.info(
                    "PortWatch %s %s %s: no value, leaving a gap", r.chokepoint.portid, day, m.field
                )
                continue
            points.append(Point(series_id(r.chokepoint, m), period, v, day >= cutoff))
    return points


def query_params(start: date, offset: int = 0, page_size: int = PAGE_SIZE) -> dict:
    ids = ",".join(f"'{c.portid}'" for c in CHOKEPOINTS)
    return {
        "where": f"portid IN ({ids}) AND date >= DATE '{start.isoformat()}'",
        "outFields": ",".join(OUT_FIELDS),
        "orderByFields": "date,portid",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "returnGeometry": "false",
        "f": "json",
    }


async def fetch_rows(
    client: httpx.AsyncClient, start: date, page_size: int = PAGE_SIZE
) -> list[Row]:
    """All rows on or after `start`, following ArcGIS paging: request
    `resultOffset` pages until `exceededTransferLimit` is no longer set."""
    rows: list[Row] = []
    offset = 0
    for _ in range(MAX_PAGES):
        doc = await get_json(client, QUERY_URL, **query_params(start, offset, page_size))
        page = parse_page(doc)
        rows.extend(page)
        n = len(doc["features"])  # parse_page has checked the shape
        offset += n
        if n == 0 or not doc.get("exceededTransferLimit"):
            return rows
    raise ProviderError("paging", f"PortWatch: more than {MAX_PAGES} pages")


async def fetch(client: httpx.AsyncClient, since: date | None = None) -> list[Point]:
    start = since or (datetime.now(UTC).date() - BACKFILL)
    return to_points(await fetch_rows(client, start))
