"""What each API route returns, as plain JSON-ready dicts.

Synchronous functions over a Store: the app runs them in a worker thread
(``asyncio.to_thread``) and caches the result. Nothing here reads the clock;
``now`` is always passed in.

Messages that came from a provider or an exception are passed through
``redact`` again and stripped of control characters on the way out, even
though the store already sanitized them: a secret registered after the
message was stored is still masked.
"""

from __future__ import annotations

import dataclasses
import math
import re
from datetime import UTC, date, datetime, timedelta
from enum import Enum

from fuelwatch import backfill, calc
from fuelwatch.api.attribution import attribution, terms_url
from fuelwatch.http import redact
from fuelwatch.models import Frequency, Point
from fuelwatch.registry import Entry, FreshnessStatus, Registry, releases
from fuelwatch.store import CollectorStatus, Store

AIS_COLLECTOR = "aisstream"
MAX_MESSAGE = 300
ROLLUP_AFTER_DAYS = 7  # AIS ranges covering more days than this come back as daily roll-ups
# The four EIA spot series are Refinitiv (LSEG) data republished by EIA. The
# dashboard charts them; /api/data serves them unless FW_API_SPOT_DATA=0
# (terms-review.md, Decision 2026-10-17).
SPOT_SERIES = frozenset({calc.BRENT, calc.WTI, "eia.spot.nyh_gasoline", "eia.spot.nyh_ulsd"})

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")


# --- encoding -------------------------------------------------------------------------


def iso_instant(t: datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def jsonable(obj: object) -> object:
    """Dataclasses, dates, enums and containers to JSON types. A non-finite
    float becomes None: missing is never shown as a number."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: jsonable(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, datetime):
        return iso_instant(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {str(k): jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple | set | frozenset):
        return [jsonable(v) for v in obj]
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


def safe_message(text: str | None) -> str | None:
    if not text:
        return None
    return _CONTROL_RE.sub(" ", redact(str(text))).strip()[:MAX_MESSAGE] or None


def _period(entry: Entry, t: datetime) -> str:
    """A 10-minute slot is an instant; every other series is a period date."""
    if entry.spec.frequency == Frequency.TEN_MINUTES:
        return iso_instant(t)
    return t.date().isoformat()


def _point(entry: Entry, p: Point | None) -> dict | None:
    if p is None:
        return None
    return {"period": _period(entry, p.period_end), "value": p.value, "provisional": p.provisional}


def release_date(entry: Entry, latest: datetime | None, now: datetime) -> date | None:
    """The scheduled release that carried `latest`: the oldest release already
    due whose expected period reaches it, else the newest one due (a release
    may hold more than its expected period). An estimate from the registry's
    schedule; the store does not record when a provider actually published."""
    if latest is None or entry.schedule.frequency != "weekly":
        return None
    due = [r for r in releases(entry.schedule, now, weeks_back=12, weeks_ahead=0) if r.at <= now]
    for r in due:
        if r.period >= latest.date():
            return r.at.date()
    return due[-1].at.date() if due else None


def _source(entry: Entry, latest: datetime | None, now: datetime) -> dict:
    return {
        "source": entry.spec.source,
        "attribution": attribution(entry.spec.source, release_date(entry, latest, now)),
        "terms_url": terms_url(entry.spec.source),
    }


# --- /api/summary ---------------------------------------------------------------------


def summary(store: Store, now: datetime) -> dict:
    return jsonable(calc.summary(store, now=now, settings=store.settings()))


# --- /api/series ----------------------------------------------------------------------


def series(store: Store, registry: Registry, now: datetime, *, spot_data: bool) -> dict:
    out = []
    for entry in registry:
        pts = store.latest(entry.id, 2)
        latest = pts[-1] if pts else None
        previous = pts[-2] if len(pts) == 2 else None
        t = latest.period_end if latest else None
        fresh = registry.freshness(entry.id, t, now)
        out.append(
            {
                "id": entry.id,
                "name": entry.spec.name,
                "unit": entry.spec.unit,
                "frequency": entry.spec.frequency.value,
                "schedule": entry.spec.schedule,
                "collector": entry.collector,
                "status": fresh.status.value,
                "as_of": _period(entry, t) if t else None,
                "late_after": iso_instant(fresh.deadline),
                "latest": _point(entry, latest),
                "previous": _point(entry, previous),
                "data_api": spot_data or entry.id not in SPOT_SERIES,
                **_source(entry, t, now),
            }
        )
    return {"generated_at": iso_instant(now), "series": out}


# --- /api/data/{series} ---------------------------------------------------------------


def uses_rollup(entry: Entry, start: date, end: date) -> bool:
    days = (end - start).days + 1
    return entry.spec.frequency == Frequency.TEN_MINUTES and days > ROLLUP_AFTER_DAYS


def data(store: Store, entry: Entry, start: date, end: date, now: datetime) -> dict:
    if uses_rollup(entry, start, end):
        resolution = "daily"
        rows = [
            {
                "period": d.day.isoformat(),
                "value": d.value,
                "provisional": d.provisional,
                "slots": d.slots,
                "partial": d.partial,
            }
            for d in store.daily_rollup(entry.id, start, end)
        ]
    else:
        resolution = "raw"
        rows = [_point(entry, p) for p in store.points(entry.id, start, end)]
    newest = store.latest(entry.id, 1)
    return {
        "series": entry.id,
        "name": entry.spec.name,
        "unit": entry.spec.unit,
        "frequency": entry.spec.frequency.value,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "resolution": resolution,
        **_source(entry, newest[0].period_end if newest else None, now),
        "points": rows,
    }


# --- /api/status ----------------------------------------------------------------------


def _row_status(
    entries: list[Entry],
    latest: dict[str, Point],
    registry: Registry,
    now: datetime,
    *,
    error: bool,
    backfilling: bool,
    count_no_data: bool,
) -> tuple[str, datetime | None]:
    """ok, late, down or backfilling, and the missed deadline when late."""
    if error:
        return "down", None
    if backfilling:
        return "backfilling", None
    missed = []
    for e in entries:
        p = latest.get(e.id)
        f = registry.freshness(e.id, p.period_end if p else None, now)
        if f.status == FreshnessStatus.LATE or (
            f.status == FreshnessStatus.NO_DATA and count_no_data
        ):
            missed.append(f.deadline)
    if missed:
        return "late", min(missed)
    return "ok", None


def status(store: Store, registry: Registry, now: datetime, ais=None) -> dict:
    """One row per source. Backfill state comes from backfill.summary(),
    which knows about partial collectors (the store's own state shows them
    as done; issue #17)."""
    bf = backfill.summary(store, registry.polled)
    latest = store.series_latest()
    rows = []
    for c in registry.collectors:
        is_ais = c.name == AIS_COLLECTOR
        if is_ais and ais is None:
            continue  # live AIS is off
        entries = registry.for_collector(c.name)
        st: CollectorStatus | None = store.get_status(c.name)
        error_class = st.error_class if st else None
        message = st.error_message if st else None
        if is_ais and ais.status.error_class:
            error_class, message = ais.status.error_class, ais.status.message
        bf_state = None if is_ais else bf.collectors.get(c.name)
        state, missed = _row_status(
            entries,
            latest,
            registry,
            now,
            error=error_class is not None,
            backfilling=bf_state in ("not_started", "running"),
            # A missing series counts as late once its history should be
            # there; live AIS has no history, so only its lateness counts.
            count_no_data=not is_ais,
        )
        if state == "late" and missed is not None:
            message = f"New data was expected by {iso_instant(missed)}."
        elif state != "down":
            message = None
        through = [latest[e.id].period_end for e in entries if e.id in latest]
        newest = max(through) if through else None
        row = {
            "collector": c.name,
            "source": entries[0].spec.source if entries else c.name,
            "status": state,
            "data_through": _period(entries[0], newest) if newest else None,
            "last_checked": iso_instant(st.last_run_at) if st and st.last_run_at else None,
            "last_success": iso_instant(st.last_success_at) if st and st.last_success_at else None,
            "error_class": error_class if state == "down" else None,
            "message": safe_message(message),
            "backfill": bf_state,
        }
        if is_ais:
            row["connected"] = bool(ais.status.connected)
        if c.name in bf.missing:
            row["missing_series"] = list(bf.missing[c.name])
        rows.append(row)
    return {
        "generated_at": iso_instant(now),
        "live_ais": ais is not None,
        "backfill": {
            "state": bf.state,
            "progress": bf.progress,
            "collectors": bf.collectors,
            "errors": {k: safe_message(v) for k, v in bf.errors.items()},
            "missing": {k: list(v) for k, v in bf.missing.items()},
        },
        "sources": rows,
    }


# --- /api/events ----------------------------------------------------------------------


def events(items) -> dict:
    return {"events": [{"date": e.date.isoformat(), "label": e.label, "url": e.url} for e in items]}


# --- request bounds -------------------------------------------------------------------

MAX_SPAN_DAYS = 5 * 365 + 2  # five years, leap days included
DEFAULT_SPAN = timedelta(days=365)
EARLIEST = date(1970, 1, 1)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class BadRange(ValueError):
    pass


def parse_day(text: str | None, name: str) -> date | None:
    if text is None:
        return None
    if not _DATE_RE.match(text):
        raise BadRange(f"{name} must be a date as YYYY-MM-DD")
    try:
        d = date.fromisoformat(text)
    except ValueError:
        raise BadRange(f"{name} is not a real date") from None
    if d < EARLIEST:
        raise BadRange(f"{name} must be on or after {EARLIEST.isoformat()}")
    return d


def data_range(start: str | None, end: str | None, today: date) -> tuple[date, date]:
    """Resolve and check a /api/data range. Default: the year up to today.
    Raises BadRange with a message fit for a 400."""
    s, e = parse_day(start, "start"), parse_day(end, "end")
    if e is None:
        e = max(today, s) if s else today
    if s is None:
        s = max(EARLIEST, e - DEFAULT_SPAN)
    if s > e:
        raise BadRange("start must be on or before end")
    if (e - s).days + 1 > MAX_SPAN_DAYS:
        raise BadRange("a request covers at most five years")
    return s, e
