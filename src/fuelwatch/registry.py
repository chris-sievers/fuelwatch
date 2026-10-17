"""The series registry: every series from every collector, its publication
schedule in machine form, and a freshness check against that schedule.

Most series are daily data published weekly, so freshness is judged against
the publication, not the data frequency. For each weekly release the
schedule gives:

- the release weekday and typical time (UTC);
- the newest period that release must contain ("expected period");
- the grace before a missed release is flagged late.

Grace follows the spec: one business day for EIA, two days for PortWatch
and the EU bulletin (the collectors' SeriesSpec.grace), one hour for live
AIS. Business days skip weekends and US federal holidays (observed dates).

EIA slips a release by a business day for each federal holiday that falls
between Monday and the release day of that week (a Monday holiday moves the
Monday retail release to Tuesday and the Wednesday WPSR to Thursday). The
retail release can slip further, so a holiday week gets one more business
day of grace there. PortWatch and the EU bulletin have no holiday rule:
their two-day grace absorbs a one-day slip.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta
from enum import Enum
from functools import cache

import httpx

from fuelwatch.collectors import aisstream, eia, eu_oil_bulletin, portwatch
from fuelwatch.models import Point, SeriesSpec

# --- business days ---------------------------------------------------------------------


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th `weekday` (Mon=0) of a month; n=-1 is the last."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


@cache
def _holidays_for(year: int) -> frozenset[date]:
    fixed = [(1, 1), (6, 19), (7, 4), (11, 11), (12, 25)]
    days = {_observed(date(year, m, d)) for m, d in fixed}
    days |= {
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
        _nth_weekday(year, 5, 0, -1),  # Memorial Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day
        _nth_weekday(year, 10, 0, 2),  # Columbus Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving
    }
    return frozenset(days)


def is_federal_holiday(d: date) -> bool:
    """US federal holiday, on its observed date. New Year's Day on a Saturday
    is observed on Dec 31 of the year before, so check both years."""
    return d in _holidays_for(d.year) or d in _holidays_for(d.year + 1)


def is_business_day(d: date) -> bool:
    return d.weekday() < 5 and not is_federal_holiday(d)


def add_business_days(d: date, n: int) -> date:
    step = 1 if n >= 0 else -1
    for _ in range(abs(n)):
        d += timedelta(days=step)
        while not is_business_day(d):
            d += timedelta(days=step)
    return d


# --- schedules -------------------------------------------------------------------------


@dataclass(frozen=True)
class Schedule:
    """A publication rule in machine form.

    Weekly: released on `weekday` (Mon=0) at `release_time` UTC. The newest
    period in a release is the scheduled release date plus
    `period_offset_days`, moved back to a business day when
    `period_business_day` is set. Continuous (`weekday` None): a point every
    `interval`, due `interval` after the previous one.
    """

    frequency: str  # "weekly" or "continuous"
    weekday: int | None = None
    release_time: time = time(0, 0)
    period_offset_days: int = 0
    period_business_day: bool = False
    grace: timedelta = timedelta(0)  # calendar grace
    grace_business_days: int = 0  # business-day grace, added to `grace`
    holiday_slip: bool = False
    holiday_extra_grace_days: int = 0  # more business days of grace in a slipped week
    interval: timedelta | None = None


@dataclass(frozen=True)
class Release:
    """One scheduled publication."""

    at: datetime  # expected publication time, UTC
    period: date  # newest period it must contain
    deadline: datetime  # flagged late after this


def release_for_week(s: Schedule, monday: date) -> Release:
    anchor = monday + timedelta(days=s.weekday)
    period = anchor + timedelta(days=s.period_offset_days)
    if s.period_business_day:
        while not is_business_day(period):
            period -= timedelta(days=1)
    day = anchor
    slipped = 0
    if s.holiday_slip:
        slipped = sum(
            1 for i in range(s.weekday + 1) if is_federal_holiday(monday + timedelta(days=i))
        )
        for _ in range(slipped):
            day = add_business_days(day, 1)
        while not is_business_day(day):
            day += timedelta(days=1)
    at = datetime.combine(day, s.release_time, UTC)
    grace_days = s.grace_business_days + (s.holiday_extra_grace_days if slipped else 0)
    due = datetime.combine(add_business_days(day, grace_days), s.release_time, UTC)
    return Release(at, period, due + s.grace)


def releases(s: Schedule, now: datetime, weeks_back: int = 12, weeks_ahead: int = 2):
    """Scheduled releases around `now`, oldest first."""
    monday = now.date() - timedelta(days=now.weekday())
    return [
        release_for_week(s, monday + timedelta(weeks=w))
        for w in range(-weeks_back, weeks_ahead + 1)
    ]


class FreshnessStatus(str, Enum):
    OK = "ok"
    LATE = "late"
    NO_DATA = "no_data"


@dataclass(frozen=True)
class Freshness:
    status: FreshnessStatus
    latest_period: datetime | None
    # The next publication the store is waiting for: when it is (or was)
    # expected, the newest period it should bring, and when it turns late.
    # For LATE this is the missed one.
    expected_publication: datetime
    expected_period: datetime
    deadline: datetime


def _midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def schedule_freshness(s: Schedule, latest: datetime | None, now: datetime) -> Freshness:
    if s.frequency == "continuous":
        assert s.interval is not None
        if latest is None:
            exp = now
            return Freshness(FreshnessStatus.NO_DATA, None, exp, exp, exp + s.grace)
        exp = latest + s.interval
        deadline = exp + s.grace
        status = FreshnessStatus.LATE if now > deadline else FreshnessStatus.OK
        return Freshness(status, latest, exp, exp, deadline)

    rels = releases(s, now)
    if latest is None:
        due = [r for r in rels if r.at <= now] or rels[:1]
        r = due[-1]
        return Freshness(FreshnessStatus.NO_DATA, None, r.at, _midnight(r.period), r.deadline)
    have = latest.date()
    r = next((r for r in rels if r.period > have), rels[-1])
    status = FreshnessStatus.LATE if r.deadline <= now else FreshnessStatus.OK
    return Freshness(status, latest, r.at, _midnight(r.period), r.deadline)


def pending_release(s: Schedule, latest: datetime | None, now: datetime) -> Release | None:
    """The newest release already due (`at <= now`) whose period is not
    stored yet, or None. The scheduler polls more often while one is pending."""
    if s.frequency != "weekly":
        return None
    due = [r for r in releases(s, now, weeks_back=2, weeks_ahead=0) if r.at <= now]
    if not due:
        return None
    r = due[-1]
    if latest is not None and latest.date() >= r.period:
        return None
    return r


def next_release(s: Schedule, now: datetime) -> datetime | None:
    if s.frequency != "weekly":
        return None
    return next((r.at for r in releases(s, now, weeks_back=0) if r.at > now), None)


# Machine form of each collector's plain-language SeriesSpec.schedule. Grace
# left at zero here is filled from SeriesSpec.grace when the registry is built.
SCHEDULES: dict[str, Schedule] = {
    # Daily spot data released weekly on Wednesday, through the trading day
    # before. 17:00 UTC is after the 10:30 ET WPSR in both EST and EDT.
    eia._SPOT_SCHEDULE: Schedule(
        "weekly",
        weekday=2,
        release_time=time(17, 0),
        period_offset_days=-1,
        period_business_day=True,
        grace_business_days=1,
        holiday_slip=True,
    ),
    # Retail survey of Monday's prices, released Monday about 5 pm ET.
    eia._RETAIL_SCHEDULE: Schedule(
        "weekly",
        weekday=0,
        release_time=time(22, 0),
        period_offset_days=0,
        grace_business_days=1,
        holiday_slip=True,
        holiday_extra_grace_days=1,
    ),
    # WPSR, Wednesday 10:30 ET (15:30 UTC in winter), week ending Friday.
    eia._WPSR_SCHEDULE: Schedule(
        "weekly",
        weekday=2,
        release_time=time(15, 30),
        period_offset_days=-5,
        grace_business_days=1,
        holiday_slip=True,
    ),
    # Tuesday about 13:00 UTC, data through the Sunday before.
    portwatch._SCHEDULE: Schedule(
        "weekly", weekday=1, release_time=time(13, 0), period_offset_days=-2
    ),
    # Thursday, prices dated the Monday before. Time of day is a guess at
    # early afternoon Brussels; the two-day grace covers it.
    eu_oil_bulletin.SERIES[0].schedule: Schedule(
        "weekly", weekday=3, release_time=time(14, 0), period_offset_days=-3
    ),
    aisstream._SCHEDULE: Schedule("continuous", interval=timedelta(minutes=10)),
}


def resolve_schedule(spec: SeriesSpec) -> Schedule:
    try:
        s = SCHEDULES[spec.schedule]
    except KeyError:
        raise ValueError(f"series {spec.id}: no machine schedule for {spec.schedule!r}") from None
    if s.grace_business_days == 0 and s.grace == timedelta(0):
        s = replace(s, grace=spec.grace)
    return s


# --- collectors and the registry -------------------------------------------------------

FetchFn = Callable[[httpx.AsyncClient, date | None], Awaitable[list[Point]]]


@dataclass(frozen=True)
class Collector:
    """A collector as the scheduler sees it. `fetch` is None for a stream."""

    name: str  # its row in the status table
    series: tuple[SeriesSpec, ...]
    fetch: FetchFn | None = None
    # Each poll re-fetches from this far before the newest stored period, so
    # revisions within it land.
    revision_window: timedelta = timedelta(0)
    # Re-run the full five-year fetch this often, for revisions further back.
    full_refresh_every: timedelta | None = None

    @property
    def polled(self) -> bool:
        return self.fetch is not None


@dataclass(frozen=True)
class Entry:
    spec: SeriesSpec
    collector: str
    schedule: Schedule

    @property
    def id(self) -> str:
        return self.spec.id


class DuplicateSeriesError(ValueError):
    pass


@dataclass(frozen=True)
class Registry:
    collectors: tuple[Collector, ...]
    entries: dict[str, Entry] = field(repr=False)

    def __contains__(self, series_id: object) -> bool:
        return series_id in self.entries

    def __iter__(self):
        return iter(self.entries.values())

    def __len__(self) -> int:
        return len(self.entries)

    def get(self, series_id: str) -> Entry:
        """KeyError for an unregistered series."""
        return self.entries[series_id]

    def collector(self, name: str) -> Collector:
        for c in self.collectors:
            if c.name == name:
                return c
        raise KeyError(name)

    def for_collector(self, name: str) -> list[Entry]:
        return [e for e in self.entries.values() if e.collector == name]

    @property
    def polled(self) -> tuple[Collector, ...]:
        return tuple(c for c in self.collectors if c.polled)

    def freshness(self, series_id: str, latest: datetime | None, now: datetime) -> Freshness:
        return schedule_freshness(self.get(series_id).schedule, latest, now)


def build(collectors: Iterable[Collector]) -> Registry:
    collectors = tuple(collectors)
    names = [c.name for c in collectors]
    if len(set(names)) != len(names):
        raise DuplicateSeriesError(f"duplicate collector names in {names}")
    entries: dict[str, Entry] = {}
    for c in collectors:
        for spec in c.series:
            if spec.id in entries:
                other = entries[spec.id].collector
                raise DuplicateSeriesError(f"series {spec.id!r} in both {other} and {c.name}")
            entries[spec.id] = Entry(spec, c.name, resolve_schedule(spec))
    return Registry(collectors, entries)


# Lambdas look the fetch up at call time, so tests can patch module.fetch.
COLLECTORS: tuple[Collector, ...] = (
    Collector(
        "eia",
        tuple(eia.SERIES),
        lambda client, since=None: eia.fetch(client, since),
        revision_window=timedelta(days=14),
    ),
    Collector(
        "portwatch",
        tuple(portwatch.SERIES),
        lambda client, since=None: portwatch.fetch(client, since),
        revision_window=portwatch.REVISION_WINDOW,
        full_refresh_every=timedelta(days=28),
    ),
    Collector(
        "eu_oil_bulletin",
        tuple(eu_oil_bulletin.SERIES),
        lambda client, since=None: eu_oil_bulletin.fetch(client, since),
        # Re-read from the newest stored Monday: that keeps `since` inside the
        # collector's latest-file window, so polls never pull the 4.5 MB history.
        revision_window=timedelta(0),
    ),
    Collector("aisstream", tuple(aisstream.SERIES)),
)

REGISTRY: Registry = build(COLLECTORS)


def get(series_id: str) -> Entry:
    return REGISTRY.get(series_id)


def freshness(series_id: str, latest: datetime | None, now: datetime) -> Freshness:
    """Status of one series given its latest stored period and `now`."""
    return REGISTRY.freshness(series_id, latest, now)
