"""Shared data types for collectors.

A point is stored against its period: a day for daily series, the
week-ending date for weekly series, a 10-minute slot for live AIS.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class Frequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    TEN_MINUTES = "10min"


@dataclass(frozen=True)
class SeriesSpec:
    """One registry entry. `schedule` is a plain-language publication rule,
    e.g. "weekly, usually Wednesday"; `grace` is how late a publication may
    be before the series is flagged late."""

    id: str
    name: str
    unit: str
    source: str
    frequency: Frequency
    schedule: str
    grace: timedelta


@dataclass(frozen=True)
class Point:
    series: str
    period_end: datetime  # UTC, timezone-aware
    value: float
    provisional: bool = False

    def __post_init__(self) -> None:
        if self.period_end.tzinfo is None:
            raise ValueError("period_end must be timezone-aware UTC")
