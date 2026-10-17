"""App-level fixtures: a tmp_path store seeded with fixture points, a fake
clock, and the app with the scheduler disabled or faked."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from fuelwatch import calc
from fuelwatch.app import create_app
from fuelwatch.collectors.ais_tracker import SERIES_SHIPS
from fuelwatch.collectors.aisstream import AisStatus
from fuelwatch.models import Point
from fuelwatch.store import Store

NOW = datetime(2026, 10, 17, 12, tzinfo=UTC)  # a Saturday


class Clock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeAis:
    def __init__(self, status: AisStatus | None = None) -> None:
        self.status = status or AisStatus(connected=True)


class FakeScheduler:
    """Stands in for Scheduler: runs until stop, records what it saw."""

    def __init__(self, store, ais=None, log=None) -> None:
        self.store = store
        self.ais = ais
        self.collections = 0
        self.log = log if log is not None else []

    async def run(self, stop: asyncio.Event) -> None:
        self.log.append("scheduler started")
        await stop.wait()
        self.log.append("scheduler saw stop")
        await asyncio.sleep(0.05)  # drains its store calls
        self.store.latest(calc.BRENT)  # the store must still be open here
        self.log.append("scheduler done")


def midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def seed(store: Store) -> None:
    today = NOW.date()
    pts: list[Point] = []
    # Daily spot prices on weekdays, six years back, through Wednesday Oct 14.
    d = today - timedelta(days=6 * 365)
    while d <= date(2026, 10, 14):
        if d.weekday() < 5:
            n = (d - date(2020, 1, 1)).days
            pts.append(Point(calc.BRENT, midnight(d), 70 + n % 20))
            pts.append(Point(calc.WTI, midnight(d), 66 + n % 20))
        d += timedelta(days=1)
    # Weekly retail prices on Mondays, through Oct 12.
    d = date(2020, 10, 19)
    while d <= date(2026, 10, 12):
        pts.append(Point(calc.GASOLINE_US, midnight(d), 3.0 + (d.month % 5) / 10))
        pts.append(Point(calc.DIESEL_US, midnight(d), 3.6 + (d.month % 5) / 10))
        d += timedelta(days=7)
    # PortWatch Hormuz, daily through Sunday Oct 11; the two newest provisional.
    d = date(2025, 6, 1)
    while d <= date(2026, 10, 11):
        prov = d >= date(2026, 10, 10)
        pts.append(Point(calc.HORMUZ, midnight(d), 90 + d.day % 10, prov))
        d += timedelta(days=1)
    # Live AIS ship counts every 10 minutes for the last ten days.
    t = midnight(today - timedelta(days=9)) + timedelta(minutes=10)
    while t <= NOW:
        pts.append(Point(SERIES_SHIPS, t, 20 + t.minute // 10))
        t += timedelta(minutes=10)
    store.upsert_points(pts, fetched_at=NOW)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        seed(s)
    return path


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def make_client(db_path, clock):
    """make_client(**create_app kwargs) -> a started TestClient."""
    started = []

    def make(**kw):
        kw.setdefault("store_factory", lambda: Store(db_path))
        kw.setdefault("scheduler_factory", None)
        kw.setdefault("metrics_port", None)
        kw.setdefault("clock", clock)
        kw.setdefault("spot_data", False)
        client = TestClient(create_app(**kw), raise_server_exceptions=False)
        client.__enter__()
        started.append(client)
        return client

    yield make
    for c in started:
        c.__exit__(None, None, None)


@pytest.fixture
def client(make_client):
    return make_client()
