"""Live checks against the Commission's Weekly Oil Bulletin files.
Run with FW_CONTRACT=1."""

from datetime import UTC, datetime, timedelta

import pytest

from fuelwatch.collectors import eu_oil_bulletin as ob
from fuelwatch.http import make_client

pytestmark = pytest.mark.contract

ALL_IDS = {s.id for s in ob.SERIES}


async def test_latest_file_has_every_series_and_is_fresh():
    async with make_client(timeout=60) as c:
        points = await ob.fetch(c, since=datetime.now(UTC).date() - timedelta(days=13))
    assert {p.series for p in points} == ALL_IDS
    newest = ob.latest_date(points)
    assert newest.weekday() == 0  # prices are as of Monday
    # Published Thursday for the Monday before; allow a skipped holiday week.
    assert datetime.now(UTC) - newest < timedelta(days=24)


async def test_history_backfill_covers_five_years():
    async with make_client(timeout=120) as c:
        points = await ob.fetch(c)
    assert {p.series for p in points} == ALL_IDS
    dates = sorted({p.period_end for p in points})
    assert dates[0] < datetime.now(UTC) - timedelta(days=5 * 365 - 14)
    assert len(dates) > 240  # about 52 weeks a year, minus holiday gaps
    assert all(0.3 < p.value < 6 for p in points)


async def test_history_and_latest_agree_on_newest_week():
    async with make_client(timeout=120) as c:
        latest = ob.parse_latest(ob.read_rows(await ob._get_bytes(c, ob.LATEST_URL)))
        hist = ob.parse_history(
            ob.read_rows(await ob._get_bytes(c, ob.HISTORY_URL), ob.HISTORY_SHEET),
            since=latest[0].period_end.date(),
        )
    h = {(p.series, p.period_end): p.value for p in hist}
    for p in latest:
        assert h.get((p.series, p.period_end)) == pytest.approx(p.value, abs=1e-4)
