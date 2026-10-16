"""Live EIA API v2 contract tests. Run with FW_CONTRACT=1 and EIA_API_KEY set.

One request per route (four in all), so the run fits even DEMO_KEY's
rate limit. Each check confirms that every registered series ID exists,
has recent data, and arrives with the fields and units the parser expects.
"""

from datetime import UTC, date, datetime, timedelta

import pytest

from fuelwatch.collectors import eia
from fuelwatch.http import get_json, make_client

pytestmark = pytest.mark.contract

LOOKBACK = timedelta(days=35)
# Newest period may be at most this old. Spot: daily data released weekly,
# through the day before release. Retail: Monday survey, out Monday or a day
# or two later. WPSR: week ending Friday, out the following Wednesday or
# Thursday. Each allows one missed release (holiday) on top.
MAX_AGE = {
    "petroleum/pri/spt": timedelta(days=15),
    "petroleum/pri/gnd": timedelta(days=16),
    "petroleum/stoc/wstk": timedelta(days=21),
    "petroleum/pnp/wiup": timedelta(days=21),
}
FIELDS = {"period", "series", "series-description", "value", "units"}

_cache: dict[str, dict] = {}


async def _recent(group: eia.Group) -> dict:
    if group.route not in _cache:
        key = eia.api_key()
        start = datetime.now(UTC).date() - LOOKBACK
        async with make_client() as c:
            _cache[group.route] = await get_json(
                c, f"{eia.BASE_URL}{group.route}/data/", **eia.query_params(group, key, start)
            )
    return _cache[group.route]


@pytest.mark.parametrize("group", eia.GROUPS, ids=lambda g: g.route)
async def test_series_exist_recent_with_expected_fields_and_units(group):
    doc = await _recent(group)
    rows = doc["response"]["data"]
    assert doc["response"]["frequency"] == group.frequency.value
    today = datetime.now(UTC).date()

    for s in group.series:
        mine = [r for r in rows if r.get("series") == s.eia_id]
        assert mine, f"{s.eia_id} returned no rows in the last {LOOKBACK.days} days"
        for r in mine:
            assert FIELDS <= r.keys(), f"{s.eia_id} missing {FIELDS - r.keys()}"
            assert r["units"] == s.eia_units, f"{s.eia_id} units {r['units']!r}"
        newest = max(date.fromisoformat(r["period"]) for r in mine)
        assert today - newest <= MAX_AGE[group.route], f"{s.eia_id} newest is {newest}"

    points = eia.parse_response(doc, group)
    assert {p.series for p in points} == {s.spec.id for s in group.series}
    assert all(p.value != 0 for p in points)
    if group.frequency is eia.Frequency.WEEKLY:
        expected_weekday = 0 if group.route.endswith("/gnd") else 4  # Monday / Friday
        assert {p.period_end.weekday() for p in points} == {expected_weekday}
