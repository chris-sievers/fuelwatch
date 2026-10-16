"""Live IMF PortWatch contract tests. Run with FW_CONTRACT=1 (no key needed).

Checks the layer and its fields, that the five tracked chokepoints exist
under the expected IDs and names, that recent data arrives, and that paging
works against the real service.
"""

from datetime import UTC, date, datetime, timedelta

import pytest

from fuelwatch.collectors import portwatch as pw
from fuelwatch.http import get_json, make_client

pytestmark = pytest.mark.contract

# Data runs through Sunday and is published on Tuesday, so the newest day is
# 2 to 9 days old. Allow two extra days for a late publish (Oct 2026 slipped
# to Wednesday).
MAX_AGE = timedelta(days=11)
FIELD_TYPES = {
    "date": "esriFieldTypeDateOnly",
    "portid": "esriFieldTypeString",
    "portname": "esriFieldTypeString",
    "n_total": "esriFieldTypeInteger",
    "n_tanker": "esriFieldTypeInteger",
    "capacity_tanker": "esriFieldTypeInteger",
}

_cache: dict[str, object] = {}


async def _get(key: str, url: str, **params) -> object:
    if key not in _cache:
        async with make_client() as c:
            _cache[key] = await get_json(c, url, **params)
    return _cache[key]


async def test_layer_and_fields():
    layer = await _get("layer", pw.LAYER_URL, f="json")
    assert layer["name"] == "Daily_Chokepoints_Data"
    assert layer["type"] == "Table"
    assert layer["advancedQueryCapabilities"]["supportsPagination"] is True
    assert layer["maxRecordCount"] >= pw.PAGE_SIZE
    fields = {f["name"]: f["type"] for f in layer["fields"]}
    for name, ftype in FIELD_TYPES.items():
        assert fields.get(name) == ftype, name


async def test_all_five_chokepoints_present_with_expected_names():
    doc = await _get(
        "names",
        pw.QUERY_URL,
        where="1=1",
        outFields="portid,portname",
        returnDistinctValues="true",
        returnGeometry="false",
        f="json",
    )
    names = {f["attributes"]["portid"]: f["attributes"]["portname"] for f in doc["features"]}
    for cp in pw.CHOKEPOINTS:
        assert names.get(cp.portid) == cp.portname, cp


async def test_recent_data_for_every_series():
    start = datetime.now(UTC).date() - timedelta(days=21)
    async with make_client() as c:
        points = await pw.fetch(c, since=start)
    newest = max(p.period_end for p in points).date()
    assert datetime.now(UTC).date() - newest <= MAX_AGE, f"newest day {newest}"
    for s in pw.SERIES:
        mine = [p for p in points if p.series == s.id]
        assert mine, s.id
        assert max(p.period_end for p in mine).date() == newest, s.id
        assert sum(p.provisional for p in mine) == pw.PROVISIONAL_DAYS, s.id
    for p in points:
        if p.series.endswith(".all"):
            assert p.value == int(p.value) and p.value >= 0


async def test_paging_matches_unpaged_count():
    start = datetime.now(UTC).date() - timedelta(days=30)
    count = await _get(
        "count",
        pw.QUERY_URL,
        where=pw.query_params(start)["where"],
        returnCountOnly="true",
        f="json",
    )
    async with make_client() as c:
        rows = await pw.fetch_rows(c, start, page_size=40)
    assert len(rows) == count["count"] > 40
    assert len({(r.chokepoint.portid, r.day) for r in rows}) == len(rows)


async def test_history_reaches_five_years_back():
    five_years = datetime.now(UTC).date() - pw.BACKFILL
    doc = await _get(
        "oldest",
        pw.QUERY_URL,
        where="portid='chokepoint6'",
        outStatistics='[{"statisticType":"min","onStatisticField":"date",'
        '"outStatisticFieldName":"oldest"}]',
        f="json",
    )
    oldest = date.fromisoformat(doc["features"][0]["attributes"]["oldest"])
    assert oldest <= five_years
