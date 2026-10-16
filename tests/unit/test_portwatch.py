import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from fuelwatch.collectors import portwatch as pw
from fuelwatch.http import ProviderError, make_client

FIX = Path(__file__).parent.parent / "fixtures" / "portwatch"


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text())


def utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def recent_rows() -> list[pw.Row]:
    return pw.parse_page(load("recent_page1.json")) + pw.parse_page(load("recent_page2.json"))


def by_key(points):
    return {(p.series, p.period_end): p for p in points}


def test_series_registry():
    ids = [s.id for s in pw.SERIES]
    assert len(ids) == len(set(ids)) == 11
    assert {
        "portwatch.hormuz.all",
        "portwatch.hormuz.tankers",
        "portwatch.hormuz.tanker_tonnage",
    } <= set(ids)
    for key in ("bab_el_mandeb", "suez", "cape_of_good_hope", "malacca"):
        assert f"portwatch.{key}.all" in ids and f"portwatch.{key}.tankers" in ids
    assert all(s.source == "IMF PortWatch" and s.frequency.value == "daily" for s in pw.SERIES)


def test_layer_fixture_has_fields_the_parser_uses():
    layer = load("layer.json")
    assert layer["type"] == "Table" and layer["name"] == "Daily_Chokepoints_Data"
    fields = {f["name"]: f["type"] for f in layer["fields"]}
    assert fields["date"] == "esriFieldTypeDateOnly"
    for f in pw.OUT_FIELDS[1:]:
        assert f in fields


def test_parse_recent_week():
    points = pw.to_points(recent_rows())
    # 7 days x (3 Hormuz series + 4 x 2 others)
    assert len(points) == 7 * 11
    pts = by_key(points)
    hz = pts[("portwatch.hormuz.all", utc(2026, 10, 7))]
    assert hz.value == 6.0 and not hz.provisional
    assert pts[("portwatch.hormuz.tankers", utc(2026, 10, 7))].value == 1.0
    assert pts[("portwatch.hormuz.tanker_tonnage", utc(2026, 10, 7))].value == 40860.0
    assert pts[("portwatch.suez.all", utc(2026, 10, 5))].value == 36.0
    assert all(p.period_end.tzinfo is UTC and p.period_end.hour == 0 for p in points)


def test_two_newest_days_are_provisional():
    points = pw.to_points(recent_rows())
    prov = {p.period_end for p in points if p.provisional}
    assert prov == {utc(2026, 10, 10), utc(2026, 10, 11)}
    assert sum(p.provisional for p in points) == 2 * 11


def test_provisional_follows_the_newest_day_of_the_publication():
    # Drop the newest day: the two before it become the provisional ones.
    rows = [r for r in recent_rows() if r.day < date(2026, 10, 11)]
    prov = {p.period_end for p in pw.to_points(rows) if p.provisional}
    assert prov == {utc(2026, 10, 9), utc(2026, 10, 10)}


def test_missing_day_is_a_gap_not_a_zero():
    doc = load("recent_page1.json")
    doc["features"] = [
        f
        for f in doc["features"]
        if not (
            f["attributes"]["portid"] == "chokepoint1" and f["attributes"]["date"] == "2026-10-06"
        )
    ]
    pts = by_key(pw.to_points(pw.parse_page(doc)))
    assert ("portwatch.suez.all", utc(2026, 10, 6)) not in pts
    assert ("portwatch.suez.all", utc(2026, 10, 5)) in pts


def test_null_value_is_a_gap():
    doc = load("recent_page1.json")
    attrs = next(
        f["attributes"] for f in doc["features"] if f["attributes"]["portid"] == "chokepoint6"
    )
    attrs["n_tanker"] = None
    attrs["capacity_tanker"] = "n/a"
    day = date.fromisoformat(attrs["date"])
    pts = by_key(pw.to_points(pw.parse_page(doc)))
    t = utc(day.year, day.month, day.day)
    assert ("portwatch.hormuz.all", t) in pts
    assert ("portwatch.hormuz.tankers", t) not in pts
    assert ("portwatch.hormuz.tanker_tonnage", t) not in pts


def test_reported_zero_is_kept():
    # PortWatch reports zero crossings at Hormuz on 2026-03-04.
    pts = by_key(pw.to_points(pw.parse_page(load("hormuz_war_start.json"))))
    assert pts[("portwatch.hormuz.all", utc(2026, 3, 4))].value == 0.0
    assert pts[("portwatch.hormuz.all", utc(2026, 2, 27))].value == 64.0


def test_epoch_millisecond_dates_still_parse():
    assert pw.parse_day(1772928000000) == date(2026, 3, 8)
    with pytest.raises(ProviderError):
        pw.parse_day("08/03/2026")


def test_renamed_chokepoint_fails_loudly():
    doc = load("recent_page1.json")
    doc["features"][0]["attributes"]["portname"] = "Something Else"
    with pytest.raises(ProviderError) as ei:
        pw.parse_page(doc)
    assert ei.value.error_class == "schema"


def test_other_chokepoints_are_ignored():
    doc = load("recent_page1.json")
    extra = copy.deepcopy(doc["features"][0])
    extra["attributes"].update(portid="chokepoint9", portname="Dover Strait")
    doc["features"].append(extra)
    assert len(pw.parse_page(doc)) == len(doc["features"]) - 1


def test_arcgis_error_body_raises():
    with pytest.raises(ProviderError) as ei:
        pw.parse_page(load("error.json"))
    assert ei.value.error_class == "provider_error"
    assert "400" in str(ei.value)


def test_missing_features_raises():
    with pytest.raises(ProviderError):
        pw.parse_page({"exceededTransferLimit": False})


def test_duplicate_rows_keep_the_last():
    rows = recent_rows()
    r = rows[0]
    rows.append(pw.Row(r.day, r.chokepoint, {**r.values, "n_total": 999.0}))
    pts = by_key(pw.to_points(rows))
    assert pts[(pw.series_id(r.chokepoint, pw.ALL_SHIPS), utc(2026, 10, 5))].value == 999.0


def test_query_params():
    q = pw.query_params(date(2021, 10, 16), offset=2000)
    assert q["where"].endswith("AND date >= DATE '2021-10-16'")
    for c in pw.CHOKEPOINTS:
        assert f"'{c.portid}'" in q["where"]
    assert q["resultOffset"] == 2000 and q["resultRecordCount"] == 1000
    assert q["orderByFields"] == "date,portid"


@respx.mock
async def test_fetch_follows_paging():
    pages = [load("recent_page1.json"), load("recent_page2.json")]
    offsets = []

    def respond(request: httpx.Request) -> httpx.Response:
        qs = parse_qs(request.url.query.decode())
        offset = int(qs["resultOffset"][0])
        offsets.append(offset)
        return httpx.Response(200, json=pages[offset // 20])

    respx.get(pw.QUERY_URL).mock(side_effect=respond)
    async with make_client() as c:
        rows = await pw.fetch_rows(c, date(2026, 10, 5), page_size=20)
    assert offsets == [0, 20]
    assert len(rows) == 35


@respx.mock
async def test_fetch_backfills_five_years_by_default():
    route = respx.get(pw.QUERY_URL).mock(
        return_value=httpx.Response(200, json=load("recent_page2.json"))
    )
    async with make_client() as c:
        points = await pw.fetch(c)
    where = parse_qs(route.calls[0].request.url.query.decode())["where"][0]
    start = date.fromisoformat(where.rsplit("'", 2)[1])
    assert (datetime.now(UTC).date() - start).days == 5 * 366
    assert points and route.call_count == 1  # no exceededTransferLimit: one page


@respx.mock
async def test_fetch_stops_on_runaway_paging():
    page = load("recent_page1.json")  # always says exceededTransferLimit
    respx.get(pw.QUERY_URL).mock(return_value=httpx.Response(200, json=page))
    async with make_client() as c:
        with pytest.raises(ProviderError) as ei:
            await pw.fetch(c, since=date(2026, 10, 5))
    assert ei.value.error_class == "paging"


@respx.mock
async def test_fetch_raises_on_arcgis_error():
    respx.get(pw.QUERY_URL).mock(return_value=httpx.Response(200, json=load("error.json")))
    async with make_client() as c:
        with pytest.raises(ProviderError):
            await pw.fetch(c, since=date(2026, 10, 5))
