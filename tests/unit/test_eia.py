"""EIA parser tests against recorded fixtures.

Fixtures in tests/fixtures/eia/ are real EIA API v2 responses recorded on
2026-10-16 with api_key=DEMO_KEY (the key is never echoed in the response
body), then trimmed to the first N rows of `response.data`. `response.total`
is left as recorded, so it is larger than the trimmed row count.
"""

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from fuelwatch.collectors import eia
from fuelwatch.http import ProviderError, make_client

FIX = Path(__file__).parent.parent / "fixtures" / "eia"
GROUP = {g.route.rsplit("/", 1)[1]: g for g in eia.GROUPS}


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text())


def utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=UTC)


def by_series(points):
    out: dict[str, dict[datetime, float]] = {}
    for p in points:
        out.setdefault(p.series, {})[p.period_end] = p.value
    return out


def test_registry_ids_unique_and_complete():
    ids = [s.id for s in eia.SERIES]
    assert len(ids) == len(set(ids)) == 4 + 14 + 4 + 1
    eia_ids = [s.eia_id for g in eia.GROUPS for s in g.series]
    assert len(eia_ids) == len(set(eia_ids))
    assert all(i.startswith("eia.") for i in ids)


def test_spot_prices_units_and_daily_periods():
    pts = eia.parse_response(load("spot_daily_recent.json"), GROUP["spt"])
    s = by_series(pts)
    assert s["eia.spot.brent"][utc(2026, 10, 14)] == 126.55
    assert s["eia.spot.wti"][utc(2026, 10, 14)] == 101.91
    assert s["eia.spot.nyh_gasoline"][utc(2026, 10, 14)] == 2.987
    assert s["eia.spot.nyh_ulsd"][utc(2026, 10, 14)] == 5.066
    for p in pts:
        assert p.period_end.tzinfo is UTC
        assert (p.period_end.hour, p.period_end.minute) == (0, 0)
        assert not p.provisional
    units = {x.id: x.unit for x in eia.SERIES}
    assert units["eia.spot.brent"] == "$/bbl"
    assert units["eia.spot.nyh_ulsd"] == "$/gal"


def test_missing_holiday_row_is_a_gap_not_zero():
    # Dec 26 2025 is a UK holiday: EIA has WTI and NYH rows but no Brent row.
    pts = eia.parse_response(load("spot_daily_yearend.json"), GROUP["spt"])
    s = by_series(pts)
    assert utc(2025, 12, 26) in s["eia.spot.wti"]
    assert utc(2025, 12, 26) not in s["eia.spot.brent"]
    # Dec 25 and Jan 1: no rows for anything.
    for series in s.values():
        assert utc(2025, 12, 25) not in series
        assert utc(2026, 1, 1) not in series
    assert all(p.value != 0 for p in pts)
    assert len(pts) == 31


@pytest.mark.parametrize("raw", [None, "", "NA", "--", "W", "0", 0, "0.0", "nan", True])
def test_empty_or_placeholder_values_become_gaps(raw):
    doc = load("spot_daily_recent.json")
    doc["response"]["data"][0]["value"] = raw
    first = doc["response"]["data"][0]
    pts = eia.parse_response(doc, GROUP["spt"])
    assert len(pts) == len(doc["response"]["data"]) - 1
    sid = {s.eia_id: s.spec.id for s in GROUP["spt"].series}[first["series"]]
    assert all(not (p.series == sid and p.period_end == utc(2026, 10, 14)) for p in pts)


def test_negative_price_is_kept():
    # WTI settled below zero in April 2020; that is data, not a gap.
    doc = load("spot_daily_recent.json")
    row = next(r for r in doc["response"]["data"] if r["series"] == "RWTC")
    row["value"] = "-36.98"
    s = by_series(eia.parse_response(doc, GROUP["spt"]))
    assert s["eia.spot.wti"][utc(2026, 10, 14)] == -36.98


def test_retail_monday_periods_all_regions():
    pts = eia.parse_response(load("retail_weekly_recent.json"), GROUP["gnd"])
    s = by_series(pts)
    assert len(s) == 14
    assert s["eia.retail.gasoline.us"][utc(2026, 10, 12)] == 4.331
    assert s["eia.retail.diesel.ca"][utc(2026, 10, 12)] == 8.091
    assert s["eia.retail.gasoline.padd1"][utc(2026, 10, 5)] == 4.147
    assert {p.period_end.weekday() for p in pts} == {0}  # Monday survey date


def test_wpsr_stocks_converted_to_million_barrels_on_friday():
    pts = eia.parse_response(load("wpsr_stocks_weekly_recent.json"), GROUP["wstk"])
    s = by_series(pts)
    assert s["eia.stocks.crude_commercial"][utc(2026, 10, 9)] == 415.577
    assert s["eia.stocks.gasoline"][utc(2026, 10, 9)] == 208.262
    assert s["eia.stocks.distillate"][utc(2026, 10, 9)] == 102.611
    assert s["eia.stocks.spr"][utc(2026, 10, 9)] == 282.983
    assert {p.period_end.weekday() for p in pts} == {4}  # week ending Friday
    assert {x.unit for x in eia.SERIES if x.id.startswith("eia.stocks.")} == {"million bbl"}


def test_refinery_utilization_percent():
    pts = eia.parse_response(load("wpsr_refinery_util_weekly_recent.json"), GROUP["wiup"])
    s = by_series(pts)["eia.refinery.utilization"]
    assert s[utc(2026, 10, 9)] == 93.5
    assert s[utc(2026, 9, 18)] == 94.0


def test_unexpected_units_is_a_schema_error():
    doc = load("wpsr_stocks_weekly_recent.json")
    doc["response"]["data"][0]["units"] = "MMBBL"
    with pytest.raises(ProviderError) as ei:
        eia.parse_response(doc, GROUP["wstk"])
    assert ei.value.error_class == "schema"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.pop("response"),
        lambda d: d["response"].__setitem__("data", None),
        lambda d: d["response"].__setitem__("frequency", "monthly"),
        lambda d: d["response"]["data"][0].__setitem__("period", "2026-10"),
    ],
)
def test_bad_shapes_are_schema_errors(mutate):
    doc = load("spot_daily_recent.json")
    mutate(doc)
    with pytest.raises(ProviderError) as ei:
        eia.parse_response(doc, GROUP["spt"])
    assert ei.value.error_class == "schema"


def test_rows_for_other_series_are_ignored():
    doc = load("spot_daily_recent.json")
    extra = copy.deepcopy(doc["response"]["data"][0])
    extra["series"] = "RSOMETHINGELSE"
    doc["response"]["data"].append(extra)
    n = len(eia.parse_response(doc, GROUP["spt"]))
    assert n == len(doc["response"]["data"]) - 1


def test_missing_key_is_config_error(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(ProviderError) as ei:
        eia.api_key()
    assert ei.value.error_class == "config"


@respx.mock
async def test_fetch_pages_and_sends_series_facets(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "planted-fake-eia-key")
    recorded = {
        "spt": load("spot_daily_recent.json"),
        "gnd": load("retail_weekly_recent.json"),
        "wstk": load("wpsr_stocks_weekly_recent.json"),
        "wiup": load("wpsr_refinery_util_weekly_recent.json"),
    }
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        route = request.url.path.rstrip("/").split("/")[-2]
        q = parse_qs(request.url.query.decode())
        seen.append((route, q))
        doc = copy.deepcopy(recorded[route])
        rows = doc["response"]["data"]
        # Serve the recorded rows in pages of 3 to exercise offset paging.
        off = int(q["offset"][0])
        doc["response"]["total"] = str(len(rows))
        doc["response"]["data"] = rows[off : off + 3]
        return httpx.Response(200, json=doc)

    respx.get(url__startswith=eia.BASE_URL).mock(side_effect=handler)
    async with make_client() as c:
        pts = await eia.fetch(c, since=date(2026, 9, 1))

    assert {p.series for p in pts} == {s.id for s in eia.SERIES}
    assert len(pts) == sum(len(d["response"]["data"]) for d in recorded.values())
    spt_calls = [q for r, q in seen if r == "spt"]
    assert len(spt_calls) == 4  # 12 rows in pages of 3
    q = spt_calls[0]
    assert q["start"] == ["2026-09-01"]
    assert q["frequency"] == ["daily"]
    assert sorted(q["facets[series][]"]) == sorted(
        ["RBRTE", "RWTC", "EER_EPMRU_PF4_Y35NY_DPG", "EER_EPD2DXL0_PF4_Y35NY_DPG"]
    )


@respx.mock
async def test_fetch_error_does_not_leak_key(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "planted-fake-eia-key")
    respx.get(url__startswith=eia.BASE_URL).mock(return_value=httpx.Response(403))
    async with make_client() as c:
        with pytest.raises(ProviderError) as ei:
            await eia.fetch(c, since=date(2026, 9, 1))
    assert "planted-fake-eia-key" not in str(ei.value)
    assert ei.value.error_class == "http_status"
