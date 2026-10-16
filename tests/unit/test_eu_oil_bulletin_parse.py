from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from fuelwatch.collectors import eu_oil_bulletin as ob
from fuelwatch.http import ProviderError, make_client

FIX = Path(__file__).parent.parent / "fixtures" / "eu_oil_bulletin"
LATEST = FIX / "latest_with_taxes_2026-10-12.xlsx"
HISTORY = FIX / "history_with_taxes_trimmed.xlsx"
PRICE_DATE = datetime(2026, 10, 12, tzinfo=UTC)


def _by_id(points):
    return {(p.series, p.period_end): p.value for p in points}


def test_series_registry():
    ids = {s.id for s in ob.SERIES}
    assert len(ids) == 10
    assert "eu_pump_petrol_eu27" in ids and "eu_pump_diesel_es" in ids
    assert all(s.unit == "EUR/l" for s in ob.SERIES)


def test_parse_latest_converts_to_eur_per_litre():
    points = ob.parse_latest(ob.read_rows(LATEST.read_bytes()))
    got = _by_id(points)
    assert len(points) == 10
    assert {p.period_end for p in points} == {PRICE_DATE}
    assert got[("eu_pump_petrol_de", PRICE_DATE)] == pytest.approx(2.214)
    assert got[("eu_pump_diesel_de", PRICE_DATE)] == pytest.approx(2.270)
    assert got[("eu_pump_petrol_fr", PRICE_DATE)] == pytest.approx(2.21222)
    assert got[("eu_pump_diesel_it", PRICE_DATE)] == pytest.approx(2.24073)
    assert got[("eu_pump_petrol_es", PRICE_DATE)] == pytest.approx(1.78954, abs=1e-5)
    # EU27 weighted average, not the euro-area row
    assert got[("eu_pump_petrol_eu27", PRICE_DATE)] == pytest.approx(2.00976, abs=1e-5)
    assert got[("eu_pump_diesel_eu27", PRICE_DATE)] == pytest.approx(2.13588, abs=1e-5)


def test_parse_history_matches_latest_for_same_week():
    hist = ob.parse_history(ob.read_rows(HISTORY.read_bytes(), ob.HISTORY_SHEET))
    latest = ob.parse_latest(ob.read_rows(LATEST.read_bytes()))
    h = _by_id(hist)
    for p in latest:
        assert h[(p.series, p.period_end)] == pytest.approx(p.value, abs=1e-5)


def test_parse_history_dates_footnotes_and_gaps():
    points = ob.parse_history(ob.read_rows(HISTORY.read_bytes(), ob.HISTORY_SHEET))
    dates = sorted({p.period_end for p in points})
    assert dates[0] == datetime(2005, 1, 3, tzinfo=UTC)
    assert dates[-1] == PRICE_DATE
    assert all(d.weekday() == 0 and d.tzinfo is UTC for d in dates)  # Mondays, UTC midnight
    # 2013-04-08 has a blank Spain petrol cell: a gap, not a zero
    gap_day = datetime(2013, 4, 8, tzinfo=UTC)
    on_gap_day = {p.series for p in points if p.period_end == gap_day}
    assert "eu_pump_petrol_es" not in on_gap_day
    assert "eu_pump_diesel_es" in on_gap_day
    assert all(p.value > 0 for p in points)


def test_parse_history_since_filter():
    rows = ob.read_rows(HISTORY.read_bytes(), ob.HISTORY_SHEET)
    points = ob.parse_history(rows, since=date(2026, 10, 1))
    assert {p.period_end for p in points} == {PRICE_DATE, datetime(2026, 10, 5, tzinfo=UTC)}


def _latest_rows():
    return [list(r) for r in ob.read_rows(LATEST.read_bytes())]


def test_blank_and_zero_cells_write_no_point():
    rows = _latest_rows()
    for r in rows:
        if r[0] == "Germany":
            r[1] = None
            r[2] = 0
    points = ob.parse_latest([tuple(r) for r in rows])
    ids = {p.series for p in points}
    assert "eu_pump_petrol_de" not in ids and "eu_pump_diesel_de" not in ids
    assert len(points) == 8


def test_unit_change_is_a_schema_error():
    rows = _latest_rows()
    rows[1][1] = "l"
    with pytest.raises(ProviderError) as ei:
        ob.parse_latest([tuple(r) for r in rows])
    assert ei.value.error_class == "schema"


def test_value_already_in_eur_per_litre_is_rejected():
    rows = _latest_rows()
    for r in rows:
        if r[0] == "France":
            r[1] = 2.21
    with pytest.raises(ProviderError) as ei:
        ob.parse_latest([tuple(r) for r in rows])
    assert ei.value.error_class == "out_of_range"


def test_missing_country_row_is_a_schema_error():
    rows = [tuple(r) for r in _latest_rows() if r[0] != "Spain"]
    with pytest.raises(ProviderError, match="Spain"):
        ob.parse_latest(rows)


def test_missing_history_column_is_a_schema_error():
    rows = [list(r) for r in ob.read_rows(HISTORY.read_bytes(), ob.HISTORY_SHEET)]
    rows[0] = [None if h == "IT_price_with_tax_diesel" else h for h in rows[0]]
    with pytest.raises(ProviderError, match="IT_price_with_tax_diesel"):
        ob.parse_history([tuple(r) for r in rows])


def test_not_xlsx():
    with pytest.raises(ProviderError) as ei:
        ob.read_rows(b"<html>not a workbook</html>")
    assert ei.value.error_class == "bad_xlsx"


XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@respx.mock
async def test_fetch_recent_since_uses_latest_file():
    respx.get(ob.LATEST_URL).mock(
        return_value=httpx.Response(
            200, content=LATEST.read_bytes(), headers={"content-type": XLSX}
        )
    )
    hist = respx.get(ob.HISTORY_URL)
    since = datetime.now(UTC).date() - timedelta(days=7)
    async with make_client() as c:
        points = await ob.fetch(c, since=since)
    assert not hist.called
    assert all(p.period_end.date() >= since for p in points)


@respx.mock
async def test_fetch_backfill_uses_history_file():
    respx.get(ob.HISTORY_URL).mock(
        return_value=httpx.Response(
            200, content=HISTORY.read_bytes(), headers={"content-type": XLSX}
        )
    )
    latest = respx.get(ob.LATEST_URL)
    async with make_client() as c:
        points = await ob.fetch(c)
    assert not latest.called
    cutoff = datetime.now(UTC) - timedelta(days=5 * 366)
    assert points and all(p.period_end >= cutoff for p in points)


@respx.mock
async def test_fetch_html_error_page_is_rejected():
    respx.get(ob.HISTORY_URL).mock(
        return_value=httpx.Response(
            200, text="<html></html>", headers={"content-type": "text/html"}
        )
    )
    async with make_client() as c:
        with pytest.raises(ProviderError) as ei:
            await ob.fetch(c)
    assert ei.value.error_class == "bad_content"


@respx.mock
async def test_fetch_http_error():
    respx.get(ob.HISTORY_URL).mock(return_value=httpx.Response(404))
    async with make_client() as c:
        with pytest.raises(ProviderError) as ei:
            await ob.fetch(c)
    assert ei.value.error_class == "http_status"
