"""Time model and calculation tests: known inputs in a tmp_path Store,
expected values worked out by hand in the comments."""

from datetime import UTC, date, datetime, timedelta

import pytest

from fuelwatch import calc, jargon
from fuelwatch.collectors import eia, eu_oil_bulletin, portwatch
from fuelwatch.models import Point
from fuelwatch.store import Store

REF = date(2026, 2, 27)  # the default reference date, a Friday
NOW = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)  # a Friday


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "fuelwatch.db")
    yield s
    s.close()


@pytest.fixture
def settings(store):
    return store.settings()


def at(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def put(store: Store, series: str, values: dict, provisional: set = frozenset()) -> None:
    store.upsert_points(
        [Point(series, at(d), v, d in provisional) for d, v in values.items()],
        fetched_at=NOW,
    )


def days(start: date, end: date, step: int = 1):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=step)


def weekdays(start: date, end: date):
    return (d for d in days(start, end) if d.weekday() < 5)


WEEKLY_STOCKS = (
    "eia.stocks.crude_commercial",
    "eia.stocks.gasoline",
    "eia.stocks.distillate",
    "eia.stocks.spr",
)


# --- defaults and series ids ----------------------------------------------------------


def test_default_settings(settings):
    assert settings.reference_date == REF
    assert settings.baseline_days == 90
    assert settings.tank_gallons == 15


def test_every_series_id_exists_in_a_collector():
    known = {s.id for s in (*eia.SERIES, *portwatch.SERIES, *eu_oil_bulletin.SERIES)}
    used = {calc.BRENT, calc.WTI, calc.CRUDE_STOCKS, calc.HORMUZ, *WEEKLY_STOCKS}
    used |= set(calc.CHOKEPOINT_SERIES.values())
    used |= {calc.gasoline(r) for r in calc.REGIONS} | {calc.diesel(r) for r in calc.REGIONS}
    assert used <= known
    assert set(calc.REGION_NAMES) == set(calc.REGIONS)


def test_now_must_be_utc(store, settings):
    with pytest.raises(ValueError):
        calc.change_since_war(store, calc.BRENT, now=datetime(2026, 10, 16), settings=settings)


# --- change since the war began --------------------------------------------------------


def test_change_since_war(store, settings):
    put(store, calc.BRENT, {date(2026, 2, 26): 69.0, REF: 70.0, date(2026, 10, 14): 91.0})
    c = calc.change_since_war(store, calc.BRENT, now=NOW, settings=settings)
    assert c.available and c.reason is None
    assert c.value == pytest.approx(21.0)  # 91 - 70
    assert c.percent == pytest.approx(30.0)  # 21 / 70
    assert c.base == calc.DatedValue(REF, 70.0)
    assert c.current == calc.DatedValue(date(2026, 10, 14), 91.0)
    assert c.as_of == date(2026, 10, 14)
    assert c.unit == "$/bbl"
    assert c.settings == {"reference_date": REF}


@pytest.mark.parametrize(
    "ref",
    [date(2026, 2, 28), date(2026, 3, 1)],  # Saturday, Sunday
    ids=["saturday", "sunday"],
)
def test_weekend_reference_date_uses_friday(store, ref):
    store.set_setting("reference_date", ref.isoformat())
    put(store, calc.BRENT, {REF: 70.0, date(2026, 3, 2): 80.0, date(2026, 10, 14): 91.0})
    c = calc.change_since_war(store, calc.BRENT, now=NOW, settings=store.settings())
    assert c.base == calc.DatedValue(REF, 70.0)
    assert c.settings == {"reference_date": ref}


def test_holiday_reference_date_uses_last_trading_day(store):
    # Presidents' Day, Monday Feb 16 2026: no WTI price that day.
    store.set_setting("reference_date", "2026-02-16")
    put(store, calc.WTI, {date(2026, 2, 13): 63.0, date(2026, 2, 17): 64.0, NOW.date(): 80.0})
    c = calc.change_since_war(store, calc.WTI, now=NOW, settings=store.settings())
    assert c.base == calc.DatedValue(date(2026, 2, 13), 63.0)
    assert c.value == pytest.approx(17.0)


def test_reference_date_before_weekly_period_date(store, settings):
    # Stocks are dated by the week-ending Friday; with the reference date on
    # Thursday Feb 26 the reading for the week ending Fri Feb 27 is not yet
    # "on or before" it, so the week ending Feb 20 is used.
    store.set_setting("reference_date", "2026-02-26")
    s = store.settings()
    put(store, calc.CRUDE_STOCKS, {date(2026, 2, 20): 420.0, REF: 425.0, date(2026, 10, 9): 400.0})
    c = calc.change_since_war(store, calc.CRUDE_STOCKS, now=NOW, settings=s)
    assert c.base == calc.DatedValue(date(2026, 2, 20), 420.0)
    assert c.value == pytest.approx(-20.0)


def test_weekly_retail_aligns_on_its_monday(store, settings):
    # Retail is stored at its Monday survey date (Feb 23), not converted to a
    # Friday; Friday Feb 27 picks up that Monday's survey.
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 3.0, date(2026, 3, 2): 3.2})
    c = calc.change_since_war(store, calc.GASOLINE_US, now=NOW, settings=settings)
    assert c.base == calc.DatedValue(date(2026, 2, 23), 3.0)
    assert c.current.day == date(2026, 3, 2)


def test_reference_date_before_any_data(store, settings):
    put(store, calc.BRENT, {date(2026, 3, 2): 80.0, date(2026, 10, 14): 91.0})
    c = calc.change_since_war(store, calc.BRENT, now=NOW, settings=settings)
    assert c.value is None and c.percent is None
    assert "reference date 2026-02-27" in c.reason
    assert c.current == calc.DatedValue(date(2026, 10, 14), 91.0)  # still shown
    assert c.as_of == date(2026, 10, 14)


def test_no_data_at_all(store, settings):
    c = calc.change_since_war(store, calc.BRENT, now=NOW, settings=settings)
    assert c.value is None and c.as_of is None and not c.available
    assert c.reason == "no data yet for eia.spot.brent"


def test_data_after_now_is_ignored(store, settings):
    put(store, calc.BRENT, {REF: 70.0, date(2026, 10, 14): 91.0, date(2026, 10, 19): 99.0})
    c = calc.change_since_war(store, calc.BRENT, now=NOW, settings=settings)
    assert c.current.value == 91.0


def test_zero_base_gives_no_percent(store, settings):
    put(store, "portwatch.hormuz.tankers", {REF: 0.0, date(2026, 10, 11): 4.0})
    c = calc.change_since_war(store, "portwatch.hormuz.tankers", now=NOW, settings=settings)
    assert c.value == 4.0 and c.percent is None


# --- change since the last reading -----------------------------------------------------


def test_change_since_last(store):
    put(store, calc.DIESEL_US, {date(2026, 10, 5): 4.0, date(2026, 10, 12): 4.1})
    c = calc.change_since_last(store, calc.DIESEL_US, now=NOW)
    assert c.value == pytest.approx(0.1)
    assert c.percent == pytest.approx(2.5)  # 0.1 / 4.0
    assert c.base.day == date(2026, 10, 5) and c.as_of == date(2026, 10, 12)
    assert c.settings == {}


def test_change_since_last_old_previous_reading(store):
    # The previous reading is outside the fast lookback window; still found.
    put(store, calc.DIESEL_US, {date(2025, 1, 6): 3.5, date(2026, 10, 12): 4.2})
    c = calc.change_since_last(store, calc.DIESEL_US, now=NOW)
    assert c.base.day == date(2025, 1, 6) and c.value == pytest.approx(0.7)


def test_change_since_last_one_reading(store):
    put(store, calc.DIESEL_US, {date(2026, 10, 12): 4.1})
    c = calc.change_since_last(store, calc.DIESEL_US, now=NOW)
    assert c.value is None and c.reason == "only one reading so far"
    assert c.current.value == 4.1


def test_change_since_last_settled_only(store):
    put(
        store,
        calc.HORMUZ,
        {date(2026, 10, 9): 10.0, date(2026, 10, 10): 12.0, date(2026, 10, 11): 50.0},
        provisional={date(2026, 10, 11)},
    )
    c = calc.change_since_last(store, calc.HORMUZ, now=NOW, settled_only=True)
    assert (c.base.value, c.current.value) == (10.0, 12.0)


# --- percent of normal ----------------------------------------------------------------

BASELINE_START = date(2025, 11, 30)  # REF minus 89 days: 90 days including REF


def hormuz_history(store, recent: dict, provisional: dict | None = None, series=calc.HORMUZ):
    base = {d: 100.0 for d in days(BASELINE_START, REF)}
    base[BASELINE_START - timedelta(days=1)] = 10_000.0  # just outside the baseline
    base[REF + timedelta(days=1)] = 10_000.0  # the war's first day: outside too
    put(store, series, base | recent | (provisional or {}), provisional=set(provisional or {}))


def test_percent_of_normal(store, settings):
    # Settled days Oct 5..11 with two real zero days; Oct 12, 13 provisional.
    recent = dict(zip(days(date(2026, 10, 5), date(2026, 10, 11)), [10, 20, 30, 0, 0, 40, 50]))
    recent[date(2026, 10, 4)] = 999.0  # the eighth day back: outside the window
    hormuz_history(store, recent, {date(2026, 10, 12): 500.0, date(2026, 10, 13): 500.0})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.reason is None
    assert r.recent_average == pytest.approx(150 / 7)  # zeros count
    assert r.recent_days == 7
    assert (r.recent_start, r.recent_end) == (date(2026, 10, 5), date(2026, 10, 11))
    assert r.baseline_average == pytest.approx(100.0)
    assert r.baseline_days_with_data == 90
    assert (r.baseline_start, r.baseline_end) == (BASELINE_START, REF)
    assert r.value == pytest.approx(150 / 7)  # 21.43% of normal
    assert r.as_of == date(2026, 10, 11)  # the newest settled day, not Oct 13
    assert r.settings == {"reference_date": REF, "baseline_days": 90}


def test_percent_of_normal_all_zero_week_is_zero_not_missing(store, settings):
    hormuz_history(store, {d: 0.0 for d in days(date(2026, 10, 5), date(2026, 10, 11))})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.available and r.value == 0.0


def test_percent_of_normal_follows_baseline_setting(store):
    store.set_setting("baseline_days", "30")
    put(store, calc.HORMUZ, {d: 50.0 for d in days(REF - timedelta(days=29), REF)})
    put(store, calc.HORMUZ, {d: 100.0 for d in days(REF - timedelta(days=89), REF - timedelta(30))})
    put(store, calc.HORMUZ, {d: 25.0 for d in days(date(2026, 10, 5), date(2026, 10, 11))})
    r = calc.percent_of_normal(store, now=NOW, settings=store.settings())
    assert r.baseline_average == 50.0 and r.value == pytest.approx(50.0)
    assert r.settings["baseline_days"] == 30


def test_percent_of_normal_window_with_gaps(store, settings):
    # 5 of 7 days present: averaged over the 5, gaps are not zeros.
    hormuz_history(store, {date(2026, 10, d): 20.0 for d in (5, 6, 8, 10, 11)})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.recent_days == 5 and r.value == pytest.approx(20.0)


def test_percent_of_normal_too_few_recent_days(store, settings):
    hormuz_history(store, {date(2026, 10, d): 20.0 for d in (5, 8, 10, 11)})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.value is None and "only 4 of the last 7" in r.reason
    assert r.as_of == date(2026, 10, 11)


def test_percent_of_normal_reference_date_before_data(store, settings):
    put(store, calc.HORMUZ, {d: 20.0 for d in days(date(2026, 10, 1), date(2026, 10, 11))})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.value is None and "only 0 of the 90 days" in r.reason
    assert r.recent_average == pytest.approx(20.0)  # the recent side is still reported


def test_percent_of_normal_thin_baseline(store, settings):
    put(store, calc.HORMUZ, {d: 100.0 for d in days(REF - timedelta(days=43), REF)})  # 44 days
    put(store, calc.HORMUZ, {d: 20.0 for d in days(date(2026, 10, 1), date(2026, 10, 11))})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.value is None and "only 44 of the 90" in r.reason and "need 45" in r.reason


def test_percent_of_normal_zero_baseline(store, settings):
    put(store, calc.HORMUZ, {d: 0.0 for d in days(BASELINE_START, REF)})
    put(store, calc.HORMUZ, {d: 20.0 for d in days(date(2026, 10, 1), date(2026, 10, 11))})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.value is None and r.reason == "the normal level is zero"


def test_percent_of_normal_no_data(store, settings):
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.value is None and r.as_of is None


def test_percent_of_normal_ignores_provisional_baseline_days(store, settings):
    # A provisional day inside the baseline would be odd, but it is excluded.
    hormuz_history(store, {d: 50.0 for d in days(date(2026, 10, 5), date(2026, 10, 11))})
    put(store, calc.HORMUZ, {REF: 10_000.0}, provisional={REF})
    r = calc.percent_of_normal(store, now=NOW, settings=settings)
    assert r.baseline_days_with_data == 89 and r.baseline_average == 100.0


def test_percent_of_normal_series(store, settings):
    # Oct 1..11 settled at 10, 20, ..., 110; Oct 12 provisional.
    recent = {date(2026, 10, d): 10.0 * d for d in range(1, 12)}
    hormuz_history(store, recent, {date(2026, 10, 12): 900.0})
    r = calc.percent_of_normal_series(store, now=NOW, settings=settings, start=date(2026, 10, 9))
    # Oct 9: days 3..9 -> mean 60; Oct 10: 4..10 -> 70; Oct 11: 5..11 -> 80.
    assert r.points == (
        calc.DatedValue(date(2026, 10, 9), pytest.approx(60.0)),
        calc.DatedValue(date(2026, 10, 10), pytest.approx(70.0)),
        calc.DatedValue(date(2026, 10, 11), pytest.approx(80.0)),
    )
    assert r.value == pytest.approx(80.0) and r.as_of == date(2026, 10, 11)
    # Oct 4 has only 4 days (1..4) behind it, so the series starts Oct 5.
    full = calc.percent_of_normal_series(store, now=NOW, settings=settings, start=date(2026, 10, 1))
    assert full.points[0].day == date(2026, 10, 5)


def test_chokepoints_vs_normal(store, settings):
    suez = calc.CHOKEPOINT_SERIES["suez"]
    hormuz_history(
        store, {d: 40.0 for d in days(date(2026, 10, 5), date(2026, 10, 11))}, None, suez
    )
    out = calc.chokepoints_vs_normal(store, now=NOW, settings=settings)
    assert set(out) == {"hormuz", "bab_el_mandeb", "suez", "cape_of_good_hope", "malacca"}
    assert out["suez"].value == pytest.approx(40.0)
    assert out["hormuz"].value is None and out["hormuz"].reason


# --- cost to fill a tank --------------------------------------------------------------


def test_tank_cost(store, settings):
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 3.0, date(2026, 10, 12): 4.0})
    t = calc.tank_cost(store, now=NOW, settings=settings)
    assert t.value == pytest.approx(60.0)  # 4.00 x 15
    assert t.then == pytest.approx(45.0) and t.then_as_of == date(2026, 2, 23)
    assert t.change == pytest.approx(15.0) and t.percent == pytest.approx(100 / 3)
    assert t.as_of == date(2026, 10, 12) and t.unit == "$"
    assert t.settings == {"tank_gallons": 15.0, "reference_date": REF}


def test_tank_cost_custom_tank_and_diesel(store):
    store.set_setting("tank_gallons", "20")
    put(store, calc.DIESEL_US, {date(2026, 2, 23): 3.5, date(2026, 10, 12): 4.25})
    t = calc.tank_cost(store, calc.DIESEL_US, now=NOW, settings=store.settings())
    assert (t.value, t.then) == (pytest.approx(85.0), pytest.approx(70.0))
    assert t.settings["tank_gallons"] == 20.0


def test_tank_cost_missing(store, settings):
    assert calc.tank_cost(store, now=NOW, settings=settings).value is None
    put(store, calc.GASOLINE_US, {date(2026, 10, 12): 4.0})  # nothing by the reference date
    t = calc.tank_cost(store, now=NOW, settings=settings)
    assert t.value == pytest.approx(60.0) and t.then is None and t.change is None


# --- Brent-WTI gap --------------------------------------------------------------------


def test_brent_wti_gap(store):
    put(store, calc.BRENT, {date(2026, 10, 9): 90.0, date(2026, 10, 12): 92.0})
    # Oct 12 is Columbus Day here: no WTI that day, so the newest pair is Oct 9.
    put(store, calc.WTI, {date(2026, 10, 9): 84.5, date(2026, 10, 8): 80.0})
    assert calc.brent_wti_gap_series(store, end=NOW) == [calc.DatedValue(date(2026, 10, 9), 5.5)]
    g = calc.brent_wti_gap(store, now=NOW)
    assert g.value == pytest.approx(5.5) and g.as_of == date(2026, 10, 9)
    assert g.unit == "$/bbl"


def test_brent_wti_gap_old_pair_and_missing(store):
    assert calc.brent_wti_gap(store, now=NOW).reason
    put(store, calc.BRENT, {date(2025, 1, 2): 75.0})
    put(store, calc.WTI, {date(2025, 1, 2): 73.0})
    assert calc.brent_wti_gap(store, now=NOW).value == pytest.approx(2.0)


# --- diesel premium -------------------------------------------------------------------


def test_diesel_premium(store):
    mon = date(2026, 10, 12)
    put(store, calc.DIESEL_US, {date(2026, 10, 5): 3.70, mon: 3.75})
    put(store, calc.GASOLINE_US, {date(2026, 10, 5): 3.10, mon: 3.12})
    put(store, calc.diesel("padd3"), {mon: 3.40})
    put(store, calc.gasoline("padd3"), {mon: 2.80})
    p = calc.diesel_premium(store, now=NOW)
    assert p.value == pytest.approx(63.0) and p.as_of == mon and p.unit == "cents/gal"
    series = calc.diesel_premium_series(store, end=NOW)
    assert [round(x.value, 6) for x in series] == [60.0, 63.0]
    all_ = calc.diesel_premiums(store, now=NOW)
    assert all_["padd3"].value == pytest.approx(60.0)
    assert all_["ca"].value is None and "eia.retail.diesel.ca" in all_["ca"].reason


# --- crude's share of the pump price --------------------------------------------------


def brent_daily(store, start=date(2026, 9, 1), end=date(2026, 10, 14), skip=()):
    put(
        store,
        calc.BRENT,
        {d: 100.0 + d.toordinal() % 50 for d in weekdays(start, end) if d not in skip},
    )


def test_crude_share_uses_brent_two_weeks_before(store):
    brent_daily(store)
    put(store, calc.BRENT, {date(2026, 9, 28): 84.0})  # Monday, two weeks before Oct 12
    put(store, calc.GASOLINE_US, {date(2026, 10, 5): 3.5, date(2026, 10, 12): 3.0})
    s = calc.crude_share(store, now=NOW)
    assert s.estimate is True
    assert s.brent_target == date(2026, 9, 28)
    assert s.brent == calc.DatedValue(date(2026, 9, 28), 84.0)
    assert s.value == pytest.approx(66.6667, rel=1e-4)  # 84 / 42 = 2.00; 2.00 / 3.00
    assert s.pump == calc.DatedValue(date(2026, 10, 12), 3.0)
    assert s.as_of == date(2026, 10, 12)


def test_crude_share_target_on_holiday_uses_day_before(store):
    # Target Mon Sep 7 2026 (Labor Day, no price): Friday Sep 4 is used.
    brent_daily(store, skip={date(2026, 9, 7)})
    put(store, calc.BRENT, {date(2026, 9, 4): 63.0})
    put(store, calc.GASOLINE_US, {date(2026, 9, 21): 2.5})
    s = calc.crude_share(store, now=datetime(2026, 9, 25, tzinfo=UTC))
    assert s.brent_target == date(2026, 9, 7)
    assert s.brent.day == date(2026, 9, 4)
    assert s.value == pytest.approx(60.0)  # 63 / 42 = 1.50; 1.50 / 2.50


def test_crude_share_brent_too_old(store):
    put(store, calc.BRENT, {date(2026, 9, 18): 84.0})  # 10 days before the target
    put(store, calc.GASOLINE_US, {date(2026, 10, 12): 3.0})
    s = calc.crude_share(store, now=NOW)
    assert s.value is None and s.estimate and "2026-09-28" in s.reason


def test_crude_share_missing_pump(store):
    s = calc.crude_share(store, now=NOW)
    assert s.value is None and s.estimate and s.as_of is None


# --- week of year and typical range ---------------------------------------------------


@pytest.mark.parametrize(
    ("d", "expected"),
    [
        (date(2026, 10, 9), (2026, 41)),
        (date(2026, 1, 2), (2026, 1)),  # Friday of ISO week 1
        (date(2025, 1, 3), (2025, 1)),
        (date(2024, 12, 27), (2024, 52)),
        (date(2021, 1, 1), (2020, 52)),  # Friday of 2020-W53, folded into 52
        (date(2027, 1, 1), (2026, 52)),  # Friday of 2026-W53
        (date(2016, 1, 1), (2015, 52)),  # Friday of 2015-W53
        (date(2026, 2, 23), (2026, 9)),  # a Monday retail date
    ],
)
def test_week_of_year(d, expected):
    assert calc.week_of_year(d) == expected


def fridays(year: int, week: int) -> date:
    return date.fromisocalendar(year, week, 5)


def test_typical_range(store):
    # Week 41 Fridays in 2021..2025 and 2020 (too old to count).
    vals = {2020: 300.0, 2021: 420.0, 2022: 410.0, 2023: 430.0, 2024: 415.0, 2025: 425.0}
    put(store, calc.CRUDE_STOCKS, {fridays(y, 41): v for y, v in vals.items()})
    put(store, calc.CRUDE_STOCKS, {fridays(2025, 40): 999.0, fridays(2025, 42): 1.0})  # neighbours
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 400.0})  # 2026-W41
    r = calc.typical_range(store, now=NOW)
    assert (r.low, r.high) == (410.0, 430.0)
    assert r.week == 41 and r.years == (2021, 2022, 2023, 2024, 2025)
    assert r.value == 400.0 and r.position == "below"
    assert r.as_of == date(2026, 10, 9) and r.unit == "million bbl"
    assert r.settings == {"years": 5}


def test_typical_range_position(store):
    put(store, calc.CRUDE_STOCKS, {fridays(y, 41): 400.0 + y - 2021 for y in range(2021, 2026)})
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 402.0})
    assert calc.typical_range(store, now=NOW).position == "within"
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 404.5})
    assert calc.typical_range(store, now=NOW).position == "above"
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 404.0})  # on the edge: within
    assert calc.typical_range(store, now=NOW).position == "within"


def test_typical_range_missing_weeks(store):
    # 2022 and 2024 have no week-41 reading: the range uses the other three.
    put(store, calc.CRUDE_STOCKS, {fridays(y, 41): 400.0 + y for y in (2021, 2023, 2025)})
    put(store, calc.CRUDE_STOCKS, {fridays(2022, 40): 1.0, fridays(2024, 42): 9999.0})
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 450.0})
    r = calc.typical_range(store, now=NOW)
    assert r.years == (2021, 2023, 2025)
    assert (r.low, r.high) == (2421.0, 2425.0)


def test_typical_range_too_few_years(store):
    put(store, calc.CRUDE_STOCKS, {fridays(y, 41): 400.0 for y in (2024, 2025)})
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 9): 450.0})
    r = calc.typical_range(store, now=NOW)
    assert r.value == 450.0  # the reading itself is still there
    assert r.low is None and r.high is None and r.position is None
    assert "only 2 of the previous 5 years" in r.reason and "week 41" in r.reason


def test_typical_range_week_53(store):
    # 2026 has an ISO week 53 whose Friday is Jan 1 2027; it uses week 52 of
    # 2021..2025. 2020's week 53 (Fri Jan 1 2021) is too old to count here.
    put(store, calc.CRUDE_STOCKS, {fridays(y, 52): 400.0 + y - 2021 for y in range(2021, 2026)})
    put(store, calc.CRUDE_STOCKS, {date(2027, 1, 1): 410.0})
    r = calc.typical_range(store, now=datetime(2027, 1, 8, tzinfo=UTC))
    assert r.week == 52 and (r.low, r.high) == (400.0, 404.0)
    assert r.position == "above"


def test_typical_range_includes_a_past_week_53(store):
    # For 2025-W52, year 2020 contributes both its week 52 and week 53.
    put(store, calc.CRUDE_STOCKS, {fridays(y, 52): 400.0 for y in range(2021, 2025)})
    put(store, calc.CRUDE_STOCKS, {fridays(2020, 52): 390.0, fridays(2020, 53): 380.0})
    put(store, calc.CRUDE_STOCKS, {fridays(2025, 52): 395.0})
    r = calc.typical_range(store, now=datetime(2026, 1, 2, tzinfo=UTC))
    assert r.years == (2020, 2021, 2022, 2023, 2024)
    assert (r.low, r.high) == (380.0, 400.0)


def test_typical_range_on_a_date(store):
    put(store, calc.CRUDE_STOCKS, {fridays(y, 9): 400.0 + y - 2021 for y in range(2021, 2026)})
    put(store, calc.CRUDE_STOCKS, {REF: 401.0, date(2026, 10, 9): 300.0})
    r = calc.typical_range(store, now=NOW, on=date(2026, 3, 1))  # a Sunday: uses Fri Feb 27
    assert r.as_of == REF and r.week == 9 and r.position == "within"


def test_typical_range_no_data(store):
    r = calc.typical_range(store, now=NOW)
    assert r.value is None and r.reason == "no data yet for eia.stocks.crude_commercial"


def test_typical_range_series(store):
    sid = "eia.stocks.gasoline"
    put(store, sid, {fridays(y, w): 200.0 + w for y in range(2021, 2026) for w in (40, 41)})
    put(store, sid, {fridays(2026, 40): 230.0, fridays(2026, 41): 250.0, fridays(2026, 42): 1.0})
    out = calc.typical_range_series(store, sid, start=date(2026, 9, 1), end=date(2026, 10, 10))
    assert out == [
        calc.RangePoint(fridays(2026, 40), 230.0, 240.0, 240.0),
        calc.RangePoint(fridays(2026, 41), 250.0, 241.0, 241.0),
    ]
    out = calc.typical_range_series(store, sid, start=date(2026, 10, 12), end=NOW)
    assert out == [calc.RangePoint(fridays(2026, 42), 1.0, None, None)]


# --- headline tiles and the summary ---------------------------------------------------


def scenario(store, *, brent, gas, hormuz, stocks, diesel=4.0):
    """A full store: values at the reference date are Brent 70, gasoline 3.00,
    diesel 3.50, Hormuz 100 a day, crude stocks 400 (range 395..405)."""
    put(store, calc.BRENT, {d: 70.0 for d in weekdays(date(2025, 11, 1), REF)})
    put(store, calc.BRENT, {d: brent - 1 for d in weekdays(date(2026, 3, 2), date(2026, 10, 13))})
    put(store, calc.BRENT, {date(2026, 10, 14): brent})
    mondays = list(days(date(2025, 11, 3), date(2026, 10, 12), 7))
    put(store, calc.GASOLINE_US, {d: 3.0 if d <= REF else gas for d in mondays})
    put(store, calc.DIESEL_US, {d: 3.5 if d <= REF else diesel for d in mondays})
    hormuz_history(
        store,
        {d: hormuz for d in days(date(2026, 3, 1), date(2026, 10, 11))},
        {date(2026, 10, 12): 0.0, date(2026, 10, 13): 0.0},
    )
    put(
        store,
        calc.CRUDE_STOCKS,
        {fridays(y, 41): 395.0 + 2.5 * (y - 2021) for y in range(2021, 2026)},
    )
    put(store, calc.CRUDE_STOCKS, {REF: 400.0, date(2026, 10, 2): 401.0, date(2026, 10, 9): stocks})


def test_tiles(store, settings):
    scenario(store, brent=91.0, gas=3.41, hormuz=12.0, stocks=380.0)
    ts = {t.key: t for t in calc.tiles(store, now=NOW, settings=settings)}
    assert list(ts) == ["brent", "gasoline", "diesel", "tank", "hormuz", "crude_stocks"]

    b = ts["brent"]
    assert b.label == "Brent crude" and b.value == 91.0 and b.as_of == date(2026, 10, 14)
    assert b.since_last.value == pytest.approx(1.0)
    assert b.since_war.value == pytest.approx(21.0) and b.since_war.percent == pytest.approx(30.0)
    assert b.sparkline[-1] == calc.DatedValue(date(2026, 10, 14), 91.0)
    assert b.sparkline[0].day >= date(2026, 10, 14) - timedelta(days=89)
    assert b.settings == {"reference_date": REF}

    t = ts["tank"]
    assert t.label == "Cost to fill a 15-gallon tank"
    assert t.value == pytest.approx(51.15)  # 3.41 x 15
    assert t.since_war.value == pytest.approx(6.15) and t.since_war.base.value == 45.0
    assert t.since_last.value == pytest.approx(0.0)
    assert t.sparkline[-1].value == pytest.approx(51.15)
    assert t.settings == {"tank_gallons": 15.0, "reference_date": REF}

    h = ts["hormuz"]
    assert h.value == pytest.approx(12.0) and h.as_of == date(2026, 10, 11)
    assert h.since_war.base == calc.DatedValue(REF, 100.0)
    assert h.since_war.value == pytest.approx(-88.0)
    assert h.since_last.value == pytest.approx(0.0)
    assert h.sparkline[-1].day == date(2026, 10, 11)  # provisional days not shown
    assert h.settings == {"reference_date": REF, "baseline_days": 90}

    s = ts["crude_stocks"]
    assert s.value == 380.0 and s.since_last.value == pytest.approx(-21.0)
    assert s.since_war.value == pytest.approx(-20.0)
    assert s.typical_range.position == "below"
    assert (s.typical_range.low, s.typical_range.high) == (395.0, 405.0)

    for tile in ts.values():
        assert tile.as_of is not None and tile.reason is None and tile.sparkline


RISING = (
    "Brent crude is $91.00 a barrel, up 30% since the war began; "
    "US gasoline is $3.41 a gallon, up 41 cents; "
    "ships crossing the Strait of Hormuz are at 12% of normal; "
    "and US crude stocks are below the typical range for this time of year."
)
FALLING = (
    "Brent crude is $63.00 a barrel, down 10% since the war began; "
    "US gasoline is $2.95 a gallon, down 5 cents; "
    "ships crossing the Strait of Hormuz are at 100% of normal; "
    "and US crude stocks are within the typical range for this time of year."
)


def test_summary_rising(store, settings):
    scenario(store, brent=91.0, gas=3.41, hormuz=12.0, stocks=380.0)
    s = calc.summary(store, now=NOW, settings=settings)
    assert s.text == RISING
    # The oldest as-of the sentence uses: crude stocks, week ending Oct 9.
    assert s.as_of == date(2026, 10, 9) and len(s.tiles) == 6
    assert {t.key: t.as_of for t in s.tiles}["brent"] == date(2026, 10, 14)
    assert s.settings == {"reference_date": REF, "baseline_days": 90, "tank_gallons": 15.0}


def test_summary_falling(store, settings):
    scenario(store, brent=63.0, gas=2.95, hormuz=100.0, stocks=400.0)
    assert calc.summary(store, now=NOW, settings=settings).text == FALLING


def test_summary_flat_and_closed_strait(store, settings):
    scenario(store, brent=70.1, gas=4.05, hormuz=0.0, stocks=420.0)
    assert calc.summary(store, now=NOW, settings=settings).text == (
        "Brent crude is $70.10 a barrel, little changed since the war began; "
        "US gasoline is $4.05 a gallon, up $1.05; "
        "no ships are crossing the Strait of Hormuz; "
        "and US crude stocks are above the typical range for this time of year."
    )


def test_summary_no_data(store, settings):
    s = calc.summary(store, now=NOW, settings=settings)
    assert s.text == "Not enough data has come in yet to sum things up."
    assert s.as_of is None
    for t in s.tiles:
        assert t.value is None and t.reason and t.as_of is None and t.sparkline == ()
        assert t.since_war.value is None and t.since_last.value is None


def test_summary_partial_data(store, settings):
    # Brent only, and only since after the reference date: no change to report,
    # so "since the war began" goes with the gasoline clause.
    put(store, calc.BRENT, {date(2026, 10, 14): 91.0})
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 3.0, date(2026, 10, 12): 3.02})
    assert calc.summary(store, now=NOW, settings=settings).text == (
        "Brent crude is $91.00 a barrel, "
        "and US gasoline is $3.02 a gallon, up 2 cents since the war began."
    )


def test_summary_three_clauses_and_as_of_ignores_unused_tiles(store, settings):
    put(store, calc.BRENT, {REF: 70.0, date(2026, 10, 14): 91.0})
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 3.0, date(2026, 10, 12): 3.41})
    put(store, calc.DIESEL_US, {date(2026, 10, 12): 4.0})  # a tile, not in the sentence
    hormuz_history(store, {d: 50.0 for d in days(date(2026, 10, 5), date(2026, 10, 11))})
    put(store, calc.CRUDE_STOCKS, {date(2026, 10, 2): 400.0})  # no typical range: not used
    s = calc.summary(store, now=NOW, settings=settings)
    assert s.text == (
        "Brent crude is $91.00 a barrel, up 30% since the war began; "
        "US gasoline is $3.41 a gallon, up 41 cents; "
        "and ships crossing the Strait of Hormuz are at 50% of normal."
    )
    assert s.as_of == date(2026, 10, 11)  # Hormuz; the Oct 2 stocks tile is not in it


def test_summary_as_of_two_clauses(store, settings):
    put(store, calc.BRENT, {REF: 70.0, date(2026, 10, 14): 91.0})
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 3.0, date(2026, 10, 12): 3.41})
    s = calc.summary(store, now=NOW, settings=settings)
    assert s.text.count(", and ") == 1 and ";" not in s.text
    assert s.as_of == date(2026, 10, 12)


@pytest.mark.parametrize(
    ("level", "text"),
    [
        (0.3, "Almost no ships are crossing the Strait of Hormuz."),  # 0.3% of normal
        (0.0, "No ships are crossing the Strait of Hormuz."),
        (0.6, "Ships crossing the Strait of Hormuz are at 1% of normal."),
    ],
)
def test_summary_near_zero_hormuz(store, settings, level, text):
    hormuz_history(store, {d: level for d in days(date(2026, 10, 5), date(2026, 10, 11))})
    assert calc.summary(store, now=NOW, settings=settings).text == text


def test_summary_single_clause(store, settings):
    put(store, calc.BRENT, {REF: 70.0, date(2026, 10, 14): 91.0})
    assert calc.summary(store, now=NOW, settings=settings).text == (
        "Brent crude is $91.00 a barrel, up 30% since the war began."
    )


@pytest.mark.parametrize(
    ("x", "text"), [(0.41, "41 cents"), (-0.01, "1 cent"), (0.999, "$1.00"), (1.05, "$1.05")]
)
def test_price_change_wording(x, text):
    assert calc._cents(x) == text


# Jargon that lives only on the About page: the shared list in fuelwatch.jargon.
JARGON = list(jargon.JARGON)
TILE_LABEL_EXCEPTIONS = jargon.ALLOWED_LABELS  # may appear as a tile label


def _jargon_in(text: str, words) -> list[str]:
    return jargon.found(text, words)


@pytest.mark.parametrize(
    "case",
    [
        {"brent": 91.0, "gas": 3.41, "hormuz": 12.0, "stocks": 380.0},
        {"brent": 63.0, "gas": 2.95, "hormuz": 100.0, "stocks": 400.0},
        {"brent": 70.1, "gas": 4.05, "hormuz": 0.0, "stocks": 420.0},
        None,
    ],
)
def test_summary_and_tile_labels_are_plain_language(store, settings, case):
    if case:
        scenario(store, **case)
    s = calc.summary(store, now=NOW, settings=settings)
    assert _jargon_in(s.text, JARGON) == []
    for t in s.tiles:
        assert _jargon_in(t.label, [w for w in JARGON if w not in TILE_LABEL_EXCEPTIONS]) == []


# --- now before the reference date: nothing newer than now is read -------------------

EARLY = datetime(2026, 2, 21, 12, 0, tzinfo=UTC)  # six days before the reference date
FUTURE_REF = "the reference date 2026-02-27 is after the data's current date 2026-02-21"


def test_change_since_war_now_before_reference_date(store, settings):
    put(store, calc.BRENT, {date(2026, 2, 20): 60.0, REF: 70.0})
    c = calc.change_since_war(store, calc.BRENT, now=EARLY, settings=settings)
    assert c.value is None and c.percent is None and c.base is None
    assert c.reason == FUTURE_REF
    assert c.current == calc.DatedValue(date(2026, 2, 20), 60.0)


def test_reference_date_today_reads_nothing_after_now(store):
    # Reference date = today: the lookup is capped at now, not the whole day.
    store.set_setting("reference_date", "2026-02-21")
    put(store, calc.BRENT, {date(2026, 2, 20): 60.0})
    store.upsert_points([Point(calc.BRENT, EARLY + timedelta(hours=6), 99.0)], fetched_at=EARLY)
    c = calc.change_since_war(store, calc.BRENT, now=EARLY, settings=store.settings())
    assert c.base == calc.DatedValue(date(2026, 2, 20), 60.0) and c.value == 0.0


def test_percent_of_normal_now_before_reference_date(store, settings):
    hormuz_history(store, {})
    for r in (
        calc.percent_of_normal(store, now=EARLY, settings=settings),
        calc.percent_of_normal_series(store, now=EARLY, settings=settings),
    ):
        assert r.value is None and r.reason == FUTURE_REF
    r = calc.percent_of_normal(store, now=EARLY, settings=settings)
    assert r.baseline_average is None and r.baseline_days_with_data == 0
    assert r.recent_end == date(2026, 2, 21)  # the recent side stops at now
    out = calc.chokepoints_vs_normal(store, now=EARLY, settings=settings)
    assert out["hormuz"].reason == FUTURE_REF


def test_tank_cost_now_before_reference_date(store, settings):
    put(store, calc.GASOLINE_US, {date(2026, 2, 16): 2.9, date(2026, 2, 23): 3.0})
    t = calc.tank_cost(store, now=EARLY, settings=settings)
    assert t.value == pytest.approx(43.5) and t.as_of == date(2026, 2, 16)  # 2.90 x 15
    assert t.then is None and t.change is None and t.percent is None
    assert t.then_reason == FUTURE_REF


def test_tiles_now_before_reference_date(store, settings):
    scenario(store, brent=91.0, gas=3.41, hormuz=12.0, stocks=380.0)
    ts = {t.key: t for t in calc.tiles(store, now=EARLY, settings=settings)}
    for key in ("brent", "gasoline", "diesel", "tank", "hormuz", "crude_stocks"):
        assert ts[key].since_war.value is None
        assert ts[key].since_war.reason == FUTURE_REF
        assert ts[key].as_of is None or ts[key].as_of <= EARLY.date()
    assert ts["brent"].value == 70.0 and ts["tank"].value == pytest.approx(45.0)
    assert ts["hormuz"].value is None and ts["hormuz"].reason == FUTURE_REF


@pytest.mark.parametrize("now", [EARLY, datetime(2026, 2, 27, 6, 0, tzinfo=UTC), NOW])
def test_no_read_goes_past_now(store, settings, now, monkeypatch):
    scenario(store, brent=91.0, gas=3.41, hormuz=12.0, stocks=380.0)
    bounds = []
    real_points, real_value_on = store.points, store.value_on

    def points(series, start=None, end=None, **kw):
        bounds.append(end)
        return real_points(series, start, end, **kw)

    def value_on(series, on):
        bounds.append(on)
        return real_value_on(series, on)

    monkeypatch.setattr(store, "points", points)
    monkeypatch.setattr(store, "value_on", value_on)
    calc.summary(store, now=now, settings=settings)
    calc.crude_share(store, now=now)
    calc.typical_range(store, now=now)
    calc.chokepoints_vs_normal(store, now=now, settings=settings)
    calc.tank_cost(store, now=now, settings=settings)
    assert bounds and None not in bounds
    for b in bounds:
        if isinstance(b, datetime):
            assert b <= now
        else:  # a date bound covers its whole day: only before today
            assert b <= now.date()
            if b == now.date():
                pytest.fail(f"whole-day bound on today's date {b}")


# --- zero prices do not raise ---------------------------------------------------------


def test_tank_cost_zero_reference_price(store, settings):
    put(store, calc.GASOLINE_US, {date(2026, 2, 23): 0.0, date(2026, 10, 12): 3.0})
    t = calc.tank_cost(store, now=NOW, settings=settings)
    assert t.then == 0.0 and t.change == pytest.approx(45.0) and t.percent is None


def test_crude_share_zero_pump_price(store):
    brent_daily(store)
    put(store, calc.GASOLINE_US, {date(2026, 10, 12): 0.0})
    s = calc.crude_share(store, now=NOW)
    assert s.value is None and s.reason == "the pump price is zero" and s.estimate


def test_tank_tile_matches_tank_cost(store, settings):
    scenario(store, brent=91.0, gas=3.41, hormuz=12.0, stocks=380.0)
    tile = {t.key: t for t in calc.tiles(store, now=NOW, settings=settings)}["tank"]
    tc = calc.tank_cost(store, now=NOW, settings=settings)
    assert (tile.value, tile.as_of, tile.settings) == (tc.value, tc.as_of, tc.settings)
    assert tile.since_war.value == tc.change and tile.since_war.percent == tc.percent
    assert tile.since_war.base == calc.DatedValue(tc.then_as_of, tc.then)
