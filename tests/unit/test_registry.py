from datetime import UTC, date, datetime, timedelta

import pytest

from fuelwatch import registry
from fuelwatch.models import Frequency, SeriesSpec
from fuelwatch.registry import (
    REGISTRY,
    Collector,
    DuplicateSeriesError,
    FreshnessStatus,
    add_business_days,
    freshness,
    is_federal_holiday,
    pending_release,
)

OK, LATE, NO_DATA = FreshnessStatus.OK, FreshnessStatus.LATE, FreshnessStatus.NO_DATA


def at(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=UTC)


def day(y, m, d):
    return at(y, m, d)


# --- registry ---------------------------------------------------------------------------


def test_every_collector_series_registered_once():
    from fuelwatch.collectors import aisstream, eia, eu_oil_bulletin, portwatch

    ids = [s.id for m in (eia, portwatch, eu_oil_bulletin, aisstream) for s in m.SERIES]
    assert len(ids) == len(set(ids)) == len(REGISTRY)
    assert REGISTRY.get("eia.spot.brent").collector == "eia"
    assert REGISTRY.get("portwatch.hormuz.all").collector == "portwatch"
    assert REGISTRY.get("eu_pump_diesel_de").collector == "eu_oil_bulletin"
    assert REGISTRY.get("hormuz_ais_ships").collector == "aisstream"
    assert "nope" not in REGISTRY
    with pytest.raises(KeyError):
        registry.get("nope")
    assert [c.name for c in REGISTRY.polled] == ["eia", "portwatch", "eu_oil_bulletin"]


def _spec(sid, schedule="every 10 minutes while connected"):
    return SeriesSpec(sid, sid, "x", "src", Frequency.DAILY, schedule, timedelta(hours=1))


def test_duplicate_series_fails():
    a = Collector("a", (_spec("s.one"),))
    b = Collector("b", (_spec("s.two"), _spec("s.one")))
    with pytest.raises(DuplicateSeriesError, match="s.one"):
        registry.build([a, b])


def test_duplicate_within_one_collector_fails():
    with pytest.raises(DuplicateSeriesError):
        registry.build([Collector("a", (_spec("s.one"), _spec("s.one")))])


def test_unknown_schedule_fails():
    with pytest.raises(ValueError, match="no machine schedule"):
        registry.build([Collector("a", (_spec("s.one", "whenever"),))])


def test_grace_follows_spec():
    eia = REGISTRY.get("eia.retail.gasoline.us").schedule
    assert eia.grace_business_days == 1
    assert REGISTRY.get("portwatch.suez.all").schedule.grace == timedelta(days=2)
    assert REGISTRY.get("hormuz_ais_ships").schedule.grace == timedelta(hours=1)


# --- business days ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "d",
    [
        date(2026, 1, 1),
        date(2026, 1, 19),  # MLK
        date(2026, 5, 25),  # Memorial Day
        date(2026, 7, 3),  # July 4 on a Saturday, observed Friday
        date(2026, 9, 7),  # Labor Day
        date(2026, 10, 12),  # Columbus Day
        date(2026, 11, 26),  # Thanksgiving
        date(2027, 12, 31),  # Jan 1 2028 is a Saturday, observed Dec 31
        date(2028, 12, 25),
    ],
)
def test_federal_holidays(d):
    assert is_federal_holiday(d)


def test_not_holidays():
    assert not is_federal_holiday(date(2026, 7, 4))  # the Saturday itself
    assert not is_federal_holiday(date(2026, 10, 13))


def test_add_business_days_skips_weekend_and_holiday():
    assert add_business_days(date(2026, 10, 9), 1) == date(2026, 10, 13)  # Fri -> Tue
    assert add_business_days(date(2026, 12, 31), 1) == date(2027, 1, 4)


# --- freshness: EIA retail (Monday, about 22:00 UTC) ---------------------------------------

RETAIL = "eia.retail.gasoline.us"


def test_retail_normal_week():
    # Week of Oct 19, 2026: no holiday. Released Mon 22:00, late after Tue 22:00.
    have = day(2026, 10, 12)
    assert freshness(RETAIL, have, at(2026, 10, 19, 12)).status == OK
    f = freshness(RETAIL, have, at(2026, 10, 20, 21, 59))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 19)
    assert f.expected_publication == at(2026, 10, 19, 22)
    late = freshness(RETAIL, have, at(2026, 10, 20, 22, 1))
    assert late.status == LATE
    assert late.deadline == at(2026, 10, 20, 22)
    assert freshness(RETAIL, day(2026, 10, 19), at(2026, 10, 20, 23)).status == OK


def test_retail_holiday_week_is_not_flagged_early():
    # Columbus Day Mon Oct 12, 2026: the expected release moves two business
    # days to Wed Oct 14 (when it really came out); late after Thu 22:00.
    have = day(2026, 10, 5)
    f = freshness(RETAIL, have, at(2026, 10, 13, 23))
    assert f.status == OK
    assert f.expected_publication == at(2026, 10, 14, 22)
    assert f.deadline == at(2026, 10, 15, 22)
    assert freshness(RETAIL, have, at(2026, 10, 14, 23)).status == OK
    assert freshness(RETAIL, have, at(2026, 10, 15, 21)).status == OK
    assert freshness(RETAIL, have, at(2026, 10, 15, 22, 1)).status == LATE


def test_retail_memorial_day_week():
    # Mon May 25 2026: expected Wed May 27, late after Thu May 28 22:00.
    have = day(2026, 5, 18)
    f = freshness(RETAIL, have, at(2026, 5, 26, 23))
    assert f.expected_publication == at(2026, 5, 27, 22)
    assert f.status == OK
    assert freshness(RETAIL, have, at(2026, 5, 28, 22, 1)).status == LATE


def test_retail_year_boundary():
    # Christmas Fri Dec 25 2026 and New Year Fri Jan 1 2027 do not move the
    # Monday release; the Tue grace still applies.
    have = day(2026, 12, 28)
    assert freshness(RETAIL, have, at(2027, 1, 4, 23)).status == OK
    f = freshness(RETAIL, have, at(2027, 1, 5, 22, 30))
    assert f.status == LATE
    assert f.expected_period == day(2027, 1, 4)


def test_retail_far_behind_reports_oldest_missed():
    f = freshness(RETAIL, day(2026, 9, 28), at(2026, 10, 21))
    assert f.status == LATE
    assert f.expected_period == day(2026, 10, 5)


# --- freshness: EIA WPSR (Wednesday, week ending Friday) -----------------------------------

STOCKS = "eia.stocks.crude_commercial"


def test_wpsr_normal_week():
    have = day(2026, 10, 9)
    f = freshness(STOCKS, have, at(2026, 10, 22, 15))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 16)
    assert freshness(STOCKS, have, at(2026, 10, 22, 15, 31)).status == LATE


def test_wpsr_holiday_week_slips_a_day():
    # Labor Day Mon Sep 7, 2026: WPSR moves to Thu Sep 10, late after Fri.
    have = day(2026, 8, 28)
    f = freshness(STOCKS, have, at(2026, 9, 10, 20))
    assert f.status == OK
    assert f.expected_publication == at(2026, 9, 10, 15, 30)
    assert f.expected_period == day(2026, 9, 4)
    assert freshness(STOCKS, have, at(2026, 9, 11, 15, 29)).status == OK
    assert freshness(STOCKS, have, at(2026, 9, 11, 15, 31)).status == LATE


def test_wpsr_thanksgiving_week_unaffected():
    # Thanksgiving is Thursday, after the Wednesday release.
    have = day(2026, 11, 13)
    # Wed Nov 25 + one business day (Thursday is the holiday) = Fri Nov 27.
    f = freshness(STOCKS, have, at(2026, 11, 26, 16))
    assert f.status == OK
    assert f.expected_publication == at(2026, 11, 25, 15, 30)
    assert f.deadline == at(2026, 11, 27, 15, 30)
    assert freshness(STOCKS, have, at(2026, 11, 27, 15, 31)).status == LATE


def test_wpsr_month_boundary():
    # Release Wed Sep 30 covers week ending Fri Sep 25; next covers Oct 2.
    f = freshness(STOCKS, day(2026, 9, 25), at(2026, 10, 1, 12))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 2)


# --- freshness: EIA spot (Wednesday, daily data through the business day before) -----------

SPOT = "eia.spot.brent"


def test_spot_expects_tuesday():
    f = freshness(SPOT, day(2026, 10, 13), at(2026, 10, 17))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 20)
    assert freshness(SPOT, day(2026, 10, 19), at(2026, 10, 22, 16, 59)).status == OK
    assert freshness(SPOT, day(2026, 10, 19), at(2026, 10, 22, 17, 1)).status == LATE


def test_spot_columbus_day_week_slips():
    # Mon Oct 12 2026 holiday: release Thu Oct 15, late after Fri Oct 16 17:00.
    have = day(2026, 10, 9)
    assert freshness(SPOT, have, at(2026, 10, 15, 17, 1)).status == OK
    assert freshness(SPOT, have, at(2026, 10, 16, 17, 1)).status == LATE


def test_spot_tuesday_holiday_rolls_back():
    # Christmas Tue Dec 25 2029: the Wednesday release can only reach Monday.
    f = freshness(SPOT, day(2029, 12, 24), at(2029, 12, 28))
    assert f.status == OK


# --- freshness: PortWatch and EU ---------------------------------------------------------

PW = "portwatch.hormuz.tankers"
EU = "eu_pump_petrol_eu27"


def test_portwatch_two_day_grace():
    have = day(2026, 10, 11)  # Sunday, from the Tue Oct 13 release
    f = freshness(PW, have, at(2026, 10, 20, 12))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 18)
    assert freshness(PW, have, at(2026, 10, 22, 12, 59)).status == OK
    assert freshness(PW, have, at(2026, 10, 22, 13, 1)).status == LATE


def test_portwatch_year_boundary():
    # Tue Jan 5 2027 release covers Sun Jan 3 2027.
    f = freshness(PW, day(2026, 12, 27), at(2027, 1, 7, 14))
    assert f.status == LATE
    assert f.expected_period == day(2027, 1, 3)


def test_eu_thursday_release_monday_prices():
    have = day(2026, 10, 12)
    f = freshness(EU, have, at(2026, 10, 22, 9))
    assert f.status == OK
    assert f.expected_period == day(2026, 10, 19)
    assert f.expected_publication == at(2026, 10, 22, 14)
    assert freshness(EU, have, at(2026, 10, 24, 14, 1)).status == LATE


def test_eu_month_boundary():
    # Thu Oct 1 2026 release carries Mon Sep 28 prices.
    assert freshness(EU, day(2026, 9, 21), at(2026, 10, 3, 15)).status == LATE
    assert freshness(EU, day(2026, 9, 28), at(2026, 10, 3, 15)).status == OK


# --- freshness: live AIS and no data -------------------------------------------------------


def test_ais_one_hour_grace():
    last = at(2026, 10, 16, 12, 0)
    assert freshness("hormuz_ais_ships", last, at(2026, 10, 16, 13, 5)).status == OK
    f = freshness("hormuz_ais_ships", last, at(2026, 10, 16, 13, 11))
    assert f.status == LATE
    assert f.expected_publication == at(2026, 10, 16, 12, 10)


def test_no_data():
    for sid in (RETAIL, PW, EU, "hormuz_ais_ships"):
        assert freshness(sid, None, at(2026, 10, 16)).status == NO_DATA


def test_pending_release():
    s = REGISTRY.get(RETAIL).schedule
    assert pending_release(s, day(2026, 10, 12), at(2026, 10, 19, 21)) is None  # not out yet
    r = pending_release(s, day(2026, 10, 12), at(2026, 10, 19, 23))
    assert r is not None and r.period == date(2026, 10, 19)
    assert pending_release(s, day(2026, 10, 19), at(2026, 10, 19, 23)) is None
