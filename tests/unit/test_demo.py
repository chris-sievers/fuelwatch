"""Demo data: deterministic, every series, and the cases the UI must show."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from fuelwatch import calc, demo
from fuelwatch.registry import REGISTRY, is_business_day
from fuelwatch.store import Store

NOW = datetime(2026, 10, 17, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def store(tmp_path_factory):
    s = Store(tmp_path_factory.mktemp("demo") / "demo.db")
    demo.seed(s, NOW)
    yield s
    s.close()


def test_every_series_has_data(store):
    latest = store.series_latest()
    assert set(latest) == {e.id for e in REGISTRY}


def test_deterministic(tmp_path, store):
    other = Store(tmp_path / "again.db")
    demo.seed(other, NOW)
    for e in REGISTRY:
        assert other.points(e.id) == store.points(e.id), e.id
    other.close()


def test_war_start_jump(store):
    s = store.settings()
    war = calc.change_since_war(store, calc.BRENT, now=NOW, settings=s)
    assert war.percent > 20
    hz = calc.percent_of_normal(store, calc.HORMUZ, now=NOW, settings=s)
    assert hz.value < 50
    days = store.points(calc.HORMUZ, date(2026, 2, 28), date(2026, 3, 20))
    assert sum(p.value == 0 for p in days) >= 10  # zero-traffic days are kept


def test_newest_two_portwatch_days_are_provisional(store):
    pts = store.points(calc.HORMUZ, date(2026, 10, 1), NOW)
    assert [p.provisional for p in pts[-3:]] == [False, True, True]


def test_one_late_source_the_rest_on_time(store):
    latest = store.series_latest()
    status = {
        e.id: REGISTRY.freshness(e.id, latest[e.id].period_end, NOW).status.value for e in REGISTRY
    }
    late = {k for k, v in status.items() if v == "late"}
    assert late == {e.id for e in REGISTRY if e.collector == "eu_oil_bulletin"}


def test_spot_prices_have_holiday_gaps(store):
    days = {p.period_end.date() for p in store.points(calc.BRENT, date(2025, 1, 1), NOW)}
    assert date(2025, 7, 4) not in days and date(2025, 9, 1) not in days  # holidays
    assert date(2025, 7, 3) in days
    assert all(is_business_day(d) for d in days)


def test_live_ais_slots_recent(store):
    p = store.latest("hormuz_ais_ships")[0]
    assert NOW - p.period_end < timedelta(minutes=10)


def test_parse_now():
    assert demo.parse_now(None) is None
    assert demo.parse_now("2026-10-17T12:00:00Z") == NOW
    with pytest.raises(ValueError, match="FW_DEMO_NOW"):
        demo.parse_now("yesterday")


def test_enabled(monkeypatch):
    monkeypatch.delenv("FW_DEMO", raising=False)
    assert demo.enabled() is False
    monkeypatch.setenv("FW_DEMO", "1")
    assert demo.enabled() is True
    monkeypatch.setenv("FW_DEMO", "yes")
    with pytest.raises(ValueError, match="FW_DEMO"):
        demo.enabled()
