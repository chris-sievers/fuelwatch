import math
import sqlite3
import threading
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from fuelwatch.__main__ import main as cli_main
from fuelwatch.http import ProviderError, redact
from fuelwatch.models import Point
from fuelwatch.store import (
    MIGRATIONS,
    SCHEMA_VERSION,
    BackfillState,
    DestructiveMigrationError,
    Store,
    check_additive,
    default_path,
    migrate,
    schema_version,
)


def day(y: int, m: int, d: int, hh: int = 0, mm: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=UTC)


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "fuelwatch.db")
    yield s
    s.close()


def rows(path) -> list[tuple]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT series, period_end, value, provisional, fetched_at FROM points "
            "ORDER BY series, period_end"
        ).fetchall()
    finally:
        conn.close()


# --- opening and configuration ----------------------------------------------------------


def test_default_path_uses_fw_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("FW_DATA_DIR", str(tmp_path / "d"))
    assert default_path() == tmp_path / "d" / "fuelwatch.db"
    with Store() as s:
        assert s.path == tmp_path / "d" / "fuelwatch.db"
    assert (tmp_path / "d" / "fuelwatch.db").exists()


def test_default_data_dir_is_dot_data(monkeypatch):
    monkeypatch.delenv("FW_DATA_DIR", raising=False)
    assert default_path().as_posix() == "data/fuelwatch.db"


def test_pragmas(store):
    conn = store._conn()
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_wal_is_on_in_the_file(tmp_path):
    with Store(tmp_path / "x.db") as s:
        s.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
        assert (tmp_path / "x.db-wal").exists()
    conn = sqlite3.connect(tmp_path / "x.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"  # persistent
    conn.close()


def test_default_settings(store):
    s = store.settings()
    assert s.reference_date == date(2026, 2, 27)
    assert s.baseline_days == 90
    assert s.tank_gallons == 15.0


def test_settings_survive_reopen_and_are_not_reseeded(tmp_path):
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        s.set_setting("tank_gallons", "12.5")
        s.set_setting("reference_date", "2026-03-01")
        s.set_setting("theme", "dark")
    with Store(path) as s:
        assert s.settings().tank_gallons == 12.5
        assert s.settings().reference_date == date(2026, 3, 1)
        assert s.get_setting("theme") == "dark"
        assert s.get_setting("missing") is None


@pytest.mark.parametrize(
    "key,value",
    [
        ("reference_date", "27/02/2026"),
        ("baseline_days", "ninety"),
        ("baseline_days", "0"),
        ("tank_gallons", "-1"),
        ("tank_gallons", "nan"),
        ("schema_version", "9"),
        ("Bad Key", "x"),
    ],
)
def test_bad_settings_refused(store, key, value):
    with pytest.raises(ValueError):
        store.set_setting(key, value)
    assert store.settings().baseline_days == 90


# --- migrations ---------------------------------------------------------------------------


def test_migrates_from_empty(tmp_path):
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        assert s.schema_version == SCHEMA_VERSION
    conn = sqlite3.connect(path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"points", "status", "meta"} <= tables
    assert schema_version(conn) == SCHEMA_VERSION
    conn.close()


def test_reopen_keeps_data_and_version(tmp_path):
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        s.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
    with Store(path) as s:
        assert s.schema_version == SCHEMA_VERSION
        assert [p.value for p in s.points("a")] == [1.0]


def test_forward_compatible_with_newer_schema(tmp_path):
    """An older image must run on a database a newer image migrated."""
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        s.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
    newer = SCHEMA_VERSION + 3
    conn = sqlite3.connect(path)
    with conn:
        # What a future additive migration might do.
        conn.execute("ALTER TABLE points ADD COLUMN source TEXT")
        conn.execute("ALTER TABLE status ADD COLUMN next_run_at INTEGER")
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)")
        conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (str(newer),))
    conn.close()

    with Store(path) as s:
        assert s.schema_version == newer
        s.upsert_points([Point("a", day(2026, 1, 2), 2.0, provisional=True)])
        assert [p.value for p in s.points("a")] == [1.0, 2.0]
        assert s.value_on("a", date(2026, 1, 1)).value == 1.0
        s.record_success("eia", at=day(2026, 1, 2))
        assert s.get_status("eia").last_success_at == day(2026, 1, 2)
        assert s.settings().baseline_days == 90
    conn = sqlite3.connect(path)
    assert schema_version(conn) == newer  # never downgraded
    conn.close()


def test_pending_migrations_apply_on_open(tmp_path):
    conn = sqlite3.connect(tmp_path / "m.db", isolation_level=None)
    assert migrate(conn, MIGRATIONS) == 1
    extra = [*MIGRATIONS, ["ALTER TABLE points ADD COLUMN note TEXT"]]
    assert migrate(conn, extra) == 2
    cols = [r[1] for r in conn.execute("PRAGMA table_info(points)")]
    assert "note" in cols
    assert migrate(conn, extra) == 2  # idempotent
    conn.close()


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE points",
        "ALTER TABLE points DROP COLUMN fetched_at",
        "ALTER TABLE points RENAME TO old_points",
        "DELETE FROM points",
        "UPDATE points SET value = 0",
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('tank_gallons', '10')",
        "drop index foo",
    ],
)
def test_destructive_migrations_refused(tmp_path, sql):
    path = tmp_path / "m.db"
    conn = sqlite3.connect(path, isolation_level=None)
    migrate(conn, MIGRATIONS)
    bad = [*MIGRATIONS, ["ALTER TABLE points ADD COLUMN ok TEXT", sql]]
    with pytest.raises(DestructiveMigrationError):
        migrate(conn, bad)
    # nothing from the refused migration ran
    assert schema_version(conn) == SCHEMA_VERSION
    assert "ok" not in [r[1] for r in conn.execute("PRAGMA table_info(points)")]
    conn.close()


def test_additive_check_ignores_words_in_literals_and_comments():
    check_additive(
        [
            "-- we never DROP anything\nCREATE TABLE t (x TEXT DEFAULT 'delete me')",
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('mode', 'update')",
        ]
    )


def test_released_migrations_are_additive():
    for statements in MIGRATIONS:
        check_additive(statements)


# --- upserts ----------------------------------------------------------------------------


def test_upsert_is_idempotent(store):
    pts = [Point("a", day(2026, 1, d), float(d)) for d in range(1, 6)]
    assert store.upsert_points(pts, fetched_at=day(2026, 1, 6)) == 5
    first = rows(store.path)
    store.upsert_points(pts, fetched_at=day(2026, 1, 6))
    assert rows(store.path) == first
    assert len(first) == 5


def test_revision_overwrites_value_and_fetched_at(store):
    store.upsert_points([Point("pw", day(2026, 2, 28), 95.0)], fetched_at=day(2026, 3, 3))
    store.upsert_points([Point("pw", day(2026, 2, 28), 64.0)], fetched_at=day(2026, 4, 1))
    assert store.points("pw") == [Point("pw", day(2026, 2, 28), 64.0)]
    assert rows(store.path)[0][4] == int(day(2026, 4, 1).timestamp())


def test_provisional_cleared_on_revision(store):
    store.upsert_points([Point("pw", day(2026, 10, 11), 40.0, provisional=True)])
    assert store.latest("pw")[0].provisional is True
    store.upsert_points([Point("pw", day(2026, 10, 11), 41.0, provisional=False)])
    assert store.latest("pw") == [Point("pw", day(2026, 10, 11), 41.0, False)]


def test_settled_point_can_become_provisional_again(store):
    # value and flag always move together: the newest publication wins.
    store.upsert_points([Point("pw", day(2026, 10, 11), 41.0)])
    store.upsert_points([Point("pw", day(2026, 10, 11), 39.0, provisional=True)])
    assert store.latest("pw") == [Point("pw", day(2026, 10, 11), 39.0, True)]


def test_real_zeros_are_kept(store):
    store.upsert_points(
        [
            Point("pw", day(2026, 3, 1), 0.0),
            Point("pw", day(2026, 3, 2), 0),
            Point("pw", day(2026, 3, 3), -0.0),
        ]
    )
    pts = store.points("pw")
    assert [p.value for p in pts] == [0.0, 0.0, 0.0]
    assert all(isinstance(p.value, float) for p in pts)


def test_gaps_stay_gaps(store):
    store.upsert_points([Point("pw", day(2026, 3, 1), 5.0), Point("pw", day(2026, 3, 4), 6.0)])
    assert [p.period_end.day for p in store.points("pw")] == [1, 4]


def test_last_point_in_batch_wins(store):
    store.upsert_points([Point("a", day(2026, 1, 1), 1.0), Point("a", day(2026, 1, 1), 2.0)])
    assert [p.value for p in store.points("a")] == [2.0]


def test_empty_batch(store):
    assert store.upsert_points([]) == 0


def test_non_utc_offset_is_converted_by_caller_not_store(store):
    est = datetime(2026, 1, 1, tzinfo=UTC).astimezone(timezone(timedelta(hours=-5)))
    with pytest.raises(ValueError, match="UTC"):
        store.upsert_points([Point("a", est, 1.0)])


@pytest.mark.parametrize(
    "bad",
    [
        Point("a", day(2026, 1, 1), math.nan),
        Point("a", day(2026, 1, 1), math.inf),
        Point("a", day(2026, 1, 1), -math.inf),
        Point("a", day(2026, 1, 1), None),
        Point("a", day(2026, 1, 1), "1.0"),
        Point("a", day(2026, 1, 1), True),
        Point("a", day(2026, 1, 1), 1.0, provisional=1),
        Point("", day(2026, 1, 1), 1.0),
        Point("Has Space", day(2026, 1, 1), 1.0),
        Point("a", day(2026, 1, 1).replace(microsecond=5), 1.0),
        Point("a", datetime(1969, 12, 31, tzinfo=UTC), 1.0),
        ("a", day(2026, 1, 1), 1.0, False),
    ],
)
def test_invalid_points_rejected_and_batch_not_written(store, bad):
    good = Point("a", day(2026, 1, 2), 2.0)
    with pytest.raises((ValueError, TypeError)):
        store.upsert_points([good, bad])
    assert store.points("a") == []


def test_naive_datetime_rejected_by_model():
    with pytest.raises(ValueError):
        Point("a", datetime(2026, 1, 1), 1.0)


def test_rejects_naive_fetched_at(store):
    with pytest.raises(ValueError):
        store.upsert_points([Point("a", day(2026, 1, 1), 1.0)], fetched_at=datetime(2026, 1, 1))


def test_failed_write_rolls_back_and_store_still_works(store, monkeypatch):
    store.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
    conn = store._conn()
    conn.execute(
        "CREATE TRIGGER boom BEFORE INSERT ON points WHEN NEW.value = 13 "
        "BEGIN SELECT RAISE(ABORT, 'boom'); END"
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.upsert_points([Point("a", day(2026, 1, 2), 2.0), Point("a", day(2026, 1, 3), 13.0)])
    assert [p.value for p in store.points("a")] == [1.0]  # one transaction, nothing landed
    assert not conn.in_transaction
    store.upsert_points([Point("a", day(2026, 1, 4), 4.0)])
    assert [p.value for p in store.points("a")] == [1.0, 4.0]


# --- range reads ----------------------------------------------------------------------------


@pytest.fixture
def daily(store):
    store.upsert_points([Point("d", day(2026, 1, n), float(n)) for n in range(1, 11)])
    return store


def test_range_is_inclusive_both_ends_with_dates(daily):
    assert [p.value for p in daily.points("d", date(2026, 1, 3), date(2026, 1, 5))] == [3, 4, 5]


def test_range_is_inclusive_both_ends_with_datetimes(daily):
    got = daily.points("d", day(2026, 1, 3), day(2026, 1, 5))
    assert [p.value for p in got] == [3, 4, 5]
    got = daily.points("d", day(2026, 1, 3, 0, 1), day(2026, 1, 4, 23, 59))
    assert [p.value for p in got] == [4]


def test_range_sub_second_start_excludes_earlier_whole_second(daily):
    start = day(2026, 1, 3).replace(microsecond=1)
    assert daily.points("d", start, date(2026, 1, 4))[0].value == 4


def test_range_open_ended_and_ordering(store):
    # inserted out of order; read back oldest first
    store.upsert_points([Point("d", day(2026, 1, n), float(n)) for n in (5, 1, 3, 2, 4)])
    assert [p.value for p in store.points("d")] == [1, 2, 3, 4, 5]
    assert [p.value for p in store.points("d", start=date(2026, 1, 4))] == [4, 5]
    assert [p.value for p in store.points("d", end=date(2026, 1, 2))] == [1, 2]


def test_range_date_end_covers_intraday_points(store):
    store.upsert_points(
        [Point("ais", day(2026, 1, 1, 23, 50), 1.0), Point("ais", day(2026, 1, 2), 2.0)]
    )
    assert [p.value for p in store.points("ais", date(2026, 1, 1), date(2026, 1, 1))] == [1.0]


def test_range_empty_and_unknown_series(daily):
    assert daily.points("d", date(2026, 2, 1), date(2026, 2, 5)) == []
    assert daily.points("d", date(2026, 1, 5), date(2026, 1, 3)) == []
    assert daily.points("nope") == []


def test_range_isolates_series(store):
    store.upsert_points([Point("a", day(2026, 1, 1), 1.0), Point("b", day(2026, 1, 1), 2.0)])
    assert [p.series for p in store.points("a")] == ["a"]


def test_settled_only(store):
    store.upsert_points(
        [
            Point("pw", day(2026, 10, 9), 30.0),
            Point("pw", day(2026, 10, 10), 31.0, True),
            Point("pw", day(2026, 10, 11), 32.0, True),
        ]
    )
    assert [p.value for p in store.points("pw", settled_only=True)] == [30.0]
    assert [p.value for p in store.latest("pw", 2, settled_only=True)] == [30.0]
    assert [p.value for p in store.latest("pw", 2)] == [31.0, 32.0]


def test_latest_n(daily):
    assert [p.value for p in daily.latest("d")] == [10]
    assert [p.value for p in daily.latest("d", 3)] == [8, 9, 10]
    assert len(daily.latest("d", 100)) == 10
    assert daily.latest("d", 0) == []
    assert daily.latest("nope", 3) == []


def test_series_latest(store):
    assert store.series_latest() == {}
    store.upsert_points(
        [
            Point("b", day(2026, 1, 1), 1.0),
            Point("b", day(2026, 1, 8), 2.0, True),
            Point("a", day(2026, 1, 3), 3.0),
        ]
    )
    latest = store.series_latest()
    assert list(latest) == ["a", "b"]
    assert latest["b"] == Point("b", day(2026, 1, 8), 2.0, True)
    assert latest["a"].value == 3.0


def test_round_trip_types(store):
    p = Point("a", day(2026, 1, 1, 12, 30), 1.25, True)
    store.upsert_points([p])
    got = store.points("a")[0]
    assert got == p
    assert got.period_end.tzinfo is UTC


# --- value_on: the time model ----------------------------------------------------------------


@pytest.fixture
def aligned(store):
    """Daily spot prices (weekdays, with a holiday gap) and weekly series at
    their providers' own period dates, around the Feb 27, 2026 reference date."""
    spot = []
    d = date(2026, 2, 16)
    while d <= date(2026, 4, 10):
        if d.weekday() < 5 and d not in (
            date(2026, 2, 16),
            date(2026, 4, 3),
        ):  # Presidents Day, Good Friday
            spot.append(
                Point(
                    "eia.spot.brent",
                    datetime(d.year, d.month, d.day, tzinfo=UTC),
                    float(d.toordinal() % 1000),
                )
            )
        d += timedelta(days=1)
    retail = [  # Monday survey dates
        Point("eia.retail.gas", day(2026, 2, 16), 3.10),
        Point("eia.retail.gas", day(2026, 2, 23), 3.12),
        Point("eia.retail.gas", day(2026, 3, 2), 3.40),
    ]
    stocks = [  # week-ending Fridays
        Point("eia.stocks.crude", day(2026, 2, 20), 420.0),
        Point("eia.stocks.crude", day(2026, 2, 27), 418.0),
        Point("eia.stocks.crude", day(2026, 3, 6), 410.0),
    ]
    store.upsert_points(spot + retail + stocks)
    return store


def spot_value(d: date) -> float:
    return float(d.toordinal() % 1000)


def test_value_on_reference_date_friday(aligned):
    ref = aligned.settings().reference_date
    assert ref.weekday() == 4  # Friday
    assert aligned.value_on("eia.spot.brent", ref).period_end == day(2026, 2, 27)
    assert aligned.value_on("eia.retail.gas", ref).value == 3.12  # Monday Feb 23
    assert aligned.value_on("eia.stocks.crude", ref).value == 418.0  # week ending Feb 27


def test_value_on_weekend_uses_friday_and_previous_monday(aligned):
    sat, sun = date(2026, 2, 28), date(2026, 3, 1)
    for d in (sat, sun):
        assert aligned.value_on("eia.spot.brent", d).period_end == day(2026, 2, 27)
        assert aligned.value_on("eia.spot.brent", d).value == spot_value(date(2026, 2, 27))
        assert aligned.value_on("eia.retail.gas", d).value == 3.12
        assert aligned.value_on("eia.stocks.crude", d).value == 418.0
    # the Monday after: weekly retail steps, stocks stay on the last Friday
    assert aligned.value_on("eia.retail.gas", date(2026, 3, 2)).value == 3.40
    assert aligned.value_on("eia.stocks.crude", date(2026, 3, 5)).value == 418.0


def test_value_on_holiday_gap(aligned):
    good_friday = date(2026, 4, 3)
    got = aligned.value_on("eia.spot.brent", good_friday)
    assert got.period_end == day(2026, 4, 2)
    assert aligned.value_on("eia.spot.brent", date(2026, 4, 5)).period_end == day(2026, 4, 2)


def test_value_on_before_first_observation(aligned):
    assert aligned.value_on("eia.spot.brent", date(2026, 2, 16)) is None  # holiday, first day
    assert aligned.value_on("eia.spot.brent", date(2026, 2, 17)).period_end == day(2026, 2, 17)
    assert aligned.value_on("eia.retail.gas", date(2026, 2, 15)) is None
    assert aligned.value_on("eia.stocks.crude", date(2026, 2, 19)) is None
    assert aligned.value_on("unknown", date(2026, 3, 1)) is None


def test_value_on_after_last_observation(aligned):
    assert aligned.value_on("eia.retail.gas", date(2030, 1, 1)).value == 3.40


def test_value_on_datetime_is_exact_instant(store):
    store.upsert_points(
        [Point("ais", day(2026, 1, 1, 12, 0), 1.0), Point("ais", day(2026, 1, 1, 12, 10), 2.0)]
    )
    assert store.value_on("ais", day(2026, 1, 1, 12, 9)).value == 1.0
    assert store.value_on("ais", day(2026, 1, 1, 12, 10)).value == 2.0
    assert store.value_on("ais", date(2026, 1, 1)).value == 2.0
    with pytest.raises(ValueError):
        store.value_on("ais", datetime(2026, 1, 1))


# --- daily roll-up ------------------------------------------------------------------------


def ais_day(series: str, d: date, values) -> list[Point]:
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    return [
        Point(series, start + timedelta(minutes=10 * (i + 1)), float(v))
        for i, v in enumerate(values)
        if v is not None
    ]


def test_daily_rollup_mean_of_full_days(store):
    d1, d2 = date(2026, 5, 1), date(2026, 5, 2)
    store.upsert_points(
        ais_day("hormuz_ais_ships", d1, [10] * 72 + [20] * 72)
        + ais_day("hormuz_ais_ships", d2, range(144))
    )
    got = store.daily_rollup("hormuz_ais_ships")
    assert [(r.day, r.slots) for r in got] == [(d1, 144), (d2, 144)]
    assert got[0].value == 15.0
    assert got[1].value == pytest.approx(71.5)
    assert all(not r.provisional for r in got)


def test_daily_rollup_slot_ending_at_midnight_belongs_to_previous_day(store):
    store.upsert_points(
        [
            Point("ais", day(2026, 5, 1, 0, 10), 1.0),  # first slot of May 1
            Point("ais", day(2026, 5, 2, 0, 0), 3.0),  # 23:50-24:00 on May 1
            Point("ais", day(2026, 5, 2, 0, 10), 7.0),  # first slot of May 2
        ]
    )
    got = store.daily_rollup("ais")
    assert [(r.day, r.value, r.slots) for r in got] == [
        (date(2026, 5, 1), 2.0, 2),
        (date(2026, 5, 2), 7.0, 1),
    ]


def test_daily_rollup_outage_is_not_zero(store):
    # a 6-hour outage: those slots are missing, and the mean is of the slots heard
    values = [12] * 54 + [None] * 36 + [12] * 54
    store.upsert_points(ais_day("ais", date(2026, 5, 1), values))
    (r,) = store.daily_rollup("ais")
    assert r.value == 12.0
    assert r.slots == 108


def test_daily_rollup_bounds_and_gap_days(store):
    pts = []
    for n in (1, 2, 4, 5):  # May 3 had no data at all
        pts += ais_day("ais", date(2026, 5, n), [n] * 144)
    store.upsert_points(pts)
    got = store.daily_rollup("ais", date(2026, 5, 2), date(2026, 5, 4))
    assert [(r.day.day, r.value) for r in got] == [(2, 2.0), (4, 4.0)]
    assert [r.day.day for r in store.daily_rollup("ais", start=date(2026, 5, 5))] == [5]
    assert [r.day.day for r in store.daily_rollup("ais", end=date(2026, 5, 1))] == [1]
    assert store.daily_rollup("nope") == []


def test_daily_rollup_zeros_count(store):
    store.upsert_points(ais_day("ais", date(2026, 5, 1), [0] * 143 + [144]))
    (r,) = store.daily_rollup("ais")
    assert r.value == 1.0


# --- status and backfill --------------------------------------------------------------------


def test_status_unknown_collector(store):
    assert store.get_status("eia") is None
    assert store.all_status() == []
    bf = store.get_backfill("eia")
    assert bf.state is BackfillState.NOT_STARTED
    assert bf.cursor is None and bf.progress == 0.0


def test_status_success_then_failure_then_success(store):
    store.record_success("eia", at=day(2026, 10, 15, 12))
    st = store.get_status("eia")
    assert st.last_run_at == st.last_success_at == day(2026, 10, 15, 12)
    assert st.error_class is None and st.error_message is None

    store.record_failure("eia", ProviderError("http_status", "EIA returned 503"), day(2026, 10, 16))
    st = store.get_status("eia")
    assert st.last_run_at == day(2026, 10, 16)
    assert st.last_success_at == day(2026, 10, 15, 12)
    assert (st.error_class, st.error_message) == ("http_status", "EIA returned 503")

    store.record_success("eia", at=day(2026, 10, 17))
    st = store.get_status("eia")
    assert st.error_class is None and st.error_message is None


def test_status_failure_never_stores_raw_exceptions(store):
    planted = "planted-fake-key"
    redact.add(planted)
    try:
        store.record_failure("eia", RuntimeError(f"boom with {planted} at /etc/passwd"))
        st = store.get_status("eia")
        assert (st.error_class, st.error_message) == ("internal", "RuntimeError")

        store.record_failure(
            "portwatch",
            ProviderError("schema", f"bad <b>doc</b>\n\x1b[31m api_key={planted} " + "x" * 2000),
        )
        st = store.get_status("portwatch")
        assert st.error_class == "schema"
        assert planted not in st.error_message
        assert "\n" not in st.error_message and "\x1b" not in st.error_message
        assert len(st.error_message) <= 500
    finally:
        redact._secrets.remove(planted)


def test_status_bad_error_class_becomes_internal(store):
    store.record_failure("eia", ProviderError("Not A Class!", "msg"))
    assert store.get_status("eia").error_class == "internal"


def test_status_bad_collector_name(store):
    with pytest.raises(ValueError):
        store.record_success("../etc")


def test_backfill_cursor_round_trip(tmp_path):
    path = tmp_path / "fuelwatch.db"
    with Store(path) as s:
        s.record_success("portwatch", at=day(2026, 10, 16))
        s.set_backfill(
            "portwatch",
            BackfillState.RUNNING,
            '{"start": "2023-01-01"}',
            0.4,
            at=day(2026, 10, 16, 1),
        )
    with Store(path) as s:  # a restart
        bf = s.get_backfill("portwatch")
        assert bf.state is BackfillState.RUNNING
        assert bf.cursor == '{"start": "2023-01-01"}'
        assert bf.progress == 0.4
        assert bf.updated_at == day(2026, 10, 16, 1)
        # backfill updates do not touch run results, and vice versa
        assert s.get_status("portwatch").last_success_at == day(2026, 10, 16)
        s.record_failure("portwatch", ProviderError("timeout", "slow"))
        assert s.get_backfill("portwatch").cursor == '{"start": "2023-01-01"}'
        s.set_backfill("portwatch", "done", None, 1.0)
        assert s.get_backfill("portwatch").state is BackfillState.DONE
        assert s.get_status("portwatch").error_class == "timeout"
        assert [st.collector for st in s.all_status()] == ["portwatch"]


@pytest.mark.parametrize(
    "state,cursor,progress",
    [
        ("paused", None, 0.0),
        (BackfillState.RUNNING, 5, 0.0),
        (BackfillState.RUNNING, None, 1.5),
        (BackfillState.RUNNING, None, -0.1),
    ],
)
def test_backfill_bad_input(store, state, cursor, progress):
    with pytest.raises((ValueError, TypeError)):
        store.set_backfill("eia", state, cursor, progress)


# --- threads ------------------------------------------------------------------------------


def test_reads_during_writes_from_other_threads(store):
    errors: list[BaseException] = []
    stop = threading.Event()
    base = day(2026, 1, 1)

    def writer(series: str):
        try:
            for i in range(200):
                store.upsert_points(
                    [
                        Point(series, base + timedelta(minutes=10 * (i * 20 + k)), float(k))
                        for k in range(20)
                    ]
                )
        except BaseException as e:  # noqa: BLE001 - surface any thread error
            errors.append(e)

    def reader():
        try:
            while not stop.is_set():
                store.points("w1")
                store.latest("w2", 5)
                store.series_latest()
                store.daily_rollup("w1")
                store.value_on("w1", date(2026, 1, 5))
        except BaseException as e:  # noqa: BLE001 - surface any thread error
            errors.append(e)

    writers = [threading.Thread(target=writer, args=(s,)) for s in ("w1", "w2")]
    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join()
    assert errors == []
    assert len(store.points("w1")) == len(store.points("w2")) == 4000


def test_each_thread_has_its_own_connection(store):
    seen = []
    t = threading.Thread(target=lambda: seen.append(store._conn()))
    t.start()
    t.join()
    assert seen[0] is not store._conn()


async def test_works_through_asyncio_to_thread(store):
    import asyncio

    await asyncio.gather(
        *[
            asyncio.to_thread(store.upsert_points, [Point("t", day(2026, 1, n), 1.0)])
            for n in range(1, 29)
        ]
    )
    assert len(await asyncio.to_thread(store.points, "t")) == 28


def test_closed_store_refuses_new_connections(tmp_path):
    s = Store(tmp_path / "x.db")
    s.close()
    with pytest.raises(RuntimeError):
        s.points("a")


# --- backup -------------------------------------------------------------------------------


def test_backup_is_a_readable_identical_copy(tmp_path, store):
    store.upsert_points([Point("a", day(2026, 1, n), float(n), n > 8) for n in range(1, 11)])
    store.upsert_points([Point("b", day(2026, 1, 1), 0.0)])
    store.record_success("eia", at=day(2026, 1, 2))
    store.set_setting("tank_gallons", "20")
    dest = tmp_path / "backups" / "copy.db"
    dest.parent.mkdir()

    assert store.backup(dest) == dest
    assert rows(dest) == rows(store.path)
    assert not dest.with_name("copy.db.tmp").exists()
    conn = sqlite3.connect(dest)
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"  # one file
    conn.close()
    with Store(dest) as restored:
        assert restored.points("a") == store.points("a")
        assert restored.get_status("eia").last_success_at == day(2026, 1, 2)
        assert restored.settings().tank_gallons == 20.0


def test_backup_overwrites_previous_copy(tmp_path, store):
    dest = tmp_path / "copy.db"
    store.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
    store.backup(dest)
    store.upsert_points([Point("a", day(2026, 1, 2), 2.0)])
    store.backup(dest)
    assert len(rows(dest)) == 2


def test_backup_refuses_live_path(store):
    with pytest.raises(ValueError):
        store.backup(store.path)


def test_backup_while_writing(tmp_path, store):
    errors = []

    def writer():
        try:
            for i in range(100):
                store.upsert_points([Point("w", day(2026, 1, 1) + timedelta(hours=i), 1.0)])
        except BaseException as e:  # noqa: BLE001 - surface any thread error
            errors.append(e)

    t = threading.Thread(target=writer)
    t.start()
    for n in range(5):
        store.backup(tmp_path / f"b{n}.db")
    t.join()
    assert errors == []
    assert len(rows(tmp_path / "b4.db")) <= 100


def test_cli_backup(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FW_DATA_DIR", str(tmp_path))
    with Store() as s:
        s.upsert_points([Point("a", day(2026, 1, 1), 1.0)])
    assert cli_main(["backup"]) == 0
    out = tmp_path / "fuelwatch-backup.db"
    assert rows(out) == rows(tmp_path / "fuelwatch.db")
    assert "fuelwatch-backup.db" in capsys.readouterr().out
    assert cli_main(["backup", str(tmp_path / "other.db")]) == 0
    assert (tmp_path / "other.db").exists()
