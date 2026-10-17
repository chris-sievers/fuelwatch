"""Rough performance check at production scale: five years of every daily
and weekly series plus a year of live AIS, about 265,000 points.

The bounds are generous (tens of times the measured numbers) so this never
flakes on a slow runner; it exists to catch an accidental O(n^2) or a lost
index. Run with ``pytest -s`` to see the timings."""

import time
from datetime import UTC, date, datetime, timedelta

from fuelwatch.models import Point
from fuelwatch.store import Store

START = datetime(2021, 10, 16, tzinfo=UTC)
DAILY_SERIES = 30  # spot prices and PortWatch chokepoints, daily for five years
WEEKLY_SERIES = 25  # retail, EU bulletin, stocks
AIS_SERIES = 4
AIS_SLOTS = 365 * 144


def dataset() -> dict[str, list[Point]]:
    out: dict[str, list[Point]] = {}
    for s in range(DAILY_SERIES):
        sid = f"daily.{s}"
        out[sid] = [Point(sid, START + timedelta(days=i), 50.0 + i % 7) for i in range(5 * 365)]
    for s in range(WEEKLY_SERIES):
        sid = f"weekly.{s}"
        out[sid] = [Point(sid, START + timedelta(weeks=i), 3.0 + i % 3) for i in range(5 * 52)]
    ais_start = datetime(2025, 10, 16, tzinfo=UTC)
    for s in range(AIS_SERIES):
        sid = f"hormuz_ais.{s}"
        out[sid] = [
            Point(sid, ais_start + timedelta(minutes=10 * (i + 1)), float(i % 40))
            for i in range(AIS_SLOTS)
        ]
    return out


def timed(fn, *args, **kw):
    t = time.perf_counter()
    result = fn(*args, **kw)
    return result, time.perf_counter() - t


def test_store_performance_at_scale(tmp_path):
    data = dataset()
    total = sum(len(v) for v in data.values())
    assert total > 250_000
    report = {}

    with Store(tmp_path / "fuelwatch.db") as store:
        # backfill-style writes: one transaction per series
        t = time.perf_counter()
        for pts in data.values():
            store.upsert_points(pts)
        report["insert all, one batch per series"] = time.perf_counter() - t

        # a PortWatch-style re-fetch of 35 days across 30 series, as one batch
        refetch = [p for s in range(DAILY_SERIES) for p in data[f"daily.{s}"][-35:]]
        _, report["re-upsert 35 days x 30 series"] = timed(store.upsert_points, refetch)

        # 10-minute AIS arrives four points per slot, one transaction each
        t = time.perf_counter()
        base = datetime(2026, 10, 16, tzinfo=UTC)
        for i in range(500):
            slot = base + timedelta(minutes=10 * (i + 1))
            store.upsert_points([Point(f"hormuz_ais.{s}", slot, 1.0) for s in range(AIS_SERIES)])
        report["500 single-slot AIS writes (per write)"] = (time.perf_counter() - t) / 500

        pts, report["5-year range, one daily series"] = timed(store.points, "daily.0")
        assert len(pts) == 5 * 365
        pts, report["1-year range of 10-minute AIS"] = timed(store.points, "hormuz_ais.0")
        assert len(pts) == AIS_SLOTS + 500
        days, report["1-year daily roll-up of AIS"] = timed(store.daily_rollup, "hormuz_ais.0")
        assert len(days) == 365 + 4  # plus the 500 slots written above
        _, report["latest 2 points"] = timed(store.latest, "hormuz_ais.0", 2)
        latest, report["series_latest (59 series)"] = timed(store.series_latest)
        assert len(latest) == DAILY_SERIES + WEEKLY_SERIES + AIS_SERIES

        t = time.perf_counter()
        for s in range(DAILY_SERIES):
            for sid in (f"daily.{s}", f"weekly.{s % WEEKLY_SERIES}"):
                assert store.value_on(sid, date(2026, 2, 27)) is not None
        report["value_on (per call)"] = (time.perf_counter() - t) / (2 * DAILY_SERIES)

        _, report["backup"] = timed(store.backup, tmp_path / "backup.db")

    size_mb = (tmp_path / "backup.db").stat().st_size / 1e6
    print(f"\nstore performance, {total:,} points, backup file {size_mb:.1f} MB")
    for name, secs in report.items():
        print(f"  {name:42s} {secs * 1000:9.2f} ms")

    assert report["insert all, one batch per series"] < 60
    assert report["1-year daily roll-up of AIS"] < 10
    assert report["value_on (per call)"] < 0.5
    assert report["series_latest (59 series)"] < 5
