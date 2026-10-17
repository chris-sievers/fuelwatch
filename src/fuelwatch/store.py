"""SQLite storage: points, collector status and settings.

One database file, ``<data dir>/fuelwatch.db``; the data dir is ``FW_DATA_DIR``
(default ``./data``). Stdlib ``sqlite3`` only.

Storage format
    ``points.period_end`` and every other timestamp are INTEGER Unix epoch
    seconds, UTC. Integers sort correctly, compare cheaply and make the daily
    roll-up a plain integer division. Points must fall on whole seconds.
    ``provisional`` is 0/1. ``points`` is a WITHOUT ROWID table clustered on
    its primary key (series, period_end), so a series range is one index walk.

Time model
    A daily or weekly point sits at UTC midnight of its period date; a live
    AIS point sits at the *end* of its 10-minute slot. Wherever a ``date`` is
    accepted it means the whole UTC day: ``[D 00:00, D+1 00:00)``. A
    ``datetime`` means that exact instant. Range bounds are inclusive at
    both ends. Every list comes back oldest first.

Threading
    One connection per thread (``threading.local``). The app calls the store
    from ``asyncio.to_thread`` workers and each worker thread gets its own
    connection, so no connection is ever shared between threads. WAL lets
    readers run while a write is in progress; writes take ``BEGIN IMMEDIATE``
    and wait up to ``busy_timeout`` for the single write lock, so concurrent
    writers queue instead of failing.

Migrations
    Additive only: each migration is a list of statements, checked for
    destructive keywords before it runs. Pending migrations apply on open in
    one transaction. A database stamped with a *newer* version than this code
    knows is used as-is (rollback-safe schema): this module names every
    column it reads and writes, so extra columns and tables are harmless.
"""

from __future__ import annotations

import logging
import math
import os
import re
import sqlite3
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Self

from fuelwatch.http import ProviderError, redact
from fuelwatch.models import Point

log = logging.getLogger(__name__)

DB_NAME = "fuelwatch.db"
BUSY_TIMEOUT_MS = 5000
DAY = 86400
MAX_MESSAGE = 500

_SERIES_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,99}$")
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_ERROR_CLASS_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")
_DESTRUCTIVE_RE = re.compile(r"\b(DROP|DELETE|UPDATE|RENAME|REPLACE|TRUNCATE)\b", re.IGNORECASE)


# --- schema ---------------------------------------------------------------------------

# Index i holds the statements that take the schema from version i to i + 1.
# Append only. Never edit or remove a released entry.
MIGRATIONS: list[list[str]] = [
    [
        """CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""",
        """CREATE TABLE points (
            series TEXT NOT NULL,
            period_end INTEGER NOT NULL,
            value REAL NOT NULL,
            provisional INTEGER NOT NULL DEFAULT 0 CHECK (provisional IN (0, 1)),
            fetched_at INTEGER NOT NULL,
            PRIMARY KEY (series, period_end)
        ) WITHOUT ROWID""",
        """CREATE TABLE status (
            collector TEXT PRIMARY KEY,
            last_run_at INTEGER,
            last_success_at INTEGER,
            last_error_class TEXT,
            last_error_message TEXT,
            backfill_state TEXT NOT NULL DEFAULT 'not_started'
                CHECK (backfill_state IN ('not_started', 'running', 'done')),
            backfill_cursor TEXT,
            backfill_progress REAL NOT NULL DEFAULT 0,
            backfill_updated_at INTEGER
        )""",
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('reference_date', '2026-02-27')",
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('baseline_days', '90')",
        "INSERT OR IGNORE INTO meta (key, value) VALUES ('tank_gallons', '15')",
    ],
]
SCHEMA_VERSION = len(MIGRATIONS)


class DestructiveMigrationError(RuntimeError):
    """A migration tried to drop, delete, update or rename something."""


def check_additive(statements: Sequence[str]) -> None:
    """Raise if any statement could destroy or rewrite existing data.
    ``INSERT OR IGNORE`` is allowed for seeding; ``INSERT OR REPLACE`` is not."""
    for sql in statements:
        code = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.DOTALL)
        code = re.sub(r"'(?:[^']|'')*'", "''", code)  # ignore words inside literals
        if m := _DESTRUCTIVE_RE.search(code):
            raise DestructiveMigrationError(f"migration uses {m.group(1).upper()}: {sql[:80]!r}")


def schema_version(conn: sqlite3.Connection) -> int:
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'"
    ).fetchone()
    if not has_meta:
        return 0
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    return int(row[0]) if row else 0


def migrate(conn: sqlite3.Connection, migrations: Sequence[Sequence[str]] = MIGRATIONS) -> int:
    """Apply pending migrations in one transaction; return the resulting version.
    Every pending migration is checked before any runs. A newer database is
    left untouched."""
    target = len(migrations)
    conn.execute("BEGIN IMMEDIATE")
    try:
        current = schema_version(conn)
        if current >= target:
            conn.execute("ROLLBACK")
            if current > target:
                log.info("database schema v%d is newer than this code (v%d)", current, target)
            return current
        pending = migrations[current:]
        for statements in pending:
            check_additive(statements)
        for statements in pending:
            for sql in statements:
                conn.execute(sql)
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (str(target),),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    log.info("database schema migrated from v%d to v%d", current, target)
    return target


# --- types ----------------------------------------------------------------------------


class BackfillState(str, Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    DONE = "done"


@dataclass(frozen=True)
class Backfill:
    state: BackfillState
    cursor: str | None  # opaque to the store; the backfill decides its format
    progress: float  # 0.0 to 1.0
    updated_at: datetime | None


@dataclass(frozen=True)
class CollectorStatus:
    collector: str
    last_run_at: datetime | None
    last_success_at: datetime | None
    error_class: str | None  # None: the last run succeeded (or none has run)
    error_message: str | None  # sanitized
    backfill: Backfill


@dataclass(frozen=True)
class DailyPoint:
    """One UTC day of an intraday series: the mean of its slots."""

    series: str
    day: date
    value: float
    slots: int  # how many 10-minute slots the day has; 144 is a full day
    provisional: bool


@dataclass(frozen=True)
class Settings:
    reference_date: date
    baseline_days: int
    tank_gallons: float


_SETTING_PARSERS = {
    "reference_date": date.fromisoformat,
    "baseline_days": int,
    "tank_gallons": float,
}


# --- conversions ----------------------------------------------------------------------


def data_dir() -> Path:
    return Path(os.environ.get("FW_DATA_DIR") or "./data")


def default_path() -> Path:
    return data_dir() / DB_NAME


def _epoch(t: datetime) -> int:
    return int(t.timestamp())


def _from_epoch(ts: int | None) -> datetime | None:
    return None if ts is None else datetime.fromtimestamp(ts, UTC)


def _day_start(d: date) -> int:
    return _epoch(datetime(d.year, d.month, d.day, tzinfo=UTC))


def _check_instant(t: datetime, what: str) -> None:
    if not isinstance(t, datetime):
        raise TypeError(f"{what} must be a datetime")
    if t.tzinfo is None or t.utcoffset() != timedelta(0):
        raise ValueError(f"{what} must be timezone-aware UTC")


def _lower(bound: date | datetime | None) -> int | None:
    """Inclusive lower bound in epoch seconds."""
    if bound is None:
        return None
    if isinstance(bound, datetime):
        _check_instant(bound, "start")
        return _epoch(bound) if bound.microsecond == 0 else _epoch(bound) + 1
    return _day_start(bound)


def _upper(bound: date | datetime | None) -> int | None:
    """Inclusive upper bound in epoch seconds; a date covers its whole day."""
    if bound is None:
        return None
    if isinstance(bound, datetime):
        _check_instant(bound, "end")
        return _epoch(bound)
    return _day_start(bound) + DAY - 1


def _point(row: tuple) -> Point:
    series, ts, value, provisional = row
    return Point(series, datetime.fromtimestamp(ts, UTC), value, bool(provisional))


def validate_point(p: object) -> tuple[str, int, float, int]:
    """Check one point and return its row values. Raises ValueError/TypeError."""
    if not isinstance(p, Point):
        raise TypeError(f"expected Point, got {type(p).__name__}")
    if not isinstance(p.series, str) or not _SERIES_RE.match(p.series):
        raise ValueError(f"bad series id {p.series!r}")
    _check_instant(p.period_end, "period_end")
    if p.period_end.microsecond:
        raise ValueError(f"period_end must be a whole second: {p.period_end.isoformat()}")
    if p.period_end.year < 1970:
        raise ValueError(f"period_end before 1970: {p.period_end.isoformat()}")
    v = p.value
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
        raise ValueError(f"value must be a finite number, got {v!r} for {p.series}")
    if not isinstance(p.provisional, bool):
        raise TypeError(f"provisional must be bool for {p.series}")
    return p.series, _epoch(p.period_end), float(v), int(p.provisional)


def _clean_message(message: str) -> str:
    text = _CONTROL_RE.sub(" ", redact(str(message))).strip()
    return text[:MAX_MESSAGE]


def _check_name(collector: str) -> None:
    if not isinstance(collector, str) or not _NAME_RE.match(collector):
        raise ValueError(f"bad collector name {collector!r}")


# --- the store ------------------------------------------------------------------------


class Store:
    """Synchronous store; safe to call from many threads (one connection each)."""

    def __init__(self, path: str | os.PathLike | None = None) -> None:
        self.path = Path(path) if path is not None else default_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._conns: list[sqlite3.Connection] = []
        self._lock = threading.Lock()
        self._closed = False
        self.schema_version = migrate(self._conn())

    # connections

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            return conn
        if self._closed:
            raise RuntimeError("store is closed")
        # isolation_level=None: no implicit transactions; we BEGIN explicitly.
        # check_same_thread=False only so close() can close every thread's
        # connection; each connection is otherwise used by its own thread.
        conn = sqlite3.connect(
            self.path,
            timeout=BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if mode.lower() != "wal":
            conn.close()
            raise RuntimeError(f"could not enable WAL mode (got {mode!r}) for {self.path}")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA synchronous = NORMAL")  # durable across crashes in WAL mode
        self._local.conn = conn
        with self._lock:
            self._conns.append(conn)
        return conn

    def close(self) -> None:
        """Close every thread's connection. Call once, at shutdown."""
        with self._lock:
            self._closed = True
            conns, self._conns = self._conns, []
        for conn in conns:
            conn.close()
        self._local = threading.local()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _write(self, sql: str, rows: Iterable[tuple] | tuple = ()) -> None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            if isinstance(rows, tuple):
                conn.execute(sql, rows)
            else:
                conn.executemany(sql, rows)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    # points: writes

    def upsert_points(self, points: Iterable[Point], fetched_at: datetime | None = None) -> int:
        """Write points in one transaction; return how many. A point for an
        existing (series, period_end) replaces its value and provisional flag
        together. The whole batch is validated first: one bad point writes
        nothing. Within a batch, the last point for a period wins."""
        rows = [validate_point(p) for p in points]
        if fetched_at is None:
            fetched_at = datetime.now(UTC)
        _check_instant(fetched_at, "fetched_at")
        if not rows:
            return 0
        ts = _epoch(fetched_at)
        self._write(
            "INSERT INTO points (series, period_end, value, provisional, fetched_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (series, period_end) DO UPDATE SET "
            "value = excluded.value, provisional = excluded.provisional, "
            "fetched_at = excluded.fetched_at",
            [(*r, ts) for r in rows],
        )
        return len(rows)

    # points: reads

    def points(
        self,
        series: str,
        start: date | datetime | None = None,
        end: date | datetime | None = None,
        *,
        settled_only: bool = False,
    ) -> list[Point]:
        """Points with start <= period_end <= end, oldest first. None is open-ended."""
        sql = "SELECT series, period_end, value, provisional FROM points WHERE series = ?"
        args: list = [series]
        if (lo := _lower(start)) is not None:
            sql += " AND period_end >= ?"
            args.append(lo)
        if (hi := _upper(end)) is not None:
            sql += " AND period_end <= ?"
            args.append(hi)
        if settled_only:
            sql += " AND provisional = 0"
        sql += " ORDER BY period_end"
        return [_point(r) for r in self._conn().execute(sql, args)]

    def latest(self, series: str, n: int = 1, *, settled_only: bool = False) -> list[Point]:
        """The newest n points, returned oldest first."""
        if n < 1:
            return []
        sql = "SELECT series, period_end, value, provisional FROM points WHERE series = ?"
        if settled_only:
            sql += " AND provisional = 0"
        sql += " ORDER BY period_end DESC LIMIT ?"
        rows = self._conn().execute(sql, (series, n)).fetchall()
        return [_point(r) for r in reversed(rows)]

    def value_on(self, series: str, on: date | datetime) -> Point | None:
        """The last observation on or before `on` (a date includes its whole
        UTC day), or None if the series has nothing that early."""
        hi = _upper(on)
        row = (
            self._conn()
            .execute(
                "SELECT series, period_end, value, provisional FROM points "
                "WHERE series = ? AND period_end <= ? ORDER BY period_end DESC LIMIT 1",
                (series, hi),
            )
            .fetchone()
        )
        return _point(row) if row else None

    def series_latest(self) -> dict[str, Point]:
        """Every stored series with its newest point."""
        # A skip-scan over the primary key: a few index seeks per series instead
        # of a GROUP BY over every row (0.5 ms rather than 33 ms at 270k rows).
        rows = self._conn().execute(
            "WITH RECURSIVE s(series) AS ("
            " SELECT MIN(series) FROM points"
            " UNION ALL"
            " SELECT (SELECT MIN(series) FROM points WHERE series > s.series)"
            " FROM s WHERE s.series IS NOT NULL) "
            "SELECT p.series, p.period_end, p.value, p.provisional FROM s JOIN points p"
            " ON p.series = s.series"
            " AND p.period_end = (SELECT MAX(period_end) FROM points WHERE series = s.series) "
            "ORDER BY p.series"
        )
        return {r[0]: _point(r) for r in rows}

    def daily_rollup(
        self, series: str, start: date | None = None, end: date | None = None
    ) -> list[DailyPoint]:
        """One point per UTC day for a 10-minute series, days start..end
        inclusive, oldest first. A slot belongs to the day it starts in: points
        stamped at slot end, so (D 00:00, D+1 00:00]. The value is the mean of
        the day's slots; missing slots are left out, never counted as zero."""
        sql = (
            "SELECT (period_end - 1) / ? AS d, AVG(value), COUNT(*), MAX(provisional) "
            "FROM points WHERE series = ?"
        )
        args: list = [DAY, series]
        if start is not None:
            sql += " AND period_end > ?"
            args.append(_day_start(start))
        if end is not None:
            sql += " AND period_end <= ?"
            args.append(_day_start(end) + DAY)
        sql += " GROUP BY d ORDER BY d"
        epoch0 = date(1970, 1, 1)
        return [
            DailyPoint(series, epoch0 + timedelta(days=d), avg, n, bool(prov))
            for d, avg, n, prov in self._conn().execute(sql, args)
        ]

    # status

    def record_success(self, collector: str, at: datetime | None = None) -> None:
        """A run finished without error; clears the last error."""
        _check_name(collector)
        ts = _epoch(at or datetime.now(UTC))
        self._write(
            "INSERT INTO status (collector, last_run_at, last_success_at) VALUES (?, ?, ?) "
            "ON CONFLICT (collector) DO UPDATE SET last_run_at = excluded.last_run_at, "
            "last_success_at = excluded.last_success_at, "
            "last_error_class = NULL, last_error_message = NULL",
            (collector, ts, ts),
        )

    def record_failure(
        self, collector: str, error: BaseException, at: datetime | None = None
    ) -> None:
        """A run failed. A ProviderError keeps its class and sanitized message;
        any other exception is stored as class 'internal' with only its type
        name, because its text may hold anything."""
        _check_name(collector)
        if isinstance(error, ProviderError) and _ERROR_CLASS_RE.match(error.error_class or ""):
            error_class, message = error.error_class, _clean_message(str(error))
        else:
            error_class, message = "internal", type(error).__name__
        ts = _epoch(at or datetime.now(UTC))
        self._write(
            "INSERT INTO status (collector, last_run_at, last_error_class, last_error_message) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (collector) DO UPDATE SET last_run_at = excluded.last_run_at, "
            "last_error_class = excluded.last_error_class, "
            "last_error_message = excluded.last_error_message",
            (collector, ts, error_class, message),
        )

    def get_status(self, collector: str) -> CollectorStatus | None:
        rows = self._status_rows("WHERE collector = ?", (collector,))
        return rows[0] if rows else None

    def all_status(self) -> list[CollectorStatus]:
        return self._status_rows("", ())

    def _status_rows(self, where: str, args: tuple) -> list[CollectorStatus]:
        rows = self._conn().execute(
            "SELECT collector, last_run_at, last_success_at, last_error_class, "
            "last_error_message, backfill_state, backfill_cursor, backfill_progress, "
            f"backfill_updated_at FROM status {where} ORDER BY collector",
            args,
        )
        return [
            CollectorStatus(
                c,
                _from_epoch(run),
                _from_epoch(ok),
                ec,
                em,
                Backfill(BackfillState(bs), bc, bp, _from_epoch(bu)),
            )
            for c, run, ok, ec, em, bs, bc, bp, bu in rows
        ]

    def get_backfill(self, collector: str) -> Backfill:
        status = self.get_status(collector)
        if status is None:
            return Backfill(BackfillState.NOT_STARTED, None, 0.0, None)
        return status.backfill

    def set_backfill(
        self,
        collector: str,
        state: BackfillState,
        cursor: str | None = None,
        progress: float = 0.0,
        at: datetime | None = None,
    ) -> None:
        """Save backfill state, cursor and progress together, for resuming
        after a restart. Call it after each chunk's points are upserted."""
        _check_name(collector)
        state = BackfillState(state)
        if cursor is not None and not isinstance(cursor, str):
            raise TypeError("cursor must be str or None")
        if not (isinstance(progress, int | float) and 0.0 <= progress <= 1.0):
            raise ValueError(f"progress must be between 0 and 1, got {progress!r}")
        ts = _epoch(at or datetime.now(UTC))
        self._write(
            "INSERT INTO status (collector, backfill_state, backfill_cursor, "
            "backfill_progress, backfill_updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (collector) DO UPDATE SET backfill_state = excluded.backfill_state, "
            "backfill_cursor = excluded.backfill_cursor, "
            "backfill_progress = excluded.backfill_progress, "
            "backfill_updated_at = excluded.backfill_updated_at",
            (collector, state.value, cursor, float(progress), ts),
        )

    # settings

    def get_setting(self, key: str) -> str | None:
        row = self._conn().execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_setting(self, key: str, value: str) -> None:
        """Set a setting. Known settings are checked so a bad value is refused
        here, not on the next read. The schema version is not a setting."""
        _check_name(key)
        if key == "schema_version":
            raise ValueError("schema_version is managed by migrations")
        value = str(value)
        if parse := _SETTING_PARSERS.get(key):
            parsed = parse(value)
            if key != "reference_date" and not (math.isfinite(parsed) and parsed > 0):
                raise ValueError(f"{key} must be a positive number")
        self._write(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def settings(self) -> Settings:
        return Settings(
            reference_date=date.fromisoformat(self.get_setting("reference_date")),
            baseline_days=int(self.get_setting("baseline_days")),
            tank_gallons=float(self.get_setting("tank_gallons")),
        )

    # backup

    def backup(self, dest: str | os.PathLike) -> Path:
        """Write a consistent copy to `dest` with SQLite's online backup API,
        safe while the app is writing. The copy is written beside `dest` and
        renamed into place, so `dest` is never a half-written file."""
        dest = Path(dest)
        if dest.resolve() == self.path.resolve():
            raise ValueError("backup destination is the live database")
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.unlink(missing_ok=True)
        target = sqlite3.connect(tmp)
        try:
            self._conn().backup(target)
            ok = target.execute("PRAGMA quick_check").fetchone()[0]
            if ok != "ok":
                raise RuntimeError(f"backup failed its integrity check: {ok}")
            # A rollback-journal copy is one self-contained file for restic.
            target.execute("PRAGMA journal_mode = DELETE")
        except BaseException:
            target.close()
            tmp.unlink(missing_ok=True)
            raise
        target.close()
        os.replace(tmp, dest)
        return dest
