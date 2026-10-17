"""Command line (also the ``fuelwatch`` console script):

    python -m fuelwatch backup [DEST]        consistent copy of the database
    python -m fuelwatch backfill [--reset]   run the five-year backfill now
    python -m fuelwatch collect --once       poll every collector once

The nightly backup timer runs ``backup`` inside the container. ``backfill``
and ``collect --once`` are manual one-offs; the app runs both itself.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

from fuelwatch.http import install_log_redaction
from fuelwatch.store import Store, data_dir

BACKUP_NAME = "fuelwatch-backup.db"


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs full request URLs at INFO, keys included; keep it quiet.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    install_log_redaction()


def _series_report(store: Store, names: list[str]) -> list[str]:
    from fuelwatch.registry import REGISTRY

    lines = []
    for name in names:
        ids = [e.id for e in REGISTRY.for_collector(name)]
        rows, present, lo, hi = 0, 0, None, None
        for sid in ids:
            pts = store.points(sid)  # oldest first
            if pts:
                rows += len(pts)
                present += 1
                lo = min(lo, pts[0].period_end) if lo else pts[0].period_end
                hi = max(hi, pts[-1].period_end) if hi else pts[-1].period_end

        def day(t):
            return t.date().isoformat() if t else "-"

        lines.append(
            f"  {name:16} {rows:7d} rows in {present}/{len(ids)} series, {day(lo)} to {day(hi)}"
        )
    return lines


def _chosen(only: list[str] | None):
    """--only names, already checked by argparse; none given means all."""
    from fuelwatch.registry import REGISTRY

    if only is None:
        return list(REGISTRY.polled)
    return [REGISTRY.collector(n) for n in dict.fromkeys(only)]


async def _backfill(args) -> int:
    from fuelwatch import backfill
    from fuelwatch.http import make_client

    collectors = _chosen(args.only)
    started = time.monotonic()
    with Store() as store:
        if args.reset:
            await backfill.reset(store, [c.name for c in collectors])
        async with make_client() as raw:
            http = backfill.paced(raw)
            results = await backfill.run(
                store, http, collectors=collectors, max_attempts=args.attempts
            )
        elapsed = time.monotonic() - started
        print(f"backfill finished in {elapsed:.1f} s")
        for r in results.values():
            note = "already done" if r.skipped else f"{r.rows} points written"
            print(
                f"  {r.collector:16} {r.state.value:8} {note}"
                + (f"  ERROR {r.error}" if r.error else "")
            )
        print("stored:")
        print("\n".join(_series_report(store, [c.name for c in collectors])))
        print("HTTP requests:", dict(sorted(http.requests.items())) or "none")
        if http.rate_limited:
            print("rate-limited responses:", http.rate_limited)
    return 0 if all(r.error is None for r in results.values()) else 1


async def _collect_once(args) -> int:
    from fuelwatch.scheduler import Scheduler

    collectors = _chosen(args.only)
    with Store() as store:
        sched = Scheduler(store, collectors=collectors, ais=None, run_backfill=False)
        results = await sched.poll_all()
        for r in results:
            line = f"  {r.collector:16} since {r.since}: {r.rows} points"
            print(line + (f"  ERROR {r.error}" if r.error else ""))
    return 0 if all(r.error is None for r in results) else 1


def main(argv: list[str] | None = None) -> int:
    from fuelwatch.registry import REGISTRY

    names = [c.name for c in REGISTRY.polled]
    parser = argparse.ArgumentParser(prog="fuelwatch")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup", help="write a consistent copy of the database")
    b.add_argument(
        "dest",
        nargs="?",
        type=Path,
        help=f"output file (default: <data dir>/{BACKUP_NAME})",
    )
    bf = sub.add_parser("backfill", help="run the five-year backfill for collectors not done")
    bf.add_argument("--reset", action="store_true", help="redo collectors already done")
    bf.add_argument("--only", action="append", choices=names, help="limit to a collector")
    bf.add_argument("--attempts", type=int, default=3, help="tries per collector (default 3)")
    c = sub.add_parser("collect", help="poll the collectors")
    c.add_argument("--once", action="store_true", required=True, help="poll each once and exit")
    c.add_argument("--only", action="append", choices=names, help="limit to a collector")
    args = parser.parse_args(argv)

    if args.command == "backup":
        dest = args.dest or data_dir() / BACKUP_NAME
        with Store() as store:
            out = store.backup(dest)
        print(f"backup written to {out}")
        return 0
    _setup_logging(args.verbose)
    if args.command == "backfill":
        return asyncio.run(_backfill(args))
    return asyncio.run(_collect_once(args))


if __name__ == "__main__":
    sys.exit(main())
