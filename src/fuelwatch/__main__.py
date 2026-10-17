"""Command line: ``python -m fuelwatch backup [DEST]`` (also the ``fuelwatch``
console script). The nightly backup timer runs this inside the container."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from fuelwatch.store import Store, data_dir

BACKUP_NAME = "fuelwatch-backup.db"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fuelwatch")
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup", help="write a consistent copy of the database")
    b.add_argument(
        "dest",
        nargs="?",
        type=Path,
        help=f"output file (default: <data dir>/{BACKUP_NAME})",
    )
    args = parser.parse_args(argv)

    if args.command == "backup":
        dest = args.dest or data_dir() / BACKUP_NAME
        with Store() as store:
            out = store.backup(dest)
        print(f"backup written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
