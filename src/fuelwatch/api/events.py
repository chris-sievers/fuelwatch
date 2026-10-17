"""Event markers: a hand-edited, committed file, ``src/fuelwatch/events.json``.

The file is a JSON list. Each entry is an object with exactly three keys::

    {"date": "2026-02-28", "label": "One neutral line", "url": "https://..."}

- ``date``: an ISO date (YYYY-MM-DD), 1970 to 2100;
- ``label``: one line of plain text, 1 to 120 characters, no control characters;
- ``url``: an absolute ``https:`` link with a host, at most 500 characters.

``load`` rejects anything else with EventsError, naming the entry. The app
loads the file once at start, so a bad file stops it from starting; CI
validates the committed file (tests/integration/test_events.py).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

PATH = Path(__file__).resolve().parent.parent / "events.json"
MAX_EVENTS = 100
MAX_LABEL = 120
MAX_URL = 500
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f  ]")
_URL_BAD_RE = re.compile(r"[\s\x00-\x1f\x7f<>\"'`\\]")
KEYS = {"date", "label", "url"}


class EventsError(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    date: date
    label: str
    url: str


def _check(i: int, raw: object) -> Event:
    where = f"event {i}"
    if not isinstance(raw, dict):
        raise EventsError(f"{where}: must be an object")
    if set(raw) != KEYS:
        raise EventsError(f"{where}: keys must be exactly {sorted(KEYS)}")
    d, label, url = raw["date"], raw["label"], raw["url"]
    if not isinstance(d, str) or not _DATE_RE.match(d):
        raise EventsError(f"{where}: date must be YYYY-MM-DD")
    try:
        day = date.fromisoformat(d)
    except ValueError:
        raise EventsError(f"{where}: {d!r} is not a real date") from None
    if not date(1970, 1, 1) <= day <= date(2100, 12, 31):
        raise EventsError(f"{where}: date out of range")
    if not isinstance(label, str) or not label.strip():
        raise EventsError(f"{where}: label must be non-empty text")
    if label != label.strip() or _CONTROL_RE.search(label):
        raise EventsError(f"{where}: label must be one line with no surrounding spaces")
    if len(label) > MAX_LABEL:
        raise EventsError(f"{where}: label is over {MAX_LABEL} characters")
    if not isinstance(url, str) or len(url) > MAX_URL or _URL_BAD_RE.search(url):
        raise EventsError(f"{where}: url must be at most {MAX_URL} characters with no spaces")
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise EventsError(f"{where}: url must be an https:// link with a host")
    return Event(day, label, url)


def parse(doc: object) -> tuple[Event, ...]:
    if not isinstance(doc, list):
        raise EventsError("events file must be a JSON list")
    if len(doc) > MAX_EVENTS:
        raise EventsError(f"events file has over {MAX_EVENTS} entries")
    events = [_check(i, raw) for i, raw in enumerate(doc)]
    return tuple(sorted(events, key=lambda e: e.date))


def load(path: Path = PATH) -> tuple[Event, ...]:
    try:
        doc = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise EventsError(f"events file is not valid JSON: {e.msg} at line {e.lineno}") from None
    return parse(doc)
