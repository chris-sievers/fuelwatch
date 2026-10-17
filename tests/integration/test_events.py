"""Event markers: the committed file must validate (this runs in CI), and
only https links, short one-line labels and real dates are accepted."""

from __future__ import annotations

import json

import pytest

from fuelwatch.api import events

GOOD = {"date": "2026-02-28", "label": "A neutral label", "url": "https://example.org/x"}


def test_committed_file_is_valid():
    assert events.PATH.name == "events.json"
    assert events.PATH.parent.name == "fuelwatch"  # in the package, not the data dir
    loaded = events.load()
    assert all(e.url.startswith("https://") for e in loaded)


def test_good_entry():
    (e,) = events.parse([GOOD])
    assert e.date.isoformat() == "2026-02-28" and e.url == GOOD["url"]


@pytest.mark.parametrize(
    "url",
    [
        "http://example.org/x",
        "javascript:alert(1)",
        "data:text/html,hi",
        "//example.org/x",
        "https:///nohost",
        "https://user:pw@example.org/",
        "https://example.org/a b",
        "HTTPS://example.org/" + "x" * 500,
        "ftp://example.org/",
        'https://example.org/"onmouseover=x',
    ],
)
def test_rejects_non_https_or_odd_links(url):
    with pytest.raises(events.EventsError, match="event 0: url"):
        events.parse([{**GOOD, "url": url}])


@pytest.mark.parametrize(
    "label",
    ["", "   ", "x" * 121, "two\nlines", " padded", "tab\there", "nul\x00", "ls x", 5],
)
def test_rejects_bad_labels(label):
    with pytest.raises(events.EventsError, match="event 0: label"):
        events.parse([{**GOOD, "label": label}])


def test_label_at_the_limit():
    events.parse([{**GOOD, "label": "x" * 120}])


@pytest.mark.parametrize("d", ["2026-02-30", "2026-2-28", "20260228", "1969-12-31", "", None])
def test_rejects_bad_dates(d):
    with pytest.raises(events.EventsError, match="event 0: date|not a real date"):
        events.parse([{**GOOD, "date": d}])


@pytest.mark.parametrize(
    "doc",
    [{}, "x", [GOOD, "not an object"], [{**GOOD, "extra": 1}], [{"date": "2026-01-01"}]],
)
def test_rejects_bad_structure(doc):
    with pytest.raises(events.EventsError):
        events.parse(doc)


def test_rejects_too_many():
    with pytest.raises(events.EventsError, match="over 100"):
        events.parse([GOOD] * 101)


def test_load_reports_bad_json(tmp_path):
    f = tmp_path / "e.json"
    f.write_text("[{")
    with pytest.raises(events.EventsError, match="not valid JSON"):
        events.load(f)


def test_bad_file_stops_the_app(tmp_path):
    from fuelwatch.app import create_app

    f = tmp_path / "e.json"
    f.write_text(json.dumps([{**GOOD, "url": "http://example.org/"}]))
    with pytest.raises(events.EventsError):
        create_app(scheduler_factory=None, metrics_port=None, events_path=f)
