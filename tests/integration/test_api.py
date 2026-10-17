"""The API: routes, bounds, headers, caching, errors, lifespan and /metrics."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, timedelta

import httpx
import pytest

from fuelwatch import calc
from fuelwatch.api import views
from fuelwatch.app import CSP, create_app
from fuelwatch.collectors.ais_tracker import SERIES_SHIPS
from fuelwatch.collectors.aisstream import AisStatus
from fuelwatch.http import ProviderError, redact
from fuelwatch.registry import REGISTRY
from fuelwatch.store import BackfillState, Store, StoreClosedError
from fuelwatch.threads import is_store_closed

from .conftest import NOW, FakeAis, FakeScheduler

HEADERS = {
    "content-security-policy": CSP,
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
}


def assert_headers(resp):
    for k, v in HEADERS.items():
        assert resp.headers.get(k) == v, (resp.status_code, k)


def counting(monkeypatch, store, name):
    calls = []
    real = getattr(store, name)

    def wrapper(*a, **kw):
        calls.append(a)
        return real(*a, **kw)

    monkeypatch.setattr(store, name, wrapper)
    return calls


# --- routes -----------------------------------------------------------------------


def test_healthz_returns_only_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.text == "ok"
    assert r.headers["content-type"].startswith("text/plain")


@pytest.mark.parametrize("path", ["/", "/about"])
def test_pages_are_csp_clean(client, path):
    r = client.get(path)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert r.headers["cache-control"] == "no-cache"
    html = r.text
    # nothing the CSP would block: no inline script or style, no event
    # handler attributes, nothing from another origin
    assert "style=" not in html and "<style" not in html
    assert not re.search(r"<script(?![^>]*\bsrc=\"/static/)", html)
    assert not re.search(r"\son[a-z]+=", html)
    assert not re.search(r"(src|href)=\"(https?:)?//", html.split("<main", 1)[0])
    assert_headers(r)


def test_static_files_served_with_headers(client):
    for path, kind in [
        ("/static/app.js", "javascript"),
        ("/static/app.css", "text/css"),
        ("/static/icon.svg", "image/svg+xml"),
        ("/static/vendor/chart.js-4.5.1/chart.umd.min.js", "javascript"),
    ]:
        r = client.get(path)
        assert r.status_code == 200, path
        assert kind in r.headers["content-type"], path
        assert_headers(r)
    assert client.get("/static/nope.js").status_code == 404


def test_version_defaults_to_unknown(client, monkeypatch):
    r = client.get("/api/version")
    assert r.json() == {"commit": "unknown", "build_time": "unknown"}


def test_version_from_env(make_client, monkeypatch):
    monkeypatch.setenv("FW_COMMIT", "0123abc")
    monkeypatch.setenv("FW_BUILD_TIME", "2026-10-17T12:00:00Z")
    r = make_client().get("/api/version")
    assert r.json() == {"commit": "0123abc", "build_time": "2026-10-17T12:00:00Z"}


def test_version_rejects_odd_values(make_client, monkeypatch):
    monkeypatch.setenv("FW_COMMIT", "<script>alert(1)</script>")
    assert make_client().get("/api/version").json()["commit"] == "unknown"


def test_summary_shape(client):
    r = client.get("/api/summary")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"text", "tiles", "as_of", "settings"}
    assert [t["key"] for t in body["tiles"]] == [
        "brent",
        "gasoline",
        "diesel",
        "tank",
        "hormuz",
        "crude_stocks",
    ]
    brent = body["tiles"][0]
    assert brent["value"] == 70 + (date(2026, 10, 14) - date(2020, 1, 1)).days % 20
    assert brent["as_of"] == "2026-10-14"
    assert body["settings"]["reference_date"] == "2026-02-27"
    assert body["text"].endswith(".")


def test_series_lists_every_registered_series(client):
    r = client.get("/api/series")
    assert r.status_code == 200
    body = r.json()
    ids = [s["id"] for s in body["series"]]
    assert ids == [e.id for e in REGISTRY]
    by = {s["id"]: s for s in body["series"]}
    brent = by[calc.BRENT]
    assert brent["latest"]["period"] == "2026-10-14"
    assert brent["previous"]["period"] == "2026-10-13"
    assert brent["status"] in ("ok", "late")
    assert brent["unit"] == "$/bbl" and brent["frequency"] == "daily"
    assert brent["as_of"] == "2026-10-14"
    assert brent["data_api"] is True  # spot: served by default (FW_API_SPOT_DATA=1)
    assert "released 2026-10-15" in brent["attribution"]  # Columbus Day moved it to Thursday
    assert "Refinitiv" in brent["attribution"]
    hz = by[calc.HORMUZ]
    assert hz["latest"] == {
        "period": "2026-10-11",
        "value": hz["latest"]["value"],
        "provisional": True,
    }
    assert hz["terms_url"] == "https://www.imf.org/en/About/copyright-and-terms"
    assert "Kpler; UN Global Platform; IMF PortWatch" in hz["attribution"]
    empty = by["eia.stocks.crude_commercial"]
    assert empty["latest"] is None and empty["previous"] is None
    assert empty["status"] == "no_data"
    ais = by[SERIES_SHIPS]
    assert ais["latest"]["period"].endswith("Z")
    eu = next(s for s in body["series"] if s["collector"] == "eu_oil_bulletin")
    assert "European Commission" in eu["attribution"] and "1,000 L" in eu["attribution"]


def test_every_source_has_attribution():
    from fuelwatch.api.attribution import attribution

    for e in REGISTRY:
        assert attribution(e.spec.source, None)


# --- /api/data --------------------------------------------------------------------


def test_data_raw_daily_with_provisional_flags(client):
    r = client.get(f"/api/data/{calc.HORMUZ}", params={"start": "2026-10-01", "end": "2026-10-17"})
    assert r.status_code == 200
    body = r.json()
    assert body["resolution"] == "raw"
    assert body["start"] == "2026-10-01" and body["end"] == "2026-10-17"
    assert [p["period"] for p in body["points"]][-2:] == ["2026-10-10", "2026-10-11"]
    assert [p["provisional"] for p in body["points"]][-3:] == [False, True, True]
    assert body["terms_url"].startswith("https://www.imf.org/")
    assert "IMF PortWatch" in body["attribution"]


def test_data_weekly_raw(client):
    r = client.get(f"/api/data/{calc.GASOLINE_US}", params={"start": "2026-09-01"})
    body = r.json()
    assert body["end"] == "2026-10-17"
    assert [p["period"] for p in body["points"]] == [
        "2026-09-07",
        "2026-09-14",
        "2026-09-21",
        "2026-09-28",
        "2026-10-05",
        "2026-10-12",
    ]
    assert "U.S. Energy Information Administration" in body["attribution"]


def test_data_default_range_is_a_year(client):
    body = client.get(f"/api/data/{calc.GASOLINE_US}").json()
    assert body["end"] == "2026-10-17"
    assert body["start"] == (NOW.date() - timedelta(days=365)).isoformat()


def test_ais_short_range_is_raw(client):
    r = client.get(f"/api/data/{SERIES_SHIPS}", params={"start": "2026-10-11", "end": "2026-10-17"})
    body = r.json()
    assert body["resolution"] == "raw"
    assert body["points"][0]["period"] == "2026-10-11T00:00:00Z"
    assert "slots" not in body["points"][0]


def test_ais_long_range_rolls_up_to_days(client):
    r = client.get(f"/api/data/{SERIES_SHIPS}", params={"start": "2026-10-08", "end": "2026-10-17"})
    body = r.json()
    assert body["resolution"] == "daily"
    days = {p["period"]: p for p in body["points"]}
    assert days["2026-10-09"]["slots"] == 144 and days["2026-10-09"]["partial"] is False
    assert days["2026-10-09"]["value"] == pytest.approx(22.5)  # mean of 20..25
    assert days["2026-10-17"]["partial"] is True  # today, still running
    assert "AISStream" in body["attribution"]


def test_unknown_series_is_404(client):
    r = client.get("/api/data/no.such.series")
    assert r.status_code == 404
    assert r.json() == {"detail": "unknown series"}
    assert_headers(r)


@pytest.mark.parametrize(
    "params",
    [
        {"start": "2026-1-1"},
        {"start": "abc"},
        {"start": "2026-02-30"},
        {"end": "20261017"},
        {"start": "2026-01-01T00:00:00"},  # over max_length
        {"start": "1969-12-31"},
        {"start": "2026-10-10", "end": "2026-10-01"},
        {"start": "2020-01-01", "end": "2026-10-17"},  # over five years
        {"start": "2021-10-16", "end": "2026-10-17"},  # 1828 days
    ],
)
def test_bad_ranges_are_400(client, params):
    r = client.get(f"/api/data/{calc.GASOLINE_US}", params=params)
    assert r.status_code == 400, params
    assert "detail" in r.json()
    assert "input" not in json.dumps(r.json())  # the sent value is never echoed
    assert_headers(r)


def test_five_years_exactly_is_allowed(client):
    r = client.get(
        f"/api/data/{calc.GASOLINE_US}", params={"start": "2021-10-17", "end": "2026-10-17"}
    )
    assert r.status_code == 200
    assert len(r.json()["points"]) > 250


def test_spot_series_excluded_when_switched_off(make_client):
    c = make_client(spot_data=False)
    for sid in views.SPOT_SERIES:
        r = c.get(f"/api/data/{sid}")
        assert r.status_code == 403, sid
        assert "Refinitiv" in r.json()["detail"]
        assert_headers(r)
    # still shown as latest values and charted on the dashboard
    by = {s["id"]: s for s in c.get("/api/series").json()["series"]}
    assert by[calc.BRENT]["latest"] is not None
    assert by[calc.BRENT]["data_api"] is False
    tiles = c.get("/api/summary").json()["tiles"]
    assert tiles[0]["key"] == "brent" and tiles[0]["value"] is not None
    assert c.get("/api/dashboard").json()["crude"]["brent"]


def test_spot_series_served_by_default(make_client):
    c = make_client()
    r = c.get(f"/api/data/{calc.BRENT}", params={"start": "2026-10-12", "end": "2026-10-14"})
    assert r.status_code == 200
    assert [p["period"] for p in r.json()["points"]] == ["2026-10-12", "2026-10-13", "2026-10-14"]
    assert "Refinitiv" in r.json()["attribution"]
    by = {s["id"]: s for s in c.get("/api/series").json()["series"]}
    assert by[calc.BRENT]["data_api"] is True


def test_spot_data_keeps_the_five_year_cap(make_client):
    c = make_client()
    r = c.get(f"/api/data/{calc.BRENT}", params={"start": "2020-01-01", "end": "2026-10-14"})
    assert r.status_code == 400


def test_spot_setting_from_env(make_client, monkeypatch, db_path, clock):
    monkeypatch.delenv("FW_API_SPOT_DATA", raising=False)
    assert make_client(spot_data=None).get(f"/api/data/{calc.WTI}").status_code == 200
    monkeypatch.setenv("FW_API_SPOT_DATA", "0")
    assert make_client(spot_data=None).get(f"/api/data/{calc.WTI}").status_code == 403
    monkeypatch.setenv("FW_API_SPOT_DATA", "1")
    assert make_client(spot_data=None).get(f"/api/data/{calc.WTI}").status_code == 200
    monkeypatch.setenv("FW_API_SPOT_DATA", "yes")
    with pytest.raises(ValueError, match="FW_API_SPOT_DATA"):
        create_app(scheduler_factory=None, metrics_port=None)


def test_only_get(client):
    assert client.post("/api/series").status_code == 405


# --- /api/events ------------------------------------------------------------------


def test_events_route(make_client, tmp_path):
    f = tmp_path / "events.json"
    f.write_text(
        json.dumps(
            [
                {"date": "2026-03-02", "label": "Second", "url": "https://example.org/b"},
                {"date": "2026-02-28", "label": "First", "url": "https://example.org/a"},
            ]
        )
    )
    body = make_client(events_path=f).get("/api/events").json()
    assert [e["label"] for e in body["events"]] == ["First", "Second"]
    assert body["events"][0] == {
        "date": "2026-02-28",
        "label": "First",
        "url": "https://example.org/a",
    }


def test_committed_events_served(client):
    assert client.get("/api/events").json() == {"events": []}


# --- /api/status ------------------------------------------------------------------


def test_status_rows(make_client, db_path, clock):
    with Store(db_path) as s:
        s.record_success("eia", at=NOW - timedelta(hours=1))
        s.set_backfill("eia", BackfillState.DONE, "2026-10-14", 1.0)
        s.record_failure("portwatch", ProviderError("http_status", "503 from portwatch"), at=NOW)
        s.set_backfill("portwatch", BackfillState.DONE, "2026-10-11", 1.0)
    body = make_client().get("/api/status").json()
    assert body["live_ais"] is False
    rows = {r["collector"]: r for r in body["sources"]}
    assert set(rows) == {"eia", "portwatch", "eu_oil_bulletin"}  # no AIS row while it is off
    eia = rows["eia"]
    assert eia["data_through"] == "2026-10-14"
    assert eia["last_checked"] == "2026-10-17T11:00:00Z"
    assert eia["status"] == "late"  # the stocks series have no data
    assert eia["message"].startswith("New data was expected by")
    pw = rows["portwatch"]
    assert pw["status"] == "down"
    assert pw["error_class"] == "http_status" and pw["message"] == "503 from portwatch"
    eu = rows["eu_oil_bulletin"]
    assert eu["status"] == "backfilling" and eu["backfill"] == "not_started"
    assert body["backfill"]["state"] == "backfilling"


def test_status_shows_partial_backfill(make_client, db_path):
    """Partial is stored as done plus a cursor marker; status must not say done (#17)."""
    marker = {"partial": {"attempt": 1, "missing": ["eia.stocks.spr"], "points": 9, "need": 9}}
    with Store(db_path) as s:
        s.set_backfill("eia", BackfillState.DONE, json.dumps(marker), 0.9)
        assert s.get_status("eia").backfill.state == BackfillState.DONE  # what the store says
    body = make_client().get("/api/status").json()
    eia = next(r for r in body["sources"] if r["collector"] == "eia")
    assert eia["backfill"] == "partial"
    assert eia["missing_series"] == ["eia.stocks.spr"]
    assert body["backfill"]["collectors"]["eia"] == "partial"
    assert body["backfill"]["missing"] == {"eia": ["eia.stocks.spr"]}


def test_status_includes_ais_when_enabled(make_client):
    ais = FakeAis(AisStatus(connected=True))
    c = make_client(scheduler_factory=lambda store: FakeScheduler(store, ais=ais))
    body = c.get("/api/status").json()
    assert body["live_ais"] is True
    row = next(r for r in body["sources"] if r["collector"] == "aisstream")
    assert row["connected"] is True
    assert row["status"] == "ok"
    assert row["data_through"] == "2026-10-17T12:00:00Z"


def test_status_ais_error_is_down(make_client):
    ais = FakeAis(AisStatus(connected=False, error_class="transport", message="ConnectError"))
    c = make_client(scheduler_factory=lambda store: FakeScheduler(store, ais=ais))
    row = next(r for r in c.get("/api/status").json()["sources"] if r["collector"] == "aisstream")
    assert row["status"] == "down" and row["error_class"] == "transport"


# --- headers and errors -----------------------------------------------------------


@pytest.mark.parametrize("path", ["/healthz", "/api/series", "/api/summary", "/nope", "/api/x"])
def test_headers_on_200_and_404(client, path):
    assert_headers(client.get(path))


def test_500_has_headers_and_no_exception_text(client, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("secret detail from deep inside")

    monkeypatch.setattr(views, "series", boom)
    r = client.get("/api/series")
    assert r.status_code == 500
    assert r.json() == {"detail": "internal error"}
    assert "secret detail" not in r.text
    assert_headers(r)


def test_closed_store_is_503(client):
    client.app.state.store.close()
    for path in ("/api/series", "/api/summary", "/api/status", f"/api/data/{calc.GASOLINE_US}"):
        r = client.get(path)
        assert r.status_code == 503, path
        assert r.json() == {"detail": "the data store is not available"}
        assert_headers(r)
    assert client.get("/healthz").text == "ok"  # liveness does not touch the store


def test_store_closed_error_is_still_the_quiet_stop(tmp_path):
    s = Store(tmp_path / "x.db")
    s.close()
    with pytest.raises(StoreClosedError) as e:
        s.latest(calc.BRENT)
    assert isinstance(e.value, RuntimeError)
    assert str(e.value) == "store is closed"
    assert is_store_closed(e.value)


# --- caching ----------------------------------------------------------------------


def test_series_cached_for_60_seconds(client, clock, monkeypatch):
    calls = counting(monkeypatch, client.app.state.store, "latest")
    first = client.get("/api/series").json()
    n = len(calls)
    assert n > 0
    clock.advance(59)
    assert client.get("/api/series").json() == first
    assert len(calls) == n  # served from the cache
    clock.advance(2)
    client.get("/api/series")
    assert len(calls) > n


def test_data_cached_per_range(client, clock, monkeypatch):
    calls = counting(monkeypatch, client.app.state.store, "points")
    url = f"/api/data/{calc.GASOLINE_US}"
    client.get(url, params={"start": "2026-01-01"})
    client.get(url, params={"start": "2026-01-01"})
    assert len(calls) == 1
    client.get(url, params={"start": "2026-02-01"})
    assert len(calls) == 2
    clock.advance(61)
    client.get(url, params={"start": "2026-01-01"})
    assert len(calls) == 3


def test_summary_cached_until_next_collection(make_client, clock, monkeypatch):
    holder = {}

    def factory(store):
        holder["s"] = FakeScheduler(store)
        return holder["s"]

    c = make_client(scheduler_factory=factory)
    calls = counting(monkeypatch, c.app.state.store, "settings")
    c.get("/api/summary")
    clock.advance(600)  # well past 60 s: still cached
    c.get("/api/summary")
    assert len(calls) == 1
    holder["s"].collections += 1  # a collection wrote points
    c.get("/api/summary")
    assert len(calls) == 2
    clock.advance(3601)  # the hourly backstop
    c.get("/api/summary")
    assert len(calls) == 3


# --- lifespan ---------------------------------------------------------------------


def test_lifespan_shutdown_order(db_path, clock):
    from fastapi.testclient import TestClient

    log: list[str] = []

    class RecordingStore(Store):
        def close(self):
            log.append("store closed")
            super().close()

    app = create_app(
        store_factory=lambda: RecordingStore(db_path),
        scheduler_factory=lambda store: FakeScheduler(store, log=log),
        metrics_port=None,
        clock=clock,
    )
    with TestClient(app) as c:
        assert c.get("/healthz").status_code == 200
        assert log == ["scheduler started"]
    assert log == ["scheduler started", "scheduler saw stop", "scheduler done", "store closed"]


def test_lifespan_runs_the_real_scheduler(db_path, clock, monkeypatch):
    """The real Scheduler, with nothing to collect, starts and stops cleanly."""
    from fastapi.testclient import TestClient

    from fuelwatch.scheduler import Scheduler

    app = create_app(
        store_factory=lambda: Store(db_path),
        scheduler_factory=lambda store: Scheduler(
            store, collectors=[], ais=None, run_backfill=False
        ),
        metrics_port=None,
        clock=clock,
    )
    with TestClient(app) as c:
        assert c.get("/api/version").status_code == 200
        sched = app.state.scheduler
        assert sched.collections == 0
    with pytest.raises(StoreClosedError):
        app.state.store.latest(calc.BRENT)


# --- /metrics ---------------------------------------------------------------------


def test_metrics_on_their_own_port(make_client, db_path):
    with Store(db_path) as s:
        s.record_success("eia", at=NOW)
        s.record_failure("portwatch", ProviderError("http_status", "503 from x"), at=NOW)
    c = make_client(metrics_port=0, metrics_host="127.0.0.1")
    assert c.get("/metrics").status_code == 404  # not on the app port
    port = c.app.state.metrics_server.server_port
    text = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=5).text
    assert 'fuelwatch_collector_up{collector="eia"} 1.0' in text
    assert 'fuelwatch_collector_up{collector="portwatch"} 0.0' in text
    assert 'collector="aisstream"' not in text  # live AIS off
    assert 'fuelwatch_collector_last_success_timestamp_seconds{collector="eia"}' in text
    assert f'fuelwatch_series_lag_seconds{{series="{calc.BRENT}"}}' in text
    assert 'fuelwatch_series_late{series="eia.stocks.crude_commercial"} 0.0' in text
    assert 'fuelwatch_series_has_data{series="eia.stocks.crude_commercial"} 0.0' in text
    assert "fuelwatch_backfill_progress " in text
    assert 'fuelwatch_backfill_state{collector="eia",state="not_started"} 1.0' in text
    assert "fuelwatch_store_open_connections " in text
    assert "fuelwatch_store_up 1.0" in text
    assert "503 from x" not in text  # no messages in labels or anywhere


def test_metrics_after_store_closed(client):
    from prometheus_client import generate_latest

    client.app.state.store.close()
    text = generate_latest(client.app.state.metrics_registry).decode()
    assert "fuelwatch_store_up 0.0" in text


def test_metrics_include_ais_when_enabled(make_client):
    from prometheus_client import generate_latest

    ais = FakeAis(AisStatus(connected=False))
    c = make_client(scheduler_factory=lambda store: FakeScheduler(store, ais=ais))
    text = generate_latest(c.app.state.metrics_registry).decode()
    assert 'fuelwatch_collector_up{collector="aisstream"} 0.0' in text
    assert f'fuelwatch_series_late{{series="{SERIES_SHIPS}"}} 0.0' in text


# --- secret redaction (spec: Testing strategy) ------------------------------------

FAKE_KEY = "PLANTEDFAKESECRETPLANTEDFAKE"


def test_planted_key_appears_in_no_response_and_not_in_metrics(make_client, db_path):
    from prometheus_client import generate_latest

    redact.add(FAKE_KEY)
    with Store(db_path) as s:
        s.record_failure(
            "eia",
            ProviderError("http_status", f"401 from https://api.eia.gov/v2/x?api_key={FAKE_KEY}"),
            at=NOW,
        )
        s.record_failure("portwatch", RuntimeError(f"boom with {FAKE_KEY}"), at=NOW)
    # A message stored before the key was known, so the store could not mask it:
    con = sqlite3.connect(db_path)
    con.execute(
        "UPDATE status SET last_error_message = ? WHERE collector = 'eia'",
        (f"401 from https://api.eia.gov/v2/x?token=1&k={FAKE_KEY}",),
    )
    con.commit()
    con.close()
    ais = FakeAis(
        AisStatus(connected=False, error_class="auth", message=f"rejected key {FAKE_KEY}")
    )
    c = make_client(scheduler_factory=lambda store: FakeScheduler(store, ais=ais), spot_data=True)
    paths = [
        "/",
        "/about",
        "/healthz",
        "/api/summary",
        "/api/series",
        "/api/events",
        "/api/status",
        "/api/version",
        "/api/data/nope",
        "/api/dashboard?range=war",
        "/api/dashboard?range=1y",
        "/api/dashboard?range=5y",
    ]
    paths += [f"/api/data/{e.id}" for e in REGISTRY]
    bodies = {}
    for p in paths:
        r = c.get(p)
        assert r.status_code < 500, p
        bodies[p] = r.text
    for p, text in bodies.items():
        assert FAKE_KEY not in text, p
    status = json.loads(bodies["/api/status"])
    rows = {r["collector"]: r for r in status["sources"]}
    assert rows["eia"]["message"].endswith("***")
    assert rows["aisstream"]["message"] == "rejected key ***"
    assert rows["portwatch"]["message"] == "RuntimeError"  # only the type of a raw exception
    assert FAKE_KEY not in json.dumps(status["backfill"])
    metrics = generate_latest(c.app.state.metrics_registry).decode()
    assert FAKE_KEY not in metrics
