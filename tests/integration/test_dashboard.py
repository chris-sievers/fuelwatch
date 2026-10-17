"""/api/dashboard: the page's one bounded feed, and demo mode end to end."""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from fuelwatch import calc, jargon
from fuelwatch.api import attribution, dashboard, views
from fuelwatch.app import STATIC_DIR, create_app
from fuelwatch.registry import REGISTRY

from .conftest import NOW
from .test_api import assert_headers


def test_dashboard_shape(client):
    r = client.get("/api/dashboard")
    assert r.status_code == 200
    assert_headers(r)
    body = r.json()
    assert body["range"] == {"key": "1y", "start": "2025-10-17", "end": "2026-10-17"}
    assert body["demo"] is False
    assert [t["key"] for t in body["summary"]["tiles"]] == [
        "brent", "gasoline", "diesel", "tank", "hormuz", "crude_stocks",
    ]  # fmt: skip
    assert set(body["series"]) == {e.id for e in REGISTRY}
    brent = body["crude"]["brent"]
    assert brent[0][0] >= "2025-10-17" and brent[-1] == ["2026-10-14", brent[-1][1]]
    assert body["hormuz"]["all"][-1][2] is True  # provisional flag carried
    assert body["hormuz"]["normal"]["end"] == "2026-02-27"
    assert len(body["chokepoints"]) == 4
    assert len(body["stocks"]) == 4
    assert {s["collector"] for s in body["status"]["sources"]} == {
        "eia", "portwatch", "eu_oil_bulletin",
    }  # fmt: skip


@pytest.mark.parametrize(
    "key,start",
    [("war", "2026-02-27"), ("1y", "2025-10-17"), ("5y", "2021-10-17")],
)
def test_ranges_are_bounded(client, key, start):
    body = client.get("/api/dashboard", params={"range": key}).json()
    assert body["range"]["start"] == start
    lo = date.fromisoformat(body["range"]["start"])
    hi = date.fromisoformat(body["range"]["end"])
    assert (hi - lo).days + 1 <= views.MAX_SPAN_DAYS
    for p in body["crude"]["brent"] + body["hormuz"]["all"]:
        assert start <= p[0] <= body["range"]["end"]


@pytest.mark.parametrize("bad", ["2y", "", "WAR", "x" * 9])
def test_bad_range_is_400(client, bad):
    r = client.get("/api/dashboard", params={"range": bad})
    assert r.status_code in (400,)
    assert_headers(r)


def test_dashboard_cached_per_range(client, clock):
    a = client.get("/api/dashboard").json()
    clock.advance(30)
    assert client.get("/api/dashboard").json()["generated_at"] == a["generated_at"]
    clock.advance(31)
    assert client.get("/api/dashboard").json()["generated_at"] != a["generated_at"]


def test_dashboard_is_gzipped(client):
    r = client.get("/api/dashboard", headers={"accept-encoding": "gzip"})
    assert r.headers["content-encoding"] == "gzip"
    assert_headers(r)


def test_labels_are_plain_language():
    for e in REGISTRY:
        assert jargon.found(dashboard.label(e.id), list(jargon.JARGON)) in ([], ["WTI"]), e.id


def test_credits_name_refinitiv_for_spot_series(client):
    meta = client.get("/api/dashboard").json()["series"]
    for sid in views.SPOT_SERIES:
        assert "Refinitiv, an LSEG business" in meta[sid]["credit"]
        assert "Refinitiv, an LSEG business" in meta[sid]["attribution"]
    assert "Refinitiv" not in meta[calc.GASOLINE_US]["credit"]


def test_about_page_carries_every_attribution_line():
    html = (STATIC_DIR / "about.html").read_text()
    for line in attribution.all_lines():
        assert line.replace("&", "&amp;") in html or line in html, line


def test_about_page_does_not_offer_spot_series_for_download():
    html = (STATIC_DIR / "about.html").read_text()
    api = html[html.index('id="api"') :]
    for sid in views.SPOT_SERIES:
        assert sid not in api
    assert "eia.spot" not in api and "Brent" not in api and "WTI" not in api


# --- demo mode --------------------------------------------------------------------------


@pytest.fixture
def demo_client(monkeypatch):
    monkeypatch.setenv("FW_DEMO", "1")
    monkeypatch.setenv("FW_DEMO_NOW", "2026-10-17T12:00:00Z")
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    monkeypatch.delenv("AISSTREAM_API_KEY", raising=False)
    app = create_app(metrics_port=None)
    with TestClient(app) as c:
        yield c
        path = app.state.store.path
    assert not path.exists()  # the throwaway store is removed on shutdown


def test_demo_mode_serves_every_series_without_keys(demo_client):
    body = demo_client.get("/api/dashboard").json()
    assert body["demo"] is True
    assert body["generated_at"] == NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    series = demo_client.get("/api/series").json()["series"]
    assert all(s["latest"] is not None for s in series)
    late = {s["id"] for s in series if s["status"] == "late"}
    assert late == {e.id for e in REGISTRY if e.collector == "eu_oil_bulletin"}
    assert body["ais"]["live"] is True
    status = {s["collector"]: s["status"] for s in body["status"]["sources"]}
    assert status == {"eia": "ok", "portwatch": "ok", "eu_oil_bulletin": "late", "aisstream": "ok"}
    assert body["events"] and all(e["label"].startswith("Demo marker") for e in body["events"])
    assert demo_client.get("/api/events").json()["events"] == body["events"]
