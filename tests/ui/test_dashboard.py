"""The dashboard in a real browser, demo mode, CSP enforced.

Screenshots: desktop light, desktop dark and phone width, saved to
tests/ui/artifacts/. No golden-image comparison: text rendering differs
between machines and Chromium builds, so a pixel diff would fail on
harmless changes. The checks below assert what matters instead.
"""

from __future__ import annotations

import json

import httpx
import pytest

from fuelwatch.app import CSP

from .conftest import ARTIFACTS, VIEWPORTS

pytestmark = pytest.mark.ui

TILES = ["brent", "gasoline", "diesel", "tank", "hormuz", "crude_stocks"]
FIGURES = [f"tile-{t}" for t in TILES] + [
    "crude",
    "crude-gap",
    "pump",
    "regions",
    "crude-share",
    "eu-petrol",
    "eu-diesel",
    "hormuz",
    "cp-bab_el_mandeb",
    "cp-suez",
    "cp-cape_of_good_hope",
    "cp-malacca",
    "ais-now",
    "stocks-crude_commercial",
    "stocks-gasoline",
    "stocks-distillate",
    "stocks-spr",
    "refinery",
]
CHARTS = 21  # 6 sparklines + 15 panel charts

# For each canvas: drawn by Chart.js, sized, and not blank.
CANVAS_STATE = """
() => [...document.querySelectorAll('canvas')].map((c) => {
  const g = c.getContext('2d');
  const d = c.width && c.height ? g.getImageData(0, 0, c.width, c.height).data : [];
  let ink = 0;
  for (let i = 3; i < d.length; i += 4) if (d[i] > 0) ink++;
  const fig = c.closest('[data-figure]');
  return {
    figure: fig ? fig.getAttribute('data-figure') : null,
    rendered: c.getAttribute('data-rendered') === 'true',
    width: c.width, height: c.height, ink,
  };
})
"""


def test_page_carries_the_csp(base_url):
    for path in ["/", "/about", "/static/app.js", "/static/vendor/chart.js-4.5.1/chart.umd.min.js"]:
        r = httpx.get(base_url + path)
        assert r.status_code == 200, path
        assert r.headers["content-security-policy"] == CSP, path


def test_csp_is_enforced_and_violations_are_caught(browser, base_url):
    """Positive control: the browser blocks inline script and style on this
    page, and the watcher sees it. So a clean run means something."""
    from .conftest import WATCH

    ctx = browser.new_context()
    ctx.add_init_script(WATCH)
    page = ctx.new_page()
    messages = []
    page.on("console", lambda m: messages.append(m.text))
    page.goto(base_url + "/about")
    page.evaluate(
        """() => {
          const s = document.createElement('script');
          s.textContent = 'window.__ran = true';
          document.head.append(s);
          const st = document.createElement('style');
          st.textContent = 'body { background: red }';
          document.head.append(st);
        }"""
    )
    page.wait_for_timeout(200)
    fw = page.evaluate("window.__fw")
    assert page.evaluate("window.__ran === true") is False
    assert any(v.startswith("script-src") for v in fw["violations"]), fw
    assert any(v.startswith("style-src") for v in fw["violations"]), fw
    assert any("Content Security Policy" in m for m in messages)
    ctx.close()


@pytest.mark.parametrize("name", list(VIEWPORTS))
def test_renders_cleanly_with_screenshot(open_page, name):
    w = open_page(name)
    page = w.page
    ARTIFACTS.mkdir(exist_ok=True)
    page.screenshot(path=str(ARTIFACTS / f"dashboard-{name}.png"), full_page=True)

    figures = page.eval_on_selector_all("[data-figure]", "els => els.map(e => e.dataset.figure)")
    assert figures == FIGURES
    canvases = page.evaluate(CANVAS_STATE)
    assert len(canvases) == CHARTS
    for c in canvases:
        assert c["rendered"] and c["width"] > 0 and c["height"] > 0, c
        assert c["ink"] > 50, c  # something was drawn
    # every tile has a number, two changes and an as-of date
    for t in TILES:
        tile = page.locator(f"[data-figure=tile-{t}]")
        assert tile.locator(".number").inner_text() not in ("", "not available")
        assert tile.locator(".change").count() >= 2
    assert page.locator("#summary").inner_text().endswith(".")
    assert page.locator("#demo-banner").is_visible()
    assert "Demo data" in page.locator("#demo-banner").inner_text()
    assert page.locator("#sources-body tr").count() == 4
    w.check()


def test_dark_theme_follows_the_system(open_page):
    dark = open_page("desktop-dark").page
    light = open_page("desktop-light").page
    bg = "getComputedStyle(document.body).backgroundColor"
    assert dark.evaluate(bg) == "rgb(13, 13, 13)"
    assert light.evaluate(bg) == "rgb(249, 249, 247)"


def test_phone_layout(open_page):
    page = open_page("phone").page
    width = VIEWPORTS["phone"]["viewport"]["width"]
    assert page.evaluate("document.documentElement.scrollWidth") <= width  # no sideways scroll
    boxes = page.eval_on_selector_all(
        ".tile",
        "els => els.map(e => { const r = e.getBoundingClientRect(); return [r.x, r.y, r.width]; })",
    )
    assert boxes[0][1] == boxes[1][1] and boxes[2][1] > boxes[0][1]  # two across
    assert all(b[2] < width / 2 for b in boxes)
    main = page.evaluate("document.querySelector('#crude-body').getBoundingClientRect().width")
    crude = page.evaluate("document.querySelector('#fig-crude').getBoundingClientRect().width")
    assert crude == pytest.approx(main, abs=1)  # charts full width
    small = page.evaluate("document.querySelector('#fig-cp-suez').getBoundingClientRect().width")
    assert small > main - 30
    # tables are cards: one block per row, labelled cells
    assert (
        page.evaluate("getComputedStyle(document.querySelector('#fig-regions tbody tr')).display")
        == "block"
    )
    assert (
        page.evaluate("getComputedStyle(document.querySelector('#fig-regions thead')).position")
        == "absolute"
    )


def test_tap_shows_a_value(open_page):
    page = open_page("phone").page
    canvas = page.locator("#fig-crude canvas")
    canvas.scroll_into_view_if_needed()
    before = page.evaluate(CANVAS_STATE)
    box = canvas.bounding_box()
    page.touchscreen.tap(box["x"] + box["width"] * 0.6, box["y"] + box["height"] * 0.5)
    page.wait_for_timeout(100)
    after = page.evaluate(CANVAS_STATE)
    i = [c["figure"] for c in before].index("crude")
    assert after[i]["ink"] > before[i]["ink"]  # the tooltip was drawn


def test_every_view_as_table_works(open_page):
    page = open_page().page
    folds = page.locator("details.as-table")
    n = folds.count()
    assert n == CHARTS
    for i in range(n):
        d = folds.nth(i)
        d.locator("summary").click()
        table = d.locator("table")
        table.wait_for(state="visible")
        assert table.locator("tbody tr").count() >= 1
        assert table.locator("thead th").count() >= 2


def test_range_buttons_reload_the_charts(open_page):
    w = open_page()
    page = w.page
    first = page.locator("#range-note").inner_text()
    for key, label in [("5y", "5 years"), ("war", "Since the war began"), ("1y", "1 year")]:
        page.click(f"[data-range='{key}']")
        page.wait_for_selector("body[data-ready='true']")
        assert label in page.locator("#range-note").inner_text()
        assert page.get_attribute(f"[data-range='{key}']", "aria-pressed") == "true"
        assert len(page.evaluate(CANVAS_STATE)) == CHARTS
    assert page.locator("#range-note").inner_text() == first


def test_untrusted_text_is_rendered_as_text(open_page, base_url):
    """Event labels and provider text are data: markup in them stays text,
    and a non-https link is dropped."""
    w = open_page(wait=False)
    page = w.page
    evil = '<img src="/static/x.png" onerror="window.__pwned=1">Tanker <b>seized</b>'

    def tamper(route):
        body = json.loads(route.fetch().text())
        body["events"] = [
            {"date": "2026-03-10", "label": evil, "url": "javascript:window.__pwned=1"},
            {
                "date": "2026-04-10",
                "label": "<script>window.__pwned=1</script>",
                "url": "https://example.org/x",
            },
        ]
        body["status"]["sources"][0]["source"] = '<img src="/static/y.png">EIA'
        route.fulfill(json=body)

    page.route("**/api/dashboard*", tamper)
    w.open(base_url + "/")
    items = page.locator("#fig-crude .events li")
    assert items.count() == 2
    assert evil in items.nth(0).inner_text()
    assert items.nth(0).locator("a").count() == 0  # javascript: link dropped
    assert items.nth(1).locator("a").get_attribute("href") == "https://example.org/x"
    assert page.locator("main img, footer img, main script, main b").count() == 0
    assert '<img src="/static/y.png">EIA' in page.locator("#sources-body").inner_text()
    assert page.evaluate("window.__pwned") is None
    w.check()


def test_about_page_is_clean(open_page, base_url):
    w = open_page(wait=False)
    w.page.goto(base_url + "/about")
    assert w.page.locator("h1").inner_text() == "About Fuelwatch"
    ARTIFACTS.mkdir(exist_ok=True)
    w.page.screenshot(path=str(ARTIFACTS / "about-desktop-light.png"), full_page=True)
    w.check()
