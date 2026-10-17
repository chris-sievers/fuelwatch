"""Plain language: no jargon on the dashboard, and an as-of label on every
figure (the spec's Testing strategy, "Plain language" row).

The text checked is everything a reader can get to: every text node in the
page with every "View as table" opened, plus aria-label, title and alt
attributes and the page title. Source credits are exempt, because the
providers' terms ask for their names ("U.S. Energy Information
Administration (EIA)", "IMF PortWatch", "AISStream"); the page marks them
with data-attribution, and this test checks those elements hold only
credits.
"""

from __future__ import annotations

import re

import pytest

from fuelwatch import jargon

pytestmark = pytest.mark.ui

PAGE_TEXT = """
() => {
  const text = [];
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let n = walker.nextNode(); n; n = walker.nextNode()) {
    const t = n.nodeValue.trim();
    if (t && !n.parentElement.closest('[data-attribution]')) text.push(t);
  }
  for (const e of document.querySelectorAll('[aria-label], [title], [alt]')) {
    for (const a of ['aria-label', 'title', 'alt']) if (e.getAttribute(a)) text.push(e.getAttribute(a));
  }
  text.push(document.title);
  const credits = [...document.querySelectorAll('[data-attribution]')].map((e) => e.textContent.trim());
  return { text, credits };
}
"""
CREDIT_PREFIXES = ("Source: ", "Sources: ", "Live counts: ", "Live AIS counts: ")
PROVIDER_NAMES = {
    "U.S. Energy Information Administration",
    "IMF PortWatch",
    "European Commission, Weekly Oil Bulletin",
    "AISStream",
}
WORDS = [w for w in jargon.JARGON if w not in jargon.ALLOWED_LABELS]
ASOF_RE = re.compile(
    r"(data through|as of|the last count was at) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}"
)


def _open_all_tables(page) -> None:
    folds = page.locator("details.as-table")
    for i in range(folds.count()):
        folds.nth(i).locator("summary").click()


@pytest.mark.parametrize("name", ["desktop-light", "phone"])
@pytest.mark.parametrize("range_key", ["1y", "war"])
def test_no_jargon_on_the_dashboard(open_page, name, range_key):
    w = open_page(name)
    page = w.page
    if range_key != "1y":
        page.click(f"[data-range='{range_key}']")
        page.wait_for_selector("body[data-ready='true']")
    _open_all_tables(page)
    got = page.evaluate(PAGE_TEXT)
    hits = {t: jargon.found(t, WORDS) for t in got["text"]}
    assert {t: h for t, h in hits.items() if h} == {}
    assert got["credits"], "no source credits on the page"
    for c in got["credits"]:
        assert c.startswith(CREDIT_PREFIXES) or c in PROVIDER_NAMES, c
    # Brent's tile names Refinitiv, as do the charts drawn from spot prices.
    for fig in ("tile-brent", "crude", "crude-gap", "pump"):
        credit = page.locator(
            f"[data-figure='{fig}'] > [data-attribution], [data-figure='{fig}'] .credit"
        )
        assert "Refinitiv, an LSEG business" in credit.first.inner_text(), fig


def test_every_figure_has_an_as_of_label(open_page):
    page = open_page().page
    figures = page.eval_on_selector_all(
        "[data-figure]",
        "els => els.map(e => [e.dataset.figure, [...e.querySelectorAll('.asof')].map(a => a.textContent)])",
    )
    assert len(figures) > 20
    for key, labels in figures:
        assert labels, key
        assert all(ASOF_RE.search(t) for t in labels), (key, labels)


def test_late_and_may_change_are_shown(open_page):
    page = open_page().page
    eu = page.locator("[data-figure='eu-petrol'] .asof").inner_text()
    assert "Late" in eu and "New data was expected by" in eu
    hz = page.locator("[data-figure='hormuz'] .asof").inner_text()
    assert "may still change" in hz
    rows = page.locator("#sources-body tr")
    statuses = [rows.nth(i).locator("td").last.inner_text() for i in range(rows.count())]
    assert sum(s.startswith("Late") for s in statuses) == 1
    assert sum(s == "OK" for s in statuses) == 3


def test_about_page_holds_the_jargon(open_page, base_url):
    """The glossary explains the words the dashboard avoids."""
    w = open_page(wait=False)
    w.page.goto(base_url + "/about")
    text = w.page.locator("main").inner_text()
    for word in ["PADD", "distillate", "WPSR", "AIS", "SPR", "provisional", "baseline", "spot"]:
        assert jargon.found(text, [word]), word
