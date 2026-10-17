"""UI tests: headless Chromium (Playwright) against the real app in demo
mode, with the app's real Content-Security-Policy header enforced.

Run with FW_UI=1 after ``pip install -e '.[dev,ui]'`` and
``python -m playwright install chromium``. Screenshots go to
tests/ui/artifacts/ (CI uploads them).

Every page a test opens is watched: any console error, page error, CSP
violation (securitypolicyviolation events and console messages), failed
request, HTTP error or request to another origin fails the test, and so
does any element that gains a style attribute (a MutationObserver installed
before the page's own scripts run).
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pytest

ARTIFACTS = Path(__file__).parent / "artifacts"
DEMO_NOW = "2026-10-17T12:00:00Z"

VIEWPORTS = {
    "desktop-light": {"viewport": {"width": 1280, "height": 900}, "color_scheme": "light"},
    "desktop-dark": {"viewport": {"width": 1280, "height": 900}, "color_scheme": "dark"},
    "phone": {
        "viewport": {"width": 390, "height": 844},
        "device_scale_factor": 2,
        "is_mobile": True,
        "has_touch": True,
        "color_scheme": "light",
    },
}

WATCH = """
(() => {
  const fw = { violations: [], styleChanges: [] };
  window.__fw = fw;
  document.addEventListener('securitypolicyviolation', (e) => {
    fw.violations.push(e.violatedDirective + ' ' + (e.blockedURI || 'inline'));
  });
  new MutationObserver((records) => {
    for (const r of records) fw.styleChanges.push(r.target.nodeName);
  }).observe(document, { attributes: true, subtree: true, attributeFilter: ['style'] });
})();
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def base_url():
    port = _free_port()
    env = dict(
        os.environ,
        FW_DEMO="1",
        FW_DEMO_NOW=DEMO_NOW,
        FW_HOST="127.0.0.1",
        FW_PORT=str(port),
        FW_METRICS_PORT=str(_free_port()),
    )
    env.pop("EIA_API_KEY", None)
    env.pop("AISSTREAM_API_KEY", None)
    proc = subprocess.Popen(
        [sys.executable, "-m", "fuelwatch", "serve"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 30
    while True:
        try:
            if httpx.get(url + "/healthz", timeout=1).text == "ok":
                break
        except httpx.HTTPError:
            pass
        if proc.poll() is not None or time.monotonic() > deadline:
            proc.kill()
            out = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
            pytest.fail(f"the demo app did not start:\n{out[-3000:]}")
        time.sleep(0.2)
    yield url
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def browser():
    sync_api = pytest.importorskip("playwright.sync_api")
    with sync_api.sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


class Watched:
    """A page with its problems recorded."""

    def __init__(self, page, base_url: str) -> None:
        self.page = page
        self.problems: list[str] = []
        origin = urlsplit(base_url)[:2]
        page.on(
            "console",
            lambda m: (
                self.problems.append(f"console {m.type}: {m.text}")
                if m.type == "error" or "Content Security Policy" in m.text
                else None
            ),
        )
        page.on("pageerror", lambda e: self.problems.append(f"page error: {e}"))
        page.on("requestfailed", lambda r: self.problems.append(f"request failed: {r.url}"))
        page.on(
            "request",
            lambda r: (
                self.problems.append(f"request to another origin: {r.url}")
                if urlsplit(r.url)[:2] != origin and not r.url.startswith(("data:", "blob:"))
                else None
            ),
        )
        page.on(
            "response",
            lambda r: (
                self.problems.append(f"HTTP {r.status}: {r.url}") if r.status >= 400 else None
            ),
        )

    def open(self, url: str) -> None:
        self.page.goto(url)
        self.page.wait_for_selector("body[data-ready]", timeout=20000)
        assert self.page.get_attribute("body", "data-ready") == "true", self.problems

    def check(self) -> None:
        fw = self.page.evaluate(
            "window.__fw || {violations: ['watcher missing'], styleChanges: []}"
        )
        inline = self.page.evaluate("document.querySelectorAll('[style]').length")
        assert self.problems == []
        assert fw["violations"] == []
        assert fw["styleChanges"] == [], "something set an inline style"
        assert inline == 0


@pytest.fixture
def open_page(browser, base_url):
    """open_page(viewport_name, path="/") -> Watched; checked again at teardown."""
    made = []

    def make(name: str = "desktop-light", path: str = "/", wait: bool = True) -> Watched:
        ctx = browser.new_context(**VIEWPORTS[name])
        ctx.add_init_script(WATCH)
        w = Watched(ctx.new_page(), base_url)
        made.append((ctx, w))
        if wait:
            w.open(base_url + path)
        return w

    yield make
    for ctx, w in made:
        try:
            w.check()
        finally:
            ctx.close()
