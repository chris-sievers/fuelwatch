"""The page's static files: vendored files match their recorded checksums,
and nothing in them would trip the CSP."""

from __future__ import annotations

import hashlib
import json
import re

from fuelwatch.app import STATIC_DIR
from fuelwatch.jargon import found

VENDOR = STATIC_DIR / "vendor"


def test_vendored_files_match_manifest():
    manifest = json.loads((VENDOR / "manifest.json").read_text())
    listed = set()
    for pkg in manifest["packages"]:
        assert re.fullmatch(r"\d+\.\d+\.\d+", pkg["version"])  # exact version
        for rel, info in pkg["files"].items():
            data = (VENDOR / rel).read_bytes()
            assert hashlib.sha256(data).hexdigest() == info["sha256"], rel
            listed.add(rel)
    on_disk = {
        str(p.relative_to(VENDOR))
        for p in VENDOR.rglob("*")
        if p.is_file() and p.name != "manifest.json"
    }
    assert on_disk == listed  # nothing vendored without a checksum
    assert any(r.endswith("LICENSE.md") for r in listed)


def test_page_references_the_vendored_version():
    html = (STATIC_DIR / "index.html").read_text()
    assert '<script src="/static/vendor/chart.js-4.5.1/chart.umd.min.js" defer></script>' in html


def test_app_js_never_sets_style_or_html():
    js = (STATIC_DIR / "app.js").read_text()
    code = re.sub(r"/\*.*?\*/|//[^\n]*", "", js, flags=re.DOTALL)  # comments may mention them
    for bad in [".style", "innerHTML", "outerHTML", "insertAdjacentHTML", "eval(", "new Function",
                "document.write", "cssText", "setAttribute('style'"]:  # fmt: skip
        assert bad not in code, bad


def test_jargon_matching():
    assert found("Stocks of distillate", ["distillate"]) == ["distillate"]
    assert found("$98 a barrel", ["$/bbl", "bbl"]) == []
    assert found("98 $/bbl", ["$/bbl"]) == ["$/bbl"]
    assert found("aisle", ["AIS"]) == []
