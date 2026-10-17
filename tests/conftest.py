import os

import pytest


def pytest_collection_modifyitems(config, items):
    run_contract = os.environ.get("FW_CONTRACT") == "1"
    run_ui = os.environ.get("FW_UI") == "1"
    skip_contract = pytest.mark.skip(reason="contract test; set FW_CONTRACT=1")
    skip_ui = pytest.mark.skip(reason="UI test; set FW_UI=1 (needs the ui extra and Chromium)")
    for item in items:
        if "contract" in item.keywords and not run_contract:
            item.add_marker(skip_contract)
        if "ui" in item.keywords and not run_ui:
            item.add_marker(skip_ui)
