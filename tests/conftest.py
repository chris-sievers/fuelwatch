import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("FW_CONTRACT") == "1":
        return
    skip = pytest.mark.skip(reason="contract test; set FW_CONTRACT=1")
    for item in items:
        if "contract" in item.keywords:
            item.add_marker(skip)
