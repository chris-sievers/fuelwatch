"""Words that live only on the About page (the spec's plain-language rule).

The tests check that none of these appear in the summary sentence, the tile
labels (tests/unit/test_calc.py) or anywhere in the rendered dashboard
outside /about (tests/ui/test_plain_language.py). Source credits are the
one exception: they quote the providers' names as their terms require, and
the page marks them with ``data-attribution``.
"""

from __future__ import annotations

import re

JARGON: tuple[str, ...] = (
    "PADD",
    "distillate",
    "WPSR",
    "AIS",
    "SPR",
    "WTI",
    "ULSD",
    "RBOB",
    "Cushing",
    "chokepoint",
    "provisional",
    "settled",
    "baseline",
    "spot",
    "bbl",
    "MMSI",
    "Class A",
    "deadweight",
    "DWT",
    "EIA",
    "PortWatch",
    "ISO",
    "percentile",
    "y/y",
    # added with the dashboard
    "Dated Brent",
    "Refinitiv",
    "LSEG",
    "Kpler",
    "transit",
    "utilization",
    "operable",
    "on-highway",
    "Euro-super",
    "gas oil",
    "MBBL",
    "RBRTE",
    "RWTC",
    "$/bbl",
    "$/gal",
    "EUR/l",
    "ships/day",
    "t/day",
    "roll-up",
    "rollup",
    "UTC offset",
    "null",
    "undefined",
    "NaN",
    "[object Object]",
)

# May appear as a label: WTI is the price's everyday name.
ALLOWED_LABELS: frozenset[str] = frozenset({"WTI"})


def found(text: str, words=JARGON) -> list[str]:
    """The jargon words in `text`, matched as whole words, any case."""
    return [
        w
        for w in words
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(w)}(?![A-Za-z0-9])", text, re.IGNORECASE)
    ]
