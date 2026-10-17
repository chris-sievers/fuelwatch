"""Attribution lines for each provider, from docs/providers/terms-review.md
in the infra repo. /api/series and /api/data carry them with every series.

Keyed by ``SeriesSpec.source``. EIA asks for the publication date; the store
does not record when a release was published, so ``release`` is the
scheduled release that carried the newest stored point (see
``views.release_date``).
"""

from __future__ import annotations

from datetime import date

from fuelwatch.collectors import aisstream, eia, eu_oil_bulletin, portwatch

IMF_TERMS_URL = "https://www.imf.org/en/About/copyright-and-terms"
CC_BY_URL = "https://creativecommons.org/licenses/by/4.0/"

_EIA = (
    "Source: U.S. Energy Information Administration (EIA), released {release}. "
    "Spot prices: EIA, from Refinitiv, an LSEG business. "
    "Calculated figures are Fuelwatch's, based on EIA data. "
    "Fuelwatch is not affiliated with or endorsed by EIA."
)
_EIA_NO_DATE = (
    "Source: U.S. Energy Information Administration (EIA). "
    "Spot prices: EIA, from Refinitiv, an LSEG business. "
    "Calculated figures are Fuelwatch's, based on EIA data. "
    "Fuelwatch is not affiliated with or endorsed by EIA."
)
_PORTWATCH = (
    "Sources: Kpler; UN Global Platform; IMF PortWatch (portwatch.imf.org). "
    '7-day averages and "vs normal" are Fuelwatch calculations. '
    f"Used under the IMF terms: {IMF_TERMS_URL}. "
    "Fuelwatch is not affiliated with or endorsed by the IMF."
)
_EU = (
    "Source: European Commission, Weekly Oil Bulletin, under CC BY 4.0 "
    f"({CC_BY_URL}). Values converted by Fuelwatch from € per 1,000 L to € per litre."
)
_AIS = "Live AIS counts: Fuelwatch, from AIS messages received through AISStream (aisstream.io)."

TERMS_URLS: dict[str, str] = {
    portwatch.SOURCE: IMF_TERMS_URL,
    eu_oil_bulletin.SOURCE: CC_BY_URL,
}


def attribution(source: str, release: date | None = None) -> str:
    if source == eia.SOURCE:
        return _EIA.format(release=release.isoformat()) if release else _EIA_NO_DATE
    if source == portwatch.SOURCE:
        return _PORTWATCH
    if source == eu_oil_bulletin.SOURCE:
        return _EU
    if source == aisstream.SERIES[0].source:
        return _AIS
    raise KeyError(source)


def terms_url(source: str) -> str | None:
    return TERMS_URLS.get(source)
