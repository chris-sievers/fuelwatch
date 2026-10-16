"""Collectors, one module per provider.

A polled collector exposes:

    SERIES: list[SeriesSpec]
    async def fetch(client: httpx.AsyncClient, since: date | None = None) -> list[Point]

`since=None` means the full five-year backfill. Collectors return points and
never write zeros to fill a gap. Parsing lives in pure functions that take
the provider's decoded response, so unit tests run against recorded
fixtures in tests/fixtures/<provider>/.
"""
