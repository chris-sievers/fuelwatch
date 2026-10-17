# Fuelwatch

A small, self-hosted dashboard that puts crude prices, US pump prices, fuel stocks and Strait of Hormuz shipping on one page, with every number labeled by source and as-of date.

Status: pre-v1. Milestone 1 validates each collector against the real providers.

## Run your own copy

One container, one Compose file, two optional API keys (EIA, AISStream). Without keys it runs in demo mode with synthetic data.

```sh
docker compose up -d
```

Setup instructions land with milestone 1.

### Demo mode

`FW_DEMO=1` runs the app on made-up data: no collectors, no keys, and a "Demo data" banner on the page. Set `FW_DEMO_NOW` (for example `2026-10-17T12:00:00Z`) to fix the clock, as the UI tests do.

```sh
pip install -e .
FW_DEMO=1 fuelwatch serve   # http://127.0.0.1:8080
```

The dashboard is `/`; `/about` explains the words and the sources, and lists the JSON API.

## Not

No trading signals, forecasts or investment advice. Not a ship tracker: live AIS is an indicator, not an official count.
