# Fuelwatch

A small, self-hosted dashboard that puts crude prices, US pump prices, fuel stocks and Strait of Hormuz shipping on one page, with every number labeled by source and as-of date.

Status: pre-v1. Milestone 1 validates each collector against the real providers.

## Run your own copy

One container, one Compose file, two optional API keys (EIA, AISStream). Without keys it runs in demo mode with synthetic data.

```sh
docker compose up -d
```

Setup instructions land with milestone 1.

## Not

No trading signals, forecasts or investment advice. Not a ship tracker: live AIS is an indicator, not an official count.
