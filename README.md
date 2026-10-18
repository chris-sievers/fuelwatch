# Fuelwatch

A small, self-hosted dashboard that puts crude prices, US and EU pump prices, US fuel stocks and Strait of Hormuz shipping on one page. A non-expert can see what the Iran war is doing to fuel prices without visiting a dozen sites.

The data is free but scattered, published on different schedules and labeled in jargon. Fuelwatch collects it, keeps five years of history and shows each number with its source and how old it is.

Status: pre-v1. The app runs from source today. The Docker image and Compose file arrive in milestone 4.

## What it is not

- No trading signals, forecasts or investment advice.
- No paid or real-time market feeds.
- No user accounts; the dashboard is read-only.
- Not a ship tracker. The live AIS count is an indicator, not an official count.

## Quick start: demo mode

Demo mode runs the app on made-up data: no keys, no network calls to providers, and a "Demo data" banner on the page. You need Python 3.11 or newer.

```sh
git clone https://github.com/chris-sievers/fuelwatch.git
cd fuelwatch
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
FW_DEMO=1 python -m fuelwatch serve
```

Open http://127.0.0.1:8080. The About page at `/about` explains the words and the sources. Set `FW_DEMO_NOW=2026-10-17T12:00:00Z` to fix the demo's clock, as the UI tests do.

## Running with real data

You need one free key and can add a second:

| Key | Get it | Without it |
| --- | --- | --- |
| `EIA_API_KEY` | Free at [eia.gov/opendata](https://www.eia.gov/opendata/) | The EIA collector reports "EIA_API_KEY is not set" in its status; crude, US pump and stored-oil figures stay empty |
| `AISSTREAM_API_KEY` | Free at [aisstream.io](https://aisstream.io) | Live AIS is off. Everything else works; Hormuz traffic still comes from IMF PortWatch |

IMF PortWatch and the EU Weekly Oil Bulletin need no key.

```sh
export EIA_API_KEY=...            # required for real data
export AISSTREAM_API_KEY=...      # optional; live AIS is off without it
python -m fuelwatch serve
```

The database is `fuelwatch.db` in `FW_DATA_DIR` (default `./data`). On first start the server answers at once and backfills five years of EIA, PortWatch and EU history in the background; the status footer shows "backfilling" until it is done. Live AIS has history only from when it was switched on.

Keep keys out of the shell history and out of git: `.env` and `*.env` files are ignored by the repo.

## Settings

All settings are environment variables.

| Variable | Default | What it does |
| --- | --- | --- |
| `FW_DATA_DIR` | `./data` | Directory holding the SQLite database `fuelwatch.db` |
| `FW_HOST` | `127.0.0.1` | Address `serve` binds to |
| `FW_PORT` | `8080` | Port for the dashboard and API |
| `FW_METRICS_HOST` | `127.0.0.1` | Address of the Prometheus `/metrics` server |
| `FW_METRICS_PORT` | `9201` | Port of the `/metrics` server, separate from the app |
| `FW_DEMO` | `0` | `1` runs demo mode: synthetic data, no collectors |
| `FW_DEMO_NOW` | unset | Fixes demo mode's clock; an ISO date and time such as `2026-10-17T12:00:00Z` (UTC if no zone is given) |
| `FW_API_SPOT_DATA` | `1` | `0` stops `/api/data` serving the four EIA spot series (Refinitiv data). The dashboard charts them either way |
| `FW_COMMIT` | `unknown` | Commit shown by `/api/version` |
| `FW_BUILD_TIME` | `unknown` | Build time shown by `/api/version` |
| `EIA_API_KEY` | unset | EIA API v2 key |
| `AISSTREAM_API_KEY` | unset | AISStream key; live AIS is off when unset |

`FW_DEMO` and `FW_API_SPOT_DATA` accept only `0` or `1`; anything else stops the app at start. Test-only variables are listed under Development.

## Commands

The `fuelwatch` command and `python -m fuelwatch` are the same. Add `-v` before the command for debug logging; keys are redacted at every level.

| Command | What it does |
| --- | --- |
| `fuelwatch serve` | Runs the app: dashboard, API, collectors, live AIS and `/metrics`, on exactly one worker |
| `fuelwatch backfill` | Runs the five-year backfill now for collectors not yet done, then prints what is stored. `--only eia`, `--only portwatch` or `--only eu_oil_bulletin` limits it (repeatable); `--reset` redoes collectors already done; `--attempts N` sets tries per collector (default 3) |
| `fuelwatch backfill --only NAME --accept-missing` | Marks a partial collector done, keeping its missing series on record. Use it when a provider has retired a series; it needs `--only` |
| `fuelwatch collect --once` | Polls each collector once and exits. Takes `--only` too |
| `fuelwatch backup [DEST]` | Writes a consistent copy of the database with SQLite's online backup API, by default to `<data dir>/fuelwatch-backup.db` |

`serve` runs the backfill and the collection schedule itself; `backfill` and `collect --once` are for manual one-offs.

## Routes

All routes are `GET` and read-only.

| Path | What it returns |
| --- | --- |
| `/` | The dashboard |
| `/about` | Plain-language glossary, source notes and the JSON API |
| `/api/dashboard?range=war\|1y\|5y` | Everything the page draws, for one time range (default `1y`). The page's own feed, not a dataset |
| `/api/summary` | Summary sentence, headline tiles and calculated numbers |
| `/api/series` | Every series with its latest and previous value, freshness, source and attribution |
| `/api/data/{series}?start=YYYY-MM-DD&end=YYYY-MM-DD` | History for one series, at most five years per request; live AIS rolls up to days for ranges over a week |
| `/api/events` | Event markers |
| `/api/status` | One row per source, with sanitized messages, and backfill progress |
| `/api/version` | `FW_COMMIT` and `FW_BUILD_TIME` |
| `/healthz` | `ok` |
| `/metrics` | Prometheus metrics, on `FW_METRICS_HOST:FW_METRICS_PORT` only |

Series available from `/api/data` include US pump prices by region (`eia.retail.gasoline.us`, `eia.retail.diesel.ca` and so on), US stored oil and refinery use (`eia.stocks.*`, `eia.refinery.utilization`), ships crossing each sea route (`portwatch.*`), EU pump prices (`eu_pump_*`) and live AIS counts (`hormuz_ais_*`). `/api/series` lists every ID.

Every response carries a strict Content-Security-Policy, `X-Content-Type-Options: nosniff` and `Referrer-Policy: no-referrer`. The page makes no third-party requests; Chart.js is vendored.

## Data sources

| Source | What | Key |
| --- | --- | --- |
| U.S. Energy Information Administration, API v2 | Brent and WTI spot, New York Harbor gasoline and diesel spot, weekly US pump prices, weekly stocks and refinery utilization | `EIA_API_KEY` |
| IMF PortWatch | Daily ships crossing Hormuz, Bab el-Mandeb, Suez, Cape of Good Hope and Malacca | None |
| European Commission Weekly Oil Bulletin | Weekly petrol and diesel prices with taxes: EU average, Germany, France, Italy, Spain | None |
| AISStream | Live ships broadcasting their position in the strait, and crossings of a line from Ras Musandam to Larak Island | `AISSTREAM_API_KEY` |

The app shows these attribution lines on its charts, on `/about` and in the `source` field of `/api/series` and `/api/data`. Keep them if you run a copy:

- EIA: "Source: U.S. Energy Information Administration (EIA), released {release date}. Spot prices: EIA, from Refinitiv, an LSEG business. Calculated figures are Fuelwatch's, based on EIA data. Fuelwatch is not affiliated with or endorsed by EIA."
- IMF PortWatch: "Sources: Kpler; UN Global Platform; IMF PortWatch (portwatch.imf.org). 7-day averages and "vs normal" are Fuelwatch calculations. Used under the IMF terms: https://www.imf.org/en/About/copyright-and-terms. Fuelwatch is not affiliated with or endorsed by the IMF."
- EU Weekly Oil Bulletin: "Source: European Commission, Weekly Oil Bulletin, under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Values converted by Fuelwatch from € per 1,000 L to € per litre."
- AISStream: "Live AIS counts: Fuelwatch, from AIS messages received through AISStream (aisstream.io)."

The four EIA spot series come from Refinitiv. Fuelwatch charts them, but they are not offered as a downloadable dataset. Each operator registers their own keys and is responsible for following each provider's terms.

## Development

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
ruff check . && ruff format --check .
pytest
```

`pytest` runs the unit and integration tests offline. Contract and UI tests skip themselves unless switched on.

**Contract tests** call the real providers:

```sh
FW_CONTRACT=1 EIA_API_KEY=... AISSTREAM_API_KEY=... pytest tests/contract
```

PortWatch and the EU bulletin need no key; the EIA and AISStream tests need theirs. `FW_RECORD_FIXTURES=1` makes the AISStream test save the first frame of each message type as a fixture. CI runs these nightly in the `contract` workflow.

**UI tests** drive headless Chromium with Playwright against the app in demo mode, with the real Content-Security-Policy enforced:

```sh
pip install -e '.[dev,ui]'
python -m playwright install chromium
FW_UI=1 pytest tests/ui
```

Screenshots go to `tests/ui/artifacts/`. On pull requests, CI runs the `checks` job (lint, tests, `pip-audit`, secret scan, workflow lint) and the `ui` job; both must pass.

## Licence

Licence: not yet chosen.
