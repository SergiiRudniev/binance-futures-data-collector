# Binance Futures Data Collector

[![Tests](https://img.shields.io/github/actions/workflow/status/SergiiRudniev/binance-futures-data-collector/ci.yml?label=tests)](https://github.com/SergiiRudniev/binance-futures-data-collector/actions)
[![Python](https://img.shields.io/badge/Python-3.10--3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-2E8B57)](LICENSE)

Restart-safe collection of public Binance USD-M futures data for BTCUSDT,
ETHUSDT and SOLUSDT. The default campaign runs for 30 days and requires no API
key.

## Data

- gap-aware aggregate trades;
- top-20 order-book snapshots and best bid/ask;
- model-ready five-second microstructure rows;
- one-second mark price and liquidation events;
- open interest, funding, basis and premium index;
- contract, mark, index, premium and continuous one-minute klines;
- global and top-trader long/short ratios and taker flow;
- exchange metadata, status heartbeats and connectivity events.

Raw records are stored as gzip JSONL. Five-second aggregates are stored as CSV.
All timestamps are UTC Unix milliseconds.

## Quick Start

```bash
git clone https://github.com/SergiiRudniev/binance-futures-data-collector.git
cd binance-futures-data-collector
cp deploy/market_collector/.env.example deploy/market_collector/.env
docker compose --env-file deploy/market_collector/.env \
  -f deploy/market_collector/compose.yaml up -d --build
```

The first start creates `campaign.json`. Restarts reuse the original start and
end timestamps instead of extending the campaign. Aggregate-trade cursors are
persisted and resume after restarts or network outages.

## Operations

```bash
docker compose -f deploy/market_collector/compose.yaml ps
docker compose -f deploy/market_collector/compose.yaml logs -f --tail 100 collector
cat /srv/binance-futures-data/primary/status/collector.json
cat /srv/binance-futures-data/backup/backup_status.json
du -sh /srv/binance-futures-data/primary /srv/binance-futures-data/backup
```

The collector uses exponential retry backoff and records outage and recovery
events. Docker health checks monitor status freshness. The backup service
creates a checksum-verified append-only mirror and never removes a mirrored
file merely because its source disappeared.

The default backup is on the same host. Set `BACKUP_DATA_DIR` to a separate
mounted device or replicate it off-host to protect against disk or host loss.

## Configuration

Collection settings are in
[`deploy/market_collector/collector.yaml`](deploy/market_collector/collector.yaml).
Storage and backup settings are in
[`deploy/market_collector/.env.example`](deploy/market_collector/.env.example).

The defaults reserve 6 GiB for primary collection and 4 GiB for backup writes.
When the reserve is reached, the service stops writing instead of filling the
host filesystem.

## Test

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r deploy/market_collector/requirements.txt pytest
python -m pytest -q
```

## Layout

```text
deploy/market_collector/   Docker, configuration, backup and health checks
scripts/                   Collector implementations
tests/                     Restart, storage and integrity tests
```
