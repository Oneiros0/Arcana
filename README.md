# Arcana

**Quantitative trading data ingestion pipeline.** Streams raw trades from
crypto exchanges into TimescaleDB, with backfill, gap-filling, and
provenance-tagged historical candles.

Arcana is the ingestion half of a two-part stack. Bar construction, feature
engineering, and modeling are handled downstream by **[Sigil](https://position5.org/products/sigil)**,
Position5's proprietary post-processing software.

```
arcana db init  →  arcana ingest ETH-USD --since 2025-01-01  →  arcana summon ETH-USD
                                                                       │
                                                                       v
                                                                   raw_trades
                                                                       │
                                                                       v
                                                          ┌──────────────────┐
                                                          │  Sigil (Position5) │
                                                          │  bars · features  │
                                                          │  models · backtest │
                                                          └──────────────────┘
```

[![CI](https://github.com/Oneiros0/Arcana/actions/workflows/ci.yml/badge.svg)](https://github.com/Oneiros0/Arcana/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

---

## Why a dedicated ingestion layer?

Earlier versions of Arcana shipped Prado-style bar builders, EWMA threshold
estimators, and a full information-driven bar zoo. In v0.4.0 we split the
project in two:

- **Arcana** — get the data right. Tick-level fidelity, idempotent writes,
  resumable backfill, and explicit provenance for every row.
- **[Sigil](https://position5.org/products/sigil)** — turn the data into something
  tradeable. Bars (standard + information-driven), labels, features, and the
  rest of the Prado pipeline live here.

The split lets each layer ship on its own schedule and keeps the ingestion
contract narrow: a `raw_trades` hypertable that is easy to read, hard to
corrupt, and honest about where every row came from.

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │                 Arcana CLI                  │
                          │  db init │ ingest │ backfill-candles │      │
                          │            summon │ status                   │
                          └──────┬──────────┬───────────┬───────────────┘
                                 │          │           │
                                 v          v           v
                          ┌────────────┐ ┌──────────┐ ┌──────────┐
                          │  Database   │ │ Backfill │ │  Daemon  │
                          │   Schema    │ │ Pipeline │ │   Mode   │
                          │   Init      │ │ (trades  │ │  (poll + │
                          │             │ │ +candles)│ │ catch-up)│
                          └─────────────┘ └────┬─────┘ └────┬─────┘
                                               │            │
                                               v            v
                                  ┌────────────────────────────────────┐
                                  │       DataSource (ABC)             │
                                  │   ┌─────────────────────────┐      │
                                  │   │   CoinbaseSource         │      │
                                  │   │   (Advanced Trade)       │      │
                                  │   │   trades + candles REST  │      │
                                  │   └─────────────────────────┘      │
                                  └──────────────────┬─────────────────┘
                                                     │
                                                     v
                                  ┌────────────────────────────────────┐
                                  │      TimescaleDB / PostgreSQL      │
                                  │                                    │
                                  │   raw_trades (hypertable)          │
                                  │     timestamp · trade_id · source  │
                                  │     pair · price · size · side     │
                                  │     data_quality                    │
                                  │                                    │
                                  │     ('tick' | 'candle_1m' | …)     │
                                  └────────────────────────────────────┘
                                                     │
                                                     v
                                  ┌────────────────────────────────────┐
                                  │  Sigil (downstream, separate repo) │
                                  │     bars · features · models       │
                                  └────────────────────────────────────┘
```

---

## Provenance: `data_quality`

Every row in `raw_trades` carries a `data_quality` tag:

| Tag | Meaning |
|-----|---------|
| `tick` | Real tick from the exchange's trade feed. Authoritative. |
| `candle_1m` | One synthetic trade per 1-minute OHLCV bucket, priced at HLC/3, sized at the candle's volume. |
| `candle_5m`, `candle_15m`, … | Same, for coarser granularities. |

Coinbase exposes tick-level history for a bounded window only; older periods
are available exclusively as OHLCV candles. Rather than fan a candle out into
four fictional O/H/L/C trades, Arcana synthesizes **one** trade per candle
and tags it. Bars built across the tick/candle boundary are *not* comparable —
downstream consumers (Sigil, your own code) must filter or split on
`data_quality` before training.

---

## Quickstart

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 14+** with the [TimescaleDB](https://docs.timescale.com/install/) extension
- A Coinbase account is **not** required (public API endpoints)

### Install

```bash
git clone https://github.com/Oneiros0/Arcana.git
cd Arcana
pip install -e ".[dev]"

arcana --help
```

### Database setup

```bash
createdb arcana
psql -d arcana -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

arcana db init
```

Connection settings prefer environment variables:

```bash
export ARCANA_DB_HOST=localhost
export ARCANA_DB_PORT=5432
export ARCANA_DB_NAME=arcana
export ARCANA_DB_USER=arcana
export ARCANA_DB_PASSWORD=secret
```

### Ingest historical trades

```bash
# Forward-walking backfill, resumable on interrupt.
arcana ingest ETH-USD --since 2025-01-01

# Bounded range
arcana ingest ETH-USD --since 2025-01-01 --until 2025-06-01

# Status snapshot
arcana status ETH-USD
```

### Reach further back with candle synthesis

```bash
# 1-minute candles for an older period
arcana backfill-candles ETH-USD --since 2023-01-01 --until 2024-01-01

# Coarser granularity when you don't need 1m fidelity
arcana backfill-candles BTC-USD --since 2022-01-01 --granularity 5m
```

Synthesized rows are tagged `data_quality='candle_1m'` (etc.) so they can be
filtered or split downstream.

### Run the live daemon

```bash
arcana summon ETH-USD                    # poll every 15 minutes
arcana summon ETH-USD --interval 300     # every 5 minutes
```

The daemon detects any gap between the last stored trade and now, catches up,
then enters the poll loop. Ctrl+C triggers a graceful shutdown with buffer
flush.

---

## CLI reference

```
arcana [--log-level DEBUG|INFO|WARNING|ERROR] [--config PATH]

  db init                       Initialize database schema (idempotent)
  ingest PAIR --since DATE      Backfill tick-level trades from Coinbase
  backfill-candles PAIR         Backfill OHLCV candles as synthetic trades
                  --since DATE  (tagged data_quality='candle_<granularity>')
  summon PAIR                   Live ingestion daemon (polls + catches gaps)
  status [PAIR]                 Trade counts, last timestamp, data gap
```

Bar construction, feature engineering, labeling, and modeling are
**not** part of Arcana — those live in [Sigil](https://position5.org/products/sigil).

---

## Project structure

```
src/arcana/
├── __init__.py              # Package root, version
├── cli.py                   # Click CLI - all user-facing commands
├── config.py                # DatabaseConfig, ArcanaConfig (Pydantic)
├── models.py                # Shared models
├── pipeline.py              # Backfill, candle backfill, daemon orchestration
├── ingestion/
│   ├── base.py              # DataSource ABC - pluggable exchange interface
│   ├── candles.py           # OHLCV Candle model + trade synthesis
│   ├── coinbase.py          # CoinbaseSource - Advanced Trade REST API
│   └── models.py            # Trade model (Pydantic, Decimal precision)
└── storage/
    └── database.py          # TimescaleDB access layer (psycopg 3)

tests/
├── test_cli.py              # CLI command tests
├── test_config.py           # Config loading tests
├── test_pipeline.py         # Backfill, candle backfill, daemon tests
├── test_ingestion/
│   ├── test_candles.py      # Candle synthesis tests
│   ├── test_coinbase.py     # Coinbase client (pagination, retry)
│   └── test_models.py       # Trade model tests
├── test_storage/
│   └── test_database.py     # Database access tests
└── fixtures/
    └── sample_advanced_trade_response.json
```

---

## Data model

### Trade

```
Trade
├── timestamp     datetime    Execution time (UTC)
├── trade_id      str         Exchange-assigned identifier
├── source        str         e.g. "coinbase"
├── pair          str         e.g. "ETH-USD"
├── price         Decimal     Execution price (quote currency)
├── size          Decimal     Execution size (base currency)
├── side          str         "buy" | "sell" | "unknown"
└── data_quality  str         "tick" | "candle_<granularity>"
```

The `raw_trades` hypertable mirrors this schema 1:1. The unique constraint
is `(source, trade_id, timestamp)` and writes use
`INSERT ... ON CONFLICT DO NOTHING`, making every operation safely idempotent.

---

## Design principles

1. **Decimal precision end-to-end.** No floating-point arithmetic in the
   data path. Prices, sizes, and any aggregation use `Decimal` from the API
   response through to database storage.
2. **Idempotent writes.** Every upsert is `ON CONFLICT DO NOTHING`. Re-run
   any operation safely — backfill, candle backfill, daemon — without
   duplicating rows.
3. **Resumable everything.** Backfill resumes from the last stored trade.
   Candle backfill resumes from its own watermark (filtered by
   `data_quality`) so it doesn't get poisoned by tick rows. The daemon
   detects and fills gaps on startup.
4. **Graceful shutdown.** SIGINT/SIGTERM handlers flush in-progress buffers
   before exit. No data loss on Ctrl+C.
5. **Honest provenance.** A row that came from a candle is never silently
   passed off as a tick. The `data_quality` column is the contract.
6. **Pluggable data sources.** The `DataSource` ABC defines the exchange
   interface. Adding Binance or Kraken means implementing one class.

---

## Development

```bash
pip install -e ".[dev]"

pytest tests/ -v
ruff check src/ tests/
ruff format --check src/ tests/
mypy src/
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARCANA_DB_HOST` | `localhost` | Database host |
| `ARCANA_DB_PORT` | `5432` | Database port |
| `ARCANA_DB_NAME` | `arcana` | Database name |
| `ARCANA_DB_USER` | `arcana` | Database user |
| `ARCANA_DB_PASSWORD` | *(empty)* | Database password |
| `ARCANA_RATE_DELAY` | `0.12` | Seconds between API requests (~8 req/s) |

---

## Extending

### Adding a new data source

Implement the `DataSource` ABC:

```python
from arcana.ingestion.base import DataSource
from arcana.ingestion.models import Trade

class BinanceSource(DataSource):
    @property
    def name(self) -> str:
        return "binance"

    def fetch_trades(self, pair, start=None, end=None, limit=1000):
        ...

    def fetch_all_trades(self, pair, start, end):
        ...

    def get_supported_pairs(self):
        return ["ETHUSDT", "BTCUSDT", ...]
```

The pipeline, daemon, and storage layer remain unchanged.

---

## Roadmap

- [x] Tick-level Coinbase ingestion (backfill + daemon)
- [x] Multi-table schema with provenance tagging
- [x] Historical candle synthesis with `data_quality` flag
- [x] `arcana.toml` config support
- [ ] TimescaleDB compression policies
- [ ] Additional exchange sources (Binance, Kraken)
- [ ] `arcana export` for CSV/Parquet snapshots

For the bar-construction and modeling roadmap, see
[Sigil](https://position5.org/products/sigil).

---

## Related

- **[Sigil](https://position5.org/products/sigil)** — Position5's downstream
  processing software. Consumes `raw_trades` and produces bars, features,
  labels, and trained models.
- *Advances in Financial Machine Learning* by Marcos Lopez de Prado —
  the methodological foundation for the bar types Sigil constructs.

---

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
