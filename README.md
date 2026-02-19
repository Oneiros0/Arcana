# Arcana

**Quantitative trading data pipeline** — ingest raw trades from crypto exchanges, construct Prado-style sampling bars, and maintain a live data feed.

Built on the methodology from Marcos Lopez de Prado's *Advances in Financial Machine Learning*.

```
arcana db init  →  arcana ingest ETH-USD --since 2025-01-01  →  arcana bars build tick_500 ETH-USD  →  arcana run ETH-USD
```

[![CI](https://github.com/Oneiros0/Arcana/actions/workflows/ci.yml/badge.svg)](https://github.com/Oneiros0/Arcana/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-green)

---

## Architecture

```
                          ┌─────────────────────────────────────────────┐
                          │                 Arcana CLI                  │
                          │    db init │ ingest │ bars build │ run      │
                          └──────┬──────────┬──────────┬────────┬──────┘
                                 │          │          │        │
                 ┌───────────────┘          │          │        └──────────────┐
                 │                          │          │                       │
                 v                          v          v                       v
          ┌─────────────┐          ┌──────────────┐  ┌──────────────┐  ┌─────────────┐
          │  Database    │          │  Ingestion   │  │ Bar Builders │  │   Daemon    │
          │  Schema      │          │  Pipeline    │  │              │  │   Mode      │
          │  Init        │          │  (backfill)  │  │  tick/vol/   │  │  (poll +    │
          │             │          │              │  │  dollar/time │  │   catch-up) │
          └─────────────┘          │              │  │  TIB/VIB/DIB │  └──────┬──────┘
                                   │              │  │  TRB/VRB/DRB │         │
                                   └──────┬───────┘  └──────┬───────┘         │
                                          │                 │                 │
                                          v                 v                 v
                                   ┌────────────────────────────────────────────┐
                                   │            DataSource (ABC)                │
                                   │         ┌─────────────────────┐            │
                                   │         │  CoinbaseSource     │            │
                                   │         │  (Advanced Trade)   │            │
                                   │         └─────────────────────┘            │
                                   └──────────────────┬─────────────────────────┘
                                                      │
                                                      v
                                   ┌────────────────────────────────────────────┐
                                   │         TimescaleDB / PostgreSQL           │
                                   │                                            │
                                   │  raw_trades (hypertable)                   │
                                   │    timestamp · trade_id · source · pair    │
                                   │    price · size · side                     │
                                   │                                            │
                                   │  bars_tick_500_eth_usd (hypertable)        │
                                   │  bars_time_5m_eth_usd  (hypertable)        │
                                   │  bars_tib_10_eth_usd   (hypertable)        │
                                   │  ... per (bar_type, pair) table            │
                                   │    time_start · time_end · bar_type        │
                                   │    OHLC · vwap · volume · dollar_volume    │
                                   │    tick_count · time_span · metadata       │
                                   └────────────────────────────────────────────┘
```

### Data Flow

```
  Exchange API           raw_trades table           per-pair bar tables
  ────────────           ────────────────           ────────────────────
                  fetch
  Coinbase  ──────────>  Trade records   ────────>  OHLCV bars
  (REST)        │        (tick-level)    process    (10 types: tick, volume,
                │                        trades     dollar, time + TIB, VIB,
                │                                   DIB, TRB, VRB, DRB)
           backward pagination
           with rate limiting
           (~8 req/s)
```

---

## Why Prado-style Bars?

Traditional time bars (1-min, 5-min, 1-hour candles) sample at fixed intervals regardless of market activity. This creates statistical problems:

**Standard Bars** — fixed threshold sampling:

| Bar Type | Trigger | Use Case |
|----------|---------|----------|
| **Time** | Fixed clock interval | Baseline comparison |
| **Tick** | Every *N* trades | Normalizes by information arrival |
| **Volume** | Every *V* units traded | Normalizes by participation |
| **Dollar** | Every *$D* notional | Normalizes by value exchanged |

**Information-Driven Bars** (Prado Ch. 2) — adaptive EWMA-based sampling:

| Bar Type | Trigger | Use Case |
|----------|---------|----------|
| **Tick Imbalance (TIB)** | Cumulative signed tick imbalance exceeds EWMA | Detects informed trading bursts |
| **Volume Imbalance (VIB)** | Cumulative signed volume imbalance exceeds EWMA | Volume-weighted order flow detection |
| **Dollar Imbalance (DIB)** | Cumulative signed dollar imbalance exceeds EWMA | Notional-weighted order flow detection |
| **Tick Run (TRB)** | Longest consecutive buy/sell run exceeds EWMA | Sequential informed trading detection |
| **Volume Run (VRB)** | Volume in longest run exceeds EWMA | Volume-weighted sequential detection |
| **Dollar Run (DRB)** | Dollar volume in longest run exceeds EWMA | Notional-weighted sequential detection |

Dollar bars are Prado's recommended default for standard bars. Information-driven bars sample when market microstructure signals suggest informed trading activity.

All 10 bar types emit identical `Bar` records with OHLCV, VWAP, tick count, time span, and optional metadata (EWMA state for info-driven bars).

---

## Quickstart

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 14+** with [TimescaleDB](https://docs.timescale.com/install/) extension
- A Coinbase account is **not** required (public API endpoints)

### Install

```bash
# Clone and install
git clone https://github.com/Oneiros0/Arcana.git
cd Arcana
pip install -e ".[dev]"

# Verify
arcana --help
```

### Database Setup

```bash
# Create the database (PostgreSQL)
createdb arcana

# Enable TimescaleDB extension (run in psql)
psql -d arcana -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Initialize Arcana schema
arcana db init --host localhost --database arcana --user arcana
```

Database connection options can also be set via environment variables:

```bash
export ARCANA_DB_HOST=localhost
export ARCANA_DB_PORT=5432
export ARCANA_DB_NAME=arcana
export ARCANA_DB_USER=arcana
export ARCANA_DB_PASSWORD=secret
```

### Ingest Historical Data

```bash
# Backfill ETH-USD trades from January 2025
arcana ingest ETH-USD --since 2025-01-01

# Backfill a specific date range
arcana ingest ETH-USD --since 2025-01-01 --until 2025-06-01

# Check ingestion status
arcana status ETH-USD
```

Backfill walks forward through time in 15-minute windows, committing trades in batches of 1,000. It is **resumable** — if interrupted, it picks up from the last stored trade.

### Build Bars

```bash
# Standard bars (fixed threshold)
arcana bars build tick_500 ETH-USD      # 500-tick bars
arcana bars build time_5m ETH-USD       # 5-minute time bars
arcana bars build dollar_50000 ETH-USD  # Dollar bars with $50k threshold
arcana bars build volume_100 ETH-USD    # Volume bars with 100-unit threshold

# Information-driven bars (adaptive EWMA threshold)
arcana bars build tib_10 ETH-USD        # Tick imbalance bars (EWMA window=10)
arcana bars build vib_10 ETH-USD        # Volume imbalance bars
arcana bars build dib_10 ETH-USD        # Dollar imbalance bars
arcana bars build trb_10 ETH-USD        # Tick run bars
arcana bars build vrb_10 ETH-USD        # Volume run bars
arcana bars build drb_10 ETH-USD        # Dollar run bars
```

Bar construction is also resumable. On restart, it continues from the last emitted bar. Information-driven bars restore their EWMA state from the metadata of the last emitted bar.

### Run the Daemon

```bash
# Start live polling (every 15 minutes by default)
arcana run ETH-USD

# Custom poll interval (5 minutes)
arcana run ETH-USD --interval 300
```

The daemon detects any gap between the last stored trade and now, catches up automatically, then enters the poll loop. Ctrl+C triggers a graceful shutdown with buffer flush.

---

## CLI Reference

```
arcana [--log-level DEBUG|INFO|WARNING|ERROR]

  db init          Initialize database schema (idempotent)
  ingest PAIR      Bulk ingest historical trades
  bars build SPEC PAIR   Construct bars from stored trades
  run PAIR         Start the live ingestion daemon
  status [PAIR]    Show trade counts and data gap
```

### Bar Spec Format

The `SPEC` argument to `bars build` follows the pattern `{type}_{threshold}`:

**Standard bars** (fixed threshold):

| Spec | Builder | Threshold |
|------|---------|-----------|
| `tick_500` | TickBarBuilder | 500 trades per bar |
| `volume_100` | VolumeBarBuilder | 100 units base currency |
| `volume_10.5` | VolumeBarBuilder | 10.5 units (decimal OK) |
| `dollar_50000` | DollarBarBuilder | $50,000 notional |
| `time_30s` | TimeBarBuilder | 30-second intervals |
| `time_5m` | TimeBarBuilder | 5-minute intervals |
| `time_1h` | TimeBarBuilder | 1-hour intervals |
| `time_1d` | TimeBarBuilder | Daily intervals |

**Information-driven bars** (adaptive EWMA threshold):

| Spec | Builder | EWMA Window |
|------|---------|-------------|
| `tib_10` | TickImbalanceBarBuilder | 10-sample EWMA |
| `vib_10` | VolumeImbalanceBarBuilder | 10-sample EWMA |
| `dib_10` | DollarImbalanceBarBuilder | 10-sample EWMA |
| `trb_10` | TickRunBarBuilder | 10-sample EWMA |
| `vrb_10` | VolumeRunBarBuilder | 10-sample EWMA |
| `drb_10` | DollarRunBarBuilder | 10-sample EWMA |

The EWMA window controls how quickly the adaptive threshold responds to changes. Smaller windows (5-10) are more reactive; larger windows (30-60) produce more stable thresholds.

---

## Docker

```bash
# Build
docker build -t arcana .

# Run ingestion
docker run --rm --network host arcana ingest ETH-USD --since 2025-01-01

# Run daemon
docker run -d --network host --name arcana-daemon arcana run ETH-USD
```

---

## Project Structure

```
src/arcana/
├── __init__.py              # Package root, version
├── cli.py                   # Click CLI — all user-facing commands
├── config.py                # DatabaseConfig, ArcanaConfig (Pydantic)
├── pipeline.py              # Orchestration: backfill, daemon, bar construction
├── ingestion/
│   ├── base.py              # DataSource ABC — pluggable exchange interface
│   ├── coinbase.py          # CoinbaseSource — Advanced Trade REST API
│   └── models.py            # Trade model (Pydantic, Decimal precision)
├── bars/
│   ├── base.py              # Bar model, Accumulator, BarBuilder ABC
│   ├── standard.py          # Tick, Volume, Dollar, Time bar builders
│   ├── imbalance.py         # Tick/Volume/Dollar Imbalance bar builders
│   ├── runs.py              # Tick/Volume/Dollar Run bar builders
│   └── utils.py             # EWMA estimator, tick rule utility
└── storage/
    └── database.py          # TimescaleDB access layer (psycopg 3)

tests/
├── test_cli.py              # CLI command tests
├── test_pipeline.py         # Pipeline orchestration tests
├── test_bars/
│   ├── test_base.py         # Accumulator and Bar model tests
│   ├── test_standard.py     # Standard bar builder tests
│   ├── test_imbalance.py    # Imbalance bar builder tests
│   ├── test_runs.py         # Run bar builder tests
│   └── test_utils.py        # EWMA estimator and tick rule tests
├── test_ingestion/
│   ├── test_coinbase.py     # API client tests (pagination, retry)
│   └── test_models.py       # Trade model tests
├── test_storage/
│   └── test_database.py     # Database utility function tests
└── fixtures/
    └── sample_advanced_trade_response.json
```

---

## Data Models

### Trade

```
Trade
├── timestamp    datetime     Execution time (UTC)
├── trade_id     str          Exchange-assigned identifier
├── source       str          "coinbase"
├── pair         str          "ETH-USD"
├── price        Decimal      Execution price (quote currency)
├── size         Decimal      Execution size (base currency)
└── side         str          "buy" | "sell" | "unknown"
```

### Bar

```
Bar
├── time_start     datetime     First trade timestamp
├── time_end       datetime     Last trade timestamp
├── bar_type       str          "tick_500", "time_5m", etc.
├── source         str          "coinbase"
├── pair           str          "ETH-USD"
├── open           Decimal      First trade price
├── high           Decimal      Highest trade price
├── low            Decimal      Lowest trade price
├── close          Decimal      Last trade price
├── vwap           Decimal      Volume-weighted average price
├── volume         Decimal      Total base currency volume
├── dollar_volume  Decimal      Total notional (price * size)
├── tick_count     int          Number of trades in bar
├── time_span      timedelta    Duration of bar
└── metadata       dict | None  Bar-specific extra info
```

---

## Design Principles

1. **Decimal precision end-to-end.** No floating-point arithmetic in the data path. Prices, volumes, and VWAP use `Decimal` from API response through to database storage.

2. **Stateful bar builders.** The accumulator carries state across batch calls. This is essential: a bar boundary can fall in the middle of a database fetch. Builder state persists until `flush()`.

3. **Idempotent writes.** `INSERT ... ON CONFLICT DO NOTHING` on all upserts. Safe to re-run any operation — backfill, bar construction, daemon — without duplicating data.

4. **Resumable everything.** Backfill resumes from the last stored trade. Bar construction resumes from the last emitted bar. The daemon detects and fills gaps on startup.

5. **Graceful shutdown.** SIGINT/SIGTERM handlers flush in-progress buffers before exit. No data loss on Ctrl+C.

6. **Pluggable data sources.** The `DataSource` ABC defines the exchange interface. Adding Binance or Kraken means implementing one class — the pipeline, bar builders, and storage layer remain unchanged.

---

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/

# Format check
ruff format --check src/ tests/

# Type check
mypy src/
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARCANA_DB_HOST` | `localhost` | Database host |
| `ARCANA_DB_PORT` | `5432` | Database port |
| `ARCANA_DB_NAME` | `arcana` | Database name |
| `ARCANA_DB_USER` | `arcana` | Database user |
| `ARCANA_DB_PASSWORD` | *(empty)* | Database password |
| `ARCANA_RATE_DELAY` | `0.12` | Seconds between API requests (~8 req/s) |

---

## Extending Arcana

### Adding a New Data Source

Implement the `DataSource` ABC:

```python
from arcana.ingestion.base import DataSource
from arcana.ingestion.models import Trade

class BinanceSource(DataSource):
    @property
    def name(self) -> str:
        return "binance"

    def fetch_trades(self, pair, start=None, end=None, limit=1000):
        # Single API call, return list[Trade]
        ...

    def fetch_all_trades(self, pair, start, end):
        # Paginated fetch, return all trades in window
        ...

    def get_supported_pairs(self):
        return ["ETHUSDT", "BTCUSDT", ...]
```

### Adding a New Bar Type

Subclass `BarBuilder`:

```python
from arcana.bars.base import Bar, BarBuilder
from arcana.ingestion.models import Trade

class EntropyBarBuilder(BarBuilder):
    def __init__(self, source, pair, threshold):
        super().__init__(source, pair)
        self._threshold = threshold

    @property
    def bar_type(self) -> str:
        return f"entropy_{self._threshold}"

    def process_trade(self, trade: Trade) -> Bar | None:
        self._acc.add(trade)
        # Your emission logic here
        if self._should_emit():
            return self._emit_and_reset()
        return None
```

---

## Roadmap

- [x] Information-driven bars (tick/volume/dollar imbalance and run bars)
- [x] EWMA threshold estimator for adaptive bar sizing
- [x] Tick rule for unsigned trades
- [x] Per-pair-per-type bar tables for query isolation
- [ ] TimescaleDB compression policies
- [ ] Configuration file support (`arcana.toml`)
- [ ] Additional exchange sources (Binance, Kraken)
- [ ] `arcana export` command for CSV/Parquet output

---

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
