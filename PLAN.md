# Arcana — Project Plan

## Vision

Arcana is an open-source Python library that ingests raw trade data from financial exchanges and constructs the information-driven sampling methods described in Marcos López de Prado's *Advances in Financial Machine Learning*. The immediate focus is crypto (Coinbase ETH-USD), but the architecture supports any trade feed — equities, forex, other CEXs, and eventually on-chain DEX swaps.

The goal: provide researchers and quant developers with properly structured bars as a foundation for ML-based trading strategies, installable via `pip install arcana`.

---

## Consolidated Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | Python 3.11+ | Prado reference implementations, quant ecosystem (pandas/numpy), low contribution barrier |
| Data source (v1) | Coinbase Advanced Trade API | Free tier, well-documented REST + WebSocket, ETH-USD liquidity |
| Trading pair (v1) | ETH-USD | Ethereum has the strongest DEX infrastructure (Uniswap) for future on-chain expansion |
| Database | TimescaleDB (PostgreSQL) | SQL interface, hypertable compression, continuous aggregates, mature ecosystem |
| Processing mode | Batch (15-minute intervals) | Sufficient for bar construction; real-time streaming deferred to v2 |
| EWMA windows | 3, 5, 15, 30, 60 (configurable) | Covers short-term to intraday horizons |
| Bar auxiliary info | VWAP, tick count, time span, high, low, open, close | Per Prado's recommendation for downstream ML |
| Distribution | pip-installable library with CLI | `pip install arcana` + `arcana` CLI commands |
| License | Apache 2.0 | Patent protection, commercial-friendly, standard for data/ML projects |

---

## Bar Types (Prado Ch. 2–3)

### Standard Bars
These sample based on a fixed threshold of activity:

| Bar Type | Sampling Rule | Use Case |
|---|---|---|
| **Time bars** | Fixed time intervals (1m, 5m, 15m, 1h, 1d) | Baseline comparison; what most platforms provide |
| **Tick bars** | Every N trades | Removes time-dependent oversampling of quiet periods |
| **Volume bars** | Every V units of volume traded | Samples proportional to market activity |
| **Dollar bars** | Every D dollars transacted | Normalizes for price changes over time; Prado's preferred standard bar |

### Information-Driven Bars (Imbalance Bars)
These sample when the imbalance of signed trades exceeds an expected value estimated via EWMA. The idea: a burst of buy-side or sell-side pressure suggests informed trading, and *that* is when you should sample.

| Bar Type | Imbalance Signal | EWMA Target |
|---|---|---|
| **Tick imbalance bars (TIB)** | Cumulative sign of trades (+1 buy, -1 sell) | Expected tick imbalance |
| **Volume imbalance bars (VIB)** | Cumulative signed volume | Expected volume imbalance |
| **Dollar imbalance bars (DIB)** | Cumulative signed dollar volume | Expected dollar imbalance |

**Implementation detail:** Trade sign is determined by the tick rule — if price > previous price, it's a buy (+1); if price < previous price, it's a sell (-1); if equal, carry forward the previous sign.

### Run Bars
Similar to imbalance bars, but instead of tracking cumulative imbalance, they track the longest *run* of consecutive buys or sells. A long run suggests sequential informed trading.

| Bar Type | Run Signal | EWMA Target |
|---|---|---|
| **Tick run bars (TRB)** | Max run length of consecutive buy/sell signs | Expected max run length |
| **Volume run bars (VRB)** | Volume accumulated during max run | Expected run volume |
| **Dollar run bars (DRB)** | Dollar volume accumulated during max run | Expected run dollar volume |

### Auxiliary Fields (Attached to Every Bar)

Every bar, regardless of type, includes:
- `open`, `high`, `low`, `close` — standard OHLC
- `vwap` — volume-weighted average price
- `tick_count` — number of trades in the bar
- `volume` — total volume
- `dollar_volume` — total dollar volume (price * volume)
- `time_start`, `time_end` — timestamp range
- `time_span` — duration in seconds

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     CLI (click)                      │
│         arcana ingest / arcana bars / arcana status   │
└──────────┬──────────────────────┬────────────────────┘
           │                      │
           ▼                      ▼
┌─────────────────┐    ┌─────────────────────┐
│   Ingestion     │    │   Bar Construction   │
│   Layer         │    │   Layer              │
│                 │    │                      │
│ ┌─────────────┐ │    │ ┌─────────────────┐  │
│ │  Coinbase   │ │    │ │ Standard Bars   │  │
│ │  Client     │ │    │ │ (time/tick/vol/ │  │
│ └─────────────┘ │    │ │  dollar)        │  │
│ ┌─────────────┐ │    │ ├─────────────────┤  │
│ │  (Future)   │ │    │ │ Imbalance Bars  │  │
│ │  Binance    │ │    │ │ (TIB/VIB/DIB)  │  │
│ │  Kraken     │ │    │ ├─────────────────┤  │
│ │  Uniswap   │ │    │ │ Run Bars        │  │
│ └─────────────┘ │    │ │ (TRB/VRB/DRB)  │  │
│                 │    │ └─────────────────┘  │
│  Abstract base: │    │                      │
│  DataSource     │    │  Abstract base:      │
│                 │    │  BarBuilder          │
└────────┬────────┘    └──────────┬───────────┘
         │                        │
         ▼                        ▼
┌─────────────────────────────────────────────┐
│              Storage Layer                   │
│                                             │
│  TimescaleDB (PostgreSQL)                   │
│  ┌──────────────┐  ┌─────────────────────┐  │
│  │ raw_trades   │  │ bars                │  │
│  │ (hypertable) │  │ (hypertable per     │  │
│  │              │  │  bar type)          │  │
│  └──────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────┘
```

### Layer Responsibilities

**Ingestion Layer** — Fetches raw trades from exchanges and normalizes them into a common `Trade` schema:
```
Trade:
  timestamp: datetime (UTC)
  price: Decimal
  size: Decimal          # volume in base currency
  side: str              # "buy" or "sell" (if available from exchange)
  trade_id: str          # exchange-specific trade ID
  source: str            # "coinbase", "binance", etc.
  pair: str              # "ETH-USD"
```

All data sources implement a `DataSource` abstract base class with:
- `fetch_trades(pair, start, end) -> list[Trade]` — historical trades
- `get_supported_pairs() -> list[str]` — available trading pairs

**Bar Construction Layer** — Takes a sequence of `Trade` objects and produces bars. All bar builders implement a `BarBuilder` abstract base class with:
- `process_trades(trades: list[Trade]) -> list[Bar]` — stateful; maintains internal accumulators across calls
- `reset()` — clear internal state
- Configurable thresholds (tick count, volume, dollar amount, EWMA window)

**Storage Layer** — Manages TimescaleDB connections, schema migrations, and read/write:
- Raw trades stored in a `raw_trades` hypertable (partitioned by time)
- Bars stored in separate hypertables per bar type
- Handles deduplication (trade IDs), upserts, and compression policies

---

## Project Structure

```
arcana/
├── src/
│   └── arcana/
│       ├── __init__.py
│       ├── cli.py                    # Click CLI entry point
│       ├── config.py                 # Configuration (TOML-based)
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── base.py              # DataSource ABC
│       │   ├── coinbase.py          # Coinbase Advanced Trade client
│       │   └── models.py           # Trade dataclass
│       │
│       ├── bars/
│       │   ├── __init__.py
│       │   ├── base.py              # BarBuilder ABC, Bar dataclass
│       │   ├── standard.py          # TimeBar, TickBar, VolumeBar, DollarBar
│       │   ├── imbalance.py         # TickImbalanceBar, VolumeImbalanceBar, DollarImbalanceBar
│       │   ├── runs.py              # TickRunBar, VolumeRunBar, DollarRunBar
│       │   └── utils.py            # EWMA, tick rule, auxiliary computation
│       │
│       └── storage/
│           ├── __init__.py
│           ├── database.py          # TimescaleDB connection & migrations
│           ├── trades.py            # Raw trade read/write
│           └── bars.py              # Bar read/write
│
├── tests/
│   ├── conftest.py
│   ├── test_ingestion/
│   │   ├── test_coinbase.py
│   │   └── test_models.py
│   ├── test_bars/
│   │   ├── test_standard.py
│   │   ├── test_imbalance.py
│   │   ├── test_runs.py
│   │   └── test_utils.py
│   └── test_storage/
│       └── test_database.py
│
├── pyproject.toml                   # Project metadata, dependencies, build config
├── LICENSE                          # Apache 2.0
├── README.md
├── PLAN.md                          # This document
└── .github/
    └── workflows/
        └── ci.yml                   # GitHub Actions: lint, test, type-check
```

---

## Dependencies

### Core
| Package | Purpose |
|---|---|
| `pandas` | DataFrames for bar data, trade batching |
| `numpy` | Numerical computation (EWMA, statistics) |
| `psycopg[binary]` | PostgreSQL/TimescaleDB driver (psycopg 3) |
| `sqlalchemy` | ORM + migration support |
| `httpx` | Async HTTP client for Coinbase REST API |
| `click` | CLI framework |
| `pydantic` | Configuration validation, data models |
| `tomli` | TOML config file parsing (stdlib in 3.11+) |

### Dev
| Package | Purpose |
|---|---|
| `pytest` | Testing framework |
| `pytest-cov` | Coverage reporting |
| `ruff` | Linting + formatting |
| `mypy` | Static type checking |
| `pre-commit` | Git hooks for lint/format |

---

## Database Schema

### `raw_trades` (TimescaleDB hypertable)
```sql
CREATE TABLE raw_trades (
    timestamp    TIMESTAMPTZ   NOT NULL,
    trade_id     TEXT          NOT NULL,
    source       TEXT          NOT NULL,    -- 'coinbase'
    pair         TEXT          NOT NULL,    -- 'ETH-USD'
    price        NUMERIC       NOT NULL,
    size         NUMERIC       NOT NULL,
    side         TEXT,                      -- 'buy', 'sell', or NULL
    UNIQUE (source, trade_id)
);

SELECT create_hypertable('raw_trades', 'timestamp');
```

### `bars` (TimescaleDB hypertable)
```sql
CREATE TABLE bars (
    time_start    TIMESTAMPTZ   NOT NULL,
    time_end      TIMESTAMPTZ   NOT NULL,
    bar_type      TEXT          NOT NULL,   -- 'time_1m', 'tick_500', 'tib_ewma5', etc.
    source        TEXT          NOT NULL,
    pair          TEXT          NOT NULL,
    open          NUMERIC       NOT NULL,
    high          NUMERIC       NOT NULL,
    low           NUMERIC       NOT NULL,
    close         NUMERIC       NOT NULL,
    vwap          NUMERIC       NOT NULL,
    volume        NUMERIC       NOT NULL,
    dollar_volume NUMERIC       NOT NULL,
    tick_count    INTEGER       NOT NULL,
    time_span     INTERVAL      NOT NULL,
    metadata      JSONB,                   -- bar-specific extra info (thresholds, EWMA state)
    UNIQUE (bar_type, source, pair, time_start)
);

SELECT create_hypertable('bars', 'time_start');
```

---

## CLI Interface

```bash
# Initialize database (run migrations)
arcana db init
arcana db status

# Ingest raw trades
arcana ingest coinbase ETH-USD                    # fetch latest trades
arcana ingest coinbase ETH-USD --since 2025-01-01 # backfill from date

# Construct bars from stored trades
arcana bars build --type tick --threshold 500     # tick bars, 500 trades each
arcana bars build --type volume --threshold 100   # volume bars, 100 ETH each
arcana bars build --type dollar --threshold 500000  # dollar bars, $500k each
arcana bars build --type time --interval 5m       # 5-minute time bars
arcana bars build --type tib --ewma-window 15     # tick imbalance bars
arcana bars build --type vib --ewma-window 30     # volume imbalance bars
arcana bars build --type dib --ewma-window 5      # dollar imbalance bars
arcana bars build --type trb --ewma-window 15     # tick run bars
arcana bars build --type vrb --ewma-window 60     # volume run bars
arcana bars build --type drb --ewma-window 3      # dollar run bars

# Export bars for analysis
arcana bars export --type tib --format parquet --output ./data/
arcana bars export --type tib --format csv --output ./data/

# Run the batch pipeline (ingest + build all configured bars)
arcana pipeline run                               # single run
arcana pipeline schedule --interval 15m           # recurring 15-min batch

# Status and diagnostics
arcana status                                     # trade count, bar counts, last update
```

---

## Implementation Phases

### Phase 1 — Foundation (MVP)
**Goal:** Ingest trades from Coinbase, store them, build standard bars.

- [ ] Project scaffolding (pyproject.toml, src layout, CI)
- [ ] Trade data model (`Trade` dataclass with Pydantic validation)
- [ ] Coinbase ingestion client (REST API, pagination, rate limiting)
- [ ] TimescaleDB storage layer (connection, migrations, raw trade CRUD)
- [ ] Standard bar builders (time, tick, volume, dollar)
- [ ] Bar auxiliary info computation (OHLCV, VWAP, tick count, time span)
- [ ] CLI: `arcana db init`, `arcana ingest`, `arcana bars build` (standard types)
- [ ] Unit tests for bar construction (known inputs -> expected outputs)
- [ ] README with quickstart

### Phase 2 — Information-Driven Bars
**Goal:** Implement Prado's information-driven sampling methods.

- [ ] Tick rule implementation (trade sign classification)
- [ ] EWMA estimator (configurable windows: 3, 5, 15, 30, 60)
- [ ] Tick imbalance bars (TIB)
- [ ] Volume imbalance bars (VIB)
- [ ] Dollar imbalance bars (DIB)
- [ ] Tick run bars (TRB)
- [ ] Volume run bars (VRB)
- [ ] Dollar run bars (DRB)
- [ ] CLI: `arcana bars build` for all imbalance/run types
- [ ] Tests with synthetic trade sequences to verify bar boundaries

### Phase 3 — Pipeline & Polish
**Goal:** End-to-end automated pipeline, export, documentation.

- [ ] Batch pipeline command (`arcana pipeline run`)
- [ ] Scheduler for recurring ingestion (`arcana pipeline schedule`)
- [ ] Parquet/CSV export (`arcana bars export`)
- [ ] `arcana status` diagnostics
- [ ] Configuration file support (`arcana.toml`)
- [ ] Comprehensive documentation
- [ ] PyPI publishing setup

### Phase 4 — Extensibility (Future)
**Goal:** Additional data sources and features.

- [ ] Additional CEX data sources (Binance, Kraken)
- [ ] On-chain DEX ingestion (Uniswap v3 event logs via RPC/subgraph)
- [ ] Real-time streaming mode (WebSocket ingestion + live bar updates)
- [ ] ETF trick for multi-instrument bars (Prado Ch. 2)
- [ ] Additional trading pairs
- [ ] Web dashboard for monitoring

---

## Coinbase API Details

**API:** Coinbase Advanced Trade API (successor to Coinbase Pro)
**Authentication:** API key + secret (free tier sufficient for historical trades)
**Rate limits:** 10 requests/second for public endpoints

**Key endpoints:**
- `GET /api/v3/brokerage/market/products/{product_id}/ticker` — current ticker
- `GET /api/v3/brokerage/market/products/{product_id}/candles` — OHLCV candles (for validation)
- Market trades via WebSocket feed for real-time (v2/future use)

**Note:** Coinbase's REST API for individual trades has limitations. For historical trade data, we may need to:
1. Use the WebSocket feed to collect trades going forward
2. Use the candle endpoint for historical backfill validation
3. Consider supplementing with a data provider like CryptoCompare or Kaiko for deep historical data

This will be refined during Phase 1 implementation when we test the actual API behavior.

---

## Guiding Principles

1. **Correctness over speed.** Prado's bar construction math must be exact. Every bar type gets tested against hand-computed examples.
2. **Pluggable data sources.** The `DataSource` ABC means adding Binance or Uniswap later is just a new class, no refactoring.
3. **Stateful bar builders.** Bar construction is inherently stateful (accumulators carry across batches). The `BarBuilder` class manages this explicitly.
4. **No premature optimization.** Python + pandas is fast enough for 15-minute batch intervals. Optimize only when profiling shows a bottleneck.
5. **Minimal dependencies.** Every dependency must earn its place.
