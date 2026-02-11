# Arcana — Project Plan

## Vision

Arcana is an open-source Python library that ingests raw trade data from financial exchanges and constructs the information-driven sampling methods described in Marcos López de Prado's *Advances in Financial Machine Learning*. The immediate focus is crypto (Coinbase ETH-USD), but the architecture supports any trade feed — equities, forex, other CEXs, and eventually on-chain DEX swaps.

The goal: provide researchers and quant developers with properly structured bars as a foundation for ML-based trading strategies, installable via `pip install arcana`.

---

## Current Status (2026-02-11)

| Component | Status | Details |
|---|---|---|
| **Ingestion pipeline** | Done | Backfill + daemon mode, binary subdivision pagination, graceful shutdown |
| **Coinbase API client** | Done | Advanced Trade API, retry with backoff, rate limiting |
| **Database layer** | Done | raw_trades + bars tables, upsert, trade/bar CRUD |
| **Standard bar builders** | Done | Time, tick, volume, dollar — all with OHLCV + VWAP |
| **Bar CLI command** | Not started | `arcana bars build` needs wiring |
| **Bar builder recovery** | Not started | DB methods exist, orchestration not connected |
| **Information-driven bars** | Not started | TIB, VIB, DIB, TRB, VRB, DRB (Phase 2) |

**Codebase:** 1,489 lines source / 1,036 lines test / 64 tests passing
**Git:** 10 commits on `claude/plan-trading-pipeline-aNfsS`

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
- `fetch_trades(pair, start, end, limit) -> list[Trade]` — single-request fetch
- `fetch_all_trades(pair, start, end) -> list[Trade]` — complete fetch with automatic pagination
- `get_supported_pairs() -> list[str]` — available trading pairs

**Bar Construction Layer** — Takes a sequence of `Trade` objects and produces bars. All bar builders implement a `BarBuilder` abstract base class with:
- `process_trade(trade: Trade) -> Bar | None` — process one trade, return bar if threshold met
- `process_trades(trades: list[Trade]) -> list[Bar]` — stateful batch processing; maintains internal accumulators across calls
- `flush() -> Bar | None` — emit in-progress bar at end of data or shutdown
- Configurable thresholds (tick count, volume, dollar amount, EWMA window)
- `Accumulator` tracks running OHLCV state (open, high, low, close, volume, dollar volume, VWAP numerator, tick count) without storing individual trades

**Storage Layer** — Manages TimescaleDB connections, schema migrations, and read/write:
- Raw trades stored in a `raw_trades` hypertable (partitioned by time)
- Bars stored in `bars` table with upsert on `(bar_type, source, pair, time_start)`
- Handles deduplication (trade IDs), upserts, and compression policies
- `get_trades_since()` loads raw trades for bar construction
- `get_last_bar_time()` provides resume points for incremental bar building

---

## Project Structure

Files marked with `*` are planned but not yet created.

```
arcana/
├── src/
│   └── arcana/
│       ├── __init__.py               # Package init, version 0.1.0
│       ├── cli.py                    # Click CLI entry point (190 lines)
│       ├── config.py                 # DatabaseConfig, ArcanaConfig (27 lines)
│       ├── pipeline.py              # ingest_backfill, run_daemon, GracefulShutdown (242 lines)
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   ├── base.py              # DataSource ABC (fetch_trades, fetch_all_trades)
│       │   ├── coinbase.py          # Coinbase Advanced Trade API client (264 lines)
│       │   └── models.py            # Trade Pydantic model (37 lines)
│       │
│       ├── bars/
│       │   ├── __init__.py           # Exports all bar types
│       │   ├── base.py              # Bar model, Accumulator, BarBuilder ABC (180 lines)
│       │   ├── standard.py          # TickBar, VolumeBar, DollarBar, TimeBar (130 lines)
│       │   ├── imbalance.py *       # TickImbalanceBar, VolumeImbalanceBar, DollarImbalanceBar
│       │   ├── runs.py *            # TickRunBar, VolumeRunBar, DollarRunBar
│       │   └── utils.py *           # EWMA estimator, tick rule
│       │
│       └── storage/
│           ├── __init__.py
│           └── database.py          # TimescaleDB: schema, trade/bar CRUD (331 lines)
│
├── scripts/
│   ├── explore_coinbase.py          # API response analysis (220 lines)
│   ├── query_trades.py              # DB trade analysis & gap detection (241 lines)
│   └── clear_trades.py              # Delete trades for re-ingestion (118 lines)
│
├── tests/
│   ├── fixtures/
│   │   └── sample_advanced_trade_response.json
│   ├── test_cli.py                  # 7 tests
│   ├── test_pipeline.py             # 7 tests
│   ├── test_ingestion/
│   │   ├── test_coinbase.py         # 16 tests (fetch, pagination, retry)
│   │   └── test_models.py           # 6 tests
│   └── test_bars/
│       ├── test_base.py             # 6 tests (Accumulator, Bar model)
│       └── test_standard.py         # 22 tests (all 4 bar types)
│
├── pyproject.toml                   # Project metadata, dependencies, build config
├── README.md
├── PLAN.md                          # This document
└── .github/
    └── workflows/
        └── ci.yml *                 # GitHub Actions: lint, test, type-check
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

**Scaffolding & Data Model:**
- [x] Project scaffolding (pyproject.toml, src layout, CI)
- [x] Trade data model (`Trade` dataclass with Pydantic validation)
- [x] Coinbase ingestion client (REST API, pagination, rate limiting)
- [x] TimescaleDB storage layer (connection, migrations, raw trade CRUD)
- [x] API response analysis & data exploration script

**Ingestion Pipeline:**
- [x] Bulk ingestion command: `arcana ingest ETH-USD --since 2025-01-01`
  - Backfills raw trades from `--since` date to present via forward time-window walk
  - Binary subdivision pagination: automatically splits busy windows to capture all trades
  - Writes to `raw_trades` table in batches
  - Resumable — on restart, picks up from `MAX(timestamp)` for the pair
  - Progress logging (trades ingested, time range covered, ETA)
- [x] Daemon mode: `arcana run ETH-USD`
  - On startup, detects last stored timestamp for the pair
  - Catches up any gap, then polls Coinbase every 15 minutes for new trades
  - Stores raw trades (bar building inline deferred to bar builders phase)
  - Runs as a background process indefinitely

**Failsafes & Resumability:**
- [x] Ingestion checkpointing — commit trades to DB in batches (every 1000 trades) so a crash mid-backfill loses at most one batch
- [x] Duplicate detection — `UNIQUE (source, trade_id)` constraint + `ON CONFLICT DO NOTHING` upserts so re-running ingestion over an overlapping range is safe
- [x] API failure retry — exponential backoff (2s, 4s, 8s, 16s) on HTTP errors, with max 4 retries before halting
- [x] Daemon heartbeat — logs last successful poll time; on restart, detects gap and backfills missed trades before resuming the poll loop
- [ ] Bar builder recovery — wire `get_last_bar_time()` + `get_trades_since()` into a bar rebuild routine at startup. The DB methods exist, but the orchestration is not yet connected.
- [x] Graceful shutdown — handles SIGINT/SIGTERM, finishes current batch and commits before exiting

**Standard Bar Builders:**
- [x] `Bar` model (Pydantic, frozen) — OHLCV, VWAP, tick count, dollar volume, time span, metadata
- [x] `Accumulator` — running OHLCV state tracker, computes VWAP from price*volume numerator
- [x] `BarBuilder` ABC — `process_trade()`, `process_trades()`, `flush()`, stateful across batches
- [x] `TickBarBuilder` — emit every N trades
- [x] `VolumeBarBuilder` — emit every V base-currency volume
- [x] `DollarBarBuilder` — emit every D notional dollars (Prado's preferred)
- [x] `TimeBarBuilder` — clock-aligned buckets, empty gaps skipped
- [x] Bar storage — `insert_bars()` upsert, `get_last_bar_time()`, `get_bar_count()`, `get_trades_since()`

**CLI & Tests:**
- [x] CLI: `arcana db init`, `arcana ingest`, `arcana run`, `arcana status`
- [ ] CLI: `arcana bars build` — wire bar builders to CLI command
- [x] Unit tests for bar construction (22 tests with hand-computed expected values)
- [x] Tests for pipeline (backfill, resume, checkpointing, graceful shutdown)
- [x] Tests for ingestion (fetch, pagination, binary subdivision, retry — 16 tests)
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
- [ ] EWMA state persistence for daemon restarts (imbalance/run bars carry state across cycles)
- [ ] CLI: `arcana bars build` for all imbalance/run types
- [ ] Tests with synthetic trade sequences to verify bar boundaries

### Phase 3 — Pipeline & Polish
**Goal:** End-to-end automated pipeline, export, documentation.

- [ ] Multi-pair support: `arcana run ETH-USD SOL-USD BTC-USD`
- [ ] Parquet/CSV export (`arcana bars export`)
- [ ] `arcana status` diagnostics (trade counts, bar counts, last update, gaps)
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

We use the **Coinbase Advanced Trade API** — the public `/market/` endpoints require no authentication.

**Base URL:** `https://api.coinbase.com`
**Authentication:** None required for `/market/` endpoints
**Rate limit:** 10 req/s (public), 30 req/s (authenticated with JWT)

### Endpoint: Get Market Trades
```
GET /api/v3/brokerage/market/products/{product_id}/ticker
```

**Parameters:**
| Param | Type | Required | Description |
|---|---|---|---|
| `limit` | int | Yes | Number of trades to return |
| `start` | string | No | UNIX timestamp — start of time window |
| `end` | string | No | UNIX timestamp — end of time window |

**Response:**
```json
{
  "trades": [
    {
      "trade_id": "uuid-string",
      "product_id": "ETH-USD",
      "price": "2845.32",
      "size": "0.5",
      "time": "2026-02-10T14:30:01.123Z",
      "side": "BUY",
      "exchange": "COINBASE"
    }
  ],
  "best_bid": "2845.25",
  "best_ask": "2845.35"
}
```

### Key notes
1. **`side` is the taker side** (`"BUY"`/`"SELL"`) — use directly, no inversion needed.
2. **All numeric values are strings.** Parse with `Decimal`, never `float`.
3. **Time-window pagination:** Use `start`/`end` UNIX timestamps to walk forward through time. No cursor management needed.
4. **No API key needed for v1.** Public endpoints are sufficient.

### Ingestion Strategy

**Bulk backfill** (`arcana ingest ETH-USD --since 2025-01-01`):
Walk forward through time in 1-hour windows:
```
Window 1: start=Jan 1 00:00, end=Jan 1 01:00 → fetch trades
Window 2: start=Jan 1 01:00, end=Jan 1 02:00 → fetch trades
...
Window N: start=today 13:00, end=now → done
```
Each batch of trades is committed to the DB. On crash, resume from `MAX(timestamp)`.

**Binary subdivision pagination:** The API returns at most 300 trades per request. For busy trading periods (2000+ trades/hour), `fetch_all_trades()` automatically splits the window in half and recurses until every sub-window fits within the 300-trade limit. Trades at subdivision boundaries are deduplicated by `trade_id`. Max recursion depth of 10 (~3.5s minimum window) prevents infinite loops.

```
fetch_all_trades(14:00, 15:00)
  → API returns 300 (at limit) → subdivide
  ├── fetch_all_trades(14:00, 14:30) → 180 trades ✓
  └── fetch_all_trades(14:30, 15:00)
      → API returns 300 (at limit) → subdivide
      ├── fetch_all_trades(14:30, 14:45) → 140 trades ✓
      └── fetch_all_trades(14:45, 15:00) → 160 trades ✓
  → merge & dedup → 480 complete trades
```

**Daemon mode** (`arcana run ETH-USD`):
```
Every 15 minutes:
  start = MAX(timestamp) from raw_trades WHERE pair = 'ETH-USD'
  end   = now
  Fetch trades → store → build bars
```

---

## Guiding Principles

1. **Correctness over speed.** Prado's bar construction math must be exact. Every bar type gets tested against hand-computed examples.
2. **Pluggable data sources.** The `DataSource` ABC means adding Binance or Uniswap later is just a new class, no refactoring.
3. **Stateful bar builders.** Bar construction is inherently stateful (accumulators carry across batches). The `BarBuilder` class manages this explicitly.
4. **No premature optimization.** Python + pandas is fast enough for 15-minute batch intervals. Optimize only when profiling shows a bottleneck.
5. **Minimal dependencies.** Every dependency must earn its place.
