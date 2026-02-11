# Arcana — Project Plan

## Vision

Arcana is an open-source Python library that ingests raw trade data from financial exchanges and constructs the information-driven sampling methods described in Marcos López de Prado's *Advances in Financial Machine Learning*. The immediate focus is crypto (Coinbase ETH-USD), but the architecture supports any trade feed — equities, forex, other CEXs, and eventually on-chain DEX swaps.

The goal: provide researchers and quant developers with properly structured bars as a foundation for ML-based trading strategies, installable via `pip install arcana`.

---

## Current Status (2026-02-11)

| Component | Status | Details |
|---|---|---|
| **Ingestion pipeline** | Done | Backfill + daemon mode, backward sequential pagination, graceful shutdown |
| **Coinbase API client** | Done | Advanced Trade API, retry with backoff, rate limiting |
| **Database layer** | Done | raw_trades + bars tables, upsert, trade/bar CRUD |
| **Standard bar builders** | Done | Time, tick, volume, dollar — all with OHLCV + VWAP |
| **Bar CLI command** | Not started | `arcana bars build` needs wiring |
| **Bar builder recovery** | Not started | DB methods exist, orchestration not connected |
| **Information-driven bars** | Not started | TIB, VIB, DIB, TRB, VRB, DRB (Phase 2) |

**Codebase:** ~1,900 lines source / ~1,250 lines test / 87 tests passing
**Git:** 12 commits on `claude/plan-trading-pipeline-aNfsS`

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

### Explicit Design Decisions (Addressing Open Questions)

These decisions were identified during peer review and are recorded here so they're explicit rather than implicit.

**1. Decimal stays Decimal — no float conversion in the hot path.**
The entire bar construction pipeline — `Trade`, `Accumulator`, `Bar` — uses `Decimal` end to end. VWAP is computed as `sum(price * size) / sum(size)` in Decimal arithmetic. There is no conversion to float64 anywhere in the ingestion → accumulation → bar emission path. This is deliberate: financial data demands exact arithmetic, and Prado's bar math (especially dollar bars and VWAP) is sensitive to rounding drift.

`pandas` and `numpy` are listed as dependencies for *downstream analysis* (e.g., loading bars into DataFrames for feature engineering, computing rolling statistics). The Decimal→float boundary lives at the **export layer** — when bars are loaded into a DataFrame for ML consumption, `Decimal` columns are cast to `float64`. This is acceptable because ML models operate in float space anyway, and the precision loss at that stage is irrelevant (it's O(10⁻¹⁵) on prices in the thousands). The critical property is that bar *construction* never loses precision.

**2. EWMA state persistence: stored in `metadata` JSONB on the last emitted bar.**
When a daemon restarts, imbalance/run bar builders recover by:
1. Loading the last emitted bar for their `bar_type` via `get_last_bar_time()`
2. Reading EWMA state from that bar's `metadata` field: `{"ewma_expected_imbalance": 142.5, "ewma_window": 15, "ewma_bar_count": 3200, "last_trade_sign": 1}`
3. Reconstructing the EWMA estimator from these values

This means the first bar after restart has the correct adaptive threshold — no warmup period needed. The tradeoff: EWMA state is coupled to the bar table, so deleting bars loses the state. This is acceptable because rebuilding bars from raw trades is idempotent and will regenerate the EWMA progression.

If EWMA state becomes complex enough to warrant its own table (e.g., multiple concurrent EWMA windows per bar type), we'll promote it. For v1, metadata JSONB is sufficient.

**3. `flush()` semantics: end-of-data and shutdown only, never between batches.**
This is a correctness invariant. `process_trades()` is designed to be called repeatedly with successive batches — the `Accumulator` carries state across calls. `flush()` forces emission of the in-progress partial bar, which is destructive (resets the accumulator). Calling `flush()` between batches would produce bars that violate the sampling threshold.

Concretely:
- **Between batches:** Do not call `flush()`. The accumulator state persists in memory.
- **End of data** (backfill complete): Call `flush()` to emit the final partial bar.
- **Shutdown** (SIGINT/SIGTERM): Call `flush()` during graceful shutdown to avoid losing accumulated state.
- **Daemon restart:** Accumulator state is lost. Rebuild from the last emitted bar's `time_end` using `get_trades_since()`. This produces a correct bar because we replay the exact trades.

For imbalance/run bars, flushing is even more critical to avoid: a premature flush would emit a bar before the adaptive EWMA threshold is reached, producing a bar with wrong statistical properties. The orchestration layer must never flush between batches.

**4. Trade sign: use exchange-provided `side` when available, tick rule as fallback.**
Coinbase provides the taker side (`BUY`/`SELL`) on every trade. The `Trade.sign()` method returns `+1` for buy, `-1` for sell using this directly. This is more accurate than the tick rule (which infers sign from price movement) because the tick rule:
- Misclassifies trades at the same price as the previous trade (carry-forward heuristic)
- Cannot distinguish a buy at the ask from a sell at the bid when the price doesn't change
- Is a workaround for data that lacks side information, not an improvement over having it

**Decision:** For sources that provide `side` (Coinbase, Binance), use it directly. The tick rule (`imbalance.py`, Phase 2) will be implemented as a utility for sources that don't provide side information (e.g., some equity feeds, DEX event logs where taker side may not be explicit). The `DataSource` ABC does not mandate that `side` is populated — it can be `"unknown"`, which signals the bar builder to apply the tick rule.

**5. Data quality: trust exchange data as-is for v1.**
We do not filter outliers, detect flash crashes, or remove erroneous trades. A single fat-finger trade at 10x market price will produce a bar with a distorted high/VWAP. This is a deliberate v1 simplification:
- Exchange-reported trades have already passed the exchange's matching engine validation
- Defining "outlier" requires a reference price, which introduces a circular dependency for bar construction
- Prado's methods are designed for exchange-quality data and don't include a preprocessing filter

**Future (v2+):** Add an optional `TradeFilter` interface that can be injected before bar construction. Possible filters: z-score on price relative to a rolling window, minimum/maximum trade size, exchange-reported trade cancellations. This is deferred because it's a feature-engineering concern, not a bar construction concern.

**6. Timestamp normalization: UTC everywhere, single-source assumption for v1.**
All timestamps are stored as `TIMESTAMPTZ` (UTC). The `Trade` model enforces UTC via Pydantic. For v1 with a single source (Coinbase), clock synchronization is not a concern — all timestamps come from the same exchange clock.

**Future multi-source consideration:** When merging trades from multiple exchanges (e.g., Coinbase + Binance for cross-exchange arbitrage bars), clock skew of 10-100ms is typical between exchanges. The `DataSource` ABC does not address this. When we add a second source, we'll need to decide: (a) trust exchange timestamps as-is (simple, accepts skew), (b) apply NTP-style offset correction per source, or (c) use arrival time at our ingestion layer. Option (a) is likely sufficient for bar construction at minute+ granularity.

**7. Bar `metadata` JSONB column: what goes where.**
The `metadata` column stores bar-type-specific information that doesn't apply to all bars. Current plan:

| Field | Column or metadata? | Rationale |
|---|---|---|
| OHLCV, VWAP, tick_count | Promoted columns | Universal, queried frequently |
| EWMA expected imbalance | metadata | Only for imbalance/run bars |
| EWMA window size | metadata | Configuration, not data |
| Threshold that triggered emission | metadata | Useful for analysis but not queried in SQL |
| Cumulative imbalance at emission | metadata | Diagnostic, imbalance bars only |
| Run length at emission | metadata | Diagnostic, run bars only |

**Promotion rule:** If we find ourselves writing `WHERE metadata->>'field' = ...` in production queries, that field should be promoted to a column. For v1, the current schema is sufficient. The `UNIQUE (bar_type, source, pair, time_start)` constraint covers all query patterns we need.

**8. `--ewma-window` CLI: single window per bar builder instance.**
Each `arcana bars build --type tib --ewma-window 15` invocation creates one bar builder with one EWMA window. To build TIBs at multiple EWMA windows simultaneously, run multiple commands (or a future `arcana.toml` config that specifies a list). This keeps the CLI simple and the bar builder stateless with respect to window configuration. The "3, 5, 15, 30, 60 (configurable)" in the design decisions table refers to the recommended set of windows for research, not a requirement that they all run simultaneously.

**9. Logging: structured, level-based, configurable.**
The pipeline uses Python's `logging` module with a configured format: `%(asctime)s [%(levelname)s] %(name)s: %(message)s`. Log levels:
- `INFO`: Window progress, trade counts, ETAs, bar emission summaries
- `DEBUG`: API request/response details, pagination steps, accumulator state
- `WARNING`: Rate limit hits, no-progress pagination stops, gap detection
- `ERROR`: API failures after retry exhaustion, DB connection errors

For daemon mode, logs go to stderr by default. A `--log-level` CLI flag controls verbosity. Structured JSON logging (for production log aggregation) is deferred to v2.

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

**Implementation detail:** Trade sign is `+1` (buy) or `-1` (sell). When the exchange provides the taker side (Coinbase, Binance), use it directly via `Trade.sign()`. For sources without side information, fall back to the tick rule: if price > previous price → buy (+1); if price < previous price → sell (-1); if equal → carry forward previous sign. See Design Decision #4.

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
│       ├── cli.py                    # Click CLI entry point (464 lines)
│       ├── config.py                 # DatabaseConfig, ArcanaConfig (27 lines)
│       ├── pipeline.py              # ingest_backfill, run_daemon, GracefulShutdown (242 lines)
│       ├── swarm.py                 # Parallel backfill: split_range, compose gen, validation (274 lines)
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
│   ├── test_swarm.py                # 24 tests (split_range, compose gen, CLI)
│   ├── test_ingestion/
│   │   ├── test_coinbase.py         # 16 tests (fetch, pagination, retry)
│   │   └── test_models.py           # 6 tests
│   └── test_bars/
│       ├── test_base.py             # 6 tests (Accumulator, Bar model)
│       └── test_standard.py         # 22 tests (all 4 bar types)
│
├── Dockerfile                       # Python 3.11-slim, pip install, ENTRYPOINT arcana
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
| `psycopg[binary]` | PostgreSQL/TimescaleDB driver (psycopg 3) |
| `httpx` | HTTP client for Coinbase REST API |
| `click` | CLI framework |
| `pydantic` | Configuration validation, data models |
| `pyyaml` | Docker Compose YAML generation (swarm module) |

### Optional (`pip install arcana[analysis]`)
| Package | Purpose |
|---|---|
| `pandas` | DataFrames for bar export, feature engineering |
| `numpy` | Numerical computation (EWMA, statistics) |

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
    side         TEXT          NOT NULL,    -- 'buy', 'sell', or 'unknown'
    UNIQUE (source, trade_id, timestamp)
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

# Parallel backfill via Docker
arcana swarm launch ETH-USD --since 2022-01-01 --workers 24   # generate compose file
arcana swarm launch ETH-USD --since 2022-01-01 --workers 24 --up  # generate + start
arcana swarm status ETH-USD --password arcana     # per-month trade counts
arcana swarm validate ETH-USD --since 2022-01-01 --password arcana  # gap detection
arcana swarm stop                                 # tear down containers
arcana swarm stop --remove-volumes                # tear down + delete data
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
  - Backfills raw trades from `--since` date to present via forward 15-minute window walk
  - Backward sequential pagination: pages backward through each window to capture all trades (O(N/300) API calls)
  - Writes to `raw_trades` table in batches of 1000
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
- [x] Tests for ingestion (fetch, backward pagination, dedup, retry — 16 tests)
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
| `limit` | int | Yes | Number of trades to return (max 1000; 2500+ returns 500 error) |
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
Walk forward through time in 15-minute windows:
```
Window 1: start=Jan 1 00:00, end=Jan 1 00:15 → fetch all trades
Window 2: start=Jan 1 00:15, end=Jan 1 00:30 → fetch all trades
...
Window N: start=today 13:00, end=now → done
```
Each batch of trades is committed to the DB in groups of 1000. On crash, resume from `MAX(timestamp)`.

**Backward sequential pagination:** The API returns at most 1000 trades per request (newest first). For busy windows (1000+ trades per 15 minutes), `fetch_all_trades()` pages backward from `end`:

```
fetch_all_trades(14:00, 14:15)       # ~2500 trades in this window
  Page 1: fetch(14:00, 14:15) → 1000 trades (14:10–14:15)
  Page 2: fetch(14:00, 14:10) → 1000 trades (14:05–14:10)
  Page 3: fetch(14:00, 14:05) → 500 trades  (14:00–14:05) ← under limit, done
  → merge & dedup by trade_id → 2500 trades, sorted ascending
```

Each page shifts `current_end` to the earliest timestamp seen, walking backward until a page returns fewer than 1000 trades (meaning we've captured everything down to `start`). This is O(N/1000) API calls — the theoretical minimum, with no wasted probe calls.

**Note:** The undocumented API limit ceiling is 1000. Values of 2500+ return a 500 Internal Server Error. This was empirically determined via `scripts/test_api_limit.py`.

**Rate limiting:** 0.12s delay between API calls (~8 req/s, under the 10 req/s public limit). A 15-minute window with ~6000 trades needs ~6 API calls × 0.12s = ~0.7s per window.

**Daemon mode** (`arcana run ETH-USD`):
```
Every 15 minutes:
  start = MAX(timestamp) from raw_trades WHERE pair = 'ETH-USD'
  end   = now
  Fetch trades → store → build bars
```

### Parallel Backfill via Docker Swarm

**The problem:** Single-process backfill is bottlenecked by the Coinbase rate limit (10 req/s per IP). A 4-year backfill of ETH-USD takes ~15 days serially. The data is time-partitioned and writes are idempotent (upsert), so parallel ingestion across non-overlapping time ranges is safe by construction.

**The approach:** Run N Docker containers, each responsible for a distinct month of historical data. Each container runs the existing `arcana ingest` command with `--since` and a computed `--until` flag. Since the Coinbase public rate limit is **per IP**, each container on a Docker network gets its own rate limit budget.

```
┌──────────────────────────────────────────────────────────┐
│                  Docker Compose / Swarm                    │
│                                                          │
│  ┌─────────────┐  ┌─────────────┐       ┌─────────────┐  │
│  │ worker-1    │  │ worker-2    │  ...  │ worker-N    │  │
│  │ 2022-01     │  │ 2022-02     │       │ 2026-01     │  │
│  │ arcana      │  │ arcana      │       │ arcana      │  │
│  │  ingest     │  │  ingest     │       │  ingest     │  │
│  │  --since    │  │  --since    │       │  --since    │  │
│  │  --until    │  │  --until    │       │  --until    │  │
│  └──────┬──────┘  └──────┬──────┘       └──────┬──────┘  │
│         │                │                      │         │
│         └────────────────┼──────────────────────┘         │
│                          ▼                                │
│              ┌─────────────────────┐                      │
│              │    TimescaleDB      │                      │
│              │    (shared)         │                      │
│              │                     │                      │
│              │  raw_trades table   │                      │
│              │  UNIQUE(source,     │                      │
│              │    trade_id)        │                      │
│              │  ON CONFLICT        │                      │
│              │    DO NOTHING       │                      │
│              └─────────────────────┘                      │
└──────────────────────────────────────────────────────────┘
```

**Why this is safe:**

1. **No write conflicts.** Each worker ingests a disjoint time range. Even if ranges overlap slightly at boundaries, the `UNIQUE(source, trade_id)` constraint with `ON CONFLICT DO NOTHING` makes duplicate writes harmless.
2. **No read coordination.** Workers don't need to know about each other. Each one runs the standard `ingest_backfill()` flow with its own `since`/`until` range.
3. **Resumable per worker.** If a container crashes, restart it — it resumes from `MAX(timestamp)` within its assigned range, same as single-process mode.
4. **Database handles concurrency.** PostgreSQL/TimescaleDB is designed for concurrent writers. The hypertable partitions by timestamp, so workers writing to different time ranges hit different chunks with minimal lock contention.

**Required code change:** Add `--until` flag to `arcana ingest` so workers can be bounded:
```bash
# Worker for January 2023
arcana ingest ETH-USD --since 2023-01-01 --until 2023-02-01

# Worker for February 2023
arcana ingest ETH-USD --since 2023-02-01 --until 2023-03-01
```

Currently `ingest_backfill()` always runs to `datetime.now()`. The `--until` flag would cap the end time, allowing the worker to exit when its range is complete.

**Estimated speedup:**

| Workers | Rate (aggregate) | 4-year ETH-USD | Wall clock |
|---|---|---|---|
| 1 (current) | ~8 req/s | ~525M trades | ~15 days |
| 6 | ~48 req/s | same | ~2.5 days |
| 12 | ~96 req/s | same | ~1.3 days |
| 24 | ~192 req/s | same | ~16 hours |
| 48 (1 per month) | ~384 req/s | same | ~8 hours |

Diminishing returns above ~24 workers because DB write throughput and container overhead become factors. The sweet spot is likely **12–24 workers** for a 4-year backfill, finishing in **1–2 days**.

**Docker Compose sketch:**
```yaml
services:
  db:
    image: timescale/timescaledb:latest-pg16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: arcana
      POSTGRES_USER: arcana
      POSTGRES_PASSWORD: arcana
    volumes:
      - arcana_data:/var/lib/postgresql/data

  worker-2022-01:
    image: arcana:latest
    command: arcana ingest ETH-USD --since 2022-01-01 --until 2022-02-01
    depends_on: [db]
    environment:
      ARCANA_DB_HOST: db

  worker-2022-02:
    image: arcana:latest
    command: arcana ingest ETH-USD --since 2022-02-01 --until 2022-03-01
    depends_on: [db]
    environment:
      ARCANA_DB_HOST: db

  # ... one service per month, or generate with a script

volumes:
  arcana_data:
```

In practice, `arcana swarm launch` generates the full compose file for any date range, automatically splitting into equal-duration worker chunks. The Dockerfile is straightforward — `pip install .` into a Python 3.11 image.

### How to Run the Swarm

**Prerequisites:**
- Docker Desktop (or Docker Engine + Docker Compose plugin) installed and running
- The Arcana repository cloned locally
- No other service using port 5432 (the swarm spins up its own TimescaleDB)

**Step 1 — Build the Docker image:**
```bash
docker build -t arcana:latest .
```
This packages the Arcana source code into a container image. Re-run this step after any code changes.

**Step 2 — Generate the compose file and review the plan:**
```bash
arcana swarm launch ETH-USD --since 2022-01-01 --until 2024-01-01 --workers 24
```
This prints a worker assignment table (which worker covers which date range) and writes `docker-compose.swarm.yml`. It does **not** start containers yet — review the plan first.

Key options:
| Flag | Default | Description |
|---|---|---|
| `--workers` | 12 | Number of parallel containers |
| `--until` | now | End of backfill range |
| `--output` | `docker-compose.swarm.yml` | Output file path |
| `--image` | `arcana:latest` | Docker image for workers |
| `--password` | `arcana` | Database password |
| `--up` | off | Auto-start containers after generating |

**Step 3 — Start the swarm:**
```bash
docker compose -f docker-compose.swarm.yml up -d
```
Or combine steps 2 and 3 with `--up`:
```bash
arcana swarm launch ETH-USD --since 2022-01-01 --until 2024-01-01 --workers 24 --up
```

**Step 4 — Monitor progress:**
```bash
# Per-month trade counts from the swarm's database
arcana swarm status ETH-USD --password arcana

# Watch container health and restarts
docker compose -f docker-compose.swarm.yml ps

# Stream worker logs (all workers)
docker compose -f docker-compose.swarm.yml logs -f

# Stream logs for a single worker
docker compose -f docker-compose.swarm.yml logs -f worker-00-20220101-20220131
```

**Step 5 — Validate coverage after completion:**
```bash
arcana swarm validate ETH-USD --since 2022-01-01 --until 2024-01-01 --password arcana
```
This scans the database for gaps (days with missing trades) and reports them. A clean run shows "No gaps detected. Coverage is complete."

**Step 6 — Stop the swarm:**
```bash
# Stop containers, keep the data volume
arcana swarm stop

# Stop containers AND delete the data volume
arcana swarm stop --remove-volumes
```

**Troubleshooting:**

| Symptom | Cause | Fix |
|---|---|---|
| `pull access denied for arcana` | Image not built locally | Run `docker build -t arcana:latest .` |
| `swarm status` shows 0 trades | Password mismatch (default for `swarm status` vs compose DB) | Pass `--password arcana` to status/validate commands |
| Worker stuck in restart loop | API rate limit or DB connection failure | Check logs: `docker compose -f docker-compose.swarm.yml logs worker-XX-...` |
| Port 5432 already in use | Local PostgreSQL is running | Stop it (`brew services stop postgresql` / `sudo systemctl stop postgresql`) or use `--port 5433` |
| Workers exit immediately | `--since` after `--until` or invalid pair | Check compose file command args |

**Storage estimate for 4-year backfill:**

| | Value |
|---|---|
| Estimated total trades | ~525 million |
| Raw storage (uncompressed) | ~80 GB |
| With TimescaleDB compression | ~15–20 GB |
| Indexes | ~30 GB uncompressed |
| **Total disk needed** | **~120 GB uncompressed, ~40 GB compressed** |

TimescaleDB compression should be enabled on the `raw_trades` hypertable for chunks older than 7 days:
```sql
ALTER TABLE raw_trades SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source, pair',
  timescaledb.compress_orderby = 'timestamp'
);
SELECT add_compression_policy('raw_trades', INTERVAL '7 days');
```

**Implementation checklist:**
- [x] Add `--until` flag to `arcana ingest` CLI and `ingest_backfill()`
- [x] Add `ARCANA_DB_HOST` / `ARCANA_DB_*` environment variable support to CLI
- [x] Create `Dockerfile`
- [x] `arcana swarm launch` — generates docker-compose.yml with N workers for any date range
- [x] `arcana swarm status` — per-month trade count from the swarm DB
- [x] `arcana swarm validate` — gap detection across the ingested range
- [x] `arcana swarm stop` — tears down containers (optionally removes volumes)
- [x] Worker restart policy (on-failure, max 5 attempts, 30s delay)
- [x] DB healthcheck — workers wait for TimescaleDB to be ready before starting
- [x] 24 tests covering split_range, compose generation, CLI commands
- [ ] Add TimescaleDB compression policy to `init_schema()`
- [ ] Document the parallel backfill workflow in README

---

## Guiding Principles

1. **Correctness over speed.** Prado's bar construction math must be exact. Every bar type gets tested against hand-computed examples.
2. **Pluggable data sources.** The `DataSource` ABC means adding Binance or Uniswap later is just a new class, no refactoring.
3. **Stateful bar builders.** Bar construction is inherently stateful (accumulators carry across batches). The `BarBuilder` class manages this explicitly.
4. **No premature optimization.** Python + pandas is fast enough for 15-minute batch intervals. Optimize only when profiling shows a bottleneck.
5. **Minimal dependencies.** Every dependency must earn its place.
