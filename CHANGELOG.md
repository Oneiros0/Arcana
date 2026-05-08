# Changelog

All notable changes to Arcana are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0 caveat: minor versions may include breaking changes; the version
header will call them out explicitly.

## [Unreleased]

## [0.5.0] — 2026-04-29

### Added
- **Historical candle backfill** via `arcana backfill-candles <PAIR>`.
  One-shot, resumable, exits when the requested range is filled. Supports
  granularities `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d`.
- **Candle synthesis module** (`src/arcana/ingestion/candles.py`). Each
  OHLCV bucket becomes a single synthetic `Trade` priced at HLC/3
  (typical price), sized at the candle's volume, side `'unknown'`,
  timestamped at the candle's start. Deliberately **not** fanned out
  to four O/H/L/C trades — fictional inter-tick timestamps would mislead
  tick/imbalance/run bar builders.
- **`data_quality` column** on `raw_trades` (default `'tick'`; candle
  rows tagged `'candle_1m'`, `'candle_5m'`, etc.) so downstream models
  can split on provenance and never train across the tick/candle boundary.
- **Composite index** `(pair, data_quality, timestamp)` so resume
  watermarks for candle backfills are not poisoned by tick rows that
  arrived after them.
- `Database.get_last_timestamp` accepts `data_quality=` to scope the
  watermark to a specific provenance class.
- 270 lines of new tests covering candle parsing, synthesis math,
  granularity helpers, and backfill orchestration.

### Changed
- `UPSERT_TRADES` now writes the `data_quality` column. Existing
  callers default to `'tick'`.
- `CoinbaseSource` gained `fetch_candles` and `fetch_all_candles`
  (paginated by 350 buckets per call, with rate-limit-aware sleeps
  between chunks).

### Migrations
- Forward-only `RAW_TRADES_MIGRATIONS` block runs on `init_schema()`
  and adds `data_quality` + the new index to pre-existing databases.
  No manual migration step.

## [0.4.0] — 2026-03-10 — **Breaking: Ingestion-Only Refactor**

### Removed
- Entire `src/arcana/bars/` package (`base`, `standard`, `imbalance`,
  `runs`, `utils`) and the matching `tests/test_bars/` tree.
  ~5,700 lines deleted.
- All bar-related CLI subcommands (`build`, `build-all`).
- `arcana.toml`'s `[[bars]]` configuration sections.

### Changed
- Project thesis narrowed: Arcana is now an ingestion layer only.
  Bar construction and modeling decisions move downstream to the
  consumer.
- `pipeline.py` slimmed from ~700 to ~140 lines. CLI from ~600 to ~200.

### Notes
- The 68 mathematical-property tests for bar invariants
  (`test_mathematical_proofs.py`) were retired with this refactor.
  The methodology lives on; the implementation does not.

## [0.3.0] — 2026-02-26

### Added
- **Mathematical proofs as tests**: 68 property tests across 12 classes
  in `tests/test_bars/test_mathematical_proofs.py`, verifying E[T]
  convergence, EWMA decay behavior, and imbalance-bar first-passage
  invariants.
- **E[T] clamping** for info-bar stability
  (`expected_ticks_constraints = [lo, hi]` per `[[bars]]` entry in
  `arcana.toml`). Borrowed from mlfinlab's `exp_num_ticks_constraints`.
- [docs/info-bar-stability.md](docs/info-bar-stability.md) — full writeup of the
  quadratic-random-walk diagnosis and the clamping fix.

### Changed
- EWMA window standardized to 20 across all info bars (~4-day lookback
  at typical bar rates).
- Imbalance bars seeded at `bars_per_day=300`, run bars at 150, to
  account for the quadratic penalty in info-bar generation.

### Fixed
- Unstable equilibrium at `E[T]=100` in balanced markets (first-passage
  time of a quadratic random walk is h²; without a clamp, E[T] drifts
  unboundedly).

## [0.2.0] — 2026-02-19 — **Breaking: Multi-Table Schema**

### Changed
- Bar storage moved from a single `bars` table to per-pair-per-type
  tables: `bars_{bar_type}_{pair_norm}`
  (e.g., `bars_tick_500_eth_usd`).
- Backfill chunk size reduced from 30 days to 10 days for memory and
  resume granularity.

### Added
- `arcana.toml` for declarative bar configuration.
- `--auto` flag for Prado-recommended imbalance bar selection.
- `build-all` to construct every configured bar type in one pass.
- Better CLI error messages and clearer `--help` text.

### Removed
- Swarm ingestion feature (deferred from MVP scope).

## [0.1.0] — 2026-02-11

### Added
- Initial trading data pipeline: Coinbase Advanced Trade ingestion,
  TimescaleDB storage, daemon + bulk backfill modes.
- `raw_trades` hypertable with `(source, trade_id, timestamp)`
  uniqueness.
- Click-based CLI: `arcana ingest`, `arcana daemon`.
- Database credentials via `ARCANA_DB_*` environment variables.
- Initial test suite for ingestion, storage, and CLI.

[Unreleased]: https://github.com/Oneiros0/Arcana/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/Oneiros0/Arcana/releases/tag/v0.5.0
[0.4.0]: https://github.com/Oneiros0/Arcana/releases/tag/v0.4.0
[0.3.0]: https://github.com/Oneiros0/Arcana/releases/tag/v0.3.0
[0.2.0]: https://github.com/Oneiros0/Arcana/releases/tag/v0.2.0
[0.1.0]: https://github.com/Oneiros0/Arcana/releases/tag/v0.1.0
