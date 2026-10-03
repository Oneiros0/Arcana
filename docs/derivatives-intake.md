# Derivatives Price Intake (Coinbase BTC/ETH Futures)

Status: **Step 1 + Step 2 complete.** Discovery, relaxed pair handling, futures
trade/candle intake, and the derivatives metrics table + poller are all implemented
and tested (156 tests green, ruff clean, live-smoke verified).

## Goal

Extend Arcana's ingestion to cover Coinbase **BTC and ETH futures** so downstream
risk tooling (VaR/CVaR, Monte-Carlo, and QAE-accelerated estimation) has clean,
complete risk factors: price series, open interest, settlement price, contract
metadata, plus the existing **spot** series for basis.

## API reality (why the design looks the way it does)

Coinbase BTC/ETH futures trade on the **same public Advanced Trade `/market/`
endpoints** Arcana already uses — **no API key required for market data**.

| Endpoint (base `https://api.coinbase.com/api/v3/brokerage`) | Auth | Use |
|---|---|---|
| `GET /market/products?product_type=FUTURE&contract_expiry_type=EXPIRING` | none | discover contracts |
| `GET /market/products/{id}` | none | metrics snapshot (`future_product_details`) |
| `GET /market/products/{id}/candles` | none | OHLCV (reused) |
| `GET /market/products/{id}/ticker` | none | trades (reused) |
| `/cfm/*`, `/intx/*` (positions, balances, orders) | JWT | **not used** — account-only |

### Live universe (as of 2026-10-03)

Filtering `product_type=FUTURE` returns FCM-venue (Coinbase Derivatives, exchange
`cde`) products. For BTC/ETH (`contract_root_unit`):

- **Dated futures**: `BIT-30OCT26-CDE`, `BIT-27NOV26-CDE`, … (BTC), `ET-30OCT26-CDE`, … (ETH)
- **Perpetual-style futures**: `BIP-20DEC30-CDE` (BTC), `ETP-20DEC30-CDE` (ETH) —
  modelled as very-long-dated (2089) `EXPIRING` contracts.

**INTX perpetuals** (`BTC-PERP-INTX`, `ETH-PERP-INTX`) are **frozen/retired** on this
API as of 2026-10-01 (moved to a Deribit-powered gateway). `trading_disabled=true`,
stale funding/OI. Excluded by default.

### Field availability differs by product class

From live `future_product_details`:

| Field | Dated/perp-style FCM (live) | INTX perp (frozen) |
|---|---|---|
| `price` (last), `volume_24h` | yes | stale |
| `open_interest` | yes | stale |
| `settlement_price` | yes | empty |
| `contract_expiry`, `contract_size`, `contract_root_unit` | yes | expiry null |
| `funding_rate`, `funding_time`, `index_price` | **empty** (no funding on these) | stale |

Implication: the metrics model must treat every metric as optional (empty string →
`None`) and keep a JSONB escape hatch for venue variation. **Basis** = futures
`price` − existing **spot** index, computed downstream.

**Perpetual-style contracts carry more.** Live checks show the perpetual-style US
futures (`BIP`/`ETP`, modelled as long-dated 2089 `EXPIRING`) **do** expose
`funding_rate` and `index_price` (e.g. `ETP-20DEC30-CDE`: funding=0, index≈2685.5),
whereas the short-dated `BIT`/`ET` futures leave those empty. So `list-futures`
intentionally captures both classes — the perpetual-style ones give the funding +
index series most useful for funding/basis risk.

## Design principles

1. **Additive, non-breaking.** The spot path is behaviorally unchanged. Existing
   tests stay green; new config/CLI have safe defaults.
2. **Reuse the trade/candle pipeline.** `raw_trades.pair` is free-text, so futures
   price series land through the existing `ingest` / `backfill-candles` commands
   with a futures product id — no schema change for price data.
3. **Per-contract storage.** Each product id is its own `pair`. Continuous/rolled
   contracts are a downstream concern; we capture `contract_expiry` so downstream
   can roll.
4. **Bar construction is out of scope** (handled by Sigil). We only guarantee
   bar-table naming does not corrupt on multi-token futures ids.

## Step 1 — discovery + pair handling + price intake

- `CoinbaseSource.list_products(product_type, contract_expiry_type)` — filtered
  product listing (public endpoint). `get_supported_pairs()` default behavior
  unchanged (backwards compatible).
- `CoinbaseSource.list_futures(underlyings=("BTC","ETH"))` — returns
  `FutureInstrument` records for matching contracts (filters on
  `contract_root_unit`).
- `FutureInstrument` model (product_id, underlying, contract_expiry_type,
  contract_expiry, contract_size, display_name, venue).
- Relax `_PAIR_PATTERN` to accept multi-segment ids (`BIT-30OCT26-CDE`,
  `BTC-PERP-INTX`); still reject junk / SQL. Bar-table **discovery** reads the
  `pair` column instead of positionally parsing the table name (fixes the
  3-token roundtrip bug).
- CLI `arcana list-futures [--underlying BTC,ETH]`.
- `arcana ingest <future-id>` / `arcana backfill-candles <future-id>` documented
  for futures (already functional).

## Step 2 — derivatives metrics (done)

- `FutureMetrics` model: `observed_at, source, pair, product_type,
  contract_expiry_type, underlying, contract_expiry, contract_size, last_price,
  volume_24h, open_interest, funding_rate, funding_time, index_price,
  settlement_price, raw(JSONB)`. All metric fields optional; empty-string → None.
- `futures_metrics` hypertable on `observed_at`, `UNIQUE(source, pair,
  observed_at)`, `ON CONFLICT DO NOTHING`, idempotent migration block.
- `CoinbaseSource.fetch_future_metrics(pair)` parses `future_product_details`.
- `pipeline.poll_metrics(source, db, pairs, interval)` snapshot daemon
  (default 300s), dedup + graceful shutdown + resume.
- CLI `arcana snapshot <pair>` (one-shot) and `arcana metrics <pair...>` (daemon).
- `[derivatives]` config block: `enabled`, `underlyings`, `expiry_types`,
  `metrics_interval`.

## Testing strategy

Baseline: full suite green before and after every change (spot path must stay
behaviorally identical).

- Golden fixtures captured from the live API:
  `tests/fixtures/sample_future_product.json`,
  `tests/fixtures/sample_futures_products.json`.
- Unit tests (mocked `httpx`): `list_products` params, `list_futures` filtering,
  relaxed/validated `_PAIR_PATTERN`, bar-table discovery on multi-token ids,
  `FutureInstrument` parsing, CLI wiring.
- Step 2 adds: `FutureMetrics` parsing (empty→None, Decimal coercion), metrics
  upsert idempotency/dedup, poller resume + shutdown.
- `python -m ruff check src/ tests/ scripts/` clean; `mypy` strict.

## Non-goals

- Authenticated endpoints (positions/balances/orders).
- INTX perpetuals as a live source (frozen).
- Bar construction for futures (Sigil).
- Risk computation (VaR/CVaR/Monte-Carlo/QAE) — downstream consumers of this data.
