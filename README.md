# Arbiter

Deterministic OHLCV ingest, DuckDB/Parquet store, and daily backtester.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## CLI (vertical slice)

### Ingest

Load a CSV into partitioned Parquet. Prints `rows_ingested` and `rows_written`.

```bash
python -m arbiter.cli ingest --symbol TEST --timeframe 1d --csv tests/fixtures/sample_ohlcv.csv --out data/
```

**Expected output:** `rows_ingested=<N> rows_written=<N>`

### Query

Run DuckDB SQL (e.g. over Parquet). For a single-cell result (e.g. `SELECT count(*)`), prints `count = <value>`.

```bash
python -m arbiter.cli query --sql "SELECT count(*) FROM read_parquet('data/TEST/1d/**/*.parquet');"
```

**Expected output:** `count = <expected rowcount>`

### Backtest

Run backtest with buy-and-hold (and optional fees/slippage). Prints JSON with metrics.

```bash
python -m arbiter.cli backtest --symbol TEST --timeframe 1d --strategy buy_and_hold --fees_bps 10 --slippage_bps 5
```

**Expected output (JSON):** `equity_final`, `cagr`, `max_drawdown`, `sharpe`

## API (FastAPI)

Run with `uvicorn arbiter.api.main:app --reload`. Data directory is set via `ARBITER_DATA_DIR` (default: `data/`).

- **GET /ohlcv?symbol=...&start=...&end=...** — OHLCV bars for symbol in date range (start/end optional, ISO dates).
- **POST /backtest** — Body: `{ "symbol", "timeframe?", "strategy?", "fees_bps?", "slippage_bps?" }`. Returns `{ equity_final, cagr, max_drawdown, sharpe }`.

## Directory layout

- `arbiter/` — ingest, query, backtest, api
- `data/` — partitioned Parquet (data/\<symbol\>/\<timeframe\>/year=YYYY/month=MM/)
- `tests/` — tests and fixtures
