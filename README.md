# Arbiter

Deterministic OHLCV ingest, DuckDB/Parquet store, and daily backtester.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## CLI

- **Ingest:** `python -m arbiter.cli ingest --symbol SYMBOL --timeframe 1d --csv PATH --out DATA_DIR`
- **Query:** `python -m arbiter.cli query --sql "SELECT ... FROM read_parquet('data/SYMBOL/1d/**/*.parquet');"`
- **Backtest:** `python -m arbiter.cli backtest --symbol SYMBOL --timeframe 1d --strategy buy_and_hold [--fees_bps N] [--slippage_bps N]`

## Directory layout

- `arbiter/` — ingest, query, backtest, api
- `data/` — partitioned Parquet: `data/<symbol>/<timeframe>/year=YYYY/month=MM/*.parquet`
- `tests/` — tests and fixtures
