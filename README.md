# Arbiter

Arbiter is a **deterministic market data + backtesting engine** that helps you **evaluate trading strategies without lying to yourself** (no lookahead, consistent timestamps, explicit fill rules, fees + slippage).

It ingests OHLCV data, normalizes it into a canonical UTC schema, stores it as partitioned Parquet, queries via DuckDB, and produces equity curves + risk metrics from backtests.

---

## Demo (the "prove it works" section)

### One command demo (after setup)
```bash
python -m arbiter.demo --symbol AAPL --timeframe 1d --strategy sma_cross
```

---

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
