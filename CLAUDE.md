# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Ingest CSV → partitioned Parquet
python -m arbiter.cli ingest --symbol AAPL --timeframe 1d --csv PATH --out data/

# Run DuckDB SQL query
python -m arbiter.cli query --sql "SELECT count(*) FROM read_parquet('data/AAPL/1d/**/*.parquet');"

# Run backtest
python -m arbiter.cli backtest --symbol AAPL --timeframe 1d --strategy sma_crossover_20_50 [--fees_bps 10] [--slippage_bps 5] [--data-dir data]
# Registered strategies: buy_and_hold (default), sma_crossover_20_50

# List strategies
python -m arbiter.cli strategies

# FastAPI server (ARBITER_DATA_DIR env var overrides default "data/" directory)
uvicorn arbiter.api.main:app --reload

# Tests
pytest                                          # all tests
pytest tests/test_backtest_golden.py            # single file
pytest tests/test_strategies.py::test_name     # single test

# Lint / format
ruff check .
ruff format .
```

## Architecture

### Data Flow

CSV → `ingest_csv.py` (column alias normalization) → `schema.py` (dtype coercion, UTC, dedup, sort) → `parquet_store.py` → Hive-partitioned Parquet at `data/<symbol>/<timeframe>/year=YYYY/month=MM/data.parquet` → DuckDB via glob queries.

### Backtest Engine Mechanics

**No-lookahead guarantee**: signals generated at bar `i` fill at bar `i+1`'s open. The last bar is always skipped (no next bar). Tests in `test_backtest_golden.py` explicitly verify against a known lookahead equity value to catch regressions.

**Fill model** (`fills.py`): buy cost = `open × (1 + slippage_bps/10000) × (1 + fees_bps/10000)`; sell proceeds = `open × (1 - slippage_bps/10000) × (1 - fees_bps/10000)`.

**Signals**: strategy returns an integer Series (`+1` = buy, `-1` = sell, `0` = hold). Position is binary (fully in or fully out); fractional shares not supported.

**Metrics** (`metrics.py`): CAGR, max drawdown, Sharpe ratio, annualized volatility — all annualized to 252 trading days. Flat equity curves return 0 volatility (guarded against division by zero).

### Strategy Interface

Strategies are callables: `(df: pd.DataFrame) -> pd.Series[int]`. Input is the full OHLCV DataFrame; output is an integer signal Series indexed identically. Register with:

```python
from arbiter.backtest.strategies import register
register("my_strategy", my_strategy)
```

Use the factory pattern for parameterized strategies (see `make_sma_crossover`); set `strategy.__name__` so it appears correctly in the registry.

### API Layer

`main.py` sets `app.state.data_dir` from `ARBITER_DATA_DIR` env var. Routes in `routes.py`:
- `GET /strategies` — list registry names
- `GET /ohlcv` — query params: `symbol` (required), `timeframe`, `start`, `end`
- `POST /backtest` — body: `symbol` (required), `timeframe`, `strategy`, `fees_bps`, `slippage_bps`; returns `{equity_final, cagr, max_drawdown, sharpe, volatility}`

### Test Structure

| File | What it covers |
|---|---|
| `test_strategies.py` | Strategy registry, signal correctness, engine integration, edge cases |
| `test_backtest_golden.py` | Deterministic equity (golden value), fees/slippage impact, no-lookahead regression |
| `test_api.py` | All HTTP endpoints via FastAPI test client |
| `test_ingest.py` | Schema validation, deduplication, column alias mapping |

Tests use temp directories and a small `sample_ohlcv.csv` fixture — no large datasets.

## Rules
- Run `pytest -q` and `ruff check .` before saying a task is done.
- Backtests must stay deterministic: no lookahead, explicit fills, fees and slippage always applied.
- Prefer type hints in Python.
- No external APIs or network calls in core logic (ingest, query, backtest).
- Small diffs. Don't refactor unrelated code.
- Use conventional commits.
- Dev machine is Windows/PowerShell: activate with `.venv\Scripts\Activate.ps1`.