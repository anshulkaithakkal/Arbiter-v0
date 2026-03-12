# Arbiter

Arbiter is a deterministic pipeline for ingesting OHLCV data, storing it in a queryable Parquet layout, and running single-asset daily backtests with configurable fees and slippage.

**What it does**

- **Ingest** — You feed it CSV OHLCV. It normalizes to a canonical schema in UTC, deduplicates by timestamp, and writes partitioned Parquet so you can query by symbol and date range.
- **Store** — Data lives under a simple partition structure: symbol, timeframe (e.g. 1d), then year and month. DuckDB can read these Parquet files directly for fast, SQL-style queries.
- **Backtest** — Single-asset, daily bars only. Market orders are filled at the next bar’s open (no lookahead). You can set fees and slippage in basis points. Outputs standard metrics: final equity, CAGR, max drawdown, and Sharpe ratio.
- **API** — A small FastAPI app exposes OHLCV query by symbol and date range, and a backtest endpoint that accepts symbol, timeframe, strategy, and cost parameters and returns the same metrics as JSON.

**What you get**

Deterministic behavior and clear execution rules: one canonical OHLCV schema, one fill rule (next open), and metrics computed from the same equity curve so results are reproducible. The codebase is organized into ingest, query, backtest, and API layers, with tests and fixtures so you can validate round-trips and backtest logic without large datasets.
