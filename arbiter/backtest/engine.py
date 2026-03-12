"""Single-asset daily backtest: next-open fills, fees/slippage, buy_and_hold strategy."""

from __future__ import annotations

import json
import duckdb
import pandas as pd
from pathlib import Path

from arbiter.backtest.fills import cost_per_share_buy, shares_bought
from arbiter.backtest.metrics import compute_metrics


def load_ohlcv_parquet(data_dir: str | Path, symbol: str, timeframe: str = "1d") -> pd.DataFrame:
    """Load OHLCV from partitioned Parquet via DuckDB read_parquet."""
    base = Path(data_dir) / symbol / timeframe
    glob = str(base / "**" / "*.parquet")
    conn = duckdb.connect(":memory:")
    try:
        df = conn.execute("SELECT * FROM read_parquet(?)", [glob]).fetchdf()
        if df.empty:
            return df
        if "timestamp_utc" in df.columns:
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        df = df.sort_values("timestamp_utc").reset_index(drop=True)
        return df
    finally:
        conn.close()


def _run_backtest_core(
    df: pd.DataFrame,
    strategy: str,
    fees_bps: float,
    slippage_bps: float,
    initial_cash: float,
) -> tuple[float, pd.Series, int]:
    """
    Core backtest on OHLCV DataFrame. Returns (equity_final, equity_curve, trading_days).
    Market order at close of bar T fills at open of bar T+1.
    """
    open_ = df["open"].values
    close = df["close"].values
    n = len(df)

    cash = initial_cash
    position = 0.0
    equity_curve = [initial_cash]

    if strategy == "buy_and_hold" and n >= 2:
        # Fill at open of bar 1 (next bar after signal at close of bar 0)
        cost = cost_per_share_buy(open_[1], fees_bps, slippage_bps)
        if cost > 0:
            position = shares_bought(cash, cost)
            cash = 0.0

    for i in range(1, n):
        eq = cash + position * close[i]
        equity_curve.append(eq)

    equity_final = cash + position * close[-1]
    equity_curve = pd.Series(equity_curve)

    trading_days = max(n - 1, 1)
    return equity_final, equity_curve, trading_days


def run_backtest(
    data_dir: str | Path,
    symbol: str,
    timeframe: str = "1d",
    strategy: str = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    initial_cash: float = 1_000_000.0,
) -> dict:
    """
    Run backtest; market orders fill at next bar open. Returns dict with
    equity_final, cagr, max_drawdown, sharpe.
    """
    df = load_ohlcv_parquet(data_dir, symbol, timeframe)
    return run_backtest_from_df(
        df, strategy=strategy, fees_bps=fees_bps, slippage_bps=slippage_bps, initial_cash=initial_cash
    )


def run_backtest_from_df(
    df: pd.DataFrame,
    strategy: str = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    initial_cash: float = 1_000_000.0,
) -> dict:
    """
    Run backtest on an OHLCV DataFrame (e.g. for tests). Same semantics as run_backtest.
    Returns dict with equity_final, cagr, max_drawdown, sharpe.
    """
    if df.empty or len(df) < 2:
        return {
            "equity_final": float(initial_cash),
            "cagr": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
        }

    equity_final, equity_curve, trading_days = _run_backtest_core(
        df, strategy, fees_bps, slippage_bps, initial_cash
    )
    m = compute_metrics(equity_curve, initial_cash, trading_days)

    return {
        "equity_final": round(equity_final, 2),
        "cagr": round(m["cagr"], 6),
        "max_drawdown": round(m["max_drawdown"], 6),
        "sharpe": round(m["sharpe"], 4),
    }


def backtest_json(
    data_dir: str | Path,
    symbol: str,
    timeframe: str = "1d",
    strategy: str = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> str:
    """Run backtest and return JSON string with equity_final, cagr, max_drawdown, sharpe."""
    result = run_backtest(
        data_dir=data_dir,
        symbol=symbol,
        timeframe=timeframe,
        strategy=strategy,
        fees_bps=fees_bps,
        slippage_bps=slippage_bps,
    )
    return json.dumps(result, indent=2)
