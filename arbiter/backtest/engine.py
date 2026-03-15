"""
Single-asset daily backtest engine.

Fill semantics: market order at close of bar T fills at open of bar T+1.
The engine is strategy-agnostic; it delegates signal generation to a StrategyFn
looked up from the registry (or passed directly as a callable).
"""

from __future__ import annotations

import json
import duckdb
import pandas as pd
from pathlib import Path

from arbiter.backtest.fills import cost_per_share_buy, shares_bought, proceeds_per_share_sell
from arbiter.backtest.metrics import compute_metrics
from arbiter.backtest.strategies import StrategyFn, get_strategy


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
    strategy_fn: StrategyFn,
    fees_bps: float,
    slippage_bps: float,
    initial_cash: float,
) -> tuple[float, pd.Series, int]:
    """
    Core backtest loop. Calls strategy_fn(df) to get a signal Series, then
    simulates fills at the next bar's open.

    Signal values:
        +1 → enter long  (if not already in position)
        -1 → exit long   (if currently in position)
         0 → hold / no action

    Signals at the final bar are ignored (no bar i+1 to fill into).

    Returns:
        (equity_final, equity_curve, trading_days)
    """
    open_ = df["open"].values
    close = df["close"].values
    n = len(df)

    signals = strategy_fn(df)

    cash = initial_cash
    position = 0.0
    equity_curve = [initial_cash]  # equity at bar 0 (pre-fill)

    for i in range(n - 1):
        sig = int(signals.iloc[i])
        next_open = open_[i + 1]

        if sig == 1 and position == 0.0:
            # Enter long: fill at open of bar i+1
            cost = cost_per_share_buy(next_open, fees_bps, slippage_bps)
            if cost > 0:
                position = shares_bought(cash, cost)
                cash = 0.0

        elif sig == -1 and position > 0.0:
            # Exit long: fill at open of bar i+1
            proceeds = proceeds_per_share_sell(next_open, fees_bps, slippage_bps)
            cash = position * proceeds
            position = 0.0

        equity_curve.append(cash + position * close[i + 1])

    equity_final = cash + position * close[-1]
    trading_days = max(n - 1, 1)
    return equity_final, pd.Series(equity_curve), trading_days


def _resolve_strategy(strategy: str | StrategyFn) -> StrategyFn:
    """Accept either a strategy name (str) or a callable directly."""
    if callable(strategy):
        return strategy
    return get_strategy(strategy)


def run_backtest(
    data_dir: str | Path,
    symbol: str,
    timeframe: str = "1d",
    strategy: str | StrategyFn = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    initial_cash: float = 1_000_000.0,
) -> dict:
    """
    Run backtest from partitioned Parquet data.

    Args:
        data_dir:     Root data directory (contains <symbol>/<timeframe>/... partitions).
        symbol:       Ticker symbol.
        timeframe:    Bar timeframe (default "1d").
        strategy:     Strategy name (str) from the registry, or a StrategyFn callable.
        fees_bps:     Round-trip commission in basis points.
        slippage_bps: Slippage per fill in basis points.
        initial_cash: Starting portfolio cash.

    Returns:
        dict with equity_final, cagr, volatility, max_drawdown, sharpe.
    """
    df = load_ohlcv_parquet(data_dir, symbol, timeframe)
    return run_backtest_from_df(
        df,
        strategy=strategy,
        fees_bps=fees_bps,
        slippage_bps=slippage_bps,
        initial_cash=initial_cash,
    )


def run_backtest_from_df(
    df: pd.DataFrame,
    strategy: str | StrategyFn = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
    initial_cash: float = 1_000_000.0,
) -> dict:
    """
    Run backtest on an in-memory OHLCV DataFrame (useful for tests).

    Args:
        df:           OHLCV DataFrame with columns [timestamp_utc, open, high, low, close, volume].
        strategy:     Strategy name (str) or StrategyFn callable.
        fees_bps:     Commission in basis points.
        slippage_bps: Slippage in basis points.
        initial_cash: Starting cash.

    Returns:
        dict with equity_final, cagr, volatility, max_drawdown, sharpe.
    """
    _empty = {
        "equity_final": float(initial_cash),
        "cagr": 0.0,
        "volatility": 0.0,
        "max_drawdown": 0.0,
        "sharpe": 0.0,
    }
    if df.empty or len(df) < 2:
        return _empty

    strategy_fn = _resolve_strategy(strategy)
    equity_final, equity_curve, trading_days = _run_backtest_core(
        df, strategy_fn, fees_bps, slippage_bps, initial_cash
    )
    m = compute_metrics(equity_curve, initial_cash, trading_days)

    return {
        "equity_final": round(equity_final, 2),
        "cagr": round(m["cagr"], 6),
        "volatility": round(m["volatility"], 6),
        "max_drawdown": round(m["max_drawdown"], 6),
        "sharpe": round(m["sharpe"], 4),
    }


def backtest_json(
    data_dir: str | Path,
    symbol: str,
    timeframe: str = "1d",
    strategy: str | StrategyFn = "buy_and_hold",
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> str:
    """Run backtest and return JSON string."""
    result = run_backtest(
        data_dir=data_dir,
        symbol=symbol,
        timeframe=timeframe,
        strategy=strategy,
        fees_bps=fees_bps,
        slippage_bps=slippage_bps,
    )
    return json.dumps(result, indent=2)
