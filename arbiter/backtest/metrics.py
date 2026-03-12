"""Backtest metrics: CAGR, volatility, max drawdown, Sharpe."""

from __future__ import annotations

import pandas as pd


def compute_cagr(equity_curve: pd.Series, initial_cash: float, trading_days: int) -> float:
    """CAGR = (equity_final / initial_cash)^(252 / trading_days) - 1."""
    if initial_cash <= 0 or trading_days <= 0:
        return 0.0
    equity_final = equity_curve.iloc[-1]
    years = trading_days / 252.0
    if years <= 0:
        return 0.0
    return (equity_final / initial_cash) ** (1 / years) - 1.0


def compute_max_drawdown(equity_curve: pd.Series) -> float:
    """Max drawdown = max (peak - trough) / peak over the curve."""
    cummax = equity_curve.cummax()
    drawdowns = (equity_curve - cummax) / cummax.replace(0, 1)
    return float(abs(drawdowns.min()))


def compute_sharpe(equity_curve: pd.Series, annualize: bool = True) -> float:
    """Sharpe = mean(daily returns) / std(daily returns) * sqrt(252) if annualize."""
    rets = equity_curve.pct_change().dropna()
    if len(rets) < 2 or rets.std() == 0:
        return 0.0
    sharpe = rets.mean() / rets.std()
    if annualize:
        sharpe *= 252 ** 0.5
    return float(sharpe)


def compute_metrics(
    equity_curve: pd.Series,
    initial_cash: float,
    trading_days: int,
) -> dict[str, float]:
    """Return dict with cagr, max_drawdown, sharpe (and optionally volatility)."""
    return {
        "cagr": compute_cagr(equity_curve, initial_cash, trading_days),
        "max_drawdown": compute_max_drawdown(equity_curve),
        "sharpe": compute_sharpe(equity_curve),
    }
