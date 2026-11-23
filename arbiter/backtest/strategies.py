"""
Strategy interface for Arbiter backtests.

A strategy is any callable with signature:
    fn(df: pd.DataFrame) -> pd.Series[int]

where the returned Series is aligned to df's index and each value is:
    +1  → enter long  (signal fires at close of bar i, fills at open of bar i+1)
    -1  → exit long   (same timing)
     0  → hold / no action

Rules:
- Signals at the final bar are always ignored (no bar i+1 to fill into).
- The engine handles position state; strategies only emit signals.
- Strategies must not look ahead (no future close/open/volume).
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd

# Type alias for a strategy function
StrategyFn = Callable[[pd.DataFrame], pd.Series]


# ---------------------------------------------------------------------------
# Built-in strategies
# ---------------------------------------------------------------------------

def buy_and_hold(df: pd.DataFrame) -> pd.Series:
    """Enter long at close of bar 0 and hold forever."""
    signals = pd.Series(0, index=df.index, dtype=int)
    if len(df) >= 1:
        signals.iloc[0] = 1
    return signals


def make_sma_crossover(fast: int = 20, slow: int = 50) -> StrategyFn:
    """
    Factory returning an SMA crossover strategy.

    Emits +1 when the fast SMA crosses above the slow SMA (enter long),
    and -1 when the fast SMA crosses below the slow SMA (exit long).
    Bars before the slow SMA window is warm are treated as no-signal.

    Args:
        fast: Look-back period for the fast simple moving average.
        slow: Look-back period for the slow simple moving average.

    Returns:
        A StrategyFn that can be passed directly to the backtest engine.
    """
    if fast >= slow:
        raise ValueError(f"fast ({fast}) must be < slow ({slow})")

    def sma_crossover(df: pd.DataFrame) -> pd.Series:
        close = df["close"]
        fast_ma = close.rolling(fast).mean()
        slow_ma = close.rolling(slow).mean()

        # True when fast is above slow; NaN periods → False (no signal yet)
        above = (fast_ma > slow_ma).fillna(value=False).astype(bool)
        # Shift by prepending False; avoids pandas FutureWarning from shift+fillna on object dtype
        prev_above = pd.Series(
            np.concatenate([[False], above.values[:-1]]),
            index=above.index,
            dtype=bool,
        )

        signals = pd.Series(0, index=df.index, dtype=int)
        signals[above & ~prev_above] = 1   # cross up → enter long
        signals[~above & prev_above] = -1  # cross down → exit long
        return signals

    sma_crossover.__name__ = f"sma_crossover_{fast}_{slow}"
    return sma_crossover


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

REGISTRY: dict[str, StrategyFn] = {
    "buy_and_hold": buy_and_hold,
    "sma_crossover_20_50": make_sma_crossover(20, 50),
}


def register(name: str, fn: StrategyFn) -> None:
    """Add a custom strategy to the registry."""
    REGISTRY[name] = fn


def get_strategy(name: str) -> StrategyFn:
    """
    Look up a strategy by name.

    Raises:
        ValueError: if name is not in the registry.
    """
    if name not in REGISTRY:
        available = ", ".join(sorted(REGISTRY.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available: {available}")
    return REGISTRY[name]


def list_strategies() -> list[str]:
    """Return sorted list of registered strategy names."""
    return sorted(REGISTRY.keys())
