"""
Tests for the strategy interface: registry, built-in strategies, engine integration.
"""

from __future__ import annotations

import pandas as pd
import pytest

from arbiter.backtest.strategies import (
    buy_and_hold,
    make_sma_crossover,
    get_strategy,
    list_strategies,
    register,
    REGISTRY,
)
from arbiter.backtest.engine import run_backtest_from_df, _run_backtest_core


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(opens, closes):
    """Build a minimal OHLCV DataFrame from open/close lists."""
    n = len(opens)
    return pd.DataFrame({
        "open":   [float(x) for x in opens],
        "high":   [float(max(o, c)) + 1 for o, c in zip(opens, closes)],
        "low":    [float(min(o, c)) - 1 for o, c in zip(opens, closes)],
        "close":  [float(x) for x in closes],
        "volume": [1_000_000.0] * n,
    })


FIXTURE_5BAR = _make_df(
    opens= [100, 101, 102, 103, 104],
    closes=[101, 102, 103, 104, 105],
)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

def test_list_strategies_contains_builtins():
    """buy_and_hold and sma_crossover_20_50 must always be in the registry."""
    names = list_strategies()
    assert "buy_and_hold" in names
    assert "sma_crossover_20_50" in names


def test_get_strategy_returns_callable():
    fn = get_strategy("buy_and_hold")
    assert callable(fn)


def test_get_strategy_unknown_raises():
    with pytest.raises(ValueError, match="Unknown strategy"):
        get_strategy("nonexistent_strategy_xyz")


def test_register_custom_strategy():
    def always_flat(df: pd.DataFrame) -> pd.Series:
        return pd.Series(0, index=df.index, dtype=int)

    register("always_flat", always_flat)
    assert "always_flat" in list_strategies()
    assert get_strategy("always_flat") is always_flat

    # cleanup so other tests aren't affected
    del REGISTRY["always_flat"]


# ---------------------------------------------------------------------------
# buy_and_hold signal tests
# ---------------------------------------------------------------------------

def test_buy_and_hold_signals_bar0_is_1():
    sigs = buy_and_hold(FIXTURE_5BAR)
    assert int(sigs.iloc[0]) == 1


def test_buy_and_hold_signals_rest_are_0():
    sigs = buy_and_hold(FIXTURE_5BAR)
    assert all(int(s) == 0 for s in sigs.iloc[1:])


def test_buy_and_hold_empty_df_returns_empty_series():
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    sigs = buy_and_hold(empty)
    assert len(sigs) == 0


# ---------------------------------------------------------------------------
# SMA crossover signal tests
# ---------------------------------------------------------------------------

def test_sma_crossover_factory_invalid_window_raises():
    with pytest.raises(ValueError, match="fast.*must be < slow"):
        make_sma_crossover(fast=10, slow=5)


def test_sma_crossover_name_reflects_windows():
    fn = make_sma_crossover(5, 20)
    assert fn.__name__ == "sma_crossover_5_20"


def test_sma_crossover_detects_cross_up():
    """fast=2, slow=3: cross-up should appear once the fast MA exceeds slow MA."""
    df = _make_df(
        opens= [10, 9,  8,  11, 12, 13],
        closes=[10, 9,  8,  11, 12, 13],
    )
    sma = make_sma_crossover(2, 3)
    sigs = sma(df)
    # Cross happens at bar 3 (fast becomes > slow after being below)
    assert int(sigs.iloc[3]) == 1, f"Expected +1 at bar 3, got {sigs.tolist()}"


def test_sma_crossover_detects_cross_down():
    """Cross-down should emit -1 when fast drops below slow."""
    df = _make_df(
        opens= [10, 11, 12, 8, 7, 6],
        closes=[10, 11, 12, 8, 7, 6],
    )
    sma = make_sma_crossover(2, 3)
    sigs = sma(df)
    # cross-up first, then cross-down when price drops
    assert -1 in sigs.values, f"Expected a -1 signal somewhere: {sigs.tolist()}"


def test_sma_crossover_no_signals_before_warmup():
    """No signals should appear in bars before the slow MA window is valid."""
    df = _make_df(
        opens= [10, 11, 12, 13, 14],
        closes=[10, 11, 12, 13, 14],
    )
    sma = make_sma_crossover(2, 4)  # slow window = 4 → bars 0-2 are NaN
    sigs = sma(df)
    # Bars 0..2 (slow window - 2) should be 0 since slow MA not yet valid
    assert int(sigs.iloc[0]) == 0
    assert int(sigs.iloc[1]) == 0
    assert int(sigs.iloc[2]) == 0


# ---------------------------------------------------------------------------
# Engine integration: strategy callable vs strategy name
# ---------------------------------------------------------------------------

def test_engine_accepts_strategy_name_string():
    result = run_backtest_from_df(FIXTURE_5BAR, strategy="buy_and_hold")
    assert result["equity_final"] > 0


def test_engine_accepts_strategy_callable():
    result = run_backtest_from_df(FIXTURE_5BAR, strategy=buy_and_hold)
    assert result["equity_final"] > 0


def test_engine_name_and_callable_produce_same_result():
    r1 = run_backtest_from_df(FIXTURE_5BAR, strategy="buy_and_hold")
    r2 = run_backtest_from_df(FIXTURE_5BAR, strategy=buy_and_hold)
    assert r1 == r2


def test_engine_unknown_strategy_name_raises():
    with pytest.raises(ValueError, match="Unknown strategy"):
        run_backtest_from_df(FIXTURE_5BAR, strategy="does_not_exist")


# ---------------------------------------------------------------------------
# Engine integration: sell path
# ---------------------------------------------------------------------------

def test_sell_reduces_position_to_zero():
    """A strategy that buys bar 0 and sells bar 2 should have cash > 0 at the end."""
    def buy_then_sell(df: pd.DataFrame) -> pd.Series:
        s = pd.Series(0, index=df.index, dtype=int)
        s.iloc[0] = 1   # buy → fill at open[1]
        s.iloc[2] = -1  # sell → fill at open[3]
        return s

    # After selling at open[3]=103, cash = (1e6/101)*103; position=0
    # Equity stays at cash value since no more position
    expected_cash = round((1_000_000 / 101) * 103, 2)
    result = run_backtest_from_df(FIXTURE_5BAR, strategy=buy_then_sell, fees_bps=0, slippage_bps=0)
    assert result["equity_final"] == expected_cash


def test_sell_with_fees_reduces_proceeds():
    def buy_then_sell(df: pd.DataFrame) -> pd.Series:
        s = pd.Series(0, index=df.index, dtype=int)
        s.iloc[0] = 1
        s.iloc[2] = -1
        return s

    no_fee = run_backtest_from_df(FIXTURE_5BAR, strategy=buy_then_sell, fees_bps=0)
    with_fee = run_backtest_from_df(FIXTURE_5BAR, strategy=buy_then_sell, fees_bps=10)
    assert with_fee["equity_final"] < no_fee["equity_final"]


# ---------------------------------------------------------------------------
# Engine: volatility in output
# ---------------------------------------------------------------------------

def test_result_contains_volatility():
    result = run_backtest_from_df(FIXTURE_5BAR, strategy="buy_and_hold")
    assert "volatility" in result
    assert isinstance(result["volatility"], float)
    assert result["volatility"] >= 0


def test_flat_equity_curve_has_zero_volatility():
    """A strategy that never trades should have 0 volatility (equity never changes)."""
    def do_nothing(df: pd.DataFrame) -> pd.Series:
        return pd.Series(0, index=df.index, dtype=int)

    result = run_backtest_from_df(FIXTURE_5BAR, strategy=do_nothing)
    assert result["volatility"] == 0.0


# ---------------------------------------------------------------------------
# Engine: edge cases
# ---------------------------------------------------------------------------

def test_empty_df_returns_initial_cash():
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    result = run_backtest_from_df(empty, strategy="buy_and_hold")
    assert result["equity_final"] == 1_000_000.0


def test_single_bar_returns_initial_cash():
    single = _make_df([100], [101])
    result = run_backtest_from_df(single, strategy="buy_and_hold")
    assert result["equity_final"] == 1_000_000.0


def test_signal_at_last_bar_is_ignored():
    """A signal at the final bar has no bar i+1 to fill into; equity should be unchanged."""
    def signal_at_last(df: pd.DataFrame) -> pd.Series:
        s = pd.Series(0, index=df.index, dtype=int)
        s.iloc[-1] = 1  # last bar — should be ignored by engine
        return s

    result = run_backtest_from_df(FIXTURE_5BAR, strategy=signal_at_last)
    assert result["equity_final"] == 1_000_000.0
