"""
Golden backtest tests: deterministic equity for fixture, fees/slippage impact, no lookahead.
"""

from pathlib import Path

import pytest

from arbiter.ingest.ingest_csv import ingest_csv
from arbiter.backtest.engine import run_backtest_from_df


FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "sample_ohlcv.csv"

# Golden values for sample_ohlcv.csv with fees_bps=0, slippage_bps=0, initial_cash=1_000_000.
# Buy at close of bar 0 -> fill at open of bar 1 = 101. position = 1e6/101, equity_final = position * 105.
GOLDEN_EQUITY_FINAL = 1_039_543.96
# If we wrongly filled at bar 0 open (100) we would get 10000 * 105 = 1_050_000 (lookahead).
LOOKAHEAD_EQUITY_IF_FILL_AT_BAR0 = 1_050_000.0


def _load_fixture_df():
    return ingest_csv(FIXTURE_CSV)


def test_golden_backtest_deterministic_equity():
    """Deterministic equity curve: run backtest on fixture, assert expected final equity."""
    df = _load_fixture_df()
    result = run_backtest_from_df(
        df,
        strategy="buy_and_hold",
        fees_bps=0.0,
        slippage_bps=0.0,
        initial_cash=1_000_000.0,
    )
    assert result["equity_final"] == GOLDEN_EQUITY_FINAL
    print("golden backtest ok")


def test_fees_slippage_reduce_equity():
    """Increasing fees_bps or slippage_bps must reduce equity_final vs zero cost."""
    df = _load_fixture_df()
    base = run_backtest_from_df(df, fees_bps=0.0, slippage_bps=0.0)
    with_fees = run_backtest_from_df(df, fees_bps=10.0, slippage_bps=0.0)
    with_slippage = run_backtest_from_df(df, fees_bps=0.0, slippage_bps=5.0)
    assert with_fees["equity_final"] < base["equity_final"]
    assert with_slippage["equity_final"] < base["equity_final"]


def test_no_lookahead_fill_at_next_open():
    """
    No lookahead: fill must be at next bar open. If we filled at bar 0 open we would get
    LOOKAHEAD_EQUITY_IF_FILL_AT_BAR0. Correct logic gives GOLDEN_EQUITY_FINAL.
    """
    df = _load_fixture_df()
    result = run_backtest_from_df(df, fees_bps=0.0, slippage_bps=0.0)
    assert result["equity_final"] != LOOKAHEAD_EQUITY_IF_FILL_AT_BAR0
    assert result["equity_final"] == GOLDEN_EQUITY_FINAL
