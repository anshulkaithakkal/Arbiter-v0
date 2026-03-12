"""Fill at next bar open; apply fees (bps) and slippage (bps)."""

from __future__ import annotations

# Execution rule: order at close of bar T fills at open of bar T+1
FILL_AT_NEXT_OPEN = "next_open"


def fill_price_at_next_open(
    open_next_bar: float,
    side: str,
    slippage_bps: float = 0.0,
) -> float:
    """
    Fill price for a market order that executes at next bar open.
    Buy: open * (1 + slippage_bps/1e4), Sell: open * (1 - slippage_bps/1e4).
    """
    if side == "buy":
        return open_next_bar * (1 + slippage_bps / 10_000)
    else:
        return open_next_bar * (1 - slippage_bps / 10_000)


def cost_per_share_buy(
    open_next_bar: float,
    fees_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> float:
    """Cost per share for a buy fill at next bar open (includes fees and slippage)."""
    fill = fill_price_at_next_open(open_next_bar, "buy", slippage_bps)
    return fill * (1 + fees_bps / 10_000)


def shares_bought(cash: float, cost_per_share: float) -> float:
    """Shares acquired when spending cash (full amount)."""
    if cost_per_share <= 0:
        return 0.0
    return cash / cost_per_share
