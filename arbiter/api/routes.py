"""API routes: GET /ohlcv, GET /strategies, POST /backtest."""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Body, HTTPException, Request

from arbiter.backtest.engine import load_ohlcv_parquet, run_backtest
from arbiter.backtest.fills import DEFAULT_FEES_BPS, DEFAULT_SLIPPAGE_BPS
from arbiter.backtest.strategies import list_strategies

router = APIRouter()


def _get_data_dir(request: Request) -> str:
    return getattr(request.app.state, "data_dir", "data")


@router.get("/strategies")
def get_strategies():
    """Return list of registered strategy names."""
    return {"strategies": list_strategies()}


@router.get("/ohlcv")
def get_ohlcv(
    request: Request,
    symbol: str,
    timeframe: str = "1d",
    start: str | None = None,
    end: str | None = None,
):
    """Return OHLCV bars for symbol/timeframe in [start, end] (ISO date strings)."""
    data_dir = _get_data_dir(request)
    df = load_ohlcv_parquet(data_dir, symbol, timeframe)
    if df.empty:
        return []
    if start is not None:
        start_d = pd.to_datetime(start, utc=True)
        df = df[df["timestamp_utc"] >= start_d]
    if end is not None:
        end_d = pd.to_datetime(end, utc=True)
        df = df[df["timestamp_utc"] <= end_d]
    df = df.copy()
    df["timestamp_utc"] = df["timestamp_utc"].astype(str)
    return df.to_dict(orient="records")


@router.post("/backtest")
def post_backtest(request: Request, body: Annotated[dict, Body()]):
    """Run backtest. Body: symbol, timeframe?, strategy?, fees_bps?, slippage_bps?."""
    symbol = body.get("symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    try:
        fees_bps = float(body.get("fees_bps", DEFAULT_FEES_BPS))
        slippage_bps = float(body.get("slippage_bps", DEFAULT_SLIPPAGE_BPS))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="fees_bps and slippage_bps must be numeric")
    data_dir = _get_data_dir(request)
    try:
        result = run_backtest(
            data_dir=data_dir,
            symbol=symbol,
            timeframe=str(body.get("timeframe", "1d")),
            strategy=str(body.get("strategy", "buy_and_hold")),
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result
