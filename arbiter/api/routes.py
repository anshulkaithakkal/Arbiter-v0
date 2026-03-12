"""API routes: GET /ohlcv, POST /backtest."""

from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, Request, HTTPException, Body

from arbiter.backtest.engine import load_ohlcv_parquet, run_backtest

router = APIRouter()


def _get_data_dir(request: Request) -> str:
    return getattr(request.app.state, "data_dir", "data")


@router.get("/ohlcv")
def get_ohlcv(
    request: Request,
    symbol: str,
    start: str | None = None,
    end: str | None = None,
):
    """Return OHLCV bars for symbol in [start, end] (ISO date strings)."""
    data_dir = _get_data_dir(request)
    df = load_ohlcv_parquet(data_dir, symbol, "1d")
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
def post_backtest(request: Request, body: dict = Body(...)):
    """Run backtest. Body: symbol, timeframe?, strategy?, fees_bps?, slippage_bps?."""
    symbol = body.get("symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    data_dir = _get_data_dir(request)
    result = run_backtest(
        data_dir=data_dir,
        symbol=symbol,
        timeframe=str(body.get("timeframe", "1d")),
        strategy=str(body.get("strategy", "buy_and_hold")),
        fees_bps=float(body.get("fees_bps", 0)),
        slippage_bps=float(body.get("slippage_bps", 0)),
    )
    return result
