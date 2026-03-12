"""FastAPI app: OHLCV query and backtest endpoints."""

from __future__ import annotations

import os

from fastapi import FastAPI

from arbiter.api.routes import router

app = FastAPI(title="Arbiter API", description="OHLCV and backtest API")
app.include_router(router)


@app.on_event("startup")
def startup():
    app.state.data_dir = os.environ.get("ARBITER_DATA_DIR", "data")
