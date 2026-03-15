"""FastAPI app: OHLCV query and backtest endpoints."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from arbiter.api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.data_dir = os.environ.get("ARBITER_DATA_DIR", "data")
    yield


app = FastAPI(title="Arbiter API", description="OHLCV and backtest API", lifespan=lifespan)
app.include_router(router)
