"""API tests using FastAPI TestClient; fixture data only (no large datasets)."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from arbiter.api.main import app
from arbiter.ingest.ingest_csv import ingest_csv
from arbiter.ingest.parquet_store import write_partitioned

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "sample_ohlcv.csv"


@pytest.fixture
def fixture_data_dir(tmp_path):
    """Write fixture OHLCV to tmp_path as Parquet so API has data without large datasets."""
    df = ingest_csv(FIXTURE_CSV)
    write_partitioned(df, tmp_path, "TEST", "1d")
    return tmp_path


@pytest.fixture
def client(fixture_data_dir):
    """TestClient with app.state.data_dir pointing at fixture Parquet."""
    app.state.data_dir = str(fixture_data_dir)
    return TestClient(app)


def test_get_ohlcv_returns_bars(client):
    r = client.get("/ohlcv", params={"symbol": "TEST"})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    assert "timestamp_utc" in data[0] and "open" in data[0] and "close" in data[0]


def test_get_ohlcv_with_start_end(client):
    r = client.get(
        "/ohlcv",
        params={"symbol": "TEST", "start": "2024-01-01", "end": "2024-12-31"},
    )
    assert r.status_code == 200
    data = r.json()
    assert len(data) >= 1


def test_post_backtest_returns_metrics(client):
    r = client.post(
        "/backtest",
        json={
            "symbol": "TEST",
            "timeframe": "1d",
            "strategy": "buy_and_hold",
            "fees_bps": 0,
            "slippage_bps": 0,
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert "equity_final" in data
    assert "cagr" in data
    assert "max_drawdown" in data
    assert "sharpe" in data


def test_post_backtest_unknown_strategy_returns_400(client):
    """Unknown strategy name should return 400, not 500."""
    r = client.post("/backtest", json={"symbol": "TEST", "strategy": "no_such_strategy"})
    assert r.status_code == 400
    assert "Unknown strategy" in r.json()["detail"]


def test_post_backtest_missing_symbol_returns_400(client):
    r = client.post("/backtest", json={"timeframe": "1d"})
    assert r.status_code == 400


def test_post_backtest_invalid_fees_returns_400(client):
    r = client.post("/backtest", json={"symbol": "TEST", "fees_bps": "not_a_number"})
    assert r.status_code == 400


def test_get_ohlcv_with_timeframe_param(client):
    """timeframe query param should be forwarded (not hardcoded to 1d)."""
    r = client.get("/ohlcv", params={"symbol": "TEST", "timeframe": "1d"})
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_backtest_defaults_match_across_entry_points(client):
    """API omitting fees/slippage must equal the engine called with the shared defaults."""
    from arbiter.backtest.engine import run_backtest
    from arbiter.backtest.fills import DEFAULT_FEES_BPS, DEFAULT_SLIPPAGE_BPS

    r = client.post("/backtest", json={"symbol": "TEST"})
    expected = run_backtest(
        app.state.data_dir,
        "TEST",
        fees_bps=DEFAULT_FEES_BPS,
        slippage_bps=DEFAULT_SLIPPAGE_BPS,
    )
    assert r.status_code == 200
    assert r.json() == expected


def test_ohlcv_unknown_symbol_returns_404(client):
    r = client.get("/ohlcv", params={"symbol": "NOPE"})
    assert r.status_code == 404


def test_backtest_unknown_symbol_returns_404(client):
    r = client.post("/backtest", json={"symbol": "NOPE"})
    assert r.status_code == 404
