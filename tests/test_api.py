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
