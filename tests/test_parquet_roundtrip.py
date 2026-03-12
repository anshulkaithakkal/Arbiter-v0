"""Test ingest → Parquet write → DuckDB read_parquet roundtrip."""

from pathlib import Path

import pytest

from arbiter.ingest.ingest_csv import ingest_csv
from arbiter.ingest.parquet_store import write_partitioned
from arbiter.query.duckdb_client import read_parquet

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "sample_ohlcv.csv"
SYMBOL = "TEST"
TIMEFRAME = "1d"


def test_parquet_roundtrip(tmp_path):
    """Ingest fixture → write to Parquet under tmp_path; read with DuckDB read_parquet; assert row count and key columns."""
    df = ingest_csv(FIXTURE_CSV)
    n_before = len(df)
    written = write_partitioned(df, tmp_path, SYMBOL, TIMEFRAME)
    assert written == n_before
    glob_path = str(tmp_path / SYMBOL / TIMEFRAME / "**" / "*.parquet")
    out = read_parquet(glob_path)
    assert len(out) == n_before
    for col in ["timestamp_utc", "open", "high", "low", "close", "volume"]:
        assert col in out.columns
