"""Test ingest → Parquet write → DuckDB read_parquet roundtrip."""

from pathlib import Path

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


def test_write_partitioned_merges_existing_month(tmp_path):
    """A second write into the same month keeps earlier rows and dedups overlaps."""
    df = ingest_csv(FIXTURE_CSV)
    first, second = df.iloc[:3], df.iloc[2:]  # one overlapping row
    write_partitioned(first, tmp_path, SYMBOL, TIMEFRAME)
    write_partitioned(second, tmp_path, SYMBOL, TIMEFRAME)
    out = read_parquet(str(tmp_path / SYMBOL / TIMEFRAME / "**" / "*.parquet"))
    assert len(out) == len(df)
    assert out["timestamp_utc"].is_unique
