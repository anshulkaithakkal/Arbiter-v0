"""Test CSV ingest: schema, dtypes, UTC, dedup."""

from pathlib import Path

import pandas as pd
import pytest

from arbiter.ingest.ingest_csv import ingest_csv
from arbiter.ingest.schema import OHLCV_COLUMNS

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "sample_ohlcv.csv"


def test_ingest_schema_columns_and_dtypes():
    """Load sample_ohlcv.csv via ingest_csv; assert schema (columns, dtypes, UTC)."""
    df = ingest_csv(FIXTURE_CSV)
    for col in OHLCV_COLUMNS:
        assert col in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"])
    assert df["timestamp_utc"].dt.tz is not None
    for col in ["open", "high", "low", "close", "volume"]:
        assert pd.api.types.is_numeric_dtype(df[col])


def test_ingest_dedup(tmp_path):
    """Assert dedup: CSV with duplicate timestamp yields one row per timestamp (keep last)."""
    dup_csv = tmp_path / "dup.csv"
    dup_csv.write_text(
        "timestamp_utc,open,high,low,close,volume\n"
        "2024-01-02 00:00:00+00:00,100,102,99,101,1000\n"
        "2024-01-02 00:00:00+00:00,100,102,99,101,2000\n"
    )
    df = ingest_csv(dup_csv)
    assert len(df) == 1
    assert df["volume"].iloc[0] == 2000


def test_ingest_missing_required_column_raises(tmp_path):
    """CSV missing a required column (e.g. volume) should raise ValueError."""
    bad_csv = tmp_path / "no_volume.csv"
    bad_csv.write_text(
        "timestamp_utc,open,high,low,close\n"
        "2024-01-02 00:00:00+00:00,100,102,99,101\n"
    )
    with pytest.raises(ValueError, match="Missing required OHLCV columns"):
        ingest_csv(bad_csv)


def test_ingest_nan_ohlcv_rows_dropped(tmp_path):
    """Rows with non-numeric OHLCV values should be dropped after coercion."""
    bad_csv = tmp_path / "nan_rows.csv"
    bad_csv.write_text(
        "timestamp_utc,open,high,low,close,volume\n"
        "2024-01-02 00:00:00+00:00,100,102,99,101,1000\n"
        "2024-01-03 00:00:00+00:00,N/A,103,100,102,1100\n"
    )
    df = ingest_csv(bad_csv)
    assert len(df) == 1
    assert df["open"].iloc[0] == 100.0
