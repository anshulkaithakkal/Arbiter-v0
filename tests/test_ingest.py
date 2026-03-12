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
