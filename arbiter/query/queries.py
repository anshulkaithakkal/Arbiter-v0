"""Run DuckDB queries; support read_parquet for OHLCV data."""

from __future__ import annotations

import duckdb
import pandas as pd

from arbiter.query.duckdb_client import read_parquet


def run_sql(sql: str) -> list[tuple]:
    """
    Execute DuckDB SQL (e.g. SELECT ... FROM read_parquet('...')).
    Returns list of rows (tuples).
    """
    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(sql)
        return result.fetchall()
    finally:
        conn.close()


def get_ohlcv(data_dir: str, symbol: str, timeframe: str = "1d", start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Get OHLCV for symbol in optional date range. Path: data_dir/symbol/timeframe/**/*.parquet."""
    from pathlib import Path
    base = Path(data_dir) / symbol / timeframe
    glob_path = str(base / "**" / "*.parquet")
    df = read_parquet(glob_path)
    if df.empty:
        return df
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    if start is not None:
        df = df[df["timestamp_utc"] >= pd.to_datetime(start, utc=True)]
    if end is not None:
        df = df[df["timestamp_utc"] <= pd.to_datetime(end, utc=True)]
    return df.sort_values("timestamp_utc").reset_index(drop=True)
