"""Thin DuckDB wrapper: connect, read_parquet via glob."""

from __future__ import annotations

import duckdb
import pandas as pd


def read_parquet(glob_path: str) -> pd.DataFrame:
    """Read Parquet files matching glob (e.g. data/SYMBOL/1d/**/*.parquet) into a DataFrame."""
    conn = duckdb.connect(":memory:")
    try:
        return conn.execute("SELECT * FROM read_parquet(?)", [glob_path]).fetchdf()
    finally:
        conn.close()
