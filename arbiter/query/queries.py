"""Run DuckDB queries; support read_parquet for OHLCV data."""

from __future__ import annotations

import duckdb


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
