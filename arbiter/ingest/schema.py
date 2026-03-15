"""Canonical OHLCV schema: UTC timestamps, types, deduplicate by timestamp."""

from __future__ import annotations

import pandas as pd

OHLCV_COLUMNS = ["timestamp_utc", "open", "high", "low", "close", "volume"]


def validate_and_coerce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and coerce DataFrame to canonical OHLCV schema.
    Ensures dtypes, normalizes timestamp to UTC, sorts by timestamp_utc,
    deduplicates by timestamp (keep last). Drops rows with any NaN OHLCV value.

    Raises:
        ValueError: if any required column is missing.
    """
    if df.empty:
        return df
    missing = [c for c in OHLCV_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {missing}")
    out = df.copy()
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=OHLCV_COLUMNS)
    out = out.sort_values("timestamp_utc").drop_duplicates(subset=["timestamp_utc"], keep="last")
    return out.reset_index(drop=True)
