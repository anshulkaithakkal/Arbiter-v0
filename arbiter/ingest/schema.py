"""Canonical OHLCV schema: UTC timestamps, types, deduplicate by timestamp."""

from __future__ import annotations

import pandas as pd

OHLCV_COLUMNS = ["timestamp_utc", "open", "high", "low", "close", "volume"]


def validate_and_coerce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and coerce DataFrame to canonical OHLCV schema.
    Ensures dtypes, normalizes timestamp to UTC, sorts by timestamp_utc,
    deduplicates by timestamp (keep last).
    """
    if df.empty:
        return df
    out = df.copy()
    if "timestamp_utc" in out.columns:
        out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["timestamp_utc"])
    out = out.sort_values("timestamp_utc").drop_duplicates(subset=["timestamp_utc"], keep="last")
    return out.reset_index(drop=True)
