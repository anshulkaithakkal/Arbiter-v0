"""Ingest OHLCV from CSV: normalize schema, enforce types, deduplicate by timestamp."""

from __future__ import annotations

import pandas as pd
from pathlib import Path

from arbiter.ingest.schema import OHLCV_COLUMNS, validate_and_coerce

# Flexible input column name mapping (e.g. date -> timestamp_utc)
COLUMN_ALIASES = {
    "timestamp_utc": ["timestamp_utc", "timestamp", "date", "datetime", "time"],
    "open": ["open", "o"],
    "high": ["high", "h"],
    "low": ["low", "l"],
    "close": ["close", "c"],
    "volume": ["volume", "v", "vol"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map input columns to canonical OHLCV names (case-insensitive)."""
    out = pd.DataFrame(index=df.index)
    cols_lower = {c.lower(): c for c in df.columns}
    for canonical, aliases in COLUMN_ALIASES.items():
        for a in aliases:
            if a in cols_lower:
                out[canonical] = df[cols_lower[a]]
                break
    return out


def _enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce dtypes and UTC for timestamp."""
    df = df.copy()
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def ingest_csv(csv_path: str | Path) -> pd.DataFrame:
    """
    Read CSV, normalize to canonical OHLCV schema (UTC), deduplicate by timestamp.
    Returns cleaned DataFrame with columns: timestamp_utc, open, high, low, close, volume.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    raw = pd.read_csv(path)
    df = _normalize_columns(raw)
    df = _enforce_types(df)
    return validate_and_coerce(df)
