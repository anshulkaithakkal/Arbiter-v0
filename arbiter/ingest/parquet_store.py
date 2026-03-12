"""Write OHLCV DataFrames to partitioned Parquet: data/<symbol>/<timeframe>/year=YYYY/month=MM/*.parquet."""

from __future__ import annotations

import pandas as pd
from pathlib import Path


def write_partitioned(
    df: pd.DataFrame,
    base_path: str | Path,
    symbol: str,
    timeframe: str = "1d",
) -> int:
    """
    Write DataFrame to partitioned Parquet under base_path.
    Layout: base_path / symbol / timeframe / year=YYYY / month=MM / part.parquet
    Returns number of rows written.
    """
    if df.empty or "timestamp_utc" not in df.columns:
        return 0
    base = Path(base_path)
    df = df.copy()
    df["year"] = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.month
    rows = 0
    for (year, month), group in df.groupby(["year", "month"]):
        out_dir = base / symbol / timeframe / f"year={year}" / f"month={month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"
        subset = group.drop(columns=["year", "month"])
        subset.to_parquet(out_file, index=False)
        rows += len(subset)
    return rows
