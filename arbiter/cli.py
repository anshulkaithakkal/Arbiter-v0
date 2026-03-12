"""CLI: ingest CSV to Parquet, run DuckDB query, run backtest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from arbiter.ingest.ingest_csv import ingest_csv
from arbiter.ingest.parquet_store import write_partitioned
from arbiter.query.queries import run_sql
from arbiter.backtest.engine import backtest_json


def cmd_ingest(args: argparse.Namespace) -> None:
    csv_path = Path(args.csv)
    out_path = Path(args.out)
    df = ingest_csv(csv_path)
    rows_ingested = len(df)
    rows_written = write_partitioned(df, out_path, args.symbol, args.timeframe)
    print(f"rows_ingested={rows_ingested} rows_written={rows_written}")


def cmd_query(args: argparse.Namespace) -> None:
    rows = run_sql(args.sql)
    if rows and len(rows) == 1 and len(rows[0]) == 1:
        print(f"count = {rows[0][0]}")
    else:
        for row in rows:
            print(row)


def cmd_backtest(args: argparse.Namespace) -> None:
    data_dir = Path(args.data_dir) if getattr(args, "data_dir", None) else Path("data")
    out = backtest_json(
        data_dir=data_dir,
        symbol=args.symbol,
        timeframe=args.timeframe,
        strategy=args.strategy,
        fees_bps=float(args.fees_bps),
        slippage_bps=float(args.slippage_bps),
    )
    print(out)


def main() -> None:
    parser = argparse.ArgumentParser(prog="arbiter")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ingest --symbol TEST --timeframe 1d --csv tests/fixtures/sample_ohlcv.csv --out data/
    p_ingest = subparsers.add_parser("ingest")
    p_ingest.add_argument("--symbol", required=True)
    p_ingest.add_argument("--timeframe", default="1d")
    p_ingest.add_argument("--csv", required=True)
    p_ingest.add_argument("--out", required=True)
    p_ingest.set_defaults(func=cmd_ingest)

    # query --sql "SELECT count(*) FROM read_parquet('data/TEST/1d/**/*.parquet');"
    p_query = subparsers.add_parser("query")
    p_query.add_argument("--sql", required=True)
    p_query.set_defaults(func=cmd_query)

    # backtest --symbol TEST --timeframe 1d --strategy buy_and_hold --fees_bps 10 --slippage_bps 5
    p_backtest = subparsers.add_parser("backtest")
    p_backtest.add_argument("--symbol", required=True)
    p_backtest.add_argument("--timeframe", default="1d")
    p_backtest.add_argument("--strategy", default="buy_and_hold")
    p_backtest.add_argument("--fees_bps", default=10)
    p_backtest.add_argument("--slippage_bps", default=5)
    p_backtest.add_argument("--data-dir", default="data", dest="data_dir")
    p_backtest.set_defaults(func=cmd_backtest)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
