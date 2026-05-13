#!/usr/bin/env python3
"""
build_monthly_funding_estimates.py

Aggregate BTCUSDT 8h funding rates by (year, month) and write a CSV table
used by cost-adjustment scripts for per-trade monthly funding estimates.

Output schema:
  year, month, avg_rate_8h, n_events, source
  source = 'observed'           - months with actual non-zero funding data
  source = 'fallback_overall_mean' - months with no data (pre-2020 gap)

Usage:
  python3 backtest/scripts/data/build_monthly_funding_estimates.py
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

BT_ROOT = Path(__file__).resolve().parent.parent.parent
FUNDING_PARQUET = BT_ROOT / "data" / "funding" / "BTCUSDT_8h.parquet"
OUT_CSV = BT_ROOT / "data" / "funding" / "BTCUSDT_monthly_estimates.csv"

# Full range covered by the parquet file
DATA_START = datetime(2018, 4, 1)
DATA_END   = datetime(2026, 4, 30)


def iter_year_months(start: datetime, end: datetime):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def main():
    if not FUNDING_PARQUET.exists():
        print(f"ERROR: {FUNDING_PARQUET} not found", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(FUNDING_PARQUET)
    df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["year"] = df["dt"].dt.year
    df["month"] = df["dt"].dt.month

    nonzero = df[df["funding_rate"] != 0]
    observed = (
        nonzero.groupby(["year", "month"])["funding_rate"]
        .agg(avg_rate_8h="mean", n_events="count")
        .reset_index()
    )

    overall_mean = float(nonzero["funding_rate"].mean())
    print(f"[info] Non-zero rows: {len(nonzero)}, overall mean rate: {overall_mean:.8f}", file=sys.stderr)

    obs_set = set(zip(observed["year"], observed["month"]))

    rows = []
    for y, m in iter_year_months(DATA_START, DATA_END):
        if (y, m) in obs_set:
            r = observed[(observed["year"] == y) & (observed["month"] == m)].iloc[0]
            rows.append({
                "year": y,
                "month": m,
                "avg_rate_8h": float(r["avg_rate_8h"]),
                "n_events": int(r["n_events"]),
                "source": "observed",
            })
        else:
            rows.append({
                "year": y,
                "month": m,
                "avg_rate_8h": overall_mean,
                "n_events": 0,
                "source": "fallback_overall_mean",
            })

    out = pd.DataFrame(rows, columns=["year", "month", "avg_rate_8h", "n_events", "source"])
    out.to_csv(OUT_CSV, index=False, float_format="%.10f")

    n_obs = (out["source"] == "observed").sum()
    n_fb  = (out["source"] == "fallback_overall_mean").sum()
    print(f"[done] {len(out)} months total — {n_obs} observed, {n_fb} fallback → {OUT_CSV}", file=sys.stdout)


if __name__ == "__main__":
    main()
