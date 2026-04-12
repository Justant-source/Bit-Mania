"""
Phase 8.3 — Fetch Fear & Greed Index Data.

Source: Alternative.me public API (https://api.alternative.me/fng/)
Free, no API key required, provides daily data back to 2018.

Output: /data/sentiment/fear_greed.parquet
  Columns: timestamp_ms (int64), value (int32), classification (str)

Usage:
    python scripts/data/fetch_fear_greed.py
    python scripts/data/fetch_fear_greed.py --limit 2000  # more history
    python scripts/data/fetch_fear_greed.py --verify       # just check existing data
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
OUTPUT_PATH = DATA_DIR / "sentiment" / "fear_greed.parquet"

# Alternative.me F&G API
FNG_API_URL = "https://api.alternative.me/fng/?limit={limit}&format=json&date_format=us"


def fetch_fng(limit: int = 0) -> list[dict]:
    """
    Fetch Fear & Greed data from Alternative.me.
    limit=0 returns all available data.
    Returns list of {timestamp_ms, value, classification}.
    """
    url = FNG_API_URL.format(limit=limit)
    print(f"[fetch_fear_greed] Fetching from {url}")

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoEngine/1.0 (backtesting research)"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Failed to fetch F&G data: {exc}\n"
            "Check network connectivity or run in Docker with host networking."
        ) from exc

    data = json.loads(raw)
    if data.get("metadata", {}).get("error"):
        raise ValueError(f"API error: {data['metadata']['error']}")

    records = data.get("data", [])
    if not records:
        raise ValueError("API returned empty data array")

    print(f"  Received {len(records)} records from API")

    results = []
    for rec in records:
        try:
            # Alternative.me returns Unix timestamp in seconds
            ts_sec = int(rec["timestamp"])
            ts_ms  = ts_sec * 1000
            value  = int(rec["value"])
            classification = str(rec.get("value_classification", ""))
            results.append({
                "timestamp_ms":     ts_ms,
                "value":            value,
                "classification":   classification,
            })
        except (KeyError, ValueError) as exc:
            print(f"  WARN: Skipping malformed record: {rec} ({exc})", file=sys.stderr)
            continue

    # Sort chronologically
    results.sort(key=lambda r: r["timestamp_ms"])
    return results


def save_parquet(records: list[dict], path: Path) -> None:
    """Save records to Parquet using polars."""
    try:
        import polars as pl
    except ImportError:
        raise ImportError("polars required. Run: pip install polars")

    df = pl.DataFrame({
        "timestamp_ms":   [r["timestamp_ms"]   for r in records],
        "value":          [r["value"]           for r in records],
        "classification": [r["classification"]  for r in records],
    }).with_columns([
        pl.col("timestamp_ms").cast(pl.Int64),
        pl.col("value").cast(pl.Int32),
    ])

    # Deduplicate
    df = df.unique(subset=["timestamp_ms"], keep="last").sort("timestamp_ms")

    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    print(f"  Saved {len(df)} rows to {path}")


def verify_parquet(path: Path) -> None:
    """Print summary statistics of existing F&G parquet."""
    try:
        import polars as pl
    except ImportError:
        print("polars not available for verification", file=sys.stderr)
        return

    if not path.exists():
        print(f"  NOT FOUND: {path}")
        return

    df = pl.read_parquet(path)
    ts_min = df["timestamp_ms"].min()
    ts_max = df["timestamp_ms"].max()
    dt_min = datetime.fromtimestamp(ts_min / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    dt_max = datetime.fromtimestamp(ts_max / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    fg_min  = df["value"].min()
    fg_max  = df["value"].max()
    fg_mean = df["value"].mean()

    print(f"\n  F&G Data Summary:")
    print(f"    Rows:       {len(df):,}")
    print(f"    Date range: {dt_min} → {dt_max}")
    print(f"    F&G range:  {fg_min} → {fg_max} (mean: {fg_mean:.1f})")
    print(f"    Extreme fear (≤25):  {(df['value'] <= 25).sum()} days")
    print(f"    Extreme greed (≥75): {(df['value'] >= 75).sum()} days")


def merge_with_existing(new_records: list[dict], path: Path) -> list[dict]:
    """Merge new records with existing parquet (append new, keep old)."""
    if not path.exists():
        return new_records

    try:
        import polars as pl
        existing = pl.read_parquet(path)
        existing_ts = set(existing["timestamp_ms"].to_list())
        appended = [r for r in new_records if r["timestamp_ms"] not in existing_ts]
        if appended:
            print(f"  Appending {len(appended)} new records to existing data")
            existing_dicts = existing.to_dicts()
            return sorted(existing_dicts + appended, key=lambda r: r["timestamp_ms"])
        else:
            print("  No new records to append (all already present)")
            return existing.to_dicts()
    except Exception as exc:
        print(f"  WARN: Could not merge with existing: {exc}", file=sys.stderr)
        return new_records


def main():
    p = argparse.ArgumentParser(description="Fetch Fear & Greed Index data")
    p.add_argument("--limit",  type=int, default=0, help="Number of records (0=all)")
    p.add_argument("--output", default=str(OUTPUT_PATH))
    p.add_argument("--verify", action="store_true", help="Verify existing file only")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing data")
    args = p.parse_args()

    out_path = Path(args.output)

    if args.verify:
        verify_parquet(out_path)
        return

    # Fetch
    try:
        records = fetch_fng(args.limit)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # Merge or overwrite
    if not args.overwrite:
        records = merge_with_existing(records, out_path)

    # Save
    save_parquet(records, out_path)
    verify_parquet(out_path)

    # V5 check
    if len(records) < 500:
        print(f"\n  WARN: Only {len(records)} days of data. Need 3 years (≥1095) for V5.")
    else:
        print(f"\n  OK: {len(records)} days of data available for backtesting.")


if __name__ == "__main__":
    main()
