#!/usr/bin/env python3
"""
backfill_2019_klines.py — One-shot script to download 2019 BTCUSDT 1h spot candles
and save them as monthly parquet files matching the runtime loader schema.

Binance USDT-M futures BTCUSDT started 2019-09-08, so spot data is used for the
full year to provide consistent warmup candles for all timeframes (1D needs ~220
days back from 2020-01-01 = ~2019-05-25).

Output: backtest/data/ohlcv/BTCUSDT/1h/2019/MM.parquet
Schema: matches 2020+ files (open_time datetime[ms,UTC], open/high/low/close/volume float64, ...)

Usage:
    python backtest/scripts/data/backfill_2019_klines.py
"""
from __future__ import annotations

import io
import sys
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests

BINANCE_SPOT_BASE = "https://data.binance.vision/data/spot/monthly/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
YEAR = 2019
MONTHS = range(1, 13)
MAX_RETRIES = 3
RETRY_DELAY = 3

SCRIPT_DIR = Path(__file__).parent
try:
    sys.path.insert(0, str(SCRIPT_DIR.parent))
    from _paths import OHLCV_ROOT
    OUT_BASE = OHLCV_ROOT / SYMBOL / INTERVAL / str(YEAR)
except ImportError:
    BT_ROOT = SCRIPT_DIR.parent.parent
    OUT_BASE = BT_ROOT / "data" / "ohlcv" / SYMBOL / INTERVAL / str(YEAR)

BINANCE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]


def _download(url: str) -> bytes | None:
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  [retry {attempt+1}] {e}", flush=True)
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"  [FAIL] {url}: {e}", flush=True)
    return None


def _fetch_month(year: int, month: int) -> pd.DataFrame | None:
    mm = f"{month:02d}"
    filename = f"{SYMBOL}-{INTERVAL}-{year}-{mm}"
    url = f"{BINANCE_SPOT_BASE}/{SYMBOL}/{INTERVAL}/{filename}.zip"
    print(f"  Downloading {filename}.zip ...", flush=True)
    content = _download(url)
    if content is None:
        return None
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_files:
            print(f"  [WARN] No CSV in {filename}.zip", flush=True)
            return None
        with zf.open(csv_files[0]) as f:
            df = pd.read_csv(f, names=BINANCE_COLUMNS, dtype={"open_time": "int64", "close_time": "int64"})
    # Drop ignore column and convert types
    df = df.drop(columns=["ignore"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base_volume", "taker_buy_quote_volume"]:
        df[col] = df[col].astype("float64")
    df["trade_count"] = df["trade_count"].astype("int64")
    return df


def _verify_schema(df: pd.DataFrame, ref_path: Path) -> None:
    """Print schema comparison between downloaded data and existing 2020 reference."""
    ref = pd.read_parquet(ref_path)
    print("\n  Schema comparison (2019 vs 2020/01):")
    for col in ref.columns:
        if col in df.columns:
            match = "✓" if str(df[col].dtype) == str(ref[col].dtype) else "✗ MISMATCH"
            print(f"    {col}: {df[col].dtype} {match}")
        else:
            print(f"    {col}: MISSING in 2019 data ✗")
    extra = set(df.columns) - set(ref.columns)
    if extra:
        print(f"  Extra cols in 2019: {extra}")


def main() -> None:
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    ref_path = OUT_BASE.parent / "2020" / "01.parquet"

    total_rows = 0
    for month in MONTHS:
        mm = f"{month:02d}"
        out_path = OUT_BASE / f"{mm}.parquet"
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            print(f"  [{YEAR}-{mm}] SKIP (already exists, {len(existing)} rows)", flush=True)
            total_rows += len(existing)
            continue

        df = _fetch_month(YEAR, month)
        if df is None:
            print(f"  [{YEAR}-{mm}] FAIL — skipping", flush=True)
            continue

        df = df.sort_values("open_time").drop_duplicates(subset=["open_time"]).reset_index(drop=True)
        df.to_parquet(out_path, engine="pyarrow", compression="zstd", index=False)
        print(f"  [{YEAR}-{mm}] OK — {len(df)} rows → {out_path}", flush=True)
        total_rows += len(df)

    print(f"\nTotal 2019 rows: {total_rows:,} (expected ~8760)", flush=True)

    # Schema validation against 2020/01.parquet
    if ref_path.exists():
        dec_path = OUT_BASE / "12.parquet"
        if dec_path.exists():
            dec_df = pd.read_parquet(dec_path)
            _verify_schema(dec_df, ref_path)

    # Continuity check: 2019-12 last ts + 1h == 2020-01 first ts
    dec_path = OUT_BASE / "12.parquet"
    jan20_path = OUT_BASE.parent / "2020" / "01.parquet"
    if dec_path.exists() and jan20_path.exists():
        dec_df = pd.read_parquet(dec_path)
        jan_df = pd.read_parquet(jan20_path)
        last_2019 = dec_df["open_time"].max()
        first_2020 = jan_df["open_time"].min()
        diff_h = (first_2020 - last_2019).total_seconds() / 3600
        status = "✓" if abs(diff_h - 1.0) < 0.01 else f"✗ gap={diff_h:.1f}h"
        print(f"\n  Continuity: 2019-12 last={last_2019} + {diff_h:.1f}h = 2020-01 first={first_2020} {status}")


if __name__ == "__main__":
    main()
