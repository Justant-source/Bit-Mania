#!/usr/bin/env python3
"""
Binance Vision Historical Data Downloader
=========================================
소스: https://data.binance.vision (무료, API 키 불필요)
다운로드: BTCUSDT 선물 1m/1h/1d OHLCV + 펀딩비 히스토리
저장: data/binance_vision/ 하위에 Parquet 형식 (ZSTD 압축)

사용법:
    python scripts/data/fetch_binance_vision.py
    python scripts/data/fetch_binance_vision.py --symbol BTCUSDT --start 2020-01
    python scripts/data/fetch_binance_vision.py --intervals 1h,1d --start 2023-01
"""

import argparse
import io
import os
import zipfile
from datetime import date
from pathlib import Path

import requests
import pandas as pd

BASE_URL = "https://data.binance.vision/data/futures/um/monthly"
_data_root = Path(os.environ.get("BACKTEST_DATA_ROOT", "/app/results/data"))
DATA_ROOT = _data_root / "binance_vision"

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]

FUNDING_COLUMNS = [
    "calc_time", "funding_interval_hours", "last_funding_rate",
]


def download_klines(symbol: str, interval: str, year: int, month: int, output_dir: Path) -> bool:
    """Download monthly klines ZIP, extract CSV, save as Parquet."""
    filename = f"{symbol}-{interval}-{year}-{month:02d}"
    url = f"{BASE_URL}/klines/{symbol}/{interval}/{filename}.zip"
    out_path = output_dir / f"{year}" / f"{month:02d}.parquet"

    if out_path.exists():
        print(f"  [SKIP] {filename} already exists")
        return True

    print(f"  [DOWNLOAD] {url}")
    try:
        resp = requests.get(url, timeout=60, allow_redirects=True)
        if resp.status_code == 404:
            print(f"  [WARN] Not found (404): {filename}")
            return False
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] HTTP error: {e}")
        return False

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = f"{filename}.csv"
        if csv_name not in zf.namelist():
            csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=None, names=KLINE_COLUMNS)

    # 일부 파일은 헤더 행 포함 (2021-09 이후) → 숫자 변환 불가 행 제거
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df = df.dropna(subset=["open_time"])
    df["open_time"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms", utc=True)

    df["close_time"] = pd.to_numeric(df["close_time"], errors="coerce")
    df = df.dropna(subset=["close_time"])
    df["close_time"] = pd.to_datetime(df["close_time"].astype("int64"), unit="ms", utc=True)
    df = df.drop(columns=["ignore"])

    # Cast numeric columns
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base_volume", "taker_buy_quote_volume"]:
        df[col] = df[col].astype(float)
    df["trade_count"] = df["trade_count"].astype(int)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, compression="zstd", index=False)
    print(f"  [OK] Saved {len(df)} rows → {out_path}")
    return True


def download_funding(symbol: str, year: int, month: int, output_dir: Path) -> bool:
    """Download monthly funding rate history."""
    filename = f"{symbol}-fundingRate-{year}-{month:02d}"
    url = f"{BASE_URL}/fundingRate/{symbol}/{filename}.zip"
    out_path = output_dir / f"{year}-{month:02d}.parquet"

    if out_path.exists():
        print(f"  [SKIP] {filename} already exists")
        return True

    print(f"  [DOWNLOAD] {url}")
    try:
        resp = requests.get(url, timeout=60, allow_redirects=True)
        if resp.status_code == 404:
            print(f"  [WARN] Not found (404): {filename}")
            return False
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] HTTP error: {e}")
        return False

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=None, names=FUNDING_COLUMNS)

    # 헤더 행 포함 파일 대응 — 숫자 변환 불가 행 제거
    df["calc_time"] = pd.to_numeric(df["calc_time"], errors="coerce")
    df = df.dropna(subset=["calc_time"])
    df["calc_time"] = pd.to_datetime(df["calc_time"].astype("int64"), unit="ms", utc=True)
    df["last_funding_rate"] = pd.to_numeric(df["last_funding_rate"], errors="coerce")
    df["funding_interval_hours"] = pd.to_numeric(df["funding_interval_hours"], errors="coerce").fillna(8).astype(int)

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, compression="zstd", index=False)
    print(f"  [OK] Saved {len(df)} rows → {out_path}")
    return True


def backfill_all(
    symbol: str = "BTCUSDT",
    intervals: list = None,
    start_year: int = 2020,
    start_month: int = 1,
    include_funding: bool = True,
):
    """Download all months from start_year/month to current month."""
    if intervals is None:
        intervals = ["1m", "1h", "1d"]

    today = date.today()
    print(f"\n=== Binance Vision Downloader ===")
    print(f"Symbol: {symbol}, Intervals: {intervals}")
    print(f"Range: {start_year}-{start_month:02d} ~ {today.year}-{today.month:02d}")
    print(f"Output: {DATA_ROOT}\n")

    # Download OHLCV
    for interval in intervals:
        print(f"\n[{interval}] OHLCV download starting...")
        out_dir = DATA_ROOT / "klines" / symbol / interval
        y, m = start_year, start_month
        while (y, m) <= (today.year, today.month):
            download_klines(symbol, interval, y, m, out_dir)
            m += 1
            if m > 12:
                m = 1
                y += 1

    # Download Funding
    if include_funding:
        print(f"\n[funding] Funding rate download starting...")
        out_dir = DATA_ROOT / "funding" / symbol
        y, m = start_year, start_month
        while (y, m) <= (today.year, today.month):
            download_funding(symbol, y, m, out_dir)
            m += 1
            if m > 12:
                m = 1
                y += 1

    print("\n=== Download complete ===")


def parse_ym(s: str):
    """Parse 'YYYY-MM' string."""
    parts = s.split("-")
    return int(parts[0]), int(parts[1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance Vision bulk downloader")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--intervals", default="1m,1h,1d")
    parser.add_argument("--start", default="2020-01", help="YYYY-MM")
    parser.add_argument("--no-funding", action="store_true")
    args = parser.parse_args()

    start_y, start_m = parse_ym(args.start)
    intervals = [i.strip() for i in args.intervals.split(",")]

    backfill_all(
        symbol=args.symbol,
        intervals=intervals,
        start_year=start_y,
        start_month=start_m,
        include_funding=not args.no_funding,
    )
