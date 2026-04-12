#!/usr/bin/env python3
"""
Coinalyze API Data Fetcher
===========================
소스: https://coinalyze.net (무료 API, 키 불필요 — 일부 엔드포인트)
수집: BTC 오픈인터레스트(OI), 청산 히스토리
저장: data/coinalyze/ 하위 Parquet

사용법:
    python scripts/data/fetch_coinalyze.py
    python scripts/data/fetch_coinalyze.py --start 2023-01-01
"""

import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd

BASE_URL = "https://api.coinalyze.net/v1"
_data_root = Path(os.environ.get("BACKTEST_DATA_ROOT", "/app/results/data"))
DATA_ROOT = _data_root / "coinalyze"

# Rate limit: 1 req/sec for free tier
RATE_LIMIT_SLEEP = 1.1


def _get(endpoint: str, params: dict) -> dict:
    """GET request with rate limiting."""
    url = f"{BASE_URL}/{endpoint}"
    time.sleep(RATE_LIMIT_SLEEP)
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_open_interest(
    symbol: str = "BTCUSDT_PERP.A",
    interval: str = "1day",
    start_dt: datetime = None,
    end_dt: datetime = None,
) -> pd.DataFrame:
    """Fetch open interest history from Coinalyze."""
    if start_dt is None:
        start_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    if end_dt is None:
        end_dt = datetime.now(tz=timezone.utc)

    print(f"Fetching OI: {symbol} {interval} {start_dt.date()} ~ {end_dt.date()}")

    params = {
        "symbols": symbol,
        "interval": interval,
        "from": int(start_dt.timestamp()),
        "to": int(end_dt.timestamp()),
    }

    try:
        data = _get("open-interest-history", params)
        if not data or not isinstance(data, list):
            print(f"  [WARN] No OI data returned for {symbol}")
            return pd.DataFrame()

        records = data[0].get("history", []) if data else []
        df = pd.DataFrame(records)
        if df.empty:
            print(f"  [WARN] Empty OI history for {symbol}")
            return df

        df["t"] = pd.to_datetime(df["t"], unit="s", utc=True)
        df = df.rename(columns={"t": "timestamp", "o": "open_interest_usd"})
        print(f"  [OK] {len(df)} rows of OI data")
        return df

    except Exception as e:
        print(f"  [ERROR] Failed to fetch OI: {e}")
        return pd.DataFrame()


def fetch_liquidations(
    symbol: str = "BTCUSDT_PERP.A",
    interval: str = "1hour",
    start_dt: datetime = None,
    end_dt: datetime = None,
) -> pd.DataFrame:
    """Fetch liquidation history from Coinalyze."""
    if start_dt is None:
        start_dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    if end_dt is None:
        end_dt = datetime.now(tz=timezone.utc)

    print(f"Fetching Liquidations: {symbol} {interval} {start_dt.date()} ~ {end_dt.date()}")

    params = {
        "symbols": symbol,
        "interval": interval,
        "from": int(start_dt.timestamp()),
        "to": int(end_dt.timestamp()),
    }

    try:
        data = _get("liquidation-history", params)
        if not data or not isinstance(data, list):
            print(f"  [WARN] No liquidation data for {symbol}")
            return pd.DataFrame()

        records = data[0].get("history", []) if data else []
        df = pd.DataFrame(records)
        if df.empty:
            print(f"  [WARN] Empty liquidation history for {symbol}")
            return df

        df["t"] = pd.to_datetime(df["t"], unit="s", utc=True)
        df = df.rename(columns={
            "t": "timestamp",
            "l": "long_liquidations_usd",
            "s": "short_liquidations_usd",
        })
        df["total_liquidations_usd"] = (
            df["long_liquidations_usd"] + df["short_liquidations_usd"]
        )
        print(f"  [OK] {len(df)} rows of liquidation data")
        return df

    except Exception as e:
        print(f"  [ERROR] Failed to fetch liquidations: {e}")
        return pd.DataFrame()


def save_parquet(df: pd.DataFrame, path: Path):
    """Save DataFrame as Parquet with ZSTD compression."""
    if df.empty:
        print(f"  [SKIP] Empty DataFrame, not saving to {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="zstd", index=False)
    print(f"  [SAVED] {len(df)} rows → {path}")


def main(start: str = "2020-01-01"):
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.now(tz=timezone.utc)

    symbol = "BTCUSDT_PERP.A"

    print(f"\n=== Coinalyze Data Fetcher ===")
    print(f"Symbol: {symbol}, Range: {start_dt.date()} ~ {end_dt.date()}\n")

    # Open Interest (daily)
    oi_df = fetch_open_interest(symbol, "1day", start_dt, end_dt)
    save_parquet(oi_df, DATA_ROOT / "open_interest" / "BTCUSDT_daily.parquet")

    # Open Interest (hourly)
    oi_hourly = fetch_open_interest(symbol, "1hour", start_dt, end_dt)
    save_parquet(oi_hourly, DATA_ROOT / "open_interest" / "BTCUSDT_hourly.parquet")

    # Liquidations (hourly)
    liq_df = fetch_liquidations(symbol, "1hour", start_dt, end_dt)
    save_parquet(liq_df, DATA_ROOT / "liquidations" / "BTCUSDT_hourly.parquet")

    print("\n=== Coinalyze fetch complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2020-01-01", help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.start)
