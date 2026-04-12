#!/usr/bin/env python3
"""
Fear & Greed Index Fetcher
===========================
소스: https://api.alternative.me/fng/ (무료, 키 불필요)
전체 히스토리 단 한 번 호출로 수집 가능 (limit=0)
저장: data/fear_greed/fear_greed_index.parquet

사용법:
    python scripts/data/fetch_fear_greed.py
"""

import os
from pathlib import Path

import requests
import pandas as pd

URL = "https://api.alternative.me/fng/?limit=0&format=json"
_data_root = Path(os.environ.get("BACKTEST_DATA_ROOT", "/app/results/data"))
DATA_ROOT = _data_root / "fear_greed"


def fetch_fear_greed() -> pd.DataFrame:
    """Fetch full Fear & Greed Index history."""
    print("Fetching Fear & Greed Index (full history)...")
    try:
        resp = requests.get(URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[ERROR] {e}")
        return pd.DataFrame()

    records = data.get("data", [])
    if not records:
        print("[WARN] No data in response")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
    df["value"] = df["value"].astype(int)
    df = df.rename(columns={"value_classification": "classification"})
    df = df[["timestamp", "value", "classification"]].sort_values("timestamp").reset_index(drop=True)

    print(f"[OK] {len(df)} days ({df['timestamp'].min().date()} ~ {df['timestamp'].max().date()})")
    return df


def main():
    print(f"\n=== Fear & Greed Index Fetcher ===")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    df = fetch_fear_greed()
    if not df.empty:
        out = DATA_ROOT / "fear_greed_index.parquet"
        df.to_parquet(out, compression="zstd", index=False)
        print(f"[SAVED] {len(df)} rows → {out}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
