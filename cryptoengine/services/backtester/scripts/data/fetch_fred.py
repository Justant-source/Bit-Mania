#!/usr/bin/env python3
"""
FRED Macro Data Fetcher
========================
소스: Federal Reserve Economic Data (FRED)
API: https://fred.stlouisfed.org (무료, 즉시 발급)
시리즈: DFF, DGS10, WALCL, CPIAUCSL
저장: data/fred/{series_id}.parquet

API 키 발급: https://fred.stlouisfed.org/docs/api/api_key.html

사용법:
    export FRED_API_KEY=your_key_here
    python scripts/data/fetch_fred.py

환경변수 없이 실행 시 .env 파일에서 읽기 시도.
"""

import os
import time
from pathlib import Path

import requests
import pandas as pd

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
_data_root = Path(os.environ.get("BACKTEST_DATA_ROOT", "/app/results/data"))
DATA_ROOT = _data_root / "fred"

SERIES = {
    "DFF":     "Federal Funds Rate (daily)",
    "DGS10":   "10-Year Treasury Yield (daily)",
    "WALCL":   "Fed Balance Sheet (weekly)",
    "CPIAUCSL": "CPI All Urban (monthly)",
}


def get_api_key() -> str:
    """Get FRED API key from env or .env file."""
    key = os.environ.get("FRED_API_KEY", "")
    if not key:
        env_file = Path(__file__).parent.parent.parent.parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("FRED_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
    return key


def fetch_series(series_id: str, api_key: str, start: str = "2018-01-01") -> pd.DataFrame:
    """Fetch a single FRED series."""
    print(f"Fetching FRED {series_id}: {SERIES.get(series_id, '?')}")
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": "9999-12-31",
        "sort_order": "asc",
    }
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] Failed to fetch {series_id}: {e}")
        return pd.DataFrame()

    observations = data.get("observations", [])
    if not observations:
        print(f"  [WARN] No observations for {series_id}")
        return pd.DataFrame()

    df = pd.DataFrame(observations)[["date", "value"]]
    df = df[df["value"] != "."]  # Remove missing values
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = df["value"].astype(float)
    df = df.rename(columns={"value": series_id.lower()})
    print(f"  [OK] {len(df)} observations ({df['date'].min().date()} ~ {df['date'].max().date()})")
    return df


def main():
    api_key = get_api_key()
    if not api_key:
        print("[ERROR] FRED_API_KEY not set.")
        print("Get free key: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("Then: export FRED_API_KEY=your_key_here")
        return

    print(f"\n=== FRED Macro Data Fetcher ===")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    for series_id in SERIES:
        df = fetch_series(series_id, api_key)
        if not df.empty:
            out = DATA_ROOT / f"{series_id}.parquet"
            df.to_parquet(out, compression="zstd", index=False)
            print(f"  [SAVED] → {out}")
        time.sleep(0.3)  # Be polite to FRED API

    print("\n=== FRED fetch complete ===")


if __name__ == "__main__":
    main()
