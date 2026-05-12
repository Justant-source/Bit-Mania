"""Coinalyze API client for historical funding rates and OHLCV.

Free tier: 60 requests/min, 6-year history.
Docs: https://coinalyze.net/api-docs/
"""
import os
import time
from datetime import datetime
from typing import Literal, Optional

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

API_KEY = os.getenv("COINALYZE_API_KEY")
BASE_URL = "https://api.coinalyze.net/v1"
RATE_LIMIT_PER_MIN = 60


class CoinalyzeClient:
    def __init__(self, api_key: str = API_KEY):
        if not api_key:
            raise ValueError("COINALYZE_API_KEY not set in .env")
        self.api_key = api_key
        self.client = httpx.Client(
            headers={"api_key": api_key},
            timeout=30.0,
        )
        self._last_request = 0.0
        self._min_interval = 60.0 / RATE_LIMIT_PER_MIN

    def _throttle(self):
        elapsed = time.time() - self._last_request
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request = time.time()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch_funding_rate_history(
        self,
        symbol: str,
        exchange: str,
        start: datetime,
        end: datetime,
        interval: Literal["8h", "1h"] = "8h",
    ) -> pd.DataFrame:
        """과거 펀딩비 데이터 수집."""
        self._throttle()
        params = {
            "symbols": f"{symbol}.{exchange.upper()}",
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "interval": interval,
        }
        r = self.client.get(f"{BASE_URL}/funding-rate-history", params=params)
        r.raise_for_status()
        data = r.json()
        if not data:
            return pd.DataFrame()
        rows = []
        for sym_data in data:
            for entry in sym_data.get("history", []):
                rows.append({
                    "symbol": symbol,
                    "exchange": exchange,
                    "timestamp": pd.to_datetime(entry["t"], unit="s", utc=True),
                    "rate": entry["c"],
                })
        return pd.DataFrame(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch_ohlcv_history(
        self,
        symbol: str,
        exchange: str,
        start: datetime,
        end: datetime,
        interval: Literal["1h", "4h", "1d"] = "1h",
    ) -> pd.DataFrame:
        """과거 OHLCV 데이터 수집."""
        self._throttle()
        params = {
            "symbols": f"{symbol}.{exchange.upper()}",
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "interval": interval,
        }
        r = self.client.get(f"{BASE_URL}/ohlcv-history", params=params)
        r.raise_for_status()
        data = r.json()
        rows = []
        for sym_data in data:
            for c in sym_data.get("history", []):
                rows.append({
                    "symbol": symbol,
                    "exchange": exchange,
                    "timestamp": pd.to_datetime(c["t"], unit="s", utc=True),
                    "open": c["o"],
                    "high": c["h"],
                    "low": c["l"],
                    "close": c["c"],
                    "volume": c["v"],
                })
        return pd.DataFrame(rows)
