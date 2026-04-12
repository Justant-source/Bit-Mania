"""
Unified Data Loader for Backtests
===================================
모든 백테스트의 데이터 로딩을 담당하는 단일 인터페이스.
실데이터만 사용합니다. 데이터 부재 시 MissingDataError 발생 (합성 폴백 없음).

사용법:
    from tests.backtest.core.data_loader import DataLoader
    from tests.backtest.core.exceptions import MissingDataError

    loader = DataLoader()
    ohlcv = loader.load_klines("BTCUSDT", "1h", "2023-01-01", "2026-04-01")
    funding = loader.load_funding("BTCUSDT", "2023-01-01", "2026-04-01")
    fear_greed = loader.load_fear_greed("2023-01-01", "2026-04-01")
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .exceptions import DataQualityError, MissingDataError

# Default data root: relative to this file, go up to backtester/, then data/
_DEFAULT_DATA_ROOT = Path(__file__).parent.parent.parent.parent / "data"

INTERVAL_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

MAX_GAP_RATIO = 0.005  # 0.5% of total bars missing is acceptable


class DataLoader:
    """
    Unified Parquet-backed data loader for all backtests.

    Parameters
    ----------
    data_root : Path, optional
        Root directory containing data/ subdirectories.
        Defaults to `backtester/data/`.
    strict : bool
        If True, any gap in data raises DataQualityError.
        If False, logs warning only. Default: True.
    """

    def __init__(
        self,
        data_root: Optional[Path] = None,
        strict: bool = True,
    ):
        self.root = Path(data_root) if data_root else _DEFAULT_DATA_ROOT
        self.strict = strict

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def load_klines(
        self,
        symbol: str,
        interval: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Load OHLCV candlestick data.

        Returns DataFrame with columns:
            open_time (datetime64[ns, UTC]), open, high, low, close,
            volume, quote_volume, trade_count

        Raises
        ------
        MissingDataError
            If no Parquet files found for symbol/interval.
        DataQualityError
            If gap ratio exceeds threshold (in strict mode).
        """
        base = self.root / "binance_vision" / "klines" / symbol / interval
        files = sorted(base.glob("**/*.parquet")) if base.exists() else []

        if not files:
            raise MissingDataError(
                f"OHLCV 데이터 없음: {symbol} {interval}\n"
                f"예상 경로: {base}\n"
                f"실행: python scripts/data/fetch_binance_vision.py "
                f"--symbol {symbol} --intervals {interval}"
            )

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

        if "open_time" not in df.columns:
            raise DataQualityError(
                f"open_time 컬럼 없음: {symbol} {interval}. 데이터 재수집 필요."
            )

        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df = df.sort_values("open_time").reset_index(drop=True)

        # Filter by date range
        df = self._filter_date_range(df, "open_time", start, end)

        if df.empty:
            raise MissingDataError(
                f"범위 내 데이터 없음: {symbol} {interval} {start} ~ {end}"
            )

        # Validate gaps
        self._validate_gaps(df, "open_time", interval, symbol)

        cols = [c for c in ["open_time", "open", "high", "low", "close",
                             "volume", "quote_volume", "trade_count"] if c in df.columns]
        return df[cols].reset_index(drop=True)

    def load_funding(
        self,
        symbol: str,
        start: str,
        end: str,
        source: str = "binance",
    ) -> pd.DataFrame:
        """
        Load 8-hour funding rate data.

        Returns DataFrame with columns:
            timestamp (datetime64[ns, UTC]), funding_rate

        Raises
        ------
        MissingDataError
            If no funding data found.
        """
        if source == "binance":
            base = self.root / "binance_vision" / "funding" / symbol
            files = sorted(base.glob("*.parquet")) if base.exists() else []
            ts_col = "calc_time"
            rate_col = "last_funding_rate"
        else:
            raise ValueError(f"Unknown source: {source}. Use 'binance'.")

        if not files:
            raise MissingDataError(
                f"펀딩비 데이터 없음: {symbol} (source={source})\n"
                f"실행: python scripts/data/fetch_binance_vision.py --symbol {symbol}"
            )

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

        df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
        df = df.sort_values(ts_col).reset_index(drop=True)
        df = self._filter_date_range(df, ts_col, start, end)

        if df.empty:
            raise MissingDataError(
                f"범위 내 펀딩비 없음: {symbol} {start} ~ {end}"
            )

        return df.rename(columns={ts_col: "timestamp", rate_col: "funding_rate"})[
            ["timestamp", "funding_rate"]
        ].reset_index(drop=True)

    def load_fear_greed(self, start: str, end: str) -> pd.DataFrame:
        """
        Load Fear & Greed Index.

        Returns DataFrame with columns:
            timestamp (datetime64[ns, UTC]), value (int), classification (str)

        Raises
        ------
        MissingDataError
            If fear_greed_index.parquet not found.
        """
        path = self.root / "fear_greed" / "fear_greed_index.parquet"
        if not path.exists():
            raise MissingDataError(
                f"Fear & Greed 데이터 없음: {path}\n"
                f"실행: python scripts/data/fetch_fear_greed.py"
            )

        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return self._filter_date_range(df, "timestamp", start, end)

    def load_fred(self, series_id: str) -> pd.DataFrame:
        """
        Load FRED macro series.

        Returns DataFrame with columns:
            date (datetime64[ns]), {series_id.lower()} (float)

        Raises
        ------
        MissingDataError
            If the series Parquet not found.
        """
        path = self.root / "fred" / f"{series_id}.parquet"
        if not path.exists():
            raise MissingDataError(
                f"FRED 데이터 없음: {series_id}\n"
                f"실행: FRED_API_KEY=xxx python scripts/data/fetch_fred.py"
            )
        df = pd.read_parquet(path)
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)

    def load_open_interest(
        self,
        symbol: str,
        interval: str = "hourly",
    ) -> pd.DataFrame:
        """Load OI from Coinalyze data."""
        path = self.root / "coinalyze" / "open_interest" / f"{symbol}_{interval}.parquet"
        if not path.exists():
            raise MissingDataError(
                f"OI 데이터 없음: {symbol} {interval}\n"
                f"실행: python scripts/data/fetch_coinalyze.py"
            )
        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    def load_liquidations(
        self,
        symbol: str,
        interval: str = "hourly",
    ) -> pd.DataFrame:
        """Load liquidation data from Coinalyze."""
        path = self.root / "coinalyze" / "liquidations" / f"{symbol}_{interval}.parquet"
        if not path.exists():
            raise MissingDataError(
                f"청산 데이터 없음: {symbol} {interval}\n"
                f"실행: python scripts/data/fetch_coinalyze.py"
            )
        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    # ──────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────

    def _filter_date_range(
        self,
        df: pd.DataFrame,
        ts_col: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start, tz="UTC")
        end_ts = pd.Timestamp(end, tz="UTC")
        return df[(df[ts_col] >= start_ts) & (df[ts_col] <= end_ts)].copy()

    def _validate_gaps(
        self,
        df: pd.DataFrame,
        ts_col: str,
        interval: str,
        symbol: str,
    ):
        """Validate timestamp continuity. Raise DataQualityError if gaps exceed threshold."""
        if interval not in INTERVAL_MINUTES:
            return  # Skip validation for unknown intervals

        expected_delta = pd.Timedelta(minutes=INTERVAL_MINUTES[interval])
        diffs = df[ts_col].diff().dropna()
        gap_mask = diffs > expected_delta * 1.5
        gap_count = gap_mask.sum()
        gap_ratio = gap_count / max(len(df), 1)

        if gap_count > 0:
            first_gap = df[ts_col][gap_mask.values].iloc[0] if gap_count > 0 else None
            msg = (
                f"타임스탬프 갭 발견: {symbol} {interval}, "
                f"{gap_count}개 갭 ({gap_ratio:.2%}), "
                f"첫 갭: {first_gap}"
            )
            if self.strict and gap_ratio > MAX_GAP_RATIO:
                raise DataQualityError(msg)
            else:
                print(f"  [WARN] {msg}")
