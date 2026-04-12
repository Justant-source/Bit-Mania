"""
Phase 8.3 — Fear & Greed Contrarian Sentiment Strategy (Jesse)

Two variants:
  Variant A: ContrarianSentimentStandalone — buy extreme fear, sell extreme greed
  Variant B: SeasonalityWithFGFilter — IntradaySeasonality filtered by F&G range

Historical evidence:
  - Nov 2021: F&G 84 (extreme greed) → BTC -77% over following year
  - Jun 2022: F&G 6 (extreme fear)  → BTC +300%+ over following year
  - Standalone vs. filter: Calmar 1.5-2x improvement when used as filter (research consensus)

Data requirement:
    /data/sentiment/fear_greed.parquet
    Columns: timestamp_ms (int, daily), value (int, 0-100), classification (str)

Run:
    # Fetch F&G data first
    python scripts/data/fetch_fear_greed.py

    # Variant A
    ./scripts/run_full_validation.sh ContrarianSentimentStandalone

    # Variant B (only if IntradaySeasonality PASS)
    ./scripts/run_full_validation.sh SeasonalityWithFGFilter

V5 Pass Criteria (Variant A):
  - CAGR ≥ 10%, Sharpe ≥ 1.0, MDD ≤ 15%
  - WF OOS Sharpe ≥ 0.6 × IS Sharpe
  - Monte Carlo 5th percentile Sharpe > 0
  - ≥ 30 trades/year

V5 Pass Criteria (Variant B):
  - All of Variant A criteria PLUS
  - Sharpe improvement ≥ 20% vs. IntradaySeasonality standalone
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import jesse.helpers as jh
from jesse import indicators as ta
from jesse.strategies import Strategy

FG_DATA_PATH = os.environ.get("FG_DATA_PATH", "/data/sentiment/fear_greed.parquet")

# F&G threshold constants
EXTREME_FEAR_THRESHOLD  = 25   # F&G < 25 → extreme fear → contrarian BUY
EXTREME_GREED_THRESHOLD = 75   # F&G > 75 → extreme greed → contrarian SELL
NEUTRAL_LOW  = 25              # For filter: below 25 = extreme (skip intraday)
NEUTRAL_HIGH = 75              # For filter: above 75 = extreme (skip intraday)


# ─── Shared F&G data loader ────────────────────────────────────────────────────

class _FearGreedLoader:
    """
    Singleton-like loader for Fear & Greed index data.
    Loaded once and cached in class variable.
    """
    _data: Optional[dict] = None  # timestamp_ms (daily) → value (int)
    _path: str = FG_DATA_PATH

    @classmethod
    def load(cls) -> dict[int, int]:
        if cls._data is not None:
            return cls._data

        path = Path(cls._path)
        if not path.exists():
            raise FileNotFoundError(
                f"Fear & Greed data not found: {path}\n"
                "Run: python scripts/data/fetch_fear_greed.py first.\n"
                "Synthetic data fallback DISABLED (V5 rules)."
            )

        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars required. Run: pip install polars")

        df = pl.read_parquet(path)

        # Normalize column names
        rename = {}
        for col in df.columns:
            low = col.lower()
            if low in ("timestamp", "timestamp_ms", "time", "ts", "date"):
                rename[col] = "timestamp_ms"
            elif low in ("value", "score", "index", "fear_greed", "fng_value"):
                rename[col] = "value"
        if rename:
            df = df.rename(rename)

        required = {"timestamp_ms", "value"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Fear & Greed parquet missing: {missing}. "
                f"Available columns: {df.columns}"
            )

        # Convert to integer milliseconds (handles both datetime and int types)
        col_dtype = str(df["timestamp_ms"].dtype)
        if "Datetime" in col_dtype or "Date" in col_dtype:
            # datetime[ms, UTC] → epoch ms integer
            df = df.with_columns(
                pl.col("timestamp_ms").dt.epoch("ms").alias("timestamp_ms")
            )
        elif df["timestamp_ms"].max() < 1e12:
            # Unix seconds → milliseconds
            df = df.with_columns(
                (pl.col("timestamp_ms") * 1000).cast(pl.Int64)
            )

        cls._data = {
            int(row["timestamp_ms"]): int(row["value"])
            for row in df.iter_rows(named=True)
        }
        return cls._data

    @classmethod
    def get_value_at(cls, timestamp_ms: int) -> Optional[int]:
        """
        Return F&G value for the day containing timestamp_ms.
        Uses floor-to-day alignment (F&G is daily).
        Returns None if no data available.
        """
        data = cls.load()

        # Align to start of day (UTC midnight)
        day_start_ms = (timestamp_ms // 86_400_000) * 86_400_000

        # Exact match
        if day_start_ms in data:
            return data[day_start_ms]

        # Look back up to 3 days for most recent value
        for offset_days in range(1, 4):
            prev_ms = day_start_ms - offset_days * 86_400_000
            if prev_ms in data:
                return data[prev_ms]

        return None


# ─── Variant A: Standalone Contrarian Strategy ────────────────────────────────

class ContrarianSentimentStandalone(Strategy):
    """
    Variant A: Pure F&G contrarian strategy.

    Entry rules:
      - F&G < 25 (extreme fear): BUY with 25% capital, hold until F&G > 45 or +60 days
      - F&G > 75 (extreme greed): EXIT long if held, or SHORT with 15% capital

    Design notes:
    - F&G is daily; strategy uses 1h candles but signals are day-level
    - Position held until sentiment normalizes, NOT a time-exit strategy
    - Conservative leverage (1x default) because MDD risk is higher in extremes
    """

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "fear_threshold",      "type": int,   "min": 15, "max": 35, "default": 25},
            {"name": "greed_threshold",     "type": int,   "min": 65, "max": 85, "default": 75},
            {"name": "exit_fear_below",     "type": int,   "min": 35, "max": 55, "default": 45},
            {"name": "position_size_pct",   "type": float, "min": 0.10, "max": 0.40, "default": 0.25},
            {"name": "short_size_pct",      "type": float, "min": 0.05, "max": 0.25, "default": 0.15},
            {"name": "max_hold_days",       "type": int,   "min": 30,  "max": 90,  "default": 60},
            {"name": "leverage",            "type": int,   "min": 1,   "max": 3,   "default": 1},
        ]

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def _fg(self) -> Optional[int]:
        """Current Fear & Greed value (0-100, daily resolution)."""
        return _FearGreedLoader.get_value_at(int(self.current_candle[0]))

    @property
    def _is_first_candle_of_day(self) -> bool:
        """Only process signals on first hourly candle of each UTC day."""
        arrow = jh.timestamp_to_arrow(self.current_candle[0])
        return arrow.hour == 0

    # ── Signal logic ───────────────────────────────────────────────────────────

    def should_long(self) -> bool:
        if self.position.is_open:
            return False
        if not self._is_first_candle_of_day:
            return False
        fg = self._fg
        if fg is None:
            return False
        return fg < self.hp["fear_threshold"]

    def should_short(self) -> bool:
        if self.position.is_open:
            return False
        if not self._is_first_candle_of_day:
            return False
        fg = self._fg
        if fg is None:
            return False
        return fg > self.hp["greed_threshold"]

    def go_long(self) -> None:
        qty = (self.balance * self.hp["position_size_pct"] * self.hp["leverage"]) / self.price
        self.buy = qty, self.price

    def go_short(self) -> None:
        qty = (self.balance * self.hp["short_size_pct"]) / self.price
        sl  = self.price * 1.08  # hard 8% stop on shorts
        self.sell      = qty, self.price
        self.stop_loss = qty, sl

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self) -> None:
        """
        Exit conditions:
        1. F&G normalized past exit threshold (for longs: F&G > exit_fear_below)
        2. Max hold days exceeded
        3. F&G extreme greed on a long position → exit
        """
        if not self._is_first_candle_of_day:
            return

        fg = self._fg
        if fg is None:
            return

        # Exit long if F&G recovered past neutral or entered extreme greed
        if self.position.type == "long":
            if fg > self.hp["exit_fear_below"]:
                self.liquidate()
                return
            if fg > self.hp["greed_threshold"]:
                self.liquidate()
                return

        # Check max hold days
        hold_ms = self.current_candle[0] - self.position.opened_at
        hold_days = hold_ms / (86_400_000)
        if hold_days > self.hp["max_hold_days"]:
            self.liquidate()


# ─── Variant B: Seasonality + F&G Filter Overlay ─────────────────────────────

class SeasonalityWithFGFilter(Strategy):
    """
    Variant B: IntradaySeasonality + Fear & Greed filter.

    Same time-based entry/exit as IntradaySeasonality (UTC 21:00 → 23:00),
    but only enters when F&G is in the neutral zone (25 < F&G < 75).

    Hypothesis: Avoiding extreme sentiment periods reduces drawdown
    and improves Sharpe by ~20-30%.

    IMPORTANT: Only backtest this if IntradaySeasonality PASSES V5 criteria.
    If seasonality alone fails, this variant has no basis.
    """

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "entry_hour_utc",      "type": int,   "min": 19,  "max": 22,  "default": 21},
            {"name": "exit_hour_utc",       "type": int,   "min": 22,  "max": 24,  "default": 23},
            {"name": "fg_low_threshold",    "type": int,   "min": 15,  "max": 35,  "default": 25},
            {"name": "fg_high_threshold",   "type": int,   "min": 65,  "max": 85,  "default": 75},
            {"name": "use_trend_filter",    "type": bool,  "default": True},
            {"name": "use_dow_filter",      "type": bool,  "default": True},
            {"name": "position_size_pct",   "type": float, "min": 0.10, "max": 0.50, "default": 0.25},
            {"name": "leverage",            "type": int,   "min": 1,   "max": 3,   "default": 2},
            {"name": "atr_stop_multiplier", "type": float, "min": 1.0, "max": 4.0, "default": 2.0},
        ]

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def _fg_neutral(self) -> bool:
        """True if F&G is in neutral zone (neither extreme fear nor greed)."""
        fg = _FearGreedLoader.get_value_at(int(self.current_candle[0]))
        if fg is None:
            return True  # no data → allow trade (conservative)
        return self.hp["fg_low_threshold"] < fg < self.hp["fg_high_threshold"]

    @property
    def _current_hour_utc(self) -> int:
        return jh.timestamp_to_arrow(self.current_candle[0]).hour

    @property
    def _current_dow(self) -> int:
        return jh.timestamp_to_arrow(self.current_candle[0]).weekday()

    @property
    def _trend_up(self) -> bool:
        if not self.hp["use_trend_filter"]:
            return True
        # Use 1200-period SMA from 1h candles (50 days × 24h) — avoids 1D data_route
        if len(self.candles) < 1200:
            return True
        return self.price > ta.sma(self.candles, 1200, "close")

    @property
    def _dow_ok(self) -> bool:
        if not self.hp["use_dow_filter"]:
            return True
        return self._current_dow in {3, 4}

    # ── Entry/Exit ─────────────────────────────────────────────────────────────

    def should_long(self) -> bool:
        if self.position.is_open:
            return False
        if self._current_hour_utc != self.hp["entry_hour_utc"]:
            return False
        if not self._dow_ok:
            return False
        if not self._trend_up:
            return False
        # KEY FILTER: skip if F&G is at extreme
        if not self._fg_neutral:
            return False
        return True

    def should_short(self) -> bool:
        return False

    def go_long(self) -> None:
        notional = self.balance * self.hp["position_size_pct"] * self.hp["leverage"]
        qty = notional / self.price
        atr = ta.atr(self.candles, 14)
        stop_price = max(
            self.price - atr * self.hp["atr_stop_multiplier"],
            self.price * 0.95
        )
        self.buy       = qty, self.price
        self.stop_loss = qty, stop_price

    def go_short(self) -> None:
        pass

    def should_cancel_entry(self) -> bool:
        return self._current_hour_utc != self.hp["entry_hour_utc"]

    def update_position(self) -> None:
        """Hard time exit at configured exit hour."""
        if self._current_hour_utc >= self.hp["exit_hour_utc"]:
            self.liquidate()
