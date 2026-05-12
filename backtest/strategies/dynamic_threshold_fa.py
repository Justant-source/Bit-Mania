"""
Track B — Dynamic Threshold Funding Arbitrage (FA) Strategy

Extends single-symbol FA with adaptive entry/exit thresholds based on recent
funding rate distribution (rolling 30-day percentile).

Rationale:
  - Static threshold (8% APR) was fitted to 2023 market conditions
  - 2024+ market compressed fundingbeta: static threshold = fewer entries
  - Solution: dynamically adjust threshold = higher entry frequency + lower hold time

Entry rules:
  - threshold_apr = max(p75(recent_positive_rates[30d]), min_absolute_threshold)
  - Entry when: funding_rate >= threshold for consecutive_intervals periods

Position management:
  - Same as FundingArbitrage: hold until rate flips or max_hold_bars exceeded
  - Settlement: credit funding income at 8h boundaries

Data requirement:
    /data/funding_rates/BTCUSDT_8h.parquet or .csv
    Columns: timestamp_ms (int), rate (float, 8h funding rate)

Run:
    python scripts/run_fa_backtest.py \
        --strategy DynamicThresholdFA \
        --start 2020-01-01 --end 2026-04-30 \
        --balance 10000 --fee 0.00055 --leverage 5

Backtest pass criteria (6-year):
  - CAGR >= 25% (vs +13.11% baseline with static 8% APR)
  - Sharpe >= 3.2
  - MDD <= -6%
  - 2024-2026 return >= +8% (vs +1.64% baseline)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from collections import deque

import numpy as np
import jesse.helpers as jh
from jesse import indicators as ta
from jesse.strategies import Strategy

FA_FUNDING_PATH = os.environ.get(
    "FA_FUNDING_PATH", "/data/funding_rates/BTCUSDT_8h.parquet"
)


# ─── Funding rate data loader (same as base FA) ────────────────────────────────

class _FundingRateLoader:
    """
    Singleton-like loader for 8h funding rate data.
    Supports both parquet (.parquet) and CSV (.csv) formats.
    """
    _data: Optional[dict] = None
    _path: str = FA_FUNDING_PATH

    @classmethod
    def load(cls) -> dict[int, float]:
        if cls._data is not None:
            return cls._data

        path = Path(cls._path)

        if path.suffix == ".parquet" and path.exists():
            return cls._load_parquet(path)

        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            return cls._load_csv(csv_path)

        raise FileNotFoundError(
            f"Funding rate data not found: {path} (or .csv variant)\n"
            "Expected: /data/funding_rates/BTCUSDT_8h.parquet or .csv"
        )

    @classmethod
    def _load_parquet(cls, path: Path) -> dict[int, float]:
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars required. Run: pip install polars")

        df = pl.read_parquet(path)

        rename = {}
        for col in df.columns:
            low = col.lower()
            if low in ("timestamp", "timestamp_ms", "time", "ts"):
                rename[col] = "timestamp_ms"
            elif low in ("rate", "value", "funding_rate", "funding"):
                rename[col] = "rate"
        if rename:
            df = df.rename(rename)

        required = {"timestamp_ms", "rate"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Funding parquet missing columns: {missing}")

        col_dtype = str(df["timestamp_ms"].dtype)
        if "Datetime" in col_dtype or "Date" in col_dtype:
            df = df.with_columns(
                pl.col("timestamp_ms").dt.epoch("ms").alias("timestamp_ms")
            )

        cls._data = {
            int(row["timestamp_ms"]): float(row["rate"])
            for row in df.iter_rows(named=True)
        }
        return cls._data

    @classmethod
    def _load_csv(cls, path: Path) -> dict[int, float]:
        import csv

        data = {}
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts_key = next(
                        k for k in row.keys()
                        if k.lower() in ("timestamp_ms", "timestamp", "ts", "time")
                    )
                    rate_key = next(
                        k for k in row.keys()
                        if k.lower() in ("rate", "funding_rate", "funding", "value")
                    )
                    ts_ms = int(row[ts_key])
                    rate = float(row[rate_key])
                    data[ts_ms] = rate
                except (StopIteration, ValueError, KeyError):
                    continue

        if not data:
            raise ValueError(f"No funding rate data loaded from CSV: {path}")

        cls._data = data
        return cls._data

    @classmethod
    def get_rate_at(cls, timestamp_ms: int) -> Optional[float]:
        """Get funding rate for 8h period containing timestamp_ms."""
        data = cls.load()

        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        hour = dt.hour
        settlement_hour = (hour // 8) * 8
        settlement_dt = dt.replace(hour=settlement_hour, minute=0, second=0, microsecond=0)
        settlement_ms = int(settlement_dt.timestamp() * 1000)

        if settlement_ms in data:
            return data[settlement_ms]

        for periods_back in range(1, 4):
            prev_ms = settlement_ms - periods_back * 8 * 3600 * 1000
            if prev_ms in data:
                return data[prev_ms]

        return None

    @classmethod
    def reset(cls):
        cls._data = None


# ─── Dynamic Threshold FA Strategy ─────────────────────────────────────────────

class DynamicThresholdFA(Strategy):
    """
    Funding Arbitrage with dynamic threshold based on rolling 30-day distribution.

    Entry:
      - Compute threshold = max(p75(recent positive rates), min_absolute_threshold)
      - Open position when rate >= threshold for consecutive_intervals periods

    Position management:
      - Identical to base FundingArbitrage
      - Track funding income, exit on reversal or max hold

    Hyperparameters:
      - min_absolute_threshold: floor threshold to avoid overfitting (default 0.0005 = 5% APR)
      - window_days: lookback period for percentile calculation (default 30)
      - window_slots: number of 8h settlement periods in window (default 90)
    """

    def __init__(self):
        super().__init__()
        self._consecutive_positive = 0
        self._consecutive_negative = 0
        self._settlement_hour_last = -1
        self._funding_direction = 0
        self._reverse_count = 0

        # Rolling window for dynamic threshold
        self.recent_rates: deque = deque(maxlen=90)  # ~30 days of 8h periods

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "min_absolute_threshold",   "type": float, "min": 0.0001, "max": 0.001,   "default": 0.0005},
            {"name": "window_days",              "type": int,   "min": 7,      "max": 90,      "default": 30},
            {"name": "window_slots",             "type": int,   "min": 24,     "max": 270,     "default": 90},
            {"name": "consecutive_intervals",    "type": int,   "min": 1,      "max": 10,      "default": 3},
            {"name": "fa_allocation_pct",        "type": float, "min": 0.10,   "max": 1.0,     "default": 0.80},
            {"name": "leverage",                 "type": int,   "min": 1,      "max": 10,      "default": 5},
            {"name": "max_hold_bars",            "type": int,   "min": 24,     "max": 1000,    "default": 168},
            {"name": "exit_reverse_count",       "type": int,   "min": 1,      "max": 10,      "default": 3},
            {"name": "reinvest_pct",             "type": float, "min": 0.0,    "max": 1.0,     "default": 0.30},
        ]

    def _get_dynamic_threshold(self) -> float:
        """
        Calculate dynamic entry threshold as p75 of recent positive rates.
        Falls back to min_absolute_threshold if insufficient data.
        """
        if len(self.recent_rates) < 10:
            return self.hp["min_absolute_threshold"]

        positive_rates = [r for r in self.recent_rates if r > 0]
        if not positive_rates:
            return self.hp["min_absolute_threshold"]

        p75 = np.percentile(positive_rates, 75)
        return max(float(p75), self.hp["min_absolute_threshold"])

    @property
    def _current_funding_rate(self) -> Optional[float]:
        return _FundingRateLoader.get_rate_at(int(self.current_candle[0]))

    @property
    def _is_settlement_candle(self) -> bool:
        arrow = jh.timestamp_to_arrow(self.current_candle[0])
        return arrow.hour in (0, 8, 16)

    @property
    def _bars_held(self) -> int:
        if not self.position.is_open:
            return 0
        return len(self.candles) - (len(self.candles) - self.position.bars_count)

    def should_long(self) -> bool:
        """Entry: rate >= dynamic_threshold for consecutive_intervals."""
        if self.position.is_open:
            return False

        if not self._is_settlement_candle:
            return False

        rate = self._current_funding_rate
        if rate is None:
            return False

        # Update rolling window
        self.recent_rates.append(rate)

        threshold = self._get_dynamic_threshold()
        if rate < threshold:
            return False

        self._consecutive_positive += 1
        self._consecutive_negative = 0

        return self._consecutive_positive >= self.hp["consecutive_intervals"]

    def should_short(self) -> bool:
        return False

    def go_long(self) -> None:
        """Enter position with dynamic threshold."""
        notional = self.balance * self.hp["fa_allocation_pct"] * self.hp["leverage"]
        qty = notional / self.price

        self._funding_direction = 1
        self._reverse_count = 0

        self.buy = qty, self.price

        if "cumulative_funding" not in self.shared_vars:
            self.shared_vars["cumulative_funding"] = 0.0
        if "dynamic_threshold_at_entry" not in self.shared_vars:
            self.shared_vars["dynamic_threshold_at_entry"] = self._get_dynamic_threshold()

    def go_short(self) -> None:
        pass

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self) -> None:
        """Position management with dynamic threshold."""
        if not self.position.is_open:
            self._consecutive_positive = 0
            self._consecutive_negative = 0
            return

        rate = self._current_funding_rate
        if rate is None:
            return

        # Settlement: credit funding income
        if self._is_settlement_candle:
            # Update rolling window
            self.recent_rates.append(rate)

            position_value = self.position.quantity * self.price
            funding_income = position_value * rate * self._funding_direction

            self.shared_vars["cumulative_funding"] = (
                self.shared_vars.get("cumulative_funding", 0.0) + funding_income
            )

            # Check for reversal
            is_reversed = (
                (self._funding_direction > 0 and rate < 0) or
                (self._funding_direction < 0 and rate > 0)
            )

            if is_reversed:
                self._reverse_count += 1
            else:
                self._reverse_count = 0

            # Exit conditions
            bars_held = self.position.bars_count

            if bars_held >= self.hp["max_hold_bars"]:
                self.liquidate()
                self._consecutive_positive = 0
                self._consecutive_negative = 0
                return

            if self._reverse_count >= self.hp["exit_reverse_count"]:
                self.liquidate()
                self._consecutive_positive = 0
                self._consecutive_negative = 0
                return

            # Update consecutive counters
            threshold = self._get_dynamic_threshold()
            if rate >= threshold:
                self._consecutive_positive += 1
                self._consecutive_negative = 0
            elif rate <= -threshold:
                self._consecutive_negative += 1
                self._consecutive_positive = 0
            else:
                self._consecutive_positive = 0
                self._consecutive_negative = 0

    def on_open_position(self, order) -> None:
        pass

    def on_close_position(self, order) -> None:
        pass
