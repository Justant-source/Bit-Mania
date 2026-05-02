"""
Track B — Multi-symbol Funding Arbitrage (FA) Strategy

Extends single-symbol FA to manage 6 concurrent positions across BTCUSDT, ETHUSDT,
SOLUSDT, AVAXUSDT, DOGEUSDT, 1000PEPEUSDT on Bybit Perpetual.

Entry rules:
  - Per-symbol: funding_rate >= entry_threshold_apr for consecutive_intervals periods
  - Capital allocation: 75% total balance / 6 symbols = per_symbol_capital
  - Leverage: 5x (hard cap, non-negotiable)

Position management:
  - Per-symbol consecutive tracking (symbol_states dict)
  - Exit: rate < exit_threshold_apr, rate flips negative, or held > max_holding_hours
  - Settlement: credit funding income to equity at each 8h settlement

Run:
    python scripts/run_fa_backtest.py \
        --strategy MultiSymbolFundingArb \
        --start 2020-01-01 --end 2026-04-30 \
        --balance 10000 --fee 0.00055 --leverage 5

Backtest pass criteria (6-year):
  - CAGR >= 20%
  - Sharpe >= 2.5
  - MDD <= -8%
  - Trades per year >= 200 (across 6 symbols)
  - 4+ symbols with positive contribution
  - Win rate >= 60%
  - 2024-2026 period return >= +5%
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict

import jesse.helpers as jh
from jesse import indicators as ta
from jesse.strategies import Strategy

FA_FUNDING_PATH = os.environ.get(
    "FA_FUNDING_PATH", "/data/funding_rates/BTCUSDT_8h.parquet"
)


# ─── Multi-symbol funding rate loader ──────────────────────────────────────────

class _MultiSymbolFundingRateLoader:
    """
    Singleton-like loader for multi-symbol 8h funding rate data.
    Maps (exchange, symbol, timestamp_ms) → rate.
    """
    _data: Optional[Dict[tuple, float]] = None  # (exchange, symbol, timestamp_ms) → rate
    _symbols: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOGEUSDT", "1000PEPEUSDT"]
    _path: str = FA_FUNDING_PATH

    @classmethod
    def load(cls) -> Dict[tuple, float]:
        if cls._data is not None:
            return cls._data

        path = Path(cls._path)

        # Try parquet first
        if path.suffix == ".parquet" and path.exists():
            return cls._load_parquet(path)

        # Try CSV as fallback
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            return cls._load_csv(csv_path)

        # Neither exists
        raise FileNotFoundError(
            f"Multi-symbol funding rate data not found: {path} (or .csv variant)\n"
            f"Expected: parquet/CSV with columns: exchange, symbol, timestamp_ms, rate"
        )

    @classmethod
    def _load_parquet(cls, path: Path) -> Dict[tuple, float]:
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars required. Run: pip install polars")

        df = pl.read_parquet(path)

        # Normalize column names
        rename = {}
        for col in df.columns:
            low = col.lower()
            if low == "exchange":
                rename[col] = "exchange"
            elif low == "symbol":
                rename[col] = "symbol"
            elif low in ("timestamp", "timestamp_ms", "time", "ts"):
                rename[col] = "timestamp_ms"
            elif low in ("rate", "value", "funding_rate", "funding"):
                rename[col] = "rate"
        if rename:
            df = df.rename(rename)

        required = {"exchange", "symbol", "timestamp_ms", "rate"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Funding parquet missing columns: {missing}")

        # Convert timestamp to int milliseconds
        col_dtype = str(df["timestamp_ms"].dtype)
        if "Datetime" in col_dtype or "Date" in col_dtype:
            df = df.with_columns(
                pl.col("timestamp_ms").dt.epoch("ms").alias("timestamp_ms")
            )

        cls._data = {
            (row["exchange"].lower(), row["symbol"], int(row["timestamp_ms"])): float(row["rate"])
            for row in df.iter_rows(named=True)
        }
        return cls._data

    @classmethod
    def _load_csv(cls, path: Path) -> Dict[tuple, float]:
        import csv

        data = {}
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    exchange = row.get("exchange", "bybit").lower()
                    symbol = row.get("symbol", "").upper()
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
                    data[(exchange, symbol, ts_ms)] = rate
                except (StopIteration, ValueError, KeyError):
                    continue

        if not data:
            raise ValueError(f"No multi-symbol funding rate data loaded from CSV: {path}")

        cls._data = data
        return cls._data

    @classmethod
    def get_rate_at(cls, exchange: str, symbol: str, timestamp_ms: int) -> Optional[float]:
        """
        Return funding rate for the 8h period containing timestamp_ms.
        Uses floor-to-settlement-time alignment (8h period: 0, 8, 16 UTC).
        """
        data = cls.load()

        # Align to 8h settlement boundary
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        hour = dt.hour
        settlement_hour = (hour // 8) * 8
        settlement_dt = dt.replace(hour=settlement_hour, minute=0, second=0, microsecond=0)
        settlement_ms = int(settlement_dt.timestamp() * 1000)

        # Exact match
        key = (exchange.lower(), symbol.upper(), settlement_ms)
        if key in data:
            return data[key]

        # Look back up to 3 periods (24 hours)
        for periods_back in range(1, 4):
            prev_ms = settlement_ms - periods_back * 8 * 3600 * 1000
            prev_key = (exchange.lower(), symbol.upper(), prev_ms)
            if prev_key in data:
                return data[prev_key]

        return None

    @classmethod
    def reset(cls):
        """Clear cached data."""
        cls._data = None


# ─── Multi-symbol FA Strategy ─────────────────────────────────────────────────

class MultiSymbolFundingArb(Strategy):
    """
    Manages 6 concurrent funding arbitrage positions.

    Capital allocation:
      - Available for FA: 75% of balance
      - Per-symbol: 75% / 6 = 12.5% per symbol
      - Position size: per_symbol_capital * leverage (5x) / price

    Entry (per-symbol):
      - funding_rate >= entry_threshold_apr for consecutive_intervals periods
      - Only enter if position not already open

    Exit (per-symbol):
      - funding_rate < exit_threshold_apr
      - funding_rate flips negative
      - position held > max_holding_hours

    Data requirement:
        /data/funding_rates/multi_symbol_8h.parquet or .csv
        Columns: exchange, symbol, timestamp_ms, rate
    """

    def __init__(self):
        super().__init__()
        self.symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOGEUSDT", "1000PEPEUSDT"]
        self.exchange = "bybit"

        # Per-symbol tracking
        self.symbol_states: Dict[str, Dict] = {}
        for sym in self.symbols:
            self.symbol_states[sym] = {
                "consecutive_positive": 0,
                "consecutive_negative": 0,
                "funding_direction": 0,
                "reverse_count": 0,
            }

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "entry_threshold_apr",      "type": float, "min": 5.0,    "max": 20.0,  "default": 8.0},
            {"name": "exit_threshold_apr",       "type": float, "min": 0.5,    "max": 5.0,   "default": 3.0},
            {"name": "consecutive_intervals",    "type": int,   "min": 1,      "max": 10,    "default": 2},
            {"name": "max_holding_hours",        "type": int,   "min": 24,     "max": 720,   "default": 168},
            {"name": "fa_allocation_pct",        "type": float, "min": 0.5,    "max": 1.0,   "default": 0.75},
            {"name": "leverage",                 "type": int,   "min": 1,      "max": 5,     "default": 5},
            {"name": "reinvest_pct",             "type": float, "min": 0.0,    "max": 1.0,   "default": 0.30},
        ]

    @property
    def _is_settlement_candle(self) -> bool:
        """Return True if current candle is at settlement time (0, 8, 16 UTC)."""
        arrow = jh.timestamp_to_arrow(self.current_candle[0])
        return arrow.hour in (0, 8, 16)

    def should_long(self) -> bool:
        """Multi-symbol: check all symbols for entry signal."""
        # In Jesse backtesting, we process candles per-symbol
        # This should_long() is called per route (symbol)
        # Extract current symbol from route
        if not hasattr(self, "symbol") or self.symbol not in self.symbols:
            return False

        if self.position.is_open:
            return False

        if not self._is_settlement_candle:
            return False

        symbol = self.symbol
        rate = _MultiSymbolFundingRateLoader.get_rate_at(
            self.exchange, symbol, int(self.current_candle[0])
        )

        if rate is None:
            return False

        # Convert rate to APR: rate is 8h funding rate
        # APR = rate * (365 * 3 / 8) ≈ rate * 136.875
        apr = rate * 365 * 3 / 8

        if apr < self.hp["entry_threshold_apr"] / 100:
            return False

        state = self.symbol_states[symbol]
        state["consecutive_positive"] += 1
        state["consecutive_negative"] = 0

        return state["consecutive_positive"] >= self.hp["consecutive_intervals"]

    def should_short(self) -> bool:
        return False

    def go_long(self) -> None:
        """Enter position with per-symbol capital allocation."""
        symbol = self.symbol
        per_symbol_capital = self.balance * self.hp["fa_allocation_pct"] / len(self.symbols)
        notional = per_symbol_capital * self.hp["leverage"]
        qty = notional / self.price

        state = self.symbol_states[symbol]
        state["funding_direction"] = 1
        state["reverse_count"] = 0

        self.buy = qty, self.price

        if "cumulative_funding" not in self.shared_vars:
            self.shared_vars["cumulative_funding"] = {}
        if symbol not in self.shared_vars["cumulative_funding"]:
            self.shared_vars["cumulative_funding"][symbol] = 0.0

    def go_short(self) -> None:
        pass

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self) -> None:
        """Per-symbol position management."""
        if not self.position.is_open:
            return

        symbol = self.symbol
        rate = _MultiSymbolFundingRateLoader.get_rate_at(
            self.exchange, symbol, int(self.current_candle[0])
        )

        if rate is None:
            return

        state = self.symbol_states[symbol]

        # Settlement: credit funding income
        if self._is_settlement_candle:
            apr = rate * 365 * 3 / 8
            position_value = self.position.quantity * self.price
            funding_income = position_value * rate * state["funding_direction"]

            if "cumulative_funding" not in self.shared_vars:
                self.shared_vars["cumulative_funding"] = {}
            self.shared_vars["cumulative_funding"][symbol] = (
                self.shared_vars["cumulative_funding"].get(symbol, 0.0) + funding_income
            )

            # Check for reversal
            is_reversed = (
                (state["funding_direction"] > 0 and rate < 0) or
                (state["funding_direction"] < 0 and rate > 0)
            )

            if is_reversed:
                state["reverse_count"] += 1
            else:
                state["reverse_count"] = 0

            # Check exit conditions
            bars_held = self.position.bars_count

            # Exit: rate below threshold
            if apr < self.hp["exit_threshold_apr"] / 100:
                self.liquidate()
                state["consecutive_positive"] = 0
                state["consecutive_negative"] = 0
                return

            # Exit: rate flips negative
            if rate < 0:
                self.liquidate()
                state["consecutive_positive"] = 0
                state["consecutive_negative"] = 0
                return

            # Exit: max holding time
            if bars_held >= self.hp["max_holding_hours"]:
                self.liquidate()
                state["consecutive_positive"] = 0
                state["consecutive_negative"] = 0
                return

            # Update consecutive counters
            if apr >= self.hp["entry_threshold_apr"] / 100:
                state["consecutive_positive"] += 1
                state["consecutive_negative"] = 0
            elif apr <= -self.hp["entry_threshold_apr"] / 100:
                state["consecutive_negative"] += 1
                state["consecutive_positive"] = 0
            else:
                state["consecutive_positive"] = 0
                state["consecutive_negative"] = 0

    def on_open_position(self, order) -> None:
        pass

    def on_close_position(self, order) -> None:
        pass
