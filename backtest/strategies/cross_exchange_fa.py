"""
Track B — Cross-exchange Funding Arbitrage (FA) Strategy

Exploits funding rate spread between Bybit (premium) and Binance/OKEx (discount).
Implements simultaneous: long cheaper exchange, short expensive exchange.

Entry rules:
  - Entry spread (APR): |rate_bybit - rate_binance| * 365 * 3 >= entry_spread_apr
  - Consecutive: spread >= entry_threshold for consecutive_intervals periods
  - Capital: 50:50 split between exchanges
  - Leverage: 5x per leg (hard cap)

Position management:
  - Per-exchange tracking (exchange_states dict)
  - Exit: spread < exit_spread_apr, or held > max_holding_hours
  - Settlement: credit funding income to equity at 8h settlements

Data requirement:
    /data/funding_rates/multi_exchange_8h.parquet or .csv
    Columns: exchange, symbol (BTCUSDT), timestamp_ms, rate

NOTE: Jesse backtest limitation
    Jesse processes single exchange per route. For true cross-exchange backtesting,
    run two separate backtests (Bybit vs Binance) and aggregate results manually.
    This strategy code demonstrates the logic for future implementation when
    Jesse supports multi-exchange orchestration.

Run:
    python scripts/run_fa_backtest.py \
        --strategy CrossExchangeFA \
        --start 2020-01-01 --end 2026-04-30 \
        --balance 10000 --fee 0.00055 --leverage 5

Backtest pass criteria (6-year, two runs aggregated):
  - CAGR >= 15%
  - Sharpe >= 2.2
  - MDD <= -8%
  - 2024-2026 return >= +3%
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
    "FA_FUNDING_PATH", "/data/funding_rates/multi_exchange_8h.parquet"
)


# ─── Cross-exchange funding rate loader ────────────────────────────────────────

class _CrossExchangeFundingRateLoader:
    """
    Singleton loader for multi-exchange BTC funding rate data.
    Maps (exchange, timestamp_ms) → rate for BTCUSDT.
    """
    _data: Optional[Dict[tuple, float]] = None  # (exchange, timestamp_ms) → rate
    _symbol: str = "BTCUSDT"
    _exchanges: list[str] = ["bybit", "binance", "okex"]
    _path: str = FA_FUNDING_PATH

    @classmethod
    def load(cls) -> Dict[tuple, float]:
        if cls._data is not None:
            return cls._data

        path = Path(cls._path)

        if path.suffix == ".parquet" and path.exists():
            return cls._load_parquet(path)

        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            return cls._load_csv(csv_path)

        raise FileNotFoundError(
            f"Cross-exchange funding rate data not found: {path} (or .csv variant)\n"
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

        # Filter to BTCUSDT only
        df = df.filter(pl.col("symbol").str.to_uppercase() == "BTCUSDT")

        cls._data = {
            (row["exchange"].lower(), int(row["timestamp_ms"])): float(row["rate"])
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
                    exchange = row.get("exchange", "").lower()
                    symbol = row.get("symbol", "").upper()

                    # Only BTCUSDT
                    if symbol != "BTCUSDT":
                        continue

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
                    data[(exchange, ts_ms)] = rate
                except (StopIteration, ValueError, KeyError):
                    continue

        if not data:
            raise ValueError(f"No cross-exchange funding rate data loaded from CSV: {path}")

        cls._data = data
        return cls._data

    @classmethod
    def get_rate_at(cls, exchange: str, timestamp_ms: int) -> Optional[float]:
        """
        Return funding rate for the 8h period containing timestamp_ms.
        Aligns to settlement boundary (8h: 0, 8, 16 UTC).
        """
        data = cls.load()

        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        hour = dt.hour
        settlement_hour = (hour // 8) * 8
        settlement_dt = dt.replace(hour=settlement_hour, minute=0, second=0, microsecond=0)
        settlement_ms = int(settlement_dt.timestamp() * 1000)

        # Exact match
        key = (exchange.lower(), settlement_ms)
        if key in data:
            return data[key]

        # Look back up to 3 periods
        for periods_back in range(1, 4):
            prev_ms = settlement_ms - periods_back * 8 * 3600 * 1000
            prev_key = (exchange.lower(), prev_ms)
            if prev_key in data:
                return data[prev_key]

        return None

    @classmethod
    def reset(cls):
        cls._data = None


# ─── Cross-exchange FA Strategy ────────────────────────────────────────────────

class CrossExchangeFA(Strategy):
    """
    Cross-exchange funding arbitrage: exploit spread between exchanges.

    BTC-only (complies with BTC single-symbol policy).

    Capital allocation:
      - Available for FA: 75% of balance
      - Per-exchange: 75% / 2 = 37.5% (long cheaper, short expensive)
      - Position size: per_exchange_capital * leverage / price

    Entry (per-pair):
      - rate_spread_apr = abs(rate_bybit - rate_binance) * 365 * 3 * 100
      - Enter when spread >= entry_spread_apr for consecutive_intervals periods

    Exit (per-pair):
      - spread < exit_spread_apr
      - position held > max_holding_hours

    Note: Jesse limitation
        Jesse processes one exchange per route. For full cross-exchange backtest:
        1. Run BackExchangeFA on Bybit (LONG)
        2. Run CrossExchangeFA on Binance (SHORT)
        3. Aggregate results: combined P&L, correlation analysis

    This code demonstrates the logic when Jesse supports multi-exchange.
    """

    def __init__(self):
        super().__init__()
        self.symbol = "BTCUSDT"
        self.exchanges = ["bybit", "binance", "okex"]

        # Per-exchange tracking
        self.exchange_states: Dict[str, Dict] = {}
        for exch in self.exchanges:
            self.exchange_states[exch] = {
                "consecutive_spread": 0,
                "funding_direction": 0,
            }

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "entry_spread_apr",         "type": float, "min": 5.0,    "max": 30.0,  "default": 12.0},
            {"name": "exit_spread_apr",          "type": float, "min": 0.5,    "max": 10.0,  "default": 4.0},
            {"name": "consecutive_intervals",    "type": int,   "min": 1,      "max": 10,    "default": 2},
            {"name": "max_holding_hours",        "type": int,   "min": 24,     "max": 720,   "default": 168},
            {"name": "fa_allocation_pct",        "type": float, "min": 0.5,    "max": 1.0,   "default": 0.75},
            {"name": "leverage",                 "type": int,   "min": 1,      "max": 5,     "default": 5},
        ]

    @property
    def _is_settlement_candle(self) -> bool:
        arrow = jh.timestamp_to_arrow(self.current_candle[0])
        return arrow.hour in (0, 8, 16)

    def _get_spread_apr(self, timestamp_ms: int) -> Optional[float]:
        """
        Calculate funding rate spread between exchanges (APR).
        Absolute value of difference * 365 * 3 * 100.
        """
        rate_bybit = _CrossExchangeFundingRateLoader.get_rate_at("bybit", timestamp_ms)
        rate_binance = _CrossExchangeFundingRateLoader.get_rate_at("binance", timestamp_ms)

        if rate_bybit is None or rate_binance is None:
            return None

        spread = abs(rate_bybit - rate_binance) * 365 * 3 * 100
        return spread

    def should_long(self) -> bool:
        """
        Entry: spread >= entry_threshold for consecutive_intervals.
        Direction: long cheaper exchange, short expensive.
        """
        if self.position.is_open:
            return False

        if not self._is_settlement_candle:
            return False

        spread = self._get_spread_apr(int(self.current_candle[0]))
        if spread is None or spread < self.hp["entry_spread_apr"] / 100:
            return False

        # Get rates to determine direction
        rate_bybit = _CrossExchangeFundingRateLoader.get_rate_at(
            "bybit", int(self.current_candle[0])
        )
        rate_binance = _CrossExchangeFundingRateLoader.get_rate_at(
            "binance", int(self.current_candle[0])
        )

        if rate_bybit is None or rate_binance is None:
            return False

        # Update consecutive counter
        for exch in self.exchanges:
            self.exchange_states[exch]["consecutive_spread"] += 1

        return self.exchange_states["bybit"]["consecutive_spread"] >= self.hp["consecutive_intervals"]

    def should_short(self) -> bool:
        return False

    def go_long(self) -> None:
        """Enter position: long if cheaper, short if expensive (Jesse LONG models both)."""
        per_exchange_capital = self.balance * self.hp["fa_allocation_pct"] / 2
        notional = per_exchange_capital * self.hp["leverage"]
        qty = notional / self.price

        for exch in self.exchanges:
            self.exchange_states[exch]["funding_direction"] = 1

        self.buy = qty, self.price

        if "cross_exchange_funding" not in self.shared_vars:
            self.shared_vars["cross_exchange_funding"] = 0.0

    def go_short(self) -> None:
        pass

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self) -> None:
        """Per-exchange position management."""
        if not self.position.is_open:
            return

        spread = self._get_spread_apr(int(self.current_candle[0]))
        if spread is None:
            return

        if self._is_settlement_candle:
            # Credit funding: bybit pays us, binance we pay
            rate_bybit = _CrossExchangeFundingRateLoader.get_rate_at(
                "bybit", int(self.current_candle[0])
            )
            rate_binance = _CrossExchangeFundingRateLoader.get_rate_at(
                "binance", int(self.current_candle[0])
            )

            if rate_bybit is not None and rate_binance is not None:
                position_value = self.position.quantity * self.price
                funding_income = position_value * (rate_bybit - rate_binance)

                self.shared_vars["cross_exchange_funding"] = (
                    self.shared_vars.get("cross_exchange_funding", 0.0) + funding_income
                )

            # Exit conditions
            bars_held = self.position.bars_count

            # Exit: spread below threshold
            if spread < self.hp["exit_spread_apr"] / 100:
                self.liquidate()
                for exch in self.exchanges:
                    self.exchange_states[exch]["consecutive_spread"] = 0
                return

            # Exit: max holding time
            if bars_held >= self.hp["max_holding_hours"]:
                self.liquidate()
                for exch in self.exchanges:
                    self.exchange_states[exch]["consecutive_spread"] = 0
                return

            # Update consecutive counter
            if spread >= self.hp["entry_spread_apr"] / 100:
                for exch in self.exchanges:
                    self.exchange_states[exch]["consecutive_spread"] += 1

    def on_open_position(self, order) -> None:
        pass

    def on_close_position(self, order) -> None:
        pass
