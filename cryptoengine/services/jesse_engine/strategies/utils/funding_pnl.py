"""
Phase 7.4 — Funding Rate P&L Calculator for Jesse Strategies.

Bybit perpetual funding schedule: every 8 hours at UTC 00:00, 08:00, 16:00.
Formula: funding_pnl = -direction × notional × funding_rate
  - direction: +1 for long, -1 for short
  - long  + positive rate → PAYS funding (negative P&L)
  - short + positive rate → RECEIVES funding (positive P&L)

Data source: /data/coinalyze/BTCUSDT_funding.parquet
  Columns: timestamp_ms (int), funding_rate (float)

Unit tests: scripts/test_funding_pnl.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000  # 8 hours in milliseconds
BYBIT_FUNDING_HOURS_UTC = {0, 8, 16}       # UTC hours when funding settles

# Allowed tolerance when checking if a timestamp is a funding settlement time (±1 min)
FUNDING_TIME_TOLERANCE_MS = 60 * 1000


class FundingTracker:
    """
    Calculates funding rate P&L for Jesse strategy positions.

    Usage in Jesse strategy:
        def __init__(self):
            super().__init__()
            self.funding = FundingTracker('/data/coinalyze/BTCUSDT_funding.parquet')

        def update_position(self):
            pnl = self.funding.calculate_pnl(
                position_direction = 1 if self.position.type == 'long' else -1,
                notional           = abs(self.position.value),
                timestamp_ms       = self.current_candle[0],
            )
            if pnl != 0.0:
                # Jesse doesn't have a direct "add to realized P&L" API,
                # so track cumulative funding separately:
                self._cumulative_funding_pnl = getattr(self, '_cumulative_funding_pnl', 0.0) + pnl
    """

    def __init__(self, funding_data_path: str) -> None:
        self._path = Path(funding_data_path)
        self._data: Optional[object] = None  # polars DataFrame, lazy-loaded
        self._cache: dict[int, float] = {}   # timestamp_ms → rate cache

    # ── Lazy loader ────────────────────────────────────────────────────────────

    def _load(self) -> None:
        if self._data is not None:
            return
        if not self._path.exists():
            raise FileNotFoundError(
                f"Funding data not found: {self._path}\n"
                "Run: python scripts/fetch_coinalyze_funding.py first.\n"
                "Synthetic data fallback is DISABLED (prohibited by V5 rules)."
            )
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars required. Run: pip install polars")

        df = pl.read_parquet(self._path)

        # Normalize column names
        col_map: dict[str, str] = {}
        for col in df.columns:
            low = col.lower()
            if low in ("timestamp", "timestamp_ms", "time", "ts"):
                col_map[col] = "timestamp_ms"
            elif low in ("funding_rate", "rate", "fundingrate", "value"):
                col_map[col] = "funding_rate"
        if col_map:
            df = df.rename(col_map)

        required = {"timestamp_ms", "funding_rate"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Funding parquet missing columns: {missing}. "
                f"Available: {df.columns}"
            )

        # Ensure timestamp is in milliseconds
        if df["timestamp_ms"].max() < 1e12:
            df = df.with_columns((pl.col("timestamp_ms") * 1000).cast(pl.Int64))

        df = df.sort("timestamp_ms")
        self._data = df

        # Build lookup cache
        for row in df.iter_rows(named=True):
            self._cache[int(row["timestamp_ms"])] = float(row["funding_rate"])

    # ── Core API ───────────────────────────────────────────────────────────────

    def _is_funding_time(self, timestamp_ms: int) -> bool:
        """
        Return True if timestamp_ms falls within ±1 min of a Bybit funding settlement.
        Bybit settles at UTC 00:00, 08:00, 16:00.
        """
        # Convert to seconds for UTC hour computation
        total_seconds = (timestamp_ms // 1000) % 86400  # seconds within the day
        hour = total_seconds // 3600
        minute_remainder = total_seconds % 3600

        # Check exact hour match (with ±1 min tolerance)
        if hour in BYBIT_FUNDING_HOURS_UTC:
            return minute_remainder <= 60 or minute_remainder >= (3600 - 60)
        return False

    def get_funding_at(self, timestamp_ms: int) -> float:
        """
        Return the funding rate active at the given timestamp.

        Looks up the most recent funding rate at or before `timestamp_ms`.
        Returns 0.0 if no data is available before the timestamp.
        """
        self._load()

        # Fast exact-match lookup
        if timestamp_ms in self._cache:
            return self._cache[timestamp_ms]

        # Linear scan for the most recent rate ≤ timestamp_ms
        # (Acceptable for 8h intervals — ~3 lookups/day over 3 years = ~3k rows)
        best_rate = 0.0
        best_ts   = -1
        for ts, rate in self._cache.items():
            if ts <= timestamp_ms and ts > best_ts:
                best_ts   = ts
                best_rate = rate
        return best_rate

    def calculate_pnl(
        self,
        position_direction: int,
        notional: float,
        timestamp_ms: int,
    ) -> float:
        """
        Calculate funding P&L if a position is held at funding settlement time.

        Args:
            position_direction: +1 for long, -1 for short
            notional:           absolute position value in USD
            timestamp_ms:       current candle open timestamp in milliseconds

        Returns:
            funding_pnl in USD (positive = received, negative = paid)
        """
        if not self._is_funding_time(timestamp_ms):
            return 0.0

        rate = self.get_funding_at(timestamp_ms)
        if rate == 0.0:
            return 0.0

        # Bybit formula: long pays if rate > 0, short receives
        pnl = -position_direction * notional * rate
        return pnl

    # ── Statistics helpers (for report generation) ─────────────────────────────

    def total_funding_pnl(
        self,
        position_events: list[dict],
    ) -> float:
        """
        Compute total funding P&L over a list of position snapshot events.

        Each event dict must have:
            {'timestamp_ms': int, 'direction': int, 'notional': float}
        """
        self._load()
        total = 0.0
        for event in position_events:
            total += self.calculate_pnl(
                event["direction"], event["notional"], event["timestamp_ms"]
            )
        return total
