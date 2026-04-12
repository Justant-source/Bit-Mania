"""
Phase 8.2 — FOMC/CPI Macro Event Strategy (Jesse)

Basis:
  - 2025: FOMC 8 events, BTC dropped on 6 of them ("sell the news")
  - FOMC/CPI days: BTC volatility 50-100% above baseline
  - Pattern: pre-event short → whipsaw wait → post-event trend follow

Trade logic (4 phases):
  Phase 1 (T-24h → T-1h): De-risk — reduce existing exposure
  Phase 2 (T-1h → T):     Pre-event SHORT position
  Phase 3 (T → T+15min):  Whipsaw wait — no position
  Phase 4 (T+15min → T+12h): Post-event trend follow (direction-based)

V5 Pass Criteria: same as all strategies + ≥ 25 trades/year (FOMC 8 + CPI 12 + other)

Data requirement:
    /data/macro_events/fomc_cpi_calendar.csv
    CSV format: event_type,timestamp_utc,description
    e.g. FOMC,2024-01-31 19:00,Rate decision

Run:
    # 1. Build the event calendar
    python scripts/data/build_macro_calendar.py --start 2023-01-01 --end 2026-12-31

    # 2. Run full validation
    ./scripts/run_full_validation.sh MacroEvent
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import os

import jesse.helpers as jh
from jesse import indicators as ta
from jesse.strategies import Strategy


# ─── Event data loader ─────────────────────────────────────────────────────────

def _load_events(csv_path: str) -> list[dict]:
    """
    Load FOMC/CPI calendar from CSV.
    Returns list of {event_type, timestamp_ms, description}.
    Raises FileNotFoundError if not found (synthetic fallback prohibited).
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Macro event calendar not found: {path}\n"
            "Run: python scripts/data/build_macro_calendar.py first.\n"
            "Synthetic data fallback DISABLED (V5 rules)."
        )

    events = []
    with open(path) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("event_type"):
                continue
            parts = line.split(",", 2)
            if len(parts) < 2:
                continue
            event_type  = parts[0].strip().upper()
            ts_str      = parts[1].strip()
            description = parts[2].strip() if len(parts) > 2 else ""
            try:
                # Try with and without seconds
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
                    try:
                        dt = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue
                else:
                    continue  # skip unparseable
                events.append({
                    "type": event_type,
                    "timestamp_ms": int(dt.timestamp() * 1000),
                    "description": description,
                })
            except Exception:
                continue  # skip bad rows

    if not events:
        raise ValueError(
            f"No events loaded from {path}. "
            "Check CSV format: event_type,timestamp_utc,description"
        )
    return sorted(events, key=lambda e: e["timestamp_ms"])


# ─── Strategy ──────────────────────────────────────────────────────────────────

MACRO_CALENDAR_PATH = os.environ.get(
    "MACRO_CALENDAR_PATH", "/data/macro_events/fomc_cpi_calendar.csv"
)


class MacroEvent(Strategy):
    """
    Trade volatility spikes around FOMC/CPI announcements.

    State machine:
        IDLE → PRE_EVENT_SHORT → WHIPSAW_WAIT → POST_EVENT_TREND → IDLE

    The strategy uses self._state to track phase.
    self._event_ts_ms stores the current event timestamp.
    """

    def __init__(self):
        super().__init__()
        self._events: list[dict] = _load_events(MACRO_CALENDAR_PATH)
        self._state: str = "IDLE"            # IDLE | PRE_SHORT | WAITING | POST_TREND
        self._event_ts_ms: int = 0           # current event reference timestamp
        self._post_entry_price: float = 0.0  # price at post-whipsaw entry

    # ── Hyperparameters ────────────────────────────────────────────────────────

    def hyperparameters(self) -> list[dict]:
        return [
            {"name": "pre_event_short_hours", "type": int,   "min": 1,    "max": 4,    "default": 1},
            {"name": "whipsaw_wait_minutes",  "type": int,   "min": 10,   "max": 60,   "default": 15},
            {"name": "post_event_hold_hours", "type": int,   "min": 4,    "max": 24,   "default": 12},
            {"name": "take_profit_pct",       "type": float, "min": 0.01, "max": 0.05, "default": 0.02},
            {"name": "stop_loss_pct",         "type": float, "min": 0.005,"max": 0.03, "default": 0.015},
            {"name": "position_size_pct",     "type": float, "min": 0.10, "max": 0.40, "default": 0.25},
        ]

    # ── Time helpers ───────────────────────────────────────────────────────────

    @property
    def _now_ms(self) -> int:
        return int(self.current_candle[0])

    def _hours_until_event(self) -> float:
        """Return hours until next FOMC/CPI event, or +inf if none within 48h."""
        now_ms = self._now_ms
        for ev in self._events:
            diff_ms = ev["timestamp_ms"] - now_ms
            if 0 < diff_ms <= 48 * 3600 * 1000:
                return diff_ms / (3600 * 1000)
        return float("inf")

    def _ms_since_last_event(self) -> float:
        """Return hours since the most recent past event, or +inf."""
        now_ms = self._now_ms
        best = float("inf")
        for ev in self._events:
            diff_ms = now_ms - ev["timestamp_ms"]
            if diff_ms >= 0:
                hours = diff_ms / (3600 * 1000)
                if hours < best:
                    best = hours
                    self._event_ts_ms = ev["timestamp_ms"]
        return best

    def _get_next_event(self) -> Optional[dict]:
        """Return the next upcoming event or None."""
        now_ms = self._now_ms
        for ev in self._events:
            if ev["timestamp_ms"] > now_ms:
                return ev
        return None

    # ── Indicators ────────────────────────────────────────────────────────────

    @property
    def _atr(self) -> float:
        return ta.atr(self.candles, 14)

    @property
    def _price_direction_post_event(self) -> int:
        """
        Determine post-event price direction.
        Compare current price to price at event timestamp.
        Returns +1 (up) or -1 (down).
        """
        if self._event_ts_ms == 0:
            return 1
        # Approximate: compare to price 1h ago (post whipsaw)
        if len(self.candles) < 2:
            return 1
        price_now = float(self.candles[-1][2])  # close
        price_1h_ago = float(self.candles[-2][2]) if len(self.candles) >= 2 else price_now
        return 1 if price_now > price_1h_ago else -1

    # ── Entry/Exit signals ────────────────────────────────────────────────────

    def should_long(self) -> bool:
        if self.position.is_open:
            return False
        # Post-event trend: enter long if market moved up after whipsaw
        hours_since = self._ms_since_last_event()
        whipsaw_h = self.hp["whipsaw_wait_minutes"] / 60.0
        if whipsaw_h <= hours_since <= (whipsaw_h + 1.0):
            if self._price_direction_post_event == 1:
                return True
        return False

    def should_short(self) -> bool:
        if self.position.is_open:
            return False
        # Pre-event short: enter 1h before event
        hours_until = self._hours_until_event()
        if 0 < hours_until <= self.hp["pre_event_short_hours"]:
            return True
        # Post-event trend: enter short if market moved down after whipsaw
        hours_since = self._ms_since_last_event()
        whipsaw_h = self.hp["whipsaw_wait_minutes"] / 60.0
        if whipsaw_h <= hours_since <= (whipsaw_h + 1.0):
            if self._price_direction_post_event == -1:
                return True
        return False

    def go_long(self) -> None:
        qty = (self.balance * self.hp["position_size_pct"]) / self.price
        tp  = self.price * (1 + self.hp["take_profit_pct"])
        sl  = self.price * (1 - self.hp["stop_loss_pct"])
        self.buy        = qty, self.price
        self.take_profit = qty, tp
        self.stop_loss   = qty, sl

    def go_short(self) -> None:
        qty = (self.balance * self.hp["position_size_pct"]) / self.price
        tp  = self.price * (1 - self.hp["take_profit_pct"])
        sl  = self.price * (1 + self.hp["stop_loss_pct"])
        self.sell       = qty, self.price
        self.take_profit = qty, tp
        self.stop_loss   = qty, sl

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self) -> None:
        """Time-based exit: close post-event positions after hold period."""
        hours_since = self._ms_since_last_event()
        if hours_since > self.hp["post_event_hold_hours"]:
            self.liquidate()
