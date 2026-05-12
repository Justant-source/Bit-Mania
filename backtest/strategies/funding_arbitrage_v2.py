"""
Phase 10.1 — Funding Arbitrage with Macro Event Filter

Extends FundingArbitrage with FOMC/CPI event blackout:
  - Suppress entry ±2 hours around FOMC/CPI announcements
  - Allows existing positions to hold through event (no forced exit)

Data requirement:
    /data/macro_events/fomc_cpi_calendar.csv
    CSV format: event_type,timestamp_utc,description
    e.g. FOMC,2024-01-31 19:00,Rate decision

If macro calendar not found, log warning and allow trades (degraded mode).

Run:
    python scripts/run_backtest.py \
        --strategy FundingArbitrageWithMacroFilter \
        --start 2023-04-01 --end 2026-04-01

V5 Impact:
  - Hypothesis: Reduces drawdown during event volatility
  - Expected: Sharpe ≥ 3.5 (same or better than baseline)
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import os

import jesse.helpers as jh
from funding_arbitrage import FundingArbitrage


MACRO_CALENDAR_PATH = os.environ.get(
    "MACRO_CALENDAR_PATH", "/data/macro_events/fomc_cpi_calendar.csv"
)


# ─── Macro event calendar loader ───────────────────────────────────────────────

class _MacroEventLoader:
    """
    Load FOMC/CPI calendar from CSV.
    Returns list of event timestamps (milliseconds).
    If file not found, returns empty list (degraded mode — no filtering).
    """
    _events: Optional[list[int]] = None
    _path: str = MACRO_CALENDAR_PATH

    @classmethod
    def load(cls) -> list[int]:
        """Load events once and cache."""
        if cls._events is not None:
            return cls._events

        path = Path(cls._path)
        if not path.exists():
            # Degraded mode: no macro filtering
            print(f"[WARN] Macro event calendar not found: {path}")
            print(f"[WARN] Proceeding without macro event blackout (degraded mode)")
            cls._events = []
            return cls._events

        events = []
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or line.startswith("event_type"):
                        continue
                    parts = line.split(",", 2)
                    if len(parts) < 2:
                        continue
                    event_type = parts[0].strip().upper()
                    ts_str = parts[1].strip()

                    # Only track FOMC and CPI
                    if event_type not in ("FOMC", "CPI"):
                        continue

                    try:
                        # Parse timestamp (flexible formats)
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
                            try:
                                dt = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                                break
                            except ValueError:
                                continue
                        else:
                            continue  # unparseable
                        events.append(int(dt.timestamp() * 1000))
                    except Exception:
                        continue  # skip bad rows
        except Exception as e:
            print(f"[WARN] Failed to load macro calendar: {e}")
            print(f"[WARN] Proceeding without macro event blackout (degraded mode)")

        cls._events = sorted(events)
        return cls._events

    @classmethod
    def is_in_blackout(cls, timestamp_ms: int, blackout_hours: float = 2.0) -> bool:
        """
        Return True if timestamp_ms is within blackout_hours of any event.
        """
        events = cls.load()
        if not events:
            return False

        blackout_ms = int(blackout_hours * 3600 * 1000)
        for event_ms in events:
            if abs(timestamp_ms - event_ms) <= blackout_ms:
                return True
        return False

    @classmethod
    def reset(cls):
        """Clear cached data (useful for testing)."""
        cls._events = None


# ─── FA with Macro Filter ──────────────────────────────────────────────────────

class FundingArbitrageWithMacroFilter(FundingArbitrage):
    """
    Funding Arbitrage + FOMC/CPI blackout overlay.

    Inherits all FA logic from FundingArbitrage.
    Overrides should_long() to add macro event blackout check.

    Entry blackout: ±2 hours around FOMC/CPI events
    Existing positions: allowed to hold through events (no exit)

    Hyperparameters (inherited from FundingArbitrage + macro-specific):
      - blackout_hours: hours before/after event to suppress entry (default 2.0)
    """

    def hyperparameters(self) -> list[dict]:
        fa_hps = super().hyperparameters()
        # Append macro-specific hyperparameters
        fa_hps.extend([
            {"name": "blackout_hours", "type": float, "min": 0.5, "max": 6.0, "default": 2.0},
        ])
        return fa_hps

    # ── Macro event check ──────────────────────────────────────────────────────

    def _is_in_macro_blackout(self) -> bool:
        """Return True if current time is in FOMC/CPI ±N hours blackout."""
        ts_ms = int(self.current_candle[0])
        return _MacroEventLoader.is_in_blackout(ts_ms, self.hp["blackout_hours"])

    # ── Override entry signal ──────────────────────────────────────────────────

    def should_long(self) -> bool:
        """
        Entry: combine FA logic + macro blackout filter.
        Suppress entry during macro event blackout.
        """
        # Check macro blackout first (cheap check)
        if self._is_in_macro_blackout():
            return False

        # Delegate to parent FA logic
        return super().should_long()
