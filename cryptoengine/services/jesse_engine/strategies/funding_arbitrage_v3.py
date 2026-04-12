"""
Phase 10.2 — Funding Arbitrage with Fear & Greed Position Sizing

Extends FundingArbitrageWithMacroFilter with dynamic position sizing based on
Fear & Greed sentiment index:

  F&G < 25 (extreme fear):   × 0.5 (reduce position size — risky)
  F&G 25-75 (neutral):       × 1.0 (normal sizing)
  F&G > 75 (extreme greed):  × 0.75 (slight reduction)

Rationale:
  - Extreme fear (F&G < 25) → high drawdown risk → reduce leverage
  - Extreme greed (F&G > 75) → overbought risk → slight reduction
  - Neutral sentiment → full position size

Data requirement:
    /data/sentiment/fear_greed.parquet
    Columns: timestamp_ms (int, daily), value (int, 0-100)

Run:
    python scripts/run_backtest.py \
        --strategy FundingArbitrageWithFGSizer \
        --start 2023-04-01 --end 2026-04-01

V5 Impact:
  - Hypothesis: Reduces drawdown during extreme sentiment periods
  - Expected: Sharpe ≥ 3.5, MDD slightly lower than baseline
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from contrarian_sentiment import _FearGreedLoader
from funding_arbitrage_v2 import FundingArbitrageWithMacroFilter


# ─── FA with F&G Dynamic Sizing ────────────────────────────────────────────────

class FundingArbitrageWithFGSizer(FundingArbitrageWithMacroFilter):
    """
    Funding Arbitrage + Macro Filter + Fear & Greed Position Sizing.

    Inherits all FA + macro logic from FundingArbitrageWithMacroFilter.
    Overrides go_long() to apply F&G multiplier to position size.

    F&G multiplier:
      - F&G < 25:   × 0.5 (reduce risk during extreme fear)
      - F&G 25-75:  × 1.0 (normal sizing)
      - F&G > 75:   × 0.75 (slight reduction during greed)

    Hyperparameters (inherited from v2 + F&G-specific):
      - fg_fear_threshold: F&G level below which to apply fear multiplier (default 25)
      - fg_greed_threshold: F&G level above which to apply greed multiplier (default 75)
      - fg_fear_multiplier: position size multiplier for extreme fear (default 0.5)
      - fg_greed_multiplier: position size multiplier for extreme greed (default 0.75)
      - fg_neutral_multiplier: position size multiplier for neutral zone (default 1.0)
    """

    def hyperparameters(self) -> list[dict]:
        hps = super().hyperparameters()
        # Append F&G-specific hyperparameters
        hps.extend([
            {"name": "fg_fear_threshold",    "type": int,   "min": 10,  "max": 40, "default": 25},
            {"name": "fg_greed_threshold",   "type": int,   "min": 60,  "max": 90, "default": 75},
            {"name": "fg_fear_multiplier",   "type": float, "min": 0.1, "max": 0.9, "default": 0.5},
            {"name": "fg_greed_multiplier",  "type": float, "min": 0.5, "max": 1.0, "default": 0.75},
            {"name": "fg_neutral_multiplier","type": float, "min": 0.9, "max": 1.0, "default": 1.0},
        ])
        return hps

    # ── F&G sentiment check ────────────────────────────────────────────────────

    def _get_fg_multiplier(self) -> float:
        """
        Return position size multiplier based on Fear & Greed sentiment.
        Default 1.0 if no data available.
        """
        try:
            fg = _FearGreedLoader.get_value_at(int(self.current_candle[0]))
            if fg is None:
                return self.hp["fg_neutral_multiplier"]

            if fg < self.hp["fg_fear_threshold"]:
                return self.hp["fg_fear_multiplier"]
            elif fg > self.hp["fg_greed_threshold"]:
                return self.hp["fg_greed_multiplier"]
            else:
                return self.hp["fg_neutral_multiplier"]
        except Exception:
            # If F&G loading fails, default to normal sizing
            return self.hp["fg_neutral_multiplier"]

    # ── Override position entry ────────────────────────────────────────────────

    def go_long(self) -> None:
        """
        Enter position with F&G-adjusted sizing.
        Inherits go_long() from parent, then multiplies notional by F&G multiplier.
        """
        # Calculate base notional (from parent logic)
        base_notional = self.balance * self.hp["fa_allocation_pct"] * self.hp["leverage"]

        # Apply F&G multiplier
        fg_mult = self._get_fg_multiplier()
        adjusted_notional = base_notional * fg_mult

        # Calculate quantity
        qty = adjusted_notional / self.price

        # Track funding direction
        self._funding_direction = 1  # positive funding direction
        self._reverse_count = 0

        # Buy at market
        self.buy = qty, self.price

        # Initialize shared vars for funding tracking
        if "cumulative_funding" not in self.shared_vars:
            self.shared_vars["cumulative_funding"] = 0.0
        if "funding_direction" not in self.shared_vars:
            self.shared_vars["funding_direction"] = self._funding_direction
        if "fg_multiplier_at_entry" not in self.shared_vars:
            self.shared_vars["fg_multiplier_at_entry"] = fg_mult
