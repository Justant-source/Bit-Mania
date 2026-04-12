"""
Phase 8.1 — Intraday Seasonality Strategy (Jesse)

Based on: Quantpedia research (Vojtko & Javorská 2023)
"Bitcoin exhibits statistically significant excess returns UTC 21:00-23:00"
Original study CAGR: 40.64%, Calmar: 1.79 (2015-2022)

Key hypothesis:
  Post-ETF (2024+), US session volume dominance (57.3%) may have
  strengthened the 21:00-23:00 UTC pattern (US market close).
  CRITICAL: Must verify this pattern persists in 2024-2026 data.

V5 Pass Criteria (ALL must be met):
  - CAGR ≥ 10%
  - Sharpe ≥ 1.0
  - MDD ≤ 15%
  - WF OOS Sharpe ≥ 0.6 × IS Sharpe
  - Monte Carlo 5th percentile Sharpe > 0
  - 2025-2026 regime: ≥ 3 trades/month
  - Sanity check: 0 CRITICAL warnings

Execution:
    # Full validation pipeline
    ./scripts/run_full_validation.sh IntradaySeasonality

    # Or manual backtest
    docker compose exec jesse jesse backtest \\
      '2023-04-01' '2026-04-01' \\
      --route 'Bybit Perpetual:BTCUSDT:1h:IntradaySeasonality'

Hyperparameters (for optimization):
    entry_hour_utc:   19-22 (default: 21) — UTC hour to enter
    exit_hour_utc:    22-24 (default: 23) — UTC hour to exit
    use_trend_filter: True/False (default: True) — only trade above 50 SMA
    use_dow_filter:   True/False (default: True) — only Thursday/Friday
    position_size_pct: 0.1-0.5 (default: 0.25) — fraction of capital
    leverage:         1-3 (default: 2) — leverage multiplier
"""

from __future__ import annotations

import jesse.helpers as jh
from jesse import indicators as ta
from jesse.strategies import Strategy


class IntradaySeasonality(Strategy):
    """
    Buy at UTC 21:00 (or configured hour), sell at UTC 23:00.
    Optional filters: 50-day SMA trend + Thursday/Friday only.

    Key design decisions:
    - No short selling (asymmetric: pattern is long-biased)
    - Hard time exit (not conditional on price)
    - Conservative 2x leverage (V5 max 3x)
    - ATR-based stop loss to limit drawdown
    """

    # ── Hyperparameter definitions ─────────────────────────────────────────────

    def hyperparameters(self) -> list[dict]:
        return [
            {
                "name": "entry_hour_utc",
                "type": int,
                "min": 19,
                "max": 22,
                "default": 21,
            },
            {
                "name": "exit_hour_utc",
                "type": int,
                "min": 22,
                "max": 24,
                "default": 23,
            },
            {
                "name": "use_trend_filter",
                "type": bool,
                "default": True,
            },
            {
                "name": "use_dow_filter",
                "type": bool,
                "default": True,
            },
            {
                "name": "position_size_pct",
                "type": float,
                "min": 0.10,
                "max": 0.50,
                "default": 0.25,
            },
            {
                "name": "leverage",
                "type": int,
                "min": 1,
                "max": 3,
                "default": 2,
            },
            {
                "name": "atr_stop_multiplier",
                "type": float,
                "min": 1.0,
                "max": 4.0,
                "default": 2.0,
            },
        ]

    # ── Time helpers ───────────────────────────────────────────────────────────

    @property
    def _current_arrow(self):
        """Return Arrow datetime object for current candle open timestamp."""
        return jh.timestamp_to_arrow(self.current_candle[0])

    @property
    def current_hour_utc(self) -> int:
        """UTC hour of current candle (0-23)."""
        return self._current_arrow.hour

    @property
    def current_dow(self) -> int:
        """Day of week: 0=Monday, 6=Sunday."""
        return self._current_arrow.weekday()

    # ── Indicators ─────────────────────────────────────────────────────────────

    @property
    def sma_50(self) -> float:
        """50-day SMA approximated from 1h candles (1200 = 50 days × 24 hours).
        Avoids requiring a separate 1D data_route in Jesse research API.
        """
        if len(self.candles) < 1200:
            return self.price  # insufficient warm-up data → assume trend up
        return ta.sma(self.candles, 1200, "close")

    @property
    def atr_14(self) -> float:
        """14-period ATR on hourly candles for stop placement."""
        return ta.atr(self.candles, 14)

    # ── Entry conditions ───────────────────────────────────────────────────────

    @property
    def _trend_ok(self) -> bool:
        """Trend filter: price must be above 50-day SMA."""
        if not self.hp["use_trend_filter"]:
            return True
        return self.price > self.sma_50

    @property
    def _dow_ok(self) -> bool:
        """Day-of-week filter: Thursday (3) or Friday (4) only."""
        if not self.hp["use_dow_filter"]:
            return True
        return self.current_dow in {3, 4}

    @property
    def _is_entry_hour(self) -> bool:
        """Check if current candle opens at the configured entry hour."""
        return self.current_hour_utc == self.hp["entry_hour_utc"]

    def should_long(self) -> bool:
        if self.position.is_open:
            return False
        if not self._is_entry_hour:
            return False
        if not self._dow_ok:
            return False
        if not self._trend_ok:
            return False
        return True

    def should_short(self) -> bool:
        # Asymmetric strategy: long-only
        return False

    def should_cancel_entry(self) -> bool:
        # If we somehow missed entry hour, cancel
        return self.current_hour_utc != self.hp["entry_hour_utc"]

    # ── Position management ────────────────────────────────────────────────────

    def go_long(self) -> None:
        """Enter long with configured size and ATR stop loss."""
        notional = self.balance * self.hp["position_size_pct"] * self.hp["leverage"]
        qty = notional / self.price

        # ATR-based stop loss
        stop_price = self.price - (self.atr_14 * self.hp["atr_stop_multiplier"])
        stop_price = max(stop_price, self.price * 0.95)  # max 5% stop

        self.buy = qty, self.price
        self.stop_loss = qty, stop_price

    def go_short(self) -> None:
        pass  # long-only strategy

    def update_position(self) -> None:
        """Time-based exit: close at configured exit hour."""
        if self.current_hour_utc >= self.hp["exit_hour_utc"]:
            self.liquidate()

    # ── Logging hooks (optional debugging) ────────────────────────────────────

    def on_open_position(self, order) -> None:
        pass

    def on_close_position(self, order, closed_trade) -> None:
        pass
