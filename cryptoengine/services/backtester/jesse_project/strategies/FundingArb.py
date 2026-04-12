# FundingArb.py — Funding Arbitrage Strategy for Jesse Backtester
#
# Strategy: Collect positive funding rates by holding perpetual short positions.
#
# Domain Knowledge:
#   - Bybit funding rate settles every 8 hours: UTC 00:00, 08:00, 16:00
#   - Correct funding PnL: notional × |funding_rate| (when short and funding > 0)
#   - Fees charged on notional: taker 0.055% per Bybit
#   - Entry: funding_rate >= min_funding_rate (default 0.01%/8h = 0.0001)
#   - Exit: funding_rate <= exit_funding_rate (default 0.005%/8h = 0.00005)
#
# Key Jesse Strategy API:
#   - should_long() / should_short() — entry signals
#   - should_exit_long() / should_exit_short() — exit signals
#   - update_position() — runs every candle while in position
#   - on_open_position() / on_close_position() — lifecycle hooks
#   - self.position.qty — position size (positive = long, negative = short)
#   - self.position.pnl — P&L in %
#   - self.leverage — leverage multiplier
#

from __future__ import annotations

from datetime import datetime, timezone, timedelta
import jesse.indicators as ta
from jesse.strategies import Strategy
from jesse import utils

import logging

logger = logging.getLogger(__name__)


class FundingArb(Strategy):
    """
    Funding Arbitrage Strategy — Collect positive funding rates with short positions.

    Hyperparameters:
        min_funding_rate (float): Entry threshold in decimal (e.g. 0.0001 = 0.01%/8h)
        exit_funding_rate (float): Exit threshold in decimal
        leverage (float): Maximum leverage (default 5.0 per CLAUDE.md limit)
        size_pct (float): Percentage of equity to deploy as position size (0.0-1.0)
    """

    # Hyperparameters for optimization
    hp = {
        'min_funding_rate': 0.0001,   # 0.01%/8h entry threshold
        'exit_funding_rate': 0.00005, # 0.005%/8h exit threshold
        'leverage': 5.0,              # max 5x per Phase 5 constraints
        'size_pct': 0.80,             # deploy 80% of equity
    }

    # Bybit funding settlement hours (UTC)
    SETTLEMENT_HOURS = {0, 8, 16}

    def __init__(self):
        super().__init__()

        # Track accumulated funding income across the backtest
        self.funding_income = 0.0
        self.last_settlement_hour = None

        # Position metadata
        self.entry_time = None
        self.entry_price = None
        self.funding_collected = 0.0

        logger.info(f"FundingArb initialized with params: {self.hp}")

    @property
    def current_funding_rate(self) -> float:
        """
        Get current funding rate for the symbol.

        NOTE: Jesse doesn't natively include funding rate in OHLCV candles.
        This implementation uses a placeholder that must be supplemented with:
        1. External funding rate data via custom indicators
        2. Real-time data from shared_vars (set by data pipeline)
        3. Coinalyze API integration (future work)

        TODO: Integrate with fetch_coinalyze_funding.py or jesse data loader
        """
        # Attempt to get from shared_vars (set by data loader)
        if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
            funding_data = self.shared_vars.get('funding_rate', {})
            if isinstance(funding_data, dict):
                return funding_data.get(self.time, 0.0)

        # Fallback: return 0.0 (no funding signal)
        # This will cause no trades until funding_rate data is plumbed in
        return 0.0

    def is_settlement_time(self) -> bool:
        """Check if current candle timestamp is a settlement time (00:00, 08:00, 16:00 UTC)."""
        dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        return dt.hour in self.SETTLEMENT_HOURS

    def should_long(self) -> bool:
        """Long is disabled in this funding arb strategy (we only go short)."""
        return False

    def should_short(self) -> bool:
        """
        Signal to open short position when funding rate exceeds entry threshold.
        """
        # Only enter if not already in a position
        if self.position.is_open:
            return False

        funding_rate = self.current_funding_rate
        return funding_rate >= self.hp['min_funding_rate']

    def should_exit_long(self) -> bool:
        """No long positions in this strategy."""
        return False

    def should_exit_short(self) -> bool:
        """
        Signal to close short position when funding rate drops below exit threshold
        or becomes negative.
        """
        if not self.position.is_open:
            return False

        if self.position.qty >= 0:
            # Not in a short position
            return False

        funding_rate = self.current_funding_rate
        return funding_rate <= self.hp['exit_funding_rate']

    def should_cancel_entry(self) -> bool:
        """Cancel pending entry if funding rate drops below minimum threshold."""
        if not self.pending_orders:
            return False

        funding_rate = self.current_funding_rate
        return funding_rate < self.hp['exit_funding_rate']

    def go_long(self):
        """Long entry disabled."""
        pass

    def go_short(self):
        """
        Open short position to collect positive funding.

        Position sizing:
          - Use fixed percentage of equity (size_pct)
          - Apply leverage to increase notional exposure
          - Example: equity=$10k, size_pct=0.80, lev=5x → notional=$40k
        """
        # Calculate position size based on current equity
        equity = self.balance
        position_value = equity * self.hp['size_pct']

        # Apply leverage to get notional exposure
        notional = position_value * self.hp['leverage']

        # Convert notional to quantity at current close price
        qty = notional / self.close

        # Place short order at market
        # Jesse syntax: negative qty for short
        self.sell_at_market(-qty)

        # Record entry metadata
        self.entry_time = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        self.entry_price = self.close
        self.funding_collected = 0.0

        logger.info(
            f"FundingArb SHORT entry: price={self.close:.2f}, qty={qty:.4f}, "
            f"notional=${notional:.2f}, funding_rate={self.current_funding_rate:.6f}"
        )

    def update_position(self):
        """
        Called every candle while a position is open.
        Accumulate funding income at settlement times.
        """
        if not self.position.is_open:
            return

        # Check if we're at a settlement time
        if self.is_settlement_time():
            # Only process once per settlement (avoid duplicate settlement hours)
            dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
            if self.last_settlement_hour != dt.hour:
                self.last_settlement_hour = dt.hour

                # Calculate funding income
                # For a short position with positive funding:
                # funding_pnl = -position_qty × notional × funding_rate
                # (Negative qty × negative return = positive income)
                funding_rate = self.current_funding_rate
                notional = abs(self.position.qty) * self.close
                funding_pnl = notional * funding_rate

                self.funding_income += funding_pnl
                self.funding_collected += funding_pnl

                logger.debug(
                    f"FundingArb settlement: funding_rate={funding_rate:.6f}, "
                    f"notional=${notional:.2f}, pnl=${funding_pnl:.2f}, "
                    f"accumulated=${self.funding_collected:.2f}"
                )

    def on_open_position(self):
        """Lifecycle hook: position opened."""
        logger.info(f"Position opened at {self.entry_price:.2f}")

    def on_close_position(self):
        """Lifecycle hook: position closed. Record final stats."""
        if self.entry_time:
            hold_duration = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc) - self.entry_time
            logger.info(
                f"Position closed after {hold_duration}. "
                f"Total funding collected: ${self.funding_collected:.2f}"
            )


class FundingArbConservative(Strategy):
    """
    Conservative variant: tighter thresholds, longer holds, smaller positions.
    Useful for stress testing and low-volatility regimes.
    """

    hp = {
        'min_funding_rate': 0.00015,  # 0.015%/8h (higher bar)
        'exit_funding_rate': 0.000025, # 0.0025%/8h (tighter exit)
        'leverage': 3.0,               # more conservative
        'size_pct': 0.50,              # smaller position
    }

    SETTLEMENT_HOURS = {0, 8, 16}

    def __init__(self):
        super().__init__()
        self.funding_income = 0.0
        self.last_settlement_hour = None

    @property
    def current_funding_rate(self) -> float:
        if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
            funding_data = self.shared_vars.get('funding_rate', {})
            if isinstance(funding_data, dict):
                return funding_data.get(self.time, 0.0)
        return 0.0

    def is_settlement_time(self) -> bool:
        dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        return dt.hour in self.SETTLEMENT_HOURS

    def should_long(self) -> bool:
        return False

    def should_short(self) -> bool:
        if self.position.is_open:
            return False
        funding_rate = self.current_funding_rate
        return funding_rate >= self.hp['min_funding_rate']

    def should_exit_long(self) -> bool:
        return False

    def should_exit_short(self) -> bool:
        if not self.position.is_open or self.position.qty >= 0:
            return False
        funding_rate = self.current_funding_rate
        return funding_rate <= self.hp['exit_funding_rate']

    def should_cancel_entry(self) -> bool:
        if not self.pending_orders:
            return False
        funding_rate = self.current_funding_rate
        return funding_rate < self.hp['exit_funding_rate']

    def go_long(self):
        pass

    def go_short(self):
        equity = self.balance
        position_value = equity * self.hp['size_pct']
        notional = position_value * self.hp['leverage']
        qty = notional / self.close
        self.sell_at_market(-qty)
        logger.info(f"Conservative SHORT: {qty:.4f} units at ${self.close:.2f}")

    def update_position(self):
        if not self.position.is_open:
            return

        if self.is_settlement_time():
            dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
            if self.last_settlement_hour != dt.hour:
                self.last_settlement_hour = dt.hour
                funding_rate = self.current_funding_rate
                notional = abs(self.position.qty) * self.close
                funding_pnl = notional * funding_rate
                self.funding_income += funding_pnl
