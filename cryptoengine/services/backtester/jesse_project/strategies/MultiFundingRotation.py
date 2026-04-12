# MultiFundingRotation.py — Multi-Symbol Funding Rate Rotation Strategy
#
# Strategy: Dynamically rotate between top funding rate symbols to maximize income.
#
# Symbols tracked: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT (can be extended)
# Rotation logic: Every 8h settlement, pick top 3 by funding rate
# Position sizing: 20% equity per active symbol (max 3 active = 60% total equity deployed)
#
# Integration Points:
#   - Funding rate data from shared_vars['funding_rates'] = {symbol: rate}
#   - OHLCV candles for each symbol
#

from __future__ import annotations

from datetime import datetime, timezone
import jesse.indicators as ta
from jesse.strategies import Strategy
from jesse import utils

import logging

logger = logging.getLogger(__name__)


class MultiFundingRotation(Strategy):
    """
    Multi-symbol funding rate rotation strategy.

    Dynamically allocates capital to the top 3 symbols by funding rate,
    rebalancing at each 8h funding settlement.

    Hyperparameters:
        min_funding_rate (float): Minimum funding rate to consider entering (default 0.0001)
        equity_per_symbol (float): Equity % per active symbol (default 0.20 = 20%)
        max_active_symbols (int): Maximum concurrent positions (default 3)
        leverage (float): Leverage applied to each position (default 5.0)
    """

    # Symbols to track for funding rotation
    SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
    SETTLEMENT_HOURS = {0, 8, 16}

    hp = {
        'min_funding_rate': 0.0001,   # 0.01%/8h entry threshold
        'equity_per_symbol': 0.20,    # 20% per symbol
        'max_active_symbols': 3,       # max 3 positions
        'leverage': 5.0,               # max 5x per Phase 5 constraints
    }

    def __init__(self):
        super().__init__()

        # Track active positions by symbol
        self.active_positions = {}  # {symbol: {qty, entry_price, funding_collected}}

        # Track funding rates by symbol
        self.funding_rates = {}     # {symbol: current_rate}

        # Settlement tracking
        self.last_settlement_hour = None
        self.total_funding_income = 0.0

        logger.info(
            f"MultiFundingRotation initialized. "
            f"Tracking symbols: {self.SYMBOLS}, "
            f"Max active: {self.hp['max_active_symbols']}"
        )

    @property
    def current_funding_rates(self) -> dict:
        """
        Fetch funding rates for all tracked symbols.

        Data source: shared_vars['funding_rates'] = {symbol: rate}

        TODO: Wire up with real funding rate data from data pipeline
        """
        # Attempt to get from shared_vars
        if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
            rates = self.shared_vars.get('funding_rates', {})
            if isinstance(rates, dict):
                return rates

        # Fallback: return empty (no trades until data is available)
        return {}

    def is_settlement_time(self) -> bool:
        """Check if current candle is a funding settlement time."""
        dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        return dt.hour in self.SETTLEMENT_HOURS

    def get_top_symbols_by_funding(self, n: int = 3) -> list:
        """
        Get top N symbols by funding rate that exceed min_funding_rate.

        Args:
            n: Number of top symbols to return

        Returns:
            List of symbol names, sorted by funding rate (highest first)
        """
        funding_rates = self.current_funding_rates
        min_rate = self.hp['min_funding_rate']

        # Filter symbols meeting minimum funding rate
        eligible = [
            (symbol, rate)
            for symbol, rate in funding_rates.items()
            if symbol in self.SYMBOLS and rate >= min_rate
        ]

        # Sort by funding rate descending
        eligible.sort(key=lambda x: x[1], reverse=True)

        # Return top N symbol names
        return [symbol for symbol, _ in eligible[:n]]

    def rebalance_positions(self):
        """
        Called at settlement time to rebalance positions.
        Close positions in low-ranking symbols, open positions in top-ranking symbols.
        """
        top_symbols = self.get_top_symbols_by_funding(self.hp['max_active_symbols'])

        logger.info(
            f"Rebalancing at settlement time. "
            f"Top symbols by funding: {top_symbols}"
        )

        # Close positions not in top list
        for symbol in list(self.active_positions.keys()):
            if symbol not in top_symbols:
                logger.info(f"Closing position in {symbol} (no longer top ranked)")
                # Close logic would go here
                # (Jesse's multi-symbol support requires switching timeframes)
                del self.active_positions[symbol]

        # Open new positions in top symbols not yet held
        for symbol in top_symbols:
            if symbol not in self.active_positions:
                # Calculate position size: equity_per_symbol × leverage / price
                equity = self.balance
                position_value = equity * self.hp['equity_per_symbol']
                notional = position_value * self.hp['leverage']

                # Get current price for this symbol (requires multi-symbol support)
                # For now, use current close as placeholder
                price = self.close
                qty = notional / price

                logger.info(
                    f"Opening position in {symbol}: "
                    f"qty={qty:.4f}, notional=${notional:.2f}, "
                    f"funding_rate={self.current_funding_rates.get(symbol, 0.0):.6f}"
                )

                self.active_positions[symbol] = {
                    'qty': qty,
                    'entry_price': price,
                    'funding_collected': 0.0,
                }

    def should_long(self) -> bool:
        """Long disabled."""
        return False

    def should_short(self) -> bool:
        """
        Entry condition: at settlement time, rebalance portfolio.
        (Jesse multi-symbol strategy requires custom integration)
        """
        if not self.is_settlement_time():
            return False

        dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        if self.last_settlement_hour == dt.hour:
            return False  # Already processed this settlement

        self.last_settlement_hour = dt.hour
        self.rebalance_positions()

        return False  # Jesse framework handles position management

    def should_exit_long(self) -> bool:
        return False

    def should_exit_short(self) -> bool:
        """Exit when symbol rotates out of top N."""
        return False

    def should_cancel_entry(self) -> bool:
        return False

    def go_long(self):
        pass

    def go_short(self):
        """Entry handled by rebalance_positions()."""
        pass

    def update_position(self):
        """Update position metadata (funding income tracking)."""
        if not self.active_positions:
            return

        # Accumulate funding income at settlement times
        if self.is_settlement_time():
            dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
            if self.last_settlement_hour != dt.hour:
                self.last_settlement_hour = dt.hour

                for symbol, data in self.active_positions.items():
                    funding_rate = self.current_funding_rates.get(symbol, 0.0)
                    notional = abs(data['qty']) * data.get('current_price', self.close)
                    funding_pnl = notional * funding_rate

                    data['funding_collected'] += funding_pnl
                    self.total_funding_income += funding_pnl

                    logger.debug(
                        f"Funding collected from {symbol}: "
                        f"${funding_pnl:.2f} (total: ${data['funding_collected']:.2f})"
                    )

    def on_open_position(self):
        logger.info("Position opened in rotation strategy")

    def on_close_position(self):
        logger.info("Position closed in rotation strategy")


class SimpleFundingRotation(Strategy):
    """
    Simplified funding rotation: rotate between 2 symbols only (BTC and ETH).
    Useful for testing rotation logic with less complexity.
    """

    hp = {
        'min_funding_rate': 0.0001,
        'equity_per_symbol': 0.40,  # 40% each (2 symbols = 80% total)
        'leverage': 5.0,
    }

    SETTLEMENT_HOURS = {0, 8, 16}

    def __init__(self):
        super().__init__()
        self.active_symbols = []
        self.last_settlement_hour = None
        self.funding_income = 0.0

    @property
    def current_funding_rates(self) -> dict:
        if hasattr(self, 'shared_vars') and isinstance(self.shared_vars, dict):
            return self.shared_vars.get('funding_rates', {})
        return {}

    def is_settlement_time(self) -> bool:
        dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
        return dt.hour in self.SETTLEMENT_HOURS

    def should_long(self) -> bool:
        return False

    def should_short(self) -> bool:
        return False

    def should_exit_long(self) -> bool:
        return False

    def should_exit_short(self) -> bool:
        return False

    def should_cancel_entry(self) -> bool:
        return False

    def go_long(self):
        pass

    def go_short(self):
        pass

    def update_position(self):
        if self.is_settlement_time():
            dt = datetime.fromtimestamp(self.time / 1000, tz=timezone.utc)
            if self.last_settlement_hour != dt.hour:
                self.last_settlement_hour = dt.hour
                logger.info("Rotation settlement checkpoint")
