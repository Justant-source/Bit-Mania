"""Buy-and-hold benchmark strategy for Jesse 2.x."""
from __future__ import annotations

from jesse.strategies import Strategy
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class BuyAndHoldStrategy(Strategy):
    """Enter long at first opportunity with 95% of capital, never exit."""

    def __init__(self):
        super().__init__()
        self._entered = False

    def should_long(self) -> bool:
        return not self._entered and not self.is_long

    def should_short(self) -> bool:
        return False

    def go_long(self):
        qty = self.balance * 0.95 * LEVERAGE / self.price
        self.buy = qty, self.price
        self._entered = True

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        pass  # Never exit
