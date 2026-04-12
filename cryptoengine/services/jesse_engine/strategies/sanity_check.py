"""
Phase 7.2 — Jesse Sanity Check Strategy.

BTC Buy-and-Hold: enters long on first bar, never exits.
Expected result for 2024: CAGR ~+120%, MDD ~-25%, Sharpe 1.5-2.0

If Jesse output diverges significantly from these known values,
there is a configuration or data import problem.
"""
from jesse.strategies import Strategy
import jesse.helpers as jh


class BtcBuyAndHold(Strategy):
    """
    Simplest possible strategy: buy BTC and hold forever.
    Used as sanity check to verify Jesse engine correctness.

    Run command:
        docker compose exec jesse jesse backtest \
          '2024-01-01' '2024-12-31' \
          --route 'Bybit Perpetual:BTCUSDT:1h:BtcBuyAndHold'

    Expected (2024): CAGR ~120%, MDD ~-25%, Sharpe 1.5-2.0
    """

    def should_long(self) -> bool:
        return self.position.is_close

    def should_short(self) -> bool:
        return False

    def go_long(self):
        # Buy with 95% of capital (leave 5% for fees)
        qty = (self.balance * 0.95) / self.price
        self.buy = qty, self.price

    def go_short(self):
        pass

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        # Never exit — hold forever
        pass

    def on_open_position(self, order) -> None:
        pass
