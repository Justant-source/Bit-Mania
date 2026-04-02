"""Delta-neutral position manager.

Ensures spot and perpetual legs stay balanced within tolerance and
monitors margin health.
"""

from __future__ import annotations

import structlog

from shared.exchange.base import ExchangeConnector
from shared.models.order import OrderRequest
from shared.models.position import Position

logger = structlog.get_logger()

# Maximum allowed divergence between spot and futures quantity (0.1%)
MAX_QTY_DIVERGENCE = 0.001
# Minimum margin buffer: 3x the maintenance margin ratio
MARGIN_BUFFER_MULTIPLIER = 3.0
# Default maintenance margin ratio for BTC perps (exchange-dependent)
DEFAULT_MAINTENANCE_MARGIN_RATIO = 0.004


class DeltaNeutralManager:
    """Manage delta-neutral spot + perpetual positions.

    Responsibilities:
    * Verify spot-futures quantity match within tolerance.
    * Monitor margin ratio and trigger warnings / rebalancing.
    * Generate rebalancing orders when quantities diverge.
    """

    def __init__(
        self,
        strategy_id: str,
        exchange: ExchangeConnector,
        spot_symbol: str = "BTC/USDT",
        perp_symbol: str = "BTC/USDT:USDT",
        max_divergence: float = MAX_QTY_DIVERGENCE,
        maintenance_margin_ratio: float = DEFAULT_MAINTENANCE_MARGIN_RATIO,
    ) -> None:
        self.strategy_id = strategy_id
        self.exchange = exchange
        self.spot_symbol = spot_symbol
        self.perp_symbol = perp_symbol
        self.max_divergence = max_divergence
        self.maintenance_margin_ratio = maintenance_margin_ratio

        self._spot_qty: float = 0.0
        self._perp_qty: float = 0.0
        self._margin_ratio: float = 0.0
        self._log = logger.bind(component="delta_neutral", strategy_id=strategy_id)

    # ── quantity tracking ───────────────────────────────────────────────

    def update_quantities(self, spot_qty: float, perp_qty: float) -> None:
        """Update cached quantities (called after fills or periodic refresh)."""
        self._spot_qty = spot_qty
        self._perp_qty = perp_qty

    @property
    def quantity_divergence(self) -> float:
        """Relative divergence between spot and perp quantities.

        Returns a ratio: 0.0 = perfectly matched, 0.001 = 0.1% off, etc.
        """
        if self._spot_qty == 0.0 and self._perp_qty == 0.0:
            return 0.0
        reference = max(self._spot_qty, self._perp_qty)
        if reference == 0.0:
            return 1.0  # one side is empty — fully diverged
        return abs(self._spot_qty - self._perp_qty) / reference

    def is_balanced(self) -> bool:
        """Return True if quantities are within allowed divergence (+/- 0.1%)."""
        return self.quantity_divergence <= self.max_divergence

    # ── margin monitoring ───────────────────────────────────────────────

    def update_margin(self, perp_position: Position | None) -> None:
        """Refresh cached margin data from a perp position snapshot."""
        if perp_position is None:
            self._margin_ratio = 0.0
            return
        self._margin_ratio = perp_position.margin_ratio

    @property
    def margin_buffer(self) -> float:
        """Ratio of current margin to maintenance margin requirement.

        Values > ``MARGIN_BUFFER_MULTIPLIER`` (3.0) are healthy.
        """
        if self.maintenance_margin_ratio == 0.0:
            return float("inf")
        if self._margin_ratio == 0.0:
            return float("inf")
        return abs(self._margin_ratio) / self.maintenance_margin_ratio

    def is_margin_healthy(self) -> bool:
        """Return True if margin buffer exceeds 3x maintenance."""
        return self.margin_buffer >= MARGIN_BUFFER_MULTIPLIER

    # ── rebalancing ─────────────────────────────────────────────────────

    async def check_and_rebalance(self) -> list[OrderRequest]:
        """Check balance and generate rebalancing orders if needed.

        Returns a list of ``OrderRequest`` objects to submit.
        """
        orders: list[OrderRequest] = []

        if self.is_balanced():
            return orders

        divergence = self.quantity_divergence
        diff = self._spot_qty - self._perp_qty

        self._log.warning(
            "delta_divergence_detected",
            spot_qty=self._spot_qty,
            perp_qty=self._perp_qty,
            divergence_pct=round(divergence * 100, 4),
        )

        if diff > 0:
            # Spot exceeds perp — need to increase perp short or sell excess spot
            adjust_qty = abs(diff)
            orders.append(
                OrderRequest(
                    strategy_id=self.strategy_id,
                    exchange=self.exchange.exchange_id,
                    symbol=self.perp_symbol,
                    side="sell",
                    order_type="limit",
                    quantity=adjust_qty,
                    post_only=True,
                )
            )
            self._log.info("rebalance_increase_perp_short", qty=adjust_qty)
        else:
            # Perp exceeds spot — need to buy more spot or reduce perp short
            adjust_qty = abs(diff)
            orders.append(
                OrderRequest(
                    strategy_id=self.strategy_id,
                    exchange=self.exchange.exchange_id,
                    symbol=self.spot_symbol,
                    side="buy",
                    order_type="limit",
                    quantity=adjust_qty,
                    post_only=True,
                )
            )
            self._log.info("rebalance_increase_spot", qty=adjust_qty)

        return orders

    # ── margin risk rebalance ───────────────────────────────────────────

    async def check_margin_risk(self, current_price: float) -> list[OrderRequest]:
        """If margin is unhealthy, generate deleveraging orders.

        Reduces both legs proportionally to restore margin health.
        """
        orders: list[OrderRequest] = []

        if self.is_margin_healthy():
            return orders

        self._log.warning(
            "margin_buffer_low",
            margin_buffer=round(self.margin_buffer, 2),
            threshold=MARGIN_BUFFER_MULTIPLIER,
        )

        # Reduce position by 25% to restore margin health
        reduce_pct = 0.25
        reduce_qty = self._perp_qty * reduce_pct

        if reduce_qty <= 0:
            return orders

        # Close perp first (higher risk), then sell spot
        orders.append(
            OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange.exchange_id,
                symbol=self.perp_symbol,
                side="buy",  # buy to close short
                order_type="limit",
                quantity=reduce_qty,
                reduce_only=True,
                post_only=True,
            )
        )
        orders.append(
            OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange.exchange_id,
                symbol=self.spot_symbol,
                side="sell",
                order_type="limit",
                quantity=reduce_qty,
                post_only=True,
            )
        )

        self._log.info("margin_deleverage", reduce_qty=reduce_qty, reduce_pct=reduce_pct)
        return orders

    # ── diagnostics ─────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Return a diagnostic summary dict."""
        return {
            "spot_qty": self._spot_qty,
            "perp_qty": self._perp_qty,
            "divergence_pct": round(self.quantity_divergence * 100, 4),
            "is_balanced": self.is_balanced(),
            "margin_buffer": round(self.margin_buffer, 2),
            "is_margin_healthy": self.is_margin_healthy(),
        }
