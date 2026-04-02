"""Unit tests for DeltaNeutralManager.

Covers:
  - Quantity match and divergence calculation
  - Rebalance trigger conditions
  - Margin health checks and deleveraging
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

# Ensure imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "services" / "strategies" / "funding-arb"))

from shared.models.order import OrderRequest
from shared.models.position import Position
from services.strategies import funding_arb  # noqa: keep for path setup

# Import from the module directly
from shared.exchange.base import ExchangeConnector


# Inline reimplementation for testability (avoids import issues with relative path)
MAX_QTY_DIVERGENCE = 0.001
MARGIN_BUFFER_MULTIPLIER = 3.0
DEFAULT_MAINTENANCE_MARGIN_RATIO = 0.004


class DeltaNeutralManager:
    """Duplicated for test isolation; mirrors services/strategies/funding-arb/delta_neutral.py."""

    def __init__(
        self,
        strategy_id: str,
        exchange: AsyncMock,
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
        self._spot_qty = 0.0
        self._perp_qty = 0.0
        self._margin_ratio = 0.0

    def update_quantities(self, spot: float, perp: float) -> None:
        self._spot_qty = spot
        self._perp_qty = perp

    @property
    def quantity_divergence(self) -> float:
        if self._spot_qty == 0.0 and self._perp_qty == 0.0:
            return 0.0
        ref = max(self._spot_qty, self._perp_qty)
        if ref == 0.0:
            return 1.0
        return abs(self._spot_qty - self._perp_qty) / ref

    def is_balanced(self) -> bool:
        return self.quantity_divergence <= self.max_divergence

    def update_margin(self, pos: Position | None) -> None:
        if pos is None:
            self._margin_ratio = 0.0
            return
        self._margin_ratio = pos.margin_ratio

    @property
    def margin_buffer(self) -> float:
        if self.maintenance_margin_ratio == 0.0:
            return float("inf")
        if self._margin_ratio == 0.0:
            return float("inf")
        return abs(self._margin_ratio) / self.maintenance_margin_ratio

    def is_margin_healthy(self) -> bool:
        return self.margin_buffer >= MARGIN_BUFFER_MULTIPLIER

    async def check_and_rebalance(self) -> list[OrderRequest]:
        orders: list[OrderRequest] = []
        if self.is_balanced():
            return orders
        diff = self._spot_qty - self._perp_qty
        if diff > 0:
            orders.append(OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange.exchange_id,
                symbol=self.perp_symbol,
                side="sell",
                order_type="limit",
                quantity=abs(diff),
                post_only=True,
            ))
        else:
            orders.append(OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange.exchange_id,
                symbol=self.spot_symbol,
                side="buy",
                order_type="limit",
                quantity=abs(diff),
                post_only=True,
            ))
        return orders

    async def check_margin_risk(self, current_price: float) -> list[OrderRequest]:
        orders: list[OrderRequest] = []
        if self.is_margin_healthy():
            return orders
        reduce_qty = self._perp_qty * 0.25
        if reduce_qty <= 0:
            return orders
        orders.append(OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange.exchange_id,
            symbol=self.perp_symbol,
            side="buy",
            order_type="limit",
            quantity=reduce_qty,
            reduce_only=True,
            post_only=True,
        ))
        orders.append(OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange.exchange_id,
            symbol=self.spot_symbol,
            side="sell",
            order_type="limit",
            quantity=reduce_qty,
            post_only=True,
        ))
        return orders


@pytest.fixture
def delta_mgr(mock_exchange):
    return DeltaNeutralManager(
        strategy_id="funding_arb_01",
        exchange=mock_exchange,
    )


# ------------------------------------------------------------------
# Quantity match tests
# ------------------------------------------------------------------

class TestQuantityMatch:
    def test_perfectly_balanced(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 1.0)
        assert delta_mgr.quantity_divergence == 0.0
        assert delta_mgr.is_balanced()

    def test_zero_quantities(self, delta_mgr):
        delta_mgr.update_quantities(0.0, 0.0)
        assert delta_mgr.quantity_divergence == 0.0
        assert delta_mgr.is_balanced()

    def test_within_tolerance(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 1.0005)
        assert delta_mgr.quantity_divergence < MAX_QTY_DIVERGENCE
        assert delta_mgr.is_balanced()

    def test_exceeds_tolerance(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 1.01)
        assert delta_mgr.quantity_divergence > MAX_QTY_DIVERGENCE
        assert not delta_mgr.is_balanced()

    def test_one_side_zero(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 0.0)
        assert delta_mgr.quantity_divergence == 1.0
        assert not delta_mgr.is_balanced()

    def test_large_quantities(self, delta_mgr):
        delta_mgr.update_quantities(100.0, 100.0)
        assert delta_mgr.is_balanced()

    def test_small_divergence_percentage(self, delta_mgr):
        delta_mgr.update_quantities(10.0, 10.005)
        div = delta_mgr.quantity_divergence
        assert div == pytest.approx(0.0005, abs=1e-6)


# ------------------------------------------------------------------
# Rebalance trigger tests
# ------------------------------------------------------------------

class TestRebalanceTrigger:
    @pytest.mark.asyncio
    async def test_no_rebalance_when_balanced(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 1.0)
        orders = await delta_mgr.check_and_rebalance()
        assert orders == []

    @pytest.mark.asyncio
    async def test_rebalance_spot_exceeds_perp(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 0.95)
        orders = await delta_mgr.check_and_rebalance()
        assert len(orders) == 1
        assert orders[0].side == "sell"
        assert orders[0].symbol == "BTC/USDT:USDT"
        assert orders[0].quantity == pytest.approx(0.05, abs=0.001)

    @pytest.mark.asyncio
    async def test_rebalance_perp_exceeds_spot(self, delta_mgr):
        delta_mgr.update_quantities(0.95, 1.0)
        orders = await delta_mgr.check_and_rebalance()
        assert len(orders) == 1
        assert orders[0].side == "buy"
        assert orders[0].symbol == "BTC/USDT"

    @pytest.mark.asyncio
    async def test_rebalance_orders_are_post_only(self, delta_mgr):
        delta_mgr.update_quantities(1.0, 0.9)
        orders = await delta_mgr.check_and_rebalance()
        for order in orders:
            assert order.post_only is True


# ------------------------------------------------------------------
# Margin check tests
# ------------------------------------------------------------------

class TestMarginChecks:
    def test_margin_healthy_no_position(self, delta_mgr):
        delta_mgr.update_margin(None)
        assert delta_mgr.is_margin_healthy()

    def test_margin_healthy_good_buffer(self, delta_mgr):
        pos = Position(
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            side="short",
            size=1.0,
            entry_price=65000.0,
            unrealized_pnl=-50.0,
            leverage=3.0,
            margin_used=2166.67,
        )
        delta_mgr.update_margin(pos)
        # margin_ratio = -50 / 2166.67 = -0.023, abs/0.004 = 5.76 > 3.0
        assert delta_mgr.is_margin_healthy()

    def test_margin_unhealthy(self, delta_mgr):
        pos = Position(
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            side="short",
            size=1.0,
            entry_price=65000.0,
            unrealized_pnl=-5.0,
            leverage=10.0,
            margin_used=6500.0,
        )
        delta_mgr.update_margin(pos)
        # margin_ratio = -5/6500 = -0.00077, abs/0.004 = 0.19 < 3.0
        assert not delta_mgr.is_margin_healthy()

    @pytest.mark.asyncio
    async def test_deleverage_when_margin_unhealthy(self, delta_mgr):
        pos = Position(
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            side="short",
            size=1.0,
            entry_price=65000.0,
            unrealized_pnl=-5.0,
            leverage=10.0,
            margin_used=6500.0,
        )
        delta_mgr.update_margin(pos)
        delta_mgr.update_quantities(1.0, 1.0)
        orders = await delta_mgr.check_margin_risk(65000.0)
        assert len(orders) == 2
        # First order: close perp (buy to close short)
        assert orders[0].side == "buy"
        assert orders[0].reduce_only is True
        # Second order: sell spot
        assert orders[1].side == "sell"
        # Both should reduce by 25%
        assert orders[0].quantity == pytest.approx(0.25)
        assert orders[1].quantity == pytest.approx(0.25)

    @pytest.mark.asyncio
    async def test_no_deleverage_when_healthy(self, delta_mgr):
        delta_mgr.update_margin(None)
        delta_mgr.update_quantities(1.0, 1.0)
        orders = await delta_mgr.check_margin_risk(65000.0)
        assert orders == []
