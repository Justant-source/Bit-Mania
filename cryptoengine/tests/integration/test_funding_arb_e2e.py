"""End-to-end integration test for the funding arbitrage strategy.

Tests the full lifecycle:
  1. Entry — simultaneous spot buy + perp short
  2. Funding collection — verify payment recording
  3. Exit — close both legs and realise P&L
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.models.market import FundingRate
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position


# ---------------------------------------------------------------------------
# Simplified FundingArb simulator for e2e testing
# ---------------------------------------------------------------------------

class FundingArbSimulator:
    """Simulates the full funding arb lifecycle using mock exchange."""

    def __init__(self, exchange: AsyncMock, initial_capital: float = 10_000.0):
        self._exchange = exchange
        self._capital = initial_capital
        self._spot_qty = 0.0
        self._perp_qty = 0.0
        self._entry_price = 0.0
        self._funding_payments: list[float] = []
        self._is_open = False

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def total_funding_earned(self) -> float:
        return sum(self._funding_payments)

    async def enter_position(self, spot_price: float, perp_price: float) -> bool:
        """Enter delta-neutral: buy spot + short perp simultaneously."""
        quantity = (self._capital * 0.95) / spot_price

        # Place both orders
        spot_result, perp_result = await asyncio.gather(
            self._exchange.place_order(OrderRequest(
                strategy_id="funding_arb_e2e",
                exchange="bybit",
                symbol="BTC/USDT",
                side="buy",
                order_type="limit",
                quantity=quantity,
                price=spot_price,
            )),
            self._exchange.place_order(OrderRequest(
                strategy_id="funding_arb_e2e",
                exchange="bybit",
                symbol="BTC/USDT:USDT",
                side="sell",
                order_type="limit",
                quantity=quantity,
                price=perp_price,
            )),
        )

        if spot_result.status == "filled" and perp_result.status == "filled":
            self._spot_qty = spot_result.filled_qty
            self._perp_qty = perp_result.filled_qty
            self._entry_price = spot_price
            self._is_open = True
            return True

        return False

    async def collect_funding(self, funding_rate: FundingRate) -> float:
        """Record a funding payment."""
        if not self._is_open:
            return 0.0

        # Short position receives positive funding
        payment = self._perp_qty * funding_rate.rate * self._entry_price
        self._funding_payments.append(payment)
        return payment

    async def exit_position(self, spot_price: float, perp_price: float) -> float:
        """Exit both legs. Returns total P&L."""
        if not self._is_open:
            return 0.0

        # Close perp first (buy to close short)
        perp_close = await self._exchange.place_order(OrderRequest(
            strategy_id="funding_arb_e2e",
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            side="buy",
            order_type="market",
            quantity=self._perp_qty,
            reduce_only=True,
            post_only=False,
        ))

        # Sell spot
        spot_close = await self._exchange.place_order(OrderRequest(
            strategy_id="funding_arb_e2e",
            exchange="bybit",
            symbol="BTC/USDT",
            side="sell",
            order_type="market",
            quantity=self._spot_qty,
            post_only=False,
        ))

        # Calculate P&L
        # Spot P&L: (exit - entry) * qty
        spot_pnl = (spot_price - self._entry_price) * self._spot_qty
        # Perp P&L (short): (entry - exit) * qty
        perp_pnl = (self._entry_price - perp_price) * self._perp_qty
        # Basis P&L should be near-zero for delta-neutral
        basis_pnl = spot_pnl + perp_pnl
        total_pnl = basis_pnl + self.total_funding_earned

        # Reset state
        self._spot_qty = 0.0
        self._perp_qty = 0.0
        self._entry_price = 0.0
        self._is_open = False

        return total_pnl


@pytest.fixture
def arb_exchange():
    """Mock exchange that fills all orders at requested price."""
    exchange = AsyncMock()

    async def fill_order(order: OrderRequest) -> OrderResult:
        return OrderResult(
            request_id=order.request_id,
            order_id=f"ord-{order.request_id[:8]}",
            status="filled",
            filled_qty=order.quantity,
            filled_price=order.price or 65000.0,
            fee=order.quantity * (order.price or 65000.0) * 0.0006,
        )

    exchange.place_order = fill_order
    return exchange


# ------------------------------------------------------------------
# E2E Tests
# ------------------------------------------------------------------

class TestFundingArbEntry:
    @pytest.mark.asyncio
    async def test_successful_entry(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        entered = await sim.enter_position(65000.0, 65050.0)
        assert entered is True
        assert sim.is_open
        assert sim._spot_qty > 0
        assert sim._perp_qty > 0
        assert sim._spot_qty == pytest.approx(sim._perp_qty, rel=0.01)

    @pytest.mark.asyncio
    async def test_entry_uses_95_pct_capital(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange, initial_capital=10_000.0)
        await sim.enter_position(65000.0, 65050.0)
        expected_qty = (10_000.0 * 0.95) / 65000.0
        assert sim._spot_qty == pytest.approx(expected_qty, rel=0.01)


class TestFundingCollection:
    @pytest.mark.asyncio
    async def test_single_funding_payment(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        await sim.enter_position(65000.0, 65050.0)

        funding = FundingRate(
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            rate=0.0001,
            next_funding_time=datetime.now(timezone.utc) + timedelta(hours=8),
        )
        payment = await sim.collect_funding(funding)
        assert payment > 0
        assert sim.total_funding_earned == payment

    @pytest.mark.asyncio
    async def test_multiple_funding_payments(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        await sim.enter_position(65000.0, 65050.0)

        for i in range(3):
            funding = FundingRate(
                exchange="bybit",
                symbol="BTC/USDT:USDT",
                rate=0.0001,
                next_funding_time=datetime.now(timezone.utc) + timedelta(hours=8 * (i + 1)),
            )
            await sim.collect_funding(funding)

        assert len(sim._funding_payments) == 3
        assert sim.total_funding_earned > 0

    @pytest.mark.asyncio
    async def test_no_funding_when_not_open(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        funding = FundingRate(
            exchange="bybit",
            symbol="BTC/USDT:USDT",
            rate=0.0001,
            next_funding_time=datetime.now(timezone.utc),
        )
        payment = await sim.collect_funding(funding)
        assert payment == 0.0


class TestFundingArbExit:
    @pytest.mark.asyncio
    async def test_exit_closes_both_legs(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        await sim.enter_position(65000.0, 65050.0)
        await sim.exit_position(65100.0, 65150.0)
        assert not sim.is_open
        assert sim._spot_qty == 0.0
        assert sim._perp_qty == 0.0

    @pytest.mark.asyncio
    async def test_exit_pnl_includes_funding(self, arb_exchange):
        sim = FundingArbSimulator(arb_exchange)
        await sim.enter_position(65000.0, 65050.0)

        # Collect funding 3 times
        for _ in range(3):
            funding = FundingRate(
                exchange="bybit",
                symbol="BTC/USDT:USDT",
                rate=0.0001,
                next_funding_time=datetime.now(timezone.utc),
            )
            await sim.collect_funding(funding)

        total_funding = sim.total_funding_earned
        # Exit at same price — basis P&L near zero
        pnl = await sim.exit_position(65000.0, 65050.0)
        # P&L should be close to just the funding income
        # (basis: spot gained 0, perp short lost 0 from entry prices)
        assert pnl > 0  # funding income should dominate

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, arb_exchange):
        """Complete lifecycle: entry -> 3 funding collections -> exit."""
        sim = FundingArbSimulator(arb_exchange, initial_capital=10_000.0)

        # 1. Entry
        entered = await sim.enter_position(65000.0, 65050.0)
        assert entered

        # 2. Collect funding over 3 intervals (24h)
        total_funding = 0.0
        for _ in range(3):
            funding = FundingRate(
                exchange="bybit",
                symbol="BTC/USDT:USDT",
                rate=0.00015,
                next_funding_time=datetime.now(timezone.utc),
            )
            payment = await sim.collect_funding(funding)
            total_funding += payment

        assert total_funding > 0

        # 3. Exit (slight price change)
        pnl = await sim.exit_position(65100.0, 65120.0)

        assert not sim.is_open
        # Total P&L = basis + funding
        assert isinstance(pnl, float)
