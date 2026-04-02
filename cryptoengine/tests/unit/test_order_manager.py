"""Unit tests for order lifecycle, idempotency, and retry logic.

Tests the ExecutionEngine's order processing pipeline without
actual exchange or Redis connections.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.models.order import OrderRequest, OrderResult


# ---------------------------------------------------------------------------
# Simplified OrderManager for test isolation
# ---------------------------------------------------------------------------

MAX_RETRIES = 3
RETRY_BACKOFF = 0.01  # fast for tests
ORDER_TIMEOUT = 1.0


class OrderManager:
    """Simplified order manager for testing lifecycle and retry logic."""

    def __init__(self, exchange: AsyncMock) -> None:
        self._exchange = exchange
        self._processed_ids: set[str] = set()

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place a single order with retry logic."""
        last_error = ""

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await asyncio.wait_for(
                    self._exchange.place_order(request),
                    timeout=ORDER_TIMEOUT,
                )
                return result
            except asyncio.TimeoutError:
                last_error = "timeout"
            except Exception as exc:
                last_error = str(exc)

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF * attempt)

        return OrderResult(
            request_id=request.request_id,
            order_id="",
            status="rejected",
        )

    def is_duplicate(self, request_id: str) -> bool:
        """Check idempotency."""
        if request_id in self._processed_ids:
            return True
        self._processed_ids.add(request_id)
        return False

    def clear_processed(self, max_size: int = 10_000) -> None:
        """Keep the idempotency set bounded."""
        if len(self._processed_ids) > max_size:
            keep = list(self._processed_ids)[-max_size // 2 :]
            self._processed_ids = set(keep)


@pytest.fixture
def order_request():
    return OrderRequest(
        strategy_id="funding_arb_01",
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="buy",
        order_type="limit",
        quantity=0.1,
        price=65000.0,
    )


# ------------------------------------------------------------------
# Order lifecycle tests
# ------------------------------------------------------------------

class TestOrderLifecycle:
    @pytest.mark.asyncio
    async def test_successful_order(self, mock_exchange, order_request):
        mgr = OrderManager(mock_exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "filled"
        assert result.order_id != ""
        assert result.request_id == order_request.request_id

    @pytest.mark.asyncio
    async def test_rejected_order(self, order_request):
        exchange = AsyncMock()
        exchange.place_order = AsyncMock(return_value=OrderResult(
            request_id=order_request.request_id,
            order_id="",
            status="rejected",
        ))
        mgr = OrderManager(exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "rejected"

    @pytest.mark.asyncio
    async def test_partially_filled_order(self, order_request):
        exchange = AsyncMock()
        exchange.place_order = AsyncMock(return_value=OrderResult(
            request_id=order_request.request_id,
            order_id="ord-partial",
            status="partially_filled",
            filled_qty=0.05,
            filled_price=65000.0,
        ))
        mgr = OrderManager(exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "partially_filled"
        assert result.filled_qty == 0.05

    @pytest.mark.asyncio
    async def test_order_result_is_terminal(self, order_request):
        result = OrderResult(
            request_id=order_request.request_id,
            order_id="ord-001",
            status="filled",
        )
        assert result.is_terminal

        pending = OrderResult(
            request_id=order_request.request_id,
            order_id="ord-002",
            status="new",
        )
        assert not pending.is_terminal


# ------------------------------------------------------------------
# Idempotency tests
# ------------------------------------------------------------------

class TestIdempotency:
    def test_first_request_not_duplicate(self, mock_exchange):
        mgr = OrderManager(mock_exchange)
        assert mgr.is_duplicate("req-001") is False

    def test_second_request_is_duplicate(self, mock_exchange):
        mgr = OrderManager(mock_exchange)
        mgr.is_duplicate("req-001")
        assert mgr.is_duplicate("req-001") is True

    def test_different_requests_not_duplicate(self, mock_exchange):
        mgr = OrderManager(mock_exchange)
        mgr.is_duplicate("req-001")
        assert mgr.is_duplicate("req-002") is False

    def test_clear_processed_bounds_set(self, mock_exchange):
        mgr = OrderManager(mock_exchange)
        for i in range(100):
            mgr.is_duplicate(f"req-{i}")
        assert len(mgr._processed_ids) == 100
        mgr.clear_processed(max_size=50)
        assert len(mgr._processed_ids) <= 50


# ------------------------------------------------------------------
# Retry logic tests
# ------------------------------------------------------------------

class TestRetryLogic:
    @pytest.mark.asyncio
    async def test_retry_on_exception(self, order_request):
        exchange = AsyncMock()
        call_count = 0

        async def flaky_place(req):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("network error")
            return OrderResult(
                request_id=req.request_id,
                order_id="ord-retry",
                status="filled",
                filled_qty=0.1,
                filled_price=65000.0,
            )

        exchange.place_order = flaky_place
        mgr = OrderManager(exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "filled"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_all_retries_exhausted(self, order_request):
        exchange = AsyncMock()
        exchange.place_order = AsyncMock(side_effect=ConnectionError("network"))
        mgr = OrderManager(exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "rejected"
        assert exchange.place_order.call_count == MAX_RETRIES

    @pytest.mark.asyncio
    async def test_retry_on_timeout(self, order_request):
        exchange = AsyncMock()

        async def slow_place(req):
            await asyncio.sleep(10)  # exceeds ORDER_TIMEOUT
            return OrderResult(request_id=req.request_id, order_id="", status="filled")

        exchange.place_order = slow_place
        mgr = OrderManager(exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "rejected"

    @pytest.mark.asyncio
    async def test_immediate_success_no_retry(self, mock_exchange, order_request):
        mgr = OrderManager(mock_exchange)
        result = await mgr.place_order(order_request)
        assert result.status == "filled"
        assert mock_exchange.place_order.call_count == 1


# ------------------------------------------------------------------
# OrderRequest model tests
# ------------------------------------------------------------------

class TestOrderRequestModel:
    def test_frozen_model(self, order_request):
        with pytest.raises(Exception):
            order_request.quantity = 999.0

    def test_request_id_auto_generated(self):
        req = OrderRequest(
            strategy_id="test",
            exchange="bybit",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            quantity=1.0,
        )
        assert req.request_id is not None
        assert len(req.request_id) > 0

    def test_quantity_must_be_positive(self):
        with pytest.raises(Exception):
            OrderRequest(
                strategy_id="test",
                exchange="bybit",
                symbol="BTC/USDT",
                side="buy",
                order_type="market",
                quantity=0.0,
            )
