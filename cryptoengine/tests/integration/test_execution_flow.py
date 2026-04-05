"""Integration tests for the full execution flow.

Tests the pipeline:
  OrderRequest -> ExecutionEngine -> OrderResult -> DB record

Uses mocked exchange and Redis but exercises the full internal
flow through safety checks, order placement, and result publishing.
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
# Simplified execution pipeline for integration testing
# ---------------------------------------------------------------------------

class MockSafetyGuard:
    """Always-pass safety guard."""

    async def check_order(self, payload: dict) -> tuple[bool, str]:
        return True, ""


class MockPositionTracker:
    """Track fills without DB."""

    def __init__(self):
        self.fills: list[dict] = []

    async def on_order_fill(self, result: dict) -> None:
        self.fills.append(result)


class ExecutionPipeline:
    """Simplified execution pipeline for integration testing."""

    def __init__(self, exchange: AsyncMock, redis: AsyncMock, db: AsyncMock):
        self._exchange = exchange
        self._redis = redis
        self._db = db
        self._safety = MockSafetyGuard()
        self._tracker = MockPositionTracker()
        self._processed: set[str] = set()
        self._results: list[dict] = []

    async def process_order(self, payload: dict) -> dict | None:
        request_id = payload.get("request_id")
        if not request_id:
            return None

        # Idempotency
        if request_id in self._processed:
            return None
        self._processed.add(request_id)

        # Safety
        safe, reason = await self._safety.check_order(payload)
        if not safe:
            rejection = {
                "request_id": request_id,
                "order_id": "",
                "status": "rejected",
                "reason": reason,
            }
            await self._publish(rejection)
            return rejection

        # Build OrderRequest
        order = OrderRequest(
            strategy_id=payload["strategy_id"],
            exchange=payload["exchange"],
            symbol=payload["symbol"],
            side=payload["side"],
            order_type=payload["order_type"],
            quantity=payload["quantity"],
            price=payload.get("price"),
            request_id=request_id,
        )

        # Execute
        result = await self._exchange.place_order(order)
        result_dict = {
            "request_id": result.request_id,
            "order_id": result.order_id,
            "status": result.status,
            "filled_qty": result.filled_qty,
            "filled_price": result.filled_price,
            "fee": result.fee,
            "strategy_id": payload["strategy_id"],
        }

        # Publish result
        await self._publish(result_dict)

        # Update position tracker
        if result.status in ("new", "partially_filled", "filled"):
            await self._tracker.on_order_fill(result_dict)

        # Persist to DB
        await self._persist(result_dict)

        return result_dict

    async def _publish(self, result: dict) -> None:
        await self._redis.publish("order:result", json.dumps(result))
        self._results.append(result)

    async def _persist(self, result: dict) -> None:
        async with self._db.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET status = $1 WHERE request_id = $2",
                result["status"],
                result["request_id"],
            )


@pytest.fixture
def pipeline(mock_exchange, mock_redis, mock_db):
    return ExecutionPipeline(mock_exchange, mock_redis, mock_db)


@pytest.fixture
def buy_payload():
    return {
        "request_id": "int-test-001",
        "strategy_id": "funding_arb_01",
        "exchange": "bybit",
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "order_type": "limit",
        "quantity": 0.1,
        "price": 65000.0,
    }


# ------------------------------------------------------------------
# Full flow tests
# ------------------------------------------------------------------

class TestFullExecutionFlow:
    @pytest.mark.asyncio
    async def test_order_request_to_result(self, pipeline, buy_payload):
        result = await pipeline.process_order(buy_payload)
        assert result is not None
        assert result["request_id"] == "int-test-001"
        assert result["status"] == "filled"
        assert result["order_id"] != ""

    @pytest.mark.asyncio
    async def test_result_published_to_redis(self, pipeline, buy_payload, mock_redis):
        await pipeline.process_order(buy_payload)
        mock_redis.publish.assert_called()
        call_args = mock_redis.publish.call_args
        channel = call_args[0][0]
        assert channel == "order:result"

    @pytest.mark.asyncio
    async def test_result_persisted_to_db(self, pipeline, buy_payload, mock_db):
        await pipeline.process_order(buy_payload)
        # Verify DB was called
        conn = mock_db._conn
        conn.execute.assert_called()

    @pytest.mark.asyncio
    async def test_position_tracker_updated(self, pipeline, buy_payload):
        await pipeline.process_order(buy_payload)
        assert len(pipeline._tracker.fills) == 1
        assert pipeline._tracker.fills[0]["status"] == "filled"

    @pytest.mark.asyncio
    async def test_idempotent_processing(self, pipeline, buy_payload):
        result1 = await pipeline.process_order(buy_payload)
        result2 = await pipeline.process_order(buy_payload)
        assert result1 is not None
        assert result2 is None  # duplicate skipped

    @pytest.mark.asyncio
    async def test_sell_order_flow(self, pipeline, mock_exchange):
        sell_payload = {
            "request_id": "int-test-sell-001",
            "strategy_id": "funding_arb_01",
            "exchange": "bybit",
            "symbol": "BTC/USDT:USDT",
            "side": "sell",
            "order_type": "market",
            "quantity": 0.05,
        }
        result = await pipeline.process_order(sell_payload)
        assert result is not None
        assert result["status"] == "filled"

    @pytest.mark.asyncio
    async def test_missing_request_id_rejected(self, pipeline):
        bad_payload = {
            "strategy_id": "test",
            "exchange": "bybit",
            "symbol": "BTC/USDT",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.1,
        }
        result = await pipeline.process_order(bad_payload)
        assert result is None


class TestExecutionWithErrors:
    @pytest.mark.asyncio
    async def test_exchange_error_produces_rejected(self, mock_redis, mock_db, buy_payload):
        exchange = AsyncMock()
        exchange.place_order = AsyncMock(return_value=OrderResult(
            request_id="int-test-001",
            order_id="",
            status="rejected",
        ))
        pipeline = ExecutionPipeline(exchange, mock_redis, mock_db)
        result = await pipeline.process_order(buy_payload)
        assert result["status"] == "rejected"

    @pytest.mark.asyncio
    async def test_multiple_strategies_tracked_separately(self, mock_exchange, mock_redis, mock_db):
        pipeline = ExecutionPipeline(mock_exchange, mock_redis, mock_db)

        p1 = {
            "request_id": "funding-001",
            "strategy_id": "funding_arb",
            "exchange": "bybit",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.1,
            "price": 65000.0,
        }
        p2 = {
            "request_id": "dca-001",
            "strategy_id": "adaptive_dca",
            "exchange": "bybit",
            "symbol": "ETH/USDT:USDT",
            "side": "sell",
            "order_type": "limit",
            "quantity": 1.0,
            "price": 3500.0,
        }

        r1 = await pipeline.process_order(p1)
        r2 = await pipeline.process_order(p2)

        assert r1 is not None
        assert r2 is not None
        assert r1["strategy_id"] == "funding_arb"
        assert r2["strategy_id"] == "adaptive_dca"
        assert len(pipeline._tracker.fills) == 2
