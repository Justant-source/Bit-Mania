"""Order Manager — order lifecycle management for the Execution Engine.

Responsibilities:
  - Place limit / market orders via the exchange connector
  - Track order state: pending -> submitted -> partial -> filled / cancelled / rejected
  - Idempotency via request_id deduplication
  - Retry logic with configurable attempts and exponential backoff
  - Post-Only order support
  - Cancel and modify orders
"""

from __future__ import annotations

import asyncio
import json
import time
from enum import Enum
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.exchange import ExchangeConnector, exchange_factory
from shared.models.order import OrderRequest, OrderResult

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Order state machine
# ---------------------------------------------------------------------------


class OrderState(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


_TERMINAL_STATES = {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}

_VALID_TRANSITIONS: dict[OrderState, set[OrderState]] = {
    OrderState.PENDING: {OrderState.SUBMITTED, OrderState.REJECTED},
    OrderState.SUBMITTED: {
        OrderState.PARTIAL,
        OrderState.FILLED,
        OrderState.CANCELLED,
        OrderState.REJECTED,
    },
    OrderState.PARTIAL: {
        OrderState.PARTIAL,
        OrderState.FILLED,
        OrderState.CANCELLED,
    },
}

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 0.5  # seconds, multiplied by attempt number
IDEMPOTENCY_SET_MAX = 50_000
IDEMPOTENCY_SET_TRIM = 25_000


class OrderManager:
    """Manages the full lifecycle of exchange orders."""

    def __init__(
        self,
        *,
        exchange: str,
        api_key: str,
        api_secret: str,
        testnet: bool,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    ) -> None:
        self._exchange_id = exchange
        self._redis = redis
        self._db_pool = db_pool
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff

        self._connector: ExchangeConnector = exchange_factory(
            exchange,
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
        )

        # In-memory order state tracking
        self._order_states: dict[str, OrderState] = {}
        # Idempotency: set of already-processed request_ids
        self._processed_request_ids: set[str] = set()
        # Map request_id -> exchange order_id for active orders
        self._request_to_order: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Connect to the exchange and restore in-flight order state."""
        await self._connector.connect()
        await self._restore_inflight_orders()
        log.info("order_manager_initialized", exchange=self._exchange_id)

    async def shutdown(self) -> None:
        """Disconnect from the exchange."""
        await self._connector.disconnect()
        log.info("order_manager_shutdown")

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Place an order from a raw payload dict.

        This is the primary entry point called by ``ExecutionEngine``.
        Delegates to ``place_limit_order`` or ``place_market_order`` based
        on ``order_type``.
        """
        request_id = payload["request_id"]

        # Idempotency check
        if self._is_duplicate(request_id):
            log.warning("order_duplicate_rejected", request_id=request_id)
            return {
                "request_id": request_id,
                "order_id": self._request_to_order.get(request_id, ""),
                "status": "rejected",
                "filled_qty": 0.0,
                "filled_price": None,
                "fee": 0.0,
                "fee_currency": "USDT",
                "reason": "duplicate_request_id",
            }

        order = OrderRequest(**payload)
        self._mark_processed(request_id)
        self._transition(request_id, None, OrderState.PENDING)

        # Persist the pending order
        await self._persist_new_order(order)

        if order.order_type == "market":
            return await self.place_market_order(order)
        else:
            return await self.place_limit_order(order)

    async def place_limit_order(self, order: OrderRequest) -> dict[str, Any]:
        """Place a limit order with retry logic.

        Supports Post-Only mode via ``order.post_only``.
        """
        return await self._execute_with_retries(order)

    async def place_market_order(self, order: OrderRequest) -> dict[str, Any]:
        """Place a market order with retry logic."""
        return await self._execute_with_retries(order)

    # ------------------------------------------------------------------
    # Cancel / Modify
    # ------------------------------------------------------------------

    async def cancel_order(
        self,
        request_id: str,
        symbol: str,
    ) -> dict[str, Any]:
        """Cancel an active order by request_id."""
        order_id = self._request_to_order.get(request_id)
        if not order_id:
            log.warning("cancel_order_unknown_request", request_id=request_id)
            return {"request_id": request_id, "status": "rejected", "reason": "unknown_request_id"}

        current_state = self._order_states.get(request_id)
        if current_state in _TERMINAL_STATES:
            log.info("cancel_order_already_terminal", request_id=request_id, state=current_state)
            return {"request_id": request_id, "status": str(current_state), "reason": "already_terminal"}

        success = await self._connector.cancel_order(order_id, symbol)

        if success:
            self._transition(request_id, current_state, OrderState.CANCELLED)
            await self._update_order_status(request_id, OrderState.CANCELLED)
            log.info("order_cancelled", request_id=request_id, order_id=order_id)
            return {"request_id": request_id, "order_id": order_id, "status": "cancelled"}

        log.warning("cancel_order_failed", request_id=request_id, order_id=order_id)
        return {"request_id": request_id, "order_id": order_id, "status": "cancel_failed"}

    async def modify_order(
        self,
        request_id: str,
        symbol: str,
        new_price: float | None = None,
        new_quantity: float | None = None,
    ) -> dict[str, Any]:
        """Modify an active order by cancelling and re-placing.

        Exchange APIs rarely support true modify; we cancel + place.
        """
        order_id = self._request_to_order.get(request_id)
        if not order_id:
            return {"request_id": request_id, "status": "rejected", "reason": "unknown_request_id"}

        current_state = self._order_states.get(request_id)
        if current_state in _TERMINAL_STATES:
            return {"request_id": request_id, "status": str(current_state), "reason": "already_terminal"}

        # Cancel the existing order
        cancelled = await self._connector.cancel_order(order_id, symbol)
        if not cancelled:
            log.warning("modify_cancel_failed", request_id=request_id, order_id=order_id)
            return {"request_id": request_id, "status": "modify_failed", "reason": "cancel_step_failed"}

        self._transition(request_id, current_state, OrderState.CANCELLED)

        # Retrieve original order data from DB to reconstruct
        original = await self._fetch_order_from_db(request_id)
        if original is None:
            return {"request_id": request_id, "status": "modify_failed", "reason": "original_order_not_found"}

        # Build a new OrderRequest with modified fields
        import uuid

        new_request_id = uuid.uuid4().hex
        new_order = OrderRequest(
            strategy_id=original["strategy_id"] or "",
            exchange=original["exchange"],
            symbol=original["symbol"],
            side=original["side"],
            order_type=original["order_type"],
            quantity=new_quantity if new_quantity is not None else original["quantity"],
            price=new_price if new_price is not None else original.get("price"),
            post_only=original.get("post_only", True),
            reduce_only=original.get("reduce_only", False),
            request_id=new_request_id,
        )

        self._mark_processed(new_request_id)
        self._transition(new_request_id, None, OrderState.PENDING)
        await self._persist_new_order(new_order)

        result = await self._execute_with_retries(new_order)
        log.info(
            "order_modified",
            old_request_id=request_id,
            new_request_id=new_request_id,
            status=result.get("status"),
        )
        return result

    # ------------------------------------------------------------------
    # Internal: execution with retries
    # ------------------------------------------------------------------

    async def _execute_with_retries(self, order: OrderRequest) -> dict[str, Any]:
        """Submit order to exchange with retry logic and state tracking."""
        request_id = order.request_id
        last_error = ""

        for attempt in range(1, self._max_retries + 1):
            try:
                self._transition(
                    request_id,
                    self._order_states.get(request_id),
                    OrderState.SUBMITTED,
                )

                result: OrderResult = await self._connector.place_order(order)

                # Track the exchange order id
                if result.order_id:
                    self._request_to_order[request_id] = result.order_id

                # Map result status to our internal state
                new_state = self._map_result_status(result.status)
                self._transition(request_id, OrderState.SUBMITTED, new_state)

                result_dict = self._result_to_dict(result, order)
                await self._update_order_from_result(result_dict)

                log.info(
                    "order_executed",
                    request_id=request_id,
                    order_id=result.order_id,
                    status=result.status,
                    attempt=attempt,
                )
                return result_dict

            except Exception as exc:
                last_error = str(exc)
                log.warning(
                    "order_attempt_failed",
                    request_id=request_id,
                    attempt=attempt,
                    max_retries=self._max_retries,
                    error=last_error,
                )

                if attempt < self._max_retries:
                    backoff = self._retry_backoff * attempt
                    await asyncio.sleep(backoff)

        # All retries exhausted
        self._transition(
            request_id,
            self._order_states.get(request_id),
            OrderState.REJECTED,
        )
        rejection = {
            "request_id": request_id,
            "order_id": "",
            "status": "rejected",
            "filled_qty": 0.0,
            "filled_price": None,
            "fee": 0.0,
            "fee_currency": "USDT",
            "reason": f"max_retries_exhausted: {last_error}",
            "strategy_id": order.strategy_id,
        }
        await self._update_order_from_result(rejection)
        return rejection

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def _transition(
        self,
        request_id: str,
        from_state: OrderState | None,
        to_state: OrderState,
    ) -> None:
        """Validate and apply a state transition, logging every change."""
        if from_state is not None and from_state in _TERMINAL_STATES:
            log.warning(
                "order_state_already_terminal",
                request_id=request_id,
                current=str(from_state),
                requested=str(to_state),
            )
            return

        if from_state is not None:
            valid_targets = _VALID_TRANSITIONS.get(from_state, set())
            if to_state not in valid_targets:
                log.warning(
                    "order_invalid_transition",
                    request_id=request_id,
                    from_state=str(from_state),
                    to_state=str(to_state),
                )
                # Allow it but log the warning -- don't block execution

        self._order_states[request_id] = to_state
        log.info(
            "order_state_transition",
            request_id=request_id,
            from_state=str(from_state) if from_state else "none",
            to_state=str(to_state),
        )

    @staticmethod
    def _map_result_status(status: str) -> OrderState:
        """Map an ``OrderResult.status`` string to an internal ``OrderState``."""
        mapping = {
            "new": OrderState.SUBMITTED,
            "partially_filled": OrderState.PARTIAL,
            "filled": OrderState.FILLED,
            "cancelled": OrderState.CANCELLED,
            "rejected": OrderState.REJECTED,
            "expired": OrderState.CANCELLED,
        }
        return mapping.get(status, OrderState.SUBMITTED)

    # ------------------------------------------------------------------
    # Idempotency helpers
    # ------------------------------------------------------------------

    def _is_duplicate(self, request_id: str) -> bool:
        return request_id in self._processed_request_ids

    def _mark_processed(self, request_id: str) -> None:
        self._processed_request_ids.add(request_id)
        # Keep the set bounded
        if len(self._processed_request_ids) > IDEMPOTENCY_SET_MAX:
            keep = list(self._processed_request_ids)[-IDEMPOTENCY_SET_TRIM:]
            self._processed_request_ids = set(keep)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    async def _persist_new_order(self, order: OrderRequest) -> None:
        """Insert a new pending order into the database."""
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders
                        (request_id, exchange, symbol, side, order_type,
                         quantity, price, status, strategy_id, post_only, reduce_only)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, $10)
                    ON CONFLICT (request_id) DO NOTHING
                    """,
                    order.request_id,
                    order.exchange,
                    order.symbol,
                    order.side,
                    order.order_type,
                    order.quantity,
                    order.price,
                    order.strategy_id,
                    order.post_only,
                    order.reduce_only,
                )
        except Exception:
            log.exception("persist_new_order_error", request_id=order.request_id)

    async def _update_order_from_result(self, result: dict[str, Any]) -> None:
        """Update order row from an execution result dict."""
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE orders
                    SET order_id = $2, status = $3, filled_qty = $4,
                        filled_price = $5, fee = $6, updated_at = NOW()
                    WHERE request_id = $1
                    """,
                    result.get("request_id"),
                    result.get("order_id", ""),
                    result.get("status"),
                    result.get("filled_qty", 0),
                    result.get("filled_price"),
                    result.get("fee", 0),
                )
        except Exception:
            log.exception("update_order_result_error", request_id=result.get("request_id"))

    async def _update_order_status(self, request_id: str, state: OrderState) -> None:
        """Update only the status column."""
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE orders SET status = $2, updated_at = NOW() WHERE request_id = $1",
                    request_id,
                    state.value,
                )
        except Exception:
            log.exception("update_order_status_error", request_id=request_id)

    async def _fetch_order_from_db(self, request_id: str) -> dict[str, Any] | None:
        """Fetch an order record by request_id."""
        try:
            async with self._db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM orders WHERE request_id = $1",
                    request_id,
                )
                return dict(row) if row else None
        except Exception:
            log.exception("fetch_order_error", request_id=request_id)
            return None

    async def _restore_inflight_orders(self) -> None:
        """On startup, load orders that were in non-terminal states.

        This prevents re-processing and lets us resume tracking.
        """
        try:
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT request_id, order_id, status
                    FROM orders
                    WHERE exchange = $1
                      AND status NOT IN ('filled', 'cancelled', 'rejected', 'expired')
                    ORDER BY created_at DESC
                    LIMIT 1000
                    """,
                    self._exchange_id,
                )
            for row in rows:
                rid = row["request_id"]
                self._mark_processed(rid)
                if row["order_id"]:
                    self._request_to_order[rid] = row["order_id"]
                state_str = row["status"]
                try:
                    self._order_states[rid] = OrderState(state_str)
                except ValueError:
                    self._order_states[rid] = OrderState.SUBMITTED

            log.info("inflight_orders_restored", count=len(rows))
        except Exception:
            log.exception("restore_inflight_orders_error")

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _result_to_dict(result: OrderResult, order: OrderRequest) -> dict[str, Any]:
        """Convert an ``OrderResult`` model to a plain dict for publishing."""
        return {
            "request_id": result.request_id,
            "order_id": result.order_id,
            "status": result.status,
            "filled_qty": result.filled_qty,
            "filled_price": result.filled_price,
            "fee": result.fee,
            "fee_currency": result.fee_currency,
            "strategy_id": order.strategy_id,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
        }
