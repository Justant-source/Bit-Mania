"""Execution Engine — receives order requests, validates, executes, publishes results.

Main loop:
  1. Subscribe to ``order:request`` Redis channel
  2. Deserialise ``OrderRequest``
  3. Run safety checks
  4. Dispatch to ``OrderManager`` for execution
  5. Publish ``OrderResult`` to ``order:result`` channel
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from order_manager import OrderManager
from position_tracker import PositionTracker
from safety import SafetyGuard
from stoploss_manager import StopLossManager
from shared.log_events import *

log = structlog.get_logger(__name__)

MAX_CONCURRENT_ORDERS = 5
ORDER_TIMEOUT = 30.0  # seconds per order
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0


class ExecutionEngine:
    """Core execution loop — bridges strategy order intents to exchange fills."""

    def __init__(
        self,
        *,
        exchange: str,
        api_key: str,
        api_secret: str,
        testnet: bool,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
        position_tracker: PositionTracker,
        stop_loss_pct: float = 0.02,
    ) -> None:
        self.exchange = exchange
        self.redis = redis
        self.db_pool = db_pool
        self.position_tracker = position_tracker

        self._order_manager = OrderManager(
            exchange=exchange,
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
            redis=redis,
            db_pool=db_pool,
        )
        self._safety = SafetyGuard(
            redis=redis,
            db_pool=db_pool,
            exchange=exchange,
        )
        # StopLossManager is initialised after OrderManager so it can share
        # the same ExchangeConnector instance via _order_manager._connector.
        self._stoploss_manager: StopLossManager | None = None
        self._stop_loss_pct = stop_loss_pct

        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_ORDERS)
        self._processed_ids: set[str] = set()  # idempotency guard
        self._active_tasks: dict[str, asyncio.Task] = {}
        self._last_network_check: float = time.monotonic()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Main event loop — subscribe and process order requests."""
        log.info(SERVICE_STARTED, message="execution engine starting")

        await self._order_manager.initialize()

        # Wire up StopLossManager now that the connector is initialised
        self._stoploss_manager = StopLossManager(
            connector=self._order_manager._connector,
            redis=self.redis,
            exchange_id=self.exchange,
            stop_loss_pct=self._stop_loss_pct,
        )

        # Recover stop-loss orders for any open positions from before restart
        open_positions = [
            {
                "symbol": pos.symbol,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "size": pos.size,
            }
            for pos in self.position_tracker.get_all_positions().values()
        ]
        if open_positions:
            await self._stoploss_manager.recover_stop_losses(open_positions)

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("order:request")

        try:
            while not shutdown.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg is None:
                    # Housekeeping: clean finished tasks
                    self._cleanup_tasks()
                    continue

                try:
                    payload = json.loads(msg["data"])
                except (json.JSONDecodeError, TypeError):
                    log.warning(ORDER_RECEIVED, message="invalid order message", raw=str(msg.get("data", ""))[:200])
                    continue

                request_id = payload.get("request_id")
                if not request_id:
                    log.warning(ORDER_RECEIVED, message="order missing request_id", payload=payload)
                    continue

                # Idempotency
                if request_id in self._processed_ids:
                    log.debug(ORDER_DUPLICATE_SKIPPED, message="order duplicate skipped", request_id=request_id)
                    continue

                self._processed_ids.add(request_id)
                # Keep set bounded
                if len(self._processed_ids) > 10_000:
                    self._processed_ids = set(list(self._processed_ids)[-5_000:])

                # Launch order processing with concurrency limit
                task = asyncio.create_task(
                    self._process_order(payload),
                    name=f"order_{request_id}",
                )
                self._active_tasks[request_id] = task

        except asyncio.CancelledError:
            pass
        finally:
            # Cancel any in-flight orders
            for task in self._active_tasks.values():
                task.cancel()
            await asyncio.gather(*self._active_tasks.values(), return_exceptions=True)
            await pubsub.unsubscribe("order:request")
            await pubsub.aclose()
            log.info(SERVICE_STOPPED, message="execution engine stopped")

    # ------------------------------------------------------------------
    # Order processing pipeline
    # ------------------------------------------------------------------

    async def _process_order(self, payload: dict[str, Any]) -> None:
        """Full order lifecycle: validate -> execute -> publish result."""
        request_id = payload["request_id"]

        async with self._semaphore:
            log.info(ORDER_RECEIVED, message="order processing start", request_id=request_id, side=payload.get("side"), qty=payload.get("quantity"))

            # --- Safety checks ---
            try:
                safe, reason = await self._safety.check_order(payload)
                if not safe:
                    await self._publish_rejection(request_id, reason)
                    return
            except Exception:
                log.exception(ORDER_SAFETY_FAILED, message="safety check error", request_id=request_id)
                await self._publish_rejection(request_id, "safety_check_internal_error")
                return

            # --- Execute with retries ---
            result: dict[str, Any] | None = None
            last_error: str = ""

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    result = await asyncio.wait_for(
                        self._order_manager.place_order(payload),
                        timeout=ORDER_TIMEOUT,
                    )
                    self._safety.record_api_response()
                    self._safety.record_api_call()
                    break
                except asyncio.TimeoutError:
                    last_error = "order_timeout"
                    log.warning(ORDER_TIMEOUT, message="order timeout", request_id=request_id, attempt=attempt)
                except Exception as exc:
                    last_error = str(exc)
                    log.warning(ORDER_RETRY, message="order attempt failed", request_id=request_id, attempt=attempt, error=last_error)

                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_BACKOFF * attempt)

            if result is None:
                await self._publish_rejection(request_id, f"execution_failed_after_{MAX_RETRIES}_retries: {last_error}")
                return

            # --- Publish result ---
            await self._publish_result(result)

            # --- Update position cache ---
            if result.get("status") in ("new", "partially_filled", "filled"):
                await self.position_tracker.on_order_fill(result)

            # --- Stop-loss management ---
            await self._handle_stoploss(payload, result)

            log.info(
                ORDER_FILLED,
                message="order processing complete",
                request_id=request_id,
                order_id=result.get("order_id"),
                status=result.get("status"),
            )

    # ------------------------------------------------------------------
    # Result publishing
    # ------------------------------------------------------------------

    async def _publish_result(self, result: dict[str, Any]) -> None:
        """Publish OrderResult to Redis and persist to DB."""
        await self.redis.publish("order:result", json.dumps(result))

        # Also publish to strategy-specific channel
        strategy_id = result.get("strategy_id")
        if strategy_id:
            await self.redis.publish(f"order:result:{strategy_id}", json.dumps(result))

        # Persist
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE orders
                    SET order_id = $2, status = $3, filled_qty = $4,
                        filled_price = $5, fee = $6, updated_at = NOW()
                    WHERE request_id = $1
                    """,
                    result.get("request_id"),
                    result.get("order_id"),
                    result.get("status"),
                    result.get("filled_qty", 0),
                    result.get("filled_price"),
                    result.get("fee", 0),
                )
        except Exception:
            log.exception(ORDER_REJECTED, message="result persist error", request_id=result.get("request_id"))

    async def _publish_rejection(self, request_id: str, reason: str) -> None:
        """Publish a rejected OrderResult."""
        result = {
            "request_id": request_id,
            "order_id": "",
            "status": "rejected",
            "filled_qty": 0.0,
            "filled_price": None,
            "fee": 0.0,
            "fee_currency": "USDT",
            "reason": reason,
        }
        await self.redis.publish("order:result", json.dumps(result))
        log.warning(ORDER_REJECTED, message="order rejected", request_id=request_id, reason=reason)

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (request_id, exchange, symbol, side, order_type, quantity, status)
                    VALUES ($1, $2, '', '', '', 0, 'rejected')
                    ON CONFLICT (request_id) DO UPDATE SET status = 'rejected', updated_at = NOW()
                    """,
                    request_id,
                    self.exchange,
                )
        except Exception:
            log.exception(ORDER_REJECTED, message="rejection persist error", request_id=request_id)

    # ------------------------------------------------------------------
    # Stop-loss lifecycle
    # ------------------------------------------------------------------

    async def _handle_stoploss(
        self,
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        """Place or cancel the exchange-native stop-loss based on order outcome.

        Rules:
        - Entry order (reduce_only=False) that filled → place a new SL
        - Exit order  (reduce_only=True)  that filled → cancel existing SL
        - Any non-filled status → no SL action
        """
        if self._stoploss_manager is None:
            return

        status = result.get("status", "")
        if status != "filled":
            return

        symbol = payload.get("symbol") or result.get("symbol", "")
        reduce_only = payload.get("reduce_only", False)

        try:
            if reduce_only:
                # Position is being closed — remove the stop-loss
                await self._stoploss_manager.cancel_stop_loss(symbol)
            else:
                # New position (or size increase) — attach a stop-loss
                filled_price = result.get("filled_price")
                filled_qty = result.get("filled_qty", 0.0)
                if not filled_price or not filled_qty:
                    log.warning(
                        ORDER_FILLED,
                        message="cannot place SL: missing filled_price or filled_qty",
                        request_id=payload.get("request_id"),
                        symbol=symbol,
                    )
                    return

                # Determine position side from order side:
                # buy order → long position; sell order → short position
                order_side = payload.get("side", "buy")
                position_side = "long" if order_side == "buy" else "short"

                await self._stoploss_manager.place_stop_loss(
                    symbol=symbol,
                    side=position_side,
                    entry_price=float(filled_price),
                    quantity=float(filled_qty),
                )
        except Exception:
            log.exception(
                ORDER_REJECTED,
                message="stop-loss management error (non-fatal)",
                request_id=payload.get("request_id"),
                symbol=symbol,
            )

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _cleanup_tasks(self) -> None:
        """Remove finished tasks from the active set."""
        done = [rid for rid, task in self._active_tasks.items() if task.done()]
        for rid in done:
            task = self._active_tasks.pop(rid)
            if task.exception() and not isinstance(task.exception(), asyncio.CancelledError):
                log.error(ORDER_REJECTED, message="order task exception", request_id=rid, error=str(task.exception()))
