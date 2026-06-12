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
# 재페그 worst-case(20×10s) + 시도별 API 지연 + 시장가 폴백 여유.
# 이전 300s는 네트워크 저하 시 재페그 도중 잘릴 수 있었다.
ORDER_TIMEOUT = 420.0


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
                    await self._publish_rejection(request_id, reason, payload=payload)
                    return
            except Exception:
                log.exception(ORDER_SAFETY_FAILED, message="safety check error", request_id=request_id)
                await self._publish_rejection(request_id, "safety_check_internal_error", payload=payload)
                return

            # --- Execute (1회) ---
            # 재시도는 OrderManager 내부(_execute_with_retries / 재페그)에서 수행한다.
            # 엔진 레벨 재실행은 request_id idempotency에 막혀 duplicate 거부만
            # 반환하므로 의미가 없고, 타임아웃 시 거래소에 남은 주문만 고아화시켰다.
            try:
                result: dict[str, Any] = await asyncio.wait_for(
                    self._order_manager.place_order(payload),
                    timeout=ORDER_TIMEOUT,
                )
                self._safety.record_api_response()
                self._safety.record_api_call()
            except asyncio.TimeoutError:
                log.error(ORDER_TIMEOUT, message="주문 실행 시간 초과 — 미체결 주문 정리 후 거부",
                          request_id=request_id, timeout_s=ORDER_TIMEOUT)
                try:
                    await self._order_manager.cancel_order(request_id, payload.get("symbol", ""))
                except Exception:
                    log.exception(ORDER_CANCELLED, message="타임아웃 주문 취소 실패 — 수동 확인 필요",
                                  request_id=request_id)
                await self._publish_rejection(
                    request_id, "order_timeout: 실행 시간 초과, 미체결 주문 취소됨", payload=payload
                )
                return
            except Exception as exc:
                log.exception(ORDER_RETRY, message="order execution error", request_id=request_id)
                await self._publish_rejection(request_id, f"execution_error: {exc}", payload=payload)
                return

            # --- Publish result ---
            await self._publish_result(result)

            # --- 미체결 종결 알림 (ERROR → Telegram) ---
            final_status = result.get("status")
            if final_status == "rejected":
                log.error(
                    ORDER_REJECTED,
                    message="주문 거부 — 전략 신호 미체결",
                    request_id=request_id,
                    strategy_id=result.get("strategy_id", payload.get("strategy_id", "")),
                    side=payload.get("side"),
                    reduce_only=payload.get("reduce_only", False),
                    reason=result.get("reason", ""),
                )
            elif final_status == "partially_filled":
                log.error(
                    ORDER_REJECTED,
                    message="부분 체결 후 잔량 미체결 종결 — 수동 확인 필요",
                    request_id=request_id,
                    strategy_id=result.get("strategy_id", payload.get("strategy_id", "")),
                    filled_qty=result.get("filled_qty"),
                    requested_qty=payload.get("quantity"),
                    reason=result.get("reason", ""),
                )

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

    async def _publish_rejection(
        self,
        request_id: str,
        reason: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Publish a rejected OrderResult.

        strategy_id를 포함해 전략별 채널(order:result:{strategy_id})로도 발행하고
        ERROR 레벨로 기록한다 (→ Telegram 알림). 2026-05-27 사고에서는 거부가
        WARNING이라 알림이 없었고 전략도 거부 사실을 몰랐다.
        """
        p = payload or {}
        strategy_id = p.get("strategy_id", "")
        result = {
            "request_id": request_id,
            "order_id": "",
            "status": "rejected",
            "filled_qty": 0.0,
            "filled_price": None,
            "fee": 0.0,
            "fee_currency": "USDT",
            "reason": reason,
            "strategy_id": strategy_id,
            "symbol": p.get("symbol", ""),
            "side": p.get("side", ""),
        }
        await self.redis.publish("order:result", json.dumps(result))
        if strategy_id:
            await self.redis.publish(f"order:result:{strategy_id}", json.dumps(result))

        log.error(
            ORDER_REJECTED,
            message="주문 거부 — 전략 신호 미체결",
            request_id=request_id,
            strategy_id=strategy_id,
            symbol=p.get("symbol", ""),
            side=p.get("side", ""),
            reduce_only=p.get("reduce_only", False),
            reason=reason,
        )

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO orders (request_id, exchange, symbol, side, order_type, quantity, status, strategy_id)
                    VALUES ($1, $2, $3, $4, $5, $6, 'rejected', $7)
                    ON CONFLICT (request_id) DO UPDATE SET status = 'rejected', updated_at = NOW()
                    """,
                    request_id,
                    self.exchange,
                    p.get("symbol", ""),
                    p.get("side", ""),
                    p.get("order_type", ""),
                    float(p.get("quantity", 0) or 0),
                    strategy_id or None,
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
                # BybitConnector.place_order already attaches stopLoss inline when
                # order.stop_loss is set — skip StopLossManager to avoid double SL.
                if payload.get("stop_loss") is not None:
                    log.debug(
                        ORDER_FILLED,
                        message="inline SL already attached, skipping StopLossManager",
                        symbol=symbol,
                    )
                    return
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
