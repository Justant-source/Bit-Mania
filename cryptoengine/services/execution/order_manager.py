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
from shared.exchange.bybit import PostOnlyRejected
from shared.models.order import OrderRequest, OrderResult
from shared.log_events import *

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

# Post-only 재페그 루프 파라미터 (테스트에서 패치 가능하도록 모듈 레벨)
REPEG_MAX_ATTEMPTS = 20
REPEG_INTERVAL_S = 10.0
REPEG_POLL_S = 0.5

# BTC/USDT:USDT 기본값 — 거래소 메타 조회 실패 시 폴백
FALLBACK_MIN_QTY = 0.001
FALLBACK_QTY_STEP = 0.001


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
        # Symbol -> (min_qty, qty_step) — 재페그 잔량 계산용
        self._qty_limits_cache: dict[str, tuple[float, float]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Connect to the exchange and restore in-flight order state."""
        await self._connector.connect()
        await self._restore_inflight_orders()
        log.info(SERVICE_STARTED, message="order manager initialized", exchange=self._exchange_id)

    async def shutdown(self) -> None:
        """Disconnect from the exchange."""
        await self._connector.disconnect()
        log.info(SERVICE_STOPPED, message="order manager shutdown")

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
            log.warning(ORDER_DUPLICATE_SKIPPED, message="order duplicate rejected", request_id=request_id)
            return {
                "request_id": request_id,
                "order_id": self._request_to_order.get(request_id, ""),
                "status": "rejected",
                "filled_qty": 0.0,
                "filled_price": None,
                "fee": 0.0,
                "fee_currency": "USDT",
                "reason": "duplicate_request_id",
                "strategy_id": payload.get("strategy_id", ""),
                "symbol": payload.get("symbol", ""),
                "side": payload.get("side", ""),
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
        """Place a limit order.

        When post_only=True, uses the 10s×20 re-peg loop with market fallback.
        Otherwise falls back to the standard retry path.
        """
        if order.post_only:
            return await self._place_with_repeg(order)
        return await self._execute_with_retries(order)

    async def place_market_order(self, order: OrderRequest) -> dict[str, Any]:
        """Place a market order with retry logic."""
        return await self._execute_with_retries(order)

    async def _fetch_peg_price(self, symbol: str, side: str) -> float:
        """Return best bid (buy) or best ask (sell) from the orderbook."""
        ob = await self._connector.get_orderbook(symbol, limit=1)
        if side == "buy":
            return ob.bids[0].price if ob.bids else 0.0
        return ob.asks[0].price if ob.asks else 0.0

    async def _place_with_repeg(self, order: OrderRequest) -> dict[str, Any]:
        """Post-only limit: 10s × 20 re-peg attempts, then market-order fallback.

        부분체결 안전성: 재페그로 취소한 이전 시도의 체결량을 누적 추적하고
        잔량만 재발주한다 (전량 재발주는 과체결 → 의도 초과 레버리지 위험).
        엔진 타임아웃 등으로 태스크가 취소되면 거래소에 떠 있는 미체결 주문을
        정리한 뒤 취소를 전파한다 (고아 주문 방지).
        """
        request_id = order.request_id
        last_order_id = ""
        # 취소된 이전 시도들의 누적 체결 (현재 활성 주문 제외)
        cum_qty = 0.0
        cum_notional = 0.0
        cum_fee = 0.0
        min_qty, qty_step = await self._get_qty_limits(order.symbol)

        try:
            for attempt in range(1, REPEG_MAX_ATTEMPTS + 1):
                # Cancel previous unfilled limit & harvest its partial fills
                if last_order_id:
                    harvested = await self._harvest_order(last_order_id, order.symbol, request_id)
                    last_order_id = ""
                    if harvested is not None and harvested.filled_qty > 0:
                        cum_qty += harvested.filled_qty
                        cum_notional += harvested.filled_qty * float(harvested.filled_price or 0.0)
                        cum_fee += harvested.fee
                        log.info(LIMIT_REPEG_ATTEMPT, message="repeg partial fill harvested",
                                 request_id=request_id, attempt=attempt,
                                 harvested_qty=harvested.filled_qty, cum_qty=cum_qty)

                # 잔량 계산 — 누적 체결분을 빼고 스텝 단위로 내림
                remaining = self._floor_to_step(order.quantity - cum_qty, qty_step)
                if remaining < min_qty:
                    return await self._finalize_combined(
                        order, cum_qty, cum_notional, cum_fee, attempt=attempt
                    )

                # Fresh best-bid/ask peg
                try:
                    peg_price = await self._fetch_peg_price(order.symbol, order.side)
                    peg_price = self._connector.price_to_precision(order.symbol, peg_price)
                except Exception as exc:
                    log.warning(LIMIT_REPEG_ATTEMPT, message="peg price fetch failed",
                                request_id=request_id, attempt=attempt, error=str(exc)[:200])
                    await asyncio.sleep(1.0)
                    continue

                attempt_order = order.model_copy(update={"price": peg_price, "quantity": remaining})
                log.info(LIMIT_REPEG_ATTEMPT, message="limit repeg attempt",
                         request_id=request_id, attempt=attempt, price=peg_price,
                         quantity=remaining, cum_filled=cum_qty)

                # Submit
                try:
                    current_state = self._order_states.get(request_id)
                    if current_state != OrderState.SUBMITTED:
                        self._transition(request_id, current_state, OrderState.SUBMITTED)
                    result: OrderResult = await self._connector.place_order(attempt_order)
                except PostOnlyRejected:
                    log.info(POSTONLY_REJECTED, message="post-only rejected, repeg immediately",
                             request_id=request_id, attempt=attempt, price=peg_price)
                    continue  # immediate retry, no 10s wait
                except Exception as exc:
                    exc_type = type(exc).__name__
                    if "InsufficientFunds" in exc_type:
                        log.error(ORDER_REJECTED, message="repeg: InsufficientFunds — aborting loop",
                                  request_id=request_id, attempt=attempt, error=str(exc)[:300])
                        break
                    log.warning(ORDER_RETRY, message="repeg attempt exception",
                                request_id=request_id, attempt=attempt, error=str(exc)[:200])
                    await asyncio.sleep(1.0)
                    continue

                if result.order_id:
                    last_order_id = result.order_id
                    self._request_to_order[request_id] = result.order_id
                    # Update DB with new order_id and peg price
                    await self._update_order_from_result({
                        "request_id": request_id,
                        "order_id": result.order_id,
                        "status": "submitted",
                        "filled_qty": cum_qty,
                        "filled_price": (cum_notional / cum_qty) if cum_qty > 0 else None,
                        "fee": cum_fee,
                    })

                # Already filled on placement (rare for post-only)
                if result.status == "filled":
                    cum_qty += result.filled_qty or remaining
                    cum_notional += (result.filled_qty or remaining) * float(result.filled_price or peg_price)
                    cum_fee += result.fee
                    return await self._finalize_combined(
                        order, cum_qty, cum_notional, cum_fee,
                        order_id=last_order_id, attempt=attempt,
                    )

                # Poll for fill during REPEG_INTERVAL_S
                poll_start = time.monotonic()
                while time.monotonic() - poll_start < REPEG_INTERVAL_S:
                    await asyncio.sleep(REPEG_POLL_S)
                    if not last_order_id:
                        break
                    try:
                        polled = await self._connector.fetch_order_result(
                            last_order_id, order.symbol, request_id
                        )
                    except Exception:
                        continue
                    if polled.status == "filled":
                        cum_qty += polled.filled_qty or remaining
                        cum_notional += (polled.filled_qty or remaining) * float(polled.filled_price or peg_price)
                        cum_fee += polled.fee
                        log.info(ORDER_FILLED, message="limit order filled during poll",
                                 request_id=request_id, order_id=last_order_id, attempt=attempt)
                        return await self._finalize_combined(
                            order, cum_qty, cum_notional, cum_fee,
                            order_id=last_order_id, attempt=attempt,
                        )

            # Attempts exhausted → cancel remainder, harvest fills, market fallback
            if last_order_id:
                harvested = await self._harvest_order(last_order_id, order.symbol, request_id)
                last_order_id = ""
                if harvested is not None and harvested.filled_qty > 0:
                    cum_qty += harvested.filled_qty
                    cum_notional += harvested.filled_qty * float(harvested.filled_price or 0.0)
                    cum_fee += harvested.fee

            remaining = self._floor_to_step(order.quantity - cum_qty, qty_step)
            if remaining < min_qty:
                return await self._finalize_combined(order, cum_qty, cum_notional, cum_fee)

            log.warning(LIMIT_FALLBACK_TO_MARKET,
                        message="limit repeg exhausted, falling back to market",
                        request_id=request_id, attempts=REPEG_MAX_ATTEMPTS,
                        remaining_qty=remaining, cum_filled=cum_qty)
            fallback = order.model_copy(
                update={"order_type": "market", "price": None, "post_only": False, "quantity": remaining}
            )
            market_result = await self._execute_with_retries(fallback)
            return await self._merge_market_fallback(order, market_result, cum_qty, cum_notional, cum_fee)

        except asyncio.CancelledError:
            # 엔진 타임아웃/서비스 종료 — 거래소에 떠 있는 주문을 정리해 고아화 방지
            if last_order_id:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._connector.cancel_order(last_order_id, order.symbol)),
                        timeout=5.0,
                    )
                    log.warning(ORDER_CANCELLED, message="repeg cancelled — outstanding order cleaned up",
                                request_id=request_id, order_id=last_order_id)
                except Exception:
                    log.error(ORDER_REJECTED, message="repeg 취소 중 미체결 주문 정리 실패 — 수동 확인 필요",
                              request_id=request_id, order_id=last_order_id)
            raise

    # ------------------------------------------------------------------
    # Re-peg helpers
    # ------------------------------------------------------------------

    async def _harvest_order(
        self, order_id: str, symbol: str, request_id: str
    ) -> OrderResult | None:
        """주문을 취소하고 그때까지의 체결량을 조회해 반환한다.

        취소 실패(이미 전량 체결 등)와 무관하게 최종 체결량을 조회한다.
        bybit 커넥터는 조회 실패 시 status='new', filled_qty=0 폴백을 반환하므로
        그 시그니처가 나오면 1회 재조회한다 (부분체결 누락 → 과체결 방지).
        """
        try:
            await self._connector.cancel_order(order_id, symbol)
        except Exception as exc:
            log.warning(ORDER_CANCELLED, message="repeg cancel error (continuing to fetch fills)",
                        order_id=order_id, error=str(exc)[:200])

        result: OrderResult | None = None
        for fetch_attempt in (1, 2):
            try:
                result = await self._connector.fetch_order_result(order_id, symbol, request_id)
            except Exception as exc:
                log.warning(ORDER_RETRY, message="harvest fetch error",
                            order_id=order_id, fetch_attempt=fetch_attempt, error=str(exc)[:200])
                result = None
            # 방금 취소한 주문이 'new'로 보이면 조회 폴백/전파 지연 — 재시도
            if result is not None and not (result.status == "new" and result.filled_qty == 0):
                return result
            await asyncio.sleep(0.3)

        if result is None:
            log.error(ORDER_REJECTED,
                      message="취소된 주문의 체결량 조회 실패 — 부분체결 미반영 가능, 수동 확인 필요",
                      order_id=order_id, request_id=request_id)
        return result

    async def _get_qty_limits(self, symbol: str) -> tuple[float, float]:
        """(min_qty, qty_step) — 거래소 메타에서 조회, 실패 시 BTC 기본값."""
        cached = self._qty_limits_cache.get(symbol)
        if cached:
            return cached
        try:
            info = await self._connector.get_min_order_sizes([symbol])
            meta = info.get(symbol, {})
            limits = (
                float(meta.get("min_qty") or FALLBACK_MIN_QTY),
                float(meta.get("qty_step") or FALLBACK_QTY_STEP),
            )
        except Exception as exc:
            log.warning(SERVICE_HEALTH_FAIL, message="min order size lookup failed, using defaults",
                        symbol=symbol, error=str(exc)[:200])
            limits = (FALLBACK_MIN_QTY, FALLBACK_QTY_STEP)
        self._qty_limits_cache[symbol] = limits
        return limits

    @staticmethod
    def _floor_to_step(qty: float, step: float) -> float:
        """수량을 거래소 스텝 단위로 내림 (음수 방지)."""
        if qty <= 0:
            return 0.0
        if step <= 0:
            return qty
        import math
        return max(math.floor((qty + 1e-12) / step) * step, 0.0)

    def _combined_result(
        self,
        order: OrderRequest,
        qty: float,
        notional: float,
        fee: float,
        status: str,
        order_id: str = "",
    ) -> dict[str, Any]:
        """누적 체결값으로 합산 결과 dict를 만든다."""
        return {
            "request_id": order.request_id,
            "order_id": order_id,
            "status": status,
            "filled_qty": round(qty, 12),
            "filled_price": (notional / qty) if qty > 0 else None,
            "fee": fee,
            "fee_currency": "USDT",
            "strategy_id": order.strategy_id,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
        }

    async def _finalize_combined(
        self,
        order: OrderRequest,
        qty: float,
        notional: float,
        fee: float,
        *,
        order_id: str = "",
        attempt: int | None = None,
    ) -> dict[str, Any]:
        """누적 체결로 주문을 filled 종결 처리한다."""
        request_id = order.request_id
        if qty <= 0:
            # 체결 없이 잔량만 최소수량 미만 — filled로 보고하면 전략이 오인하므로 거부 처리
            self._transition(request_id, self._order_states.get(request_id), OrderState.REJECTED)
            rejection = self._combined_result(order, 0.0, 0.0, fee, "rejected", order_id)
            rejection["reason"] = "repeg_no_fill_below_min_qty"
            await self._update_order_from_result(rejection)
            log.error(ORDER_REJECTED, message="재페그 종결 시 체결량 0 — 거부 처리",
                      request_id=request_id)
            return rejection
        self._transition(request_id, self._order_states.get(request_id), OrderState.FILLED)
        result_dict = self._combined_result(order, qty, notional, fee, "filled", order_id)
        await self._update_order_from_result(result_dict)
        log.info(ORDER_FILLED, message="limit repeg fill complete",
                 request_id=request_id, filled_qty=result_dict["filled_qty"],
                 filled_price=result_dict["filled_price"], attempt=attempt)
        return result_dict

    async def _merge_market_fallback(
        self,
        order: OrderRequest,
        market_result: dict[str, Any],
        cum_qty: float,
        cum_notional: float,
        cum_fee: float,
    ) -> dict[str, Any]:
        """시장가 폴백 결과에 재페그 부분체결 누적을 합산한다."""
        if cum_qty <= 0:
            return market_result

        m_qty = float(market_result.get("filled_qty") or 0.0)
        m_price = float(market_result.get("filled_price") or 0.0)
        total_qty = cum_qty + m_qty
        total_notional = cum_notional + m_qty * m_price
        total_fee = cum_fee + float(market_result.get("fee") or 0.0)

        status = market_result.get("status", "")
        if status == "rejected":
            # 시장가 폴백 실패 — 부분체결만 남음. 전략이 재동기화하도록 부분 종결로 보고.
            status = "partially_filled"
            log.error(ORDER_REJECTED,
                      message="시장가 폴백 거부 — 재페그 부분체결만 보유, 잔량 미체결",
                      request_id=order.request_id, cum_filled=cum_qty,
                      reason=market_result.get("reason", ""))

        merged = self._combined_result(
            order, total_qty, total_notional, total_fee, status,
            order_id=market_result.get("order_id", ""),
        )
        if market_result.get("reason"):
            merged["reason"] = market_result["reason"]
        await self._update_order_from_result(merged)
        return merged

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
            log.warning(ORDER_CANCELLED, message="cancel order unknown request", request_id=request_id)
            return {"request_id": request_id, "status": "rejected", "reason": "unknown_request_id"}

        current_state = self._order_states.get(request_id)
        if current_state in _TERMINAL_STATES:
            log.info(ORDER_CANCELLED, message="cancel order already terminal", request_id=request_id, state=current_state)
            return {"request_id": request_id, "status": str(current_state), "reason": "already_terminal"}

        success = await self._connector.cancel_order(order_id, symbol)

        if success:
            self._transition(request_id, current_state, OrderState.CANCELLED)
            await self._update_order_status(request_id, OrderState.CANCELLED)
            log.info(ORDER_CANCELLED, message="order cancelled", request_id=request_id, order_id=order_id)
            return {"request_id": request_id, "order_id": order_id, "status": "cancelled"}

        log.warning(ORDER_CANCELLED, message="cancel order failed", request_id=request_id, order_id=order_id)
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
            log.warning(ORDER_CANCELLED, message="modify cancel failed", request_id=request_id, order_id=order_id)
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
            ORDER_SENT,
            message="order modified",
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

                # Notify safety guard of successful API response
                try:
                    await self._redis.set(
                        "execution:last_api_response",
                        str(int(__import__("time").monotonic() * 1000)),
                        ex=60,
                    )
                except Exception:
                    pass  # Non-critical

                # Track the exchange order id
                if result.order_id:
                    self._request_to_order[request_id] = result.order_id

                # Map result status to our internal state
                new_state = self._map_result_status(result.status)
                self._transition(request_id, OrderState.SUBMITTED, new_state)

                result_dict = self._result_to_dict(result, order)
                await self._update_order_from_result(result_dict)

                log.info(
                    ORDER_SENT,
                    message="order executed",
                    request_id=request_id,
                    order_id=result.order_id,
                    status=result.status,
                    attempt=attempt,
                )
                return result_dict

            except Exception as exc:
                last_error = str(exc)
                exc_type = type(exc).__name__

                # Non-retryable errors: fail immediately
                non_retryable_keywords = (
                    "AuthenticationError", "InsufficientFunds", "InvalidOrder",
                    "BadSymbol", "PermissionDenied",
                )
                if any(kw in exc_type for kw in non_retryable_keywords):
                    log.error(
                        ORDER_REJECTED,
                        message="order non-retryable error",
                        request_id=request_id,
                        error_type=exc_type,
                        error=last_error,
                    )
                    break  # Do not retry

                log.warning(
                    ORDER_RETRY,
                    message="order attempt failed",
                    request_id=request_id,
                    attempt=attempt,
                    max_retries=self._max_retries,
                    error=last_error,
                )

                if attempt < self._max_retries:
                    # Exponential backoff with jitter
                    import random
                    backoff = self._retry_backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
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
                ORDER_REJECTED,
                message="order state already terminal",
                request_id=request_id,
                current=str(from_state),
                requested=str(to_state),
            )
            return

        if from_state is not None:
            valid_targets = _VALID_TRANSITIONS.get(from_state, set())
            if to_state not in valid_targets:
                log.warning(
                    ORDER_REJECTED,
                    message="order invalid state transition",
                    request_id=request_id,
                    from_state=str(from_state),
                    to_state=str(to_state),
                )
                # Allow it but log the warning -- don't block execution

        self._order_states[request_id] = to_state
        log.info(
            ORDER_SENT,
            message="order state transition",
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
            log.exception(ORDER_REJECTED, message="persist new order error", request_id=order.request_id)

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
            log.exception(ORDER_REJECTED, message="update order result error", request_id=result.get("request_id"))

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
            log.exception(ORDER_REJECTED, message="update order status error", request_id=request_id)

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
            log.exception(ORDER_REJECTED, message="fetch order error", request_id=request_id)
            return None

    async def _restore_inflight_orders(self) -> None:
        """On startup, load orders that were in non-terminal states.

        재시작하면 재페그/폴링 루프가 끊겨 in-flight 주문을 관리할 주체가 없다.
        거래소에 아직 떠 있는 주문은 취소해 고아화를 막는다 (떠 있는 채로 나중에
        체결되면 전략·SL 관리 밖의 포지션이 생긴다). 전략은 사전 동기화로
        다음 봉에서 진실 기반으로 재판단한다.
        """
        try:
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT request_id, order_id, status, symbol
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

            log.info(SERVICE_HEALTH_OK, message="inflight orders restored", count=len(rows))

            # 고아 주문 정리
            for row in rows:
                rid = row["request_id"]
                oid = row["order_id"]
                symbol = row["symbol"]
                if not oid:
                    # 거래소 도달 전 pending — 재시작으로 유실됐으므로 거부 종결
                    self._order_states[rid] = OrderState.REJECTED
                    await self._update_order_status(rid, OrderState.REJECTED)
                    log.warning(ORDER_REJECTED, message="재시작 시 미발주 pending 주문 거부 종결",
                                request_id=rid)
                    continue
                try:
                    status = await self._connector.fetch_order_status(oid, symbol)
                    if status == "open":
                        await self._connector.cancel_order(oid, symbol)
                        self._order_states[rid] = OrderState.CANCELLED
                        await self._update_order_status(rid, OrderState.CANCELLED)
                        log.error(ORDER_CANCELLED,
                                  message="재시작 시 고아 미체결 주문 취소 — 전략이 다음 봉에 재판단",
                                  request_id=rid, order_id=oid, symbol=symbol)
                    elif status == "closed":
                        result = await self._connector.fetch_order_result(oid, symbol, rid)
                        self._order_states[rid] = OrderState.FILLED
                        await self._update_order_from_result({
                            "request_id": rid,
                            "order_id": oid,
                            "status": "filled",
                            "filled_qty": result.filled_qty,
                            "filled_price": result.filled_price,
                            "fee": result.fee,
                        })
                        log.warning(ORDER_FILLED,
                                    message="재시작 중 in-flight 주문 체결 확인 — DB 갱신 (전략은 포지션 동기화로 반영)",
                                    request_id=rid, order_id=oid, filled_qty=result.filled_qty)
                    else:
                        log.warning(SERVICE_HEALTH_FAIL, message="in-flight 주문 상태 미확인",
                                    request_id=rid, order_id=oid, status=status)
                except Exception:
                    log.exception(SERVICE_HEALTH_FAIL, message="in-flight 주문 정리 실패",
                                  request_id=rid, order_id=oid)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="restore inflight orders error")

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
