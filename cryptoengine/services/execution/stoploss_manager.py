"""Stop-Loss Manager — exchange-native stop-loss order placement and lifecycle.

Responsibilities:
  - On position entry: place a conditional stop-loss order on the exchange
    (Bybit: ``stopLoss`` param or separate conditional order)
  - On position exit: cancel any pending stop-loss order for that position
  - On bot restart: recover stop-loss orders for all existing open positions
    (reconciliation hook, called by PositionTracker after sync)
  - Cache active stop-loss order IDs in Redis
    (key: ``cache:stoploss:{exchange}:{symbol}``)

Stop-Loss Calculation (FA strategy, lev=5x, MDD=-4.52%)
  - Default: entry_price × (1 - stop_loss_pct) for long (spot hedge)
  - Default: entry_price × (1 + stop_loss_pct) for short (perp)
  - stop_loss_pct is configurable; default -2.0% per-leg

Bybit API:
  - Inline SL: pass ``params={"stopLoss": {"triggerPrice": price}}``
    in ``create_order`` alongside the main order.
  - Standalone SL: place a separate ``stopMarket`` order with
    ``reduce_only=True`` after the main order fills.

This module uses the *standalone* approach so it can be attached to any
already-open position (including those recovered on restart).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import redis.asyncio as aioredis
import structlog

from shared.exchange import ExchangeConnector
from shared.log_events import (
    ORDER_SENT,
    ORDER_CANCELLED,
    ORDER_REJECTED,
    SERVICE_HEALTH_OK,
    SERVICE_HEALTH_FAIL,
    SERVICE_STARTED,
)

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

# Default stop-loss distance expressed as a fraction of entry price (per leg).
# FA strategy: lev=5x → -2% per-leg ≈ -10% equity drawdown before triggering.
# This matches the fa80_lev5_r30 backtest profile (MDD -4.52%, 0 liquidations).
DEFAULT_STOP_LOSS_PCT: float = 0.02  # 2 % below/above entry

# Redis key pattern for cached stop-loss order IDs.
_CACHE_KEY = "cache:stoploss:{exchange}:{symbol}"

# Redis TTL for cached stop-loss order IDs (seconds).
CACHE_TTL: int = 86_400  # 24 h

# How long to wait between retries when placing/cancelling SL orders.
_RETRY_BACKOFF: float = 1.0
_MAX_RETRIES: int = 3


class StopLossManager:
    """Manages exchange-native stop-loss orders tied to open positions.

    Thread-safe (all state is confined to this asyncio event loop).
    Uses a standalone ``stop_market`` / reduce-only order so the SL
    survives bot restarts — the order lives on the exchange, not in memory.
    """

    def __init__(
        self,
        *,
        connector: ExchangeConnector,
        redis: aioredis.Redis,
        exchange_id: str,
        stop_loss_pct: float = DEFAULT_STOP_LOSS_PCT,
    ) -> None:
        self._connector = connector
        self._redis = redis
        self._exchange_id = exchange_id
        self._stop_loss_pct = stop_loss_pct

        # Local in-memory map: symbol -> sl_order_id (mirrors Redis cache)
        self._sl_orders: dict[str, str] = {}

        log.info(
            SERVICE_STARTED,
            message="stoploss manager initialised",
            exchange=exchange_id,
            stop_loss_pct=stop_loss_pct,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def place_stop_loss(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        quantity: float,
        *,
        stop_loss_pct: float | None = None,
    ) -> str | None:
        """Place a reduce-only stop-loss order for a freshly opened position.

        Parameters
        ----------
        symbol      : Exchange symbol (e.g. ``BTC/USDT:USDT``)
        side        : Position side — ``"long"`` or ``"short"``
        entry_price : Average fill price of the entry order
        quantity    : Position size (contracts / coins)
        stop_loss_pct : Override the instance default (fractional, e.g. 0.02)

        Returns
        -------
        The exchange stop-loss order ID, or ``None`` on failure.
        """
        pct = stop_loss_pct if stop_loss_pct is not None else self._stop_loss_pct
        sl_price = self._calc_sl_price(side, entry_price, pct)

        # The SL order is on the *opposite* side to the position
        sl_order_side = "sell" if side == "long" else "buy"

        log.info(
            ORDER_SENT,
            message="placing stop-loss order",
            exchange=self._exchange_id,
            symbol=symbol,
            position_side=side,
            entry_price=entry_price,
            sl_price=sl_price,
            sl_order_side=sl_order_side,
            quantity=quantity,
            stop_loss_pct=pct,
        )

        sl_order_id: str | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                sl_order_id = await self._submit_stop_order(
                    symbol=symbol,
                    side=sl_order_side,
                    quantity=quantity,
                    trigger_price=sl_price,
                )
                break
            except Exception as exc:
                log.warning(
                    ORDER_REJECTED,
                    message="stop-loss placement attempt failed",
                    exchange=self._exchange_id,
                    symbol=symbol,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(_RETRY_BACKOFF * attempt)

        if sl_order_id:
            await self._cache_sl_order(symbol, sl_order_id)
            log.info(
                ORDER_SENT,
                message="stop-loss order placed",
                exchange=self._exchange_id,
                symbol=symbol,
                sl_order_id=sl_order_id,
                sl_price=sl_price,
            )
        else:
            log.error(
                ORDER_REJECTED,
                message="stop-loss placement failed after all retries",
                exchange=self._exchange_id,
                symbol=symbol,
            )

        return sl_order_id

    async def cancel_stop_loss(self, symbol: str) -> bool:
        """Cancel the active stop-loss order for *symbol* when closing a position.

        Returns ``True`` if cancelled (or no SL was active), ``False`` on error.
        """
        sl_order_id = await self._get_cached_sl_order(symbol)
        if not sl_order_id:
            log.debug(
                SERVICE_HEALTH_OK,
                message="no active stop-loss to cancel",
                exchange=self._exchange_id,
                symbol=symbol,
            )
            return True

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                success = await self._connector.cancel_order(sl_order_id, symbol)
                if success:
                    await self._clear_sl_cache(symbol)
                    log.info(
                        ORDER_CANCELLED,
                        message="stop-loss order cancelled",
                        exchange=self._exchange_id,
                        symbol=symbol,
                        sl_order_id=sl_order_id,
                    )
                    return True
                # cancel_order returned False — exchange rejected; treat as gone
                log.warning(
                    ORDER_CANCELLED,
                    message="stop-loss cancel returned False (already gone?)",
                    exchange=self._exchange_id,
                    symbol=symbol,
                    sl_order_id=sl_order_id,
                )
                await self._clear_sl_cache(symbol)
                return True
            except Exception as exc:
                log.warning(
                    ORDER_CANCELLED,
                    message="stop-loss cancel attempt failed",
                    exchange=self._exchange_id,
                    symbol=symbol,
                    sl_order_id=sl_order_id,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(_RETRY_BACKOFF * attempt)

        log.error(
            ORDER_REJECTED,
            message="stop-loss cancel failed after all retries",
            exchange=self._exchange_id,
            symbol=symbol,
            sl_order_id=sl_order_id,
        )
        return False

    async def recover_stop_losses(
        self,
        positions: list[dict[str, Any]],
    ) -> None:
        """Re-attach stop-loss orders to open positions after a bot restart.

        Called by the reconciliation flow once ``PositionTracker.sync_from_exchange``
        has completed.  For each open position:

        1. Check if a cached SL order ID exists in Redis.
        2. Verify the order is still open on the exchange (via ``fetch_order``).
        3. If the cached order is gone (filled / cancelled / missing), place a new SL.

        Parameters
        ----------
        positions : list of position dicts with keys:
                    ``symbol``, ``side``, ``entry_price``, ``size``
        """
        log.info(
            SERVICE_STARTED,
            message="recovering stop-loss orders",
            exchange=self._exchange_id,
            position_count=len(positions),
        )

        for pos in positions:
            symbol = pos.get("symbol", "")
            side = pos.get("side", "long")
            entry_price = float(pos.get("entry_price", 0))
            size = float(pos.get("size", 0))

            if not symbol or entry_price <= 0 or size <= 0:
                continue

            cached_id = await self._get_cached_sl_order(symbol)
            sl_alive = False

            if cached_id:
                sl_alive = await self._is_sl_order_active(cached_id, symbol)
                if sl_alive:
                    log.info(
                        SERVICE_HEALTH_OK,
                        message="existing stop-loss still active",
                        exchange=self._exchange_id,
                        symbol=symbol,
                        sl_order_id=cached_id,
                    )
                else:
                    log.warning(
                        SERVICE_HEALTH_FAIL,
                        message="cached stop-loss no longer active — replacing",
                        exchange=self._exchange_id,
                        symbol=symbol,
                        cached_sl_order_id=cached_id,
                    )
                    await self._clear_sl_cache(symbol)

            if not sl_alive:
                await self.place_stop_loss(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    quantity=size,
                )

    # ------------------------------------------------------------------
    # Private: order submission
    # ------------------------------------------------------------------

    async def _submit_stop_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        trigger_price: float,
    ) -> str:
        """Submit a stop-market reduce-only order to the exchange.

        Uses the raw CCXT ``create_order`` call directly via the connector's
        underlying ``_exchange`` attribute when available, otherwise falls back
        to the connector's public ``place_order`` API with ``stop_market`` type.

        Returns the exchange-assigned order ID.

        Raises on any failure so the caller can retry.
        """
        # Prefer direct CCXT access for maximum control over params
        ccxt_exchange = getattr(self._connector, "_exchange", None)
        if ccxt_exchange is not None:
            result = await ccxt_exchange.create_order(
                symbol=symbol,
                type="StopMarket",
                side=side,
                amount=quantity,
                price=None,
                params={
                    "triggerPrice": trigger_price,
                    "reduceOnly": True,
                    "triggerBy": "MarkPrice",
                },
            )
            order_id: str = result.get("id", "")
            if not order_id:
                raise RuntimeError(f"Exchange returned empty order id for {symbol} SL")
            return order_id

        # Fallback: use the connector's public interface (for non-Bybit connectors)
        from shared.models.order import OrderRequest
        import uuid

        sl_request = OrderRequest(
            strategy_id="stoploss_manager",
            exchange=self._exchange_id,
            symbol=symbol,
            side=side,
            order_type="stop_market",
            quantity=quantity,
            price=None,
            post_only=False,
            reduce_only=True,
            stop_loss=trigger_price,
            request_id=uuid.uuid4().hex,
        )
        from shared.models.order import OrderResult
        result_model: OrderResult = await self._connector.place_order(sl_request)
        if not result_model.order_id:
            raise RuntimeError(f"Connector returned empty order_id for {symbol} SL")
        return result_model.order_id

    # ------------------------------------------------------------------
    # Private: SL price calculation
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_sl_price(side: str, entry_price: float, pct: float) -> float:
        """Calculate trigger price for the stop-loss.

        - Long position (spot hedge leg): trigger below entry
          ``sl_price = entry_price * (1 - pct)``
        - Short position (perp leg): trigger above entry
          ``sl_price = entry_price * (1 + pct)``
        """
        if side == "long":
            return entry_price * (1.0 - pct)
        else:
            return entry_price * (1.0 + pct)

    # ------------------------------------------------------------------
    # Private: exchange order status check
    # ------------------------------------------------------------------

    async def _is_sl_order_active(self, order_id: str, symbol: str) -> bool:
        """Return True if the given order is still open/untriggered on exchange."""
        ccxt_exchange = getattr(self._connector, "_exchange", None)
        if ccxt_exchange is None:
            # Cannot verify without direct ccxt access; assume gone to be safe
            return False
        try:
            order = await ccxt_exchange.fetch_order(order_id, symbol)
            status = order.get("status", "closed")
            return status == "open"
        except Exception as exc:
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="fetch_order for SL check failed",
                exchange=self._exchange_id,
                symbol=symbol,
                order_id=order_id,
                error=str(exc),
            )
            return False

    # ------------------------------------------------------------------
    # Private: Redis cache helpers
    # ------------------------------------------------------------------

    def _cache_key(self, symbol: str) -> str:
        return _CACHE_KEY.format(exchange=self._exchange_id, symbol=symbol)

    async def _cache_sl_order(self, symbol: str, order_id: str) -> None:
        key = self._cache_key(symbol)
        payload = json.dumps({"order_id": order_id, "ts": time.time()})
        try:
            await self._redis.setex(key, CACHE_TTL, payload)
            self._sl_orders[symbol] = order_id
        except Exception as exc:
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="failed to cache SL order id",
                exchange=self._exchange_id,
                symbol=symbol,
                error=str(exc),
            )

    async def _get_cached_sl_order(self, symbol: str) -> str | None:
        # Check local memory first
        if symbol in self._sl_orders:
            return self._sl_orders[symbol]
        key = self._cache_key(symbol)
        try:
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                order_id = data.get("order_id", "")
                if order_id:
                    self._sl_orders[symbol] = order_id
                    return order_id
        except Exception as exc:
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="failed to read SL cache",
                exchange=self._exchange_id,
                symbol=symbol,
                error=str(exc),
            )
        return None

    async def _clear_sl_cache(self, symbol: str) -> None:
        self._sl_orders.pop(symbol, None)
        key = self._cache_key(symbol)
        try:
            await self._redis.delete(key)
        except Exception as exc:
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="failed to clear SL cache",
                exchange=self._exchange_id,
                symbol=symbol,
                error=str(exc),
            )
