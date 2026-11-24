"""Bybit linear-perpetual connector (ccxt REST only).

Ported from ``shared/exchange/bybit.py``. Differences from the old connector:

* plain ``ccxt.async_support`` instead of ``ccxt.pro`` (nothing here needs a
  websocket; ``ccxt.pro.bybit`` only adds ``watch_*`` on top of the same REST
  implementation, so request parameters are identical);
* no ABC / factory, no websocket ``subscribe_*``, no ``get_funding_rate``,
  no ``get_trading_fees``;
* ``get_ohlcv`` returns the raw ccxt rows ``[ts_ms, open, high, low, close,
  volume]`` (oldest first). The last row is the bar still in progress.
  The caller decides which bars are confirmed;
* ``get_balance`` also returns ``"wallet"`` (see its docstring);
* ``cancel_conditional_orders`` is new (best-effort stale stop cleanup);
* ``last_error`` keeps the text of the last swallowed order error and
  ``last_error_ambiguous`` says whether it was ambiguous (see ``is_ambiguous_error``);
* ``cancel_open_orders`` cancels all regular open orders of a symbol.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import ccxt.async_support as ccxt_async
import structlog

from shared.models.market import OrderBook, OrderBookLevel
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position

log = structlog.get_logger(__name__)

_DEFAULT_RATE_LIMIT = 50  # ms between REST calls
MAX_LEVERAGE: int = 3  # Project policy: never exceed 3x leverage

_POSTONLY_MARKERS = ("110030", "postonly", "post-only", "post_only", "would immediately")

_STATUS_MAP: dict[str, str] = {
    "open": "new",
    "closed": "filled",
    "canceled": "cancelled",
    "expired": "expired",
    "rejected": "rejected",
}


def is_ambiguous_error(exc: BaseException) -> bool:
    """True when it is unknown whether the exchange received (and kept) the request.

    Definite: the exchange answered with an error (ccxt ``ExchangeError`` family: InvalidOrder,
    InsufficientFunds, BadRequest, AuthenticationError, BadSymbol, PermissionDenied ...), so the
    order does not exist. Ambiguous: ccxt ``NetworkError`` (RequestTimeout, ExchangeNotAvailable,
    DDoSProtection, ...: the request may have been processed before the answer was lost) and any
    exception that is not a ccxt exchange error (unknown failure, e.g. while parsing the answer).
    """
    if isinstance(exc, ccxt_async.NetworkError):  # checked first: ExchangeNotAvailable is a NetworkError
        return True
    return not isinstance(exc, ccxt_async.ExchangeError)


class PostOnlyRejected(Exception):
    """Bybit post-only rejection: order would execute as taker (retCode 110030)."""


class BybitConnector:
    """Async Bybit linear-perpetual connector implementing ``bot.interfaces.Exchange``."""

    exchange_id: str = "bybit"

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = False,
        rate_limit: int = _DEFAULT_RATE_LIMIT,
    ) -> None:
        opts: dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "rateLimit": rate_limit,
            "options": {
                "defaultType": "swap",
                "defaultSubType": "linear",
                "adjustForTimeDifference": True,
                "recvWindow": 20000,  # WSL clock drift: default 5s -> 20s
            },
        }
        if testnet:
            opts["sandbox"] = True

        self._opts = opts  # kept so a stale session can be rebuilt
        self._exchange: ccxt_async.bybit = ccxt_async.bybit(opts)
        self._connected = False
        self._rate_limiter = asyncio.Semaphore(10)
        # Text of the last exception swallowed by place_order (diagnostics only).
        self.last_error: str = ""
        # True when the last swallowed error does not prove the order was refused (timeout, 5xx,
        # unknown exception): the order may exist on the exchange.
        self.last_error_ambiguous: bool = False

    # ── lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        if self._connected:
            return
        try:
            await self._exchange.load_markets()
            self._connected = True
            log.info("bybit_connector_ready", testnet=bool(getattr(self._exchange, "sandbox", False)))
        except Exception:
            # Close the aiohttp session opened by load_markets() to avoid a leak.
            try:
                await self._exchange.close()
            except Exception:
                pass
            raise

    async def disconnect(self) -> None:
        if not self._connected:
            return
        await self._exchange.close()
        self._connected = False
        log.info("bybit_connector_closed")

    # ── helpers ──────────────────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("BybitConnector is not connected: call connect() first")

    async def _refresh_session(self) -> None:
        """Recover from stale aiohttp connections by recreating the ccxt instance."""
        try:
            await self._exchange.close()
        except Exception:
            pass
        self._exchange = ccxt_async.bybit(self._opts)
        await self._exchange.load_markets()
        log.info("bybit_session_refreshed")

    # ── market data ─────────────────────────────────────────────────────

    async def get_ticker(self, symbol: str) -> dict[str, Any]:
        self._ensure_connected()
        async with self._rate_limiter:
            return await self._exchange.fetch_ticker(symbol)

    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook:
        self._ensure_connected()
        async with self._rate_limiter:
            raw = await self._exchange.fetch_order_book(symbol, limit)
        return OrderBook(
            exchange=self.exchange_id,
            symbol=symbol,
            bids=[OrderBookLevel(price=p, quantity=q) for p, q in raw["bids"]],
            asks=[OrderBookLevel(price=p, quantity=q) for p, q in raw["asks"]],
            timestamp=datetime.fromtimestamp(raw["timestamp"] / 1000, tz=timezone.utc)
            if raw.get("timestamp")
            else datetime.now(tz=timezone.utc),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "4h",
        since: int | None = None,
        limit: int = 1000,
    ) -> list[Any]:
        """Raw ccxt rows ``[ts_ms, o, h, l, c, v]``, oldest first.

        The last row is the bar still in progress; it is not confirmed.
        """
        self._ensure_connected()
        async with self._rate_limiter:
            return await self._exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

    # ── trading ──────────────────────────────────────────────────────────

    async def place_order(self, order: OrderRequest) -> OrderResult:
        """Place one order.

        Raises ``PostOnlyRejected`` when Bybit refuses a post-only order that
        would cross. Any other exchange error is swallowed and returned as a
        ``rejected`` result (text in ``self.last_error``): a blind retry after a
        timeout could double an order, so callers must re-sync from the exchange.
        """
        self._ensure_connected()
        params: dict[str, Any] = {}
        if order.post_only:
            params["postOnly"] = True
        if order.reduce_only:
            params["reduceOnly"] = True
        if order.stop_loss is not None:
            params["stopLoss"] = {"triggerPrice": order.stop_loss}
        if order.take_profit is not None:
            params["takeProfit"] = {"triggerPrice": order.take_profit}

        try:
            async with self._rate_limiter:
                result = await self._exchange.create_order(
                    symbol=order.symbol,
                    type=order.order_type.replace("_", ""),
                    side=order.side,
                    amount=order.quantity,
                    price=order.price,
                    params=params,
                )
            # Bybit market orders often return status=None/open with no fill info.
            # Fetch once to get the actual filled status.
            order_id = result.get("id", "")
            if order_id and result.get("status") not in ("closed",):
                await asyncio.sleep(0.5)
                try:
                    async with self._rate_limiter:
                        fetched = await self._exchange.fetch_order(
                            order_id, order.symbol, params={"acknowledged": True}
                        )
                    if fetched:
                        result = fetched
                except Exception:
                    pass  # keep the original result
            self.last_error = ""
            self.last_error_ambiguous = False
            return OrderResult(
                request_id=order.request_id,
                order_id=order_id or result.get("id", ""),
                status=_STATUS_MAP.get(result.get("status"), "new"),
                filled_qty=result.get("filled", 0.0) or 0.0,
                filled_price=result.get("average"),
                fee=(result.get("fee") or {}).get("cost", 0.0) or 0.0,
                fee_currency=(result.get("fee") or {}).get("currency", "USDT") or "USDT",
                timestamp=datetime.now(tz=timezone.utc),
            )
        except Exception as exc:
            exc_str = str(exc)
            if any(m in exc_str.lower() for m in _POSTONLY_MARKERS):
                raise PostOnlyRejected(exc_str)
            self.last_error = f"{type(exc).__name__}: {exc_str[:300]}"
            self.last_error_ambiguous = is_ambiguous_error(exc)
            log.error("order_placement_failed", error=self.last_error, ambiguous=self.last_error_ambiguous,
                      request_id=order.request_id)
            return OrderResult(
                request_id=order.request_id,
                order_id="",
                status="rejected",
                timestamp=datetime.now(tz=timezone.utc),
            )

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                await self._exchange.cancel_order(order_id, symbol)
            return True
        except Exception as exc:
            log.warning("cancel_order_failed", order_id=order_id, error=str(exc)[:300])
            return False

    async def cancel_open_orders(self, symbol: str) -> int:
        """Cancel every regular (non-conditional) open order for ``symbol``; returns the count.

        Used after an ambiguous order placement (timeout) to make sure no order the bot does
        not track keeps resting. Assumption (not verifiable offline): ccxt ``cancel_all_orders``
        without ``trigger`` only touches regular orders, and the inline position stop loss sent
        with entries is a position attribute (tpslMode Full), not an order, so it is unaffected.
        Raises on API failure so the caller knows the cleanup did not happen.
        """
        self._ensure_connected()
        async with self._rate_limiter:
            res = await self._exchange.cancel_all_orders(symbol)
        if isinstance(res, list):
            return len(res)
        return 0

    async def cancel_conditional_orders(self, symbol: str) -> int:
        """Cancel any open conditional (stop) orders for ``symbol``; returns the count.

        Best-effort cleanup used after an exit. The inline position stop-loss
        sent with entries is a position-level setting (not an order in this
        list), so normally there is nothing to cancel.
        """
        self._ensure_connected()
        cancelled = 0
        async with self._rate_limiter:
            orders = await self._exchange.fetch_open_orders(symbol, params={"trigger": True})
        for o in orders or []:
            oid = o.get("id")
            if not oid:
                continue
            if await self.cancel_order(oid, symbol):
                cancelled += 1
        return cancelled

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage, capped at MAX_LEVERAGE."""
        self._ensure_connected()
        safe_leverage = min(leverage, MAX_LEVERAGE)
        try:
            async with self._rate_limiter:
                await self._exchange.set_leverage(safe_leverage, symbol)
            log.info("leverage_set", symbol=symbol, leverage=safe_leverage)
        except Exception as exc:
            # Bybit raises if leverage is already set to this value
            log.warning("set_leverage_warning", symbol=symbol, error=str(exc)[:300])

    async def set_margin_mode(self, symbol: str, mode: str = "isolated") -> None:
        """Set margin mode (isolated prevents cross-margin liquidation)."""
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                await self._exchange.set_margin_mode(mode, symbol)
            log.info("margin_mode_set", symbol=symbol, mode=mode)
        except Exception as exc:
            if "already" in str(exc).lower() or "110026" in str(exc):
                log.debug("margin_mode_already_set", mode=mode, symbol=symbol)
            else:
                log.warning("set_margin_mode_warning", symbol=symbol, error=str(exc)[:300])

    # ── account ──────────────────────────────────────────────────────────

    async def get_position(self, symbol: str) -> Position | None:
        self._ensure_connected()
        async with self._rate_limiter:
            positions = await self._exchange.fetch_positions([symbol])
        for pos in positions:
            size = float(pos.get("contracts", 0) or 0)
            if size == 0:
                continue
            return Position(
                exchange=self.exchange_id,
                symbol=symbol,
                side="long" if pos["side"] == "long" else "short",
                size=size,
                entry_price=float(pos.get("entryPrice", 0) or 0),
                unrealized_pnl=float(pos.get("unrealizedPnl", 0) or 0),
                leverage=float(pos.get("leverage", 1) or 1),
                liquidation_price=float(pos["liquidationPrice"]) if pos.get("liquidationPrice") else None,
                margin_used=float(pos.get("initialMargin", 0) or 0),
            )
        return None

    async def get_balance(self) -> dict[str, float]:
        """USDT balance.

        Keys (all from ccxt ``fetch_balance``; Bybit coin ``walletBalance`` /
        ``availableToWithdraw``):

        * ``total``  : ``bal["total"]["USDT"]`` = wallet balance. Excludes
          unrealized PnL. This is what the old execution-engine published to
          Redis as both ``cache:wallet_balance`` and ``cache:balance:bybit``
          (the same JSON under two keys), and what the old orchestrator used
          as equity.
        * ``wallet`` : identical to ``total`` (explicit name for risk code).
        * ``free``   : ``bal["free"]["USDT"]`` = available balance.
        * ``used``   : ``bal["used"]["USDT"]``.
        """
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                bal = await self._exchange.fetch_balance()
        except Exception as first_exc:
            log.warning(
                "get_balance_failed_refreshing_session",
                exc_type=type(first_exc).__name__,
                error=str(first_exc)[:300],
            )
            await self._refresh_session()
            async with self._rate_limiter:
                bal = await self._exchange.fetch_balance()
        total = float(bal.get("total", {}).get("USDT", 0) or 0)
        return {
            "total": total,
            "free": float(bal.get("free", {}).get("USDT", 0) or 0),
            "used": float(bal.get("used", {}).get("USDT", 0) or 0),
            "wallet": total,
        }

    async def get_min_order_sizes(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        """``{symbol: {min_qty, qty_step, min_notional, contract_size}}`` from market metadata."""
        self._ensure_connected()
        result: dict[str, dict[str, float]] = {}
        markets = self._exchange.markets or {}
        for symbol in symbols:
            market = markets.get(symbol, {})
            limits = market.get("limits", {})
            precision = market.get("precision", {})
            result[symbol] = {
                "min_qty": float(limits.get("amount", {}).get("min") or 0.001),
                "qty_step": float(precision.get("amount") or 0.001),
                "min_notional": float(limits.get("cost", {}).get("min") or 1.0),
                "contract_size": float(market.get("contractSize") or 1.0),
            }
        return result

    # ── order queries ────────────────────────────────────────────────────

    async def fetch_order_status(self, order_id: str, symbol: str) -> str:
        """ccxt order status for polling: 'open', 'closed', 'canceled', ... or 'unknown'."""
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                raw = await self._exchange.fetch_order(order_id, symbol, params={"acknowledged": True})
            return raw.get("status") or "unknown"
        except Exception as exc:
            log.debug("fetch_order_status_failed", order_id=order_id, error=str(exc)[:200])
            return "unknown"

    async def fetch_order_result(self, order_id: str, symbol: str, request_id: str) -> OrderResult:
        """Fetch a placed order as an OrderResult (re-peg fill confirmation).

        On a query failure returns ``status='new'`` with zero fill (the executor
        treats that signature as "unknown" and re-queries).
        """
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                raw = await self._exchange.fetch_order(order_id, symbol, params={"acknowledged": True})
            return OrderResult(
                request_id=request_id,
                order_id=order_id,
                status=_STATUS_MAP.get(raw.get("status"), "new"),
                filled_qty=float(raw.get("filled") or 0),
                filled_price=raw.get("average"),
                fee=float((raw.get("fee") or {}).get("cost") or 0),
                fee_currency=(raw.get("fee") or {}).get("currency") or "USDT",
                timestamp=datetime.now(tz=timezone.utc),
            )
        except Exception as exc:
            log.warning("fetch_order_result_failed", order_id=order_id, error=str(exc)[:300])
            return OrderResult(
                request_id=request_id,
                order_id=order_id,
                status="new",
                timestamp=datetime.now(tz=timezone.utc),
            )

    def price_to_precision(self, symbol: str, price: float) -> float:
        """Round price to the exchange tick size for limit order placement."""
        try:
            return float(self._exchange.price_to_precision(symbol, price))
        except Exception:
            return round(price, 1)  # BTC/USDT perp default tick = $0.1
