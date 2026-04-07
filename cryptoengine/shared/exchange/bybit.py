"""Bybit futures connector built on ccxt.pro (async WebSocket + REST)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import ccxt.pro as ccxtpro
import structlog

from shared.exchange.base import ExchangeConnector
from shared.models.market import (
    FundingRate,
    OHLCV,
    OrderBook,
    OrderBookLevel,
)
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position

log = structlog.get_logger(__name__)

_DEFAULT_RATE_LIMIT = 50  # ms between REST calls
MAX_LEVERAGE: int = 2  # Project policy: never exceed 2x leverage


class BybitConnector(ExchangeConnector):
    """Full async Bybit linear-perpetual connector."""

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
                "recvWindow": 20000,  # WSL clock drift 대응: 기본 5s → 20s
            },
        }
        if testnet:
            opts["sandbox"] = True

        self._exchange: ccxtpro.bybit = ccxtpro.bybit(opts)
        self._connected = False
        self._rate_limiter = asyncio.Semaphore(10)

    # ── lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        if self._connected:
            return
        try:
            await self._exchange.load_markets()
            self._connected = True
            log.info("bybit connector ready (testnet=%s)", getattr(self._exchange, "sandbox", False))
        except Exception:
            # Clean up the aiohttp session created during load_markets() to prevent leaks
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
        log.info("bybit connector closed")

    # ── helpers ──────────────────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("BybitConnector is not connected — call connect() first")

    # ── market data ─────────────────────────────────────────────────────

    async def get_ticker(self, symbol: str) -> dict[str, Any]:
        self._ensure_connected()
        async with self._rate_limiter:
            ticker = await self._exchange.fetch_ticker(symbol)
        return ticker

    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook:
        self._ensure_connected()
        async with self._rate_limiter:
            raw = await self._exchange.fetch_order_book(symbol, limit)
        return OrderBook(
            exchange=self.exchange_id,
            symbol=symbol,
            bids=[OrderBookLevel(price=p, quantity=q) for p, q in raw["bids"]],
            asks=[OrderBookLevel(price=p, quantity=q) for p, q in raw["asks"]],
            timestamp=datetime.fromtimestamp(
                raw["timestamp"] / 1000, tz=timezone.utc
            )
            if raw.get("timestamp")
            else datetime.now(tz=timezone.utc),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int = 100,
    ) -> list[OHLCV]:
        self._ensure_connected()
        async with self._rate_limiter:
            raw = await self._exchange.fetch_ohlcv(
                symbol, timeframe, since=since, limit=limit
            )
        return [
            OHLCV(
                exchange=self.exchange_id,
                symbol=symbol,
                timeframe=timeframe,
                open=c[1],
                high=c[2],
                low=c[3],
                close=c[4],
                volume=c[5],
                timestamp=datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
            )
            for c in raw
        ]

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        self._ensure_connected()
        async with self._rate_limiter:
            raw = await self._exchange.fetch_funding_rate(symbol)
        return FundingRate(
            exchange=self.exchange_id,
            symbol=symbol,
            rate=raw["fundingRate"],
            predicted_rate=raw.get("nextFundingRate"),
            next_funding_time=datetime.fromtimestamp(
                raw["fundingDatetime"]
                if isinstance(raw.get("fundingDatetime"), (int, float))
                else raw.get("fundingTimestamp", 0) / 1000,
                tz=timezone.utc,
            ),
            collected_at=datetime.now(tz=timezone.utc),
        )

    # ── trading ──────────────────────────────────────────────────────────

    async def place_order(self, order: OrderRequest) -> OrderResult:
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
            status_map: dict[str, str] = {
                "open": "new",
                "closed": "filled",
                "canceled": "cancelled",
                "expired": "expired",
                "rejected": "rejected",
            }
            return OrderResult(
                request_id=order.request_id,
                order_id=result["id"],
                status=status_map.get(result["status"], "new"),
                filled_qty=result.get("filled", 0.0) or 0.0,
                filled_price=result.get("average"),
                fee=result.get("fee", {}).get("cost", 0.0) or 0.0,
                fee_currency=result.get("fee", {}).get("currency", "USDT") or "USDT",
                timestamp=datetime.now(tz=timezone.utc),
            )
        except Exception as exc:
            log.exception("order placement failed: %s", exc)
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
            log.warning("cancel_order failed for %s: %s", order_id, exc)
            return False

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage for a symbol, capped at MAX_LEVERAGE."""
        self._ensure_connected()
        safe_leverage = min(leverage, MAX_LEVERAGE)
        try:
            async with self._rate_limiter:
                await self._exchange.set_leverage(safe_leverage, symbol)
            log.info("leverage_set symbol=%s leverage=%d", symbol, safe_leverage)
        except Exception as exc:
            # Some exchanges raise if leverage is already set to this value
            log.warning("set_leverage warning symbol=%s: %s", symbol, exc)

    async def set_margin_mode(self, symbol: str, mode: str = "isolated") -> None:
        """Set margin mode for a symbol to isolated (prevents cross-margin liquidation)."""
        self._ensure_connected()
        try:
            async with self._rate_limiter:
                await self._exchange.set_margin_mode(mode, symbol)
            log.info("margin_mode_set symbol=%s mode=%s", symbol, mode)
        except Exception as exc:
            # Bybit raises if margin mode is already set
            if "already" in str(exc).lower() or "110026" in str(exc):
                log.debug("margin_mode already %s for %s", mode, symbol)
            else:
                log.warning("set_margin_mode warning symbol=%s: %s", symbol, exc)

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
                liquidation_price=float(pos["liquidationPrice"])
                if pos.get("liquidationPrice")
                else None,
                margin_used=float(pos.get("initialMargin", 0) or 0),
            )
        return None

    async def get_balance(self) -> dict[str, float]:
        self._ensure_connected()
        async with self._rate_limiter:
            bal = await self._exchange.fetch_balance()
        return {
            "total": float(bal.get("total", {}).get("USDT", 0) or 0),
            "free": float(bal.get("free", {}).get("USDT", 0) or 0),
            "used": float(bal.get("used", {}).get("USDT", 0) or 0),
        }

    async def get_trading_fees(self, symbols: list[str] | None = None) -> dict[str, dict[str, float]]:
        """계정 VIP 등급 기반 실제 수수료 조회.

        Returns:
            {symbol: {"maker": float, "taker": float}}

        Fallback: 조회 실패 시 Bybit VIP0 기본값 반환.
        """
        self._ensure_connected()
        defaults = {"maker": 0.0002, "taker": 0.00055}
        try:
            async with self._rate_limiter:
                if symbols:
                    raw = await self._exchange.fetch_trading_fees(symbols)
                else:
                    raw = await self._exchange.fetch_trading_fees()
            result: dict[str, dict[str, float]] = {}
            for sym, fee_data in raw.items():
                result[sym] = {
                    "maker": float(fee_data.get("maker") or defaults["maker"]),
                    "taker": float(fee_data.get("taker") or defaults["taker"]),
                }
            log.info("trading_fees_fetched", symbols=list(result.keys())[:5], count=len(result))
            return result
        except Exception as exc:
            log.warning("get_trading_fees failed, using defaults: %s", exc)
            if symbols:
                return {s: dict(defaults) for s in symbols}
            return {"_default": dict(defaults)}

    async def get_min_order_sizes(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        """심볼별 최소 주문 크기 및 계약 단위 조회.

        Returns:
            {symbol: {"min_qty": float, "qty_step": float, "min_notional": float}}
        """
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

    # ── websocket streams ────────────────────────────────────────────────

    async def subscribe_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        self._ensure_connected()
        while True:
            try:
                ticker = await self._exchange.watch_ticker(symbol)
                yield ticker
            except Exception as exc:
                log.warning("ticker ws error for %s: %s — reconnecting", symbol, exc)
                await asyncio.sleep(1)

    async def subscribe_orderbook(self, symbol: str) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        while True:
            try:
                raw = await self._exchange.watch_order_book(symbol)
                yield OrderBook(
                    exchange=self.exchange_id,
                    symbol=symbol,
                    bids=[OrderBookLevel(price=p, quantity=q) for p, q in raw["bids"]],
                    asks=[OrderBookLevel(price=p, quantity=q) for p, q in raw["asks"]],
                    timestamp=datetime.fromtimestamp(
                        raw["timestamp"] / 1000, tz=timezone.utc
                    )
                    if raw.get("timestamp")
                    else datetime.now(tz=timezone.utc),
                )
            except Exception as exc:
                log.warning("orderbook ws error for %s: %s — reconnecting", symbol, exc)
                await asyncio.sleep(1)

    async def subscribe_trades(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        self._ensure_connected()
        while True:
            try:
                trades = await self._exchange.watch_trades(symbol)
                for trade in trades:
                    yield trade
            except Exception as exc:
                log.warning("trades ws error for %s: %s — reconnecting", symbol, exc)
                await asyncio.sleep(1)
