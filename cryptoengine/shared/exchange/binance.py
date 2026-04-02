"""Binance futures connector — placeholder for future implementation."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from shared.exchange.base import ExchangeConnector
from shared.models.market import FundingRate, OHLCV, OrderBook
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position


class BinanceConnector(ExchangeConnector):
    """Placeholder — will mirror BybitConnector once Binance support is added."""

    exchange_id: str = "binance"

    def __init__(self, **kwargs: Any) -> None:
        self._config = kwargs

    async def connect(self) -> None:
        raise NotImplementedError("BinanceConnector is not yet implemented")

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def get_ticker(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError

    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook:
        raise NotImplementedError

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int = 100,
    ) -> list[OHLCV]:
        raise NotImplementedError

    async def get_funding_rate(self, symbol: str) -> FundingRate:
        raise NotImplementedError

    async def place_order(self, order: OrderRequest) -> OrderResult:
        raise NotImplementedError

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        raise NotImplementedError

    async def get_position(self, symbol: str) -> Position | None:
        raise NotImplementedError

    async def get_balance(self) -> dict[str, float]:
        raise NotImplementedError

    async def subscribe_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError
        yield  # type: ignore[misc]

    async def subscribe_orderbook(self, symbol: str) -> AsyncIterator[OrderBook]:
        raise NotImplementedError
        yield  # type: ignore[misc]

    async def subscribe_trades(self, symbol: str) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError
        yield  # type: ignore[misc]
