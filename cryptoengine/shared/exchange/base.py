"""Abstract base class every exchange connector must implement."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from shared.models.market import FundingRate, OHLCV, OrderBook
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position


class ExchangeConnector(ABC):
    """Unified async interface for CEX interaction."""

    exchange_id: str

    # ── lifecycle ────────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    # ── market data ─────────────────────────────────────────────────────

    @abstractmethod
    async def get_ticker(self, symbol: str) -> dict[str, Any]: ...

    @abstractmethod
    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook: ...

    @abstractmethod
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int = 100,
    ) -> list[OHLCV]: ...

    @abstractmethod
    async def get_funding_rate(self, symbol: str) -> FundingRate: ...

    # ── trading ──────────────────────────────────────────────────────────

    @abstractmethod
    async def place_order(self, order: OrderRequest) -> OrderResult: ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool: ...

    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage for a symbol. Must be called before placing futures orders."""
        ...

    @abstractmethod
    async def set_margin_mode(self, symbol: str, mode: str = "isolated") -> None:
        """Set margin mode for a symbol. Use 'isolated' to limit loss to position margin."""
        ...

    # ── account ──────────────────────────────────────────────────────────

    @abstractmethod
    async def get_position(self, symbol: str) -> Position | None: ...

    @abstractmethod
    async def get_balance(self) -> dict[str, float]: ...

    # ── websocket streams ────────────────────────────────────────────────

    @abstractmethod
    async def subscribe_ticker(self, symbol: str) -> AsyncIterator[dict[str, Any]]: ...

    @abstractmethod
    async def subscribe_orderbook(self, symbol: str) -> AsyncIterator[OrderBook]: ...

    @abstractmethod
    async def subscribe_trades(self, symbol: str) -> AsyncIterator[dict[str, Any]]: ...
