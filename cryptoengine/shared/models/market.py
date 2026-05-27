"""Market-data domain models."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class FundingRate(BaseModel):
    """Perpetual-swap funding rate snapshot."""

    exchange: str
    symbol: str
    rate: float
    predicted_rate: float | None = None
    next_funding_time: datetime
    collected_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"frozen": True}


class OHLCV(BaseModel):
    """Single candlestick bar."""

    exchange: str
    symbol: str
    timeframe: str  # e.g. "1m", "5m", "1h"
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime

    model_config = {"frozen": True}

    @property
    def mid(self) -> float:
        return (self.high + self.low) / 2.0

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open


class OrderBookLevel(BaseModel):
    """Single price/quantity level."""

    price: float
    quantity: float


class OrderBook(BaseModel):
    """L2 order-book snapshot."""

    exchange: str
    symbol: str
    bids: list[OrderBookLevel] = Field(default_factory=list)
    asks: list[OrderBookLevel] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def mid_price(self) -> float | None:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_ask + self.best_bid) / 2.0
        return None
