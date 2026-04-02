"""Order domain models."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    """Intent to place an order — created by a strategy, consumed by execution."""

    strategy_id: str
    exchange: str
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["limit", "market", "stop_limit", "stop_market"]
    quantity: float = Field(gt=0)
    price: float | None = None
    post_only: bool = True
    reduce_only: bool = False
    stop_loss: float | None = None
    take_profit: float | None = None
    request_id: str = Field(default_factory=lambda: uuid.uuid4().hex)

    model_config = {"frozen": True}


class OrderResult(BaseModel):
    """Acknowledgement returned after an order is processed by the exchange."""

    request_id: str
    order_id: str
    status: Literal[
        "new",
        "partially_filled",
        "filled",
        "cancelled",
        "rejected",
        "expired",
    ]
    filled_qty: float = 0.0
    filled_price: float | None = None
    fee: float = 0.0
    fee_currency: str = "USDT"
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"frozen": True}

    @property
    def is_terminal(self) -> bool:
        return self.status in {"filled", "cancelled", "rejected", "expired"}
