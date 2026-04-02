"""Position and portfolio state models."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class Position(BaseModel):
    """A single open futures position."""

    exchange: str
    symbol: str
    side: Literal["long", "short"]
    size: float = Field(ge=0)
    entry_price: float = Field(gt=0)
    unrealized_pnl: float = 0.0
    leverage: float = Field(gt=0, le=125)
    liquidation_price: float | None = None
    margin_used: float = 0.0

    @property
    def notional(self) -> float:
        return self.size * self.entry_price

    @property
    def margin_ratio(self) -> float:
        if self.margin_used == 0:
            return 0.0
        return self.unrealized_pnl / self.margin_used


class StrategySnapshot(BaseModel):
    """Per-strategy slice inside a portfolio snapshot."""

    strategy_id: str
    allocated_capital: float
    current_pnl: float
    position_count: int


class PortfolioState(BaseModel):
    """Aggregate portfolio state used by risk / kill-switch."""

    total_equity: float
    unrealized_pnl: float = 0.0
    realized_pnl_today: float = 0.0
    daily_drawdown: float = 0.0
    weekly_drawdown: float = 0.0
    strategies: list[StrategySnapshot] = Field(default_factory=list)
    kill_switch_triggered: bool = False
    snapshot_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def total_pnl(self) -> float:
        return self.unrealized_pnl + self.realized_pnl_today
