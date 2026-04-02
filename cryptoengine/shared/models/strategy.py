"""Strategy lifecycle models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class StrategyCommand(BaseModel):
    """Command sent to a strategy instance (start / stop / reconfigure)."""

    strategy_id: str
    action: Literal["start", "stop", "pause", "resume", "reconfigure"]
    allocated_capital: float | None = None
    max_drawdown: float | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class StrategyStatus(BaseModel):
    """Heartbeat / status emitted by a running strategy."""

    strategy_id: str
    is_running: bool
    allocated_capital: float = 0.0
    current_pnl: float = 0.0
    position_count: int = 0
    last_tick: datetime = Field(default_factory=datetime.utcnow)
