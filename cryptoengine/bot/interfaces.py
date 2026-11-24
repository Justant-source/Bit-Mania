"""Contracts between the bot's modules.

The bot is one asyncio process. Modules call each other directly through
these protocols; there is no message bus. Tests substitute fakes for
``Exchange`` and ``Notifier``.

Module ownership:
    exchange.py, execution.py, safety.py   order path
    strategy.py, risk.py, state.py, db.py, app.py, main.py, cli.py   decisions and lifecycle
    notify.py, telegram_bot.py, report.py, logging_setup.py   Telegram
    web.py   dashboard
    replay_data.py   Binance spot candles for the replay backtest
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from shared.models.market import OrderBook
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position

SYMBOL = "BTC/USDT:USDT"
EXCHANGE = "bybit"
STRATEGY_ID = "supertrend-01"
TIMEFRAME = "4h"
BAR_MS = 14_400_000


# ── exchange ────────────────────────────────────────────────────────────────


@runtime_checkable
class Exchange(Protocol):
    """The subset of the Bybit connector the bot uses. REST only."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_ticker(self, symbol: str) -> dict[str, Any]: ...
    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook: ...
    async def get_ohlcv(
        self, symbol: str, timeframe: str = "4h", since: int | None = None, limit: int = 1000
    ) -> list[Any]: ...
    async def place_order(self, order: OrderRequest) -> OrderResult: ...
    async def cancel_order(self, order_id: str, symbol: str) -> bool: ...
    async def fetch_order_status(self, order_id: str, symbol: str) -> str: ...
    async def fetch_order_result(self, order_id: str, symbol: str, request_id: str) -> OrderResult: ...
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...
    async def set_margin_mode(self, symbol: str, mode: str = "isolated") -> None: ...
    async def get_position(self, symbol: str) -> Position | None: ...
    async def get_balance(self) -> dict[str, float]: ...
    async def get_min_order_sizes(self, symbols: list[str]) -> dict[str, dict[str, float]]: ...
    def price_to_precision(self, symbol: str, price: float) -> float: ...


# ── order path ──────────────────────────────────────────────────────────────


@dataclass
class ExecutionResult:
    """Terminal outcome of one order request, after re-pegs and any market fallback."""

    request_id: str
    status: str  # "filled" | "partial" | "cancelled" | "rejected"
    filled_qty: float = 0.0
    filled_price: float | None = None
    fee: float = 0.0
    reason: str = ""  # rejection or cancel reason; "" when filled
    order_ids: list[str] = field(default_factory=list)

    @property
    def is_filled(self) -> bool:
        return self.status == "filled"

    @property
    def any_fill(self) -> bool:
        return self.filled_qty > 0


class EntryGate(Protocol):
    """Decides whether orders that open or add risk may go out.

    Reduce-only orders never consult the gate: a close must always be possible.
    """

    def entries_blocked(self) -> tuple[bool, str]: ...


class Executor(Protocol):
    async def start(self) -> None:
        """Cancel orphaned open orders left by a previous run."""

    async def execute(
        self, order: OrderRequest, *, signal_ts: datetime | None = None
    ) -> ExecutionResult:
        """Run safety checks, then work the order to a terminal state.

        Blocks for at most the re-peg budget (about 200s) plus the market
        fallback. Never raises for a rejected or unfilled order; it returns
        an ExecutionResult with the reason.
        """

    async def get_position(self) -> Position | None:
        """Position as the exchange reports it now. None when flat."""

    async def get_balance(self) -> dict[str, float]:
        """{"total": equity, "free": free margin, "wallet": wallet balance}."""

    async def close_position(self, reason: str) -> ExecutionResult | None:
        """Close the whole position with a reduce-only market order.

        Retries until the exchange reports flat or the retry budget ends.
        Returns None when there was nothing to close.
        """


# ── notifications ───────────────────────────────────────────────────────────


class Notifier(Protocol):
    """Sends operator messages to Telegram. Must never raise."""

    async def send(self, html: str, *, dedup_key: str | None = None) -> None: ...

    async def entry(self, *, qty: float, price: float, stop_loss: float | None,
                    equity: float | None) -> None: ...

    async def exit(self, *, qty: float, entry_price: float | None, exit_price: float,
                   pnl: float | None, reason: str, hold: str | None) -> None: ...

    async def order_rejected(self, *, side: str, reason: str, will_retry: bool) -> None: ...

    async def kill_switch(self, *, level: str, reason: str, action: str,
                          daily_dd_pct: float, weekly_dd_pct: float) -> None: ...

    async def anomaly(self, *, kind: str, message: str) -> None: ...


# ── operator control ────────────────────────────────────────────────────────


class BotControl(Protocol):
    """What Telegram commands and the CLI may ask the running bot to do."""

    async def status(self) -> dict[str, Any]:
        """Keys: equity, position (dict or None), halted, halt_reason,
        kill_level, last_bar_ts, last_signal (dict or None), next_bar_at,
        daily_dd_pct, weekly_dd_pct, monthly_dd_pct, heartbeat_at."""

    async def emergency_close(self, requested_by: str) -> dict[str, Any]:
        """Halt new entries, then close any position. Called only after the
        operator has confirmed. Keys: had_position, closed, flat, detail."""

    async def resume(self, requested_by: str) -> dict[str, Any]:
        """Clear a manual halt. Keys: was_halted, halted."""
