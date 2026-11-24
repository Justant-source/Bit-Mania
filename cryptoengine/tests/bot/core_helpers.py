"""Shared helpers for the core-module tests (strategy, risk, state, db, app).

Plain module (tests/bot has no __init__.py): test files do ``from core_helpers import ...``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT / "tests"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import asyncpg  # noqa: E402
import pytest_asyncio  # noqa: E402

import bot.execution as _ex  # noqa: E402
from bot.db import Database  # noqa: E402
from bot.interfaces import BAR_MS, ExecutionResult  # noqa: E402
from shared.models.position import Position  # noqa: E402

PG = dict(host="127.0.0.1", port=55432, user="postgres", password="test")
SYM = "BTC/USDT:USDT"
TABLES = ("orders", "positions", "supertrend_signals", "portfolio_snapshots", "ohlcv_history",
          "bot_state", "replay_ohlcv_4h", "replay_reference_trades")


async def ensure_database(name: str) -> None:
    admin = await asyncpg.connect(database="postgres", **PG)
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name):
            await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


@pytest_asyncio.fixture
async def db():
    await ensure_database("test_core")
    d = Database(host=PG["host"], port=PG["port"], name="test_core",
                 user=PG["user"], password=PG["password"], max_size=6)
    await d.connect()
    await d.ensure_schema()
    async with d.pool.acquire() as c:
        await c.execute("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY")
    yield d
    await d.close()


class FakeClock:
    """Virtual time: sleep() advances it instantly (and yields to the loop)."""

    def __init__(self, start: datetime | None = None) -> None:
        self.t = start or datetime(2026, 11, 20, 8, 0, 10, tzinfo=timezone.utc)
        self.sleeps: list[float] = []
        self.on_sleep = None  # optional callable(clock) run after each sleep
        # Gate for the trader's same-bar retry sleep: a sleep of exactly ``gate_s`` seconds
        # waits for a permit (see settle()), so a test decides when the retry may fire.
        self.gate_s: float | None = None
        self.sem = asyncio.Semaphore(0)

    def now(self) -> datetime:
        return self.t

    def set(self, t: datetime) -> None:
        self.t = t

    def advance(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if self.gate_s is not None and seconds == self.gate_s:
            await self.sem.acquire()
        self.t += timedelta(seconds=max(seconds, 0.0))
        await asyncio.sleep(0)
        if self.on_sleep is not None:
            self.on_sleep(self)


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def make_rows(n: int, end_open_ms: int, *, drift: float = 0.004, start: float = 20_000.0,
              tail_drift: float | None = None, tail: int = 0) -> list[list[float]]:
    """Raw ccxt rows [ts, o, h, l, c, v], oldest first, last bar opens at end_open_ms.

    Smoothly rising by default (entry conditions hold); the last ``tail`` bars use
    ``tail_drift`` instead (a sharp fall triggers the EMA cross exit)."""
    rows: list[list[float]] = []
    price = start
    for i in range(n):
        d = tail_drift if (tail_drift is not None and i >= n - tail) else drift
        o = price
        price = price * (1 + d) * (1 + 0.004 * math.sin(i * 0.7)) / (1 + 0.004 * math.sin((i - 1) * 0.7))
        c = price
        rows.append([end_open_ms - (n - 1 - i) * BAR_MS, o, max(o, c) * 1.002, min(o, c) * 0.998, c, 100.0])
    return rows


def rows_to_candles(rows: list[list[float]]) -> list[dict]:
    return [{"ts": int(r[0]), "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows]


def pos(size: float = 0.01, entry: float = 100_000.0, side: str = "long") -> Position:
    return Position(exchange="bybit", symbol=SYM, side=side, size=size, entry_price=entry,
                    leverage=3.0, unrealized_pnl=1.5)


class ScriptedExecutor:
    """Executor double: results are popped from ``results``; position/balance are plain attrs."""

    def __init__(self) -> None:
        self.results: list[ExecutionResult] = []
        self.orders = []
        self.position: Position | None = None
        self.balance = {"total": 1000.0, "free": 1000.0, "used": 0.0, "wallet": 1000.0}
        self.pos_failures = 0
        self.balance_fail = False
        self.on_execute = None
        self.closed: list[str] = []
        self.close_result: ExecutionResult | None = None

    async def start(self) -> None: ...

    async def execute(self, order, *, signal_ts=None):
        self.orders.append(order)
        r = self.results.pop(0) if self.results else ExecutionResult(order.request_id, "rejected",
                                                                     reason="scripted_empty")
        r = dataclasses.replace(r, request_id=order.request_id)
        if self.on_execute is not None:
            self.on_execute(order, r)
        return r

    async def get_position(self):
        if self.pos_failures > 0:
            self.pos_failures -= 1
            raise ConnectionError("down")
        return self.position

    async def get_balance(self):
        if self.balance_fail:
            raise ConnectionError("down")
        return dict(self.balance)

    async def close_position(self, reason):
        self.closed.append(reason)
        self.position = None
        return self.close_result


def filled(qty: float, price: float = 100_000.0, fee: float = 0.1) -> ExecutionResult:
    return ExecutionResult("x", "filled", filled_qty=qty, filled_price=price, fee=fee)


def rejected(reason: str = "post_only_failed") -> ExecutionResult:
    return ExecutionResult("x", "rejected", reason=reason)


def partial(qty: float, price: float = 100_000.0) -> ExecutionResult:
    return ExecutionResult("x", "partial", filled_qty=qty, filled_price=price, reason="partial")


def fast_executor_timings(monkeypatch) -> None:
    monkeypatch.setattr(_ex, "REPEG_INTERVAL_S", 0.05)
    monkeypatch.setattr(_ex, "REPEG_POLL_S", 0.01)
    monkeypatch.setattr(_ex, "REPEG_ERROR_SLEEP_S", 0.01)
    monkeypatch.setattr(_ex, "RETRY_BACKOFF_S", 0.01)
    monkeypatch.setattr(_ex, "SETTLE_POLL_S", 0.01)
    monkeypatch.setattr(_ex, "CLOSE_RETRY_PAUSE_S", 0.01)


async def settle(trader, rounds: int = 1) -> None:
    """Let the pending same-bar retry fire ``rounds`` times (needs clock.gate_s), awaiting each."""
    for _ in range(rounds):
        await asyncio.sleep(0)
        t = trader._retry_task
        if t is None or t.done():
            break
        trader._clock.sem.release()
        await t


def build_stack(db, *, clock=None, fx=None, scripted: ScriptedExecutor | None = None,
                max_capital: float = 1000.0, retry_delay: float = 60.0, dry_run: bool = False,
                risk_cfg=None, replay_sync=None, report_fn=None, heartbeat_path=None):
    """Wire the whole bot against fakes. With ``scripted`` the order path is a
    ScriptedExecutor, otherwise the real OrderExecutor + SafetyGuard on a FakeExchange."""
    from types import SimpleNamespace

    from bot.app import BotApp
    from bot.execution import OrderExecutor
    from bot.risk import RiskConfig, RiskManager
    from bot.safety import SafetyGuard
    from bot.state import BotState
    from bot.strategy import Trader
    from fakes.exchange import FakeExchange
    from fakes.notifier import FakeNotifier

    clock = clock or FakeClock()
    fx = fx or FakeExchange(wallet=1000.0)
    notifier = FakeNotifier()
    state = BotState(db, clock=clock)
    if scripted is not None:
        executor = scripted
    else:
        executor = OrderExecutor(fx, db, SafetyGuard(fx, state, leverage_limit=3.0, cache_ttl=0.0), dry_run=dry_run)
    trader = Trader(executor, db, state, notifier, max_capital_usd=max_capital, clock=clock,
                    retry_delay_s=retry_delay)
    risk = RiskManager(db, state, executor, notifier, config=risk_cfg or RiskConfig(), clock=clock,
                       on_position_closed=trader.after_forced_close)
    app = BotApp(db=db, exchange=fx, executor=executor, notifier=notifier, state=state, trader=trader,
                 risk=risk, clock=clock, report_fn=report_fn, replay_sync=replay_sync,
                 heartbeat_path=heartbeat_path)
    trader.on_fill = app.snapshot_after_fill
    clock.gate_s = retry_delay if (hasattr(clock, "gate_s") and retry_delay < 1) else None
    return SimpleNamespace(fx=fx, executor=executor, notifier=notifier, state=state, trader=trader,
                           risk=risk, app=app, clock=clock)
