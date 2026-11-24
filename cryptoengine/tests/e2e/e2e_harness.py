"""Harness for the end-to-end tests.

The REAL modules are wired exactly like ``bot/main.py::amain`` wires them
(Database, BotState, SafetyGuard, OrderExecutor, Trader, RiskManager, BotApp,
TelegramCommands, build_daily_report, replay-less web server, logging ERROR ->
notifier.anomaly). Only these are substituted:

* the exchange            -> ``tests/fakes/exchange.py::FakeExchange``
* Telegram notifications  -> ``E2ENotifier`` (FakeNotifier + tolerant ``send``)
* Telegram command input  -> fake ``Update`` objects fed to the real TelegramCommands
* the clock               -> ``E2EClock`` (virtual time), injected where main.py
  would pass nothing (main.py leaves every ``clock=`` at its SystemClock default;
  the constructors all take ``clock=`` so no production code is bypassed).

``shared.kill_switch`` reads ``datetime.now`` directly (not an injectable clock);
``patch_kill_switch_clock`` swaps the module-level ``datetime`` name for a shim
bound to the virtual clock so its cooldown agrees with the bot's clock.
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import random
import socket
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT / "tests"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import asyncpg  # noqa: E402

import bot.execution as _ex  # noqa: E402
from bot.interfaces import BAR_MS, SYMBOL  # noqa: E402
from fakes.exchange import FakeExchange  # noqa: E402
from fakes.notifier import FakeNotifier  # noqa: E402

PG = dict(host="127.0.0.1", port=55432, user="postgres", password="test")
DB_NAME = "test_e2e"
TABLES = ("orders", "positions", "supertrend_signals", "portfolio_snapshots", "ohlcv_history",
          "bot_state", "replay_ohlcv_4h", "replay_reference_trades")
CHAT = "555"
DASH_TOKEN = "dash-secret"


async def ensure_database(name: str = DB_NAME) -> None:
    admin = await asyncpg.connect(database="postgres", **PG)
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name):
            await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


# ── virtual time ────────────────────────────────────────────────────────────


def floor_boundary(dt: datetime) -> datetime:
    ms = int(dt.timestamp() * 1000)
    return datetime.fromtimestamp((ms // BAR_MS) * BAR_MS / 1000, tz=timezone.utc)


class E2EClock:
    """Virtual clock. ``sleep`` advances time instantly (and yields).

    The trader's same-bar retry sleeps exactly ``retry_s`` seconds; while
    ``hold_retries`` is set that sleep blocks until ``release_retry()`` so a test
    decides when the retry may fire (everything else, e.g. the 1s/2s position
    read backoff, just advances time).
    """

    def __init__(self, start: datetime, *, retry_s: float = 60.0) -> None:
        self.t = start
        self.retry_s = retry_s
        self.hold_retries = True
        self.gate_pred = None  # callable() -> bool: is the current task the trader's retry task?
        self.on_sleep = None   # callable(clock) run after every sleep (feed publishing, stop conditions)
        self.sleeps: list[float] = []
        self._sem = asyncio.Semaphore(0)

    def now(self) -> datetime:
        return self.t

    def set(self, t: datetime) -> None:
        self.t = t

    def advance(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)

    def release_retry(self, n: int = 1) -> None:
        for _ in range(n):
            self._sem.release()

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if self.hold_retries and seconds == self.retry_s and (self.gate_pred is None or self.gate_pred()):
            await self._sem.acquire()
        self.t += timedelta(seconds=max(seconds, 0.0))
        await asyncio.sleep(0)
        if self.on_sleep is not None:
            self.on_sleep(self)


def patch_kill_switch_clock(monkeypatch, clock: E2EClock) -> None:
    import shared.kill_switch as ks

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            t = clock.now()
            return t if tz is None else t.astimezone(tz)

    monkeypatch.setattr(ks, "datetime", _DT)


def fast_executor_timings(monkeypatch) -> None:
    """Shrink the executor's real-time waits (20 re-pegs x 10 s would take minutes)."""
    monkeypatch.setattr(_ex, "REPEG_INTERVAL_S", 0.03)
    monkeypatch.setattr(_ex, "REPEG_POLL_S", 0.005)
    monkeypatch.setattr(_ex, "REPEG_ERROR_SLEEP_S", 0.001)
    monkeypatch.setattr(_ex, "RETRY_BACKOFF_S", 0.001)
    monkeypatch.setattr(_ex, "SETTLE_POLL_S", 0.001)
    monkeypatch.setattr(_ex, "CLOSE_RETRY_PAUSE_S", 0.001)
    monkeypatch.setattr(_ex, "PERSIST_BACKOFF_S", 0.001)


# ── notifier ────────────────────────────────────────────────────────────────


class E2ENotifier(FakeNotifier):
    """FakeNotifier + what the real TelegramNotifier accepts (reply_markup, close)."""

    async def send(self, html: str, *, dedup_key: str | None = None, reply_markup: Any = None,
                   **_: Any) -> None:
        self._rec("send", html=html, dedup_key=dedup_key, reply_markup=reply_markup)

    async def close(self) -> None:
        return None

    def anomalies(self, kind: str | None = None) -> list[dict[str, Any]]:
        return [a for a in self.of("anomaly") if kind is None or a["kind"] == kind]

    def texts(self) -> list[str]:
        return [c["html"] for c in self.of("send")]

    def mark(self) -> int:
        return len(self.calls)

    def since(self, mark: int, name: str) -> list[dict[str, Any]]:
        return [kw for n, kw in self.calls[mark:] if n == name]


# ── candle generation ───────────────────────────────────────────────────────


def _bar(prev_close: float, c: float, rng: random.Random, wick: float = 0.002):
    o = prev_close
    return (o, max(o, c) * (1 + wick * rng.random()), min(o, c) * (1 - wick * rng.random()), c)


def synth_series(n_down: int = 350, seed: int = 7, start: float = 80_000.0) -> list[tuple]:
    """Gentle noisy decline (supertrend down, no entry) used as the common prefix."""
    rng = random.Random(seed)
    bars, p = [], start
    for _ in range(n_down):
        c = p * (1 - 0.001 + rng.uniform(-0.003, 0.003))
        bars.append(_bar(p, c, rng))
        p = c
    return bars


def to_candles(bars: list[tuple], ts0: int) -> list[dict[str, Any]]:
    return [{"ts": ts0 + i * BAR_MS, "open": b[0], "high": b[1], "low": b[2], "close": b[3],
             "volume": 100.0} for i, b in enumerate(bars)]


def extend(bars: list[tuple], pct: float, n: int = 1, seed: int = 99) -> list[tuple]:
    """Append ``n`` bars each moving ``pct`` from the previous close (deterministic wicks)."""
    rng = random.Random(seed + len(bars))
    out = list(bars)
    for _ in range(n):
        p = out[-1][3]
        out.append(_bar(p, p * (1 + pct), rng))
    return out


def first_entry_index(bars: list[tuple], *, step: float = 0.012, limit: int = 60):
    """Rally from the prefix until strategy.evaluate() first reports entry_ok.

    Returns (bars_including_entry_bar, entry_index)."""
    from bot.strategy import evaluate

    cur = list(bars)
    for _ in range(limit):
        cur = extend(cur, step)
        cs = to_candles(cur, 0)
        s = evaluate(cs, has_position=False)
        if s is not None and s.entry_ok:
            return cur, len(cur) - 1
    raise AssertionError("synthetic rally never produced an entry signal")


def evaluate_at(bars: list[tuple], **kw):
    from bot.strategy import evaluate

    return evaluate(to_candles(bars, 0), **kw)


# ── feed: publishes candles to the FakeExchange as virtual time passes ──────


class Feed:
    """Bybit-like candle feed: ``get_ohlcv`` returns every closed bar plus the
    in-progress one as the last row. Bars become visible ``delay`` seconds after
    their close (default 0). The book/last follow the latest closed bar's close."""

    def __init__(self, fx: FakeExchange, candles: list[dict[str, Any]], *, spread: float = 0.1,
                 follow_price: bool = True) -> None:
        self.fx = fx
        self.candles = candles
        self.spread = spread
        self.follow_price = follow_price
        self.delay: dict[int, float] = {}
        self.visible_until: int | None = None  # index limit (exclusive) for hand control

    def publish(self, now: datetime) -> None:
        now_ms = now.timestamp() * 1000
        confirmed = []
        stale = False  # a bar that has closed but the exchange does not serve yet
        for c in self.candles:
            d = self.delay.get(c["ts"], 0.0) * 1000
            if c["ts"] + BAR_MS + d <= now_ms:
                confirmed.append(c)
            else:
                stale = c["ts"] + BAR_MS <= now_ms
                break
        if self.visible_until is not None:
            confirmed = confirmed[: self.visible_until]
        rows = [[c["ts"], c["open"], c["high"], c["low"], c["close"], c["volume"]] for c in confirmed]
        if confirmed:
            last = confirmed[-1]
            nxt_ts = last["ts"] + BAR_MS
            if not stale:  # Bybit's last row is the in-progress bar
                rows.append([nxt_ts, last["close"], last["close"], last["close"], last["close"], 1.0])
            if self.follow_price:
                px = last["close"]
                self.fx.set_book(bid=round(px, 1), ask=round(px + self.spread, 1))
                self.fx.set_last(px)
        self.fx.set_candles(rows)

    def close_of(self, ts: int) -> float:
        return next(c["close"] for c in self.candles if c["ts"] == ts)


def make_feed(fx: FakeExchange, bars: list[tuple], *, end: datetime | None = None, **kw) -> Feed:
    """Place ``bars`` so the last one closes at ``end`` (default: latest real 4h boundary)."""
    end = end or floor_boundary(datetime.now(timezone.utc))
    ts0 = int(end.timestamp() * 1000) - len(bars) * BAR_MS
    return Feed(fx, to_candles(bars, ts0), **kw)


# ── database fault injection ────────────────────────────────────────────────


class FlakyPool:
    """Wraps the asyncpg pool; while ``down`` every acquire() raises like a dead DB."""

    def __init__(self, real) -> None:
        self._real = real
        self.down = False
        self.failures = 0

    def acquire(self, *a, **k):
        if self.down:
            self.failures += 1
            raise ConnectionError("db down (injected)")
        return self._real.acquire(*a, **k)

    def __getattr__(self, name):
        return getattr(self._real, name)


# ── telegram fakes (command side) ───────────────────────────────────────────


class Msg:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def reply_text(self, text, **kw):
        self.sent.append((text, kw))


class Query:
    def __init__(self, data: str) -> None:
        self.data = data
        self.edits: list[str] = []
        self.answered = False

    async def answer(self, *a, **k):
        self.answered = True

    async def edit_message_text(self, text, **kw):
        self.edits.append(text)


def cmd_update(chat: str = CHAT, user: int = 42):
    return SimpleNamespace(effective_chat=SimpleNamespace(id=int(chat)), effective_message=Msg(),
                           effective_user=SimpleNamespace(id=user), callback_query=None)


def cb_update(data: str, chat: str = CHAT, user: int = 42):
    q = Query(data)
    return SimpleNamespace(effective_chat=SimpleNamespace(id=int(chat)), effective_message=None,
                           effective_user=SimpleNamespace(id=user), callback_query=q), q


def buttons(update) -> list[str]:
    kb = update.effective_message.sent[-1][1]["reply_markup"].to_dict()["inline_keyboard"][0]
    return [b["callback_data"] for b in kb]


class MonoClock:
    """Mutable monotonic clock for TelegramCommands' 60 s confirm window."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


# ── the stack ───────────────────────────────────────────────────────────────


@dataclasses.dataclass
class Stack:
    db: Any
    fx: FakeExchange
    clock: E2EClock
    notifier: E2ENotifier
    state: Any
    safety: Any
    executor: Any
    trader: Any
    risk: Any
    app: Any
    commands: Any
    mono: MonoClock
    feed: Feed | None = None
    web: Any = None

    # -- driving ------------------------------------------------------------

    def publish(self) -> None:
        if self.feed is not None:
            self.feed.publish(self.clock.now())

    async def flush(self, n: int = 5) -> None:
        """Let fire-and-forget tasks (log->notifier alerts, retry tasks) run."""
        for _ in range(n):
            await asyncio.sleep(0)

    async def at(self, t: datetime) -> None:
        """Jump to ``t``, publish candles, run one scheduler tick."""
        self.clock.set(t)
        self.publish()
        await self.app.tick()
        await self.flush()

    async def run_until(self, end: datetime, *, max_step: float | None = None) -> None:
        """What ``BotApp.run`` does, minus the real sleeping: tick, then advance by ``_tick_delay``."""
        while self.clock.now() < end:
            self.publish()
            await self.app.tick()
            await self.flush(2)
            step = self.app._tick_delay()
            if max_step is not None:
                step = min(step, max_step)
            self.clock.advance(min(step, (end - self.clock.now()).total_seconds()) or 0.1)
        self.publish()

    async def bar(self, idx: int) -> None:
        """Advance to just after bar ``idx`` closed (+3 s) and run one tick."""
        c = self.feed.candles[idx]
        await self.at(datetime.fromtimestamp((c["ts"] + BAR_MS) / 1000 + 3, tz=timezone.utc))

    def bar_close_time(self, idx: int) -> datetime:
        return datetime.fromtimestamp((self.feed.candles[idx]["ts"] + BAR_MS) / 1000, tz=timezone.utc)

    # -- db helpers ---------------------------------------------------------

    async def fetch(self, sql: str, *args):
        async with self.db.pool.acquire() as c:
            return [dict(r) for r in await c.fetch(sql, *args)]

    async def orders(self, where: str = "TRUE"):
        return await self.fetch(f"SELECT * FROM orders WHERE {where} ORDER BY id")

    async def positions(self):
        return await self.fetch("SELECT * FROM positions ORDER BY id")

    async def signals(self):
        return await self.fetch("SELECT *, (EXTRACT(EPOCH FROM bar_ts)*1000)::BIGINT AS ts_ms "
                                "FROM supertrend_signals ORDER BY bar_ts")

    async def snapshots(self):
        return await self.fetch("SELECT * FROM portfolio_snapshots ORDER BY snapshot_at, id")

    async def bot_state(self) -> dict[str, Any]:
        import json

        out = {}
        for r in await self.fetch("SELECT key, value FROM bot_state"):
            v = r["value"]
            out[r["key"]] = json.loads(v) if isinstance(v, str) else v
        return out

    # -- telegram -----------------------------------------------------------

    async def telegram_emergency_close(self, *, press: bool = True):
        upd = cmd_update()
        await self.commands.cmd_emergency_close(upd)
        yes, _no = buttons(upd)
        if not press:
            return upd, None
        cb, q = cb_update(yes)
        await self.commands.on_callback(cb)
        return upd, q

    async def telegram_resume(self, *, press: bool = True):
        upd = cmd_update()
        await self.commands.cmd_resume(upd)
        if not upd.effective_message.sent or "reply_markup" not in upd.effective_message.sent[-1][1] \
                or upd.effective_message.sent[-1][1]["reply_markup"] is None:
            return upd, None
        yes, _no = buttons(upd)
        if not press:
            return upd, None
        cb, q = cb_update(yes)
        await self.commands.on_callback(cb)
        return upd, q


async def build_stack(
    db, fx: FakeExchange, clock: E2EClock, *, notifier: E2ENotifier | None = None,
    risk_cfg=None, max_capital: float = 1000.0, dry_run: bool = False, feed: Feed | None = None,
    housekeeping_s: float = 900.0, heartbeat_path: str | None = None,
    dashboard_url: str = "https://dash.example/?t=" + DASH_TOKEN, retry_delay: float = 60.0,
    attach_logs: bool = True, replay_sync=None,
) -> Stack:
    """Construct the stack exactly as bot/main.py::amain does (see module docstring)."""
    from bot.app import BotApp
    from bot.execution import OrderExecutor
    from bot.logging_setup import attach_notifier, setup_logging
    from bot.report import build_daily_report, report_keyboard
    from bot.risk import RiskConfig, RiskManager
    from bot.safety import SafetyGuard
    from bot.state import BotState
    from bot.strategy import Trader
    from bot.telegram_bot import TelegramCommands

    notifier = notifier or E2ENotifier()
    if attach_logs:
        setup_logging("WARNING")
        attach_notifier(notifier)
    cfg = risk_cfg or RiskConfig.from_yaml(_ROOT / "config" / "bot.yaml")

    state = BotState(db, clock=clock)
    safety = SafetyGuard(fx, state, leverage_limit=3.0, clock=lambda: clock.now().timestamp())
    executor = OrderExecutor(fx, db, safety, dry_run=dry_run)
    trader = Trader(executor, db, state, notifier, max_capital_usd=max_capital, clock=clock,
                    retry_delay_s=retry_delay)
    risk = RiskManager(db, state, executor, notifier, config=cfg, clock=clock,
                       on_position_closed=trader.after_forced_close)
    holder: dict[str, Any] = {}

    async def report_fn():
        html = await build_daily_report(db, holder["app"])
        return html, (report_keyboard(dashboard_url) if dashboard_url else None)

    app = BotApp(db=db, exchange=fx, executor=executor, notifier=notifier, state=state,
                 trader=trader, risk=risk, clock=clock, report_fn=report_fn, replay_sync=replay_sync,
                 heartbeat_path=heartbeat_path, housekeeping_s=housekeeping_s)
    holder["app"] = app
    trader.on_fill = app.snapshot_after_fill
    clock.gate_pred = lambda: asyncio.current_task() is trader._retry_task
    mono = MonoClock()
    commands = TelegramCommands("tok", CHAT, app, db, dashboard_url=dashboard_url, clock=mono)
    st = Stack(db=db, fx=fx, clock=clock, notifier=notifier, state=state, safety=safety,
               executor=executor, trader=trader, risk=risk, app=app, commands=commands,
               mono=mono, feed=feed)
    st.publish()
    return st


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def approx_qty(capital: float, price: float, step: float = 0.001) -> float:
    q = capital * 0.95 * 3 / price
    return math.floor((q + 1e-12) / step) * step


# ── canned signal series (deterministic) ────────────────────────────────────

_PLAN_CACHE: dict[str, Any] = {}


def entry_plan() -> dict[str, Any]:
    """Prefix with no entry signal followed by a rally whose last bar is the first entry bar.

    ``bars``   all bars up to and including the entry bar
    ``idx``    index of the entry bar (``len(bars)-1``)
    """
    if "plan" not in _PLAN_CACHE:
        base = synth_series()
        bars, idx = first_entry_index(base)
        for j in range(60, idx):  # sanity: no entry signal anywhere before the entry bar
            s = evaluate_at(bars[: j + 1], has_position=False)
            assert s is None or not s.entry_ok, f"unexpected early entry at {j}"
        _PLAN_CACHE["plan"] = {"bars": bars, "idx": idx}
    return _PLAN_CACHE["plan"]


def ema_exit_tail(bars: list[tuple], entry_price: float, *, limit: int = 120):
    """Fall gently until the first exit signal, which must be ``ema_cross`` (not atr).

    Tries decline rates from -0.2 %/bar down; returns (bars, exit_index, reason)."""
    for pct in (-0.002, -0.003, -0.004, -0.006, -0.008, -0.01):
        cur = list(bars)
        for _ in range(limit):
            cur = extend(cur, pct)
            s = evaluate_at(cur, has_position=True, entry_price=entry_price)
            if s.exit_signal:
                if s.exit_reason == "ema_cross":
                    return cur, len(cur) - 1, s.exit_reason
                break
    raise AssertionError("no ema_cross exit found")


def atr_crash(bars: list[tuple], entry_price: float, *, search=(0.03, 0.2, 0.0025)):
    """One crash bar that triggers atr_distance WITHOUT an ema cross.

    Returns (bars_with_crash_bar, crash_index, crash_pct); the equally large UP bar is
    ``extend(bars, +crash_pct)`` (callers assert it does not exit)."""
    lo, hi, st = search
    pct = lo
    while pct <= hi:
        cur = extend(bars, -pct)
        s = evaluate_at(cur, has_position=True, entry_price=entry_price)
        if s.exit_signal and s.exit_reason == "atr_distance":
            return cur, len(cur) - 1, pct
        pct += st
    raise AssertionError("no atr crash found")


def feed_for(fx: FakeExchange, clock: E2EClock, bars: list[tuple], first_live: int, **kw) -> Feed:
    """Feed where bar ``first_live-1`` has just closed at the clock's 4h boundary and
    bar ``j >= first_live`` closes ``(j-first_live+1)`` bars later."""
    b0 = floor_boundary(clock.now())
    return make_feed(fx, bars, end=b0 + timedelta(milliseconds=(len(bars) - first_live) * BAR_MS), **kw)
