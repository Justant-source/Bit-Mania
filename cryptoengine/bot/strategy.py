"""Supertrend 4h long-only strategy: pure signal evaluation plus the Trader.

Ported from services/strategies/supertrend/strategy.py (combo #7908). The
exchange is the truth: position state is re-synced before every decision and
after every non-filled outcome, and internal state only changes on a confirmed
fill. ``executor.execute`` returns the terminal result directly, so the old
order:result subscription and pending-order watcher are gone; the same-bar
retry (60 s, remainder only) is kept.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import pandas as pd
import structlog

from bot.db import Database
from bot.indicators import compute_atr, compute_ema, compute_supertrend
from bot.interfaces import (
    BAR_MS,
    EXCHANGE,
    STRATEGY_ID,
    SYMBOL,
    EntryGate,
    ExecutionResult,
    Executor,
    Notifier,
)
from bot.state import BotState, Clock, SystemClock
from shared.models.order import OrderRequest
from shared.models.position import Position

log = structlog.get_logger("bot.strategy")

# ── Strategy parameters (combo #7908) ───────────────────────────────────
ST_FACTOR = 2.6
ST_PERIOD = 9
FAST_EMA_LEN = 7
SLOW_EMA_LEN = 29
DIR_EMA_LEN = 240
ATR_MULT = 3.3
LEVERAGE = 3
# dir_ema(240) seed decays as (1-k)^L; L=1000 makes it match the full-history
# backtest (see tests/bot/test_indicator_parity.py).
CANDLE_LOOKBACK = 1000

# ── Operational constants ───────────────────────────────────────────────
RETRY_DELAY_S = 60.0
MIN_ORDER_QTY = 0.001
MIN_NOTIONAL_USD = 65.0
MIN_BARS = max(ST_PERIOD, DIR_EMA_LEN, SLOW_EMA_LEN) + 20

KEY_LAST_LIQUIDATION_TS = "last_liquidation_ts"
KEY_ATR_COOLDOWN_UNTIL = "atr_cooldown_until"

# Rejection reasons after which the same bar is not retried.
_NO_RETRY_PREFIXES = ("kill_switch", "entries_blocked", "dry_run")


# ── candles ─────────────────────────────────────────────────────────────


def candle_from_ohlcv(c: Any) -> dict[str, Any]:
    """Raw ccxt row [ts_ms, o, h, l, c, v], OHLCV model or dict -> candle dict (ts in ms)."""
    if isinstance(c, (list, tuple)):
        return {"open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
                "close": float(c[4]), "volume": float(c[5]), "ts": int(c[0])}
    if isinstance(c, dict):
        return {k: (int(c[k]) if k == "ts" else float(c[k])) for k in
                ("open", "high", "low", "close", "volume", "ts")}
    return {
        "open": float(c.open),
        "high": float(c.high),
        "low": float(c.low),
        "close": float(c.close),
        "volume": float(c.volume),
        "ts": int(round(c.timestamp.timestamp() * 1000)),
    }


# ── pure evaluation ─────────────────────────────────────────────────────


@dataclass
class Signal:
    """Everything the strategy computes for one confirmed bar."""

    bar_ts: int  # bar open time, ms
    st_dir: int
    st_line: float
    fast_ema: float
    slow_ema: float
    dir_ema: float
    price: float
    atr_14: float
    had_position: bool
    entry_ok: bool
    exit_signal: bool
    exit_reason: str | None
    expected_action: str  # "enter" | "exit" | "hold"
    expected_qty: float | None = None
    expected_stop_loss: float | None = None


def evaluate(
    candles: list[dict[str, Any]] | pd.DataFrame,
    *,
    has_position: bool,
    entry_price: float = 0.0,
    last_liquidation_ts: int = 0,
    atr_cooldown_until: int = 0,
) -> Signal | None:
    """Indicators + entry/exit decision for the last bar of ``candles``.

    Returns None when there are too few bars. Pure: no I/O, no clock.
    Entry: st_dir == 1 and fast > slow and price > dir_ema and the bar is past
    both cooldowns. Exit: fast < slow (ema_cross) or, downside only,
    price <= entry - ATR(14) * 3.3 (atr_distance).
    """
    df = candles if isinstance(candles, pd.DataFrame) else pd.DataFrame(list(candles))
    if len(df) < MIN_BARS:
        return None
    st_dir, st_line = compute_supertrend(df, ST_PERIOD, ST_FACTOR)
    fast_ema = float(compute_ema(df, FAST_EMA_LEN).iloc[-1])
    slow_ema = float(compute_ema(df, SLOW_EMA_LEN).iloc[-1])
    dir_ema = float(compute_ema(df, DIR_EMA_LEN).iloc[-1])
    price = float(df["close"].iloc[-1])
    atr_14 = float(compute_atr(df, 14))
    bar_ts = int(df["ts"].iloc[-1])

    entry_ok = False
    exit_signal = False
    exit_reason: str | None = None
    action = "hold"
    if not has_position:
        entry_ok = bool(
            st_dir == 1
            and fast_ema > slow_ema
            and price > dir_ema
            and bar_ts > last_liquidation_ts
            and bar_ts > atr_cooldown_until
        )
        if entry_ok:
            action = "enter"
    else:
        ema_cross_exit = fast_ema < slow_ema
        # ATR is a downside stop only; upside rides until the EMA death cross.
        atr_distance_exit = price <= entry_price - atr_14 * ATR_MULT
        if ema_cross_exit or atr_distance_exit:
            exit_signal = True
            exit_reason = "ema_cross" if ema_cross_exit else "atr_distance"
            action = "exit"
    return Signal(
        bar_ts=bar_ts, st_dir=int(st_dir), st_line=float(st_line), fast_ema=fast_ema,
        slow_ema=slow_ema, dir_ema=dir_ema, price=price, atr_14=atr_14,
        had_position=has_position, entry_ok=entry_ok, exit_signal=exit_signal,
        exit_reason=exit_reason, expected_action=action,
    )


def entry_sizing(capital: float, price: float, leverage: int = LEVERAGE) -> tuple[float, float]:
    """(quantity, stop_loss_price): capital * 0.95 * leverage / price; SL at -70%/lev."""
    return (capital * 0.95 * leverage) / price, price * (1 - 0.70 / leverage)


def remainder_is_dust(remainder: float, price: float) -> bool:
    """Remainders at/below zero, the min quantity or the min notional are ignored."""
    return (
        remainder <= 1e-8
        or remainder < MIN_ORDER_QTY
        or (price > 0 and remainder * price < MIN_NOTIONAL_USD)
    )


def _fmt_hold(delta: timedelta) -> str:
    secs = max(0, int(delta.total_seconds()))
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    return f"{d}일 {h}시간 {rem // 60}분" if d else f"{h}시간 {rem // 60}분"


# ── Trader ──────────────────────────────────────────────────────────────


class Trader:
    """Bar handling, entry/exit, same-bar retry, cooldowns, position truth-sync."""

    def __init__(
        self,
        executor: Executor,
        db: Database,
        state: BotState,
        notifier: Notifier,
        *,
        max_capital_usd: float,
        gate: EntryGate | None = None,
        clock: Clock | None = None,
        leverage: int = LEVERAGE,
        retry_delay_s: float = RETRY_DELAY_S,
        on_fill: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._executor = executor
        self._db = db
        self._state = state
        self._notifier = notifier
        self._gate: EntryGate = gate or state
        self._clock: Clock = clock or SystemClock()
        self._max_capital = float(max_capital_usd)
        self._leverage = leverage
        self._retry_delay = retry_delay_s
        self.on_fill = on_fill

        self._candles: list[dict[str, Any]] = []
        self._has_position = False
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._synced_once = False
        self._last_liquidation_ts = 0
        self._atr_cooldown_until = 0
        self._last_bar_ts = 0
        self._last_signal: Signal | None = None
        self._last_wallet: float | None = None
        self._last_total: float | None = None

        self._generation = 0
        self._retry_task: asyncio.Task | None = None
        self._retry_critical = False
        self.lock = asyncio.Lock()

    # ── read-only views ─────────────────────────────────────────────────

    @property
    def has_position(self) -> bool:
        return self._has_position

    @property
    def position_qty(self) -> float:
        return self._position_qty

    @property
    def entry_price(self) -> float:
        return self._entry_price

    @property
    def last_signal(self) -> Signal | None:
        return self._last_signal

    @property
    def last_bar_ts(self) -> int:
        return self._last_bar_ts

    @property
    def last_liquidation_ts(self) -> int:
        return self._last_liquidation_ts

    @property
    def atr_cooldown_until(self) -> int:
        return self._atr_cooldown_until

    # ── lifecycle ───────────────────────────────────────────────────────

    def load_state(self) -> None:
        """Restore cooldown timestamps persisted in bot_state."""
        self._last_liquidation_ts = int(self._state.get(KEY_LAST_LIQUIDATION_TS, 0) or 0)
        self._atr_cooldown_until = int(self._state.get(KEY_ATR_COOLDOWN_UNTIL, 0) or 0)

    async def shutdown(self) -> None:
        """Stop background retries. Never touches the position."""
        self._generation += 1
        task = self._retry_task
        self._retry_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def process_missed_bar(self, candles: list[dict[str, Any]]) -> bool:
        """Evaluate the latest confirmed bar if supertrend_signals never saw it.

        Same rule as the old on_start: only when a previous bar was processed
        (MAX(bar_ts) is not NULL) and the latest confirmed bar is newer.
        The signal row written by on_bar makes this fire once.
        """
        confirmed = self._confirmed(candles)
        if not confirmed:
            return False
        try:
            async with self._db.pool.acquire() as conn:
                last_ms = await conn.fetchval(
                    "SELECT (EXTRACT(EPOCH FROM MAX(bar_ts)) * 1000)::BIGINT FROM supertrend_signals"
                )
        except Exception:
            log.exception("missed_bar_check_error")
            return False
        latest = int(confirmed[-1]["ts"])
        if last_ms is None or latest <= int(last_ms):
            return False
        log.warning("missed_bar_on_start", message="재시작 중 놓친 확정 봉 발견 — 즉시 신호 처리",
                    last_processed_ts=int(last_ms), latest_bar_ts=latest)
        await self.on_bar(confirmed)
        return True

    # ── bar handling ────────────────────────────────────────────────────

    def _confirmed(self, candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Drop any bar that has not closed yet (defence in depth; app filters too)."""
        now_ms = int(self._clock.now().timestamp() * 1000)
        return [c for c in candles if int(c["ts"]) + BAR_MS <= now_ms]

    async def on_bar(self, candles: list[dict[str, Any]]) -> None:
        """Handle one newly confirmed 4h bar (``candles`` ends with that bar)."""
        self._generation += 1  # invalidates any pending same-bar retry
        task = self._retry_task
        if task is not None and not task.done() and not self._retry_critical:
            task.cancel()
        async with self.lock:
            await self._on_bar_locked(candles)

    async def _on_bar_locked(self, candles: list[dict[str, Any]]) -> None:
        gen = self._generation
        self._candles = self._confirmed(candles)[-CANDLE_LOOKBACK:]
        if len(self._candles) < MIN_BARS:
            log.warning("insufficient_candles", message="지표 계산에 필요한 봉 부족 — 신호 건너뜀",
                        have=len(self._candles), need=MIN_BARS)
            return
        synced = await self.sync_position("pre_signal")
        try:
            sig = evaluate(
                self._candles, has_position=self._has_position, entry_price=self._entry_price,
                last_liquidation_ts=self._last_liquidation_ts,
                atr_cooldown_until=self._atr_cooldown_until,
            )
        except Exception:
            log.exception("indicator_computation_error")
            return
        if sig is None:
            return
        self._last_bar_ts = sig.bar_ts

        allocated, effective = await self._capital()
        if sig.entry_ok:
            sig.expected_qty, sig.expected_stop_loss = entry_sizing(
                effective or 0.0, sig.price, self._leverage
            ) if effective else (None, None)
        self._last_signal = sig
        log.info("supertrend_signal", bar_ts=sig.bar_ts, st_dir=sig.st_dir, price=round(sig.price, 2),
                 fast_ema=round(sig.fast_ema, 2), slow_ema=round(sig.slow_ema, 2),
                 dir_ema=round(sig.dir_ema, 2), atr_14=round(sig.atr_14, 2),
                 action=sig.expected_action)

        if sig.entry_ok:
            if not synced:
                # Cannot verify we are flat: never open a second position blind.
                log.error("entry_skipped_unverified_position",
                          message="거래소 포지션 조회 실패 — 진입 보류 후 재시도")
                self._schedule_retry("enter", sig.bar_ts, sig.expected_qty or 0.0, None, False, gen)
            elif not effective:
                log.error("entry_skipped_no_balance", message="잔고 조회 실패 — 진입 보류 후 재시도")
                self._schedule_retry("enter", sig.bar_ts, 0.0, None, False, gen)
            else:
                await self._enter(sig.price, sig.bar_ts, capital=effective, gen=gen)
        elif sig.exit_signal and sig.exit_reason:
            await self._exit(sig.exit_reason, sig.exit_reason == "atr_distance",
                             sig.price, sig.bar_ts, gen=gen)

        # Persist after the action so a crash mid-order leaves the bar "missed"
        # and the next start re-evaluates it (idempotent thanks to the sync).
        await self._persist_signal(sig, allocated)

    async def _capital(self) -> tuple[float, float]:
        """(allocated, effective): allocated = min(wallet, max_capital); effective =
        min(allocated, live total equity). (0, 0) when no balance was ever readable."""
        try:
            bal = await self._executor.get_balance()
            total = float(bal.get("total") or 0.0)
            wallet = float(bal.get("wallet") or total)
            if wallet > 0:
                self._last_wallet = wallet
            if total > 0:
                self._last_total = total
        except Exception as exc:
            log.warning("balance_read_error", message=f"{type(exc).__name__}: {str(exc)[:200]}")
        wallet = self._last_wallet
        if not wallet:
            return 0.0, 0.0
        allocated = min(wallet, self._max_capital)
        total = self._last_total or allocated
        effective = min(allocated, total)
        if effective < allocated:
            log.info("entry_capital_reduced", message="실시간 equity가 할당자본보다 낮아 축소 진입",
                     allocated=round(allocated, 2), equity=round(effective, 2))
        return allocated, effective

    async def _persist_signal(self, s: Signal, allocated: float) -> None:
        try:
            async with self._db.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO supertrend_signals (
                        bar_ts, computed_at, st_dir, st_line, fast_ema, slow_ema, dir_ema,
                        price, atr_14, allocated_capital, had_position, entry_ok,
                        exit_signal, exit_reason, expected_action,
                        expected_qty, expected_stop_loss
                    ) VALUES (
                        to_timestamp($1::bigint / 1000.0), NOW(),
                        $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT (bar_ts) DO NOTHING
                    """,
                    s.bar_ts, s.st_dir, s.st_line, s.fast_ema, s.slow_ema, s.dir_ema, s.price,
                    s.atr_14, allocated, s.had_position, s.entry_ok, s.exit_signal,
                    s.exit_reason, s.expected_action, s.expected_qty, s.expected_stop_loss,
                )
        except Exception as exc:
            log.error("signal_persist_error", exc_type=type(exc).__name__, exc_msg=str(exc),
                      bar_ts=s.bar_ts)

    # ── entry ───────────────────────────────────────────────────────────

    async def _enter(
        self, price: float, bar_ts: int, *, capital: float | None = None,
        quantity: float | None = None, target_qty: float | None = None,
        base_qty: float = 0.0, gen: int,
    ) -> None:
        blocked, why = self._gate.entries_blocked()
        if blocked:
            log.warning("entry_blocked", message="진입 차단 중 — 주문 안 냄", reason=why)
            await self._notifier.order_rejected(side="buy", reason=why, will_retry=False)
            return
        qty, stop_loss = entry_sizing(capital or 0.0, price, self._leverage)
        if quantity is not None:
            qty = quantity
        target = qty if target_qty is None else target_qty
        if qty < MIN_ORDER_QTY or qty * price < MIN_NOTIONAL_USD:
            log.warning("entry_order_too_small", quantity=qty, notional=qty * price,
                        min_notional=MIN_NOTIONAL_USD)
            return
        order = OrderRequest(
            strategy_id=STRATEGY_ID, exchange=EXCHANGE, symbol=SYMBOL, side="buy",
            order_type="limit", quantity=qty, price=price, post_only=True, reduce_only=False,
            stop_loss=stop_loss,
        )
        signal_ts = datetime.fromtimestamp(bar_ts / 1000, tz=timezone.utc)
        log.info("entry_order_submitted", quantity=round(qty, 4), price=round(price, 2),
                 stop_loss=round(stop_loss, 2))
        try:
            res = await self._executor.execute(order, signal_ts=signal_ts)
        except Exception:
            log.exception("entry_order_error")
            res = ExecutionResult(request_id=order.request_id, status="rejected",
                                  reason="executor_exception")

        if res.any_fill:
            # A partial fill is a real position: confirm what filled, retry the rest.
            await self._confirm_entry(res, order, base_qty)
            # Dust remainders are ignored; anything bigger is retried this bar.
            if not remainder_is_dust(target - self._position_qty, price):
                self._schedule_retry("enter", bar_ts, target, None, False, gen)
            return
        await self._handle_not_filled("enter", res, order.quantity)
        if not self._no_retry(res.reason):
            self._schedule_retry("enter", bar_ts, target, None, False, gen)

    async def _confirm_entry(self, res: ExecutionResult, order: OrderRequest, base_qty: float) -> None:
        filled = res.filled_qty if res.filled_qty > 0 else order.quantity
        px = float(res.filled_price or order.price or 0.0)
        if base_qty > 0 and self._entry_price > 0 and px > 0:
            self._entry_price = (self._entry_price * base_qty + px * filled) / (base_qty + filled)
        elif px > 0:
            self._entry_price = px
        self._has_position = True
        self._position_qty = base_qty + filled
        self._synced_once = True
        log.info("entry_confirmed", message="진입 체결 확정", qty=round(self._position_qty, 6),
                 entry_price=round(self._entry_price, 2))
        await self._db_upsert_position(self._position_qty, self._entry_price)
        await self._notifier.entry(qty=filled, price=px, stop_loss=order.stop_loss,
                                   equity=self._last_total)
        await self._fire_on_fill()

    # ── exit ────────────────────────────────────────────────────────────

    async def _exit(
        self, reason: str, atr_triggered: bool, price: float | None, bar_ts: int,
        *, is_retry: bool = False, gen: int,
    ) -> None:
        if not self._has_position or self._position_qty <= 0:
            return
        qty = self._position_qty
        entry_price = self._entry_price
        order = OrderRequest(
            strategy_id=STRATEGY_ID, exchange=EXCHANGE, symbol=SYMBOL, side="sell",
            order_type="limit" if price is not None else "market", quantity=qty, price=price,
            post_only=price is not None, reduce_only=True,
        )
        signal_ts = datetime.fromtimestamp(bar_ts / 1000, tz=timezone.utc)
        log.info("exit_order_submitted", reason=reason, quantity=round(qty, 4), is_retry=is_retry)
        try:
            res = await self._executor.execute(order, signal_ts=signal_ts)
        except Exception:
            log.exception("exit_order_error")
            res = ExecutionResult(request_id=order.request_id, status="rejected",
                                  reason="executor_exception")
        if res.is_filled:
            await self._confirm_exit(res, reason, atr_triggered, bar_ts, qty, entry_price)
            return
        await self._handle_not_filled("exit", res, qty)
        if not self._no_retry(res.reason):
            self._schedule_retry("exit", bar_ts, 0.0, reason, atr_triggered, gen)

    async def _confirm_exit(
        self, res: ExecutionResult, reason: str, atr_triggered: bool, bar_ts: int,
        qty: float, entry_price: float,
    ) -> None:
        """Cooldown timestamps are set here only; a rejected exit leaves none."""
        if atr_triggered:
            self._atr_cooldown_until = bar_ts + BAR_MS
            await self._safe_state_set(KEY_ATR_COOLDOWN_UNTIL, self._atr_cooldown_until)
        self._last_liquidation_ts = bar_ts
        await self._safe_state_set(KEY_LAST_LIQUIDATION_TS, self._last_liquidation_ts)
        self._has_position = False
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._synced_once = True
        exit_px = float(res.filled_price or 0.0)
        pnl = (exit_px - entry_price) * (res.filled_qty or qty) - res.fee if entry_price > 0 and exit_px > 0 else None
        hold = await self._db_close_position(reason, want_hold=True)
        log.info("exit_confirmed", message="청산 체결 확정", reason=reason)
        await self._notifier.exit(qty=res.filled_qty or qty, entry_price=entry_price or None,
                                  exit_price=exit_px, pnl=pnl, reason=reason, hold=hold)
        await self._fire_on_fill()

    # ── non-filled handling ─────────────────────────────────────────────

    @staticmethod
    def _no_retry(reason: object) -> bool:
        return str(reason or "").startswith(_NO_RETRY_PREFIXES)

    async def _handle_not_filled(self, action: str, res: ExecutionResult, qty: float) -> None:
        if str(res.reason).startswith("dry_run"):
            log.info("dry_run_order_skipped", action=action)  # no alert, no sync, no retry
            return
        will_retry = not self._no_retry(res.reason)
        log.warning("order_not_filled", message="주문 미체결 — 거래소 기준 재동기화", action=action,
                    status=res.status, reason=res.reason, filled_qty=res.filled_qty, qty=qty)
        await self._notifier.order_rejected(
            side="buy" if action == "enter" else "sell",
            reason=f"{res.status}: {res.reason}" if res.reason else res.status,
            will_retry=will_retry,
        )
        await self.sync_position("post_reject")

    # ── same-bar retry ──────────────────────────────────────────────────

    def _schedule_retry(
        self, action: str, bar_ts: int, target_qty: float, exit_reason: str | None,
        atr_triggered: bool, gen: int,
    ) -> None:
        """Retry every ``retry_delay`` s until the next bar. Remainder only; the
        sleep phase is cancelled by a new bar, an order in flight is never cut."""
        if gen != self._generation:
            return
        task = self._retry_task
        if task is not None and not task.done() and not self._retry_critical \
                and task is not asyncio.current_task():
            task.cancel()
        self._retry_critical = False
        self._retry_task = asyncio.create_task(
            self._retry(action, bar_ts, target_qty, exit_reason, atr_triggered, gen)
        )

    async def _retry(
        self, action: str, bar_ts: int, target_qty: float, exit_reason: str | None,
        atr_triggered: bool, gen: int,
    ) -> None:
        try:
            await self._clock.sleep(self._retry_delay)
            if gen != self._generation:
                return
            async with self.lock:
                if gen != self._generation:
                    return
                self._retry_critical = True
                await self._retry_locked(action, bar_ts, target_qty, exit_reason, atr_triggered, gen)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("same_bar_retry_error")
        finally:
            if self._retry_task is asyncio.current_task():
                self._retry_critical = False

    async def _retry_locked(
        self, action: str, bar_ts: int, target_qty: float, exit_reason: str | None,
        atr_triggered: bool, gen: int,
    ) -> None:
        synced = await self.sync_position("same_bar_retry")
        if gen != self._generation or not self._candles:
            return
        price = float(self._candles[-1]["close"])
        if action == "exit":
            if not self._has_position:
                log.info("same_bar_retry_skipped", message="재시도 전 포지션 이미 청산됨", action="exit")
                return
            await self._exit(exit_reason or "exit", atr_triggered, price, bar_ts,
                             is_retry=True, gen=gen)
            return
        # entry: the signal must still stand and the remaining quantity must be real
        sig = evaluate(
            self._candles, has_position=False, last_liquidation_ts=self._last_liquidation_ts,
            atr_cooldown_until=self._atr_cooldown_until,
        )
        if sig is None or not sig.entry_ok:
            log.info("same_bar_retry_skipped", message="재시도 시 진입 신호 소멸", action="enter")
            return
        if not synced:
            self._schedule_retry("enter", bar_ts, target_qty, None, False, gen)
            return
        actual = self._position_qty if self._has_position else 0.0
        if target_qty <= 0:  # the first attempt never got a size (no balance)
            _, effective = await self._capital()
            if not effective:
                self._schedule_retry("enter", bar_ts, 0.0, None, False, gen)
                return
            await self._enter(price, bar_ts, capital=effective, gen=gen)
            return
        remainder = target_qty - actual
        if remainder_is_dust(remainder, price):
            return
        await self._enter(price, bar_ts, quantity=remainder, target_qty=target_qty,
                          base_qty=actual, gen=gen)

    # ── position truth sync ─────────────────────────────────────────────

    async def sync_position(self, context: str) -> bool:
        """Correct internal state from the exchange (exchange = truth).

        Divergence is logged at ERROR (-> Telegram) and the real value adopted.
        After 3 failed reads the previous belief is kept and False returned.
        """
        pos: Position | None = None
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                pos = await self._executor.get_position()
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if attempt < 2:
                    await self._clock.sleep(1.0 * (attempt + 1))
        if last_exc is not None:
            log.error(
                "position_sync_error",
                message=(f"{type(last_exc).__name__}: {str(last_exc)[:300]} "
                         f"(3회 재시도 실패 — 내부 상태 has_position={self._has_position} 유지)"),
                context=context, error_type=type(last_exc).__name__,
            )
            return False
        await self._apply_exchange_position(pos, context)
        return True

    async def _apply_exchange_position(
        self, pos: Position | None, context: str, *, log_divergence: bool = True,
        close_reason: str = "exchange_flat",
    ) -> str | None:
        """Adopt ``pos``. Returns "appeared"/"vanished" on a has-position flip."""
        if pos is not None and pos.side == "short":
            log.error("unexpected_short_position",
                      message="Long-only 전략 계정에 숏 포지션 감지 — 수동 확인 필요",
                      size=pos.size, context=context)
            actual_qty, actual_entry = 0.0, 0.0
        else:
            actual_qty = float(pos.size) if pos else 0.0
            actual_entry = float(pos.entry_price) if pos else 0.0
        actual_has = actual_qty > 0
        flip: str | None = None
        if actual_has != self._has_position:
            flip = "appeared" if actual_has else "vanished"
            if not self._synced_once:
                # first read after a start: adopting the exchange state is expected
                log.info("position_adopted", message="시작 시 거래소 포지션 확인", context=context,
                         actual_qty=round(actual_qty, 6))
            elif log_divergence:
                log.error("position_state_divergence",
                          message="전략 내부 상태 ≠ 거래소 실포지션 — 실제값으로 강제 교정",
                          context=context, believed_has_position=self._has_position,
                          believed_qty=round(self._position_qty, 6), actual_qty=round(actual_qty, 6))
        elif actual_has and self._position_qty > 0 \
                and abs(actual_qty - self._position_qty) / actual_qty > 0.01:
            log.warning("position_size_drift", message="포지션 수량 드리프트 — 실제값으로 교정",
                        context=context, believed_qty=round(self._position_qty, 6),
                        actual_qty=round(actual_qty, 6))
        self._has_position = actual_has
        self._position_qty = actual_qty
        if actual_has and actual_entry > 0:
            self._entry_price = actual_entry
        elif not actual_has:
            self._entry_price = 0.0
        self._synced_once = True
        if actual_has:
            await self._db_upsert_position(
                actual_qty, actual_entry, unrealized=float(pos.unrealized_pnl) if pos else 0.0,
                leverage=float(pos.leverage) if pos else None,
            )
        else:
            await self._db_close_position(close_reason)
        return flip

    async def reconcile_fresh(self) -> tuple[str | None, Position | None]:
        """Housekeeping check: read the position while holding the trader lock, so
        a same-bar retry that fills in between cannot be mistaken for a position
        that appeared or vanished. Raises if the exchange read fails."""
        async with self.lock:
            pos = await self._executor.get_position()
            believed_qty = self._position_qty
            flip = await self._apply_exchange_position(
                pos, "housekeeping", log_divergence=False, close_reason="exchange_side",
            )
        await self._alert_flip(flip, believed_qty)
        return flip, pos

    async def reconcile(self, pos: Position | None) -> str | None:
        """Like reconcile_fresh but with an already fetched position."""
        async with self.lock:
            believed_qty = self._position_qty
            flip = await self._apply_exchange_position(
                pos, "housekeeping", log_divergence=False, close_reason="exchange_side",
            )
        await self._alert_flip(flip, believed_qty)
        return flip

    async def _alert_flip(self, flip: str | None, believed_qty: float) -> None:
        if flip == "vanished":
            await self._notifier.anomaly(
                kind="position_vanished",
                message=(f"보유 중이던 포지션({believed_qty:.4f} BTC)이 거래소에서 사라졌습니다 "
                         "(거래소 스톱 체결 또는 수동 청산 추정). 내부 상태와 DB를 정리했습니다."),
            )
        elif flip == "appeared":
            await self._notifier.anomaly(
                kind="position_appeared",
                message=(f"봇이 진입하지 않은 포지션({self._position_qty:.4f} BTC)이 거래소에서 "
                         "감지되었습니다 (수동 거래 추정). 내부 상태를 맞췄습니다."),
            )

    async def after_forced_close(self, reason: str) -> None:
        """Risk/operator closed the position through the executor: drop retries
        and adopt the exchange state. No cooldown is set (as the old on_stop)."""
        self._generation += 1
        task = self._retry_task
        if task is not None and not task.done() and not self._retry_critical:
            task.cancel()
        try:
            pos = await self._executor.get_position()
        except Exception as exc:
            log.error("position_sync_error", message=f"{type(exc).__name__}: {str(exc)[:200]}",
                      context="forced_close")
            return
        await self._apply_exchange_position(pos, "forced_close", log_divergence=False,
                                            close_reason=reason)
        await self._fire_on_fill()

    # ── persistence helpers (best effort) ───────────────────────────────

    async def _safe_state_set(self, key: str, value: Any) -> None:
        try:
            await self._state.set(key, value)
        except Exception:
            log.exception("state_persist_error", key=key)

    async def _fire_on_fill(self) -> None:
        if self.on_fill is not None:
            try:
                await self.on_fill()
            except Exception:
                log.exception("on_fill_error")

    async def _db_upsert_position(
        self, size: float, entry_price: float, *, unrealized: float = 0.0,
        leverage: float | None = None,
    ) -> None:
        """Open row on entry, update the open row afterwards (position_tracker semantics)."""
        lev = float(leverage if leverage else self._leverage)
        try:
            async with self._db.pool.acquire() as conn:
                existing = await conn.fetchval(
                    "SELECT id FROM positions WHERE strategy_id=$1 AND exchange=$2 AND symbol=$3 "
                    "AND closed_at IS NULL ORDER BY id DESC LIMIT 1",
                    STRATEGY_ID, EXCHANGE, SYMBOL,
                )
                if existing:
                    await conn.execute(
                        "UPDATE positions SET size=$2, entry_price=$3, current_price=$3, "
                        "unrealized_pnl=$4, leverage=$5 WHERE id=$1",
                        existing, size, entry_price, unrealized, lev,
                    )
                else:
                    await conn.execute(
                        "INSERT INTO positions (strategy_id, exchange, symbol, side, size, entry_price, "
                        "current_price, unrealized_pnl, leverage, opened_at) "
                        "VALUES ($1,$2,$3,'long',$4,$5,$5,$6,$7,NOW())",
                        STRATEGY_ID, EXCHANGE, SYMBOL, size, entry_price, unrealized, lev,
                    )
        except Exception:
            log.exception("persist_position_error")

    async def _db_close_position(self, reason: str, *, want_hold: bool = False) -> str | None:
        """Close the open row (closed_at=NOW(), close_reason). Returns hold time text."""
        hold: str | None = None
        try:
            async with self._db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "UPDATE positions SET closed_at=NOW(), close_reason=$3 "
                    "WHERE exchange=$1 AND symbol=$2 AND closed_at IS NULL "
                    "RETURNING opened_at, closed_at",
                    EXCHANGE, SYMBOL, reason[:50],
                )
            if want_hold and row and row["opened_at"] and row["closed_at"]:
                hold = _fmt_hold(row["closed_at"] - row["opened_at"])
        except Exception:
            log.exception("clear_position_db_error")
        return hold


__all__ = [
    "Trader", "Signal", "evaluate", "entry_sizing", "remainder_is_dust", "candle_from_ohlcv",
    "CANDLE_LOOKBACK", "MIN_BARS",
]
