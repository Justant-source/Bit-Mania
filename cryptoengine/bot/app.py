"""BotApp: wires the modules together, runs the scheduler, implements BotControl.

Scheduler (one asyncio loop, no websocket):
  * every 4h boundary + 3 s: REST-fetch candles, wait (retry every 5 s) until the
    bar that just closed is present, hand the confirmed bars to Trader.on_bar;
  * housekeeping every 15 min: position + balance reads, reconcile, risk, snapshot;
  * daily report at 00:00 UTC;
  * heartbeat on every loop tick (at most 60 s apart).

Time and sleeping come from an injected ``Clock``; every collaborator is passed
in, so tests drive the whole thing with fakes. A shutdown (SIGTERM) never
closes a position.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable

import structlog

from bot.db import Database
from bot.interfaces import BAR_MS, EXCHANGE, SYMBOL, TIMEFRAME, Exchange, Executor, Notifier
from bot.risk import RiskManager
from bot.state import MANUAL_HALT, BotState, Clock, SystemClock
from bot.strategy import CANDLE_LOOKBACK, Trader, candle_from_ohlcv

log = structlog.get_logger("bot.app")

DB_SYMBOL = "BTCUSDT"
BOUNDARY_OFFSET_S = 3
BAR_RETRY_S = 5.0
BAR_ANOMALY_AFTER_S = 600.0
BAR_SLOW_RETRY_S = 60.0
HOUSEKEEPING_S = 900.0
HEARTBEAT_MAX_GAP_S = 60.0
KEY_HEARTBEAT = "heartbeat"


def bar_boundary_ms(now_ms: int) -> int:
    """Start (ms) of the next 4h boundary strictly after ``now_ms``."""
    return (now_ms // BAR_MS + 1) * BAR_MS


class BotApp:
    def __init__(
        self,
        *,
        db: Database,
        exchange: Exchange,
        executor: Executor,
        notifier: Notifier,
        state: BotState,
        trader: Trader,
        risk: RiskManager,
        clock: Clock | None = None,
        report_fn: Callable[[], Awaitable[tuple[str, Any]]] | None = None,
        replay_sync: Callable[[Database], Awaitable[Any]] | None = None,
        heartbeat_path: str | None = "/tmp/heartbeat_ok",
        leverage: int = 3,
        housekeeping_s: float = HOUSEKEEPING_S,
    ) -> None:
        self.db = db
        self.exchange = exchange
        self.executor = executor
        self.notifier = notifier
        self.state = state
        self.trader = trader
        self.risk = risk
        self.clock: Clock = clock or SystemClock()
        self._report_fn = report_fn
        self._replay_sync = replay_sync
        self._heartbeat_path = heartbeat_path
        self._leverage = leverage
        self._housekeeping_s = housekeeping_s

        self._services: list[tuple[Callable | None, Callable | None]] = []
        self._stop = asyncio.Event()
        self._started = False

        self._next_bar_at: datetime = self.clock.now()
        self._next_house_at: datetime = self.clock.now()
        self._next_report_at: datetime = self.clock.now()
        self._pending_bar: int | None = None
        self._pending_missed_check = False
        self._bar_started_at: datetime | None = None
        self._bar_retry_at: datetime | None = None
        self._bar_anomaly_sent = False
        self._hk_failures = 0
        self._last_heartbeat: datetime | None = None
        self._last_equity: float | None = None
        self._bars: list[dict[str, Any]] = []

    # ── wiring ──────────────────────────────────────────────────────────

    def add_service(self, start: Callable | None = None, stop: Callable | None = None) -> None:
        """Register an async start/stop pair (Telegram commands, web server)."""
        self._services.append((start, stop))

    def request_stop(self) -> None:
        self._stop.set()

    # ── startup ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.exchange.connect()
        try:
            await self.exchange.set_margin_mode(SYMBOL, "isolated")
            await self.exchange.set_leverage(SYMBOL, self._leverage)
        except Exception:
            log.exception("exchange_config_error")
        await self.executor.start()
        await self.state.load()
        self.trader.load_state()
        await self.risk.load()
        await self.trader.sync_position("on_start")

        try:
            self._bars = await self._fetch_confirmed()
            log.info("candles_backfilled", count=len(self._bars))
            await self._store_bars(self._bars, overwrite=False)
            await self.trader.process_missed_bar(self._bars)
        except Exception:
            log.exception("startup_candle_error")
        # The bar that closed most recently must be seen. If REST does not serve
        # it yet (restart right after a boundary) or the fetch failed, keep
        # asking; it is evaluated only if supertrend_signals never saw it.
        latest_closed = (self._now_ms() // BAR_MS) * BAR_MS - BAR_MS
        if not any(b["ts"] == latest_closed for b in self._bars):
            start_now = self.clock.now()
            self._pending_bar = latest_closed
            self._pending_missed_check = True
            self._bar_started_at = start_now
            self._bar_retry_at = start_now
            self._bar_anomaly_sent = False

        for start, _ in self._services:
            if start is not None:
                try:
                    await start()
                except Exception:
                    log.exception("service_start_error")

        now = self.clock.now()
        self._next_bar_at = self._boundary_after(now)
        self._next_house_at = now
        self._next_report_at = self._next_midnight(now)
        self._started = True
        await self._send_startup_message()

    async def _send_startup_message(self) -> None:
        pos = self.trader.has_position
        txt = (
            "<b>봇 시작</b>\n"
            f"포지션: {'보유 ' + format(self.trader.position_qty, '.4f') + ' BTC' if pos else '없음'}\n"
            f"봉 {len(self._bars)}개 로드, 다음 봉 처리: {self._next_bar_at:%Y-%m-%d %H:%M:%S} UTC"
        )
        blocked, why = self.state.entries_blocked()
        if blocked:
            txt += f"\n진입 차단 중: {why}"
        await self.notifier.send(txt)

    # ── main loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
                await self.tick()
                await self._wait(self._tick_delay())
        finally:
            await self.shutdown()

    async def tick(self) -> None:
        """One scheduler pass: refresh state, heartbeat, run whatever is due."""
        now = self.clock.now()
        try:
            await self.state.load()  # picks up changes made by the CLI
        except Exception:
            log.exception("state_refresh_error")
        await self._heartbeat()
        await self._guard(self._service_bar, "bar")
        if self.clock.now() >= self._next_house_at:
            await self._guard(self.housekeeping, "housekeeping")
            self._next_house_at = self.clock.now() + timedelta(seconds=self._housekeeping_s)
        if self.clock.now() >= self._next_report_at:
            self._next_report_at = self._next_midnight(self.clock.now())
            await self._guard(self.daily_report, "daily_report")

    async def _guard(self, fn: Callable[[], Awaitable[Any]], name: str) -> None:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("scheduler_task_error", task=name)

    def _tick_delay(self) -> float:
        now = self.clock.now()
        due = [self._next_bar_at, self._next_house_at, self._next_report_at,
               now + timedelta(seconds=HEARTBEAT_MAX_GAP_S)]
        if self._pending_bar is not None and self._bar_retry_at is not None:
            due.append(self._bar_retry_at)
        return max(0.1, (min(due) - now).total_seconds())

    async def _wait(self, delay: float) -> None:
        sleeper = asyncio.ensure_future(self.clock.sleep(delay))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (sleeper, stopper):
                if not t.done():
                    t.cancel()
            await asyncio.gather(sleeper, stopper, return_exceptions=True)

    # ── time helpers ────────────────────────────────────────────────────

    @staticmethod
    def _boundary_after(now: datetime) -> datetime:
        b = bar_boundary_ms(int(now.timestamp() * 1000))
        return datetime.fromtimestamp(b / 1000 + BOUNDARY_OFFSET_S, tz=timezone.utc)

    @staticmethod
    def _next_midnight(now: datetime) -> datetime:
        return datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)

    def _now_ms(self) -> int:
        return int(self.clock.now().timestamp() * 1000)

    # ── 4h bar handling ─────────────────────────────────────────────────

    async def _fetch_confirmed(self) -> list[dict[str, Any]]:
        """Last 1000 bars by REST, minus the in-progress one (the last element
        Bybit returns): a bar counts as confirmed only once ts + 4h <= now."""
        raw = await self.exchange.get_ohlcv(SYMBOL, TIMEFRAME, limit=CANDLE_LOOKBACK)
        now_ms = self._now_ms()
        by_ts = {c["ts"]: c for c in (candle_from_ohlcv(r) for r in raw)}
        return [by_ts[t] for t in sorted(by_ts) if t + BAR_MS <= now_ms]

    async def _service_bar(self) -> None:
        now = self.clock.now()
        if now >= self._next_bar_at:
            boundary_ms = ((int(now.timestamp()) - BOUNDARY_OFFSET_S) * 1000 // BAR_MS) * BAR_MS
            self._pending_bar = boundary_ms - BAR_MS  # open time of the bar that just closed
            self._pending_missed_check = False
            self._bar_started_at = now
            self._bar_retry_at = now
            self._bar_anomaly_sent = False
            self._next_bar_at = self._boundary_after(now)
        if self._pending_bar is not None and self._bar_retry_at is not None \
                and now >= self._bar_retry_at:
            await self._attempt_bar()

    async def _attempt_bar(self) -> None:
        assert self._pending_bar is not None and self._bar_started_at is not None
        expected = self._pending_bar
        now = self.clock.now()
        elapsed = (now - self._bar_started_at).total_seconds()
        try:
            bars = await self._fetch_confirmed()
        except Exception as exc:
            log.warning("bar_fetch_error", message=f"{type(exc).__name__}: {str(exc)[:200]}")
            bars = []
        if not any(b["ts"] == expected for b in bars):
            if elapsed >= BAR_ANOMALY_AFTER_S and not self._bar_anomaly_sent:
                self._bar_anomaly_sent = True
                await self.notifier.anomaly(
                    kind="bar_missing",
                    message=(f"4h 봉 {datetime.fromtimestamp(expected / 1000, tz=timezone.utc):%Y-%m-%d %H:%M} UTC "
                             f"마감 후 {int(elapsed // 60)}분이 지나도 REST에 없음 — 계속 재시도"),
                )
            self._bar_retry_at = now + timedelta(
                seconds=BAR_RETRY_S if elapsed < BAR_ANOMALY_AFTER_S else BAR_SLOW_RETRY_S)
            return
        self._pending_bar = None
        self._bar_retry_at = None
        self._bars = bars
        try:
            if self._pending_missed_check:
                await self.trader.process_missed_bar(bars)
            else:
                await self.trader.on_bar(bars)
        except Exception:
            log.exception("on_bar_error")
        self._pending_missed_check = False
        await self._store_bars(bars[-3:], overwrite=True)
        if self._replay_sync is not None:
            try:
                await self._replay_sync(self.db)
            except Exception:
                log.exception("replay_sync_error")

    async def _store_bars(self, bars: list[dict[str, Any]], *, overwrite: bool) -> None:
        if not bars:
            return
        conflict = (
            "DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
            "close=EXCLUDED.close, volume=EXCLUDED.volume" if overwrite else "DO NOTHING"
        )
        rows = [
            (EXCHANGE, DB_SYMBOL, TIMEFRAME, datetime.fromtimestamp(b["ts"] / 1000, tz=timezone.utc),
             Decimal(str(b["open"])), Decimal(str(b["high"])), Decimal(str(b["low"])),
             Decimal(str(b["close"])), Decimal(str(b["volume"])))
            for b in bars
        ]
        try:
            async with self.db.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO ohlcv_history (exchange, symbol, timeframe, timestamp, open, high, "
                    "low, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) "
                    f"ON CONFLICT (exchange, symbol, timeframe, timestamp) {conflict}",
                    rows,
                )
        except Exception:
            log.exception("ohlcv_store_error")

    # ── housekeeping ────────────────────────────────────────────────────

    async def housekeeping(self) -> None:
        """Every 15 min: read position + balance, reconcile, evaluate risk, snapshot."""
        pos = None
        pos_ok = bal_ok = False
        bal: dict[str, float] = {}
        flip = None
        try:
            # Read under the trader lock so an order being worked is not misread.
            flip, pos = await self.trader.reconcile_fresh()
            pos_ok = True
        except Exception as exc:
            log.warning("housekeeping_position_error", message=f"{type(exc).__name__}: {str(exc)[:200]}")
        try:
            bal = await self.executor.get_balance()
            bal_ok = True
        except Exception as exc:
            log.warning("housekeeping_balance_error", message=f"{type(exc).__name__}: {str(exc)[:200]}")

        if not (pos_ok and bal_ok):
            self._hk_failures += 1
            if self._hk_failures == 3:
                await self.notifier.anomaly(
                    kind="housekeeping_failing",
                    message="거래소 잔고/포지션 조회가 연속 3회 실패했습니다 (15분 주기). 연결 상태 확인 필요.",
                )
        else:
            self._hk_failures = 0

        if bal_ok:
            equity = float(bal.get("wallet") or bal.get("total") or 0.0)
            if equity > 0:
                self._last_equity = equity
                await self.risk.evaluate(equity, pos, position_known=pos_ok)
                await self.risk.record_snapshot(
                    equity, position_count=1 if pos is not None and pos.size > 0 else 0,
                    force=flip is not None,
                )
            else:
                log.warning("housekeeping_zero_equity", message="잔고 0 — 리스크 평가 건너뜀")
        await self._heartbeat()

    async def snapshot_after_fill(self) -> None:
        """Trader.on_fill hook: portfolio_snapshots row right after a fill/close."""
        bal = await self.executor.get_balance()
        pos = await self.executor.get_position()
        equity = float(bal.get("wallet") or bal.get("total") or 0.0)
        if equity > 0:
            self._last_equity = equity
            await self.risk.record_snapshot(
                equity, position_count=1 if pos is not None and pos.size > 0 else 0,
                unrealized=float(pos.unrealized_pnl) if pos else 0.0, force=True,
            )

    async def _heartbeat(self) -> None:
        now = self.clock.now()
        self._last_heartbeat = now
        try:
            await self.state.set(KEY_HEARTBEAT, {"at": now.isoformat()})
        except Exception:
            log.exception("heartbeat_state_error")
        if self._heartbeat_path:
            try:
                Path(self._heartbeat_path).touch()
            except OSError:
                pass

    async def daily_report(self) -> None:
        if self._report_fn is None:
            return
        html, markup = await self._report_fn()
        key = f"daily_report:{self.clock.now():%Y-%m-%d}"
        if markup is not None:
            await self.notifier.send(html, dedup_key=key, reply_markup=markup)  # type: ignore[call-arg]
        else:
            await self.notifier.send(html, dedup_key=key)

    # ── BotControl ──────────────────────────────────────────────────────

    async def status(self) -> dict[str, Any]:
        pos_dict: dict[str, Any] | None = None
        try:
            pos = await self.executor.get_position()
            if pos is not None and pos.size > 0:
                pos_dict = {
                    "symbol": pos.symbol, "side": pos.side, "size": pos.size,
                    "entry_price": pos.entry_price, "unrealized_pnl": pos.unrealized_pnl,
                    "leverage": pos.leverage, "liquidation_price": pos.liquidation_price,
                }
        except Exception:
            if self.trader.has_position:  # fall back to the bot's own belief
                pos_dict = {"symbol": SYMBOL, "side": "long", "size": self.trader.position_qty,
                            "entry_price": self.trader.entry_price, "unrealized_pnl": None,
                            "leverage": self._leverage, "liquidation_price": None}
        equity = self._last_equity
        try:
            bal = await self.executor.get_balance()
            equity = float(bal.get("wallet") or bal.get("total") or 0.0) or equity
        except Exception:
            pass
        blocked, why = self.state.entries_blocked()
        sig = self.trader.last_signal
        last_signal = dataclasses.asdict(sig) if sig is not None else await self._last_signal_from_db()
        last_bar = self.trader.last_bar_ts
        return {
            "equity": equity,
            "position": pos_dict,
            "halted": blocked,
            "halt_reason": why or None,
            "kill_level": self.risk.level.name,
            "last_bar_ts": datetime.fromtimestamp(last_bar / 1000, tz=timezone.utc) if last_bar else None,
            "last_signal": last_signal,
            "next_bar_at": self._next_bar_at,
            "daily_dd_pct": self.risk.daily_dd * 100.0,
            "weekly_dd_pct": self.risk.weekly_dd * 100.0,
            "monthly_dd_pct": self.risk.monthly_dd * 100.0,
            "heartbeat_at": self._last_heartbeat,
        }

    async def _last_signal_from_db(self) -> dict[str, Any] | None:
        try:
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM supertrend_signals ORDER BY bar_ts DESC LIMIT 1")
            return dict(row) if row else None
        except Exception:
            return None

    async def emergency_close(self, requested_by: str) -> dict[str, Any]:
        """Halt entries (persisted), then close any position with a reduce-only
        market order. Works while blocked: the executor never consults the gate
        for reduce-only orders."""
        log.critical("emergency_close_requested", by=requested_by)
        had_position = True  # unknown until proven flat
        try:
            pos = await self.executor.get_position()
            had_position = pos is not None and pos.size > 0
        except Exception:
            log.exception("emergency_position_read_error")
        try:
            await self.risk.manual_halt(requested_by)
        except Exception:
            log.exception("emergency_halt_error")  # closing matters more than the flag

        closed = False
        detail = ""
        flat = False
        for attempt in (1, 2):
            try:
                res = await self.executor.close_position("emergency_close")
                closed = closed or (res is not None and res.any_fill)
                detail = f"close_position: {getattr(res, 'status', None)}"
            except Exception as exc:
                log.exception("emergency_close_error")
                detail = f"{type(exc).__name__}: {str(exc)[:200]}"
            try:
                pos = await self.executor.get_position()
                flat = pos is None or pos.size <= 0
            except Exception:
                flat = False
            if flat:
                break
        await self.trader.after_forced_close("emergency_close")
        if not flat:
            await self.notifier.anomaly(
                kind="emergency_close_not_flat",
                message=f"긴급 청산 후에도 거래소가 flat이 아닙니다 ({detail}). 수동 확인 필요.",
            )
        else:
            await self.notifier.send(
                "<b>긴급 청산 완료</b>\n거래소 flat 확인. 진입은 /resume 전까지 차단됩니다."
                if had_position else
                "<b>진입 정지</b>\n보유 포지션 없음. 진입은 /resume 전까지 차단됩니다."
            )
        return {"had_position": had_position, "closed": closed, "flat": flat, "detail": detail}

    async def resume(self, requested_by: str) -> dict[str, Any]:
        was = await self.risk.resume(requested_by)
        blocked, _ = self.state.entries_blocked()
        if was:
            await self.notifier.send(f"<b>수동 정지 해제</b> ({requested_by}) — 진입 재개"
                                     + (" (포트폴리오 쿨다운 차단은 유지)" if blocked else ""))
        return {"was_halted": was, "halted": blocked}

    # ── shutdown ────────────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Stop everything; the position stays open (restart recovery is on_start)."""
        self._stop.set()
        await self.trader.shutdown()
        for _, stop in reversed(self._services):
            if stop is not None:
                try:
                    await stop()
                except Exception:
                    log.exception("service_stop_error")
        if self._started:
            try:
                await self.notifier.send("<b>봇 종료</b> — 포지션은 유지됩니다.")
            except Exception:
                pass
        try:
            await self.exchange.disconnect()
        except Exception:
            log.exception("exchange_disconnect_error")
        self._started = False


__all__ = ["BotApp", "bar_boundary_ms", "MANUAL_HALT"]
