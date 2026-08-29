"""Supertrend 4h Long-Only Strategy for BTC/USDT:USDT perpetuals.

Signals: Supertrend direction + EMA cross + direction filter
Position: Long-only, leverage 3x, ATR stop-loss (no ATR take-profit)
Data: 4h candles from Redis channel

상태 관리 원칙 (2026-05-27 미체결 사고 재발 방지):
  - 진실은 거래소다. 매 봉 신호 판단 전 get_position으로 내부 상태를 교정한다.
  - 주문 제출은 낙관적 상태 갱신 없이 pending으로 추적하고,
    order:result 수신 또는 포지션 폴링으로만 확정한다.
  - exit 거부 시 60초 후 1회 재시도, entry 거부는 재시도하지 않는다 (스킵이 안전).
  - 봉 마감 메시지 누락은 워치독이 REST 백필로 복구한다.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import deque
from typing import Any

import pandas as pd
import structlog

from shared.exchange import ExchangeConnector, exchange_factory
from shared.log_events import *
from shared.models.order import OrderRequest
from shared.models.strategy import StrategyStatus
from shared.redis_client import RedisClient

# Allow import of base_strategy from parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from base_strategy import BaseStrategy  # noqa: E402

from .indicators import compute_atr, compute_ema, compute_supertrend  # returns (dir, line)

logger = structlog.get_logger()

# ── Strategy Parameters (combo #7908) ───────────────────────────────────

ST_FACTOR = 2.6
ST_PERIOD = 9
FAST_EMA_LEN = 7
SLOW_EMA_LEN = 29
DIR_EMA_LEN = 240
ATR_MULT = 3.3
LEVERAGE = 3

# ── Operational Constants ──────────────────────────────────────────────

SYMBOL = "BTC/USDT:USDT"
TIMEFRAME = "4h"
# dir_ema(240) 시드는 (1-k)^L 로 감쇠한다 — L=300이면 ~8% 잔존해 dir_ema가
# 백테스트(전체 히스토리)와 최대 ~3.8% 어긋나고 진입필터가 갈린다. L=1000이면
# 잔차 <0.03%로 사실상 정합 (Bybit get_ohlcv 단일요청 최대치이기도 하다).
# 검증: tests/unit/test_supertrend_parity.py::window_stability
CANDLE_LOOKBACK = 1000
_4H_MS = 14_400_000  # Redis ts is in milliseconds

# 주문 확정 추적 (execution-engine ORDER_TIMEOUT=420s와 정렬)
PENDING_DEADLINE_S = 450.0   # 결과/체결 확인 대기 한도
PENDING_POLL_S = 20.0        # pending 중 거래소 포지션 백스톱 폴링 주기
EXIT_RETRY_DELAY_S = 60.0    # exit 거부 시 재시도 대기
BAR_GRACE_MS = 600_000       # 봉 마감 후 10분까지 미수신이면 피드 장애로 간주
BALANCE_CACHE_KEY = "cache:balance:bybit"  # execution-engine이 60초마다 갱신

# ── Shutdown modes that skip liquidation ────────────────────────────────

_SHUTDOWN_NO_LIQUIDATE = frozenset({"service_shutdown"})


class SupertrendLiveStrategy(BaseStrategy):
    """Supertrend 4h long-only strategy."""

    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        super().__init__(strategy_id, config)

        # Load hyperparameters from config with defaults
        self.st_factor = config.get("st_factor", ST_FACTOR)
        self.st_period = config.get("st_period", ST_PERIOD)
        self.fast_ema_len = config.get("fast_ema_len", FAST_EMA_LEN)
        self.slow_ema_len = config.get("slow_ema_len", SLOW_EMA_LEN)
        self.dir_ema_len = config.get("dir_ema_len", DIR_EMA_LEN)
        self.atr_mult = config.get("atr_mult", ATR_MULT)
        self.leverage = config.get("leverage", LEVERAGE)

        # Exchange connector (initialized in on_start)
        self._exchange: ExchangeConnector | None = None

        # OHLCV deque for technical analysis
        self._candles: deque = deque(maxlen=CANDLE_LOOKBACK)

        # Position tracking
        self._has_position = False
        self._position_qty = 0.0
        self._entry_price = 0.0

        # Cooldown tracking (epoch seconds)
        self._last_liquidation_ts = 0
        self._atr_cooldown_until = 0
        self._last_bar_ts = 0

        # Signal latch: set by Redis subscription, consumed by tick
        self._new_bar = False

        # Background subscription tasks
        self._sub_task: asyncio.Task | None = None
        self._result_sub_task: asyncio.Task | None = None

        # 미확정 주문 추적: {request_id, action, qty, bar_ts, ref_price,
        #   exit_reason, atr_triggered, retried, deadline}
        # pending 중에는 tick이 새 주문을 내지 않고, 확정(결과 수신/포지션 폴링)
        # 시에만 내부 포지션 상태를 갱신한다.
        self._pending_order: dict[str, Any] | None = None
        self._pending_watch_task: asyncio.Task | None = None

        # 봉 워치독: 마지막으로 백필을 시도한 봉 시작 ts (봉당 1회 제한)
        self._watchdog_attempted_ts = 0

        # Strong references to fire-and-forget tasks (prevents GC before completion)
        self._bg_tasks: set[asyncio.Task] = set()

        self._log = logger.bind(strategy_id=strategy_id, strategy="supertrend")

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialize exchange, backfill candles, and start Redis subscription."""
        self.allocated_capital = capital

        self._log.info(
            "strategy_start_init",
            message="Supertrend 4h 전략 초기화 중",
            capital=capital,
            symbol=SYMBOL,
            leverage=self.leverage,
        )

        # Ensure supertrend_signals table exists (self-create, idempotent)
        if self._db_pool:
            try:
                async with self._db_pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS supertrend_signals (
                            id                 BIGSERIAL PRIMARY KEY,
                            bar_ts             TIMESTAMPTZ NOT NULL,
                            computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            st_dir             SMALLINT    NOT NULL,
                            st_line            DOUBLE PRECISION,
                            fast_ema           DOUBLE PRECISION NOT NULL,
                            slow_ema           DOUBLE PRECISION NOT NULL,
                            dir_ema            DOUBLE PRECISION NOT NULL,
                            price              DOUBLE PRECISION NOT NULL,
                            atr_14             DOUBLE PRECISION NOT NULL,
                            allocated_capital  DOUBLE PRECISION NOT NULL,
                            had_position       BOOLEAN NOT NULL,
                            entry_ok           BOOLEAN NOT NULL,
                            exit_signal        BOOLEAN NOT NULL,
                            exit_reason        VARCHAR(20),
                            expected_action    VARCHAR(10) NOT NULL,
                            expected_qty       DOUBLE PRECISION,
                            expected_stop_loss DOUBLE PRECISION
                        )
                    """)
                    # Idempotent migration: add st_line if not present (pre-existing DBs)
                    await conn.execute("""
                        ALTER TABLE supertrend_signals
                            ADD COLUMN IF NOT EXISTS st_line DOUBLE PRECISION
                    """)
                    await conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_supertrend_signals_bar_ts
                            ON supertrend_signals (bar_ts)
                    """)
            except Exception:
                self._log.exception("signal_table_create_error")

        # Create exchange connector
        self._exchange = exchange_factory(
            "bybit",
            api_key=os.environ.get("BYBIT_API_KEY", ""),
            api_secret=os.environ.get("BYBIT_API_SECRET", ""),
            testnet=os.environ.get("BYBIT_TESTNET", "true").lower() == "true",
        )
        await self._exchange.connect()

        # Set margin mode and leverage
        try:
            await self._exchange.set_margin_mode(SYMBOL, "isolated")
            await self._exchange.set_leverage(SYMBOL, self.leverage)
            self._log.info(
                "exchange_config_set",
                margin_mode="isolated",
                leverage=self.leverage,
            )
        except Exception:
            self._log.exception("exchange_config_error")

        # Backfill candles — 확정 봉만 (진행 중 봉이 섞이면 라이브 확정 봉과
        # 같은 ts가 중복돼 지표가 왜곡된다)
        try:
            now_ms = int(time.time() * 1000)
            ohlcv_list = await self._exchange.get_ohlcv(
                SYMBOL, TIMEFRAME, limit=CANDLE_LOOKBACK
            )
            for candle in ohlcv_list:
                candle_dict = self._ohlcv_to_candle(candle)
                if candle_dict["ts"] + _4H_MS <= now_ms:  # 마감된 봉만
                    self._candles.append(candle_dict)

            self._log.info(
                "candles_backfilled",
                count=len(self._candles),
                oldest_close=self._candles[0]["close"] if self._candles else None,
                latest_close=self._candles[-1]["close"] if self._candles else None,
            )
        except Exception:
            self._log.exception("candle_backfill_error")

        # 포지션 복구: Redis 캐시(스테일 가능) 대신 거래소 직접 조회 (진실 기반)
        await self._sync_position_from_exchange("on_start")

        # 재시작 중 놓친 봉 마감 처리: 마지막 처리 봉(supertrend_signals 기록)보다
        # 새 확정 봉이 있으면 즉시 신호를 계산한다. 사전 포지션 동기화 덕분에
        # 이미 처리된 봉을 재처리해도 멱등하다.
        try:
            if self._db_pool and self._candles:
                async with self._db_pool.acquire() as conn:
                    last_processed_ms = await conn.fetchval(
                        "SELECT (EXTRACT(EPOCH FROM MAX(bar_ts)) * 1000)::BIGINT FROM supertrend_signals"
                    )
                latest_ts = int(self._candles[-1]["ts"])
                if last_processed_ms is not None and latest_ts > int(last_processed_ms):
                    self._new_bar = True
                    self._log.warning(
                        "missed_bar_on_start",
                        message="재시작 중 놓친 확정 봉 발견 — 즉시 신호 처리",
                        last_processed_ts=int(last_processed_ms),
                        latest_bar_ts=latest_ts,
                    )
        except Exception:
            self._log.exception("missed_bar_check_error")

        # Start background subscription tasks
        self._sub_task = asyncio.create_task(self._subscribe_market_data())
        self._result_sub_task = asyncio.create_task(self._subscribe_order_results())

        self._log.info(
            STRATEGY_STARTED,
            message="Supertrend 4h 전략 시작 완료",
            candles=len(self._candles),
        )

    async def on_stop(self, reason: str) -> None:
        """Stop strategy: cancel subscription, liquidate if needed, disconnect."""
        self._log.info(STRATEGY_STOPPING, message="Supertrend 전략 종료 중", reason=reason)

        # Cancel background tasks
        for task in (self._sub_task, self._result_sub_task, self._pending_watch_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._sub_task = None
        self._result_sub_task = None
        self._pending_watch_task = None

        # 청산 판단 전 거래소 진실 동기화 — pending 중 종료 등으로 내부 상태가
        # 실제와 다를 수 있다 (service_shutdown은 청산하지 않으므로 생략)
        if reason not in _SHUTDOWN_NO_LIQUIDATE:
            await self._sync_position_from_exchange("on_stop")

        # Liquidate position if stopping for other reasons (not service shutdown)
        if reason not in _SHUTDOWN_NO_LIQUIDATE and self._has_position:
            try:
                self._log.info(
                    "position_liquidate_on_stop",
                    quantity=self._position_qty,
                    reason=reason,
                )
                exit_order = OrderRequest(
                    strategy_id=self.strategy_id,
                    exchange="bybit",
                    symbol=SYMBOL,
                    side="sell",
                    order_type="market",
                    quantity=self._position_qty,
                    price=None,
                    post_only=False,
                    reduce_only=True,
                )
                await self.submit_order(exit_order)
            except Exception:
                self._log.exception("position_liquidate_error")

        # Disconnect exchange
        if self._exchange:
            try:
                await self._exchange.disconnect()
            except Exception:
                self._log.exception("exchange_disconnect_error")

        self._log.info(STRATEGY_STOPPED, message="Supertrend 전략 종료 완료")

    async def get_status(self) -> StrategyStatus:
        """Return current strategy status."""
        return StrategyStatus(
            strategy_id=self.strategy_id,
            is_running=self.is_running,
            allocated_capital=self.allocated_capital,
            current_pnl=self.current_pnl,
            position_count=1 if self._has_position else 0,
        )

    async def _rebalance(self, new_capital: float) -> None:
        """Handle capital changes."""
        self._log.info(
            STRATEGY_REBALANCE,
            message="자본 변경",
            old_capital=self.allocated_capital,
            new_capital=new_capital,
        )
        # Do not resize position mid-trade; it will rebalance on next entry

    # ── Main Tick Loop ────────────────────────────────────────────────

    async def tick(self) -> None:
        """Process one tick: check for new bar, compute signals, enter/exit."""
        # 봉 피드 워치독 — 마감 메시지 누락 시 REST 백필로 래치 복구
        await self._bar_watchdog()

        # If no new confirmed bar, nothing to do
        if not self._new_bar:
            return

        # 직전 주문이 아직 미확정이면 래치를 유지한 채 보류 — 확정/시한 초과 후
        # 다음 틱에서 처리한다 (pending은 PENDING_DEADLINE_S 내에 반드시 해소됨)
        if self._pending_order is not None:
            return

        self._new_bar = False

        # Need minimum data for indicators
        min_bars = max(self.st_period, self.dir_ema_len, self.slow_ema_len) + 20
        if len(self._candles) < min_bars:
            self._log.warning(
                "insufficient_candles",
                message="지표 계산에 필요한 봉 부족 — 신호 건너뜀",
                have=len(self._candles),
                need=min_bars,
            )
            return

        # Build DataFrame from deque
        df = pd.DataFrame(list(self._candles))

        # Compute indicators
        try:
            st_dir, st_line = compute_supertrend(df, self.st_period, self.st_factor)
            fast_ema = compute_ema(df, self.fast_ema_len).iloc[-1]
            slow_ema = compute_ema(df, self.slow_ema_len).iloc[-1]
            dir_ema = compute_ema(df, self.dir_ema_len).iloc[-1]
            price = df["close"].iloc[-1]
            atr_14 = compute_atr(df, 14)

            self._last_bar_ts = int(self._candles[-1].get("ts", int(time.time())))
        except Exception:
            self._log.exception("indicator_computation_error")
            return

        self._log.debug(
            "supertrend_signals",
            st_dir=st_dir,
            fast_ema=round(fast_ema, 2),
            slow_ema=round(slow_ema, 2),
            dir_ema=round(dir_ema, 2),
            price=round(price, 2),
            atr_14=round(atr_14, 2),
        )

        # ── 신호 판단 전 거래소 진실 동기화 ─────────────────────────────
        # 내부 믿음이 아닌 실포지션 기준으로 entry/exit을 판단한다.
        # 주문 거부·유실로 발산했더라도 여기서 교정되므로 같은 봉에서
        # 올바른 신호가 나간다 (2026-05-27 미체결 사고 재발 방지의 핵심).
        await self._sync_position_from_exchange("pre_signal")

        # ── Pre-compute expected action for signal logging ──────────────
        # Capture pre-decision state before any order submission
        had_position = self._has_position
        entry_ok = False
        exit_signal = False
        exit_reason: str | None = None
        expected_action = "hold"
        expected_qty: float | None = None
        expected_stop_loss: float | None = None
        effective_capital = self.allocated_capital

        if not had_position:
            entry_ok = bool(
                st_dir == 1
                and fast_ema > slow_ema
                and price > dir_ema
                and self._last_bar_ts > self._last_liquidation_ts
                and self._last_bar_ts > self._atr_cooldown_until
            )
            if entry_ok:
                expected_action = "enter"
                effective_capital = await self._get_effective_capital()
                expected_qty = (effective_capital * 0.95 * self.leverage) / price
                expected_stop_loss = price * (1 - 0.70 / self.leverage)
        else:
            ema_cross_exit = fast_ema < slow_ema
            atr_stop = atr_14 * self.atr_mult
            # ATR is stop-loss only (downside). Upside rides until EMA death-cross.
            atr_distance_exit = price <= self._entry_price - atr_stop
            if ema_cross_exit or atr_distance_exit:
                exit_signal = True
                exit_reason = "ema_cross" if ema_cross_exit else "atr_distance"
                expected_action = "exit"

        # Persist signal asynchronously (fire-and-forget, never blocks trading)
        _task = asyncio.create_task(
            self._persist_signal(
                bar_ts=self._last_bar_ts,
                st_dir=st_dir,
                st_line=st_line,
                fast_ema=float(fast_ema),
                slow_ema=float(slow_ema),
                dir_ema=float(dir_ema),
                price=float(price),
                atr_14=float(atr_14),
                had_position=had_position,
                entry_ok=entry_ok,
                exit_signal=exit_signal,
                exit_reason=exit_reason,
                expected_action=expected_action,
                expected_qty=expected_qty,
                expected_stop_loss=expected_stop_loss,
            )
        )
        self._bg_tasks.add(_task)
        _task.add_done_callback(self._bg_tasks.discard)

        # ── Entry Signal ────────────────────────────────────────────────

        if entry_ok:
            await self._enter_long(price, effective_capital)
            return

        # ── Exit Signal (has position) ──────────────────────────────────

        if exit_signal:
            await self._exit_long(exit_reason, exit_reason == "atr_distance", price)
            return

    # ── Signal Persistence ─────────────────────────────────────────────

    async def _persist_signal(
        self,
        bar_ts: int,
        st_dir: int,
        st_line: float,
        fast_ema: float,
        slow_ema: float,
        dir_ema: float,
        price: float,
        atr_14: float,
        had_position: bool,
        entry_ok: bool,
        exit_signal: bool,
        exit_reason: str | None,
        expected_action: str,
        expected_qty: float | None,
        expected_stop_loss: float | None,
    ) -> None:
        """Persist the per-bar computed signal to supertrend_signals table."""
        if not self._db_pool:
            return
        try:
            async with self._db_pool.acquire() as conn:
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
                    bar_ts,
                    st_dir,
                    st_line,
                    fast_ema,
                    slow_ema,
                    dir_ema,
                    price,
                    atr_14,
                    self.allocated_capital,
                    had_position,
                    entry_ok,
                    exit_signal,
                    exit_reason,
                    expected_action,
                    expected_qty,
                    expected_stop_loss,
                )
        except Exception as exc:
            self._log.error(
                "signal_persist_error",
                exc_type=type(exc).__name__,
                exc_msg=str(exc),
                bar_ts=bar_ts,
            )

    # ── Order Submission ───────────────────────────────────────────────

    async def _enter_long(self, price: float, effective_capital: float | None = None) -> None:
        """Submit a long entry order.

        상태는 낙관적으로 갱신하지 않는다 — pending으로 추적하고 체결 확정
        (order:result 수신 또는 포지션 폴링) 시에만 반영한다.
        """
        # Position sizing: 95% of capital * leverage / price
        # effective_capital = min(할당자본, 실시간 equity) — implied leverage
        # 한도(3.0x) 사전 차단 방지 (사이징 2.85x는 한도 대비 여유 5%뿐)
        capital = effective_capital if effective_capital and effective_capital > 0 else self.allocated_capital
        qty = (capital * 0.95 * self.leverage) / price

        # Minimum order check (Bybit ~$65 notional, 0.001 BTC min)
        min_notional = 65.0
        if qty * price < min_notional:
            self._log.warning(
                "entry_order_too_small",
                quantity=qty,
                notional=qty * price,
                min_notional=min_notional,
            )
            return

        # Catastrophic backstop: equity stop at -70% = price drop 70%/LEVERAGE
        stop_loss_price = price * (1 - 0.70 / self.leverage)

        order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange="bybit",
            symbol=SYMBOL,
            side="buy",
            order_type="limit",
            quantity=qty,
            price=price,   # initial peg = bar close; repeg loop updates it
            post_only=True,
            reduce_only=False,
            stop_loss=stop_loss_price,
        )

        try:
            await self.submit_order(order)
        except Exception:
            self._log.exception("entry_order_error")
            return

        self._set_pending(
            order.request_id, "enter", qty=qty, bar_ts=self._last_bar_ts, ref_price=price
        )
        self._log.info(
            "entry_order_submitted",
            message="진입 주문 제출 — 체결 확정 대기",
            quantity=round(qty, 4),
            entry_price=round(price, 2),
            notional=round(qty * price, 2),
            stop_loss=round(stop_loss_price, 2),
            capital_used=round(capital, 2),
        )

    async def _exit_long(
        self,
        reason: str,
        atr_triggered: bool = False,
        price: float | None = None,
        is_retry: bool = False,
    ) -> None:
        """Submit a long exit order.

        쿨다운 타임스탬프(_last_liquidation_ts/_atr_cooldown_until)는 체결
        확정 시(_confirm_exit)에만 설정한다 — 거부된 청산이 쿨다운을 남기면 안 된다.
        """
        if not self._has_position:
            return

        order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange="bybit",
            symbol=SYMBOL,
            side="sell",
            order_type="limit" if price is not None else "market",
            quantity=self._position_qty,
            price=price,      # initial peg = bar close; repeg loop updates it
            post_only=price is not None,
            reduce_only=True,
        )

        try:
            await self.submit_order(order)
        except Exception:
            self._log.exception("exit_order_error")
            return

        self._set_pending(
            order.request_id,
            "exit",
            qty=self._position_qty,
            bar_ts=self._last_bar_ts,
            ref_price=price,
            exit_reason=reason,
            atr_triggered=atr_triggered,
            retried=is_retry,
        )
        self._log.info(
            "exit_order_submitted",
            message="청산 주문 제출 — 체결 확정 대기",
            reason=reason,
            atr_triggered=atr_triggered,
            closed_qty=round(self._position_qty, 4),
            is_retry=is_retry,
        )

    # ── Pending Order Tracking ─────────────────────────────────────────

    def _set_pending(
        self,
        request_id: str,
        action: str,
        *,
        qty: float,
        bar_ts: int,
        ref_price: float | None = None,
        exit_reason: str | None = None,
        atr_triggered: bool = False,
        retried: bool = False,
    ) -> None:
        """주문을 미확정 상태로 추적하고 백스톱 폴링을 기동한다."""
        self._pending_order = {
            "request_id": request_id,
            "action": action,
            "qty": qty,
            "bar_ts": bar_ts,
            "ref_price": ref_price,
            "exit_reason": exit_reason,
            "atr_triggered": atr_triggered,
            "retried": retried,
            "deadline": time.monotonic() + PENDING_DEADLINE_S,
        }
        if self._pending_watch_task and not self._pending_watch_task.done():
            self._pending_watch_task.cancel()
        self._pending_watch_task = asyncio.create_task(self._watch_pending_order())

    def _confirm_entry(self, filled_qty: float, filled_price: float | None, source: str) -> None:
        """진입 체결 확정 — 실체결값으로 상태 갱신 (ATR exit 정확도 개선)."""
        pending = self._pending_order
        if pending is None or pending["action"] != "enter":
            return
        self._has_position = True
        self._position_qty = filled_qty if filled_qty > 0 else float(pending["qty"])
        if filled_price and float(filled_price) > 0:
            self._entry_price = float(filled_price)
        elif pending.get("ref_price"):
            self._entry_price = float(pending["ref_price"])
        self._pending_order = None
        self._log.info(
            "entry_confirmed",
            message="진입 체결 확정",
            qty=round(self._position_qty, 6),
            entry_price=round(self._entry_price, 2),
            source=source,
        )

    def _confirm_exit(self, source: str) -> None:
        """청산 체결 확정 — 쿨다운 타임스탬프는 여기서만 설정한다."""
        pending = self._pending_order
        if pending is None or pending["action"] != "exit":
            return
        bar_ts = int(pending["bar_ts"])
        if pending.get("atr_triggered"):
            self._atr_cooldown_until = bar_ts + _4H_MS
        self._last_liquidation_ts = bar_ts
        self._has_position = False
        self._position_qty = 0.0
        self._entry_price = 0.0
        self._pending_order = None
        self._log.info(
            "exit_confirmed",
            message="청산 체결 확정",
            reason=pending.get("exit_reason"),
            source=source,
        )

    async def _handle_order_result(self, result: dict[str, Any]) -> None:
        """order:result 메시지 처리 — pending 주문의 확정/거부 반영."""
        pending = self._pending_order
        request_id = result.get("request_id")
        if pending is None or request_id != pending["request_id"]:
            self._log.debug(
                "order_result_ignored", request_id=request_id, status=result.get("status")
            )
            return

        status = result.get("status")
        if status == "filled":
            if pending["action"] == "enter":
                self._confirm_entry(
                    float(result.get("filled_qty") or 0.0),
                    result.get("filled_price"),
                    source="order_result",
                )
            else:
                self._confirm_exit(source="order_result")
            return

        if status in ("rejected", "partially_filled"):
            # rejected: 미체결. partially_filled: 시장가 폴백 실패 후 부분만 체결.
            # 어느 쪽이든 거래소 진실로 재동기화하고, exit이면 1회 재시도한다.
            action = pending["action"]
            was_retried = bool(pending.get("retried"))
            exit_reason = pending.get("exit_reason") or "exit"
            atr_triggered = bool(pending.get("atr_triggered"))
            self._pending_order = None

            self._log.error(
                "order_not_filled",
                message="주문 미체결 통보 수신 — 거래소 기준 재동기화",
                request_id=request_id,
                action=action,
                status=status,
                reason=result.get("reason", ""),
            )
            await self._sync_position_from_exchange("post_reject")

            if action == "exit" and self._has_position and not was_retried:
                self._schedule_exit_retry(exit_reason, atr_triggered)
            return

        # "new" 등 중간 상태 — order_manager는 최종 결과만 발행하므로 통상 없음

    def _schedule_exit_retry(self, reason: str, atr_triggered: bool) -> None:
        """exit 거부 60초 후 1회 재시도 (일시적 차단 자동 회복).

        Kill Switch 등 지속적 차단이면 재시도도 거부되고 ERROR 알림이 한 번 더
        발행된다 → 수동 대응. entry는 재시도하지 않는다 (스킵이 안전한 방향).
        """
        async def _retry() -> None:
            try:
                await asyncio.sleep(EXIT_RETRY_DELAY_S)
                if not self.is_running or self.is_paused:
                    return
                if self._pending_order is not None:
                    return
                await self._sync_position_from_exchange("exit_retry")
                if not self._has_position:
                    self._log.info(
                        "exit_retry_skipped", message="재시도 전 포지션 이미 청산됨"
                    )
                    return
                price = float(self._candles[-1]["close"]) if self._candles else None
                self._log.warning(
                    "exit_retry_submitting",
                    message="청산 주문 1회 재시도",
                    reason=reason,
                )
                await self._exit_long(reason, atr_triggered, price, is_retry=True)
            except asyncio.CancelledError:
                pass
            except Exception:
                self._log.exception("exit_retry_error")

        task = asyncio.create_task(_retry())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _watch_pending_order(self) -> None:
        """pending 백스톱: order:result가 유실돼도 거래소 포지션으로 확정한다."""
        try:
            while True:
                await asyncio.sleep(PENDING_POLL_S)
                pending = self._pending_order
                if pending is None:
                    return
                try:
                    pos = await self._exchange.get_position(SYMBOL) if self._exchange else None
                except Exception:
                    continue
                actual_qty = float(pos.size) if pos and pos.side == "long" else 0.0

                if pending["action"] == "enter" and actual_qty > 0:
                    self._confirm_entry(
                        actual_qty,
                        float(pos.entry_price) if pos else None,
                        source="position_poll",
                    )
                    return
                if pending["action"] == "exit" and actual_qty <= 0:
                    self._confirm_exit(source="position_poll")
                    return

                if time.monotonic() >= float(pending["deadline"]):
                    action = pending["action"]
                    was_retried = bool(pending.get("retried"))
                    exit_reason = pending.get("exit_reason") or "exit"
                    atr_triggered = bool(pending.get("atr_triggered"))
                    self._pending_order = None
                    self._log.error(
                        "pending_order_unresolved",
                        message="주문 확정 시한 초과 — 결과 미수신·기대 상태 미달, 재동기화",
                        request_id=pending["request_id"],
                        action=action,
                    )
                    await self._sync_position_from_exchange("pending_deadline")
                    if action == "exit" and self._has_position and not was_retried:
                        self._schedule_exit_retry(exit_reason, atr_triggered)
                    return
        except asyncio.CancelledError:
            pass
        except Exception:
            self._log.exception("pending_watch_error")

    # ── Position Truth Sync ────────────────────────────────────────────

    async def _sync_position_from_exchange(self, context: str) -> bool:
        """거래소 실포지션으로 내부 상태를 교정한다 (진실 = 거래소).

        발산(보유 여부 불일치) 감지 시 ERROR 로그(→ Telegram 알림)를 남기고
        실제값을 채택한다. 조회 실패 시 기존 믿음을 유지한다.
        """
        if self._exchange is None:
            return False
        try:
            pos = await self._exchange.get_position(SYMBOL)
        except Exception:
            self._log.exception("position_sync_error", context=context)
            return False

        if pos is not None and pos.side == "short":
            # Long-only 전략에 숏 포지션 — 외부 개입 의심, 자동 관리 대상 아님
            self._log.error(
                "unexpected_short_position",
                message="Long-only 전략 계정에 숏 포지션 감지 — 수동 확인 필요",
                size=pos.size,
                context=context,
            )
            actual_qty = 0.0
            actual_entry = 0.0
        else:
            actual_qty = float(pos.size) if pos else 0.0
            actual_entry = float(pos.entry_price) if pos else 0.0
        actual_has = actual_qty > 0

        if actual_has != self._has_position:
            self._log.error(
                "position_state_divergence",
                message="전략 내부 상태 ≠ 거래소 실포지션 — 실제값으로 강제 교정",
                context=context,
                believed_has_position=self._has_position,
                believed_qty=round(self._position_qty, 6),
                actual_qty=round(actual_qty, 6),
            )
        elif (
            actual_has
            and self._position_qty > 0
            and abs(actual_qty - self._position_qty) / actual_qty > 0.01
        ):
            self._log.warning(
                "position_size_drift",
                message="포지션 수량 드리프트 — 실제값으로 교정",
                context=context,
                believed_qty=round(self._position_qty, 6),
                actual_qty=round(actual_qty, 6),
            )

        self._has_position = actual_has
        self._position_qty = actual_qty
        if actual_has and actual_entry > 0:
            self._entry_price = actual_entry
        elif not actual_has:
            self._entry_price = 0.0
        return True

    async def _get_effective_capital(self) -> float:
        """min(할당자본, 실시간 equity) — safety implied leverage 차단 예방.

        equity가 할당자본보다 낮으면(미실현 손실·수수료 누적) 그만큼 줄여
        진입한다. 미진입(신호 무시)보다 축소 진입이 백테스트 동작에 가깝다.
        """
        try:
            raw = await self._redis.get(BALANCE_CACHE_KEY)
            if raw:
                equity = float(json.loads(raw).get("total", 0) or 0)
                if equity > 0:
                    if equity < self.allocated_capital:
                        self._log.info(
                            "entry_capital_reduced",
                            message="실시간 equity가 할당자본보다 낮아 축소 진입",
                            allocated=round(self.allocated_capital, 2),
                            equity=round(equity, 2),
                        )
                    return min(self.allocated_capital, equity)
        except Exception:
            self._log.warning(
                "equity_cache_read_error",
                message="equity 캐시 조회 실패 — 할당자본으로 사이징",
            )
        return self.allocated_capital

    # ── Redis Subscription ─────────────────────────────────────────────

    async def _subscribe_market_data(self) -> None:
        """Background task: subscribe to 4h OHLCV Redis channel."""
        try:
            pubsub = self._redis.client.pubsub()
            await pubsub.subscribe("market:ohlcv:bybit:BTCUSDT:4h")

            self._log.info(
                "ohlcv_subscription_started",
                channel="market:ohlcv:bybit:BTCUSDT:4h",
            )

            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue

                if msg["type"] != "message":
                    continue

                try:
                    data = json.loads(msg["data"])
                    if data.get("confirmed", False):
                        await self._ingest_candle(data)
                except json.JSONDecodeError:
                    self._log.warning("ohlcv_json_decode_error", data=msg["data"])
                except Exception:
                    self._log.exception("ohlcv_parse_error")

        except asyncio.CancelledError:
            self._log.info("ohlcv_subscription_cancelled")
        except Exception:
            self._log.exception("ohlcv_subscription_error")
        finally:
            try:
                await pubsub.close()
            except Exception:
                pass

    async def _ingest_candle(self, data: dict[str, Any]) -> None:
        """확정 봉 수신 처리: 중복 교체·과거 봉 무시·갭 백필 후 래치를 set한다."""
        candle = {
            "open": float(data.get("open", 0)),
            "high": float(data.get("high", 0)),
            "low": float(data.get("low", 0)),
            "close": float(data.get("close", 0)),
            "volume": float(data.get("volume", 0)),
            "ts": int(data.get("ts", int(time.time()))),
        }
        last_ts = int(self._candles[-1]["ts"]) if self._candles else 0
        ts = candle["ts"]

        if last_ts and ts == last_ts:
            # 같은 봉 재수신 (WS 재연결 재발행 등) — 중복 append 대신 교체
            self._candles[-1] = candle
        elif last_ts and ts < last_ts:
            self._log.warning("stale_candle_ignored", ts=ts, last_ts=last_ts)
            return
        else:
            if last_ts and ts > last_ts + _4H_MS:
                await self._backfill_gap(last_ts, ts)
            self._candles.append(candle)

        self._new_bar = True
        self._log.debug(
            "candle_received",
            close=candle["close"],
            ts=ts,
            total_candles=len(self._candles),
        )

    async def _backfill_gap(self, last_ts: int, new_ts: int) -> None:
        """수신 봉과 보유 봉 사이의 누락 구간을 REST로 백필한다."""
        missing = int((new_ts - last_ts) / _4H_MS) - 1
        if missing <= 0 or self._exchange is None:
            return
        self._log.error(
            "bar_gap_detected",
            message=f"4h 봉 갭 감지 — {missing}개 누락, REST 백필",
            last_ts=last_ts,
            new_ts=new_ts,
        )
        try:
            ohlcv_list = await self._exchange.get_ohlcv(
                SYMBOL, TIMEFRAME, limit=min(missing + 2, CANDLE_LOOKBACK)
            )
        except Exception:
            self._log.exception("bar_gap_backfill_error")
            return
        added = 0
        for c in ohlcv_list:
            candle = self._ohlcv_to_candle(c)
            if last_ts < candle["ts"] < new_ts:
                self._candles.append(candle)
                added += 1
        self._log.info("bar_gap_backfilled", added=added, missing=missing)

    async def _bar_watchdog(self) -> None:
        """봉 피드 워치독: 마감 봉을 제때 못 받으면 REST로 직접 복구한다.

        market-data 장애·재시작·pub/sub 유실로 확정 봉 메시지를 놓치면
        그 봉의 신호가 영구 누락된다 — 마감 후 BAR_GRACE_MS가 지나도록
        새 봉이 없으면 직접 조회한다 (봉당 1회 시도).
        """
        if not self._candles or self._exchange is None or not self.is_running:
            return
        now_ms = int(time.time() * 1000)
        last_ts = int(self._candles[-1]["ts"])

        # 마지막 보유 봉(시작 L)의 다음 봉은 L+8h에 마감된다
        if now_ms <= last_ts + 2 * _4H_MS + BAR_GRACE_MS:
            return

        latest_closed = ((now_ms // _4H_MS) - 1) * _4H_MS  # 마감된 가장 최근 봉의 시작 ts
        if self._watchdog_attempted_ts >= latest_closed:
            return
        self._watchdog_attempted_ts = latest_closed

        self._log.error(
            "bar_feed_stall",
            message="봉 마감 메시지 미수신 — REST 백필 시도 (피드 장애 의심)",
            last_ts=last_ts,
            now_ms=now_ms,
        )
        try:
            need = min(int((now_ms - last_ts) / _4H_MS) + 2, CANDLE_LOOKBACK)
            ohlcv_list = await self._exchange.get_ohlcv(SYMBOL, TIMEFRAME, limit=need)
        except Exception:
            self._log.exception("bar_watchdog_backfill_error")
            return

        added = 0
        for c in ohlcv_list:
            candle = self._ohlcv_to_candle(c)
            ts = candle["ts"]
            if ts > last_ts and ts + _4H_MS <= now_ms:  # 확정 봉만
                self._candles.append(candle)
                last_ts = ts
                added += 1
        if added:
            self._new_bar = True
            self._log.warning(
                "bar_watchdog_recovered",
                message="워치독이 누락 봉 복구 — 신호 처리 재개",
                added=added,
            )

    @staticmethod
    def _ohlcv_to_candle(candle: Any) -> dict[str, Any]:
        """OHLCV pydantic 모델 → 내부 캔들 dict (ts는 ms, Redis 포맷과 동일)."""
        return {
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "ts": int(candle.timestamp.timestamp() * 1000),
        }

    async def _subscribe_order_results(self) -> None:
        """Background task: 주문 결과 채널 구독 — 체결/거부를 전략 상태에 반영.

        기존에는 운영 서비스 어디에서도 order:result를 구독하지 않아 주문이
        거부돼도 전략이 모른 채 상태가 발산했다 (2026-05-27 사고의 구조적 원인).
        """
        try:
            pubsub = self._redis.client.pubsub()
            channel = f"order:result:{self.strategy_id}"
            await pubsub.subscribe(channel)

            self._log.info("order_result_subscription_started", channel=channel)

            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue

                if msg["type"] != "message":
                    continue

                try:
                    result = json.loads(msg["data"])
                    await self._handle_order_result(result)
                except json.JSONDecodeError:
                    self._log.warning(
                        "order_result_json_decode_error", data=str(msg.get("data"))[:200]
                    )
                except Exception:
                    self._log.exception("order_result_handle_error")

        except asyncio.CancelledError:
            self._log.info("order_result_subscription_cancelled")
        except Exception:
            self._log.exception("order_result_subscription_error")
        finally:
            try:
                await pubsub.close()
            except Exception:
                pass
