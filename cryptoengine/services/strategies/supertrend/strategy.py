"""Supertrend 4h Long-Only Strategy for BTC/USDT:USDT perpetuals.

Signals: Supertrend direction + EMA cross + direction filter
Position: Long-only, leverage 3x, ATR-based exit
Data: 4h candles from Redis channel
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

from .indicators import compute_atr, compute_ema, compute_supertrend

logger = structlog.get_logger()

# ── Strategy Parameters (combo #173) ────────────────────────────────────

ST_FACTOR = 2.4
ST_PERIOD = 8
FAST_EMA_LEN = 7
SLOW_EMA_LEN = 27
DIR_EMA_LEN = 230
ATR_MULT = 3.2
LEVERAGE = 3

# ── Operational Constants ──────────────────────────────────────────────

SYMBOL = "BTC/USDT:USDT"
TIMEFRAME = "4h"
CANDLE_LOOKBACK = 300
_4H_MS = 14_400_000  # Redis ts is in milliseconds

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

        # Background subscription task
        self._sub_task: asyncio.Task | None = None

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

        # Backfill candles
        try:
            ohlcv_list = await self._exchange.get_ohlcv(
                SYMBOL, TIMEFRAME, limit=CANDLE_LOOKBACK
            )
            for candle in ohlcv_list:
                # get_ohlcv returns OHLCV pydantic models (shared/models/market.py)
                # with attributes: open, high, low, close, volume, timestamp (datetime)
                candle_dict = {
                    "open": float(candle.open),
                    "high": float(candle.high),
                    "low": float(candle.low),
                    "close": float(candle.close),
                    "volume": float(candle.volume),
                    "ts": int(candle.timestamp.timestamp() * 1000),  # → ms, matches Redis ts
                }
                self._candles.append(candle_dict)

            self._log.info(
                "candles_backfilled",
                count=len(self._candles),
                oldest_close=self._candles[0]["close"] if self._candles else None,
                latest_close=self._candles[-1]["close"] if self._candles else None,
            )
        except Exception:
            self._log.exception("candle_backfill_error")

        # Start background subscription task
        self._sub_task = asyncio.create_task(self._subscribe_market_data())

        self._log.info(
            STRATEGY_STARTED,
            message="Supertrend 4h 전략 시작 완료",
            candles=len(self._candles),
        )

    async def on_stop(self, reason: str) -> None:
        """Stop strategy: cancel subscription, liquidate if needed, disconnect."""
        self._log.info(STRATEGY_STOPPING, message="Supertrend 전략 종료 중", reason=reason)

        # Cancel subscription task
        if self._sub_task:
            self._sub_task.cancel()
            try:
                await self._sub_task
            except asyncio.CancelledError:
                pass

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
        # If no new confirmed bar, nothing to do
        if not self._new_bar:
            return

        self._new_bar = False

        # Need minimum data for indicators
        min_bars = max(self.st_period, self.dir_ema_len, self.slow_ema_len) + 20
        if len(self._candles) < min_bars:
            self._log.debug(
                "insufficient_candles",
                have=len(self._candles),
                need=min_bars,
            )
            return

        # Build DataFrame from deque
        df = pd.DataFrame(list(self._candles))

        # Compute indicators
        try:
            st_dir = compute_supertrend(df, self.st_period, self.st_factor)
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

        # ── Pre-compute expected action for signal logging ──────────────
        # Capture pre-decision state before any order submission
        had_position = self._has_position
        entry_ok = False
        exit_signal = False
        exit_reason: str | None = None
        expected_action = "hold"
        expected_qty: float | None = None
        expected_stop_loss: float | None = None

        if not had_position:
            entry_ok = (
                st_dir == 1
                and fast_ema > slow_ema
                and price > dir_ema
                and self._last_bar_ts > self._last_liquidation_ts
                and self._last_bar_ts > self._atr_cooldown_until
            )
            if entry_ok:
                expected_action = "enter"
                expected_qty = (self.allocated_capital * 0.95 * self.leverage) / price
                expected_stop_loss = price * (1 - 0.70 / self.leverage)
        else:
            ema_cross_exit = fast_ema < slow_ema
            atr_stop = atr_14 * self.atr_mult
            atr_distance_exit = abs(price - self._entry_price) >= atr_stop
            if ema_cross_exit or atr_distance_exit:
                exit_signal = True
                exit_reason = "ema_cross" if ema_cross_exit else "atr_distance"
                expected_action = "exit"

        # Persist signal asynchronously (fire-and-forget, never blocks trading)
        asyncio.create_task(
            self._persist_signal(
                bar_ts=self._last_bar_ts,
                st_dir=st_dir,
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

        # ── Entry Signal ────────────────────────────────────────────────

        if entry_ok:
            await self._enter_long(price)
            return

        # ── Exit Signal (has position) ──────────────────────────────────

        if exit_signal:
            await self._exit_long(exit_reason, exit_reason == "atr_distance")
            return

    # ── Signal Persistence ─────────────────────────────────────────────

    async def _persist_signal(
        self,
        bar_ts: int,
        st_dir: int,
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
                        bar_ts, computed_at, st_dir, fast_ema, slow_ema, dir_ema,
                        price, atr_14, allocated_capital, had_position, entry_ok,
                        exit_signal, exit_reason, expected_action,
                        expected_qty, expected_stop_loss
                    ) VALUES (
                        to_timestamp($1::bigint / 1000.0), NOW(),
                        $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
                    )
                    ON CONFLICT (bar_ts) DO NOTHING
                    """,
                    bar_ts,
                    st_dir,
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
        except Exception:
            self._log.exception("signal_persist_error")

    # ── Order Submission ───────────────────────────────────────────────

    async def _enter_long(self, price: float) -> None:
        """Submit a long entry order."""
        # Position sizing: 95% of capital * leverage / price
        qty = (self.allocated_capital * 0.95 * self.leverage) / price

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
            order_type="market",
            quantity=qty,
            price=None,
            post_only=False,
            reduce_only=False,
            stop_loss=stop_loss_price,
        )

        try:
            await self.submit_order(order)
            self._has_position = True
            self._position_qty = qty
            self._entry_price = price

            self._log.info(
                "entry_order_submitted",
                quantity=round(qty, 4),
                entry_price=round(price, 2),
                notional=round(qty * price, 2),
                stop_loss=round(stop_loss_price, 2),
            )
        except Exception:
            self._log.exception("entry_order_error")

    async def _exit_long(self, reason: str, atr_triggered: bool = False) -> None:
        """Submit a long exit order."""
        if not self._has_position:
            return

        order = OrderRequest(
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

        try:
            closed_qty = self._position_qty
            await self.submit_order(order)
            self._has_position = False
            self._position_qty = 0.0

            # Set cooldown if ATR-triggered (ts in ms, cooldown = 1 bar = 4h)
            if atr_triggered:
                self._atr_cooldown_until = self._last_bar_ts + _4H_MS

            self._last_liquidation_ts = self._last_bar_ts

            self._log.info(
                "exit_order_submitted",
                reason=reason,
                atr_triggered=atr_triggered,
                closed_qty=round(closed_qty, 4),
            )
        except Exception:
            self._log.exception("exit_order_error")

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
                    confirmed = data.get("confirmed", False)

                    if confirmed:
                        candle = {
                            "open": float(data.get("open", 0)),
                            "high": float(data.get("high", 0)),
                            "low": float(data.get("low", 0)),
                            "close": float(data.get("close", 0)),
                            "volume": float(data.get("volume", 0)),
                            "ts": int(data.get("ts", int(time.time()))),
                        }
                        self._candles.append(candle)
                        self._new_bar = True

                        self._log.debug(
                            "candle_received",
                            close=candle["close"],
                            ts=candle["ts"],
                            total_candles=len(self._candles),
                        )
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
