"""Market Regime Detector — classifies current market conditions.

Uses ADX, Bollinger Band width, ATR, and EMA to produce a regime label:
  - ranging       : ADX < 20 and BB width < median
  - trending_up   : ADX > 25 and price > EMA20
  - trending_down : ADX > 25 and price < EMA20
  - volatile      : ATR > avg_ATR * 2.0

Publishes a ``MarketRegime`` message to Redis channel ``market:regime``
every time a new confirmed 5m candle arrives.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import redis.asyncio as aioredis
import structlog

from indicators import compute_adx, compute_atr, compute_bb, compute_ema
from shared.log_events import *

log = structlog.get_logger(__name__)

# Minimum number of candles needed before detection can begin
MIN_CANDLES = 50

# Regime thresholds
ADX_TRENDING_THRESHOLD = 25.0
ADX_RANGING_THRESHOLD = 20.0
ATR_VOLATILE_MULTIPLIER = 2.0


class RegimeDetector:
    """Subscribes to OHLCV updates and classifies the market regime."""

    def __init__(
        self,
        *,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
        symbol: str,
        exchange: str,
        detection_timeframe: str = "5m",
        lookback: int = 200,
    ) -> None:
        self.redis = redis
        self.db_pool = db_pool
        self.symbol = symbol
        self.exchange = exchange
        self.detection_timeframe = detection_timeframe
        self.lookback = lookback

        self._candles: list[dict[str, Any]] = []
        self._last_regime: str | None = None

        # 확정 로직 상태
        self._pending_regime: str | None = None
        self._consecutive_count: int = 0
        self._confirmed_regime: str | None = None
        self._confirmation_threshold: int = 3
        self._last_confirmed_regime: str | None = None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Main loop — listen for confirmed candles and run detection."""
        log.info(SERVICE_STARTED, message="regime detector starting", symbol=self.symbol, tf=self.detection_timeframe)

        # Pre-load historical candles from DB
        await self._load_history()

        channel = f"market:ohlcv:{self.exchange}:{self.symbol}:{self.detection_timeframe}"
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel)

        try:
            while not shutdown.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg is None:
                    continue

                payload = json.loads(msg["data"])

                # Only process confirmed (closed) candles
                if not payload.get("confirmed", False):
                    continue

                self._append_candle(payload)
                await self._detect_and_publish()

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            log.info(SERVICE_STOPPED, message="regime detector stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _load_history(self) -> None:
        """Load recent confirmed candles from PostgreSQL."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts, open, high, low, close, volume
                FROM ohlcv
                WHERE exchange = $1 AND symbol = $2 AND timeframe = $3
                ORDER BY ts DESC
                LIMIT $4
                """,
                self.exchange,
                self.symbol,
                self.detection_timeframe,
                self.lookback,
            )

        for row in reversed(rows):
            self._candles.append({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "ts": row["ts"].timestamp() * 1000,
            })

        log.info(SERVICE_HEALTH_OK, message="regime history loaded", candle_count=len(self._candles))

    def _append_candle(self, payload: dict[str, Any]) -> None:
        """Append a new candle and trim to lookback window."""
        self._candles.append({
            "open": float(payload["open"]),
            "high": float(payload["high"]),
            "low": float(payload["low"]),
            "close": float(payload["close"]),
            "volume": float(payload["volume"]),
            "ts": payload["ts"],
        })
        if len(self._candles) > self.lookback:
            self._candles = self._candles[-self.lookback:]

    def _build_df(self) -> pd.DataFrame:
        """Convert internal candle buffer to a DataFrame."""
        df = pd.DataFrame(self._candles)
        df.columns = ["open", "high", "low", "close", "volume", "ts"]
        return df

    async def _detect_and_publish(self) -> None:
        """Run regime classification logic and publish to Redis."""
        if len(self._candles) < MIN_CANDLES:
            log.debug(MARKET_REGIME_CHANGED, message="insufficient data for regime detection", candles=len(self._candles))
            return

        df = self._build_df()

        # Compute indicators
        adx_values = compute_adx(df, period=14)
        atr_values = compute_atr(df, period=14)
        bb_upper, bb_mid, bb_lower = compute_bb(df, period=20, std_dev=2.0)
        ema20 = compute_ema(df, period=20)

        current_adx = adx_values.iloc[-1]
        current_atr = atr_values.iloc[-1]
        current_close = df["close"].iloc[-1]
        current_ema20 = ema20.iloc[-1]
        current_bb_upper = bb_upper.iloc[-1]
        current_bb_lower = bb_lower.iloc[-1]
        bb_width = (current_bb_upper - current_bb_lower) / bb_mid.iloc[-1] if bb_mid.iloc[-1] != 0 else 0.0

        # Median BB width over lookback
        bb_widths = (bb_upper - bb_lower) / bb_mid
        bb_widths = bb_widths.replace([np.inf, -np.inf], np.nan).dropna()
        median_bb_width = float(bb_widths.median()) if len(bb_widths) > 0 else bb_width

        # Average ATR over lookback
        avg_atr = float(atr_values.mean())

        # --- Classification ---
        regime: str
        confidence: float

        if not np.isnan(current_atr) and avg_atr > 0 and current_atr > avg_atr * ATR_VOLATILE_MULTIPLIER:
            regime = "volatile"
            confidence = min(1.0, current_atr / (avg_atr * ATR_VOLATILE_MULTIPLIER * 1.5))
        elif current_adx < ADX_RANGING_THRESHOLD and bb_width < median_bb_width:
            regime = "ranging"
            adx_factor = max(0.0, 1.0 - current_adx / ADX_RANGING_THRESHOLD)
            bb_factor = max(0.0, 1.0 - bb_width / median_bb_width) if median_bb_width > 0 else 0.5
            confidence = (adx_factor + bb_factor) / 2.0
        elif current_adx > ADX_TRENDING_THRESHOLD and current_close > current_ema20:
            regime = "trending_up"
            confidence = min(1.0, (current_adx - ADX_TRENDING_THRESHOLD) / 50.0 + 0.5)
        elif current_adx > ADX_TRENDING_THRESHOLD and current_close < current_ema20:
            regime = "trending_down"
            confidence = min(1.0, (current_adx - ADX_TRENDING_THRESHOLD) / 50.0 + 0.5)
        else:
            # Ambiguous — lean toward ranging with low confidence
            regime = "ranging"
            confidence = 0.3

        confidence = round(float(np.clip(confidence, 0.0, 1.0)), 4)

        # ── 연속 횟수 추적 및 확정 판단 ──
        if regime == self._pending_regime:
            self._consecutive_count += 1
        else:
            self._pending_regime = regime
            self._consecutive_count = 1

        is_confirmed = self._consecutive_count >= self._confirmation_threshold
        newly_confirmed = is_confirmed and regime != self._confirmed_regime

        change_reason: str | None = None
        if newly_confirmed:
            self._confirmed_regime = regime
            change_reason = self._build_reason(
                regime, self._consecutive_count,
                float(current_adx), float(current_atr), confidence,
            )

        # Publish raw regime
        regime_msg = {
            "regime": regime,
            "confidence": confidence,
            "adx": round(float(current_adx), 4),
            "volatility": round(float(current_atr), 4),
            "bb_width": round(float(bb_width), 6),
            "detected_at": datetime.now(tz=timezone.utc).isoformat(),
            "consecutive": self._consecutive_count,
        }

        await self.redis.publish("market:regime", json.dumps(regime_msg))
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        cache_mapping = {k: str(v) for k, v in regime_msg.items()}
        cache_mapping["raw_regime_at"] = now_iso
        cache_mapping["consecutive_count"] = str(self._consecutive_count)
        if self._confirmed_regime:
            cache_mapping["confirmed_regime"] = self._confirmed_regime
        await self.redis.hset("cache:regime", mapping=cache_mapping)

        # 확정 시점에 별도 채널 발행 및 Redis 키 갱신
        if newly_confirmed:
            confirmed_msg = {**regime_msg, "reason": change_reason}
            await self.redis.publish("market:regime:confirmed", json.dumps(confirmed_msg))
            await self.redis.set("market:regime:confirmed", json.dumps(confirmed_msg), ex=3600)
            confirmed_now_iso = datetime.now(tz=timezone.utc).isoformat()
            await self.redis.hset("cache:regime", mapping={
                "confirmed_regime": regime,
                "confirmed_at": confirmed_now_iso,
            })
            log.info(
                MARKET_REGIME_CHANGED,
                message="regime confirmed",
                new=regime,
                consecutive=self._consecutive_count,
                reason=change_reason,
            )

        # Persist to DB
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_regime_history
                  (symbol, regime, confidence, indicators, consecutive_count, is_confirmed, change_reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                self.symbol,
                regime,
                confidence,
                json.dumps({
                    "adx": round(float(current_adx), 4),
                    "volatility": round(float(current_atr), 4),
                    "bb_width": round(float(bb_width), 6),
                }),
                self._consecutive_count,
                is_confirmed,
                change_reason,
            )

            # regime_raw_log 저장 (매 캔들마다)
            await conn.execute(
                """
                INSERT INTO regime_raw_log
                  (symbol, regime, confidence, adx, atr, bb_width, is_confirmed, consecutive_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                self.symbol,
                regime,
                confidence,
                round(float(current_adx), 4),
                round(float(current_atr), 4),
                round(float(bb_width), 6),
                is_confirmed,
                self._consecutive_count,
            )

            # regime_transitions 저장 (확정 레짐이 변경될 때만)
            if newly_confirmed and regime != self._last_confirmed_regime:
                old_regime = self._last_confirmed_regime if self._last_confirmed_regime else "unknown"
                await conn.execute(
                    """
                    INSERT INTO regime_transitions
                      (symbol, previous_regime, new_regime, transition_type, confirmed, confirmed_at)
                    VALUES ($1, $2, $3, 'confirmed', TRUE, NOW())
                    """,
                    self.symbol,
                    old_regime,
                    regime,
                )
                self._last_confirmed_regime = regime

        if regime != self._last_regime:
            log.info(
                MARKET_REGIME_CHANGED,
                message="regime changed",
                old=self._last_regime,
                new=regime,
                confidence=confidence,
                adx=round(float(current_adx), 2),
                consecutive=self._consecutive_count,
            )
            self._last_regime = regime
        else:
            log.debug(
                MARKET_REGIME_CHANGED,
                message="regime unchanged",
                regime=regime,
                confidence=confidence,
                consecutive=self._consecutive_count,
            )

    @staticmethod
    def _build_reason(regime: str, count: int, adx: float, atr: float, conf: float) -> str:
        """확정 레짐 변경 근거 문자열 생성."""
        REGIME_KO = {
            "trending_up": "상승 추세",
            "trending_down": "하락 추세",
            "ranging": "횡보",
            "volatile": "고변동성",
        }
        label = REGIME_KO.get(regime, regime)
        if regime == "volatile":
            return (
                f"{count}회 연속 {label} 감지 "
                f"(ATR×{ATR_VOLATILE_MULTIPLIER:.0f} 초과, 신뢰도={conf:.0%}) → 레짐 확정"
            )
        return (
            f"{count}회 연속 {label} 감지 "
            f"(ADX={adx:.1f}, 신뢰도={conf:.0%}) → 레짐 확정"
        )
