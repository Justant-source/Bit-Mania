"""Market Data Collector — WebSocket kline ingestion for Bybit perpetual futures.

Responsibilities:
  - WebSocket stream: kline.240 (4h) only
  - Publish confirmed and in-progress bars to Redis
  - Persist confirmed bars to PostgreSQL
  - Automatic reconnection with exponential backoff
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import asyncpg
import redis.asyncio as aioredis
import structlog
import websockets
import websockets.exceptions

from shared.log_events import *

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_MAINNET = "wss://stream.bybit.com/v5/public/linear"
WS_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"
REST_MAINNET = "https://api.bybit.com"
REST_TESTNET = "https://api-testnet.bybit.com"

KLINE_TIMEFRAMES = ["240"]  # Bybit notation — live trading + dashboard use 4h only
TF_MAP = {"1": "1m", "5": "5m", "15": "15m", "60": "1h", "240": "4h"}
TF_BYBIT_INTERVAL = {"4h": "240"}
TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}

MAX_RECONNECT_DELAY = 120  # seconds
BASE_RECONNECT_DELAY = 1

# Gap recovery: only backfill up to this many hours on startup
BACKFILL_MAX_HOURS = 48


class MarketDataCollector:
    """Ingests live market data from Bybit via WS + REST."""

    def __init__(
        self,
        *,
        exchange: str,
        symbol: str,
        api_key: str,
        api_secret: str,
        testnet: bool,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
    ) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.redis = redis
        self.db_pool = db_pool

        self._ws_url = WS_TESTNET if testnet else WS_MAINNET
        self._rest_base = REST_TESTNET if testnet else REST_MAINNET
        self._reconnect_delay = BASE_RECONNECT_DELAY
        self._last_heartbeat: float = 0.0
        self._ws: Any | None = None
        self._force_reconnect = False
        self._pending_core_sub = False

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Top-level loop: run WS + REST pollers concurrently."""
        log.info(SERVICE_STARTED, message="collector starting", symbol=self.symbol)
        await self.backfill_ohlcv_gaps()
        tasks = [
            asyncio.create_task(self._ws_loop(shutdown), name="ws_loop"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info(SERVICE_STOPPED, message="collector stopped")

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    async def _ws_loop(self, shutdown: asyncio.Event) -> None:
        """Connect to WebSocket with exponential-backoff reconnection."""
        while not shutdown.is_set():
            self._force_reconnect = False
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = BASE_RECONNECT_DELAY
                    await self._subscribe(ws)
                    log.info(MARKET_WS_CONNECTED, message="WebSocket connected", url=self._ws_url)

                    async for raw in ws:
                        if shutdown.is_set() or self._force_reconnect:
                            break
                        await self._handle_message(raw)

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.InvalidStatusCode,
                ConnectionRefusedError,
                OSError,
            ) as exc:
                log.warning(MARKET_WS_DISCONNECTED, message="WebSocket disconnected", error=str(exc), reconnect_in=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception:
                exc_type, exc_val, _ = sys.exc_info()
                # Log as general service error (not WebSocket-specific)
                # to distinguish from WS protocol errors
                log.error(
                    SERVICE_HEALTH_FAIL,
                    message="WebSocket loop error, reconnecting",
                    exc=str(exc_val),
                    exc_type=exc_type.__name__ if exc_type else "Unknown",
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)
            finally:
                self._ws = None
                self._pending_core_sub = False

    def _core_topics(self) -> list[str]:
        """Live subscriptions: 4h kline only."""
        return [f"kline.{tf}.{self.symbol}" for tf in KLINE_TIMEFRAMES]

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Subscribe to core BTCUSDT topics only."""
        core = self._core_topics()
        self._pending_core_sub = True
        await ws.send(json.dumps({"op": "subscribe", "args": core}))
        log.info(MARKET_WS_CONNECTED, message="WebSocket subscribed (core)", topics=core)

    async def _handle_subscribe_ack(self, data: dict[str, Any]) -> None:
        """Handle subscribe/unsubscribe ack. Core failure forces reconnect."""
        if data.get("success") is not False:
            self._pending_core_sub = False
            return

        ret_msg = str(data.get("ret_msg", ""))
        log.error(MARKET_WS_RECONNECTING, message="WebSocket subscription failed", data=data)

        bad_topic = None
        if "topic:" in ret_msg:
            bad_topic = ret_msg.rsplit("topic:", 1)[-1].strip()

        if self._pending_core_sub or (bad_topic and bad_topic in set(self._core_topics())):
            log.error(
                MARKET_WS_RECONNECTING,
                message="core WebSocket subscription failed — forcing reconnect",
                data=data,
            )
            self._force_reconnect = True
            if self._ws is not None:
                await self._ws.close()
            return

        # Ambiguous failure with no pending markers — reconnect to restore core feed.
        self._force_reconnect = True
        if self._ws is not None:
            await self._ws.close()

    async def _handle_message(self, raw: str | bytes) -> None:
        """Route incoming WS messages to the appropriate handler."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning(MARKET_WS_RECONNECTING, message="invalid JSON from WebSocket", raw=raw[:200])
            return

        # Pong / subscription confirmations
        if "op" in data:
            op = data.get("op")
            if op in ("subscribe", "unsubscribe"):
                await self._handle_subscribe_ack(data)
            return

        topic: str | None = data.get("topic")
        if topic is None:
            return

        payload = data.get("data")
        if payload is None:
            return

        if topic.startswith("kline"):
            await self._on_kline(topic, payload)

    # ------------------------------------------------------------------
    # WS handlers
    # ------------------------------------------------------------------

    async def _on_kline(self, topic: str, candles: list[dict[str, Any]]) -> None:
        """Publish OHLCV candles and persist closed bars."""
        # topic format: kline.{interval}.{symbol}
        parts = topic.split(".")
        if len(parts) < 3:
            log.warning(MARKET_OHLCV_STORED, message="invalid kline topic", topic=topic)
            return
        bybit_tf = parts[1]
        symbol = ".".join(parts[2:])
        tf = TF_MAP.get(bybit_tf, bybit_tf)
        channel = f"market:ohlcv:{self.exchange}:{symbol}:{tf}"

        for c in candles:
            try:
                open_ = float(c["open"])
                high = float(c["high"])
                low = float(c["low"])
                close = float(c["close"])
                volume = float(c["volume"])
                ts_ms = int(c["start"])
            except (KeyError, ValueError, TypeError) as exc:
                log.warning(MARKET_OHLCV_STORED, message="kline parse error", exc=str(exc), raw=str(c)[:200])
                continue
            confirmed = c.get("confirm", False)
            ohlcv = {
                "exchange": self.exchange,
                "symbol": symbol,
                "timeframe": tf,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "ts": ts_ms,
                "confirmed": confirmed,
            }
            await self.redis.publish(channel, json.dumps(ohlcv))

            # Cache latest bar in Redis hash for quick lookups
            cache_key = f"cache:ohlcv:{self.exchange}:{symbol}:{tf}"
            await self.redis.hset(cache_key, mapping={
                "open": str(ohlcv["open"]),
                "high": str(ohlcv["high"]),
                "low": str(ohlcv["low"]),
                "close": str(ohlcv["close"]),
                "volume": str(ohlcv["volume"]),
                "ts": str(ohlcv["ts"]),
                "confirmed": "1" if confirmed else "0",
            })

            # Persist only confirmed (closed) candles
            if confirmed:
                ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ohlcv_history (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (exchange, symbol, timeframe, timestamp) DO UPDATE
                        SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, volume = EXCLUDED.volume
                        """,
                        self.exchange,
                        symbol,
                        tf,
                        ts_dt,
                        open_,
                        high,
                        low,
                        close,
                        volume,
                    )

    # ------------------------------------------------------------------
    # Startup gap recovery
    # ------------------------------------------------------------------

    async def backfill_ohlcv_gaps(self) -> None:
        """Detect missing OHLCV candles since last recorded timestamp and backfill via REST.

        Only runs on startup. Caps backfill at BACKFILL_MAX_HOURS (48h) to avoid
        excessive API calls when the service has been down for a long time.
        Initial historical seeding should be done with scripts/seed_historical.py.
        """
        now_ms = int(time.time() * 1000)
        max_lookback_ms = BACKFILL_MAX_HOURS * 3600 * 1000

        for tf, interval_str in TF_BYBIT_INTERVAL.items():
            tf_ms = TF_SECONDS[tf] * 1000

            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT MAX(timestamp) AS last_ts FROM ohlcv_history "
                    "WHERE exchange = $1 AND symbol = $2 AND timeframe = $3",
                    self.exchange, self.symbol, tf,
                )

            if row is None or row["last_ts"] is None:
                log.info(
                    MARKET_OHLCV_STORED,
                    message="no existing ohlcv data, skipping backfill (run seed_historical.py first)",
                    timeframe=tf, symbol=self.symbol,
                )
                continue

            last_ms = int(row["last_ts"].timestamp() * 1000)
            gap_ms = now_ms - last_ms - tf_ms

            if gap_ms <= tf_ms:
                # At most 1 candle missing — negligible
                continue

            # Clamp start to BACKFILL_MAX_HOURS ago
            start_ms = max(last_ms + tf_ms, now_ms - max_lookback_ms)
            gap_hours = gap_ms / 3_600_000

            log.info(
                MARKET_OHLCV_STORED,
                message="ohlcv gap detected, backfilling",
                timeframe=tf, gap_hours=round(gap_hours, 1), symbol=self.symbol,
                backfill_from=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
            )

            await self._fetch_and_store_klines(
                interval_str=interval_str, tf=tf, start_ms=start_ms, end_ms=now_ms,
            )

    async def _fetch_and_store_klines(
        self, interval_str: str, tf: str, start_ms: int, end_ms: int,
    ) -> None:
        """Batch-fetch klines from Bybit REST and upsert into ohlcv_history."""
        url = f"{self._rest_base}/v5/market/kline"
        tf_ms = TF_SECONDS[tf] * 1000
        total = 0
        cursor_ms = start_ms

        async with aiohttp.ClientSession() as session:
            while cursor_ms < end_ms:
                params = {
                    "category": "linear",
                    "symbol": self.symbol,
                    "interval": interval_str,
                    "start": cursor_ms,
                    "end": end_ms,
                    "limit": 1000,
                }
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        data = await resp.json()
                except Exception as exc:
                    log.error(MARKET_OHLCV_STORED, message="kline backfill fetch error", exc=str(exc), tf=tf)
                    break

                if data.get("retCode") != 0:
                    log.warning(MARKET_OHLCV_STORED, message="kline API error", response=data, tf=tf)
                    break

                klines = data.get("result", {}).get("list", [])
                if not klines:
                    break

                # Bybit returns newest-first — reverse to process in chronological order
                klines = list(reversed(klines))

                records = []
                for k in klines:
                    try:
                        ts_ms_k = int(k[0])
                        if ts_ms_k < start_ms or ts_ms_k >= end_ms:
                            continue
                        records.append((
                            self.exchange, self.symbol, tf,
                            datetime.fromtimestamp(ts_ms_k / 1000, tz=timezone.utc),
                            float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]),
                        ))
                    except (IndexError, ValueError, TypeError):
                        continue

                if records:
                    async with self.db_pool.acquire() as conn:
                        await conn.executemany(
                            """
                            INSERT INTO ohlcv_history
                                (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (exchange, symbol, timeframe, timestamp) DO NOTHING
                            """,
                            records,
                        )
                    total += len(records)

                if len(klines) < 1000:
                    break  # No more pages

                cursor_ms = int(klines[-1][0]) + tf_ms
                await asyncio.sleep(0.1)  # Be polite to the API

        log.info(
            MARKET_OHLCV_STORED,
            message="ohlcv backfill complete",
            timeframe=tf, candles_inserted=total, symbol=self.symbol,
        )
