"""Market Data Collector — WebSocket + REST ingestion for Bybit perpetual futures.

Responsibilities:
  - WebSocket streams: orderbook (depth 25, 100ms), trades, kline (4h only), funding rate
  - REST polling (1-5 min): open interest, long/short ratio, liquidation data
  - Publish all data to Redis pub/sub channels
  - Persist to PostgreSQL
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
from quarterly_lifecycle import (
    FALLBACK_QUARTERLY_SYMBOLS,
    resolve_quarterly_symbols,
    run_lifecycle_check,
)

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

REST_POLL_INTERVAL_OI = 60         # seconds — open interest
REST_POLL_INTERVAL_RATIO = 300     # seconds — long/short ratio
REST_POLL_INTERVAL_LIQ = 120       # seconds — liquidations
QUARTERLY_LIFECYCLE_INTERVAL = 86_400  # seconds — daily quarterly sync

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
        # Track C: mutable active quarterlies — NEVER hardcode expired contracts into
        # the same subscribe batch as BTCUSDT (Bybit rejects the whole batch).
        self._quarterly_symbols: list[str] = list(FALLBACK_QUARTERLY_SYMBOLS)
        self._ws: Any | None = None
        self._force_reconnect = False
        self._pending_core_sub = False
        self._pending_quarterly_topics: set[str] = set()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Top-level loop: run WS + REST pollers concurrently."""
        log.info(SERVICE_STARTED, message="collector starting", symbol=self.symbol)
        self._quarterly_symbols = await resolve_quarterly_symbols(rest_base=self._rest_base)
        log.info(
            MARKET_WS_CONNECTED,
            message="active quarterly symbols resolved",
            symbols=self._quarterly_symbols,
        )
        await self.backfill_ohlcv_gaps()
        tasks = [
            asyncio.create_task(self._ws_loop(shutdown), name="ws_loop"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_open_interest, REST_POLL_INTERVAL_OI), name="poll_oi"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_long_short_ratio, REST_POLL_INTERVAL_RATIO), name="poll_ratio"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_liquidations, REST_POLL_INTERVAL_LIQ), name="poll_liq"),
            asyncio.create_task(self._quarterly_lifecycle_loop(shutdown), name="quarterly_lifecycle"),
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
                self._pending_quarterly_topics.clear()

    def _core_topics(self) -> list[str]:
        """BTCUSDT topics required for trading — never mixed with quarterlies."""
        topics = [
            f"orderbook.1.{self.symbol}",
            f"publicTrade.{self.symbol}",
        ]
        for tf in KLINE_TIMEFRAMES:
            topics.append(f"kline.{tf}.{self.symbol}")
        topics.append(f"tickers.{self.symbol}")
        return topics

    def _quarterly_topics(self, symbols: list[str] | None = None) -> list[str]:
        topics: list[str] = []
        for qsym in symbols if symbols is not None else self._quarterly_symbols:
            topics.append(f"kline.1.{qsym}")
            topics.append(f"tickers.{qsym}")
        return topics

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Subscribe core BTCUSDT first, then quarterlies in a separate batch.

        Bybit rejects an entire subscribe args list when any topic is invalid
        (e.g. expired quarterly). Keeping core separate protects OHLCV feed.
        """
        core = self._core_topics()
        self._pending_core_sub = True
        await ws.send(json.dumps({"op": "subscribe", "args": core}))
        log.info(MARKET_WS_CONNECTED, message="WebSocket subscribed (core)", topics=core)

        quarterly = self._quarterly_topics()
        if quarterly:
            self._pending_quarterly_topics = set(quarterly)
            await ws.send(json.dumps({"op": "subscribe", "args": quarterly}))
            log.info(
                MARKET_WS_CONNECTED,
                message="WebSocket subscribed (quarterly)",
                topics=quarterly,
            )

    async def _ws_subscribe_topic(self, topic: str) -> None:
        ws = self._ws
        if ws is None:
            return
        self._pending_quarterly_topics.add(topic)
        await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))

    async def _ws_unsubscribe_topic(self, topic: str) -> None:
        ws = self._ws
        if ws is None:
            return
        await ws.send(json.dumps({"op": "unsubscribe", "args": [topic]}))

    async def _quarterly_lifecycle_loop(self, shutdown: asyncio.Event) -> None:
        """Daily sync: drop expired quarterlies, subscribe newly listed ones."""
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=QUARTERLY_LIFECYCLE_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass
            if shutdown.is_set():
                break
            try:
                updated = await run_lifecycle_check(
                    self._quarterly_symbols,
                    self._ws_subscribe_topic,
                    self._ws_unsubscribe_topic,
                    rest_base=self._rest_base,
                )
                self._quarterly_symbols = updated
            except Exception as exc:
                log.warning(
                    SERVICE_HEALTH_FAIL,
                    message="quarterly lifecycle check failed",
                    exc=str(exc),
                )

    async def _handle_subscribe_ack(self, data: dict[str, Any]) -> None:
        """Handle subscribe/unsubscribe ack. Core failure forces reconnect."""
        if data.get("success") is not False:
            # Successful ack: clear the pending batch that was waiting.
            # Core and quarterly are sent as separate ops; prefer clearing
            # core first when both markers could still be set.
            if self._pending_core_sub:
                self._pending_core_sub = False
            elif self._pending_quarterly_topics:
                self._pending_quarterly_topics.clear()
            return

        ret_msg = str(data.get("ret_msg", ""))
        log.error(MARKET_WS_RECONNECTING, message="WebSocket subscription failed", data=data)

        # Expired/invalid quarterly topic — drop it and retry remaining quarterlies.
        # Never reconnect-loop solely for Track C optional feeds.
        bad_topic = None
        if "topic:" in ret_msg:
            bad_topic = ret_msg.rsplit("topic:", 1)[-1].strip()

        is_quarterly_topic = bool(
            bad_topic and (
                bad_topic in self._pending_quarterly_topics
                or any(bad_topic.endswith(f".{s}") for s in self._quarterly_symbols)
            )
        )

        if is_quarterly_topic and bad_topic:
            bad_symbol = bad_topic.split(".", 2)[-1] if bad_topic.count(".") >= 2 else None
            if bad_symbol and bad_symbol in self._quarterly_symbols:
                self._quarterly_symbols = [s for s in self._quarterly_symbols if s != bad_symbol]
                log.warning(
                    MARKET_WS_RECONNECTING,
                    message="dropped invalid quarterly symbol from subscribe set",
                    symbol=bad_symbol,
                    remaining=self._quarterly_symbols,
                )
            self._pending_quarterly_topics.discard(bad_topic)
            remaining = [t for t in self._quarterly_topics() if t != bad_topic]
            self._pending_quarterly_topics = set(remaining)
            if remaining and self._ws is not None:
                await self._ws.send(json.dumps({"op": "subscribe", "args": remaining}))
            return

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

        # Unknown failure while quarterly pending — drop all pending quarterlies, keep core.
        if self._pending_quarterly_topics:
            log.warning(
                MARKET_WS_RECONNECTING,
                message="quarterly subscribe batch failed; continuing with core only",
                data=data,
            )
            self._pending_quarterly_topics.clear()
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

        if topic.startswith("orderbook"):
            await self._on_orderbook(data)
        elif topic.startswith("publicTrade"):
            await self._on_trades(payload)
        elif topic.startswith("kline"):
            await self._on_kline(topic, payload)
        elif topic.startswith("tickers"):
            await self._on_ticker(payload)

    # ------------------------------------------------------------------
    # WS handlers
    # ------------------------------------------------------------------

    def _is_quarterly_symbol(self, symbol: str) -> bool:
        """Check if symbol is a tracked quarterly future."""
        return symbol in self._quarterly_symbols

    async def _on_orderbook(self, msg: dict[str, Any]) -> None:
        """Process orderbook snapshot / delta."""
        payload = msg["data"]
        channel = f"market:orderbook:{self.exchange}:{self.symbol}"
        ob = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "type": msg.get("type", "snapshot"),
            "bids": payload.get("b", []),
            "asks": payload.get("a", []),
            "ts": payload.get("u", int(time.time() * 1000)),
        }
        await self.redis.publish(channel, json.dumps(ob))

    async def _on_trades(self, trades: list[dict[str, Any]]) -> None:
        """Publish each trade tick to Redis (no DB — trades table is for strategy executions)."""
        channel = f"market:trades:{self.exchange}:{self.symbol}"

        for t in trades:
            try:
                price = float(t["p"])
                qty = float(t["v"])
                side = t["S"].lower()
                ts_ms = int(t["T"])
            except (KeyError, ValueError, TypeError) as exc:
                log.warning(MARKET_TICKER_RECEIVED, message="trades parse error", exc=str(exc), raw=str(t)[:200])
                continue
            trade_msg = {
                "exchange": self.exchange,
                "symbol": self.symbol,
                "price": price,
                "quantity": qty,
                "side": side,
                "ts": ts_ms,
            }
            await self.redis.publish(channel, json.dumps(trade_msg))

    async def _on_kline(self, topic: str, candles: list[dict[str, Any]]) -> None:
        """Publish OHLCV candles and persist closed bars."""
        # topic format: kline.{interval}.{symbol}
        parts = topic.split(".")
        if len(parts) < 3:
            log.warning(MARKET_OHLCV_STORED, message="invalid kline topic", topic=topic)
            return
        bybit_tf = parts[1]
        symbol = ".".join(parts[2:])  # Handle symbols with dots (e.g. quarterly)
        tf = TF_MAP.get(bybit_tf, bybit_tf)

        # Determine which table and channel to use
        is_quarterly = self._is_quarterly_symbol(symbol)
        if is_quarterly:
            channel = f"market:ohlcv:{self.exchange}:{symbol}:{tf}"
            table = "quarterly_futures_history"
        else:
            channel = f"market:ohlcv:{self.exchange}:{symbol}:{tf}"
            table = "ohlcv_history"

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
                    if is_quarterly:
                        # For quarterly futures, use the quarterly_futures_history table
                        await conn.execute(
                            """
                            INSERT INTO quarterly_futures_history
                                (exchange, symbol, underlying, expiry_date, timestamp, open, high, low, close, volume)
                            VALUES ($1, $2, 'BTC', '2099-12-31', $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (exchange, symbol, timestamp) DO UPDATE
                            SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                                close = EXCLUDED.close, volume = EXCLUDED.volume
                            """,
                            self.exchange,
                            symbol,
                            ts_dt,
                            open_,
                            high,
                            low,
                            close,
                            volume,
                        )
                    else:
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

    async def _on_ticker(self, payload: dict[str, Any]) -> None:
        """Handle ticker updates — includes funding rate and mark price."""
        symbol = payload.get("symbol", self.symbol)
        is_quarterly = self._is_quarterly_symbol(symbol)

        channel = f"market:ticker:{self.exchange}:{symbol}"
        ticker = {
            "exchange": self.exchange,
            "symbol": symbol,
            "last_price": payload.get("lastPrice"),
            "mark_price": payload.get("markPrice"),
            "index_price": payload.get("indexPrice"),
            "funding_rate": payload.get("fundingRate"),
            "next_funding_time": payload.get("nextFundingTime"),
            "open_interest": payload.get("openInterest"),
            "volume_24h": payload.get("volume24h"),
            "turnover_24h": payload.get("turnover24h"),
        }
        await self.redis.publish(channel, json.dumps(ticker))

        # Track C Phase C1: Calculate and store quarterly perp spread
        if is_quarterly:
            quarterly_price = payload.get("markPrice")
            if quarterly_price is not None:
                # Fetch perp price from cache
                perp_price_bytes = await self.redis.get(f"cache:price:{self.exchange}:{self.symbol}")
                if perp_price_bytes:
                    try:
                        perp_price = float(perp_price_bytes)
                        spread = (float(quarterly_price) - perp_price) / perp_price
                        ts_ms = int(time.time() * 1000)
                        ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

                        async with self.db_pool.acquire() as conn:
                            await conn.execute(
                                """
                                INSERT INTO quarterly_perp_spread
                                    (quarterly_symbol, perp_symbol, timestamp, spread, quarterly_price, perp_price)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT (quarterly_symbol, perp_symbol, timestamp) DO NOTHING
                                """,
                                symbol,
                                self.symbol,
                                ts_dt,
                                spread,
                                float(quarterly_price),
                                perp_price,
                            )
                    except (ValueError, TypeError) as e:
                        log.warning(MARKET_TICKER_RECEIVED, message="spread calculation error", exc=str(e))

        # Cache funding rate for quick access
        if payload.get("fundingRate") is not None:
            funding_channel = f"market:funding:{self.exchange}:{symbol}"
            funding_msg = {
                "exchange": self.exchange,
                "symbol": symbol,
                "rate": payload["fundingRate"],
                "predicted_rate": payload.get("nextFundingRate"),
                "next_funding_time": payload.get("nextFundingTime"),
            }
            await self.redis.publish(funding_channel, json.dumps(funding_msg))
            await self.redis.hset(f"cache:funding:{self.exchange}:{symbol}", mapping={
                "rate": str(payload["fundingRate"]),
                "next_funding_time": str(payload.get("nextFundingTime", "")),
            })

        # Cache mark price for perp (used for quarterly spread calculation)
        if not is_quarterly and payload.get("markPrice") is not None:
            await self.redis.set(f"cache:price:{self.exchange}:{symbol}", str(payload.get("markPrice")))

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

    # ------------------------------------------------------------------
    # REST pollers
    # ------------------------------------------------------------------

    async def _rest_poll_loop(
        self,
        shutdown: asyncio.Event,
        poller_fn,
        interval: float,
    ) -> None:
        """Generic polling loop wrapper."""
        while not shutdown.is_set():
            last_exc: Exception | None = None
            for attempt in range(3):
                try:
                    await poller_fn()
                    last_exc = None
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)  # 1s, 2s
            if last_exc is not None:
                log.error(SERVICE_HEALTH_FAIL, message="REST poll error", poller=poller_fn.__name__, exc_info=last_exc)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    async def _poll_open_interest(self) -> None:
        """Fetch open interest from Bybit REST API."""
        url = f"{self._rest_base}/v5/market/open-interest"
        params = {"category": "linear", "symbol": self.symbol, "intervalTime": "5min", "limit": "1"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        if data.get("retCode") != 0:
            log.warning(SERVICE_HEALTH_FAIL, message="OI API error", response=data)
            return

        records = data.get("result", {}).get("list", [])
        if not records:
            return

        oi = records[0]
        channel = f"market:open_interest:{self.exchange}:{self.symbol}"
        msg = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "open_interest": oi.get("openInterest"),
            "ts": oi.get("timestamp"),
        }
        await self.redis.publish(channel, json.dumps(msg))
        await self.redis.hset(f"cache:oi:{self.exchange}:{self.symbol}", mapping={
            "open_interest": str(oi.get("openInterest", "")),
            "ts": str(oi.get("timestamp", "")),
        })
        log.debug(MARKET_TICKER_RECEIVED, message="OI polled", open_interest=oi.get("openInterest"))

    async def _poll_long_short_ratio(self) -> None:
        """Fetch global long/short ratio from Bybit."""
        url = f"{self._rest_base}/v5/market/account-ratio"
        params = {"category": "linear", "symbol": self.symbol, "period": "5min", "limit": "1"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        if data.get("retCode") != 0:
            log.warning(SERVICE_HEALTH_FAIL, message="long/short ratio API error", response=data)
            return

        records = data.get("result", {}).get("list", [])
        if not records:
            return

        ratio = records[0]
        channel = f"market:long_short_ratio:{self.exchange}:{self.symbol}"
        msg = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "buy_ratio": ratio.get("buyRatio"),
            "sell_ratio": ratio.get("sellRatio"),
            "ts": ratio.get("timestamp"),
        }
        await self.redis.publish(channel, json.dumps(msg))
        log.debug(MARKET_TICKER_RECEIVED, message="long/short ratio polled", buy_ratio=ratio.get("buyRatio"))

    async def _poll_liquidations(self) -> None:
        """Fetch recent liquidation data via Bybit's dedicated liquidation endpoint."""
        # Use Bybit's dedicated liquidation endpoint
        # GET /v5/market/liquidation returns actual forced liquidations
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.bybit.com/v5/market/liquidation",
                    params={"category": "linear", "symbol": self.symbol, "limit": 200},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        log.debug(SERVICE_HEALTH_FAIL, message="liquidation endpoint unavailable", status=resp.status)
                        liq_trades = []
                        return
                    data = await resp.json()
            if not isinstance(data, dict):
                liq_trades = []
                return
            liq_list = data.get("result", {}).get("list", [])
            # liq_list items have: price, side, size, time
            liq_trades = [
                {
                    "price": float(item.get("price", 0)),
                    "side": item.get("side", ""),
                    "size": float(item.get("size", 0)),
                    "ts": int(item.get("time", 0)),
                }
                for item in liq_list
            ]
        except Exception as exc:
            log.warning(SERVICE_HEALTH_FAIL, message="liquidation fetch error", exc=str(exc))
            liq_trades = []

        if not liq_trades:
            return

        channel = f"market:liquidations:{self.exchange}:{self.symbol}"
        for lt in liq_trades:
            msg = {
                "exchange": self.exchange,
                "symbol": self.symbol,
                "price": lt["price"],
                "qty": lt["size"],
                "side": lt["side"],
                "ts": lt["ts"],
            }
            await self.redis.publish(channel, json.dumps(msg))

        log.debug(MARKET_TICKER_RECEIVED, message="liquidations polled", count=len(liq_trades))
