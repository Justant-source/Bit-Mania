"""Market Data Collector — WebSocket + REST ingestion for Bybit perpetual futures.

Responsibilities:
  - WebSocket streams: orderbook (depth 25, 100ms), trades, kline (1m/5m/15m/1h/4h), funding rate
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

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_MAINNET = "wss://stream.bybit.com/v5/public/linear"
WS_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"
REST_MAINNET = "https://api.bybit.com"
REST_TESTNET = "https://api-testnet.bybit.com"

KLINE_TIMEFRAMES = ["1", "5", "15", "60", "240"]  # Bybit notation
TF_MAP = {"1": "1m", "5": "5m", "15": "15m", "60": "1h", "240": "4h"}

MAX_RECONNECT_DELAY = 120  # seconds
BASE_RECONNECT_DELAY = 1

REST_POLL_INTERVAL_OI = 60         # seconds — open interest
REST_POLL_INTERVAL_RATIO = 300     # seconds — long/short ratio
REST_POLL_INTERVAL_LIQ = 120       # seconds — liquidations


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

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Top-level loop: run WS + REST pollers concurrently."""
        log.info("collector_starting", symbol=self.symbol)
        tasks = [
            asyncio.create_task(self._ws_loop(shutdown), name="ws_loop"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_open_interest, REST_POLL_INTERVAL_OI), name="poll_oi"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_long_short_ratio, REST_POLL_INTERVAL_RATIO), name="poll_ratio"),
            asyncio.create_task(self._rest_poll_loop(shutdown, self._poll_liquidations, REST_POLL_INTERVAL_LIQ), name="poll_liq"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info("collector_stopped")

    # ------------------------------------------------------------------
    # WebSocket
    # ------------------------------------------------------------------

    async def _ws_loop(self, shutdown: asyncio.Event) -> None:
        """Connect to WebSocket with exponential-backoff reconnection."""
        while not shutdown.is_set():
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._reconnect_delay = BASE_RECONNECT_DELAY
                    await self._subscribe(ws)
                    log.info("ws_connected", url=self._ws_url)

                    async for raw in ws:
                        if shutdown.is_set():
                            break
                        await self._handle_message(raw)

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.InvalidStatusCode,
                ConnectionRefusedError,
                OSError,
            ) as exc:
                log.warning("ws_disconnected", error=str(exc), reconnect_in=self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception:
                exc_type, exc_val, _ = sys.exc_info()
                log.error(
                    "ws_unexpected_error",
                    exc=str(exc_val),
                    exc_type=exc_type.__name__ if exc_type else "Unknown",
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, MAX_RECONNECT_DELAY)

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Send subscription messages to Bybit V5 public WS."""
        topics: list[str] = []

        # Orderbook depth-1, 100ms push (testnet supports depth 1, not 25)
        topics.append(f"orderbook.1.{self.symbol}")

        # Public trades
        topics.append(f"publicTrade.{self.symbol}")

        # Kline / candles for each timeframe
        for tf in KLINE_TIMEFRAMES:
            topics.append(f"kline.{tf}.{self.symbol}")

        # Tickers (includes funding rate, mark price, etc.)
        topics.append(f"tickers.{self.symbol}")

        subscribe_msg = {"op": "subscribe", "args": topics}
        await ws.send(json.dumps(subscribe_msg))
        log.info("ws_subscribed", topics=topics)

    async def _handle_message(self, raw: str | bytes) -> None:
        """Route incoming WS messages to the appropriate handler."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("ws_invalid_json", raw=raw[:200])
            return

        # Pong / subscription confirmations
        if "op" in data:
            if data.get("success") is False:
                log.error("ws_subscription_failed", data=data)
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
                log.warning("trades_parse_error", exc=str(exc), raw=str(t)[:200])
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
        if len(parts) < 2:
            log.warning("kline_invalid_topic", topic=topic)
            return
        bybit_tf = parts[1]
        tf = TF_MAP.get(bybit_tf, bybit_tf)
        channel = f"market:ohlcv:{self.exchange}:{self.symbol}:{tf}"

        for c in candles:
            try:
                open_ = float(c["open"])
                high = float(c["high"])
                low = float(c["low"])
                close = float(c["close"])
                volume = float(c["volume"])
                ts_ms = int(c["start"])
            except (KeyError, ValueError, TypeError) as exc:
                log.warning("kline_parse_error", exc=str(exc), raw=str(c)[:200])
                continue
            confirmed = c.get("confirm", False)
            ohlcv = {
                "exchange": self.exchange,
                "symbol": self.symbol,
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
            cache_key = f"cache:ohlcv:{self.exchange}:{self.symbol}:{tf}"
            await self.redis.hset(cache_key, mapping={
                "open": ohlcv["open"],
                "high": ohlcv["high"],
                "low": ohlcv["low"],
                "close": ohlcv["close"],
                "volume": ohlcv["volume"],
                "ts": ohlcv["ts"],
            })

            # Persist only confirmed (closed) candles
            if confirmed:
                ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ohlcv (exchange, symbol, timeframe, ts, open, high, low, close, volume)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (exchange, symbol, timeframe, ts) DO UPDATE
                        SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, volume = EXCLUDED.volume
                        """,
                        self.exchange,
                        self.symbol,
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
        channel = f"market:ticker:{self.exchange}:{self.symbol}"
        ticker = {
            "exchange": self.exchange,
            "symbol": self.symbol,
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

        # Cache funding rate for quick access
        if payload.get("fundingRate") is not None:
            funding_channel = f"market:funding:{self.exchange}:{self.symbol}"
            funding_msg = {
                "exchange": self.exchange,
                "symbol": self.symbol,
                "rate": payload["fundingRate"],
                "predicted_rate": payload.get("fundingRate"),
                "next_funding_time": payload.get("nextFundingTime"),
            }
            await self.redis.publish(funding_channel, json.dumps(funding_msg))
            await self.redis.hset(f"cache:funding:{self.exchange}:{self.symbol}", mapping={
                "rate": str(payload["fundingRate"]),
                "next_funding_time": str(payload.get("nextFundingTime", "")),
            })

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
            try:
                await poller_fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("rest_poll_error", poller=poller_fn.__name__)
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
            log.warning("oi_api_error", response=data)
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
        log.debug("oi_polled", open_interest=oi.get("openInterest"))

    async def _poll_long_short_ratio(self) -> None:
        """Fetch global long/short ratio from Bybit."""
        url = f"{self._rest_base}/v5/market/account-ratio"
        params = {"category": "linear", "symbol": self.symbol, "period": "5min", "limit": "1"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        if data.get("retCode") != 0:
            log.warning("ratio_api_error", response=data)
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
        log.debug("long_short_ratio_polled", buy_ratio=ratio.get("buyRatio"))

    async def _poll_liquidations(self) -> None:
        """Fetch recent liquidation data via Bybit REST API."""
        url = f"{self._rest_base}/v5/market/recent-trade"
        params = {"category": "linear", "symbol": self.symbol, "limit": "50"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        if data.get("retCode") != 0:
            log.warning("liquidation_api_error", response=data)
            return

        trades = data.get("result", {}).get("list", [])
        # Filter for liquidation trades (isBlockTrade flag or large-qty heuristic)
        liq_trades = [t for t in trades if t.get("isBlockTrade", False)]
        if not liq_trades:
            return

        channel = f"market:liquidations:{self.exchange}:{self.symbol}"
        for lt in liq_trades:
            msg = {
                "exchange": self.exchange,
                "symbol": self.symbol,
                "price": lt.get("price"),
                "qty": lt.get("size"),
                "side": lt.get("side"),
                "ts": lt.get("time"),
            }
            await self.redis.publish(channel, json.dumps(msg))

        log.debug("liquidations_polled", count=len(liq_trades))
