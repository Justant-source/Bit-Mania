"""Binance USDT-M Futures WebSocket collector.

Public WebSocket: wss://fstream.binance.com/stream
Streams:
  - btcusdt@markPrice@1s   — mark price + funding rate
  - btcusdt@kline_1m       — 1m OHLCV
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis
import structlog
import websockets
import websockets.exceptions

log = structlog.get_logger(__name__)

BINANCE_WS = "wss://fstream.binance.com/stream"
MAX_RECONNECT_DELAY = 120
BASE_RECONNECT_DELAY = 1

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")


async def binance_collector(db: asyncpg.Pool, redis: aioredis.Redis):
    streams = ["btcusdt@markPrice@1s", "btcusdt@kline_1m"]
    url = f"{BINANCE_WS}?streams={'/'.join(streams)}"
    delay = BASE_RECONNECT_DELAY

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                log.info("binance_ws_connected")
                delay = BASE_RECONNECT_DELAY
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        stream = data.get("stream", "")
                        payload = data.get("data", {})

                        if "markPrice" in stream:
                            await db.execute(
                                """INSERT INTO multi_exchange_funding
                                   (exchange, symbol, timestamp, mark_price, funding_rate, next_funding_time)
                                   VALUES ('binance', $1, to_timestamp($2::bigint / 1000), $3, $4, to_timestamp($5::bigint / 1000))
                                   ON CONFLICT (exchange, symbol, timestamp) DO NOTHING""",
                                payload["s"],
                                payload["E"],
                                payload["p"],
                                payload["r"],
                                payload["T"],
                            )

                        elif "kline" in stream:
                            k = payload.get("k", {})
                            if k.get("x"):  # candle closed
                                await db.execute(
                                    """INSERT INTO multi_exchange_ohlcv
                                       (exchange, symbol, interval, timestamp, open, high, low, close, volume)
                                       VALUES ('binance', $1, '1m', to_timestamp($2::bigint / 1000), $3, $4, $5, $6, $7)
                                       ON CONFLICT (exchange, symbol, interval, timestamp) DO NOTHING""",
                                    k["s"], k["t"], k["o"], k["h"], k["l"], k["c"], k["v"],
                                )
                    except Exception as e:
                        log.warning("binance_msg_error", error=str(e))

        except websockets.exceptions.ConnectionClosed:
            log.warning("binance_ws_disconnected", reconnect_in=delay)
        except Exception as e:
            log.error("binance_ws_error", error=str(e), reconnect_in=delay)

        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def main():
    db = await asyncpg.create_pool(DB_DSN)
    redis = aioredis.from_url(REDIS_URL)
    log.info("binance_collector_starting")
    await binance_collector(db, redis)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
