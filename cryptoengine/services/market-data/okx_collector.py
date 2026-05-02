"""OKX Public WebSocket collector.

WS: wss://ws.okx.com:8443/ws/v5/public
Channels:
  - funding-rate (instId: BTC-USDT-SWAP)
  - candle1m (instId: BTC-USDT-SWAP)
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

OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
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

SUB_MSG = {
    "op": "subscribe",
    "args": [
        {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
        {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
        {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
    ],
}


async def okx_collector(db: asyncpg.Pool, redis: aioredis.Redis):
    delay = BASE_RECONNECT_DELAY

    while True:
        try:
            async with websockets.connect(OKX_WS, ping_interval=25) as ws:
                await ws.send(json.dumps(SUB_MSG))
                log.info("okx_ws_connected")
                delay = BASE_RECONNECT_DELAY

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if "data" not in data:
                            continue
                        channel = data.get("arg", {}).get("channel")

                        for entry in data["data"]:
                            if channel == "funding-rate":
                                await db.execute(
                                    """INSERT INTO multi_exchange_funding
                                       (exchange, symbol, timestamp, funding_rate, next_funding_time)
                                       VALUES ('okx', 'BTCUSDT', to_timestamp($1::bigint / 1000), $2, to_timestamp($3::bigint / 1000))
                                       ON CONFLICT (exchange, symbol, timestamp) DO NOTHING""",
                                    int(entry["fundingTime"]),
                                    float(entry["fundingRate"]),
                                    int(entry["nextFundingTime"]),
                                )
                            elif channel == "candle1m":
                                # entry: [ts, o, h, l, c, vol, ...]
                                await db.execute(
                                    """INSERT INTO multi_exchange_ohlcv
                                       (exchange, symbol, interval, timestamp, open, high, low, close, volume)
                                       VALUES ('okx', 'BTCUSDT', '1m', to_timestamp($1::bigint / 1000), $2, $3, $4, $5, $6)
                                       ON CONFLICT (exchange, symbol, interval, timestamp) DO NOTHING""",
                                    int(entry[0]), float(entry[1]), float(entry[2]),
                                    float(entry[3]), float(entry[4]), float(entry[5]),
                                )
                    except Exception as e:
                        log.warning("okx_msg_error", error=str(e))

        except websockets.exceptions.ConnectionClosed:
            log.warning("okx_ws_disconnected", reconnect_in=delay)
        except Exception as e:
            log.error("okx_ws_error", error=str(e), reconnect_in=delay)

        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def main():
    db = await asyncpg.create_pool(DB_DSN)
    redis = aioredis.from_url(REDIS_URL)
    log.info("okx_collector_starting")
    await okx_collector(db, redis)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
