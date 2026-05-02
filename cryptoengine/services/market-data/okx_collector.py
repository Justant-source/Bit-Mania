"""OKX WebSocket collector.

Funding rate: wss://ws.okx.com:8443/ws/v5/public
  - funding-rate channel (실제 정산 시각마다)

Candle 1m: wss://ws.okx.com:8443/ws/v5/business
  - candle1m channel (business endpoint 전용)
"""
from __future__ import annotations

import asyncio
import json
import os

import asyncpg
import redis.asyncio as aioredis
import structlog
import websockets
import websockets.exceptions

log = structlog.get_logger(__name__)

OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_BUSINESS = "wss://ws.okx.com:8443/ws/v5/business"
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


async def _run_ws(url: str, sub_msg: dict, db: asyncpg.Pool, handler):
    """Generic reconnecting WebSocket loop."""
    delay = BASE_RECONNECT_DELAY
    while True:
        try:
            async with websockets.connect(url, ping_interval=25) as ws:
                await ws.send(json.dumps(sub_msg))
                log.info("okx_ws_connected", endpoint=url.split("/")[-1])
                delay = BASE_RECONNECT_DELAY
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if "data" not in data:
                            continue
                        await handler(data, db)
                    except Exception as e:
                        log.warning("okx_msg_error", error=str(e))
        except websockets.exceptions.ConnectionClosed:
            log.warning("okx_ws_disconnected", endpoint=url.split("/")[-1], reconnect_in=delay)
        except Exception as e:
            log.error("okx_ws_error", error=str(e), reconnect_in=delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def _handle_public(data: dict, db: asyncpg.Pool):
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


async def _handle_business(data: dict, db: asyncpg.Pool):
    channel = data.get("arg", {}).get("channel")
    for entry in data["data"]:
        if channel == "candle1m":
            # entry: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            # confirm == "1" means candle is closed
            if len(entry) >= 6:
                await db.execute(
                    """INSERT INTO multi_exchange_ohlcv
                       (exchange, symbol, interval, timestamp, open, high, low, close, volume)
                       VALUES ('okx', 'BTCUSDT', '1m', to_timestamp($1::bigint / 1000), $2, $3, $4, $5, $6)
                       ON CONFLICT (exchange, symbol, interval, timestamp) DO NOTHING""",
                    int(entry[0]), float(entry[1]), float(entry[2]),
                    float(entry[3]), float(entry[4]), float(entry[5]),
                )


async def okx_collector(db: asyncpg.Pool, redis: aioredis.Redis):
    public_sub = {
        "op": "subscribe",
        "args": [
            {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
        ],
    }
    business_sub = {
        "op": "subscribe",
        "args": [
            {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
        ],
    }
    # 두 엔드포인트를 동시에 운영
    await asyncio.gather(
        _run_ws(OKX_WS_PUBLIC, public_sub, db, _handle_public),
        _run_ws(OKX_WS_BUSINESS, business_sub, db, _handle_business),
    )


async def main():
    db = await asyncpg.create_pool(DB_DSN)
    redis = aioredis.from_url(REDIS_URL)
    log.info("okx_collector_starting")
    await okx_collector(db, redis)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
