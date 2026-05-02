"""Binance USDT-M Futures REST API polling collector.

WebSocket(fstream.binance.com)이 서버 환경에서 geo-block됨.
REST API polling으로 동일 데이터를 수집:
  - 1분봉 OHLCV: 60초마다 poll (직전 마감 candle 저장)
  - 펀딩비: 8시간마다 poll (정산 시각 00/08/16 UTC 직후)

Binance REST API는 국내 IP에서 정상 응답 확인됨.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import aiohttp
import asyncpg
import redis.asyncio as aioredis
import structlog

log = structlog.get_logger(__name__)

BINANCE_REST = "https://fapi.binance.com"
SYMBOL = "BTCUSDT"

KLINE_POLL_INTERVAL = 60       # seconds
FUNDING_POLL_INTERVAL = 300    # 5분마다 polling (정산 시각 감지)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")


async def poll_klines(db: asyncpg.Pool, session: aiohttp.ClientSession):
    """1분봉 OHLCV polling — 60초마다 직전 마감 candle 저장."""
    while True:
        try:
            async with session.get(
                f"{BINANCE_REST}/fapi/v1/klines",
                params={"symbol": SYMBOL, "interval": "1m", "limit": 2},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                candles = await r.json()
                # index 0 = 이전 마감 candle (closed), index 1 = 현재 진행 중
                closed = candles[0]
                # [open_time, open, high, low, close, volume, close_time, ...]
                await db.execute(
                    """INSERT INTO multi_exchange_ohlcv
                       (exchange, symbol, interval, timestamp, open, high, low, close, volume)
                       VALUES ('binance', $1, '1m', to_timestamp($2::bigint / 1000), $3, $4, $5, $6, $7)
                       ON CONFLICT (exchange, symbol, interval, timestamp) DO NOTHING""",
                    SYMBOL,
                    int(closed[0]),
                    float(closed[1]),
                    float(closed[2]),
                    float(closed[3]),
                    float(closed[4]),
                    float(closed[5]),
                )
        except Exception as e:
            log.warning("binance_kline_poll_error", error=str(e))
        await asyncio.sleep(KLINE_POLL_INTERVAL)


async def poll_funding(db: asyncpg.Pool, session: aiohttp.ClientSession):
    """펀딩비 polling — 5분마다 최신 정산 기록 저장."""
    last_saved_ts = 0

    while True:
        try:
            # 최근 펀딩비 정산 이력
            async with session.get(
                f"{BINANCE_REST}/fapi/v1/fundingRate",
                params={"symbol": SYMBOL, "limit": 3},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                items = await r.json()

            # 다음 펀딩 시각 (프리뷰)
            async with session.get(
                f"{BINANCE_REST}/fapi/v1/premiumIndex",
                params={"symbol": SYMBOL},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                premium = await r.json()

            next_funding_time = int(premium.get("nextFundingTime", 0))

            for item in items:
                fund_ts = int(item["fundingTime"])
                if fund_ts <= last_saved_ts:
                    continue
                await db.execute(
                    """INSERT INTO multi_exchange_funding
                       (exchange, symbol, timestamp, funding_rate, mark_price, next_funding_time)
                       VALUES ('binance', $1, to_timestamp($2::bigint / 1000), $3, $4, to_timestamp($5::bigint / 1000))
                       ON CONFLICT (exchange, symbol, timestamp) DO NOTHING""",
                    SYMBOL,
                    fund_ts,
                    float(item["fundingRate"]),
                    float(item.get("markPrice", 0) or 0),
                    next_funding_time,
                )
                last_saved_ts = max(last_saved_ts, fund_ts)
                log.info("binance_funding_saved", ts=fund_ts, rate=item["fundingRate"])

        except Exception as e:
            log.warning("binance_funding_poll_error", error=str(e))

        await asyncio.sleep(FUNDING_POLL_INTERVAL)


async def binance_collector(db: asyncpg.Pool, redis: aioredis.Redis):
    async with aiohttp.ClientSession() as session:
        log.info("binance_rest_collector_starting", mode="REST polling")
        await asyncio.gather(
            poll_klines(db, session),
            poll_funding(db, session),
        )


async def main():
    db = await asyncpg.create_pool(DB_DSN)
    redis = aioredis.from_url(REDIS_URL)
    await binance_collector(db, redis)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
