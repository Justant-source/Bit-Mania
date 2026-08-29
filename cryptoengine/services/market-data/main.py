"""Market Data Service — entry point.

Initialises Redis, PostgreSQL, and launches:
  1. MarketDataCollector  — WebSocket + REST ingestion
  2. FundingMonitor        — funding-rate tracking & alerting
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import pathlib
import signal
import sys

import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.logging_config import setup_logging
from shared.log_writer import init_log_writer, close_log_writer
from shared.log_events import *
from shared.required_env import redact_url, require_env
from collector import MarketDataCollector
from funding_monitor import FundingMonitor

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{require_env('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = require_env("REDIS_URL")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

EXCHANGE = os.getenv("EXCHANGE", "bybit")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")

SERVICE_NAME = "market-data"

log = structlog.get_logger(__name__)


async def _create_tables(pool: asyncpg.Pool) -> None:
    """Ensure persistence tables exist (idempotent)."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_rate_history (
                id                 BIGSERIAL PRIMARY KEY,
                exchange           TEXT        NOT NULL,
                symbol             TEXT        NOT NULL,
                rate               DOUBLE PRECISION NOT NULL,
                predicted_rate     DOUBLE PRECISION,
                timestamp          TIMESTAMPTZ NOT NULL,
                UNIQUE (exchange, symbol, timestamp)
            );
            """
        )
    log.info(SERVICE_HEALTH_OK, message="database tables ensured")


async def main() -> None:
    # --- Connection pools ---
    db_pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DB_DSN,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    await init_log_writer(SERVICE_NAME, db_pool)
    setup_logging(level=LOG_LEVEL, service_name=SERVICE_NAME, db_pool=db_pool)
    log = structlog.get_logger()
    log.info(SERVICE_STARTED, message="market-data 서비스 시작", exchange=EXCHANGE, symbol=SYMBOL)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    log.info(REDIS_CONNECTED, message="Redis 연결 성공", redis=redact_url(REDIS_URL))

    await _create_tables(db_pool)

    # --- Service instances ---
    collector = MarketDataCollector(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        redis=redis_client,
        db_pool=db_pool,
    )
    funding = FundingMonitor(
        exchange=EXCHANGE,
        symbol=SYMBOL,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        coinglass_api_key=COINGLASS_API_KEY,
        redis=redis_client,
        db_pool=db_pool,
    )

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        log.info(SERVICE_STOPPING, message="shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    async def _heartbeat_publisher(shutdown: asyncio.Event) -> None:
        """30초마다 Redis에 서비스 하트비트 발행. TTL=300초(5분)."""
        service_name = "market-data"
        while not shutdown.is_set():
            try:
                await redis_client.setex(
                    f"heartbeat:{service_name}",
                    300,  # 5분 TTL
                    _json.dumps({
                        "service": service_name,
                        "ts": asyncio.get_event_loop().time(),
                        "status": "alive",
                    })
                )
                pathlib.Path("/tmp/heartbeat_ok").touch()
            except Exception:
                log.warning(SERVICE_HEALTH_FAIL, message="heartbeat publish failed", service=service_name)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    # --- Launch tasks ---
    tasks = [
        asyncio.create_task(collector.run(shutdown_event), name="collector"),
        asyncio.create_task(funding.run(shutdown_event), name="funding_monitor"),
        asyncio.create_task(_heartbeat_publisher(shutdown_event), name="heartbeat_publisher"),
    ]

    log.info(SERVICE_STARTED, message="all tasks launched", count=len(tasks))

    # Wait until shutdown is requested
    await shutdown_event.wait()
    log.info(SERVICE_STOPPING, message="market-data 서비스 종료 중")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Cleanup
    await redis_client.aclose()
    log.info(SERVICE_STOPPED, message="market-data 서비스 종료")
    await close_log_writer()
    await db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
