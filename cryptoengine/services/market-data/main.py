"""Market Data Service — entry point.

Initialises Redis, PostgreSQL, and launches:
  1. MarketDataCollector  — WebSocket + REST ingestion
  2. RegimeDetector       — real-time market-regime classification
  3. FundingMonitor        — funding-rate tracking & alerting
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
import logging
import structlog

from collector import MarketDataCollector
from regime_detector import RegimeDetector
from funding_monitor import FundingMonitor

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

EXCHANGE = os.getenv("EXCHANGE", "bybit")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if LOG_LEVEL == "DEBUG" else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, LOG_LEVEL, logging.INFO)  # type: ignore[arg-type]
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def _create_tables(pool: asyncpg.Pool) -> None:
    """Ensure persistence tables exist (idempotent)."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ohlcv (
                id            BIGSERIAL PRIMARY KEY,
                exchange      TEXT        NOT NULL,
                symbol        TEXT        NOT NULL,
                timeframe     TEXT        NOT NULL,
                ts            TIMESTAMPTZ NOT NULL,
                open          DOUBLE PRECISION NOT NULL,
                high          DOUBLE PRECISION NOT NULL,
                low           DOUBLE PRECISION NOT NULL,
                close         DOUBLE PRECISION NOT NULL,
                volume        DOUBLE PRECISION NOT NULL,
                UNIQUE (exchange, symbol, timeframe, ts)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id            BIGSERIAL PRIMARY KEY,
                exchange      TEXT        NOT NULL,
                symbol        TEXT        NOT NULL,
                price         DOUBLE PRECISION NOT NULL,
                quantity      DOUBLE PRECISION NOT NULL,
                side          TEXT        NOT NULL,
                ts            TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS funding_rate_history (
                id                 BIGSERIAL PRIMARY KEY,
                exchange           TEXT        NOT NULL,
                symbol             TEXT        NOT NULL,
                rate               DOUBLE PRECISION NOT NULL,
                predicted_rate     DOUBLE PRECISION,
                timestamp          TIMESTAMPTZ NOT NULL,
                UNIQUE (exchange, symbol, timestamp)
            );

            CREATE TABLE IF NOT EXISTS market_regime_history (
                id            BIGSERIAL PRIMARY KEY,
                symbol        TEXT        NOT NULL DEFAULT 'BTCUSDT',
                regime        TEXT        NOT NULL,
                confidence    DOUBLE PRECISION,
                indicators    JSONB,
                detected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    log.info("database_tables_ensured")


async def main() -> None:
    _configure_logging()
    log.info("market_data_service_starting", exchange=EXCHANGE, symbol=SYMBOL)

    # --- Connection pools ---
    db_pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DB_DSN,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    log.info("connections_established", redis=REDIS_URL)

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
    regime_detector = RegimeDetector(redis=redis_client, db_pool=db_pool, symbol=SYMBOL, exchange=EXCHANGE)
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
        log.info("shutdown_signal_received")
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
                log.warning("heartbeat_publish_failed", service=service_name)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    # --- Launch tasks ---
    tasks = [
        asyncio.create_task(collector.run(shutdown_event), name="collector"),
        asyncio.create_task(regime_detector.run(shutdown_event), name="regime_detector"),
        asyncio.create_task(funding.run(shutdown_event), name="funding_monitor"),
        asyncio.create_task(_heartbeat_publisher(shutdown_event), name="heartbeat_publisher"),
    ]

    log.info("all_tasks_launched", count=len(tasks))

    # Wait until shutdown is requested
    await shutdown_event.wait()
    log.info("shutting_down_tasks")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Cleanup
    await redis_client.aclose()
    await db_pool.close()
    log.info("market_data_service_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
