"""Market Data Service — entry point.

Initialises Redis, PostgreSQL, and launches:
  1. MarketDataCollector  — WebSocket + REST ingestion
  2. RegimeDetector       — real-time market-regime classification
  3. FundingMonitor        — funding-rate tracking & alerting
"""

from __future__ import annotations

import asyncio
import os
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

            CREATE TABLE IF NOT EXISTS funding_rates (
                id                 BIGSERIAL PRIMARY KEY,
                exchange           TEXT        NOT NULL,
                symbol             TEXT        NOT NULL,
                rate               DOUBLE PRECISION NOT NULL,
                predicted_rate     DOUBLE PRECISION,
                next_funding_time  TIMESTAMPTZ NOT NULL,
                collected_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (exchange, symbol, next_funding_time)
            );

            CREATE TABLE IF NOT EXISTS market_regimes (
                id            BIGSERIAL PRIMARY KEY,
                regime        TEXT        NOT NULL,
                confidence    DOUBLE PRECISION NOT NULL,
                adx           DOUBLE PRECISION,
                volatility    DOUBLE PRECISION,
                bb_width      DOUBLE PRECISION,
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

    # --- Launch tasks ---
    tasks = [
        asyncio.create_task(collector.run(shutdown_event), name="collector"),
        asyncio.create_task(regime_detector.run(shutdown_event), name="regime_detector"),
        asyncio.create_task(funding.run(shutdown_event), name="funding_monitor"),
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
