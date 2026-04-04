"""Execution Engine Service — entry point.

Subscribes to ``order:request`` Redis channel, starts the execution engine,
order manager, position tracker, and safety module.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import signal

import asyncpg
import redis.asyncio as aioredis
import logging
import structlog

from engine import ExecutionEngine
from position_tracker import PositionTracker
from shared.exchange.factory import exchange_factory

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
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
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"


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
    """Ensure execution-specific tables exist."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id             BIGSERIAL PRIMARY KEY,
                request_id     TEXT UNIQUE NOT NULL,
                order_id       TEXT,
                exchange       TEXT        NOT NULL,
                symbol         TEXT        NOT NULL,
                side           TEXT        NOT NULL,
                order_type     TEXT        NOT NULL,
                quantity       DOUBLE PRECISION NOT NULL,
                price          DOUBLE PRECISION,
                status         TEXT        NOT NULL DEFAULT 'pending',
                filled_qty     DOUBLE PRECISION DEFAULT 0,
                filled_price   DOUBLE PRECISION,
                fee            DOUBLE PRECISION DEFAULT 0,
                fee_currency   TEXT DEFAULT 'USDT',
                strategy_id    TEXT,
                post_only      BOOLEAN DEFAULT TRUE,
                reduce_only    BOOLEAN DEFAULT FALSE,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS positions (
                id              BIGSERIAL PRIMARY KEY,
                exchange        TEXT        NOT NULL,
                symbol          TEXT        NOT NULL,
                side            TEXT        NOT NULL,
                size            DOUBLE PRECISION NOT NULL,
                entry_price     DOUBLE PRECISION NOT NULL,
                unrealized_pnl  DOUBLE PRECISION DEFAULT 0,
                leverage        DOUBLE PRECISION DEFAULT 1,
                liquidation_price DOUBLE PRECISION,
                margin_used     DOUBLE PRECISION DEFAULT 0,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (exchange, symbol, side)
            );

            CREATE INDEX IF NOT EXISTS idx_orders_request_id ON orders (request_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
            CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders (strategy_id);
            """
        )
    log.info("execution_tables_ensured")


async def main() -> None:
    _configure_logging()
    log.info("execution_service_starting", exchange=EXCHANGE)

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

    # --- Position tracker (sync on startup) ---
    position_tracker = PositionTracker(
        exchange=EXCHANGE,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        redis=redis_client,
        db_pool=db_pool,
    )
    await position_tracker.sync_from_exchange()

    # --- Publish wallet balance to Redis for orchestrator (periodic) ---
    import json as _json

    async def _heartbeat_publisher(shutdown: asyncio.Event) -> None:
        """30초마다 Redis에 서비스 하트비트 발행. TTL=300초(5분)."""
        service_name = "execution-engine"
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

    async def _balance_publisher(shutdown: asyncio.Event) -> None:
        """Refresh wallet balance in Redis every 60 s so orchestrator never sees 0."""
        while not shutdown.is_set():
            try:
                connector = exchange_factory(
                    EXCHANGE,
                    api_key=BYBIT_API_KEY,
                    api_secret=BYBIT_API_SECRET,
                    testnet=BYBIT_TESTNET,
                )
                await connector.connect()
                balance = await connector.get_balance()
                await redis_client.setex(
                    "cache:wallet_balance",
                    300,  # 5분 TTL (60초마다 갱신하므로 충분)
                    _json.dumps(balance),
                )
                log.info("wallet_balance_published", total_usdt=balance.get("total", 0))
                await connector.disconnect()
            except Exception:
                log.exception("wallet_balance_publish_failed")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    # 최초 1회 즉시 실행
    try:
        connector = exchange_factory(
            EXCHANGE,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=BYBIT_TESTNET,
        )
        await connector.connect()
        balance = await connector.get_balance()
        await redis_client.setex("cache:wallet_balance", 300, _json.dumps(balance))
        log.info("wallet_balance_published", total_usdt=balance.get("total", 0))
        await connector.disconnect()
    except Exception:
        log.exception("wallet_balance_publish_failed")

    # --- Execution engine ---
    engine = ExecutionEngine(
        exchange=EXCHANGE,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        redis=redis_client,
        db_pool=db_pool,
        position_tracker=position_tracker,
    )

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # --- Launch ---
    tasks = [
        asyncio.create_task(engine.run(shutdown_event), name="execution_engine"),
        asyncio.create_task(position_tracker.run(shutdown_event), name="position_tracker"),
        asyncio.create_task(_balance_publisher(shutdown_event), name="balance_publisher"),
        asyncio.create_task(_heartbeat_publisher(shutdown_event), name="heartbeat_publisher"),
    ]

    log.info("execution_tasks_launched", count=len(tasks))

    await shutdown_event.wait()
    log.info("shutting_down_tasks")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await redis_client.aclose()
    await db_pool.close()
    log.info("execution_service_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
