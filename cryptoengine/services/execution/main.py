"""Execution Engine Service — entry point.

Subscribes to ``order:request`` Redis channel, starts the execution engine,
order manager, position tracker, and safety module.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import signal
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.logging_config import setup_logging
from shared.log_writer import init_log_writer, close_log_writer
from shared.log_events import *
from engine import ExecutionEngine
from position_tracker import PositionTracker
from shared.exchange.factory import exchange_factory

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

SERVICE_NAME = "execution-engine"

log = structlog.get_logger(__name__)


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
    log.info(SERVICE_HEALTH_OK, message="execution tables ensured")


async def _verify_initial_balance(
    connector: Any,
    expected_usd: float,
    tolerance_pct: float = 5.0,
) -> bool:
    """Phase 5: 시작 시 잔고가 예상값과 일치하는지 검증.

    Args:
        connector: 거래소 커넥터 (connect() 완료 상태)
        expected_usd: 예상 초기 잔고 (EXPECTED_INITIAL_BALANCE_USD)
        tolerance_pct: 허용 오차 % (기본 5%)

    Returns:
        True = 검증 통과 (차이가 tolerance 이내)
        False = 검증 실패 (시작 거부 권장)
    """
    try:
        balance = await connector.get_balance()
        actual_usdt = float(balance.get("total", balance.get("USDT", {}).get("total", 0)) or 0)
        diff_pct = abs(actual_usdt - expected_usd) / expected_usd * 100 if expected_usd > 0 else 0
        log.info(
            SERVICE_HEALTH_OK,
            message="Phase5 잔고 검증",
            expected_usd=expected_usd,
            actual_usdt=round(actual_usdt, 4),
            diff_pct=round(diff_pct, 2),
        )
        if diff_pct > tolerance_pct:
            log.error(
                SERVICE_HEALTH_FAIL,
                message="Phase5 잔고 불일치 — 시작 거부",
                expected_usd=expected_usd,
                actual_usdt=round(actual_usdt, 4),
                diff_pct=round(diff_pct, 2),
                tolerance_pct=tolerance_pct,
            )
            return False
        return True
    except Exception as exc:
        log.warning(SERVICE_HEALTH_FAIL, message="Phase5 잔고 조회 실패 (검증 건너뜀)", exc=str(exc))
        return True  # 조회 실패 시 차단하지 않음 (보수적이지 않은 선택이지만 가용성 우선)


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
    log.info(SERVICE_STARTED, message="execution-engine 서비스 시작", exchange=EXCHANGE)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    log.info(REDIS_CONNECTED, message="Redis 연결 성공", redis=REDIS_URL)

    await _create_tables(db_pool)

    # ── Phase 5: 초기 잔고 검증 ──────────────────────────────
    # EXPECTED_INITIAL_BALANCE_USD > 0 이고 메인넷(BYBIT_TESTNET=false)이면 검증
    _expected_balance = float(os.getenv("EXPECTED_INITIAL_BALANCE_USD", "0"))
    _is_mainnet = not BYBIT_TESTNET
    if _expected_balance > 0 and _is_mainnet:
        log.info(SERVICE_HEALTH_OK, message="Phase5 잔고 검증 시작",
                 expected_usd=_expected_balance, testnet=BYBIT_TESTNET)
        _verify_connector = exchange_factory(
            EXCHANGE,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=BYBIT_TESTNET,
        )
        try:
            await _verify_connector.connect()
            _balance_ok = await _verify_initial_balance(_verify_connector, _expected_balance)
        finally:
            try:
                await _verify_connector.disconnect()
            except Exception:
                pass

        if not _balance_ok:
            # Telegram 알림 시도
            try:
                import json as _json
                _r = aioredis.from_url(REDIS_URL, decode_responses=True)
                await _r.publish("ce:alerts:anomaly", _json.dumps({
                    "type": "anomaly",
                    "message": (
                        f"⛔ Phase 5 잔고 불일치\n"
                        f"예상: ${_expected_balance:.2f}\n"
                        f"실제: 조회 결과 불일치\n"
                        "execution-engine 시작 거부됨 — 잔고 확인 필요"
                    ),
                    "severity": "critical",
                }))
                await _r.aclose()
            except Exception:
                pass
            raise RuntimeError(
                f"Phase 5 잔고 불일치: EXPECTED_INITIAL_BALANCE_USD={_expected_balance} "
                "와 실제 잔고가 5% 이상 차이남 — 시작 거부. "
                "잔고 확인 후 EXPECTED_INITIAL_BALANCE_USD 수정 또는 unset."
            )
        log.info(SERVICE_HEALTH_OK, message="Phase5 잔고 검증 통과")
    elif _expected_balance == 0 and _is_mainnet:
        log.warning(SERVICE_HEALTH_FAIL,
                    message="Phase5 잔고 검증 건너뜀 — EXPECTED_INITIAL_BALANCE_USD 미설정",
                    recommendation="Phase 5에서는 EXPECTED_INITIAL_BALANCE_USD=200 설정 권장")

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
                log.warning(SERVICE_HEALTH_FAIL, message="heartbeat publish failed", service=service_name)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def _balance_publisher(shutdown: asyncio.Event) -> None:
        """Refresh wallet balance in Redis every 60 s so orchestrator never sees 0."""
        while not shutdown.is_set():
            connector = None
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
                log.info(SERVICE_HEALTH_OK, message="wallet balance published", total_usdt=balance.get("total", 0))
            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="wallet balance publish failed")
            finally:
                if connector is not None:
                    try:
                        await connector.disconnect()
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    # 최초 1회 즉시 실행
    _init_connector = None
    try:
        _init_connector = exchange_factory(
            EXCHANGE,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=BYBIT_TESTNET,
        )
        await _init_connector.connect()
        balance = await _init_connector.get_balance()
        await redis_client.setex("cache:wallet_balance", 300, _json.dumps(balance))
        log.info(SERVICE_HEALTH_OK, message="wallet balance published (initial)", total_usdt=balance.get("total", 0))
    except Exception:
        log.exception(SERVICE_HEALTH_FAIL, message="wallet balance publish failed (initial)")
    finally:
        if _init_connector is not None:
            try:
                await _init_connector.disconnect()
            except Exception:
                pass

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
        log.info(SERVICE_STOPPING, message="shutdown signal received")
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

    log.info(SERVICE_STARTED, message="execution tasks launched", count=len(tasks))

    await shutdown_event.wait()
    log.info(SERVICE_STOPPING, message="execution-engine 서비스 종료 중")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await redis_client.aclose()
    log.info(SERVICE_STOPPED, message="execution-engine 서비스 종료")
    await close_log_writer()
    await db_pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
