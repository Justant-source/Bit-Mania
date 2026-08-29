"""Execution Engine Service — entry point.

Subscribes to ``order:request`` Redis channel, starts the execution engine,
order manager, position tracker, and safety module.
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import pathlib
import signal
from datetime import datetime, timezone
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
# 0.2333 = 70% equity loss threshold / 3x leverage = entry × 0.7667 catastrophic stop
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.2333"))

SERVICE_NAME = "execution-engine"
# Phase 5: 전원 장애 후에도 유지되는 잔고 기준선 (TTL 없음 — Redis 볼륨 영속)
EQUITY_BASELINE_KEY = "ce:phase5:equity_baseline"

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

            CREATE INDEX IF NOT EXISTS idx_orders_request_id ON orders (request_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
            CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders (strategy_id);
            """
        )
    log.info(SERVICE_HEALTH_OK, message="execution tables ensured")


async def _read_equity_baseline(redis: Any) -> float | None:
    """Redis ``ce:phase5:equity_baseline`` 에서 마지막 잔고 기준선을 읽는다."""
    try:
        raw = await redis.get(EQUITY_BASELINE_KEY)
        if not raw:
            return None
        data = _json.loads(raw)
        equity = float(data.get("equity", 0) or 0)
        return equity if equity > 0 else None
    except Exception as exc:
        log.warning(
            SERVICE_HEALTH_FAIL,
            message="Phase5 equity baseline 읽기 실패",
            exc=str(exc)[:300],
        )
        return None


async def _write_equity_baseline(redis: Any, equity: float, source: str) -> None:
    """잔고 기준선을 Redis에 영속 저장 (TTL 없음 — 전원 장애 후 복구용)."""
    if equity <= 0:
        return
    payload = _json.dumps({
        "equity": round(float(equity), 8),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
    })
    await redis.set(EQUITY_BASELINE_KEY, payload)
    # runtime 주기는 wallet balance published 로그로 충분 — 기동/명시적 갱신만 INFO
    _log_fn = log.info if source != "runtime" else log.debug
    _log_fn(
        SERVICE_HEALTH_OK,
        message="Phase5 equity baseline 저장",
        equity=round(float(equity), 4),
        source=source,
        key=EQUITY_BASELINE_KEY,
    )


async def _verify_initial_balance(
    connector: Any,
    expected_usd: float,
    tolerance_pct: float = 5.0,
    expected_source: str = "env",
) -> tuple[bool, float]:
    """Phase 5: 시작 시 잔고가 예상값과 일치하는지 검증.

    Args:
        connector: 거래소 커넥터 (connect() 완료 상태)
        expected_usd: 예상 잔고 (Redis baseline 또는 EXPECTED_INITIAL_BALANCE_USD)
        tolerance_pct: 허용 오차 % (기본 5%)
        expected_source: 기준값 출처 (``redis`` / ``env``)

    Returns:
        (ok, actual_usdt) — ok=True 이면 검증 통과 (또는 조회 실패로 건너뜀)
    """
    try:
        balance = await connector.get_balance()
        actual_usdt = float(balance.get("total", balance.get("USDT", {}).get("total", 0)) or 0)
        diff_pct = abs(actual_usdt - expected_usd) / expected_usd * 100 if expected_usd > 0 else 0
        log.info(
            SERVICE_HEALTH_OK,
            message="Phase5 잔고 검증",
            expected_usd=expected_usd,
            expected_source=expected_source,
            actual_usdt=round(actual_usdt, 4),
            diff_pct=round(diff_pct, 2),
        )
        if diff_pct > tolerance_pct:
            log.error(
                SERVICE_HEALTH_FAIL,
                message="Phase5 잔고 불일치 — 시작 거부",
                expected_usd=expected_usd,
                expected_source=expected_source,
                actual_usdt=round(actual_usdt, 4),
                diff_pct=round(diff_pct, 2),
                tolerance_pct=tolerance_pct,
            )
            return False, actual_usdt
        return True, actual_usdt
    except Exception as exc:
        log.warning(SERVICE_HEALTH_FAIL, message="Phase5 잔고 조회 실패 (검증 건너뜀)", exc=str(exc))
        return True, 0.0  # 조회 실패 시 차단하지 않음 (가용성 우선)


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
    # EXPECTED_INITIAL_BALANCE_USD > 0 이고 메인넷이면 검증.
    # Redis ce:phase5:equity_baseline 이 있으면 .env 대신 그 값을 expected 로 사용
    # (전원 장애 후 재기동 시 운영 중 드리프트를 반영).
    _env_expected = float(os.getenv("EXPECTED_INITIAL_BALANCE_USD", "0"))
    _is_mainnet = not BYBIT_TESTNET
    if _env_expected > 0 and _is_mainnet:
        _redis_baseline = await _read_equity_baseline(redis_client)
        if _redis_baseline is not None:
            _expected_balance = _redis_baseline
            _expected_source = "redis"
        else:
            _expected_balance = _env_expected
            _expected_source = "env"

        log.info(
            SERVICE_HEALTH_OK,
            message="Phase5 잔고 검증 시작",
            expected_usd=_expected_balance,
            expected_source=_expected_source,
            env_expected_usd=_env_expected,
            testnet=BYBIT_TESTNET,
        )
        _verify_connector = exchange_factory(
            EXCHANGE,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=BYBIT_TESTNET,
        )
        try:
            await _verify_connector.connect()
            _balance_ok, _actual_usdt = await _verify_initial_balance(
                _verify_connector,
                _expected_balance,
                expected_source=_expected_source,
            )
        finally:
            try:
                await _verify_connector.disconnect()
            except Exception:
                pass

        if not _balance_ok:
            try:
                _r = aioredis.from_url(REDIS_URL, decode_responses=True)
                await _r.publish("ce:alerts:anomaly", _json.dumps({
                    "type": "anomaly",
                    "message": (
                        f"⛔ Phase 5 잔고 불일치\n"
                        f"예상: ${_expected_balance:.2f} (source={_expected_source})\n"
                        f"실제: ${_actual_usdt:.2f}\n"
                        "execution-engine 시작 거부됨 — 잔고 확인 필요"
                    ),
                    "severity": "critical",
                }))
                await _r.aclose()
            except Exception:
                pass
            raise RuntimeError(
                f"Phase 5 잔고 불일치: expected={_expected_balance} "
                f"(source={_expected_source}) actual={_actual_usdt} "
                "— 5% 이상 차이로 시작 거부. "
                "잔고 확인 후 EXPECTED_INITIAL_BALANCE_USD 수정 또는 "
                f"Redis {EQUITY_BASELINE_KEY} 갱신."
            )
        if _actual_usdt > 0:
            await _write_equity_baseline(redis_client, _actual_usdt, source="startup_ok")
        log.info(
            SERVICE_HEALTH_OK,
            message="Phase5 잔고 검증 통과",
            expected_source=_expected_source,
            actual_usdt=round(_actual_usdt, 4),
        )
    elif _env_expected == 0 and _is_mainnet:
        log.warning(SERVICE_HEALTH_FAIL,
                    message="Phase5 잔고 검증 건너뜀 — EXPECTED_INITIAL_BALANCE_USD 미설정",
                    recommendation="Phase 5에서는 EXPECTED_INITIAL_BALANCE_USD 설정 권장")

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
        """Refresh wallet balance in Redis every 60 s so orchestrator never sees 0.

        PositionTracker의 이미 연결된 커넥터를 재사용 — 매번 load_markets() 재호출 방지.
        Phase 5 equity baseline 도 함께 갱신해 전원 장애 후 재기동 게이트가 최신 잔고를 쓰게 함.
        """
        consecutive_failures = 0
        while not shutdown.is_set():
            try:
                balance = await position_tracker.get_balance()
                balance_json = _json.dumps(balance)
                await redis_client.setex("cache:wallet_balance", 300, balance_json)
                await redis_client.setex(f"cache:balance:{EXCHANGE}", 300, balance_json)
                total = float(balance.get("total", 0) or 0)
                if total > 0 and _is_mainnet and _env_expected > 0:
                    await _write_equity_baseline(redis_client, total, source="runtime")
                log.info(SERVICE_HEALTH_OK, message="wallet balance published", total_usdt=balance.get("total", 0))
                # Successful exchange API call — keep safety network timer fresh
                engine._safety.record_api_response()
                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                # 첫 실패 또는 10회 누적마다 ERROR (→ Telegram 알림)
                # 그 외는 WARNING (Telegram 알림 없음) — 알림 폭주 방지
                if consecutive_failures == 1 or consecutive_failures % 10 == 0:
                    log.error(
                        SERVICE_HEALTH_FAIL,
                        message="wallet balance publish failed",
                        exc_type=type(exc).__name__,
                        exc=str(exc)[:500],
                        consecutive_failures=consecutive_failures,
                    )
                else:
                    log.warning(
                        SERVICE_HEALTH_FAIL,
                        message="wallet balance publish failed (suppressed)",
                        exc_type=type(exc).__name__,
                        exc=str(exc)[:300],
                        consecutive_failures=consecutive_failures,
                    )
            # 실패 누적 시 지수 백오프: 60s → 120s → 300s(상한)
            wait = 60 if consecutive_failures == 0 else min(60 * (2 ** min(consecutive_failures - 1, 2)), 300)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    # 최초 1회 즉시 실행 — position_tracker 커넥터 재사용
    try:
        balance = await position_tracker.get_balance()
        _initial_balance_json = _json.dumps(balance)
        await redis_client.setex("cache:wallet_balance", 300, _initial_balance_json)
        await redis_client.setex(f"cache:balance:{EXCHANGE}", 300, _initial_balance_json)
        _total0 = float(balance.get("total", 0) or 0)
        if _total0 > 0 and _is_mainnet and _env_expected > 0:
            await _write_equity_baseline(redis_client, _total0, source="runtime")
        log.info(SERVICE_HEALTH_OK, message="wallet balance published (initial)", total_usdt=balance.get("total", 0))
    except Exception as exc:
        log.error(SERVICE_HEALTH_FAIL, message="wallet balance publish failed (initial)", exc_type=type(exc).__name__, exc=str(exc)[:500])

    # --- Execution engine ---
    engine = ExecutionEngine(
        exchange=EXCHANGE,
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        testnet=BYBIT_TESTNET,
        redis=redis_client,
        db_pool=db_pool,
        position_tracker=position_tracker,
        stop_loss_pct=STOP_LOSS_PCT,
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
