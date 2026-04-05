#!/usr/bin/env python3
"""Phase 4 헬스체크 스크립트.

자동으로 검증 가능한 항목을 체크하고 결과를 출력합니다.

사용법:
    docker compose --profile backtest run --rm backtester python scripts/phase4_health_check.py

환경변수:
    DB_PASSWORD, DB_HOST (default: postgres), DB_NAME (default: cryptoengine),
    DB_USER (default: cryptoengine), DB_PORT (default: 5432),
    BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_TESTNET,
    REDIS_URL (default: redis://redis:6379)
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

# ── 의존성 임포트 (없으면 메시지 출력 후 종료) ─────────────────────────────
try:
    import asyncpg
except ImportError:
    print("[FATAL] asyncpg 패키지가 없습니다. pip install asyncpg")
    sys.exit(1)

try:
    import redis.asyncio as aioredis
except ImportError:
    print("[FATAL] redis 패키지가 없습니다. pip install redis")
    sys.exit(1)

try:
    import ccxt.async_support as ccxt
except ImportError:
    print("[FATAL] ccxt 패키지가 없습니다. pip install ccxt")
    sys.exit(1)

# ── 결과 집계 ─────────────────────────────────────────────────────────────

results: list[dict[str, Any]] = []


def record(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    results.append({"name": name, "status": status, "detail": detail})
    marker = "[PASS]" if passed else "[FAIL]"
    print(f"  {marker} {name}" + (f" — {detail}" if detail else ""))


# ── 환경변수 ─────────────────────────────────────────────────────────────

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "cryptoengine")
DB_USER = os.getenv("DB_USER", "cryptoengine")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")


# ── 1. DB 연결 ────────────────────────────────────────────────────────────

async def check_db() -> asyncpg.Connection | None:
    """PostgreSQL 연결 확인."""
    print("\n[1] PostgreSQL 연결 확인")
    try:
        dsn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        conn = await asyncio.wait_for(asyncpg.connect(dsn), timeout=10)
        version = await conn.fetchval("SELECT version()")
        record("PostgreSQL 연결", True, version.split(",")[0])
        return conn
    except asyncio.TimeoutError:
        record("PostgreSQL 연결", False, "연결 타임아웃 (10s)")
    except Exception as e:
        record("PostgreSQL 연결", False, str(e))
    return None


# ── 2. Redis 연결 ─────────────────────────────────────────────────────────

async def check_redis() -> aioredis.Redis | None:
    """Redis ping 확인."""
    print("\n[2] Redis 연결 확인")
    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        pong = await asyncio.wait_for(r.ping(), timeout=5)
        record("Redis ping", pong is True or pong == "PONG", f"응답: {pong}")
        return r
    except asyncio.TimeoutError:
        record("Redis ping", False, "연결 타임아웃 (5s)")
    except Exception as e:
        record("Redis ping", False, str(e))
    return None


# ── 3. Bybit 테스트넷 API ─────────────────────────────────────────────────

async def check_bybit_api() -> None:
    """ccxt로 테스트넷 잔고 조회 — BYBIT_TESTNET=true 검증 포함."""
    print("\n[3] Bybit 테스트넷 API 확인")

    if not BYBIT_TESTNET:
        record("BYBIT_TESTNET=true", False, "BYBIT_TESTNET 환경변수가 false — 절대 실전 전환 금지")
        return

    record("BYBIT_TESTNET=true", True, "테스트넷 모드 활성화됨")

    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        record("Bybit API 키 설정", False, "BYBIT_API_KEY 또는 BYBIT_API_SECRET 미설정")
        return

    exchange = ccxt.bybit(
        {
            "apiKey": BYBIT_API_KEY,
            "secret": BYBIT_API_SECRET,
            "options": {"defaultType": "spot"},
        }
    )
    exchange.set_sandbox_mode(True)

    try:
        balance = await asyncio.wait_for(exchange.fetch_balance(), timeout=15)
        usdt_free = balance.get("USDT", {}).get("free", 0)
        record(
            "Bybit 테스트넷 잔고 조회",
            True,
            f"USDT 잔고: {usdt_free:,.2f}",
        )
    except asyncio.TimeoutError:
        record("Bybit 테스트넷 잔고 조회", False, "API 타임아웃 (15s)")
    except Exception as e:
        record("Bybit 테스트넷 잔고 조회", False, str(e))
    finally:
        await exchange.close()


# ── 4. 오픈 포지션 확인 ───────────────────────────────────────────────────

async def check_open_positions(conn: asyncpg.Connection) -> None:
    """positions 테이블에서 현재 오픈 포지션 조회."""
    print("\n[4] 오픈 포지션 확인")
    try:
        rows = await conn.fetch(
            "SELECT strategy_id, symbol, side, size, entry_price, opened_at "
            "FROM positions WHERE closed_at IS NULL ORDER BY opened_at DESC"
        )
        if rows:
            record("오픈 포지션", True, f"총 {len(rows)}개 포지션 오픈")
            for row in rows:
                print(
                    f"       strategy={row['strategy_id']}  symbol={row['symbol']}"
                    f"  side={row['side']}  qty={row['size']}"
                    f"  entry={row['entry_price']}  since={row['opened_at']}"
                )
        else:
            record("오픈 포지션", True, "현재 오픈 포지션 없음 (정상)")
    except Exception as e:
        record("오픈 포지션 조회", False, str(e))


# ── 5. Kill Switch 이력 ───────────────────────────────────────────────────

async def check_kill_switch_history(conn: asyncpg.Connection) -> None:
    """kill_switch_events 테이블 최근 5개 조회."""
    print("\n[5] Kill Switch 이력 확인")
    try:
        rows = await conn.fetch(
            "SELECT level, reason, triggered_at, resolved_at "
            "FROM kill_switch_events ORDER BY triggered_at DESC LIMIT 5"
        )
        if rows:
            unresolved = [r for r in rows if r["resolved_at"] is None]
            if unresolved:
                record(
                    "Kill Switch 이력",
                    False,
                    f"미해제 Kill Switch {len(unresolved)}건 — 수동 확인 필요",
                )
            else:
                record("Kill Switch 이력", True, f"최근 {len(rows)}건 모두 해제됨")
            for row in rows:
                resolved = str(row["resolved_at"]) if row["resolved_at"] else "미해제"
                print(
                    f"       level={row['level']}  reason={row['reason']}"
                    f"  triggered={row['triggered_at']}  resolved={resolved}"
                )
        else:
            record("Kill Switch 이력", True, "발동 이력 없음")
    except Exception as e:
        record("Kill Switch 이력 조회", False, str(e))


# ── 6. 서비스 상태 (market-data) ─────────────────────────────────────────

async def check_market_data_service(r: aioredis.Redis) -> None:
    """Redis에서 cache:ohlcv:bybit:BTCUSDT:1h 키 확인."""
    print("\n[6] market-data 서비스 상태 확인")
    key = "cache:ohlcv:bybit:BTCUSDT:1h"
    try:
        value = await r.hgetall(key)
        if value:
            close = value.get("close", "?")
            record("market-data 서비스", True, f"BTC 1h 캔들 수신 중 — close={close}")
        else:
            record(
                "market-data 서비스",
                False,
                f"Redis 키 '{key}' 없음 — market-data 서비스 미동작 가능성",
            )
    except Exception as e:
        record("market-data 서비스 상태", False, str(e))


# ── 7. 펀딩비 모니터링 ────────────────────────────────────────────────────

async def check_funding_rate(r: aioredis.Redis) -> None:
    """Redis에서 최신 펀딩비 확인."""
    print("\n[7] 펀딩비 모니터링")
    candidates = [
        "cache:funding:bybit:BTCUSDT",
        "market:funding_rate:BTCUSDT",
        "market:funding_rate:bybit:BTCUSDT",
    ]
    found = False
    for key in candidates:
        try:
            value = await r.hgetall(key)  # hash type (hset by market-data)
            if not value:
                value = await r.get(key)  # fallback: string type
            if value:
                rate = value.get("rate", value) if isinstance(value, dict) else value
                record("펀딩비 데이터", True, f"키={key}  rate={rate}")
                found = True
                break
        except Exception:
            pass

    if not found:
        # pub/sub 채널 이력은 직접 조회 불가 — 최근 스트림/리스트 확인
        try:
            # market:funding_rate 채널에 데이터가 있는지 간접 확인
            keys = await r.keys("market:funding*")
            if keys:
                record("펀딩비 데이터", True, f"관련 키 {len(keys)}개 발견: {keys[:3]}")
            else:
                record(
                    "펀딩비 데이터",
                    False,
                    "Redis에서 펀딩비 키 없음 — market-data 서비스 확인 필요",
                )
        except Exception as e:
            record("펀딩비 데이터", False, str(e))


# ── 8. 24h 미수신 알림 ────────────────────────────────────────────────────

async def check_funding_rate_history(conn: asyncpg.Connection) -> None:
    """funding_rate_history 테이블에 최근 24h 데이터가 있는지 확인."""
    print("\n[8] 최근 24h 펀딩비 히스토리 확인")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM funding_rate_history WHERE timestamp >= $1",
            cutoff,
        )
        if count and count > 0:
            record(
                "24h 펀딩비 히스토리",
                True,
                f"최근 24h 동안 {count}건 기록됨 (8h 간격이면 최소 3건 기대)",
            )
        else:
            record(
                "24h 펀딩비 히스토리",
                False,
                "최근 24h 데이터 없음 — market-data 또는 DB 저장 로직 확인",
            )
    except Exception as e:
        # 테이블이 없을 수도 있음
        if "does not exist" in str(e).lower() or "relation" in str(e).lower():
            record(
                "24h 펀딩비 히스토리",
                False,
                "funding_rate_history 테이블 없음 — DB 마이그레이션 필요",
            )
        else:
            record("24h 펀딩비 히스토리 조회", False, str(e))


# ── 요약 출력 ─────────────────────────────────────────────────────────────

def print_summary() -> int:
    """결과 요약을 출력하고 실패 건수를 반환."""
    passed = [r for r in results if r["status"] == "PASS"]
    failed = [r for r in results if r["status"] == "FAIL"]

    print("\n" + "=" * 60)
    print("Phase 4 헬스체크 결과 요약")
    print("=" * 60)
    print(f"  총 체크 항목 : {len(results)}")
    print(f"  통과 (PASS)  : {len(passed)}")
    print(f"  실패 (FAIL)  : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\n[조치 필요 항목]")
        for r in failed:
            print(f"  - {r['name']}: {r['detail']}")
    else:
        print("\n모든 항목 통과 — Phase 4 테스트넷 포워드 테스트 준비 완료.")

    print()
    return len(failed)


# ── 메인 ─────────────────────────────────────────────────────────────────

async def main() -> int:
    print("=" * 60)
    print("Phase 4 헬스체크 — 테스트넷 포워드 테스트 준비 검증")
    print(f"실행 시각: {datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 60)

    # DB 및 Redis 연결
    conn = await check_db()
    r = await check_redis()

    # Bybit API (DB/Redis 독립)
    await check_bybit_api()

    # DB 의존 체크
    if conn:
        await check_open_positions(conn)
        await check_kill_switch_history(conn)
        await check_funding_rate_history(conn)
        await conn.close()
    else:
        print("\n[4][5][8] DB 연결 실패로 건너뜀")
        for name in ["오픈 포지션 조회", "Kill Switch 이력 조회", "24h 펀딩비 히스토리 조회"]:
            record(name, False, "DB 연결 불가")

    # Redis 의존 체크
    if r:
        await check_market_data_service(r)
        await check_funding_rate(r)
        await r.aclose()
    else:
        print("\n[6][7] Redis 연결 실패로 건너뜀")
        for name in ["market-data 서비스 상태", "펀딩비 데이터"]:
            record(name, False, "Redis 연결 불가")

    return print_summary()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
