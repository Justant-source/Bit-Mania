#!/usr/bin/env python3
"""Phase 5 메인넷 전환 스크립트.

실행 전 반드시 다음 조건을 충족해야 한다:
  - .env 파일이 cryptoengine/ 디렉토리에 존재
  - DB에 open position이 0개
  - 메인넷 API 키가 .env에 설정됨
  - 사용자가 'yes I am sure' 확인 입력

안전 절차:
  1. .env 백업 → .env.testnet.bak.YYYYMMDD
  2. 각 조건 사전 검증 (실패 시 종료)
  3. 사용자 이중 확인
  4. BYBIT_TESTNET=false 설정
  5. Redis 포지션 캐시 클리어
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


# ── 경로 설정 ─────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
CRYPTOENGINE_DIR = SCRIPT_DIR.parent
ENV_FILE = CRYPTOENGINE_DIR / ".env"
ENV_BACKUP_SUFFIX = datetime.now().strftime("%Y%m%d")


def load_env(path: Path) -> dict[str, str]:
    """Load key=value pairs from .env file."""
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def write_env_key(path: Path, key: str, value: str) -> None:
    """Update a single key=value in .env file in-place."""
    lines = path.read_text().splitlines()
    updated = False
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(f"{key}=") or stripped.startswith(f"{key} ="):
            new_lines.append(f"{key}={value}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"{key}={value}")
    path.write_text("\n".join(new_lines) + "\n")


def step(n: int, msg: str) -> None:
    print(f"\n[Step {n}] {msg}")


def ok(msg: str) -> None:
    print(f"  ✅ {msg}")


def warn(msg: str) -> None:
    print(f"  ⚠️  {msg}")


def fail(msg: str) -> None:
    print(f"  ❌ {msg}")
    sys.exit(1)


async def check_db_positions(env: dict[str, str]) -> int:
    """Return count of open positions in DB."""
    try:
        import asyncpg
    except ImportError:
        warn("asyncpg 없음 — DB 포지션 확인 건너뜀")
        return -1

    dsn = (
        f"postgresql://{env.get('DB_USER', 'cryptoengine')}"
        f":{env.get('DB_PASSWORD', 'cryptoengine')}"
        f"@{env.get('DB_HOST', 'localhost')}"
        f":{env.get('DB_PORT', '5432')}"
        f"/{env.get('DB_NAME', 'cryptoengine')}"
    )
    try:
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2, command_timeout=10)
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM positions WHERE status='open'")
            count = row["cnt"] if row else 0
        await pool.close()
        return count
    except Exception as e:
        warn(f"DB 연결 실패: {e}")
        return -1


async def clear_redis_position_cache(env: dict[str, str]) -> None:
    """Delete position-related Redis keys."""
    try:
        import redis.asyncio as aioredis
    except ImportError:
        warn("redis 패키지 없음 — Redis 캐시 클리어 건너뜀")
        return

    redis_url = env.get("REDIS_URL", "redis://localhost:6379")
    try:
        r = aioredis.from_url(redis_url, decode_responses=True)
        patterns = ["cache:position:*", "strategy:saved_state:*", "cache:stoploss:*"]
        total_deleted = 0
        for pattern in patterns:
            keys = await r.keys(pattern)
            if keys:
                deleted = await r.delete(*keys)
                total_deleted += deleted
                ok(f"Redis 키 삭제: {pattern} ({deleted}개)")
        if total_deleted == 0:
            ok("Redis 포지션 캐시: 삭제할 키 없음 (정상)")
        await r.aclose()
    except Exception as e:
        warn(f"Redis 연결 실패: {e} — 수동으로 확인 필요")


async def check_mainnet_balance(env: dict[str, str]) -> float | None:
    """Query Bybit mainnet balance. Returns USDT balance or None on error."""
    try:
        import ccxt.async_support as ccxt
    except ImportError:
        warn("ccxt 없음 — 잔고 조회 건너뜀")
        return None

    api_key = env.get("BYBIT_API_KEY", "")
    api_secret = env.get("BYBIT_API_SECRET", "")
    if not api_key or not api_secret:
        return None

    try:
        exchange = ccxt.bybit({
            "apiKey": api_key,
            "secret": api_secret,
            "options": {"defaultType": "unified"},
        })
        balance = await exchange.fetch_balance()
        await exchange.close()
        usdt = float(balance.get("USDT", {}).get("total", 0) or 0)
        return usdt
    except Exception as e:
        warn(f"잔고 조회 실패: {e}")
        return None


async def main() -> None:
    print("=" * 60)
    print("  CryptoEngine — Phase 5 메인넷 전환 스크립트")
    print("=" * 60)
    print("\n⚠️  이 스크립트는 BYBIT_TESTNET=false로 전환합니다.")
    print("   실제 자금이 위험에 노출될 수 있습니다. 신중하게 진행하세요.\n")

    # ── Step 1: .env 존재 확인 ─────────────────────────────────
    step(1, ".env 파일 확인")
    if not ENV_FILE.exists():
        fail(f".env 파일 없음: {ENV_FILE}\n   cp .env.example .env 후 API 키 설정 필요")
    ok(f".env 존재: {ENV_FILE}")
    env = load_env(ENV_FILE)

    # ── Step 2: .env 백업 ──────────────────────────────────────
    step(2, ".env 백업")
    backup_path = ENV_FILE.parent / f".env.testnet.bak.{ENV_BACKUP_SUFFIX}"
    if backup_path.exists():
        warn(f"백업 파일 이미 존재: {backup_path.name} — 덮어쓰기")
    shutil.copy2(ENV_FILE, backup_path)
    ok(f"백업 완료: {backup_path.name}")
    print(f"   📌 롤백 필요 시: python scripts/switch_to_testnet.py")

    # ── Step 3: 현재 BYBIT_TESTNET 값 확인 ────────────────────
    step(3, "BYBIT_TESTNET 값 확인")
    current_testnet = env.get("BYBIT_TESTNET", "true").lower()
    if current_testnet == "false":
        warn("BYBIT_TESTNET 이미 false — 중복 전환 시도")
        print("   이미 메인넷 모드입니다. 계속 진행하면 Redis 캐시 클리어만 수행됩니다.")
    else:
        ok(f"현재 BYBIT_TESTNET={current_testnet} → false로 전환 예정")

    # ── Step 4: API 키 존재 확인 ──────────────────────────────
    step(4, "메인넷 API 키 확인")
    api_key = env.get("BYBIT_API_KEY", "")
    api_secret = env.get("BYBIT_API_SECRET", "")
    if not api_key or api_key.startswith("test") or len(api_key) < 10:
        fail("BYBIT_API_KEY 없거나 테스트넷 키로 보임 — 메인넷 키 설정 필요")
    if not api_secret or len(api_secret) < 10:
        fail("BYBIT_API_SECRET 없음 — 메인넷 시크릿 설정 필요")
    ok(f"API 키 존재: {api_key[:8]}***")

    # ── Step 5: DB 오픈 포지션 확인 ──────────────────────────
    step(5, "DB 오픈 포지션 확인")
    open_count = await check_db_positions(env)
    if open_count == -1:
        warn("DB 확인 건너뜀 — 수동으로 포지션 없음을 확인하세요")
        print("   확인 명령: docker compose exec postgres psql -U cryptoengine -d cryptoengine")
        print("              -c \"SELECT * FROM positions WHERE status='open';\"")
    elif open_count > 0:
        fail(f"오픈 포지션 {open_count}개 존재 — 모두 청산 후 재시도\n"
             "   make emergency 또는 Telegram /emergency_close 사용")
    else:
        ok("오픈 포지션 0개 확인")

    # ── Step 6: 메인넷 잔고 조회 ──────────────────────────────
    step(6, "메인넷 잔고 조회 (테스트 연결)")
    usdt_balance = await check_mainnet_balance(env)
    if usdt_balance is None:
        warn("잔고 조회 실패 — API 키 권한 또는 네트워크 확인 필요")
        print("   Bybit 웹사이트에서 수동으로 잔고 확인 후 계속하세요")
    elif usdt_balance < 100:
        warn(f"메인넷 USDT 잔고 부족: {usdt_balance:.2f} USDT (Phase 5 최소 $200 권장)")
    else:
        ok(f"메인넷 USDT 잔고: {usdt_balance:.2f} USDT")

    # ── Step 7: 사용자 이중 확인 ──────────────────────────────
    step(7, "최종 확인")
    print("\n  ⚠️  다음 작업을 수행합니다:")
    print("    1. BYBIT_TESTNET=true → false 변경")
    print("    2. Redis 포지션 캐시 전체 삭제")
    print(f"\n  백업 파일: {backup_path.name}")
    print("  롤백 명령: python scripts/switch_to_testnet.py\n")

    answer1 = input("  계속하려면 'yes I am sure' 를 입력하세요: ").strip()
    if answer1 != "yes I am sure":
        print("\n  취소되었습니다. 변경 사항 없음.")
        sys.exit(0)

    answer2 = input("  마지막 확인: 실제 자금으로 운영됩니다. 'MAINNET' 입력: ").strip()
    if answer2 != "MAINNET":
        print("\n  취소되었습니다. 변경 사항 없음.")
        sys.exit(0)

    # ── Step 8: .env 수정 ─────────────────────────────────────
    step(8, ".env 수정 (BYBIT_TESTNET=false)")
    write_env_key(ENV_FILE, "BYBIT_TESTNET", "false")
    ok("BYBIT_TESTNET=false 설정 완료")

    # ── Step 9: Redis 캐시 클리어 ─────────────────────────────
    step(9, "Redis 포지션 캐시 클리어")
    await clear_redis_position_cache(env)

    # ── 완료 ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  ✅ 메인넷 전환 완료!")
    print("=" * 60)
    print("\n  다음 단계:")
    print("  1. docker compose stop funding-arb strategy-orchestrator execution-engine")
    print("  2. .env에 PHASE5_MODE=true, EXPECTED_INITIAL_BALANCE_USD=200,")
    print("     STRICT_MONITORING_HOURS=24 추가")
    print("  3. docker compose --profile backtest run --rm backtester \\")
    print("       python scripts/phase5_preflight.py")
    print("  4. preflight 8개 항목 PASS 확인 후 서비스 기동")
    print("  5. arch/PHASE4_MONITORING.md → Phase 5 체크리스트 참조\n")


if __name__ == "__main__":
    asyncio.run(main())
