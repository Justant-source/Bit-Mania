#!/usr/bin/env python3
"""Phase 5 → 테스트넷 롤백 스크립트.

메인넷 운영 중 문제 발생 시 테스트넷으로 안전하게 돌아간다.

안전 절차:
  1. 현재 .env 백업 → .env.mainnet.bak.YYYYMMDD
  2. .env.testnet.bak.* 파일 탐색 (가장 최신 선택)
  3. 사용자 확인 후 testnet 백업에서 BYBIT_TESTNET=true 복원
  4. Redis 포지션 캐시 클리어
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).parent
CRYPTOENGINE_DIR = SCRIPT_DIR.parent
ENV_FILE = CRYPTOENGINE_DIR / ".env"


def load_env(path: Path) -> dict[str, str]:
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


async def clear_redis_position_cache(env: dict[str, str]) -> None:
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
        warn(f"Redis 연결 실패: {e}")


async def main() -> None:
    print("=" * 60)
    print("  CryptoEngine — 테스트넷 롤백 스크립트")
    print("=" * 60)
    print("\n  이 스크립트는 BYBIT_TESTNET=true로 롤백합니다.\n")

    # ── Step 1: .env 존재 확인 ─────────────────────────────────
    step(1, ".env 파일 확인")
    if not ENV_FILE.exists():
        fail(f".env 파일 없음: {ENV_FILE}")
    env = load_env(ENV_FILE)
    current_testnet = env.get("BYBIT_TESTNET", "true").lower()
    if current_testnet == "true":
        warn("이미 BYBIT_TESTNET=true — 롤백 불필요할 수 있음")
    else:
        ok(f"현재 BYBIT_TESTNET={current_testnet} → true로 롤백 예정")

    # ── Step 2: 현재 .env 백업 ────────────────────────────────
    step(2, "현재 .env 백업 (메인넷 설정 보존)")
    backup_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    mainnet_bak = ENV_FILE.parent / f".env.mainnet.bak.{backup_suffix}"
    import shutil as _shutil
    _shutil.copy2(ENV_FILE, mainnet_bak)
    ok(f"메인넷 설정 백업: {mainnet_bak.name}")

    # ── Step 3: testnet 백업 파일 탐색 ───────────────────────
    step(3, "테스트넷 백업 파일 탐색")
    testnet_baks = sorted(
        ENV_FILE.parent.glob(".env.testnet.bak.*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if testnet_baks:
        latest_bak = testnet_baks[0]
        ok(f"가장 최신 테스트넷 백업: {latest_bak.name}")
        print(f"   모든 백업: {[p.name for p in testnet_baks]}")
    else:
        warn("테스트넷 백업 파일 없음 — BYBIT_TESTNET=true만 직접 설정")
        latest_bak = None

    # ── Step 4: 사용자 확인 ───────────────────────────────────
    step(4, "확인")
    if latest_bak:
        print(f"\n  복원 대상: {latest_bak.name}")
        print("  복원 후 BYBIT_TESTNET=true 강제 설정")
    else:
        print("  BYBIT_TESTNET=true 만 설정 (나머지 .env 값 유지)")

    answer = input("\n  테스트넷으로 롤백하려면 'rollback' 입력: ").strip()
    if answer != "rollback":
        print("\n  취소되었습니다.")
        sys.exit(0)

    # ── Step 5: 롤백 실행 ────────────────────────────────────
    step(5, "롤백 실행")
    if latest_bak:
        # 백업 파일로 완전 복원
        _shutil.copy2(latest_bak, ENV_FILE)
        ok(f"{latest_bak.name} → .env 복원 완료")
    # 반드시 BYBIT_TESTNET=true 보장
    write_env_key(ENV_FILE, "BYBIT_TESTNET", "true")
    # Phase5 모드 해제
    write_env_key(ENV_FILE, "PHASE5_MODE", "false")
    ok("BYBIT_TESTNET=true 설정 확인")
    ok("PHASE5_MODE=false 설정")

    # ── Step 6: Redis 캐시 클리어 ─────────────────────────────
    step(6, "Redis 포지션 캐시 클리어")
    env_new = load_env(ENV_FILE)
    await clear_redis_position_cache(env_new)

    # ── 완료 ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  ✅ 테스트넷 롤백 완료!")
    print("=" * 60)
    print("\n  다음 단계:")
    print("  1. docker compose up -d --build --no-deps funding-arb execution-engine")
    print("  2. docker compose logs -f funding-arb | grep -E '복구|recovered|started'")
    print("  3. 테스트넷 포지션 상태 확인\n")
    print("  ⚠️  메인넷 설정 백업: " + mainnet_bak.name)
    print("      절대 삭제하지 마세요 (재전환 시 참고용)\n")


if __name__ == "__main__":
    asyncio.run(main())
