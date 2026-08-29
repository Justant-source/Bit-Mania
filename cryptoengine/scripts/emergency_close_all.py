"""긴급 청산 — 모든 포지션 시장가 청산 + 신규 주문 차단.

`make emergency`가 호출한다. execution-engine 컨테이너 안에서 stdin으로 주입되어
실행되므로(`docker compose exec -T execution-engine python - < 이 파일`) 이미지 재빌드
없이 즉시 동작한다.

## 왜 오케스트레이터를 우회하는가

정상 경로는 `KillSwitch.trigger*()` → `_on_kill_switch_trigger` 콜백 → 전략에
`action="stop"` 발행 → `on_stop()`이 청산하는 흐름이다. 그런데 2026-08-29 확인 결과
`ce:kill_switch` 채널에 **구독자가 없어서** 외부(텔레그램/CLI)에서 그 체인을 시작할
방법이 없었다. 즉 운영자 수동 청산 경로가 실효 상태였다.

이 스크립트는 그 체인의 마지막 단계(전략 stop 명령)를 **직접 발행**해서 재시작 없이
청산을 성사시킨다. 근본 수정(오케스트레이터에 `ce:kill_switch` 구독 추가)은 재빌드가
필요하므로 별도 작업 D7로 분리되어 있다 — `.request/legacy-cleanup-deferred-20260829.md`.

근본 수정이 배포된 뒤에도 이 스크립트는 유효하다. 오케스트레이터가 죽어 있을 때도
동작하는 **최후 수단(last resort)**이기 때문이다.

## 수행 순서

1. `ce:kill_switch:active = true` SET — execution-engine safety가 신규 주문을 즉시 차단
   (safety.py `_check_kill_switch`, fail-closed)
2. `ce:kill_switch` PUBLISH — 근본 수정 배포 후에는 오케스트레이터가 이걸 받아
   정규 킬스위치 체인을 돌린다 (미배포 시에는 무해하게 무시됨)
3. `strategy:command:supertrend-01`로 `action="stop", reason="kill_switch"` 발행
   → `on_stop()`이 거래소 상태 동기화 후 시장가 청산
   ※ reason이 `_SHUTDOWN_NO_LIQUIDATE`("service_shutdown" 등)에 들어가면 청산하지
     않으므로 반드시 "kill_switch"여야 한다
4. 청산 확인 — `ce:positions:all` 캐시를 폴링해 포지션이 사라졌는지 검증

종료 코드: 0=청산 확인, 1=미확인(수동 개입 필요), 2=Redis 연결 실패
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import redis.asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

KILL_SWITCH_CHANNEL = "ce:kill_switch"
KILL_SWITCH_ACTIVE_KEY = "ce:kill_switch:active"
POSITIONS_CACHE_KEY = "ce:positions:all"

# strategy_id → command 채널. 채널명은 base_strategy가 구독하는
# f"strategy:command:{strategy_id}" 규칙을 따른다 (services/strategies/base_strategy.py:115).
# 전략 인스턴스 id는 supertrend/main.py의 STRATEGY_ID 기본값과 일치해야 한다.
STRATEGY_IDS: tuple[str, ...] = ("supertrend-01",)

VERIFY_TIMEOUT_SECONDS = 60
VERIFY_POLL_SECONDS = 3


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(msg: str) -> None:
    print(f"[{_now()}] {msg}", flush=True)


async def _open_position_count(redis) -> int | None:
    """ce:positions:all 캐시에서 열린 포지션 수를 읽는다. 캐시 부재/파싱 실패 시 None."""
    try:
        raw = await redis.get(POSITIONS_CACHE_KEY)
    except Exception as exc:  # noqa: BLE001 — 검증 단계, 어떤 실패도 치명적이지 않음
        _log(f"  경고: 포지션 캐시 조회 실패 ({exc})")
        return None
    if raw is None:
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(data, dict):
        positions = data.get("positions", [])
    elif isinstance(data, list):
        positions = data
    else:
        return None
    return sum(1 for p in positions if float(p.get("size") or 0) != 0)


async def main() -> int:
    triggered_by = os.getenv("USER", "cli")
    _log("=== 긴급 청산 시작 ===")
    _log(f"REDIS_URL={REDIS_URL}  triggered_by={triggered_by}")

    try:
        redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis.ping()
    except Exception as exc:  # noqa: BLE001 — 연결 실패는 즉시 중단해야 한다
        _log(f"치명적: Redis 연결 실패 — {exc}")
        _log("수동 조치 필요: 거래소 UI에서 직접 청산할 것")
        return 2

    before = await _open_position_count(redis)
    _log(f"청산 전 열린 포지션: {before if before is not None else '확인 불가'}")

    payload = json.dumps(
        {
            "level": 4,
            "trigger_reason": "manual_cli_emergency",
            "triggered_by": triggered_by,
            "timestamp": _now(),
        }
    )

    # 1) 신규 주문 차단 — 청산 명령보다 먼저 걸어야 경합이 없다
    await redis.set(KILL_SWITCH_ACTIVE_KEY, "true")
    _log("1/4 신규 주문 차단 완료 (ce:kill_switch:active=true)")

    # 2) 정규 킬스위치 채널 발행 (근본 수정 배포 후 오케스트레이터가 수신)
    await redis.publish(KILL_SWITCH_CHANNEL, payload)
    _log("2/4 ce:kill_switch 발행 완료")

    # 3) 전략에 stop 직접 발행 — 이것이 실제 청산을 일으킨다
    for strategy_id in STRATEGY_IDS:
        channel = f"strategy:command:{strategy_id}"
        cmd = json.dumps(
            {
                "strategy_id": strategy_id,
                "action": "stop",
                "allocated_capital": 0.0,
                "params": {"reason": "kill_switch"},
            }
        )
        receivers = await redis.publish(channel, cmd)
        _log(f"3/4 stop 명령 발행 → {channel} (수신자 {receivers}명)")
        if receivers == 0:
            _log("  ⚠️  수신자 0 — 전략 서비스가 죽어 있을 수 있다. 거래소 UI 직접 확인 필요")

    # 4) 청산 확인
    _log(f"4/4 청산 확인 중 (최대 {VERIFY_TIMEOUT_SECONDS}초)...")
    waited = 0
    while waited < VERIFY_TIMEOUT_SECONDS:
        await asyncio.sleep(VERIFY_POLL_SECONDS)
        waited += VERIFY_POLL_SECONDS
        count = await _open_position_count(redis)
        if count == 0:
            _log(f"✅ 청산 확인 — 열린 포지션 0 ({waited}초 경과)")
            await redis.aclose()
            return 0
        _log(f"  대기 중... 열린 포지션={count if count is not None else '확인 불가'} ({waited}s)")

    _log("⚠️  청산 미확인 — 타임아웃")
    _log("수동 확인 필수:")
    _log("  1) 대시보드 http://localhost:3000/supertrend")
    _log("  2) docker compose logs --tail=50 supertrend")
    _log("  3) 미청산 시 Bybit UI에서 직접 청산")
    _log("신규 주문은 차단된 상태로 유지된다 (ce:kill_switch:active=true)")
    await redis.aclose()
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
