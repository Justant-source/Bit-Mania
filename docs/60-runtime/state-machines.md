---
title: L6 Runtime — 상태 전이 · service_shutdown 복구
last_updated: 2026-08-29
---

# L6 Runtime — 상태 전이 · service_shutdown 복구

> 주문 상태, Kill Switch 레벨, 서비스 복구 흐름의 완전한 상태 다이어그램 및 전이 규칙.

---

## §1. OrderState 내부 (Diagram K1)

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/services/execution/order_manager.py:36 -->

```mermaid
stateDiagram-v2
    direction LR
    [*] --> pending : 주문 생성
    pending --> submitted : Bybit API 제출 성공
    submitted --> partially_filled : 부분 체결
    submitted --> filled : 완전 체결
    submitted --> cancelled : 취소
    submitted --> rejected : 거부
    partially_filled --> filled : 잔량 체결
    partially_filled --> cancelled : 취소
    filled --> [*]
    cancelled --> [*]
    rejected --> [*]

    note right of rejected
        exit 거부 시 60초 후 1회 자동 재시도
        entry 거부 시 재시도 없음 (진입 스킵)
    end note
```

### 내부 `OrderState` vs 외부 `OrderResult.status` 매핑

| 내부 OrderState | 외부 OrderResult.status | 비고 |
|---|---|---|
| pending | — | 내부만 (Bybit 미인식) |
| submitted | new | Bybit 접수 확인 |
| partially_filled | partially_filled | 부분 체결 |
| filled | filled | 완전 체결 |
| cancelled | cancelled | 취소 |
| rejected | rejected | 거부 |
| — | expired | Bybit 만료 (내부 enum 없음) |

---

## §2. KillLevel 상태 전이 (Diagram K2)

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/shared/kill_switch.py:42, cryptoengine/config/orchestrator.yaml, cryptoengine/services/orchestrator/core.py -->

```mermaid
stateDiagram-v2
    direction LR
    [*] --> NONE : 시스템 시작
    NONE --> STRATEGY : check_strategy()<br/>전략 손실 > 임계값
    NONE --> PORTFOLIO : check()<br/>일일/주간/월간 손실 초과 (AND)
    NONE --> SYSTEM : system_healthy=False<br/>Dead Man's Switch
    NONE --> MANUAL : Telegram /emergency_close<br/>또는 ce:kill_switch 외부 발행
    STRATEGY --> NONE : cooldown 60분 후 auto_resume()
    PORTFOLIO --> NONE : cooldown 60분 후 auto_resume()
    SYSTEM --> NONE : reset_manual() 수동 호출
    MANUAL --> NONE : reset_manual() 수동 호출

    note right of MANUAL
        Level 4(MANUAL)는 auto_resume 불가
        reset_manual() 또는 오케스트레이터 프로세스 재생성
        + ce:kill_switch:active 키 삭제
    end note
```

### Phase 5 임계값 (cryptoengine/config/orchestrator.yaml §phase5)

| 주기 | 퍼센트 | 절대값 | 발동 조건 |
|---|---|---|---|
| 일일 | -5% | -$10 | AND (둘 다 초과) |
| 주간 | -10% | -$20 | AND |
| 월간 | -15% | -$30 | AND |
| cooldown | — | — | 60분 |

**ACK 프로토콜**: `ACK_TIMEOUT_SECONDS=5`, `ACK_MAX_RETRIES=3`

---

## §3. service_shutdown 복구 흐름 (Diagram K3)

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/services/strategies/supertrend/strategy.py:73, cryptoengine/services/execution/engine.py:108, cryptoengine/services/execution/stoploss_manager.py:248 -->

```mermaid
stateDiagram-v2
    direction TB
    [*] --> running : 정상 운영 중

    state "재시작 (1시간 내)" as restart {
        shutdown : on_stop(reason="service_shutdown")<br/>청산 없이 Redis에 상태 저장
        recover : recover_stop_losses(open_positions)<br/>recovery_resync()
        resume : 포지션 복구 완료
        shutdown --> recover
        recover --> resume
    }

    running --> shutdown : service_shutdown 신호
    resume --> running : 전략 재개

    state "재시작 실패 (1시간 초과)" as fail {
        dead_man : Dead Man's Switch 발동<br/>Kill Switch SYSTEM 레벨
    }
    shutdown --> dead_man : 1시간 초과 미재시작
```

### 복구 핵심 코드

- **청산 제외 사유**: `_SHUTDOWN_NO_LIQUIDATE = frozenset({"service_shutdown"})` — 이 사유 시 청산 없음
- **포지션 상태**: `positions.close_reason` — `signal | stop_loss | kill_switch` (service_shutdown 없음 = 청산 미발생)
- **복구 경로**: `on_stop()` → Redis `strategy:saved_state:supertrend-01` (TTL 3600s) → `recover_stop_losses()` → `recovery_resync()`

**Dead Man's Switch와 혼동하지 말 것**: EE 하트비트 미수신 **5분** → KillLevel SYSTEM. Redis 포지션 스냅샷 TTL **1시간**은 배포 복구용이다. Postgres만 잠시 내려도 EE 프로세스가 살아 있으면 dead-man은 발동하지 않는다 (D1 tar 시 EE 유지).

---

## §4. 외부 킬스위치 수신 (2026-08-29)

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/services/orchestrator/core.py -->

2026-08-29 이전: `ce:kill_switch` **구독자 0**. Telegram이 발행해도 `ce:kill_switch:active`만 서고 포지션은 남음. ACK 타임아웃. `make emergency`는 없는 모듈로 즉시 실패.

현재: `StrategyOrchestrator._listen_external_kill()`이 채널을 구독한다 (`PUBSUB NUMSUB ce:kill_switch` ≥ 1). 페이로드 JSON `trigger_reason` / `triggered_by` → `KillSwitch.trigger_manual()` → 기존 `_on_kill_switch_trigger` → 전략 `stop(reason=kill_switch)`. **`shared/kill_switch.py`는 수정하지 않음.**

`make -C cryptoengine emergency`는 `emergency_close_all.py`를 EE stdin으로 주입: active 키 SET + 채널 PUBLISH + 전략 stop **직접** 발행(오케스트레이터 다운 시 최후 수단). 청산 확인 후에만 ST/orch `stop`.

플랫 북 검증(2026-08-29 18:35 KST): 테스트 페이로드 발행 → `kill_switch_triggered` 로그 → `DEL`/`SET 0` active → ST 먼저 재생성 후 orch. 이후 `is_running=true`.

---

## §5. 전략 `start` 레이스 (`tick_interval` 60s)

<!-- last-verified: 2026-08-29 -->
<!-- code-ref: cryptoengine/services/strategies/base_strategy.py, cryptoengine/config/strategies/supertrend.yaml -->

메인 루프는 커맨드를 짧게 drain한 뒤 `asyncio.sleep(tick_interval)`(60초)한다. 오케스트레이터가 구독 전에 `start`를 발행하면 메시지는 Redis pub/sub 버퍼에 남고, **다음 틱에서** `command_received`가 난다. 대시보드 `is_running: false`가 1분 미만 지속될 수 있다.

배포 시 **supertrend를 strategy-orchestrator보다 먼저** 올린다. 상세: [ADR-0010](../shared/90-adr/0010-ops-cleanup-20260829.md).

---
