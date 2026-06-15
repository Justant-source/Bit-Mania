---
title: L6 Runtime — 상태 전이 · service_shutdown 복구
last_updated: 2026-06-15
---

# L6 Runtime — 상태 전이 · service_shutdown 복구

> 주문 상태, Kill Switch 레벨, 서비스 복구 흐름의 완전한 상태 다이어그램 및 전이 규칙.

---

## §1. OrderState 내부 (Diagram K1)

<!-- last-verified: 2026-06-15 -->
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

<!-- last-verified: 2026-06-15 -->
<!-- code-ref: cryptoengine/shared/kill_switch.py:42, cryptoengine/config/orchestrator.yaml -->

```mermaid
stateDiagram-v2
    direction LR
    [*] --> NONE : 시스템 시작
    NONE --> STRATEGY : check_strategy()<br/>전략 손실 > 임계값
    NONE --> PORTFOLIO : check()<br/>일일/주간/월간 손실 초과 (AND)
    NONE --> SYSTEM : system_healthy=False<br/>Dead Man's Switch
    NONE --> MANUAL : /kill Telegram 명령
    STRATEGY --> NONE : cooldown 60분 후 auto_resume()
    PORTFOLIO --> NONE : cooldown 60분 후 auto_resume()
    SYSTEM --> NONE : reset_manual() 수동 호출
    MANUAL --> NONE : reset_manual() 수동 호출

    note right of MANUAL
        Level 4는 auto_resume 불가
        운영자 명시적 /reset 필요
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

<!-- last-verified: 2026-06-15 -->
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

---
