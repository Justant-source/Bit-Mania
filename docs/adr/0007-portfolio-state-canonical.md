---
title: PortfolioState Canonical Source — shared/models/position.py
last_updated: 2026-08-29
adr_number: "0007"
status: Proposed
date: 2026-06-15
related_code:
  - cryptoengine/shared/models/position.py
  - cryptoengine/services/orchestrator/portfolio_monitor.py
---

# ADR-0007. PortfolioState 정본 — shared/models/position.py

## Status

Proposed (2026-06-15). **2026-08-29 운영 창에서 구현하지 않음** — 포트폴리오 캐시는 여전히 orchestrator `portfolio_monitor` + Redis `cache:wallet_balance`.

## Context

`PortfolioState` 클래스가 두 곳에 정의돼 있다.

| 위치 | 라인 | 비고 |
|---|---|---|
| `cryptoengine/shared/models/position.py:44` | ~44 | **정본 후보** — shared/ 공용 모듈 |
| `cryptoengine/services/orchestrator/portfolio_monitor.py:29` | ~29 | 중복 정의 |

두 정의가 동기화된 보장이 없고, 서비스 코드가 어느 쪽을 import하는지 일관성이 없다. `shared/` 모듈은 전 서비스에서 공유되는 목적을 가지며, 서비스-로컬 중복 정의는 SSOT를 위반한다.

## Decision

`cryptoengine/shared/models/position.py`의 `PortfolioState`를 **유일한 정본**으로 채택한다.

- `services/orchestrator/portfolio_monitor.py`의 중복 정의를 제거하고 `from shared.models.position import PortfolioState`로 교체한다.
- `PortfolioState`를 참조하는 다른 모든 서비스는 `shared.models.position`에서 import한다.
- `shared/` 변경 시 전 서비스 이미지 재빌드(불변 규칙 §5)를 준수한다.

본 ADR은 **결정만 기록**한다. 코드 수정은 후속 PR에서 수행한다.

## Consequences

**긍정**:
- 단일 정의 → 필드 추가 시 한 곳만 수정
- `shared/` 소유권 명확화 (모든 서비스가 동일 타입 사용)

**부정/위험**:
- `portfolio_monitor.py`의 로컬 중복 정의에 의존하는 코드가 있다면 import 경로 수정 필요
- `shared/` 변경 → 전 서비스 이미지 재빌드 트리거 (불변 규칙, 허용됨)

**후속 작업**: `fix/portfolio-state-dedup` PR에서 코드 변경 수행.
