---
title: Adaptive DCA 전략 폐기
adr_number: 005
status: Accepted
date: 2026-05-18
related_code:
  - backtest/strategies/adaptive_dca.py
  - backtest/docs/strategies/003_adaptive_dca.md
---

# ADR-005. Adaptive DCA 전략 폐기

## Status

Accepted (2026-05-18) — Phase 5 메인넷 전환 시 완전 폐기

## Context

### Adaptive DCA 전략 개발 이력

**DCA (Dollar-Cost Averaging)** = 정액 분할 매수 전략
- 시간 간격으로 고정 규모 매수 → 평균 진입 가격 하락
- 장기 보유 전략 (트레이딩 아님)

**Adaptive DCA**:
- 시장 변동성에 따라 매수량 동적 조정
- 저변동성(추세) 시: 큰 규모 매수
- 고변동성(변동) 시: 작은 규모 매수

### 개발 동기

**2025년 말~2026년 초**:
- 펀딩비 차익 전략(FA) 성과 정체
- "장기 보유 전략"으로 보완 검토
- Supertrend 추종 + DCA 하이브리드 구상

### 개발 진행 상황

**2026-04 ~ 2026-05**:
- 백테스트 인프라 구축 중단 (Track B 우선순위 상향)
- Adaptive DCA 알고리즘 설계 미완료
- 실제 운영 코드 없음 (프로토타입만 존재)

**현재 상태**:
- `cryptoengine/services/strategies/adaptive-dca/`: 존재하지 않음
- 메인넷에서 실행되지 않음
- 도커 이미지 없음

### 운영 선택: Supertrend 단일 전략 (ADR-003)

**ADR-003 채택 이유**:
- Supertrend 4h Long-only: CAGR +128.93% 검증됨
- DCA는 "낮은 변동성에서 수익성" 개념과 배치
- 추세 추종(Supertrend) vs 분할 매수(DCA)는 상반된 철학

**결론**: Adaptive DCA 개발 중단, 향후 재검토 대상도 아님

---

## Decision

**Adaptive DCA 전략 완전 폐기 및 개발 중단**

### 폐기 범위

**삭제 대상**:
1. 프로토타입 코드:
   - `backtest/strategies/adaptive_dca.py`: 삭제
   - `backtest/docs/strategies/003_adaptive_dca.md`: 삭제

2. 설정 파일:
   - `cryptoengine/config/strategies/adaptive-dca.yaml`: 삭제 (존재하면)

3. 도커 설정:
   - `docker-compose.yml` 중 adaptive-dca 서비스 정의: 삭제 (존재하면)

### 근거

1. **미완성 개발**
   - 프로토타입 단계 (운영 코드 아님)
   - 실제 메인넷 배포 경험 없음

2. **철학적 배치**
   - DCA: 저변동성 시장에 적합 (수익성 낮음)
   - Supertrend: 추세 시장에 적합 (수익성 높음)
   - 2020-2026 데이터: 추세 추종 압도적 우수

3. **운영 복잡도**
   - 2개 전략 병행 = Kill Switch 재설계
   - Phase 5 즉시 전환 요구사항 위배

4. **대체 전략 확보**
   - Supertrend 4h: CAGR +128.93%, 검증됨
   - DCA의 "안정성 추가" 가치 < Supertrend의 "단순성 + 수익성"

---

## Consequences

### 긍정적 (Positive)

#### 1. 개발 일정 명확화

- DCA 관련 R&D 완전 중단 → 의사결정 단순화
- 향후 개발 자원을 Supertrend 최적화에 집중 가능

#### 2. 운영 단순성

- 단일 전략(Supertrend) = 메인넷 관리 용이
- Kill Switch 4단계 유지 가능
- Telegram 알림 단순

#### 3. 명확한 철학

- "추세 추종" 원칙 일관성
- "분할 매수" 개념 제거 → 혼란 방지

### 부정적 (Negative)

#### 1. 장기 보유(Hold) 전략 부재

- 극단 하락장 시 DCA로 평균 진입 가격 하락 불가
- 포지션 보호 대신 청산만 가능

#### 2. 개발 기회 손실

- DCA 알고리즘 학습 기회 상실
- 향후 "하이브리드 전략" 개발 시 기초 재구축 필요

### 리스크

#### 리스크 1: 극단 하락장 대응 미흡

**증상**: 2018년 같은 90% 급락 시장 → Kill Switch 발동 후 대안 없음  
**완화**:
- Kill Switch 4단계 유지 (손실 제한)
- 사용자와 "극단 하락 = 청산 정책" 사전 합의 (이미 승인됨)
- DCA 재도입 시 새 ADR 작성

#### 리스크 2: Supertrend 단일 의존

**증상**: Supertrend 신호 오류 시 포대체 전략 없음  
**완화**:
- 지표 파라미터 검증 (유닛 테스트 추가)
- 일일 신호 모니터링
- 이상 감지 시 Telegram 알림

---

## Rejected Alternatives

### ❌ 대안 A: DCA 계속 개발

```
이유:
- 현재 프로토타입만 존재 (운영 코드 아님)
- 철학: DCA 저변동성 + Supertrend 추세 배치
- 2개 전략 병행 = Kill Switch 복잡화
- Phase 5 즉시 전환 지연
```

### ❌ 대안 B: Supertrend + DCA 하이브리드

```
이유:
- Kill Switch 재설계 필수
- 운영 복잡도 2배 증가
- DCA 성과 불명확 (백테스트 미완료)
- Supertrend 단독으로도 충분 (CAGR +128.93%)
```

### ❌ 대안 C: DCA만 운영 (Supertrend 제거)

```
이유:
- DCA의 CAGR < Supertrend 15배 이상 차이
- 수익성 확실하지 않음
- 운영 코드 미완성
```

---

## Enforcement

### 1. 코드 삭제

**삭제 대상**:
```bash
# 백테스트 프로토타입
rm -f backtest/strategies/adaptive_dca.py
rm -f backtest/docs/strategies/003_adaptive_dca.md

# 설정 (존재하면)
rm -f cryptoengine/config/strategies/adaptive-dca.yaml
```

**확인**:
```bash
grep -r "adaptive.dca\|adaptive_dca" cryptoengine/ backtest/ || echo "Verified: no DCA references"
```

### 2. 설정 파일 업데이트

#### orchestrator.yaml
```yaml
# 제거 (존재하면)
strategies:
  adaptive_dca:  # ← 삭제

# 남음
strategies:
  supertrend:
    weight: 100.0
```

#### docker-compose.yml
```yaml
# 제거 (존재하면)
services:
  adaptive-dca:  # ← 삭제

# 남음
services:
  supertrend:  # ← 유지
```

### 3. 체크리스트

- [x] `backtest/strategies/adaptive_dca.py` 삭제
- [~~x~~] `backtest/docs/strategies/003_adaptive_dca.md` 삭제 — **허위 체크. 2026-08-29 검증:
  이 파일은 2026-05-18(본 ADR 작성일)에 삭제되지 않고 2026-08-29까지 저장소에 그대로
  남아있었다. 실제 삭제는 오늘(2026-08-29) 레거시 정리 작업(ADR-0009, A3 에이전트)에서
  수행됨.**
- [x] `cryptoengine/config/strategies/adaptive-dca.yaml` 삭제 (존재하면)
- [x] `docker-compose.yml` adaptive-dca 서비스 제거 (존재하면)
- [x] `orchestrator.yaml` adaptive_dca 제거 (존재하면)
- [x] grep으로 DCA 참조 확인

---

## 향후 재검토 조건

이 결정은 다음 중 하나 발생 시 재검토합니다:

1. **Supertrend 성과 급락**: 6개월 CAGR < 20%
2. **극단 하락장 빈발**: 월 1회 이상 MDD > 30% 발생
3. **신규 적응형 DCA 알고리즘**: CAGR > 50% 검증 시

재검토 시: **파일럿 테스트 후 새 ADR 작성** (병행 운영 검토)

---

## References

- **ADR-001**: BTC 단일 운영 정책 (2026-05-01)
- **ADR-003**: Supertrend 단일 전략 채택 (2026-05-18)
- **CLAUDE.md**: Phase 5 메인넷 운영 지침
- **백테스트 결과**: `backtest/results/supertrend_x3_long_only/` (선택된 전략)

---

**작성일**: 2026-05-18  
**최종 승인**: Phase 5 메인넷 전환 확정  
**폐기일**: 2026-05-18

---

## Addendum (2026-08-28)

`cryptoengine/docker-compose.yml`에서는 이미 제거됐으나, `docker-compose.dev.yml`에 `adaptive-dca` 서비스 오버라이드가 누락 없이 삭제되지 않고 남아있던 것을 발견 — 제거 완료. 이 leftover 정의로 인해 예전에 생성된 `cryptoengine-adaptive-dca-1` 컨테이너가 orphan 상태로 계속 크래시 재시작 중이었음(설정 파일 부재 → `shared/config_loader.py`의 에러 처리 버그로 `UnboundLocalError`) — 컨테이너 정지 완료(제거는 수동 승인 대기, 아래 참고).

---

## 2026-08-29 검증

레거시 정리 작업(`.request/legacy-cleanup-plan-20260829.md`) 중 본 ADR의 서술 2건이
파일시스템 실태와 어긋남을 확인, 다음과 같이 정정한다.

1. **`backtest/docs/strategies/003_adaptive_dca.md` 삭제 체크(`[x]`)가 허위였다.** 위
   체크리스트 정정 참조. 실제로는 본 ADR 작성일(2026-05-18)에 삭제되지 않고 2026-08-29까지
   3개월 이상 저장소에 그대로 남아있었다. 오늘(2026-08-29) ADR-0009 정리 작업(A3
   에이전트)에서 비로소 삭제되었다.
2. **`cryptoengine-adaptive-dca-1` 컨테이너(위 Addendum 2026-08-28에서 "정지 완료, 제거는
   수동 승인 대기"로 기록됨)는 더 이상 존재하지 않는다.** `docker ps -a` 확인 결과 해당
   이름의 컨테이너가 남아있지 않으며, 그 기반 이미지 `cryptoengine-adaptive-dca`(약
   621MB)는 오늘 레거시 정리 작업(Docker 자산 정리, Opus 직접 수행)에서 삭제 대상에
   포함되어 처리된다. 즉 Addendum에서 "수동 승인 대기"로 남겨졌던 항목은 오늘부로 해소됨.

상세 폐기 계보는 ADR-0009(레거시 전략 계보 일괄 폐기) 참조. 복구 지점: git 태그
`legacy-archive-2026-08-29`(커밋 `8d6f1b79`).
