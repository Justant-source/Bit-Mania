---
title: Funding Arbitrage 전략 폐기
adr_number: 004
status: Accepted
supersedes: ADR-002
date: 2026-05-18
related_code:
  - backtest/strategies/multi_symbol_funding_arb.py
  - backtest/strategies/cross_exchange_fa.py
  - backtest/strategies/dynamic_threshold_fa.py
  - backtest/docs/strategies/001_funding_arb.md
  - backtest/scripts/data/fetch_multi_symbol.py
  - backtest/scripts/data/fetch_multi_exchange.py
---

# ADR-004. Funding Arbitrage 전략 폐기

## Status

Accepted (2026-05-18) — Phase 5 메인넷 전환 시 완전 폐기  
**Supersedes**: ADR-002 (Multi-symbol FA 도입 검토)

## Context

### FA 전략 개발 이력

**2026-04 ~ 2026-05**: Funding Arbitrage 백테스트 진행
- **Phase**: 다중 심볼 수익성 탐색 (Track B)
- **대상 심볼**: 6개 (BTC, ETH, SOL, BNB, DOGE, PEPE)
- **후보 전략**: 3가지
  1. Multi-symbol FA (B1): 6종 동시 거래
  2. Cross-exchange FA (B2): Bybit vs Binance 스프레드
  3. Dynamic Threshold FA (B3): 적응형 진입 임계값

### FA 전략의 기본 원리

**펀딩비 차익거래 (Funding Rate Arbitrage)**:
- 상품 선물 시장의 "펀딩비" = 롱-숏 포지션 간 이자 지급
- 청담 시장(contango): 숏 → 롱 지급 (수익 기회)
- 백완시(backwardation): 롱 → 숏 지급 (손실 회피)

**2023년 이후 문제점**:
1. **BTC 펀딩비 침식**: +0.0003 → +0.0001 (알파 소멸)
2. **ETH 구조 변화**: 스테이킹 도입 후 공적분 붕괴
3. **알트 유동성 부족**: 슬리피지 0.1-0.3% (거래 수수료 잠식)
4. **거래 빈도 증가**: 1200회/년 → 수수료 폭증 (1320 bps vs 6.6 bps BTC-only)

### Track B 백테스트 결과 (2026-05-17 완료)

**합격 기준** (ADR-002에서 정의):
- Multi-symbol FA: CAGR ≥ 20%, Sharpe ≥ 2.5, MDD ≤ -8%
- Cross-exchange FA: CAGR ≥ 15%, Sharpe ≥ 2.2, MDD ≤ -8%
- Dynamic Threshold FA: CAGR ≥ 25%, Sharpe ≥ 3.2, MDD ≤ -6%

**실제 결과**:

| 전략 | CAGR | Sharpe | MDD | 합격 |
|------|------|--------|------|------|
| B1 (Multi-symbol) | +8.3% | 1.1 | -12.5% | ❌ |
| B2 (Cross-exchange) | +11.2% | 1.4 | -10.8% | ❌ |
| B3 (Dynamic Threshold) | +14.5% | 1.9 | -9.2% | ❌ |

**분석**:
- **모든 후보 전략이 합격 기준 미충족**
- 2024-2026 시장 환경에서 펀딩비 알파 이미 소멸
- 데이터 부족 심볼(PEPE 2023년 이후만) 신뢰도 낮음
- 거래 수수료가 백테스트 초과 성능을 완전히 잠식

### 대체 전략 선택: Supertrend 4h Long-only

**ADR-003에서 채택**:
- CAGR +128.93% (FA 대비 15배)
- Sharpe 1.30 (안정성 충분)
- MDD -86.94% (극단 리스크, 사용자 수용)
- 단일 BTC, 단순한 운영

---

## Decision

**Funding Arbitrage 전략 완전 폐기**

### 폐기 범위

**삭제 대상**:
1. `cryptoengine/services/strategies/funding-arb/` (전체)
   - 현재는 이미 미운영 상태
   - 메인넷에서 실행되지 않음
   - 도커 이미지 제거

2. 백테스트 코드 (선택적 보존 — **2026-08-29 검증 결과 실제로는 삭제됨, 아래 정정 참조**):
   - `backtest/strategies/multi_symbol_funding_arb.py`: 보존 (역사 학습용)
   - `backtest/strategies/cross_exchange_fa.py`: 보존 (역사 학습용)
   - `backtest/strategies/dynamic_threshold_fa.py`: 보존 (역사 학습용)
   - `backtest/docs/strategies/001_funding_arb.md`: 보존 (역사 학습용)

3. 데이터 수집 스크립트:
   - `backtest/scripts/data/fetch_multi_symbol.py`: 삭제
   - `backtest/scripts/data/fetch_multi_exchange.py`: 삭제
   - `backtest/scripts/data/validate_data_quality.py`: 삭제

### 근거

1. **백테스트 성과 미충족**
   - 모든 후보 전략이 합격 기준 미달성
   - 2026년 현재 펀딩비 알파 소멸 추세 확인

2. **시장 환경 악화**
   - 2024-2026 평균 펀딩비 급락 (+0.0003 → +0.0001)
   - ETH 스테이킹 후 상관도 변화
   - 알트 유동성 부족 (슬리피지 과다)

3. **운영 복잡도 > 수익성**
   - 6종 심볼, 1200회/년 거래 = 모니터링 부담 과다
   - Kill Switch 6배 복잡화
   - 수수료 1320 bps > 백테스트 초과 이득

4. **대체 전략 확보**
   - Supertrend 4h Long-only: CAGR +128.93%
   - 단순한 운영, 명확한 진입/청산 신호
   - BTC-only (ADR-001) 정책 일치

---

## Consequences

### 긍정적 (Positive)

#### 1. 운영 복잡도 대폭 감소

- 6종 심볼 모니터링 → 단일 BTC만
- Telegram 알림 개수 1/6 축소
- Kill Switch 로직 단순화

#### 2. 수익성 극대화

- 거래 수수료 절감: 1320 bps → 6.6 bps (200배)
- CAGR +128.93% (FA 대비 15배)
- 현금 흐름 개선

#### 3. 시스템 신뢰도 향상

- 검증된 Supertrend 지표 사용
- 백테스트 6년 데이터 기반
- 실제 펀딩비 환경 반영 완료

#### 4. 개발 시간 절감

- FA 관련 버그 수정/모니터링 작업 중단
- R&D 자원 향후 전략 개발에 할당 가능

### 부정적 (Negative)

#### 1. 펀딩비 알파 포기

- 극저 펀딩비 환경(0.00005 이하)에서 대체 전략 없음
- 향후 펀딩비 회복 시 선택지 제한

#### 2. FA 지식 손실

- 코드 폐기 시 구현 세부 사항 추적 곤란
- 재도입 시 처음부터 재개발 필요

#### 3. 다각화 기회 상실

- 단일 전략(Supertrend) 의존도 증가
- 지표 오류/버그 시 포트폴리오 전체 타격

### 리스크

#### 리스크 1: 향후 펀딩비 회복

**증상**: 2027년 펀딩비 평균 > 0.0003 회복  
**완화**:
- 월별 펀딩비 추이 모니터링
- 펀딩비 > 0.0002 지속 3개월 시 ADR 재검토
- 새 FA 전략 R&D 신청 (별도 프로젝트)

#### 리스크 2: Supertrend 신호 신뢰도 저하

**증상**: 위신호율 > 30% (이전 CAGR 기준 대비)  
**완화**:
- 월별 신호 정확도 모니터링
- 이상 감지 시 Telegram 알림
- 지표 파라미터 조정 검토 (새 ADR)

---

## Rejected Alternatives

### ❌ 대안 A: FA 계속 개발 (Threshold 하향)

```
이유:
- 합격 기준 자체가 시장 현실 반영하여 설정됨
- 기준을 낮추면 미래 수익성 보장 불가
- 운영 복잡도는 여전함
```

### ❌ 대안 B: FA + Supertrend 병행 (포트폴리오)

```
이유:
- Kill Switch 재설계 필요 (2개 전략)
- Phase 5 즉시 전환 요구사항 위배
- FA 수익성 불확실, Supertrend 검증됨 → 선택지 명확
```

### ❌ 대안 C: 특정 심볼만 FA (예: ETH)

```
이유:
- 백테스트 결과가 6종 조합 기준
- 단일 심볼 FA의 변동성/유동성 악화 예상
- BTC-only 정책(ADR-001) 위배
```

---

## Enforcement

### 1. 코드 삭제

**삭제 대상**:
```bash
# 운영 중지
rm -rf cryptoengine/services/strategies/funding-arb/
rm -f cryptoengine/config/strategies/funding-arb.yaml

# 데이터 수집 스크립트 삭제
rm -f backtest/scripts/data/fetch_multi_symbol.py
rm -f backtest/scripts/data/fetch_multi_exchange.py
rm -f backtest/scripts/data/validate_data_quality.py
```

**보존 대상** (역사 학습):
```
backtest/strategies/multi_symbol_funding_arb.py
backtest/strategies/cross_exchange_fa.py
backtest/strategies/dynamic_threshold_fa.py
backtest/docs/strategies/001_funding_arb.md
```

### 2. 설정 파일 업데이트

#### orchestrator.yaml
```yaml
# 제거
strategies:
  funding_arb:  # ← 삭제

# 남음
strategies:
  supertrend:
    weight: 100.0
```

#### docker-compose.yml
```yaml
# 제거
services:
  funding-arb:  # ← 삭제

# 남음
services:
  supertrend:  # ← 유지
```

### 3. 문서 업데이트

#### ADR-002 (본 ADR 의해 Supersede)

```yaml
# 원본 ADR-002 frontmatter 수정
status: Superseded by ADR-004  # (이전: PROPOSED)
```

### 4. 체크리스트

- [x] `cryptoengine/services/strategies/funding-arb/` 삭제
- [x] `cryptoengine/config/strategies/funding-arb.yaml` 삭제
- [x] `backtest/scripts/data/fetch_*` 3개 파일 삭제
- [x] `orchestrator.yaml` funding_arb 제거
- [x] `docker-compose.yml` funding-arb 서비스 제거
- [x] ADR-002 상태 변경 (PROPOSED → Superseded by ADR-004)
- [x] Telegram 핸들러의 funding_arb 예시 제거 (→ supertrend)
- [~~x~~] `backtest/docs/strategies/001_funding_arb.md` 삭제 — **허위 체크. 2026-08-29 검증:
  이 파일은 2026-05-18(본 ADR 작성일)에 삭제되지 않고 2026-08-29까지 저장소에 그대로
  남아있었다. 실제 삭제는 오늘(2026-08-29) 레거시 정리 작업(ADR-0009, A3 에이전트)에서
  수행됨.**

---

## 향후 재검토 조건

이 결정은 다음 중 하나 발생 시 재검토합니다:

1. **펀딩비 회복**: 평균 > 0.0002 지속 3개월
2. **Supertrend 성과 급락**: CAGR < 20% 지속 6개월
3. **신규 FA 알고리즘 발견**: 기존 대비 +10%p 이상 개선 제안

재검토 시: **새 ADR 작성** (파일럿 테스트 후 채택)

---

## References

- **ADR-001**: BTC 단일 운영 정책 (2026-05-01)
- **ADR-002**: Multi-symbol FA 도입 검토 (2026-05-02, 본 ADR 의해 Supersede)
- **ADR-003**: Supertrend 단일 전략 채택 (2026-05-18)
- **CLAUDE.md**: Phase 5 메인넷 운영 지침
- **Track B 결과**: `backtest/results/` (B1/B2/B3 폴더)
- **펀딩비 데이터**: Coinalyze 6년 역사 (2020~2026)

---

**작성일**: 2026-05-18  
**최종 승인**: Phase 5 메인넷 전환 확정  
**폐기일**: 2026-05-18

---

## 2026-08-29 검증

레거시 정리 작업(`.request/legacy-cleanup-plan-20260829.md`) 중 본 ADR의 "Enforcement" 절
서술 2건이 파일시스템 실태와 어긋남을 확인, 다음과 같이 정정한다.

1. **"보존 대상 (역사 학습)" 절이 허위였다.** `backtest/strategies/multi_symbol_funding_arb.py`,
   `cross_exchange_fa.py`, `dynamic_threshold_fa.py`는 "보존"으로 기록되었으나, `git log`
   확인 결과 이미 커밋 `3da55859`("chore(backtest): scripts 대규모 정리 + jesse_engine
   이동 + 레거시 결과 삭제")에서 삭제되어 2026-08-29 시점 저장소에 존재하지 않는다.
   정정: 위 3개 파일은 **삭제됨** (보존되지 않음).
2. **`backtest/docs/strategies/001_funding_arb.md` 삭제 체크(`[x]`)가 허위였다.** 실제로는
   본 ADR 작성일(2026-05-18)에 삭제되지 않고 2026-08-29까지 3개월 이상 저장소에 그대로
   남아있었다. 오늘(2026-08-29) ADR-0009 정리 작업(A3 에이전트)에서 비로소 삭제되었다.

두 항목 모두 결과적으로는 "최종적으로 삭제됨"에 수렴하지만, 본 ADR이 주장한 시점·상태
(2026-05-18에 이미 완료)는 사실이 아니었다. 상세 폐기 계보는 ADR-0009(레거시 전략 계보
일괄 폐기) 참조. 복구 지점: git 태그 `legacy-archive-2026-08-29`(커밋 `8d6f1b79`).
