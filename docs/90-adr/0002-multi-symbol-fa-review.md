---
title: Multi-symbol Funding Arbitrage 도입 검토
adr_number: "0002"
status: Superseded by ADR-004
date: 2026-05-02
superseded_by: 004
allow_missing_refs: true
related_code:
  - backtest/strategies/multi_symbol_funding_arb.py
  - backtest/strategies/cross_exchange_fa.py
  - backtest/strategies/dynamic_threshold_fa.py
  - backtest/scripts/data/fetch_multi_symbol.py
  - backtest/scripts/data/fetch_multi_exchange.py
---

# ADR-0002. Multi-symbol Funding Arbitrage 도입 검토

## Status

Superseded by ADR-004 (2026-05-18) — 백테스트 Track B 결과 합격 기준 미충족으로 완전 폐기.  
**2026-08-29**: `related_code` 경로의 전략·fetch 스크립트는 ADR-0009로 저장소에서 삭제. 복구는 태그 `legacy-archive-2026-08-29` (`2ee11756`). 히스토리 재작성(ADR-0010 D9)으로 구 커밋 해시 `3da55859`는 `6f33efba`.

## Context

### 현재 정책 (ADR-001: BTC 단일 운영)

2026-05-01 채택됨:
- BTC-only 운영 원칙
- 6개월 Phase 4 포워드 테스트 (fa80_lev5_r30): CAGR +34.87%, Sharpe 3.583
- Multi-symbol 거래 금지

### 새로운 증거

**2025년 말-2026년 초 시장 압축장 분석**:
- BTC 펀딩비: +0.0003 → +0.0001 (알파 소멸, CAGR +1.64%)
- ETH 펀딩비: +0.0002 → -0.0001 (변동성 ↑)
- SOL 펀딩비: +0.0004 → +0.0002 (여전히 양수)
- DOGE 펀딩비: +0.0006 → +0.0003 (변동성 높음)
- PEPE 펀딩비: +0.0008 → +0.0004 (2023년 이후 데이터만 존재)

**알트코인 유동성 개선**:
- Bybit ETH 주문장 깊이: 2024년 $2M → 2026년 $8M+
- SOL 거래량: 2024년 일평균 $500M → 2026년 $1.2B+
- 슬리피지 개선: 최대 포지션 크기 확대 가능

### 제안의 배경

Track B 백테스트: 세 가지 전략 검증
1. **Multi-symbol FA** (B1): 6종 동시 거래
2. **Cross-exchange FA** (B2): Bybit vs Binance/OKEx 스프레드
3. **Dynamic Threshold FA** (B3): 적응형 진입 임계값

---

## Decision

**조건부 정책 전환 검토 시작**

BTC-only 정책을 유지하되, 아래 조건을 **모두 충족**하면 메인넷 Phase 5에서 Multi-symbol 도입을 승인한다.

### 합격 기준 (6년 백테스트: 2020-01-01 ~ 2026-04-30)

#### 1. Multi-symbol FA (B1)

- **CAGR** ≥ 20% (BTC 단독 +13.11% 대비 +7%p)
- **Sharpe ratio** ≥ 2.5
- **MDD** ≤ -8%
- **연간 거래수** ≥ 200건 (6개 심볼 전체)
- **6개 심볼 중 최소 4종** 양수 기여
- **승률** ≥ 60%
- **2024-2026 수익** (압축장) ≥ +5%

#### 2. Cross-exchange FA (B2)

- **CAGR** ≥ 15% (단일거래소 대비 스프레드 이득)
- **Sharpe ratio** ≥ 2.2
- **MDD** ≤ -8%
- **2024-2026 수익** ≥ +3%

#### 3. Dynamic Threshold FA (B3)

- **CAGR** ≥ 25% (정적 8% 임계값 대비 +7%p)
- **Sharpe ratio** ≥ 3.2
- **MDD** ≤ -6%
- **2024-2026 수익** ≥ +8%

### 선택 로직 (메인넷 전환 시)

합격 기준 충족 결과에 따라:

```
IF (B1 pass) AND (B2 pass) AND (B3 pass):
    → Multi-symbol FA 채택 (가장 안정적)
ELIF (B3 pass) AND (B1 OR B2 pass):
    → Dynamic Threshold FA + (B1 OR B2) 하이브리드
ELIF (B1 pass):
    → Multi-symbol FA만 채택
ELIF (B3 pass):
    → Dynamic Threshold FA만 채택
ELSE:
    → BTC-only (ADR-001 유지)
```

---

## Consequences

### 긍정적 (Multi-symbol 도입 시)

#### 1. 알파 확대

- BTC 단독 + 알트 분산 = 진입 기회 증가
- 압축장에서도 음펀딩 심볼(ETH)로 회피 가능
- 펀딩비 변동성 높은 심볼(DOGE, PEPE) 활용

#### 2. 리스크 분산

- 단일 심볼 청산 위험 ↓
- 상관도 낮은 심볼 조합 (BTC vs alt)
- 극단 시장 변동 시 회피 경로 확대

#### 3. 자본 효율

- 동시 6개 포지션으로 자본 회전율 ↑
- 휴지 자본 감소
- 년간 거래 빈도 2배 이상

### 부정적 (도입 시 리스크)

#### 1. 데이터 품질 차이

- PEPE: 2023년 이후만 데이터 (백테스트 신뢰도 ↓)
- ETH 스테이킹 후 펀딩 구조 변경 (2023년 단절)
- Bybit 상장 역사 짧은 심볼 (DOGE: 2021년, SOL: 2021년)

#### 2. 운영 복잡도 증가

- Kill Switch 임계값 6배 조정 필요
- Telegram 알림: 6개 심볼 추적
- 수동 청산 SOP 복잡화
- 팀원 온보딩 난이도 ↑

#### 3. 유동성 리스크

- 알트 심볼 유동성 < BTC (슬리피지 0.1-0.3%)
- 극단 시장 시 청산 어려움 가능
- Bybit 테마 거래(theme trading) 영향 받을 수 있음

#### 4. 거래 수수료 증가

- BTC-only: ~6 거래/년 × 2 leg × 0.055% = 6.6 bps
- 6-symbol: ~1200 거래/년 × 2 leg × 0.055% = 1320 bps (약 200배)
- 백테스트 우수 성과도 실운영에서 침식 가능

---

## Rejected Alternatives

### ❌ 대안 A: 즉시 Multi-symbol 도입

```
이유:
- 2024-2025 시장 데이터 부족 (PEPE는 2023년 이후만)
- 운영 복잡도 검증 없음
- Kill Switch 재설계 필수
```

### ❌ 대안 B: BTC-only 유지 (변경 없음)

```
이유:
- 2025년 말 펀딩비 알파 소멸 (CAGR +1.64%)
- 수익성 악화 추세 미대응
- 향후 6개월 Phase 5 진입 지체
```

---

## Enforcement

### Track B 백테스트 일정

| 단계 | 작업 | 기한 |
|------|------|------|
| 1 | 데이터 수집 (6년, 6 심볼) | 2026-05-10 |
| 2 | B1 Multi-symbol FA 백테스트 | 2026-05-15 |
| 3 | B2 Cross-exchange FA 백테스트 | 2026-05-20 |
| 4 | B3 Dynamic Threshold FA 백테스트 | 2026-05-25 |
| 5 | 결과 분석 + 최종 결정 | 2026-05-30 |

### 코드 구현

**新규 전략 파일** (이 ADR 승인 시):
```
backtest/strategies/
  - multi_symbol_funding_arb.py
  - cross_exchange_fa.py
  - dynamic_threshold_fa.py
```

**데이터 수집 스크립트**:
```
backtest/scripts/data/
  - coinalyze_client.py (API client)
  - fetch_multi_symbol.py (6 심볼 데이터)
  - fetch_multi_exchange.py (3 거래소 데이터)
  - validate_data_quality.py (품질 검증)
```

### 승인 체크리스트

- [ ] Track B 모든 백테스트 완료
- [ ] 합격 기준 2개 이상 전략 통과
- [ ] Kill Switch 재설계 검증
- [ ] Telegram 알림 설정 검증
- [ ] 메인넷 전환 SOP 작성 (멀티심볼 버전)

---

## References

- **ADR-001**: BTC 단일 운영 정책 (2026-05-01)
- **CLAUDE.md**: Track B 백테스트 인프라 명시
- **시장 데이터**: Coinalyze 6년 역사 펀딩비
- **기존 백테스트**: fa80_lev5_r30 (Phase 4 결과)

---

## 향후 재검토 조건

이 제안이 백테스트에서 합격 기준을 충족하지 못할 경우:

1. **BTC-only 정책 강화** (ADR-001 재확인)
2. **개별 심볼 분석**: 음펀딩 심볼(ETH) 단독 검토
3. **Dynamic Threshold FA만 도입**: 정적 임계값 개선 대체안

---

**작성일**: 2026-05-02  
**상태**: PROPOSED (백테스트 대기)  
**다음 단계**: Track B 완료 후 2026-05-30 최종 결정

---

## 2026-08-29 검증

레거시 정리 작업(`.request/legacy-cleanup-plan-20260829.md`) 중 본 ADR을 재검토, 다음을
확인한다.

1. **Superseded 상태는 정확하다.** 상단 frontmatter/Status의 "Superseded by ADR-004
   (2026-05-18)"는 ADR-0004("Supersedes: ADR-002")와 상호 일치하며, Track B 백테스트가
   합격 기준을 전부 미충족했다는 서술도 ADR-0004의 실제 결과 표와 부합한다. 정정 불필요.
2. **frontmatter `related_code`의 5개 경로가 전부 더 이상 존재하지 않는다.** 확인 결과:
   - `backtest/strategies/multi_symbol_funding_arb.py` — 삭제됨 (커밋 `6f33efba`
     "chore(backtest): scripts 대규모 정리 + jesse_engine 이동 + 레거시 결과 삭제"에서
     이미 제거, 오늘 이전)
   - `backtest/strategies/cross_exchange_fa.py` — 동일 커밋에서 삭제됨
   - `backtest/strategies/dynamic_threshold_fa.py` — 동일 커밋에서 삭제됨
   - `backtest/scripts/data/fetch_multi_symbol.py` — 삭제됨
   - `backtest/scripts/data/fetch_multi_exchange.py` — 삭제됨

   Track B(멀티심볼/교차거래소/동적임계값 FA)가 합격 기준을 충족하지 못해 폐기된
   ADR-0004의 결정에 따라 예정대로 삭제된 것이며, 문서 서술과 모순되지 않는다. 다만
   frontmatter가 가리키는 파일이 실재하지 않으므로, 이 절이 향후 링크·경로 검증
   도구에서 걸릴 수 있음을 남겨둔다 — `related_code`는 역사적 참조로만 유효하며 현재
   코드베이스에는 해당 경로가 없다. 상세 폐기 계보는 ADR-0009 참조. 복구 지점: git 태그
   `legacy-archive-2026-08-29`(커밋 `2ee11756`).

