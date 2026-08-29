---
title: BTC 단일 운영 정책 채택
adr_number: 001
status: Accepted
date: 2026-05-01
related_code:
  - cryptoengine/config/strategies/funding-arb.yaml
  - cryptoengine/config/strategies/adaptive-dca.yaml
  - cryptoengine/config/exchanges/bybit.yaml
  - cryptoengine/services/market-data/feature_engine.py
---

# ADR-001. BTC 단일 운영 정책 채택

## Status

Accepted (2026-05-01). **2026-08-29**: 아래 `related_code`의 funding-arb / adaptive-dca yaml·`feature_engine.py`는 저장소에서 삭제됨(ADR-0009). 정책 자체는 Supertrend `BTCUSDT` 단일로 **계속 유효**.

## Context

기존 funding-arb config는 5개 페어(BTC/ETH/SOL/BNB/XRP)를 활성화하고 있었습니다.

### 백테스트 결과의 문제점

- **백테스트 v3** (멀티심볼 펀딩 로테이션): 음수 결과
- **백테스트 v4** (BTC/ETH 페어): 음수 결과
- **백테스트 v5** (BTC 단일): CAGR +34.87%, Sharpe 3.583 (채택)

### 시장 환경 변화

**2023년 이후 알트코인 펀딩비 변화**:
- ETF 출시 이후 알트코인 펀딩비 "천장" 압축
- ETH 스테이킹 도입으로 원래 공적분(co-integration) 붕괴
- 멀티심볼 펀딩 로테이션의 알파 소멸

**BTCUSDT의 우월성**:
- 시장 깊이: 모든 심볼 중 최대
- 슬리피지: 최소 (시장 깊이 + 유동성)
- 펀딩비: 안정성 최고
- 데이터: 완전함 (2020년부터)

---

## Decision

**Multi-symbol 거래를 원칙적으로 금지합니다.**  
**모든 전략·백테스트·설정은 BTCUSDT만 사용합니다.**

### 명확한 규칙

1. **funding-arb 전략**: BTCUSDT만 허용
2. **adaptive-dca 전략**: BTCUSDT만 허용
3. **백테스트**: BTCUSDT만 (Jesse, 자체 엔진)
4. **향후 신규 전략**: BTCUSDT 제약 필수

---

## Consequences

### 긍정적 (Positive)

#### 1. 변동성/유동성 리스크 단순화

- 단일 심볼 → 모니터링 대상 축소
- Kill Switch 임계값 설정 및 검증 용이
- 시스템 복잡도 감소

#### 2. Bybit BTCUSDT 시장 깊이 활용

- 주문 슬리피지 최소화 (-0.05% 이상)
- 거래량이 많은 시간대 청산 수월
- 대량 포지션도 시장 영향 최소

#### 3. 모니터링 및 SOP 단순화

- Telegram 알림: 1개 심볼만 추적
- 수동 청산(Manual Close) SOP: 간단함
- 팀원 온보딩: 학습곡선 짧음

#### 4. 백테스트 신뢰도 향상

- 실데이터(BTCUSDT 펀딩비) 완전
- 합성 폴백(synthetic fallback) 제거
- 신뢰도 ↑

### 부정적 (Negative)

#### 1. 종목 분산 효과 상실

- 단일 심볼 → 시스템 리스크 집중
- 극단 시장 변동 시 회피 불가능
- (예: 2024년 비트코인만 급등, 알트는 정체)

#### 2. 진입 기회 제한

- ETH, SOL 음펀딩 환경 포기
- BTCUSDT 펀딩비 > 0 조건만 거래
- 거래 횟수 감소 (향후 1개월 데이터 기준)

#### 3. 장기 전략 역할 축소

- 기존: 멀티심볼 로테이션으로 항시 포지션
- 현재: 펀딩비 양수 조건에서만 거래
- 극저 펀딩비 환경에서 유휴 자본 증가

---

## Rejected Alternatives

### ❌ 대안 A: 5종 멀티심볼 유지

```
이유:
- 백테스트 v4 결과 음수 (CAGR -2.3%)
- ETF 이후 알트 펀딩비 천장 압축
- 변동성 증가로 마진 관리 복잡화
```

### ❌ 대안 B: BTC/ETH 페어 (2심볼)

```
이유:
- 백테스트 v4 결과 음수 (CAGR -0.8%)
- ETH 스테이킹 후 공적분 붕괴
- Sharpe 0.07 (매우 낮음)
```

### ❌ 대안 C: 음의 펀딩 양방향 (DOT/APT)

```
이유:
- 신뢰성 부족 (Bybit 상장 역사 짧음)
- 유동성 낮음 (슬리피지 > 0.2%)
- BTCUSDT 음펀딩 활용으로 대체 가능
```

### ❌ 대안 D: 멀티시그 시스템 (BTC + 1개 헤징)

```
이유:
- 구현 복잡도 과도 (거래소 차익거래)
- 데이터 동기화 어려움
- 점진적 축소(BTC→페어 제한)로 대체
```

---

## Enforcement

### 1. 설정 파일 (YAML)

#### funding-arb.yaml
```yaml
pairs: [BTCUSDT]  # 고정, 변경 금지
```

#### adaptive-dca.yaml
```yaml
pairs: [BTCUSDT]  # 고정, 변경 금지
```

### 2. 거래소 설정 (exchange)

#### bybit.yaml
```yaml
symbols:
  - BTCUSDT  # 활성화
# ETH, SOL, BNB, XRP: 정의 제거
```

### 3. 코드 검증 (Python)

#### services/market-data/feature_engine.py
```python
SYMBOLS = ["BTCUSDT"]  # 하드코딩
assert len(SYMBOLS) == 1, "Only BTCUSDT allowed"
```

#### services/strategies/funding-arb/main.py
```python
config = load_config('config/strategies/funding-arb.yaml')
assert config['pairs'] == ['BTCUSDT'], "Must be BTCUSDT only"
```

### 4. CI/CD 규칙 (선택)

```bash
# .github/workflows/validate.yml (향후)
- name: Validate symbol policy
  run: |
    grep -q "pairs: \[BTCUSDT\]" config/strategies/funding-arb.yaml
    grep -q "pairs: \[BTCUSDT\]" config/strategies/adaptive-dca.yaml
```

---

## 변경 방지 체크리스트

- [x] funding-arb.yaml pairs: [BTCUSDT] 확정
- [x] adaptive-dca.yaml pairs: [BTCUSDT] 확정
- [x] orchestrator.yaml에서 다른 심볼 제거
- [x] feature_engine.py SYMBOLS = ["BTCUSDT"]
- [x] Bybit exchange config에서 ETH/SOL/BNB/XRP 삭제
- [x] 도커 이미지 빌드 (검증 포함)

---

## References

- **백테스트 결과**: `/result/12.sta.html` (fa80_lev5_r30)
- **시장 분석**: 2023년 이후 알트 펀딩비 데이터
- **정책 문서**: `CLAUDE.md` → "BTC-only trading policy"
- **관련 회의 노트**: (내부 기록)

---

## 향후 재검토 조건

이 정책은 다음 중 하나 발생 시 재검토합니다:

1. **알트코인 펀딩비 회복** (평균 > 0.0005 지속 3개월)
2. **공적분 관계 복구** (ETH-BTC 상관도 > 0.95)
3. **백테스트 성과 저하** (6개월 CAGR < 10%)

재검토 시: **새 ADR 작성** (이 ADR을 Supersede)

---

**작성일**: 2026-05-01  
**최종 승인**: [사용자명]  
**시행일**: 2026-05-01

