# FA Strategy: Jesse vs Self-Engine 성과 비교

## 개요

본 문서는 Jesse 프레임워크 기반 FA(Funding Arbitrage) 백테스트 결과와 기존 자체 엔진 결과를 비교합니다.

**평가 기간**: 2020-04-01 ~ 2026-03-31 (6년)  
**전략 설정**: fa80_lev5_r30 (자본비율 80%, 레버리지 5x, 재투자 30%)

---

## ⚠️ 중요 경고: 데이터 갭

### 자체 엔진 6년 백테스트의 신뢰성 문제

자체 엔진의 6년 결과 (CAGR +34.87%, Sharpe 3.583, MDD -4.52%)는 **데이터 갭으로 인해 과장되었을 가능성이 높습니다**:

| 기간 | 데이터 출처 | 신뢰성 |
|------|----------|--------|
| 2020-04 ~ 2023-03 (36개월) | **합성 폴백** (고정 0.0001) | ❌ 극도로 낙관적 |
| 2023-04 ~ 2026-03 (36개월) | **실제 Bybit** | ✅ 검증됨 |

**문제점**:
- Bybit은 2023년 4월 이전의 펀딩비 데이터를 제공하지 않음
- 기존 백테스트 엔진이 2020-2022 구간을 상수 0.0001로 대체
- 실제 2020-2022년 비트코인 약세장에서 펀딩비는 음수 또는 훨씬 낮았음
- 결과: 6년 평균 성과가 부풀려짐

### Jesse 재시뮬레이션의 목표

Jesse 기반 결과는 다음을 확인합니다:
1. **2023-2026 실데이터만** 기준으로 성과 재계산 (3년 CAGR 연율화)
2. **실제 마이너스 펀딩비** 환경에서의 전략 강건성
3. **자체 엔진과의 차이점** 도출 (로직 검증)

---

## 성과 비교 테이블

### 전체 6년 (2020-04 ~ 2026-03)

| 지표 | 자체 엔진 | Jesse | 차이 | 신뢰성 |
|------|----------|-------|------|--------|
| **CAGR** | +34.87% | [PENDING: Jesse backtest result] | - | ⚠️ 자체 엔진 위험 |
| **Sharpe 비율** | 3.583 | [PENDING: Jesse backtest result] | - | ⚠️ 위험 |
| **최대낙폭 (MDD)** | -4.52% | [PENDING: Jesse backtest result] | - | ⚠️ 위험 |
| **거래 수** | ~950회 | [PENDING: Jesse backtest result] | - | - |
| **청산 발생** | 0회 | [PENDING: Jesse backtest result] | - | - |
| **최종 자산** | ~$35,487 | [PENDING: Jesse backtest result] | - | - |

### 3년 (2023-04 ~ 2026-03) — 실데이터만

| 지표 | 자체 엔진 | Jesse | 차이 | 신뢰성 |
|------|----------|-------|------|--------|
| **CAGR (3yr 연율)** | [PENDING: 자체 엔진 재계산] | [PENDING: Jesse backtest result] | - | ✅ 높음 |
| **Sharpe 비율** | [PENDING: 재계산] | [PENDING: Jesse backtest result] | - | ✅ 높음 |
| **최대낙폭 (MDD)** | [PENDING: 재계산] | [PENDING: Jesse backtest result] | - | ✅ 높음 |
| **거래 수** | [PENDING] | [PENDING: Jesse backtest result] | - | - |

---

## 예상 결과 시나리오

### 시나리오 A: Jesse ≈ 자체 엔진 (로직 동등)

```
만약 Jesse CAGR ≈ +34% (6년, 2023-2026 실데이터 추정)

→ 해석:
  1. 자체 엔진 로직이 정확함
  2. 2023-2026 구간에서도 높은 성과 유지
  3. 기존 파라미터 (fa80_lev5_r30) 채택 가능
  
→ 결론: 준비 완료, 메인넷 진입 검토
```

### 시나리오 B: Jesse < 자체 엔진 (데이터 차이)

```
만약 Jesse CAGR ≈ +15~20% (6년)

→ 해석:
  1. 2020-2022 합성 폴백이 +34.87%의 절반 정도 기여
  2. Jesse는 실제 펀딩비 환경을 반영
  3. 기대값 조정 필요
  
→ 결론: 보수 설정 (fa80_lev4_r30) 권장
```

### 시나리오 C: Jesse > 자체 엔진 (검증 이슈)

```
만약 Jesse CAGR > +40%

→ 해석:
  1. 자체 엔진에 버그 있음
  2. Jesse 로직이 더 최적화됨
  3. 자체 엔진 재검토 필요
  
→ 결론: Jesse 결과 신뢰, 자체 엔진 로직 감사
```

---

## Jesse 백테스트 절차

### 1단계: 실데이터 수집

```bash
# PostgreSQL (cryptoengine DB) → Parquet 추출
python scripts/data/export_funding_rates.py

# 출력: /data/funding_rates/BTCUSDT_8h.parquet
```

**확인사항:**
- 펀딩비 데이터 범위: 2023-04 ~ 현재 확인
- 갭 > 10일 없음 확인
- 통계: 양수 vs 음수 펀딩비 비율

### 2단계: Jesse 프로젝트 설정

```bash
cd jesse_project/

# DB 초기화 (또는 기존 사용)
jesse install-pkg jesse-exchange-bybit

# OHLCV 캔들 임포트 (Binance Vision)
python scripts/download_binance_vision.py

# 펀딩비 로더 연결 (custom loader)
# → strategies/FundingArb.py의 self.shared_vars['funding_rate'] 업데이트
```

### 3단계: Jesse 백테스트 실행

```bash
# 단일 회차
jesse backtest FundingArb 2020-04-01 2026-03-31

# 결과 저장
# → jesse_project/backtest_result/ 또는 캐시
```

### 4단계: 결과 분석

```bash
# HTML 리포트 생성
jesse report

# 월별 수익, 거래 목록, equity curve 확인
```

---

## 실행 체크리스트

- [ ] `export_funding_rates.py` 실행, `/data/funding_rates/` 확인
- [ ] Jesse 프로젝트 `README.md` 검토
- [ ] 펀딩비 로더 (`FundingArb.py`) 수정 (if 필요)
- [ ] Jesse 백테스트 단일 회차 실행
- [ ] 결과 CSV/JSON 추출
- [ ] 6년 CAGR, Sharpe, MDD 기록
- [ ] 3년 (2023-2026) 별도 계산
- [ ] 자체 엔진과 비교 테이블 완성
- [ ] 데이터 갭 경고 문서화
- [ ] 최종 권장사항 작성

---

## 참고자료

- [FA 전략 명세서](/docs/fa_strategy_specification.md)
- [자체 엔진 6년 백테스트 보고서](/.result/12.sta.html)
- [Jesse 프로젝트](../jesse_project/README.md)
- [데이터 수집 파이프라인](/scripts/data/export_funding_rates.py)

---

**작성일**: 2026-04-12  
**상태**: 진행 중 (Jesse 결과 대기)  
**다음 단계**: 2단계 실데이터 수집 → Jesse 설정
