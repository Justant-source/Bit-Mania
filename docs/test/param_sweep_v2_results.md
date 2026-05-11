---
title: param_sweep_v2 결과 리포트
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/param_sweep_v2.py
last_updated: 2026-05-11
---

# param_sweep_v2 — 5시간 백테스트 결과 리포트

**실행일**: 2026-05-11  
**소요 시간**: 약 5시간 (6-container 병렬)  
**총 백테스트**: 2,688회 (7전략 × 16combo × 3TF × 2variant × 4period)

---

## 1. 실행 개요

### 평가 기준 — 4-Period

| 기간 | 시작 | 종료 | 시장 국면 |
|------|------|------|---------|
| **P1** | 2021-04-01 | 2026-04-30 | 고점 매수 → 현재 (최악 진입) |
| **P2** | 2022-12-01 | 2026-04-30 | 저점 매수 → 현재 (최선 진입) |
| **P3** | 2021-04-01 | 2025-09-30 | 고점 → 다음 고점 (전체 사이클) |
| **P4** | 2022-12-01 | 2025-09-30 | 저점 → 고점 (Bull run) |

**점수식**: `mean(P1~P4 CAGR)` — 단, 하나라도 `MDD < -35%` 또는 `trades < 5`이면 `-999` (즉시 탈락)

### 탐색 공간

| 항목 | 내용 |
|------|------|
| 전략 | 7개 (supertrend, trendtype, supertrend_trendtype, tradeiq_220320, tradeiq_220323, stoch, momentum_ma) |
| Combo | 전략당 16개 (기존 v1 6개 + 신규 10개) |
| TF | 1h / 4h / 1D |
| Variant | long_only / bidirectional |
| 병렬 실행 | 6 Docker container (컨테이너당 1~2 전략, workers=1~2) |

---

## 2. 전략별 종합 결과

### 생존 여부 요약

| 전략 | 유효 combo 수 (전체 96) | 최고 score | 최고 TF/variant | 판정 |
|------|----------------------|-----------|----------------|------|
| **supertrend** | 37 | **+38.39** | 4h/long_only | ✅ KEEP |
| **supertrend_trendtype** | 42 | **+31.78** | 4h/long_only | ✅ KEEP |
| **trendtype** | 24 | **+29.33** | 1D/long_only | ✅ KEEP |
| **tradeiq_220320** | 32 | **+22.12** | 1D/long_only | ✅ KEEP |
| **tradeiq_220323** | 19 | **+17.27** | 4h/bidirectional | ✅ KEEP (조건부) |
| **stoch** | **0** | -999 | — | ❌ DELETE |
| **momentum_ma** | **0** | -999 | — | ❌ DELETE |

> 유효 combo = 4개 기간 모두 MDD ≥ -35% AND trades ≥ 5 통과한 조합

---

## 3. 전략별 상세 결과

### 3-1. SupertrendStrategy ✅

**최적**: `4h / long_only` — combo#16 (st_factor=**2.5**, st_period=7) — score **+38.39**

#### TF별 최적 파라미터 요약

| TF | variant | combo# | st_factor | st_period | score | full-period CAGR | Sharpe | MDD |
|----|---------|--------|-----------|-----------|-------|-----------------|--------|-----|
| **4h** | long_only | #16 | **2.5** | 7 | **+38.39** | +38.27% | 1.170 | -26.44% |
| **1D** | long_only | #4 | 3.0 | **5** | +30.48 | +33.54% | 1.052 | -23.96% |
| **1h** | long_only | #11 | **5.0** | 7 | +10.66 | +10.46% | 0.483 | -33.12% |
| 4h | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1D | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1h | bidirectional | — | — | — | ALL -999 | — | — | — |

#### 4h/long_only 최적 combo#16 — 4-Period 분석

| 기간 | CAGR | Sharpe | MDD | 거래수 | 승률 | PF |
|------|------|--------|-----|--------|------|----|
| P1 (고점→현재) | +26.70% | 0.988 | -26.44% | 229 | 44.5% | 1.307 |
| P2 (저점→현재) | +41.18% | 1.399 | -25.02% | 174 | 45.4% | 1.367 |
| P3 (고점→고점) | +31.87% | 1.085 | -26.44% | 216 | 44.9% | 1.366 |
| P4 (Bull run) | +53.82% | 1.599 | -25.02% | 161 | 46.0% | 1.450 |

> P1 vs P4 CAGR 차이 +27.1%p → 진입 시점에 크게 좌우되나, P1도 +26.7%로 안정적.  
> MDD 일관성: 전 기간 -25~-26.4% 범위로 거의 고정 → 리스크 예측 가능성 높음.

---

### 3-2. SupertrendTrendTypeStrategy ✅

**최적**: `4h / long_only` — combo#6 (st_factor=**2.0**, atr_len=**10**) — score **+31.78**

#### TF별 최적 파라미터 요약

| TF | variant | combo# | st_factor | atr_len | score | full-period CAGR | Sharpe | MDD |
|----|---------|--------|-----------|---------|-------|-----------------|--------|-----|
| **4h** | long_only | #6 | **2.0** | **10** | **+31.78** | +33.44% | 1.121 | -29.05% |
| **1D** | long_only | #16 | **2.5** | **12** | +27.64 | +27.37% | 0.968 | -29.55% |
| **1h** | long_only | #11 | **5.0** | 14 | +13.61 | — | — | — |
| 4h | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1D | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1h | bidirectional | — | — | — | ALL -999 | — | — | — |

#### 4h/long_only 최적 combo#6 — 4-Period 분석

| 기간 | CAGR | Sharpe | MDD | 거래수 | 승률 | PF |
|------|------|--------|-----|--------|------|----|
| P1 (고점→현재) | +27.13% | 1.059 | -29.05% | 188 | 45.2% | 1.350 |
| P2 (저점→현재) | +30.23% | 1.163 | -29.05% | 145 | 45.5% | 1.342 |
| P3 (고점→고점) | +31.56% | 1.143 | -29.05% | 177 | 45.8% | 1.398 |
| P4 (Bull run) | +38.20% | 1.308 | -29.05% | 134 | 46.3% | 1.397 |

> MDD가 전 기간 -29.05% 고정 — 동일 손실 구간에서 반복 발생. 진입보다 구조적 MDD.  
> P1~P3 CAGR 편차가 작음(4.4%p) → supertrend 단독보다 진입시점 의존도 낮음.

---

### 3-3. TrendTypeStrategy ✅

**최적**: `1D / long_only` — combo#6 (atr_len=**10**, di_len=**10**) — score **+29.33**

#### TF별 최적 파라미터 요약

| TF | variant | combo# | atr_len | di_len | score | full-period CAGR | Sharpe | MDD |
|----|---------|--------|---------|--------|-------|-----------------|--------|-----|
| **1D** | long_only | #6 | **10** | **10** | **+29.33** | +22.91% | 0.978 | -21.16% |
| **1h** | long_only | #13 | 14 | **21** | +25.60 | +17.66% | 0.752 | -27.74% |
| **1D** | bidirectional | #4 | 14 | **10** | +18.01 | +23.87% | 0.765 | -32.35% |
| 4h | long_only | — | — | — | ALL -999 | — | — | — |
| 4h | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1h | bidirectional | — | — | — | ALL -999 | — | — | — |

#### 1D/long_only 최적 combo#6 — 4-Period 분석

| 기간 | CAGR | Sharpe | MDD | 거래수 | 승률 | PF |
|------|------|--------|-----|--------|------|----|
| P1 (고점→현재) | +22.13% | 1.018 | -21.01% | 70 | 51.4% | 1.776 |
| P2 (저점→현재) | +28.65% | 1.205 | -20.81% | 53 | 56.6% | 1.813 |
| P3 (고점→고점) | +27.43% | 1.163 | -21.01% | 66 | 51.5% | 2.069 |
| P4 (Bull run) | +39.11% | 1.444 | **-15.01%** | 49 | 57.1% | 2.176 |

> **7개 전략 중 MDD 최저** — P4에서 MDD -15%는 전략 전체 최소값.  
> 승률 51~57% — 유일하게 50% 초과. 높은 PF(1.8~2.2)와 결합해 편안한 드로우다운.  
> 4h는 전 조합 -999: 중간 TF에서 ADX 시그널 노이즈 과다로 추정.

---

### 3-4. TradeIQ220320Strategy ✅

**최적**: `1D / long_only` — combo#8 (rsi_len=**18**, atr_mult=**4.0**) — score **+22.12**

#### TF별 최적 파라미터 요약

| TF | variant | combo# | rsi_len | atr_mult | score | full-period CAGR | Sharpe | MDD |
|----|---------|--------|---------|----------|-------|-----------------|--------|-----|
| **1D** | long_only | #8 | **18** | **4.0** | **+22.12** | +30.01% | 1.081 | -27.36% |
| **4h** | long_only | #15 | 21 | **4.0** | +11.67 | +11.63% | 0.541 | -32.10% |
| 1D | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1h | long_only | — | — | — | ALL -999 | — | — | — |
| 4h | bidirectional | — | — | — | ALL -999 | — | — | — |
| 1h | bidirectional | — | — | — | ALL -999 | — | — | — |

#### 1D/long_only 최적 combo#8 — 4-Period 분석

| 기간 | CAGR | Sharpe | MDD | 거래수 | 승률 | PF |
|------|------|--------|-----|--------|------|----|
| P1 (고점→현재) | +16.44% | 0.743 | -27.36% | 67 | 34.3% | 1.588 |
| P2 (저점→현재) | +23.45% | 0.964 | -27.36% | 56 | 35.7% | 1.652 |
| P3 (고점→고점) | +19.09% | 0.800 | -27.36% | 67 | 34.3% | 1.610 |
| P4 (Bull run) | +29.49% | 1.077 | -27.36% | 56 | 35.7% | 1.679 |

> 승률 34~36%로 낮지만 PF 1.6~1.7 → 손절은 좁고 수익은 크게 태우는 추세 추종형.  
> MDD 전 기간 -27.36% 고정 — 동일 구간에서 반복 손실. trendtype보다 드로우다운 깊음.  
> 4h는 점수가 크게 낮아짐(+11.67) → 1D 특화 전략.

---

### 3-5. TradeIQ220323Strategy ✅ (조건부)

**최적**: `4h / bidirectional` — combo#2 (cci_period=**20**, ce_mult=**3.0**) — score **+17.27**

#### TF별 최적 파라미터 요약

| TF | variant | combo# | cci_period | ce_mult | score | full-period CAGR | Sharpe | MDD |
|----|---------|--------|-----------|---------|-------|-----------------|--------|-----|
| **4h** | bidirectional | #2 | **20** | 3.0 | **+17.27** | +14.65% | 0.665 | -32.19% |
| **4h** | long_only | #6 | 14 | **2.5** | +15.76 | +11.25% | 0.598 | -32.58% |
| **1h** | bidirectional | #11 | **30** | 3.0 | +11.88 | — | — | — |
| **1h** | long_only | #15 | 30 | **3.5** | +6.71 | +6.60% | 0.452 | -26.09% |
| **1D** | long_only | #5 | 20 | **3.5** | +5.95 | — | — | — |
| 1D | bidirectional | — | — | — | ALL -999 | — | — | — |

#### 4h/bidirectional 최적 combo#2 — 4-Period 분석

| 기간 | CAGR | Sharpe | MDD | 거래수 | 승률 | PF |
|------|------|--------|-----|--------|------|----|
| P1 (고점→현재) | +15.29% | 0.694 | -32.19% | 104 | 56.7% | 1.303 |
| P2 (저점→현재) | +11.50% | 0.664 | -24.17% | 66 | 56.1% | 1.302 |
| P3 (고점→고점) | +21.67% | 0.877 | -32.19% | 94 | 59.6% | 1.483 |
| P4 (Bull run) | +20.63% | 1.026 | -20.24% | 56 | 60.7% | 1.642 |

> 유일하게 bidirectional이 long_only보다 높은 점수 — 하락장 공매도가 수익에 기여.  
> 승률 57~61% — tradeiq 계열 특성. 그러나 Sharpe 0.7 미만으로 리스크 조정 수익은 낮음.  
> **주의**: score +17.27은 5개 전략 중 최저. 실전 투입 전 롤링 윈도우 재검증 권장.

---

### 3-6. StochStrategy ❌ DELETE

**전 96 combo (6TF×variant × 16combo)에서 MDD -60%+ 초과로 전멸.**

| TF | variant | 유효 combo | 최고 MDD | 판정 |
|----|---------|-----------|---------|------|
| 1D | long_only | 0/16 | -60% ~ -89% | FAIL |
| 1D | bidirectional | 0/16 | -60% ~ -89% | FAIL |
| 4h | long_only | 0/16 | -60% ~ -89% | FAIL |
| 4h | bidirectional | 0/16 | -60% ~ -89% | FAIL |
| 1h | long_only | 0/16 | -60% ~ -89% | FAIL |
| 1h | bidirectional | 0/16 | -60% ~ -89% | FAIL |

**원인 추정**: Stochastic 과매수/과매도 시그널은 BTC 강세장에서 반복 공매도 진입으로 손실 누적. `direction_ema_len=200` 필터가 있음에도 2021~2022년 급락 구간에서 회복 불가 수준의 드로우다운 발생.

---

### 3-7. MomentumMAStrategy ❌ DELETE

**전 96 combo에서 MDD -54%+ 초과로 전멸.**

| TF | variant | 유효 combo | 판정 |
|----|---------|-----------|------|
| 1D | long_only | 0/16 | FAIL |
| 4h | long_only | 0/16 | FAIL |
| 1h | long_only | 0/16 | FAIL |
| 전 bidirectional | — | 0/48 | FAIL |

**원인 추정**: 선형 회귀 기울기 기반 모멘텀 필터가 BTC 급락 후 반등 초기에 진입 지연 또는 역방향 진입 유발. `val_ma_len=100` 장기 이동평균 대비 가격이 하방에 있을 때 long 차단 효과가 불충분.

---

## 4. 챔피언 런 결과 — 전체 기간 (2021-04-01 ~ 2026-04-30)

최적 파라미터로 전체 기간 재실행한 결과.

| 전략 | TF | variant | CAGR | Sharpe | MDD | trades |
|------|----|---------|------|--------|-----|--------|
| **supertrend** | 4h | long_only | **+38.27%** | **1.170** | -26.44% | 247 |
| **supertrend** | 1D | long_only | +33.54% | 1.052 | **-23.96%** | 43 |
| **supertrend_trendtype** | 4h | long_only | +33.44% | 1.121 | -29.05% | 200 |
| **tradeiq_220320** | 1D | long_only | +30.01% | 1.081 | -27.36% | 72 |
| **supertrend_trendtype** | 1D | long_only | +27.37% | 0.968 | -29.55% | 33 |
| **trendtype** | 1D | bidirectional | +23.87% | 0.765 | -32.35% | 166 |
| **trendtype** | 1D | long_only | +22.91% | 0.978 | **-21.16%** | 77 |
| **trendtype** | 1h | long_only | +17.66% | 0.752 | -27.74% | 1380 |
| **tradeiq_220323** | 4h | bidirectional | +14.65% | 0.665 | -32.19% | 108 |
| **tradeiq_220323** | 4h | long_only | +11.25% | 0.598 | -32.58% | 83 |
| **supertrend** | 1h | long_only | +10.46% | 0.483 | -33.12% | 981 |
| **tradeiq_220323** | 1h | long_only | +6.60% | 0.452 | -26.09% | 187 |

---

## 5. 핵심 인사이트

### 5-1. TF별 특성

| TF | 특징 |
|----|------|
| **1D** | 고수익 고품질. supertrend(+33%), tradeiq_220320(+30%), trendtype(+23%) — Sharpe 1.0+ |
| **4h** | 균형형. supertrend(+38%), st_trendtype(+33%). 거래수 충분하고 MDD -26~-29% |
| **1h** | 점수 낮음(+10~+13). 과매매로 수수료 영향 큼, MDD -27~-33%로 1D 대비 개선 없음 |

### 5-2. Variant별 특성

- **long_only 압도적 우위**: 7전략 × 3TF × 16combo = 336 조합 중 long_only가 대부분의 유효 combo 차지
- **bidirectional은 대부분 -999**: BTC 상승 편향 시장에서 공매도가 MDD를 폭발시킴
- **예외**: trendtype/1D, tradeiq_220323/4h → 이 두 조합은 bidirectional도 유효

### 5-3. 시장 국면별 성과 (P1 vs P4 격차)

| 전략/TF | P1 CAGR | P4 CAGR | 격차 | 해석 |
|---------|---------|---------|------|------|
| supertrend/4h | +26.7% | +53.8% | +27.1%p | 진입시점 의존도 높음 |
| supertrend_trendtype/4h | +27.1% | +38.2% | +11.1%p | 진입시점 의존도 낮음 ✓ |
| trendtype/1D | +22.1% | +39.1% | +17.0%p | 중간 |
| tradeiq_220320/1D | +16.4% | +29.5% | +13.1%p | 중간 |
| tradeiq_220323/4h bidir | +15.3% | +20.6% | +5.3%p | 진입시점 가장 독립적 ✓ |

> **supertrend_trendtype**과 **tradeiq_220323**은 진입 시점에 가장 덜 민감 — 임의 시점 투자 시 유리.

### 5-4. MDD 안전성 랭킹

| 순위 | 전략/TF | 최악 MDD (P1 기준) |
|------|---------|-------------------|
| 1 | trendtype/1D/long_only | **-21.01%** |
| 2 | supertrend/1D/long_only | -23.96% |
| 3 | supertrend/4h/long_only | -26.44% |
| 4 | tradeiq_220320/1D/long_only | -27.36% |
| 5 | supertrend_trendtype/4h/long_only | -29.05% |
| 6 | tradeiq_220323/4h/bidirectional | -32.19% |

---

## 6. v1 vs v2 변경 사항

| 전략 | v1 최적 파라미터 | v1 CAGR | v2 최적 파라미터 | v2 CAGR | 개선 |
|------|----------------|---------|----------------|---------|------|
| supertrend | st_factor=2.0 / 4h | +33.44% | st_factor=**2.5** / 4h | +38.27% | **+4.83%p** |
| trendtype | di_len=18 / 1D | +25.83% | atr_len=10, di_len=10 / 1D | +22.91% | -2.92%p |
| tradeiq_220320 | rsi_len=18 / 1D | +29.94% | rsi_len=18, atr_mult=**4.0** / 1D | +30.01% | +0.07%p |
| supertrend_trendtype | st_factor=2.0 / 4h | +33.44% | 동일 (st_factor=2.0, atr_len=10) | +33.44% | 변화 없음 |
| tradeiq_220323 | baseline 유지 | — | cci=20, ce=3.0 / 4h bidir | +14.65% | 신규 발굴 |
| stoch | v1 전 FAIL | — | v2도 전 FAIL | — | — |
| momentum_ma | v1 전 FAIL | — | v2도 전 FAIL | — | — |

---

## 7. 권장 운영 조합 (실전 후보)

### Tier 1 — 단독 운영 가능

| 전략 | TF | variant | CAGR | Sharpe | MDD | 추천 이유 |
|------|----|---------|------|--------|-----|---------|
| supertrend | 4h | long_only | +38.27% | 1.170 | -26.44% | 최고 수익, MDD 안정 |
| supertrend | 1D | long_only | +33.54% | 1.052 | -23.96% | 거래 적음, 편안한 MDD |
| supertrend_trendtype | 4h | long_only | +33.44% | 1.121 | -29.05% | 진입시점 독립성 ✓ |
| tradeiq_220320 | 1D | long_only | +30.01% | 1.081 | -27.36% | 1D 안정형 |

### Tier 2 — 포트폴리오 보완용

| 전략 | TF | variant | CAGR | Sharpe | MDD | 추천 이유 |
|------|----|---------|------|--------|-----|---------|
| trendtype | 1D | long_only | +22.91% | 0.978 | **-21.16%** | MDD 최저, 보수적 운영 |
| tradeiq_220323 | 4h | bidirectional | +14.65% | 0.665 | -32.19% | 진입시점 독립, 리스크 분산 |

### Tier 3 — 사용 비권장

| 전략 | 이유 |
|------|------|
| supertrend/1h | score +10, 수수료 영향 큼 |
| tradeiq_220323/1D | trades ≤ 10으로 통계적 신뢰도 낮음 |
| stoch / momentum_ma | 전 조합 MDD -60%+ 탈락 |

---

## 8. 다음 단계

```
1. 롤링 윈도우 재검증 (Tier 1 조합 대상)
   python3 cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py \
     --strategies supertrend supertrend_trendtype tradeiq_220320 trendtype \
     --skip-plots

2. stoch / momentum_ma 코드 보관 (삭제 보류, 리팩 후 재평가 가능)

3. 포워드 테스트 고려 (Phase 4 전략 교체 시)
   현재: fa80_lev5_r30 (펀딩비 차익거래, CAGR +34.87%)
   후보: supertrend/4h/long_only (방향성 전략, 상호 보완 가능성)
```

---

*생성: 2026-05-11 | 스크립트: param_sweep_v2.py | 데이터: 2020-05-01 ~ 2026-04-30*
