# 7-Strategies 롤링 윈도우 분석 리포트 (자동 생성)

**생성 시각**: 2026-05-11 02:59 UTC
**생성 방식**: `rolling_window_analysis.py` (LLM 직접 작성 금지)

---

## 1. 방법론 요약

| 항목 | 값 |
|------|-----|
| 분석 기간 | 2021-01 ~ 2026-04 |
| 윈도우 크기 | 3~63개월 (3개월 단위, 21가지) |
| 시프트 단위 | **3개월 (분기 단위)** |
| 총 윈도우 수/조합 | 231개 |
| 대상 조합 | 7 strategies × 2 variants × 4 TF = 56 조합 |
| 총 평가 건수 | 56 × 231 = **12,936건** |
| 초기 자금 | $10,000 (각 윈도우 독립) |
| Margin cap | 0.95 (잔고의 최대 95%) |
| 청산 기준 | 잔고 ≤ $500 (5%) |

### fail_strict 정의

`fail_strict = net_pnl ≤ $0` — 해당 구간에서 손익이 0 이하인 경우.
`fail_strict_ratio = fail_strict 건수 / 전체 윈도우 수`

---

## 2. 전략 종합 판정

| 순위 | 전략 | 판정 | fail_strict 평균 | 비고 |
|------|------|------|-----------------|------|
| 1 | `trendtype` | 🟢 KEEP | 24.2% ████ | |
| 2 | `supertrend` | 🟢 KEEP | 24.4% ████ | |
| 3 | `supertrend_trendtype` | 🟢 KEEP | 27.2% █████ | |
| 4 | `tradeiq_psar_ha` | 🟢 KEEP | 30.8% ██████ | |
| 5 | `stoch` | 🟢 KEEP | 35.4% ███████ | |
| 6 | `momentum_ma` | 🟢 KEEP | 43.0% ████████ | |
| 7 | `tradeiq_cci_ce` | 🟢 KEEP | 58.7% ███████████ | |

---

## 3. 전략별 상세 분석 (56 조합)

### 🟢 `trendtype` (TrendTypeStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 24.2%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 27.3% | ✅ | $+23,766 | 2021-10~N/A | $-2,077 |
| long_only | 2h | 14.7% | ✅ | $+10,530 | 2025-01~N/A | $-2,084 |
| long_only | 4h | 20.8% | ✅ | $+11,874 | 2025-01~N/A | $-2,975 |
| long_only | 1D | 11.3% | ✅ | $+21,749 | 2025-01~N/A | $-2,092 |
| bidirectional | 1h | 19.9% | ✅ | $+28,284 | 2023-01~N/A | $-2,747 |
| bidirectional | 2h | 51.9% | ✅ | $-6,212 | 2021-01~N/A | $-6,250 |
| bidirectional | 4h | 38.1% | ✅ | $+467 | 2023-07~N/A | $-5,926 |
| bidirectional | 1D | 9.5% | ✅ | $+36,268 | 2025-04~N/A | $-2,019 |

### 🟢 `supertrend` (SupertrendStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 24.4%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 33.8% | ✅ | $+6,702 | 2022-01~N/A | $-2,455 |
| long_only | 2h | 18.2% | ✅ | $+14,150 | 2025-01~N/A | $-3,452 |
| long_only | 4h | 10.8% | ✅ | $+38,113 | 2022-04~N/A | $-1,806 |
| long_only | 1D | 15.6% | ✅ | $+34,165 | 2022-01~N/A | $-1,397 |
| bidirectional | 1h | 32.9% | ✅ | $+7,175 | 2022-07~N/A | $-4,783 |
| bidirectional | 2h | 35.5% | ✅ | $+3,145 | 2025-01~N/A | $-4,938 |
| bidirectional | 4h | 21.2% | ✅ | $+10,543 | 2025-01~N/A | $-3,253 |
| bidirectional | 1D | 27.3% | ✅ | $+15,008 | 2022-07~N/A | $-4,109 |

### 🟢 `supertrend_trendtype` (SupertrendTrendTypeStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 27.2%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 32.5% | ✅ | $+8,511 | 2023-04~N/A | $-1,235 |
| long_only | 2h | 22.1% | ✅ | $+8,049 | 2025-01~N/A | $-3,293 |
| long_only | 4h | 10.4% | ✅ | $+26,155 | 2025-01~N/A | $-2,091 |
| long_only | 1D | 17.3% | ✅ | $+22,196 | 2025-01~N/A | $-2,576 |
| bidirectional | 1h | 27.3% | ✅ | $+7,660 | 2022-10~N/A | $-2,080 |
| bidirectional | 2h | 49.8% | ✅ | $-2,743 | 2023-04~N/A | $-5,360 |
| bidirectional | 4h | 32.0% | ✅ | $+9,153 | 2023-04~N/A | $-3,772 |
| bidirectional | 1D | 26.4% | ✅ | $+7,423 | 2025-01~N/A | $-2,852 |

### 🟢 `tradeiq_psar_ha` (TradeIQPsarHaStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 30.8%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 27.3% | ✅ | $+12,649 | 2025-07~N/A | $-1,239 |
| long_only | 2h | 19.9% | ✅ | $+6,709 | 2022-04~N/A | $-3,035 |
| long_only | 4h | 21.2% | ✅ | $+9,705 | 2022-04~N/A | $-2,370 |
| long_only | 1D | 12.6% | ✅ | $+22,066 | 2024-04~N/A | $-1,593 |
| bidirectional | 1h | 24.2% | ✅ | $+11,269 | 2022-07~N/A | $-4,231 |
| bidirectional | 2h | 42.4% | ✅ | $-1,139 | 2024-04~N/A | $-4,493 |
| bidirectional | 4h | 62.8% | ✅ | $-1,612 | 2022-07~N/A | $-4,394 |
| bidirectional | 1D | 35.9% | ✅ | $+5,801 | 2021-04~N/A | $-4,086 |

### 🟢 `stoch` (StochStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 35.4%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 31.2% | ✅ | $+7,635 | 2021-10~N/A | $-6,723 |
| long_only | 2h | 22.9% | ✅ | $+11,637 | 2021-04~N/A | $-5,570 |
| long_only | 4h | 25.1% | ✅ | $+13,172 | 2021-04~N/A | $-6,250 |
| long_only | 1D | 28.1% | ✅ | $+8,943 | 2021-04~N/A | $-7,334 |
| bidirectional | 1h | 19.5% | ✅ | $+36,250 | 2021-01~N/A | $-5,169 |
| bidirectional | 2h | 87.5% | ✅ | $-7,518 | 2021-01~N/A | $-7,540 |
| bidirectional | 4h | 30.7% | ✅ | $+3,361 | 2024-04~N/A | $-5,694 |
| bidirectional | 1D | 38.5% | ✅ | $+6,221 | 2024-04~N/A | $-6,011 |

### 🟢 `momentum_ma` (MomentumMAStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 43.0%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 19.9% | ✅ | $+14,359 | 2025-07~N/A | $-3,615 |
| long_only | 2h | 39.4% | ✅ | $-722 | 2021-04~N/A | $-7,208 |
| long_only | 4h | 32.5% | ✅ | $+2,220 | 2021-04~N/A | $-7,010 |
| long_only | 1D | 32.5% | ✅ | $+1,255 | 2021-04~N/A | $-6,480 |
| bidirectional | 1h | 48.5% | ✅ | $-1,388 | 2021-04~N/A | $-5,465 |
| bidirectional | 2h | 82.2% | ✅ | $-8,302 | 2021-01~N/A | $-8,457 |
| bidirectional | 4h | 25.5% | ✅ | $-881 | 2021-01~N/A | $-7,220 |
| bidirectional | 1D | 63.6% | ✅ | $-4,436 | 2021-01~N/A | $-6,083 |

### 🟢 `tradeiq_cci_ce` (TradeIQCciCeStrategy)

**판정**: KEEP  |  **평균 fail_strict**: 58.7%  |  **전체기간 평균 PnL**: $+0

| variant | TF | fail_strict_ratio | any_positive | 전체기간 PnL | worst 구간 | worst PnL |
|---------|----|--------------------|--------------|------------|-----------|----------|
| long_only | 1h | 56.3% | ✅ | $-1,734 | 2021-04~N/A | $-3,775 |
| long_only | 2h | 87.9% | ✅ | $-2,297 | 2022-01~N/A | $-4,707 |
| long_only | 4h | 19.9% | ✅ | $+6,210 | 2022-01~N/A | $-2,579 |
| long_only | 1D | 87.0% | ✅ | $-4,955 | 2021-01~N/A | $-5,284 |
| bidirectional | 1h | 48.5% | ✅ | $+2,257 | 2021-04~N/A | $-3,775 |
| bidirectional | 2h | 75.8% | ✅ | $-850 | 2022-01~N/A | $-5,746 |
| bidirectional | 4h | 10.4% | ✅ | $+10,714 | 2025-07~N/A | $-1,579 |
| bidirectional | 1D | 83.5% | ✅ | $-5,313 | 2022-07~N/A | $-6,395 |

---

## 4. 윈도우 크기별 fail_strict 분포

대표 크기(3/12/24/36/48/63개월)에서 전략별 fail_strict_ratio를 비교한다.

| 전략 | 3m | 12m | 24m | 36m | 48m | 63m |
|------|----|----|-----|-----|-----|-----|
| `trendtype` | 51% | 30% | 19% | 12% | 15% | 12% |
| `supertrend` | 58% | 33% | 19% | 8% | 2% | 0% |
| `supertrend_trendtype` | 60% | 36% | 21% | 9% | 4% | 12% |
| `tradeiq_psar_ha` | 55% | 33% | 26% | 11% | 17% | 25% |
| `stoch` | 55% | 44% | 36% | 21% | 15% | 12% |
| `momentum_ma` | 58% | 47% | 41% | 36% | 31% | 62% |
| `tradeiq_cci_ce` | 70% | 56% | 56% | 55% | 56% | 62% |

---

## 5. 연도별 평균 PnL ($10,000 기준)

각 전략의 12개월 윈도우에서 연도별 평균 net_pnl을 집계한다.

| 전략 | 2021 | 2022 | 2023 | 2024 | 2025 |
|------|------|------|------|------|------|
| `trendtype` | $+2,633 | $+2,504 | $+3,801 | $+881 | $-1,747 |
| `supertrend` | $+2,430 | $+836 | $+4,404 | $+2,051 | $-863 |
| `supertrend_trendtype` | $+2,836 | $+1,139 | $+3,937 | $+867 | $-1,616 |
| `tradeiq_psar_ha` | $+1,488 | $-156 | $+2,906 | $+1,415 | $-551 |
| `stoch` | $-315 | $+867 | $+7,150 | $+2,476 | $-993 |
| `momentum_ma` | $-2,211 | $+750 | $+4,626 | $+2,748 | $-1,477 |
| `tradeiq_cci_ce` | $+825 | $-254 | $-184 | $-160 | $+688 |

---

## 6. 최악 윈도우 Top 25

| 순위 | 전략 | variant | TF | 창 크기 | 기간 | net_pnl | 청산 |
|------|------|---------|----|---------|----|---------|------|
| 1 | `momentum_ma` | bidirectional | 2h | 60m | 2021-01~2025-12 | $-8,457 |  |
| 2 | `momentum_ma` | bidirectional | 2h | 63m | 2021-01~2026-03 | $-8,339 |  |
| 3 | `momentum_ma` | bidirectional | 2h | 57m | 2021-01~2025-09 | $-8,250 |  |
| 4 | `momentum_ma` | bidirectional | 2h | 54m | 2021-01~2025-06 | $-8,087 |  |
| 5 | `momentum_ma` | bidirectional | 2h | 21m | 2021-01~2022-09 | $-8,024 |  |
| 6 | `momentum_ma` | bidirectional | 2h | 42m | 2021-01~2024-06 | $-7,946 |  |
| 7 | `momentum_ma` | bidirectional | 2h | 24m | 2021-01~2022-12 | $-7,922 |  |
| 8 | `momentum_ma` | bidirectional | 2h | 39m | 2021-01~2024-03 | $-7,887 |  |
| 9 | `momentum_ma` | bidirectional | 2h | 51m | 2021-01~2025-03 | $-7,836 |  |
| 10 | `momentum_ma` | bidirectional | 2h | 18m | 2021-01~2022-06 | $-7,815 |  |
| 11 | `momentum_ma` | bidirectional | 2h | 45m | 2021-01~2024-09 | $-7,688 |  |
| 12 | `momentum_ma` | bidirectional | 2h | 48m | 2021-01~2024-12 | $-7,628 |  |
| 13 | `stoch` | bidirectional | 2h | 63m | 2021-01~2026-03 | $-7,540 |  |
| 14 | `stoch` | bidirectional | 2h | 54m | 2021-01~2025-06 | $-7,532 |  |
| 15 | `stoch` | bidirectional | 2h | 51m | 2021-01~2025-03 | $-7,477 |  |
| 16 | `stoch` | bidirectional | 2h | 57m | 2021-01~2025-09 | $-7,445 |  |
| 17 | `stoch` | bidirectional | 2h | 33m | 2021-01~2023-09 | $-7,418 |  |
| 18 | `stoch` | bidirectional | 2h | 42m | 2021-01~2024-06 | $-7,415 |  |
| 19 | `stoch` | bidirectional | 2h | 60m | 2021-01~2025-12 | $-7,384 |  |
| 20 | `momentum_ma` | bidirectional | 2h | 9m | 2021-01~2021-09 | $-7,348 |  |
| 21 | `stoch` | long_only | 1D | 21m | 2021-04~2022-12 | $-7,334 |  |
| 22 | `stoch` | bidirectional | 2h | 36m | 2021-01~2023-12 | $-7,297 |  |
| 23 | `momentum_ma` | bidirectional | 2h | 33m | 2021-01~2023-09 | $-7,264 |  |
| 24 | `momentum_ma` | bidirectional | 4h | 12m | 2021-01~2021-12 | $-7,220 |  |
| 25 | `momentum_ma` | long_only | 2h | 21m | 2021-04~2022-12 | $-7,208 |  |

---

## 7. 단기(3개월) 윈도우 분석

단기 윈도우에서 손실 빈도가 높을수록 변동성이 크거나 특정 시기에 취약하다.

| 전략 | 3m 윈도우 수 | fail_strict 건수 | fail_strict_ratio |
|------|------------|-----------------|------------------|
| `trendtype` | 168 | 86 | 51.2% |
| `supertrend` | 168 | 97 | 57.7% |
| `supertrend_trendtype` | 168 | 100 | 59.5% |
| `tradeiq_psar_ha` | 168 | 92 | 54.8% |
| `stoch` | 168 | 92 | 54.8% |
| `momentum_ma` | 168 | 97 | 57.7% |
| `tradeiq_cci_ce` | 168 | 118 | 70.2% |

---

## 8. 전체 기간(63개월) 단일 윈도우

63개월 창 = 2021-01~2026-03 전체 기간. 각 조합별 최종 수익.

| 전략 | variant | TF | net_pnl | 청산 |
|------|---------|----|---------|----|
| `supertrend` | long_only | 4h | $+38,113 |  |
| `trendtype` | bidirectional | 1D | $+36,268 |  |
| `stoch` | bidirectional | 1h | $+36,250 |  |
| `supertrend` | long_only | 1D | $+34,165 |  |
| `trendtype` | bidirectional | 1h | $+28,284 |  |
| `supertrend_trendtype` | long_only | 4h | $+26,155 |  |
| `trendtype` | long_only | 1h | $+23,766 |  |
| `supertrend_trendtype` | long_only | 1D | $+22,196 |  |
| `tradeiq_psar_ha` | long_only | 1D | $+22,066 |  |
| `trendtype` | long_only | 1D | $+21,749 |  |
| `supertrend` | bidirectional | 1D | $+15,708 |  |
| `momentum_ma` | long_only | 1h | $+14,359 |  |
| `supertrend` | long_only | 2h | $+14,150 |  |
| `stoch` | long_only | 4h | $+12,774 |  |
| `tradeiq_psar_ha` | long_only | 1h | $+12,649 |  |
| `trendtype` | long_only | 4h | $+11,874 |  |
| `tradeiq_psar_ha` | bidirectional | 1h | $+11,269 |  |
| `tradeiq_cci_ce` | bidirectional | 4h | $+11,245 |  |
| `stoch` | long_only | 2h | $+11,126 |  |
| `supertrend` | bidirectional | 4h | $+11,024 |  |
| `trendtype` | long_only | 2h | $+10,324 |  |
| `tradeiq_psar_ha` | long_only | 4h | $+9,705 |  |
| `supertrend_trendtype` | bidirectional | 4h | $+9,601 |  |
| `stoch` | long_only | 1D | $+9,360 |  |
| `supertrend_trendtype` | long_only | 1h | $+8,511 |  |
| `supertrend_trendtype` | long_only | 2h | $+8,049 |  |
| `supertrend_trendtype` | bidirectional | 1h | $+7,660 |  |
| `stoch` | long_only | 1h | $+7,635 |  |
| `supertrend_trendtype` | bidirectional | 1D | $+7,423 |  |
| `stoch` | bidirectional | 1D | $+7,249 |  |
| `supertrend` | bidirectional | 1h | $+7,175 |  |
| `tradeiq_psar_ha` | long_only | 2h | $+6,709 |  |
| `supertrend` | long_only | 1h | $+6,702 |  |
| `tradeiq_cci_ce` | long_only | 4h | $+6,210 |  |
| `tradeiq_psar_ha` | bidirectional | 1D | $+5,688 |  |
| `stoch` | bidirectional | 4h | $+3,674 |  |
| `supertrend` | bidirectional | 2h | $+3,145 |  |
| `tradeiq_cci_ce` | bidirectional | 1h | $+2,257 |  |
| `momentum_ma` | long_only | 4h | $+1,992 |  |
| `momentum_ma` | long_only | 1D | $+1,139 |  |
| `trendtype` | bidirectional | 4h | $+555 |  |
| `tradeiq_cci_ce` | bidirectional | 2h | $-647 |  |
| `momentum_ma` | bidirectional | 4h | $-953 |  |
| `momentum_ma` | long_only | 2h | $-961 |  |
| `tradeiq_psar_ha` | bidirectional | 2h | $-1,139 |  |
| `momentum_ma` | bidirectional | 1h | $-1,388 |  |
| `tradeiq_psar_ha` | bidirectional | 4h | $-1,612 |  |
| `tradeiq_cci_ce` | long_only | 1h | $-1,734 |  |
| `tradeiq_cci_ce` | long_only | 2h | $-2,297 |  |
| `supertrend_trendtype` | bidirectional | 2h | $-2,743 |  |
| `momentum_ma` | bidirectional | 1D | $-4,476 |  |
| `tradeiq_cci_ce` | long_only | 1D | $-4,955 |  |
| `tradeiq_cci_ce` | bidirectional | 1D | $-5,301 |  |
| `trendtype` | bidirectional | 2h | $-6,250 |  |
| `stoch` | bidirectional | 2h | $-7,540 |  |
| `momentum_ma` | bidirectional | 2h | $-8,339 |  |

---

## 9. 결론 및 권고

### DELETE 후보

> 없음 — 7개 전략 모두 최소 1개 이상의 양수 구간 보유.

### WEAK 후보

> 없음

### 주요 관찰

- **최고 안정성**: `tradeiq_cci_ce` — fail_strict 평균 58.7%
- **최저 안정성**: `trendtype` — fail_strict 평균 24.2%
- 모든 전략이 KEEP 판정: 각 전략의 특정 TF/variant 조합에서 양수 구간이 존재
- `tradeiq_cci_ce`이 fail 비율 58.7%로 가장 불안정 — 상세 운용 주의 필요
- `trendtype`, `supertrend`, `supertrend_trendtype`이 안정성 상위권
