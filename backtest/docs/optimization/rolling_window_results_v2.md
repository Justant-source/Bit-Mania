---
title: 롤링 윈도우 테스트 결과 (v2 파라미터 기준)
category: test
related_code:
  - cryptoengine/services/jesse_engine/scripts/rolling_window_analysis.py
last_updated: 2026-05-11
when_to_update: v2 champion run 후 데이터 재실행 시
---

# 롤링 윈도우 테스트 결과 (v2 champion 파라미터)

**실행일**: 2026-05-11  
**데이터 범위**: 2021-01 ~ 2026-04 (65개월)  
**Window sizes**: 3~63개월 (3개월 단위, 21가지)  
**대상**: 7전략 × 6조합(3TF × 2variant) = 42 combos × 231 windows = **9,702 evaluations**  
**기준**: `fail_strict` = 구간 누적 PnL ≤ $0 | `fail_meaningful` = PnL < -$100

---

## 1. 전략 종합 판정

롤링 윈도우 기준 전략 단위 판정 — 모든 7개 전략이 **KEEP** (어떤 TF/variant에서도 최소 1회 이상 양수 구간 존재).

| 전략 | 판정 | avg fail% | 최선 조합 | 최악 조합 |
|------|------|----------|---------|---------|
| supertrend | ✅ KEEP | 22.8% | long_only/4h (10.4%) | bidirectional/1h (50.6%) |
| supertrend_trendtype | ✅ KEEP | 24.0% | long_only/4h (9.1%) | bidirectional/1h (54.5%) |
| trendtype | ✅ KEEP | 23.2% | long_only/1D (11.7%) | bidirectional/1h (41.1%) |
| tradeiq_psar_ha | ✅ KEEP | 44.7% | long_only/1D (19.1%) | bidirectional/1h (93.1%) |
| tradeiq_cci_ce | ✅ KEEP | 32.3% | bidirectional/4h (10.4%) | bidirectional/1D (88.3%) |
| stoch | ✅ KEEP* | 28.9% | bidirectional/1h (19.5%) | bidirectional/1D (38.5%) |
| momentum_ma | ✅ KEEP* | 37.1% | long_only/1h (19.9%) | bidirectional/1D (63.6%) |

> \* stoch, momentum_ma: 롤링 윈도우는 KEEP이나 **param_sweep_v2에서 전 16 combo MDD -60%+ 초과로 실전 투입 불가**.  
> 롤링 윈도우는 PnL>0 여부만 확인하고 MDD를 검사하지 않으므로, 극단적 드로우다운을 가진 전략도 KEEP 판정 가능.

---

## 2. 조합별 Fail 비율 전체 순위

`$10,000` 기준 가상 PnL 시뮬레이션. fail = 구간 누적 순손실 발생.

| 순위 | 전략 | TF | Variant | Fail% | 전체기간 PnL | 최악 PnL | 최선 PnL |
|------|------|----|---------|-------|-----------|---------|---------|
| 🥇1 | supertrend_trendtype | 4h | long_only | **9.1%** | +$36,478 | -$1,871 | +$44,265 |
| 🥈2 | supertrend | 4h | long_only | **10.4%** | +$46,152 | -$1,683 | +$53,430 |
| 🥉3 | tradeiq_cci_ce | 4h | bidirectional | **10.4%** | +$10,714 | -$1,579 | +$15,026 |
| 4 | trendtype | 1D | long_only | 11.7% | +$20,000 | -$1,616 | +$25,780 |
| 5 | trendtype | 1D | bidirectional | 12.1% | +$21,259 | -$1,940 | +$24,897 |
| 6 | trendtype | 1h | long_only | 13.0% | +$13,771 | -$1,444 | +$14,671 |
| 7 | supertrend | 4h | bidirectional | 13.9% | +$17,343 | -$3,532 | +$30,171 |
| 8 | tradeiq_cci_ce | 1h | bidirectional | 14.3% | +$4,931 | -$1,611 | +$8,003 |
| 9 | supertrend_trendtype | 1h | long_only | 14.7% | +$10,703 | -$2,440 | +$16,469 |
| 10 | supertrend_trendtype | 1D | long_only | 15.6% | +$26,269 | -$2,188 | +$36,426 |
| 11 | supertrend | 1D | long_only | 16.0% | +$36,646 | -$1,397 | +$40,319 |
| 12 | tradeiq_cci_ce | 4h | long_only | 17.8% | +$7,642 | -$2,629 | +$10,991 |
| 13 | supertrend | 1h | long_only | 18.6% | +$6,985 | -$2,306 | +$12,540 |
| 14 | tradeiq_psar_ha | 1D | long_only | 19.1% | +$30,452 | -$1,889 | +$30,993 |
| — | stoch | 1h | bidirectional | 19.5% | +$36,250 | -$5,169 | +$85,740 |
| 15 | momentum_ma | 1h | long_only | 19.9% | +$14,359 | -$3,615 | +$36,920 |
| 16 | tradeiq_psar_ha | 4h | long_only | 20.8% | +$7,965 | -$2,594 | +$10,907 |
| 17 | trendtype | 4h | long_only | 20.8% | +$11,896 | -$2,973 | +$21,421 |
| — | ... | | | | | | |
| ⚠️ | tradeiq_cci_ce | 1D | long_only | 39.8% | **-$1,175** | -$3,169 | +$4,037 |
| ❌ | tradeiq_cci_ce | 1D | bidirectional | 88.3% | **-$5,670** | -$6,375 | +$2,925 |
| ❌ | tradeiq_psar_ha | 1h | bidirectional | **93.1%** | **-$7,129** | -$7,363 | +$2,775 |

---

## 3. 5개 Champion 조합 심층 분석

### 손실 구간 패턴 (최악 3개 window)

#### supertrend / 4h / long_only (fail 10.4%, PnL +$46,152)

| 손실 구간 | window | 손실 | 해석 |
|---------|--------|------|------|
| 2022-04 | 9개월 | -$1,683 | Luna 붕괴~FTX 직전 하락장 집중 |
| 2021-04 | 3개월 | -$1,656 | BTC 고점 후 5월 급락 |
| 2022-07 | 6개월 | -$1,344 | FTX 붕괴 전후 Bear market |

→ **손실 모두 Bear market 구간**. 청산(liquidated) 없음. 최대 손실 -$1,683 ($10k 대비 16.8%).

#### supertrend_trendtype / 4h / long_only (fail 9.1%, PnL +$36,478)

| 손실 구간 | window | 손실 | 해석 |
|---------|--------|------|------|
| 2025-01 | 3개월 | -$1,871 | 2025년 초 조정 구간 |
| 2021-04 | 3개월 | -$1,572 | 2021년 5월 급락 |
| 2025-01 | 15개월 | -$1,435 | 2025~2026년 박스권 |

→ 최악 구간이 2022년 Bear market이 아닌 **2025년 조정** → 최근 시장에서 다소 취약.

#### trendtype / 1D / long_only (fail 11.7%, PnL +$20,000)

| 손실 구간 | window | 손실 | 해석 |
|---------|--------|------|------|
| 2025-07 | 9개월 | -$1,616 | 2025년 하반기 조정 |
| 2025-01 | 15개월 | -$1,606 | 2025~2026년 장기 횡보 |
| 2022-01 | 12개월 | -$1,509 | LUNA/FTX 하락장 |

→ **최대 손실이 -$1,616으로 7개 전략 중 최소**. 방어적 특성 확인.

#### tradeiq_psar_ha / 1D / long_only (fail 19.1%, PnL +$30,452)

| 손실 구간 | window | 손실 | 해석 |
|---------|--------|------|------|
| 2024-04 | 6개월 | -$1,889 | 2024년 4월 조정 |
| 2024-07 | 3개월 | -$1,194 | 2024년 7월 급락 |
| 2022-01 | 15개월 | -$1,018 | Bear market |

→ **2024년 구간에서 반복 손실** — 2024년 조정 패턴에 취약.

#### tradeiq_cci_ce / 4h / bidirectional (fail 10.4%, PnL +$10,714)

| 손실 구간 | window | 손실 | 해석 |
|---------|--------|------|------|
| 2025-07 | 6개월 | -$1,579 | 2025년 하반기 |
| 2025-07 | 9개월 | -$1,511 | 동일 구간 연장 |
| 2025-10 | 3개월 | -$1,327 | 2025년 4분기 |

→ **2025년 하반기에 집중 손실** — 최근 시장 부진. 전체 PnL이 가장 낮음 (+$10k).

---

## 4. 위험 조합 — 실전 투입 불가

| 조합 | Fail% | 전체기간 PnL | 결론 |
|------|-------|-----------|------|
| tradeiq_psar_ha/1h/bidirectional | **93.1%** | -$7,129 | 즉시 배제 |
| tradeiq_cci_ce/1D/bidirectional | 88.3% | -$5,670 | 즉시 배제 |
| momentum_ma/1D/bidirectional | 63.6% | -$4,436 | 배제 |
| supertrend_trendtype/1h/bidirectional | 54.5% | -$2,699 | 배제 |
| supertrend/1h/bidirectional | 50.6% | -$957 | 배제 |
| tradeiq_psar_ha/4h/bidirectional | 55.8% | -$616 | 배제 |
| tradeiq_cci_ce/1D/long_only | 39.8% | **-$1,175** | trades≤10으로 신뢰도 낮음, 배제 |

---

## 5. param_sweep v2 × 롤링 윈도우 교차 검증

두 테스트를 조합한 최종 판정:

| 전략 | TF | Variant | Sweep score | 롤링 fail% | 교차 판정 | 비고 |
|------|----|---------|------------|----------|---------|------|
| supertrend | 4h | long_only | +38.39 | 10.4% | ✅✅ **최우선** | 양쪽 모두 1위권 |
| supertrend_trendtype | 4h | long_only | +31.78 | 9.1% | ✅✅ **최우선** | 롤링 전체 1위 |
| supertrend | 1D | long_only | +30.48 | 16.0% | ✅ 우수 | 거래수 43회로 적음 |
| tradeiq_psar_ha | 1D | long_only | +22.12 | 19.1% | ✅ 우수 | 2024년 구간 취약 |
| trendtype | 1D | long_only | +29.33 | 11.7% | ✅ 우수 | MDD 최저, 방어적 |
| tradeiq_cci_ce | 4h | bidirectional | +17.27 | 10.4% | ⚠️ 조건부 | score 낮음, 최근 부진 |
| stoch | any | any | ALL -999 | 19~38% | ❌ 불가 | MDD -60%, 실전 투입 불가 |
| momentum_ma | any | any | ALL -999 | 20~64% | ❌ 불가 | MDD -54%, 실전 투입 불가 |

---

## 6. 핵심 결론

1. **supertrend/4h/long_only** (st_factor=2.5, st_period=7) + **supertrend_trendtype/4h/long_only** (st_factor=2.0, atr_len=10): 두 테스트 모두 최상위 → **실전 후보 1순위**

2. **trendtype/1D/long_only** (atr_len=10, di_len=10): 가장 낮은 MDD(-21%)와 최소 손실(-$1,616) → **보수적 운영에 최적**

3. **Long_only가 일관되게 우수**: Bidirectional은 대부분 2배 이상 높은 fail% → BTC 상승 편향 시장에서 공매도 전략은 구조적으로 불리

4. **2022년 Bear market 내성**: Tier 1 전략들은 Bear market에서 손실이 발생하되 청산(-$1,600~-$1,900)이나 파산 없이 회복 → 리스크 허용 범위 내

5. **stoch/momentum_ma**: 롤링 윈도우 KEEP 판정은 MDD 미검사 아티팩트 → **param_sweep의 DELETE 판정이 우선**

---

*생성: 2026-05-11 | 스크립트: rolling_window_analysis.py | 데이터: v2 champion 파라미터*
