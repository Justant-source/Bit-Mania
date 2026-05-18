---
status: archived-2026-05-12
archived_reason: "모든 TF/방향에서 FAIL. CCI 저거래수 + Sharpe 미달. 개선 가능성 낮음."
last_updated: 2026-05-18
---

> ⚠️ **아카이브됨 (2026-05-12)**: 전체 TF/방향에서 FAIL. Phase 5 후보 제외.

# 020 — TradeIQ CCI-CE 전략 (CCI + Chandelier Exit)

코드: `backtest/strategies/external/TradeIQCciCeStrategy.py`  
현재 상태: **ARCHIVED** — 전 TF FAIL

## 개요

**CCI (Commodity Channel Index) + Chandelier Exit** 결합. CCI가 하단 경계(-100)를 하향돌파 후 재상향할 때 진입, Chandelier Exit가 방향 확인. 평균 회귀 + 추세 필터 하이브리드 전략이나 실제 성과는 7개 전략 중 최하위.

## 알고리즘

### 진입 조건

```python
def should_long() -> bool:
    cci_prev, cci_cur = _cci_vals()
    cci_cross_up = (cci_prev < cci_lower and cci_cur > cci_lower)  # CCI 상향 교차
    ce_direction = _chandelier()[2]   # Chandelier Exit 방향
    return cci_cross_up and ce_direction == 1.0
```

### Chandelier Exit

```python
ce_stop_long  = highest_high(ce_period) - ce_mult * atr(ce_period)
ce_stop_short = lowest_low(ce_period) + ce_mult * atr(ce_period)
direction = 1 if price > ce_stop_long else -1
```

### 청산 조건

```python
def update_position():
    atr_stop = atr(14) * atr_mult
    if price <= entry - atr_stop or price >= entry + atr_stop:
        liquidate()
```

## 파라미터 (v2 기본)

| 파라미터 | 값 | 설명 |
|---------|-----|------|
| `cci_period` | 20 | CCI 계산 주기 |
| `cci_lower` | -100.0 | CCI 하단 경계 |
| `cci_upper` | 100.0 | CCI 상단 경계 |
| `ce_period` | 22 | Chandelier 주기 |
| `ce_mult` | 3.0 | Chandelier ATR 배수 |
| `atr_mult` | 3.0 | 스탑 ATR 배수 |

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| 4h | long_only | 4.29% | 0.298 | -33.90% | 80 | ❌ FAIL |
| 4h | bidirectional | 21.94% | 0.867 | -31.84% | 121 | ❌ FAIL |
| 1D | long_only | 2.66% | 0.279 | -15.84% | 5 | ❌ FAIL |
| 1D | bidirectional | 1.22% | 0.161 | -31.20% | 12 | ❌ FAIL |
| 1h | long_only | 6.60% | 0.452 | -26.09% | 187 | ❌ FAIL |
| 1h | bidirectional | 7.82% | 0.466 | -24.67% | 257 | ❌ FAIL |

> 1D 거래수 5건 (long_only) — 통계 신뢰 불가.  
> 4h bidirectional이 CAGR 21.94%로 가장 높으나 MDD 게이트 미통과.

## 파라미터 스윕 이력 (v2/v3, 2021-04~2026-04 기준)

| TF | 방향 | Ver | `cci_period` | `ce_mult` | Score | P1 CAGR | MDD | 비고 |
|---|---|---|---|---|---|---|---|---|
| 4h | bidirectional | v3 | **18** | **3.0** | **+29.38** | +23.0% | -31.8% | ✅ 최우선 (단 gates 미통과) |
| 4h | long_only | v2 | **14** | **2.5** | **+15.76** | +11.8% | -32.6% | ⚠️ CAGR 미달 |
| 1h | bidirectional | v2 | 30 | 3.0 | +11.88 | +8.9% | -24.7% | ⚠️ 조건부 |
| 1D | long_only | v2 | 20 | 3.5 | +5.95 | +5.2% | -24.2% | ⚠️ 거래수 10건 미만 |

> v2: 16 combo (cci_period × ce_mult). 4h long_only에서만 PASS 수준 score.  
> v3: 4h bidir 세밀 탐색 — cci_period=18이 v2 best(20)보다 CAGR +5.7pp 개선.  
> 그러나 4h bidir CAGR 21.94%(전체 기간) → Sharpe 0.867로 전략 판정 기준에서 FAIL.

## 아카이브 이유

1. **거래수 부족**: 1D long_only 5건 — 통계 불신뢰
2. **CAGR 미달**: long_only 전 TF에서 5% 미만 또는 매우 낮음
3. **구조적 약점**: CCI 교차 신호가 4h 이상 TF에서 매우 희소 (80건/5년)
4. **v3 개선 한계**: cci_period=18이 bidir 성과를 개선하나 PASS 기준 미충족

## 복원 조건

- CCI 파라미터 대폭 재설계 (cci_lower 변경, 다중 CCI 조합) 또는
- 다른 시장(알트코인) 환경에서 재검증 시 고려

## 관련 문서

- 백테스트 결과 경로: `backtest/results/7-strategies/tradeiq_cci_ce/`
- 알고리즘 상세: `.result/02_STRATEGIES.md` §7
