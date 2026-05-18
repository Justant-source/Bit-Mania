---
status: archived-2026-05-12
archived_reason: "v2/v3 스윕 전 조합 -999 FAIL. MDD 구조적 문제 (-60~-84%). Phase 5 후보 제외."
last_updated: 2026-05-18
---

> ⚠️ **아카이브됨 (2026-05-12)**: v2+v3 파라미터 스윕 전 조합 FAIL. Phase 5 실전 후보에서 제외.

# 016 — Stochastic 전략 (Stoch + EMA)

코드: `backtest/strategies/external/StochStrategy.py`  
현재 상태: **ARCHIVED** — 전 TF/방향 FAIL

## 개요

**Stochastic K + EMA 교차 + Heikin Ashi** 기반 과매도 반전 전략. v3에서 200 EMA 필터(`use_direction_ema=True`) 추가했으나 MDD 구조적 문제(-60%+) 해결 불가.

## 알고리즘

### 진입 조건

```python
def should_long() -> bool:
    # 조건 A: 과매도 반전
    cond_a = (k <= 20 and ha_close > ha_open and not is_downtrend and k > k_prev)
    if use_direction_ema:
        cond_a = cond_a and price > ema(200)
    # 조건 B: 강한 상승 추세 (EMA 이중 상향)
    cond_b = (fast_ema > slow_ema and fast_ema > fast_ema_prev and slow_ema > slow_ema_prev)
    return cond_a or cond_b
```

### 청산 조건

```python
def update_position():
    atr_stop = atr(14) * atr_mult
    if price <= entry - atr_stop or price >= entry + atr_stop:
        liquidate()
```

## 파라미터

| 파라미터 | 기본값 | 범위 | 설명 |
|---------|-------|------|------|
| `stoch_k_period` | 14 | 10~20 | Stochastic K 기간 |
| `atr_mult` | 3.0 | 1.5~5.0 | ATR 손절/익절 배수 |
| `fast_n` | 7 | 5~15 | 단기 EMA |
| `slow_n` | 20 | 15~30 | 장기 EMA |
| `direction_ema_len` | 200 | 100~300 | 방향 필터 EMA |
| `use_direction_ema` | False(v2) / True(v3) | bool | 200 EMA 필터 |

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| 4h | long_only | 17.50% | 0.572 | -63.25% | 228 | ❌ FAIL |
| 4h | bidirectional | -14.03% | 0.014 | -67.92% | 246 | ❌ FAIL |
| 1D | long_only | 19.75% | 0.611 | -70.30% | 35 | ❌ FAIL |
| 1D | bidirectional | 38.65% | 0.870 | -69.75% | 35 | ❌ FAIL |
| 1h | long_only | -0.67% | 0.258 | -67.75% | 827 | ❌ FAIL |
| 1h | bidirectional | 1.83% | 0.313 | -70.68% | 881 | ❌ FAIL |

## 파라미터 스윕 이력

**v2 스윕 (16 combo)**: 전 조합 score=-999. MDD -54~-89% (PASS 기준 -35% 대폭 초과).  
**v3 스윕 (24 combo, use_direction_ema=True)**: 전 조합 score=-999. 200 EMA 필터 추가에도 MDD 구조 미개선.

> v2/v3 합산 40 combo 중 PASS 0건. Stochastic 평균 회귀 + ATR 스탑 구조가 BTC 추세장에서 MDD를 제어하지 못함.

## 아카이브 이유

MDD -60%+ 는 Stochastic 신호 구조에서 비롯된 것으로, 파라미터 조정만으로 해결 불가.  
복원 조건: 진입 로직 재설계 (예: 이중 TF 필터, 변동성 레짐 필터) 후 재검증.

## 관련 문서

- 백테스트 결과: `backtest/results/7-strategies/stoch/`
- 스윕 원본 데이터: `backtest/results/param_sweep/v2/stoch/`, `backtest/results/param_sweep/v3/stoch/`
