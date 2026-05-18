---
status: candidate-2026-05-17
last_updated: 2026-05-18
---

# 019 — Supertrend + TrendType 하이브리드 (5-Factor Confluence)

코드: `backtest/strategies/external/SupertrendTrendTypeStrategy.py`  
현재 상태: **CANDIDATE** — 4h long_only PASS, 1D long_only PASS.

## 개요

**TrendType (ADX/DMI) + Supertrend + EMA 교차 + 200 EMA 필터** 5가지 동시 확인. 7개 전략 중 가장 선택적인 진입 전략. 4h long_only CAGR 29.51%, Sharpe 0.997, MDD -30.41%.

## 알고리즘

### 진입 조건 (5 Factor AND)

```python
def should_long() -> bool:
    tt = _trend_type()
    if tt != 2.0:                     # ① TrendType: 강한 상승만
        return False
    return (
        price > supertrend_line        # ② ST 상승추세
        and fast_ema > slow_ema        # ③ EMA 골든크로스
        and price > direction_ema      # ④ 장기 EMA 위 (상승장)
    )
```

### 청산 조건

```python
def update_position():
    if fast_ema < slow_ema:           # EMA 데드크로스
        liquidate()
    atr_stop = atr(14) * atr_mult
    if price <= entry - atr_stop or price >= entry + atr_stop:
        liquidate()
```

## 파라미터 (채택 설정 — v2 champion)

| 파라미터 | 값 | 설명 |
|---------|-----|------|
| `atr_len` | **12** | TrendType ATR 기간 (v2 champion) |
| `atr_ma_len` | 20 | ATR 평활 |
| `di_len` | 14 | DI 기간 |
| `smooth` | 1 | DMI 평활 |
| `st_factor` | **2.5** | Supertrend ATR 배수 (v2 champion) |
| `st_period` | 7 | Supertrend ATR 기간 |
| `fast_ema_len` | 7 | 단기 EMA |
| `slow_ema_len` | 20 | 장기 EMA |
| `direction_ema_len` | 200 | 방향 필터 EMA |
| `atr_mult` | 3.0 | ATR 손절/익절 배수 |

> v3 스윕에서 (2.5, 12) champion 주변 세밀 탐색 수행.

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| **4h** | **long_only** | **29.51%** | **0.997** | **-30.41%** | 293 | **✅ PASS** |
| 4h | bidirectional | 23.23% | 0.681 | -43.62% | 564 | ❌ FAIL |
| **1D** | **long_only** | **30.07%** | **0.964** | **-39.33%** | 82 | **✅ PASS** |
| 1D | bidirectional | 38.71% | 0.937 | -56.52% | 136 | ✅ PASS |
| 1h | long_only | 20.75% | 0.769 | -58.33% | 1123 | ❌ FAIL |
| 1h | bidirectional | 14.46% | 0.518 | -74.55% | 2224 | ❌ FAIL |

> 4h와 1D 모두 PASS — 7개 전략 중 **가장 다양한 TF에서 PASS**.  
> Supertrend 단독(44.69%)보다 CAGR이 낮지만, MDD가 비슷하여 risk-adjusted return은 유사.

## Supertrend 단독 vs 하이브리드 비교 (4h long_only 1x)

| 항목 | Supertrend (#016) | Supertrend+TrendType (#019) | 차이 |
|-----|------------------|-----------------------------|------|
| CAGR | 44.69% | 29.51% | -15.18pp |
| Sharpe | 1.246 | 0.997 | -0.249 |
| MDD | -30.35% | -30.41% | ≈동일 |
| 거래수 | 398 | 293 | -105건 |

> TrendType 필터 추가로 거래수 감소(-26%), CAGR 감소(-15pp). MDD는 거의 동일.  
> 진입 선택성은 높아지나 전체 수익 감소. Supertrend 단독이 현 시점 우세.

## 파라미터 스윕 이력 (v2/v3, 2021-04~2026-04 기준)

| TF | 방향 | Ver | `st_factor` | `atr_len` | Score | P1 CAGR | MDD | 비고 |
|---|---|---|---|---|---|---|---|---|
| 4h | long_only | v2 | **2.0** | **10** | **+31.78** | +27.1% | -29.0% | ✅ 채택 |
| 1D | long_only | v2 | **2.5** | **12** | **+27.64** | +24.4% | -29.6% | ✅ 우수 |
| 1h | long_only | v2 | 5.0 | 14 | +13.61 | +10.8% | -31.3% | ⚠️ 조건부 |

> v2 4h champion(st_factor=2.0, atr_len=10): baseline(3.0/14) 대비 CAGR +4.3pp. 16-combo 탐색.  
> v3 4h bidirectional: 13 combo 전부 MDD > -35% 미충족으로 score=-999. 숏 필터 추가 불필요.  
> bidirectional 전 조합에서 MDD -39%+ → TrendType 필터가 숏 진입을 오히려 악화.

## 복원 조건

- 횡보장 지속 시 Supertrend 대비 낮은 손실로 방어 가능
- 포트폴리오 다각화 시 편입 후보 (낮은 MDD + 다중 TF PASS)

## 관련 문서

- 백테스트 결과 경로: `backtest/results/7-strategies/supertrend_trendtype/`
- 알고리즘 상세: `.result/02_STRATEGIES.md` §6
- TrendType 단독: `018_trendtype.md`
- Supertrend 단독: `016_supertrend.md`
