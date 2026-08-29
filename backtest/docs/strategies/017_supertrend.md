---
status: active-2026-05-18
last_updated: 2026-08-20
---

# 016 — Supertrend 전략 (Triple Confirmation)

코드: `backtest/strategies/external/SupertrendStrategy.py`  
운영 코드: `cryptoengine/services/strategies/supertrend/`  
현재 상태: **ACTIVE** — Phase 5 메인넷 실전 (combo #7908, 3x)

## 개요

**Supertrend (ATR 기반 추세선) + EMA 교차 + 200 EMA 방향 필터** 3중 확인 추세 추종 전략. 3개 조건이 모두 충족될 때만 롱 진입. 7개 백테스트 전략 중 4h long-only 1x 기준 **최고 성과** (CAGR 44.69%, Sharpe 1.246).

## 알고리즘

### 진입 조건 (AND)

```python
def should_long() -> bool:
    return (
        price > supertrend_line     # ① ST 상승추세 (가격이 ST선 위)
        and fast_ema > slow_ema     # ② EMA 골든크로스
        and price > direction_ema   # ③ 장기 EMA 위 (상승장 확인)
    )
```

### 청산 조건

```python
def update_position():
    if fast_ema < slow_ema:                   # EMA 데드크로스 → 즉시 청산
        liquidate()
    atr_stop = atr(14) * atr_mult
    if price <= entry - atr_stop:             # ATR 손절
        liquidate()
    # ATR 익절 없음 — 상승은 EMA 데드크로스까지 보유 (2026-08-20)
    # ATR 손절 후 1캔들 재진입 금지
```

## 파라미터

두 파라미터 세트가 동일 알고리즘을 사용. v3 스윕 챔피언(combo_18)에서 추가 최적화하여 combo #7908이 채택됨.

| 파라미터 | combo_18 (v3 챔피언) | combo #7908 (채택·live) | 설명 |
|---------|---------------------|------------------------|------|
| `st_factor` | 2.5 | **2.6** | Supertrend ATR 배수 |
| `st_period` | 6 | **9** | Supertrend ATR 기간 |
| `fast_ema_len` | 7 | **7** | 단기 EMA |
| `slow_ema_len` | 20 | **29** | 장기 EMA |
| `direction_ema_len` | 200 | **240** | 방향 필터 EMA |
| `atr_mult` | 3.0 | **3.3** | ATR 손절 배수 (익절 없음) |

> combo #7908은 Phase 5 파라미터 최적화 스윕(v7_st)에서 선정됨. 2026-08-20 정본(Bybit 네이티브 4h, ATR 손절만): CAGR +219.06%, Sharpe 1.667, MDD -66.70%, 198 trades. 이전 익절 포함 수치(137.64%/360)는 폐기.

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| **4h** | **long_only** | **44.69%** | **1.246** | **-30.35%** | 398 | **✅ PASS** |
| 4h | bidirectional | 33.15% | 0.815 | -38.42% | 749 | ❌ FAIL |
| 1D | long_only | 42.51% | 1.156 | -57.86% | 109 | ✅ PASS |
| 1D | bidirectional | 35.85% | 0.856 | -52.95% | 195 | ✅ PASS |
| 1h | long_only | -1.44% | 0.123 | -68.11% | 1580 | ❌ FAIL |
| 1h | bidirectional | -17.22% | -0.111 | -91.89% | 2990 | ❌ FAIL |

> 판정 기준: CAGR ≥ 5%, Sharpe ≥ 0.5, MDD ≥ -80%, Trades ≥ 30, WinRate ≥ 35%, ProfitFactor ≥ 1.2

## 3x 레버리지 결과 (4h long_only, 2017-08~2026-04)

| 파라미터 세트 | CAGR | Sharpe | MDD | 거래수 | 판정 | 비고 |
|---|---|---|---|---|---|---|
| combo_18 (v3 챔피언) | 110.44% | 1.224 | -73.43% | 398 | ❌ FAIL (PF<1.2) | ProfitFactor 1.14 |
| combo #164 | 115.79% | 1.252 | -86.32% | 379 | ❌ FAIL | |
| combo #173 | 128.93% | 1.304 | -86.94% | 378 | ❌ FAIL | 이전 live (v4 스윕) |
| combo #176 | 123.42% | 1.282 | -86.87% | 367 | ❌ FAIL | |
| **combo #7908** | **219.06%** | **1.667** | **-66.70%** | 198 | **✅ PASS (Bybit 네이티브, ATR 손절만)** | **현재 live (2026-08-20)** |

> Bybit 네이티브 4h (2026-08-20, ATR 손절만): CAGR 219.06%, Sharpe 1.667, MDD -66.70%, 198 trades.  
> 이전 익절 포함 정본(2026-06-14): 137.64% / 1.349 / -73.29% / 360 trades.

## 통합 경위

구 파일 기반 결과 트리(`backtest/results/7-strategies/supertrend/`)의 combo_18 / combo #7908은
동일 알고리즘의 파라미터 변형이었고, 이 문서 하나로 통합 관리한다.
해당 트리는 2026-08-29 레거시 정리에서 삭제되었다 (Binance 기준·pre-Bybit-native 수치라 현행 정본과 상충).
복구 필요 시 git 태그 `legacy-archive-2026-08-29` (commit 8d6f1b79) 참조 — ADR-0009.

## 파라미터 스윕 이력 (v2/v3, 2021-04~2026-04 기준)

> 점수식: mean(P1~P4 CAGR), ALL MDD≥-35% AND trades≥5 조건. P1=2021~2026 / P2=2022~2026 / P3=2021~2025 / P4=2022~2025

| TF | 방향 | Ver | `st_factor` | `st_period` | Score | P1 CAGR | MDD | 비고 |
|---|---|---|---|---|---|---|---|---|
| 4h | long_only | v3 | **2.5** | **12** | **+38.92** | +26.3% | -28.1% | ✅ 최우선 |
| 4h | long_only | v2 | 2.5 | 7 | +38.39 | +26.7% | -26.4% | v2 champion |
| 1D | long_only | v3 | **2.8** | **7** | **+31.71** | +23.9% | -24.0% | ✅ 우수 |
| 1h | long_only | v2 | 5.0 | 7 | +10.66 | +4.6% | -33.1% | ⚠️ 조건부 |

> v3 4h champion(2.5/12)에서 추가 최적화 진행 → combo #7908 (st_factor=2.6, st_period=9) 채택.

## 관련 문서

- 운영 사양 (live): `docs/70-policy/strategy.md`
- 최적화 판정: `.result/15_SUPERTREND_4H_3X_STRATEGY_SPEC.md`
- 파라미터 스윕: `.result/16_SUPERTREND_OPTIMIZATION_VERDICT.md`
- 현행 sweep 결과: `backtest-postgres` / `jesse_db` (`st_sweeps`·`st_combos`·`st_window_results`)
- sweep 서술 문서: `backtest/results/supertrend_x3_long_only/docs/sweeps/`
- 폐기된 구 결과 트리: ADR-0009 참조
