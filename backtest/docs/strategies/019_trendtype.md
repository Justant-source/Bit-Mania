---
status: candidate-2026-05-12
last_updated: 2026-05-18
---

# 018 — TrendType 전략 (ADX/DMI 추세 분류)

코드: `backtest/strategies/external/TrendTypeStrategy.py`  
현재 상태: **CANDIDATE** — 1D long_only PASS.

## 개요

**ADX + DMI(Directional Movement Index)** 기반 추세 강도 분류 전략. 강한 상승 추세(`TrendType = 2.0`)일 때만 진입하여 횡보장을 선택적으로 회피. 1D long_only에서 CAGR 21.54%, Sharpe 0.919, MDD -27.54% 달성.

## 알고리즘

### 추세 분류

```python
def _trend_type() -> float:
    """
    IncrementalTrendType: ADX + DMI 기반
    반환값:
       2.0  = 강한 상승 추세 → 진입
      -2.0  = 강한 하강 추세 → 숏 (bidirectional)
       0.0  = 횡보장       → 미진입
    """
    return self._tt.update(candles)
```

### 진입/청산 조건

```python
def should_long() -> bool:
    return _trend_type() == 2.0       # 강한 상승 추세만 진입

def update_position():
    if _trend_type() != 2.0:          # 추세 약화 즉시 청산
        liquidate()
    atr_stop = atr(atr_len) * atr_mult
    if price <= entry - atr_stop or price >= entry + atr_stop:
        liquidate()
```

## 파라미터 (채택 설정 — v2 champion)

| 파라미터 | 값 | 설명 |
|---------|-----|------|
| `atr_len` | **10** | ATR 기간 (v2 champion) |
| `atr_ma_len` | 20 | ATR 평활 기간 |
| `di_len` | **10** | DI(방향 지수) 기간 (v2 champion) |
| `adx_len` | 10 | ADX 기간 (di_len 동기화) |
| `smooth` | 1 | DMI 평활 |
| `atr_mult` | 3.0 | ATR 손절/익절 배수 |

> v3 스윕에서 (10,10) champion 주변 세밀 탐색 수행.

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| **1D** | **long_only** | **21.54%** | **0.919** | **-27.54%** | 85 | **✅ PASS** |
| 1D | bidirectional | 24.82% | 0.784 | -33.22% | 161 | ❌ FAIL |
| 4h | long_only | 15.59% | 0.676 | -33.81% | 441 | ❌ FAIL |
| 4h | bidirectional | -1.99% | 0.154 | -72.95% | 807 | ❌ FAIL |
| 1h | long_only | 17.66% | 0.752 | -27.74% | 1380 | ❌ FAIL |
| 1h | bidirectional | 4.13% | 0.312 | -60.23% | 3026 | ❌ FAIL |

> 4h/1D long_only에서 MDD가 비교적 낮음. CAGR이 낮아 메인 전략으로는 부적합하나,  
> 다른 전략과 포트폴리오 결합 시 MDD 헷지 효과 기대 가능.

## 특징

- **선택성**: 강한 추세만 거래 → 횡보장 손실 최소
- **CAGR 한계**: 선택적 진입으로 수익 기회 제한 (CAGR 21%)
- **1h 특이점**: 1h long_only에서 거래수 1380건에도 MDD -27.74% 유지 (우수)
  - 단 CAGR 17.66%로 ProfitFactor 게이트 미통과

## 파라미터 스윕 이력 (v2/v3, 2021-04~2026-04 기준)

| TF | 방향 | Ver | `atr_len` | `di_len` | Score | P1 CAGR | MDD | 비고 |
|---|---|---|---|---|---|---|---|---|
| 1D | long_only | v3 | 10 | **10** | 최적 탐색 중 | — | — | v3 champion |
| 1D | long_only | v2 | **14** | **18** | — | +25.8% | -24.8% | ✅ 채택 |
| 4h | long_only | v3 | 10 | 10 | — | — | — | 4h best |

> v2 champion: atr_len=14, di_len=18 (baseline di=14 대비 CAGR +1.6pp, MDD 개선).  
> Walk-forward 최적화(`wf_optimize_skopt.py`): IS 2021~2022, OOS 2023~2025, n_calls=40.

## 복원 조건

- Walk-forward 검증에서 2023~2026 구간 안정성 확인 시 포트폴리오 편입 후보
- Supertrend 하이브리드(`019_supertrend_trendtype.md`)로 결합 시 신뢰도 향상 가능

## 관련 문서

- 백테스트 결과 경로: `backtest/results/7-strategies/trendtype/`
- 알고리즘 상세: `.result/02_STRATEGIES.md` §5
- 하이브리드 버전: `019_supertrend_trendtype.md`
