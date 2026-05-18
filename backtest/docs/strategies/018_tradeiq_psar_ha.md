---
status: candidate-2026-05-12
last_updated: 2026-05-18
---

# 017 — TradeIQ PSAR-HA 전략 (4-Factor Confluence)

코드: `backtest/strategies/external/TradeIQPsarHaStrategy.py`  
현재 상태: **CANDIDATE** — 1D long_only PASS. 4h는 ProfitFactor 게이트 미통과.

## 개요

**Parabolic SAR + 200 EMA + RSI + Heikin Ashi** 4가지 지표 동시 확인 진입. 고신뢰도 진입을 위해 모든 조건이 충족될 때만 포지션 진입. 1D long_only에서 CAGR 22.43%, Sharpe 0.864, MDD -26.09% 달성.

## 알고리즘

### 진입 조건 (AND)

```python
def should_long() -> bool:
    return (
        parabolic_sar < price        # ① SAR 상향 (가격이 SAR 위)
        and price > ema(200)          # ② 장기 EMA 위 (추세 확인)
        and rsi(rsi_len) > 50         # ③ RSI 50 이상 (모멘텀)
        and ha_close > ha_open        # ④ Heikin Ashi 양봉
    )
```

### 청산 조건

```python
def update_position():
    # 4가지 조건 중 하나라도 위반 시 청산
    if price < psar or price < ema(200) or rsi < 50:
        liquidate()
    atr_stop = atr(14) * atr_mult
    if price <= entry - atr_stop or price >= entry + atr_stop:
        liquidate()
```

## 파라미터 (채택 설정 — v2 champion)

| 파라미터 | 값 | 설명 |
|---------|-----|------|
| `psar_start` | 0.02 | SAR 초기 가속도 |
| `psar_inc` | 0.02 | SAR 가속도 증가 |
| `psar_max` | 0.2 | SAR 최대 가속도 |
| `direction_ema_len` | 200 | 추세 필터 EMA |
| `rsi_len` | **18** | RSI 주기 (v2 champion) |
| `atr_mult` | **3.0** | ATR 손절/익절 배수 |

> v3 스윕에서 더 넓은 stops(atr_mult=5.0~6.0)로 2024 손실 회복 탐색 진행.  
> 고신뢰도 진입으로 거래 빈도가 낮아 1h 타임프레임에서는 성과 미흡.

## 백테스트 결과 (1x, 2017-08~2026-04)

| TF | 방향 | CAGR | Sharpe | MDD | 거래수 | 판정 |
|---|---|---|---|---|---|---|
| **1D** | **long_only** | **22.43%** | **0.864** | **-26.09%** | 86 | **✅ PASS** |
| 1D | bidirectional | 13.33% | 0.516 | -45.01% | 133 | ❌ FAIL |
| 4h | long_only | 12.94% | 0.586 | -29.96% | 377 | ❌ FAIL |
| 4h | bidirectional | -0.61% | 0.179 | -47.89% | 779 | ❌ FAIL |
| 1h | long_only | -1.22% | 0.096 | -47.97% | 1664 | ❌ FAIL |
| 1h | bidirectional | -19.90% | -0.343 | -75.94% | 3246 | ❌ FAIL |

> 4h long_only: MDD -29.96%는 통과하나 ProfitFactor 미달로 FAIL.  
> 1D long_only는 전체 7전략 중 MDD가 가장 낮은(-26.09%) 보수적 성과.

## 파라미터 스윕 이력 (v2/v3, 2021-04~2026-04 기준)

| TF | 방향 | Ver | `rsi_len` | `atr_mult` | Score | P1 CAGR | MDD | 비고 |
|---|---|---|---|---|---|---|---|---|
| 1D | long_only | v2 | **18** | **4.0** | **+22.12** | +16.4% | -27.4% | ✅ 최우선 |
| 4h | long_only | v2 | 21 | 4.0 | +11.67 | +8.6% | -32.1% | ⚠️ 조건부 |

> v2 champion: rsi_len=18 (기본값 14 대비 CAGR +5.5pp 향상). wider stops(atr_mult=4.0)이 1D에서 유효.  
> v3 스윕 (wider stops 5.0~6.0): 2024 손실 회복 탐색. 1D best 22.12 → 소폭 개선 확인.

## 채택 권고 조건

- 현재 시스템: Supertrend 4h 3x 운용 중 → 병행 투입 불필요
- 복원 조건: Supertrend 4h 이상 이탈 시 1D long_only 포트폴리오 편입 후보
- 추가 탐색: v3 wider stops (atr_mult=5.0~6.0)로 4h PASS 여부 재검증

## 관련 문서

- 백테스트 결과 경로: `backtest/results/7-strategies/tradeiq_psar_ha/`
- 알고리즘 상세: `.result/02_STRATEGIES.md` §4
