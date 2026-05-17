---
strategy: SupertrendStrategy
verdict: FAIL
created: 2026-05-17
---

# SupertrendStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 12.13% | ≥ +5% | ✓ |
| Sharpe | 0.584 | ≥ 0.5 | ✓ |
| MDD | -86.48% | ≥ -80% | ✗ |
| Trades | 387 | ≥ 30 | ✓ |
| Win Rate | 39.0% | ≥ 35% | ✓ |
| Profit Factor | 1.019 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-01-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)
- 데이터: 실제 Binance 1m OHLC (intrabar wick 손절 포함)

## v2.0 파라미터 (스윕 최적화)
| 파라미터 | 값 | 근거 |
|---|---|---|
| st_factor | 2.5 | supertrend_x3_long_only sweep 최적 |
| st_period | 9 | |
| fast_ema | 8 | |
| slow_ema | 25 | |
| dir_ema | 230 | |
| atr_mult | 3.2 | |

**스윕 지표**: sweet_spot_score 92.6 / PLATEAU / tier_pass=1 (8 rolling windows)
**스윕 mean_CAGR**: 181.2% (롤링 8개 창 평균) — 전체 기간 단일 백테스트와 다를 수 있음

## CryptoEngine 적용 권고
- 전체 기간(2021-2026) MDD -86.5%는 3x 레버리지의 한계. 롤링 창 기준은 worst_mdd_recent -53.3%
- 운영 결정: 스윕 sweet_spot_score·PLATEAU 기준 운영 후보, 실제 배포 전 forward test 필요
