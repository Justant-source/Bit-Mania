---
strategy: TrendTypeStrategy
verdict: PASS
created: 2026-05-12
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 28.55% | ≥ +5% | ✓ |
| Sharpe | 1.099 | ≥ 0.5 | ✓ |
| MDD | -27.07% | ≥ -30% | ✓ |
| Trades | 88 | ≥ 30 | ✓ |
| Win Rate | 55.7% | ≥ 35% | ✓ |
| Profit Factor | 2.018 | ≥ 1.2 | ✓ |

## 종합 판정: **PASS**

## 관찰 사항
- 백테스트 기간: 2017-08-18 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] walk-forward 검증 후 paper trading 검토
- [ ] fa80_lev5_r30 기존 전략과 상관관계 분석 필요
