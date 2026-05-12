---
strategy: SupertrendStrategy
verdict: PASS
created: 2026-05-12
---

# SupertrendStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 28.09% | ≥ +5% | ✓ |
| Sharpe | 0.947 | ≥ 0.5 | ✓ |
| MDD | -23.96% | ≥ -30% | ✓ |
| Trades | 40 | ≥ 30 | ✓ |
| Win Rate | 45.0% | ≥ 35% | ✓ |
| Profit Factor | 2.056 | ≥ 1.2 | ✓ |

## 종합 판정: **PASS**

## 관찰 사항
- 백테스트 기간: 2021-01-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] walk-forward 검증 후 paper trading 검토
- [ ] fa80_lev5_r30 기존 전략과 상관관계 분석 필요
