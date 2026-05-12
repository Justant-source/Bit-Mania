---
strategy: MomentumMAStrategy
verdict: FAIL
created: 2026-05-10
---

# MomentumMAStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 3.12% | ≥ +5% | ✗ |
| Sharpe | 0.463 | ≥ 0.5 | ✗ |
| MDD | -69.63% | ≥ -30% | ✗ |
| Trades | 54 | ≥ 30 | ✓ |
| Win Rate | 42.6% | ≥ 35% | ✓ |
| Profit Factor | 1.013 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-04-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: CAGR ≥ 5%, Sharpe ≥ 0.5, MDD ≥ -30%, ProfitFactor ≥ 1.2
