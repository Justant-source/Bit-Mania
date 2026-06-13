---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-27
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | -15.66% | ≥ +5% | ✗ |
| Sharpe | -0.076 | ≥ 0.5 | ✗ |
| MDD | -89.89% | ≥ -30% | ✗ |
| Trades | 5160 | ≥ 30 | ✓ |
| Win Rate | 37.1% | ≥ 35% | ✓ |
| Profit Factor | 0.979 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2017-08-18 ~ 2026-05-28 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: CAGR ≥ 5%, Sharpe ≥ 0.5, MDD ≥ -30%, ProfitFactor ≥ 1.2
