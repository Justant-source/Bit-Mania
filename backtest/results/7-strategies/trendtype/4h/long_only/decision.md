---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-27
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 17.75% | ≥ +5% | ✓ |
| Sharpe | 0.731 | ≥ 0.5 | ✓ |
| MDD | -45.09% | ≥ -30% | ✗ |
| Trades | 678 | ≥ 30 | ✓ |
| Win Rate | 43.8% | ≥ 35% | ✓ |
| Profit Factor | 1.143 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2017-08-18 ~ 2026-05-28 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: MDD ≥ -30%, ProfitFactor ≥ 1.2
