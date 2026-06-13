---
strategy: SupertrendTrendTypeStrategy
verdict: FAIL
created: 2026-05-27
---

# SupertrendTrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 16.78% | ≥ +5% | ✓ |
| Sharpe | 0.672 | ≥ 0.5 | ✓ |
| MDD | -45.84% | ≥ -30% | ✗ |
| Trades | 1096 | ≥ 30 | ✓ |
| Win Rate | 39.3% | ≥ 35% | ✓ |
| Profit Factor | 1.073 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2017-08-18 ~ 2026-05-28 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: MDD ≥ -30%, ProfitFactor ≥ 1.2
