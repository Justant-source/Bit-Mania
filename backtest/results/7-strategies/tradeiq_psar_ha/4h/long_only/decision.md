---
strategy: TradeIQPsarHaStrategy
verdict: FAIL
created: 2026-05-12
---

# TradeIQPsarHaStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 12.94% | ≥ +5% | ✓ |
| Sharpe | 0.586 | ≥ 0.5 | ✓ |
| MDD | -29.96% | ≥ -30% | ✓ |
| Trades | 377 | ≥ 30 | ✓ |
| Win Rate | 36.6% | ≥ 35% | ✓ |
| Profit Factor | 1.147 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-01-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: ProfitFactor ≥ 1.2
