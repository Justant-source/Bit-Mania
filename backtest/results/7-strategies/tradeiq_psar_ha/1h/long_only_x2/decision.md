---
strategy: TradeIQPsarHaStrategy
verdict: FAIL
created: 2026-05-10
---

# TradeIQPsarHaStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 31.33% | ≥ +5% | ✓ |
| Sharpe | 0.719 | ≥ 0.5 | ✓ |
| MDD | -27.66% | ≥ -30% | ✓ |
| Trades | 26 | ≥ 30 | ✗ |
| Win Rate | 46.2% | ≥ 35% | ✓ |
| Profit Factor | 1.877 | ≥ 1.2 | ✓ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-04-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: Trades ≥ 30
