---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-09
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 20.14% | ≥ +5% | ✓ |
| Sharpe | 0.634 | ≥ 0.5 | ✓ |
| MDD | -17.28% | ≥ -30% | ✓ |
| Trades | 15 | ≥ 30 | ✗ |
| Win Rate | 53.3% | ≥ 35% | ✓ |
| Profit Factor | 1.582 | ≥ 1.2 | ✓ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2023-01-01 ~ 2023-12-31 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: Trades ≥ 30
