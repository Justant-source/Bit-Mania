---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-09
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 60.18% | ≥ +5% | ✓ |
| Sharpe | 0.767 | ≥ 0.5 | ✓ |
| MDD | -24.60% | ≥ -30% | ✓ |
| Trades | 11 | ≥ 30 | ✗ |
| Win Rate | 45.5% | ≥ 35% | ✓ |
| Profit Factor | 3.324 | ≥ 1.2 | ✓ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2020-01-01 ~ 2020-12-31 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: Trades ≥ 30
