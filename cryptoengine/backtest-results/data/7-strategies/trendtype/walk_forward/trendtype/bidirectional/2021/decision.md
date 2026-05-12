---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-09
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 66.98% | ≥ +5% | ✓ |
| Sharpe | 0.996 | ≥ 0.5 | ✓ |
| MDD | -20.27% | ≥ -30% | ✓ |
| Trades | 10 | ≥ 30 | ✗ |
| Win Rate | 60.0% | ≥ 35% | ✓ |
| Profit Factor | 1.596 | ≥ 1.2 | ✓ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-01-01 ~ 2021-12-31 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: Trades ≥ 30
