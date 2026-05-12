---
strategy: TrendTypeStrategy
verdict: FAIL
created: 2026-05-09
---

# TrendTypeStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 0.00% | ≥ +5% | ✗ |
| Sharpe | 0.000 | ≥ 0.5 | ✗ |
| MDD | 0.00% | ≥ -30% | ✓ |
| Trades | 0 | ≥ 30 | ✗ |
| Win Rate | 0.0% | ≥ 35% | ✗ |
| Profit Factor | 0.000 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2022-01-01 ~ 2022-12-31 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: CAGR ≥ 5%, Sharpe ≥ 0.5, Trades ≥ 30, WinRate ≥ 35%, ProfitFactor ≥ 1.2
