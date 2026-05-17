---
strategy: SupertrendStrategy
verdict: FAIL
created: 2026-05-17
---

# SupertrendStrategy 백테스트 결과

## 메트릭

| 항목 | 결과 | 기준 | 통과 |
|------|------|------|------|
| CAGR | 13.13% | ≥ +5% | ✓ |
| Sharpe | 0.544 | ≥ 0.5 | ✓ |
| MDD | -46.21% | ≥ -80% | ✓ |
| Trades | 387 | ≥ 30 | ✓ |
| Win Rate | 39.0% | ≥ 35% | ✓ |
| Profit Factor | 1.100 | ≥ 1.2 | ✗ |

## 종합 판정: **FAIL**

## 관찰 사항
- 백테스트 기간: 2021-01-01 ~ 2026-04-30 (6년)
- 기준: post-ETF 압축장 표준 (backtest-skillset.md)
- 데이터: 실제 Binance 1m OHLC (intrabar wick 손절 포함)

## CryptoEngine 적용 권고
- [ ] archived 처리 (폐기 round 1)
- [ ] 실패 항목: ProfitFactor ≥ 1.2
