# Jesse Strategies — CryptoEngine V5

## Strategy Index

| File | Class | Phase | Status |
|------|-------|-------|--------|
| `sanity_check.py` | BtcBuyAndHold | 7.2 | Validation tool |
| `intraday_seasonality.py` | IntradaySeasonality | 8.1 | Pending backtest |
| `macro_event.py` | MacroEvent | 8.2 | Pending backtest |
| `contrarian_sentiment.py` | ContrarianSentimentStandalone | 8.3A | Pending backtest |
| `contrarian_sentiment.py` | SeasonalityWithFGFilter | 8.3B | Pending (requires 8.1 PASS) |

## V5 Pass Criteria (all strategies)

| Criterion | Threshold |
|-----------|-----------|
| CAGR | ≥ 10% |
| Sharpe | ≥ 1.0 |
| MDD | ≤ 15% |
| WF OOS/IS Sharpe ratio | ≥ 0.6 |
| Monte Carlo 5th percentile Sharpe | > 0 |
| Trades per year | ≥ 30 |
| Sanity check CRITICALs | 0 |

## IntradaySeasonality — Design Notes

**Entry logic**: Long at UTC 21:00 (configurable 19-22h)
**Exit logic**: Hard time exit at UTC 23:00 (configurable 22-24h)
**Filters**:
- Trend: price > 50-day SMA (optional)
- Day-of-week: Thursday/Friday only (optional, Quantpedia finding)
**Stop loss**: 2× ATR below entry price
**Leverage**: 2x (max 3x per V5 rules)

### Key validation questions:
1. Does the 21h UTC pattern persist in 2024-2026?
2. Does the DOW filter (Thu/Fri) improve or hurt in compressed market?
3. What happens to trade count in compressed 2025-2026 regime?

## Running backtests

```bash
# Single strategy full validation
./scripts/run_full_validation.sh IntradaySeasonality

# Manual Jesse backtest
docker compose exec jesse jesse backtest \
  '2023-04-01' '2026-04-01' \
  --route 'Bybit Perpetual:BTCUSDT:1h:IntradaySeasonality'

# 2024-2026 sub-period (pattern persistence check)
docker compose exec jesse jesse backtest \
  '2024-01-01' '2026-04-01' \
  --route 'Bybit Perpetual:BTCUSDT:1h:IntradaySeasonality'
```
