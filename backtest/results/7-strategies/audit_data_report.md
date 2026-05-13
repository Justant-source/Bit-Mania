# V4 백테스트 데이터 신뢰성 검증 리포트

**생성 시각**: 2026-05-13T03:52:01.903463+00:00
**스크립트**: `audit_v4_data.py`

## 요약

| 카테고리 | 검증 건수 | 실패 | 경고 | 결과 |
|---------|----------|------|------|------|
| 0_load_basic | 67 | 0 | 0 | ✅ PASS |
| 1A_per_trade_pnl | 30071 | 0 | 0 | ✅ PASS |
| 1B_finishing_eq_starting_plus_net | 67 | 0 | 0 | ✅ PASS |
| 1B_gross_loss | 67 | 0 | 0 | ✅ PASS |
| 1B_gross_profit | 67 | 0 | 0 | ✅ PASS |
| 1B_losing_count | 67 | 0 | 0 | ✅ PASS |
| 1B_net_profit_pct | 67 | 0 | 0 | ✅ PASS |
| 1B_profit_factor | 24 | 0 | 0 | ✅ PASS |
| 1B_total_trades | 67 | 0 | 0 | ✅ PASS |
| 1B_trades_vs_monthly | 67 | 0 | 0 | ✅ PASS |
| 1B_trades_vs_net_profit | 67 | 0 | 0 | ✅ PASS |
| 1B_winning_count | 67 | 0 | 0 | ✅ PASS |
| 1C_low_trade_count | 67 | 0 | 9 | ⚠️ WARN |
| 1C_marker_missing | 67 | 0 | 0 | ✅ PASS |
| 1C_month_format | 3210 | 0 | 0 | ✅ PASS |
| 1C_trade_date_bounds | 30071 | 0 | 0 | ✅ PASS |
| 1D_lev_no_trades | 24 | 0 | 0 | ✅ PASS |
| 1D_lev_orphan | 24 | 0 | 0 | ✅ PASS |
| 1E_btc_data_missing | 1 | 0 | 1 | ⚠️ WARN |
| 1G_maker_fee | 30071 | 0 | 0 | ✅ PASS |
| 1H_sanity_critical1_nan | 67 | 0 | 0 | ✅ PASS |
| 1H_sanity_critical2_lookahead | 67 | 0 | 0 | ✅ PASS |
| 1H_sanity_warn1_overfee | 67 | 0 | 0 | ✅ PASS |

## ⚠️ 1C_low_trade_count — 9 warnings

- `label`=stoch/1h/bidirectional_x2, `trades`=22
- `label`=stoch/1h/bidirectional_x3, `trades`=23
- `label`=tradeiq_psar_ha/1h/long_only_x2, `trades`=26
- `label`=tradeiq_psar_ha/1h/long_only_x3, `trades`=26
- `label`=trendtype/1h/long_only_x2, `trades`=28
- `label`=trendtype/1h/long_only_x3, `trades`=28
- `label`=buy_and_hold/1D/buy_and_hold, `trades`=1
- `label`=tradeiq_cci_ce/1D/bidirectional, `trades`=12
- `label`=tradeiq_cci_ce/1D/long_only, `trades`=5

## ⚠️ 1E_btc_data_missing — 1 warnings

- 

