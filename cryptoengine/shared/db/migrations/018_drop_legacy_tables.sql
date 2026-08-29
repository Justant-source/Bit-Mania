-- 018_drop_legacy_tables.sql — D2/D3 legacy table cleanup (ADR-0006)
--
-- DROP TABLE IF EXISTS only. Do not apply against live postgres from D4
-- (code-only). Live apply is a later operations step.
--
-- NEVER DROP (keep-list):
--   supertrend_signals, orders, service_logs, portfolio_snapshots,
--   ohlcv_history, positions, strategy_states, kill_switch_events,
--   daily_pnl, llm_judgments, llm_reports, funding_rate_history
--
-- Operational ``trades`` (init_schema fill log) is not a separate legacy
-- table; it is not dropped. SSOT candle/funding tables are ohlcv_history
-- and funding_rate_history.

-- D2 — quarterly / calendar-spread leftovers
DROP TABLE IF EXISTS quarterly_perp_spread;
DROP TABLE IF EXISTS quarterly_futures_history;

-- D3 — unused data tables
DROP TABLE IF EXISTS market_regime_history;
DROP TABLE IF EXISTS regime_raw_log;
DROP TABLE IF EXISTS regime_transitions;
DROP TABLE IF EXISTS macro_indicators;
DROP TABLE IF EXISTS onchain_metrics;
DROP TABLE IF EXISTS multi_exchange_ohlcv;
DROP TABLE IF EXISTS multi_exchange_funding;

-- D3 — empty shells
DROP TABLE IF EXISTS dca_purchases;
DROP TABLE IF EXISTS grid_orders;
DROP TABLE IF EXISTS market_regimes;
DROP TABLE IF EXISTS etf_flow_history;
DROP TABLE IF EXISTS etf_flow_results;
DROP TABLE IF EXISTS xgboost_ensemble_results;
DROP TABLE IF EXISTS calendar_spread_results;
DROP TABLE IF EXISTS volatility_squeeze_results;
DROP TABLE IF EXISTS funding_extreme_reversal_results;
DROP TABLE IF EXISTS regime_accuracy_results;
DROP TABLE IF EXISTS liquidation_history;
DROP TABLE IF EXISTS macro_events;
DROP TABLE IF EXISTS fear_greed_history;
DROP TABLE IF EXISTS strategy_variant_results;
DROP TABLE IF EXISTS weight_optimization_results;
DROP TABLE IF EXISTS walk_forward_results;
DROP TABLE IF EXISTS test12_results;
DROP TABLE IF EXISTS backtest_results;

-- Legacy names (not ohlcv_history / funding_rate_history)
DROP TABLE IF EXISTS ohlcv;
DROP TABLE IF EXISTS funding_rates;
