-- Migration 017: Drop ohlcv_1m_longterm table
-- Reason: Strategy is 4h-only, doesn't need 3+ years of 1m data in live postgres
-- Live system retains only rolling 7-day window of 1m OHLCV in ohlcv_history

DROP TABLE IF EXISTS ohlcv_1m_longterm CASCADE;

\echo '[migration 017] ohlcv_1m_longterm dropped'
