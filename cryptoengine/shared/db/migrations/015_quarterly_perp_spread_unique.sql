-- Migration 015: Add unique constraint to quarterly_perp_spread
-- Fixes ON CONFLICT (quarterly_symbol, perp_symbol, timestamp) DO NOTHING in collector.py

ALTER TABLE quarterly_perp_spread
    ADD CONSTRAINT IF NOT EXISTS quarterly_perp_spread_symbol_ts_key
    UNIQUE (quarterly_symbol, perp_symbol, timestamp);
