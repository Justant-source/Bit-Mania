-- Migration 012: Quarterly perpetual spread tracking
-- Track C Phase C1: quarterly_perp_spread table for spread analysis

CREATE TABLE IF NOT EXISTS quarterly_perp_spread (
    id BIGSERIAL PRIMARY KEY,
    quarterly_symbol VARCHAR(20) NOT NULL,
    perp_symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    spread NUMERIC(10, 8) NOT NULL,
    quarterly_price NUMERIC(20, 8) NOT NULL,
    perp_price NUMERIC(20, 8) NOT NULL,
    annualized_basis NUMERIC(10, 6),
    days_to_expiry INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qps_symbol_time ON quarterly_perp_spread (quarterly_symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_qps_time ON quarterly_perp_spread (timestamp DESC);

-- Required by collector.py ON CONFLICT (quarterly_symbol, perp_symbol, timestamp) DO NOTHING
ALTER TABLE quarterly_perp_spread
    ADD CONSTRAINT IF NOT EXISTS quarterly_perp_spread_symbol_ts_key
    UNIQUE (quarterly_symbol, perp_symbol, timestamp);
