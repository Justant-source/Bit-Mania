-- Migration 006: Multi-symbol Open Interest tracking
-- For future multi-symbol funding rotation backtest

CREATE TABLE IF NOT EXISTS open_interest_history (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    oi_usd DECIMAL(20,2) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_oi_lookup ON open_interest_history(exchange, symbol, timestamp);
