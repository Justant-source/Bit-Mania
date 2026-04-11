-- Migration 011: Long-term 1-minute OHLCV storage
-- Separate table from the regular ohlcv_history which has 30-day retention
-- Stores 3+ years of 1m candlestick data for feature engineering

CREATE TABLE IF NOT EXISTS ohlcv_1m_longterm (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL DEFAULT 'bybit',
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL(20,2),
    high DECIMAL(20,2),
    low DECIMAL(20,2),
    close DECIMAL(20,2) NOT NULL,
    volume DECIMAL(20,8),
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_1m_lt_symbol_time ON ohlcv_1m_longterm(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_1m_lt_time ON ohlcv_1m_longterm(timestamp DESC);
