-- Migration 013: Multi-exchange funding rates and OHLCV
-- Track C Phase C2: Binance and OKX data collection

CREATE TABLE IF NOT EXISTS multi_exchange_funding (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    funding_rate NUMERIC(10, 8) NOT NULL,
    mark_price NUMERIC(20, 8),
    next_funding_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mef_unique ON multi_exchange_funding (exchange, symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_mef_time ON multi_exchange_funding (timestamp DESC);


CREATE TABLE IF NOT EXISTS multi_exchange_ohlcv (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 8),
    high NUMERIC(20, 8),
    low NUMERIC(20, 8),
    close NUMERIC(20, 8),
    volume NUMERIC(20, 8),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_meo_unique ON multi_exchange_ohlcv (exchange, symbol, interval, timestamp);
