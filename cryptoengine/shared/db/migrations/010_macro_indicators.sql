-- Migration 010: Macro indicators (DXY, VIX, US10Y) for backtesting
-- Stores daily macro data from FRED API

CREATE TABLE IF NOT EXISTS macro_indicators (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    indicator VARCHAR(30) NOT NULL,
    value DECIMAL(20,6),
    source VARCHAR(20) DEFAULT 'fred',
    collected_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(date, indicator)
);

CREATE INDEX IF NOT EXISTS idx_macro_date ON macro_indicators(date DESC, indicator);
CREATE INDEX IF NOT EXISTS idx_macro_indicator ON macro_indicators(indicator, date DESC);
