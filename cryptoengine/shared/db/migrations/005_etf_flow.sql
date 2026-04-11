-- Migration 005: ETF Flow & Macro Events Tables
-- Purpose: Store BTC ETF flow data and macro event calendar for momentum strategies

CREATE TABLE IF NOT EXISTS etf_flow_history (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    total_flow_usd DECIMAL(20,2) NOT NULL,
    ibit_flow_usd DECIMAL(20,2),
    fbtc_flow_usd DECIMAL(20,2),
    other_flow_usd DECIMAL(20,2),
    cumulative_flow_usd DECIMAL(20,2),
    source VARCHAR(20) NOT NULL,
    collected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etf_flow_date ON etf_flow_history(date DESC);
CREATE INDEX IF NOT EXISTS idx_etf_flow_source ON etf_flow_history(source);

CREATE TABLE IF NOT EXISTS macro_events (
    id BIGSERIAL PRIMARY KEY,
    event_date DATE NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    impact_level INTEGER DEFAULT 3,
    UNIQUE(event_date, event_type)
);

CREATE INDEX IF NOT EXISTS idx_macro_events_date ON macro_events(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_macro_events_type ON macro_events(event_type);
