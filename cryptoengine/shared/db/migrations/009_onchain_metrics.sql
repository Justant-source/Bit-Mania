-- Migration 009: 온체인 메트릭 + 공포탐욕 지수 테이블
-- BT_TASK_08 온체인 매크로 복합 전략용

CREATE TABLE IF NOT EXISTS onchain_metrics (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(10) NOT NULL DEFAULT 'BTC',
    date DATE NOT NULL,
    price_usd DECIMAL(20,2),
    market_cap_usd DECIMAL(30,2),
    realized_cap_usd DECIMAL(30,2),
    mvrv DECIMAL(10,4),
    mvrv_zscore DECIMAL(10,4),
    asopr DECIMAL(10,4),
    exchange_netflow_usd DECIMAL(20,2),
    exchange_balance_btc DECIMAL(20,8),
    active_supply_180d DECIMAL(20,8),
    source VARCHAR(20) NOT NULL DEFAULT 'coinmetrics',
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(asset, date)
);

CREATE INDEX IF NOT EXISTS idx_onchain_date ON onchain_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_onchain_asset_date ON onchain_metrics(asset, date);

CREATE TABLE IF NOT EXISTS fear_greed_history (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    value INTEGER NOT NULL,
    classification VARCHAR(30),
    collected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fg_date ON fear_greed_history(date DESC);
