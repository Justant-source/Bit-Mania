-- Migration 008: 청산 히스토리 테이블
-- BT_TASK_07 청산 캐스케이드 전략용

CREATE TABLE IF NOT EXISTS liquidation_history (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    long_liquidations_usd DECIMAL(20,2),
    short_liquidations_usd DECIMAL(20,2),
    total_liquidations_usd DECIMAL(20,2),
    oi_change_pct DECIMAL(10,4),
    source VARCHAR(20) NOT NULL,
    UNIQUE(exchange, symbol, timestamp, source)
);

CREATE INDEX IF NOT EXISTS idx_liquidation_time ON liquidation_history(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_liquidation_total ON liquidation_history(total_liquidations_usd DESC);
CREATE INDEX IF NOT EXISTS idx_liquidation_symbol ON liquidation_history(symbol, timestamp);
