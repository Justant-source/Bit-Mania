-- Migration 007: 분기물 선물 히스토리 테이블
-- BT_TASK_06 캘린더 스프레드 전략용

CREATE TABLE IF NOT EXISTS quarterly_futures_history (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(30) NOT NULL,
    underlying VARCHAR(20) NOT NULL,
    expiry_date DATE NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL(20,2),
    high DECIMAL(20,2),
    low DECIMAL(20,2),
    close DECIMAL(20,2) NOT NULL,
    volume DECIMAL(20,8),
    open_interest DECIMAL(20,2),
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_qf_symbol_time ON quarterly_futures_history(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_qf_expiry ON quarterly_futures_history(expiry_date);
CREATE INDEX IF NOT EXISTS idx_qf_underlying ON quarterly_futures_history(underlying, expiry_date);
