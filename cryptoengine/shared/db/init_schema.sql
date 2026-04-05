-- ============================================================
-- CryptoEngine — PostgreSQL Initial Schema
-- 비트코인 선물 자동매매 시스템 데이터베이스
-- ============================================================

-- ──────────────── trades ────────────────
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(10) NOT NULL,      -- buy, sell
    order_type      VARCHAR(10) NOT NULL,      -- limit, market
    quantity        DECIMAL(20, 8) NOT NULL,
    price           DECIMAL(20, 2) NOT NULL,
    fee             DECIMAL(20, 8),
    fee_currency    VARCHAR(10),
    pnl             DECIMAL(20, 8),            -- 실현 손익 (청산 시)
    order_id        VARCHAR(100),
    request_id      VARCHAR(100) UNIQUE,
    status          VARCHAR(20) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    filled_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy_id, created_at);
CREATE INDEX IF NOT EXISTS idx_trades_filled ON trades(filled_at);
CREATE INDEX IF NOT EXISTS idx_trades_request_id ON trades(request_id);

-- ──────────────── positions ────────────────
CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(10) NOT NULL,
    size            DECIMAL(20, 8) NOT NULL,
    entry_price     DECIMAL(20, 2) NOT NULL,
    current_price   DECIMAL(20, 2),
    unrealized_pnl  DECIMAL(20, 8),
    leverage        DECIMAL(5, 2) DEFAULT 1.0,
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    close_reason    VARCHAR(50)                -- signal, stop_loss, kill_switch
);

CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy_id, opened_at);
CREATE INDEX IF NOT EXISTS idx_positions_open ON positions(strategy_id) WHERE closed_at IS NULL;

-- ──────────────── funding_payments ────────────────
CREATE TABLE IF NOT EXISTS funding_payments (
    id              BIGSERIAL PRIMARY KEY,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    funding_rate    DECIMAL(10, 6) NOT NULL,
    payment         DECIMAL(20, 8) NOT NULL,   -- 수취한 펀딩비 (USDT)
    position_size   DECIMAL(20, 8) NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_funding_collected ON funding_payments(collected_at);
CREATE INDEX IF NOT EXISTS idx_funding_exchange_symbol ON funding_payments(exchange, symbol, collected_at);

-- ──────────────── portfolio_snapshots ────────────────
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    total_equity        DOUBLE PRECISION NOT NULL,
    unrealized_pnl      DOUBLE PRECISION DEFAULT 0,
    realized_pnl_today  DOUBLE PRECISION DEFAULT 0,
    daily_drawdown      DOUBLE PRECISION DEFAULT 0,
    weekly_drawdown     DOUBLE PRECISION DEFAULT 0,
    monthly_drawdown    DOUBLE PRECISION DEFAULT 0,
    sharpe_ratio_30d    DOUBLE PRECISION,
    strategies          JSONB DEFAULT '[]',
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_time ON portfolio_snapshots(snapshot_at);

-- ──────────────── market_regime_history ────────────────
CREATE TABLE IF NOT EXISTS market_regime_history (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20) NOT NULL DEFAULT 'BTCUSDT',
    regime      VARCHAR(20) NOT NULL,  -- 'trending', 'ranging', 'volatile'
    confidence  DECIMAL(5, 3),
    indicators  JSONB,                 -- {"atr_ratio": 0.02, "adx": 28.5, "bb_width": 0.15}
    detected_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_regime_history_time ON market_regime_history(detected_at);
CREATE INDEX IF NOT EXISTS idx_regime_history_symbol ON market_regime_history(symbol, detected_at);

-- ──────────────── daily_reports ────────────────
CREATE TABLE IF NOT EXISTS daily_reports (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE UNIQUE NOT NULL,
    starting_equity DECIMAL(20, 2),
    ending_equity   DECIMAL(20, 2),
    daily_pnl       DECIMAL(20, 8),
    daily_return    DECIMAL(10, 6),            -- 일일 수익률 %
    trade_count     INTEGER,
    funding_income  DECIMAL(20, 8),
    grid_income     DECIMAL(20, 8),
    dca_value       DECIMAL(20, 8),
    max_drawdown    DECIMAL(10, 6),
    llm_summary     TEXT                       -- LLM이 생성한 일일 요약
);

CREATE INDEX IF NOT EXISTS idx_daily_reports_date ON daily_reports(date);

-- ──────────────── strategy_states ────────────────
CREATE TABLE IF NOT EXISTS strategy_states (
    id                BIGSERIAL PRIMARY KEY,
    strategy_id       VARCHAR(50) UNIQUE NOT NULL,
    is_running        BOOLEAN DEFAULT FALSE,
    allocated_capital DECIMAL(20, 2),
    current_pnl       DECIMAL(20, 8),
    position_count    INTEGER DEFAULT 0,
    config_override   JSONB,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────── kill_switch_events ────────────────
CREATE TABLE IF NOT EXISTS kill_switch_events (
    id               BIGSERIAL PRIMARY KEY,
    level            INTEGER NOT NULL,          -- 1: strategy, 2: portfolio, 3: system, 4: manual
    reason           VARCHAR(200) NOT NULL,
    positions_closed INTEGER,
    pnl_at_trigger   DECIMAL(20, 8),
    details          JSONB,
    triggered_at     TIMESTAMPTZ DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_kill_switch_triggered ON kill_switch_events(triggered_at);

-- ──────────────── llm_judgments ────────────────
CREATE TABLE IF NOT EXISTS llm_judgments (
    id                BIGSERIAL PRIMARY KEY,
    rating            VARCHAR(20) NOT NULL,     -- strong_buy, buy, hold, sell, strong_sell
    confidence        DECIMAL(5, 3),
    regime            VARCHAR(20),
    reasoning         TEXT,
    weight_adjustment JSONB,
    bull_summary      TEXT,
    bear_summary      TEXT,
    risk_flags        JSONB,
    actual_outcome    VARCHAR(20),              -- 실제 결과 (회고 시 기록)
    accuracy_score    DECIMAL(5, 3),
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    evaluated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_llm_judgments_created ON llm_judgments(created_at);

-- ──────────────── ohlcv_history ────────────────
CREATE TABLE IF NOT EXISTS ohlcv_history (
    id          BIGSERIAL PRIMARY KEY,
    exchange    VARCHAR(20) NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    timeframe   VARCHAR(5) NOT NULL,
    open        DECIMAL(20, 2) NOT NULL,
    high        DECIMAL(20, 2) NOT NULL,
    low         DECIMAL(20, 2) NOT NULL,
    close       DECIMAL(20, 2) NOT NULL,
    volume      DECIMAL(20, 8) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_lookup
    ON ohlcv_history(exchange, symbol, timeframe, timestamp);

-- ──────────────── funding_rate_history ────────────────
CREATE TABLE IF NOT EXISTS funding_rate_history (
    id              BIGSERIAL PRIMARY KEY,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    rate            DECIMAL(10, 6) NOT NULL,
    predicted_rate  DECIMAL(10, 6),
    timestamp       TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_rate_lookup
    ON funding_rate_history(exchange, symbol, timestamp);

-- ──────────────── grid_orders ────────────────
CREATE TABLE IF NOT EXISTS grid_orders (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    grid_level      INTEGER NOT NULL,
    side            VARCHAR(10) NOT NULL,
    price           DECIMAL(20, 2) NOT NULL,
    quantity        DECIMAL(20, 8) NOT NULL,
    order_id        VARCHAR(100),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    filled_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_grid_orders_strategy ON grid_orders(strategy_id, status);

-- ──────────────── dca_purchases ────────────────
CREATE TABLE IF NOT EXISTS dca_purchases (
    id              BIGSERIAL PRIMARY KEY,
    fear_greed_index INTEGER NOT NULL,
    multiplier      DECIMAL(5, 2) NOT NULL,
    amount_usdt     DECIMAL(20, 2) NOT NULL,
    btc_quantity    DECIMAL(20, 8) NOT NULL,
    btc_price       DECIMAL(20, 2) NOT NULL,
    avg_cost_basis  DECIMAL(20, 2),
    total_btc_held  DECIMAL(20, 8),
    purchased_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dca_purchased ON dca_purchases(purchased_at);

-- ──────────────── llm_reports ────────────────
-- LLM 분석 리포트 전문 저장 (대시보드에서 리스트/상세 조회)
CREATE TABLE IF NOT EXISTS llm_reports (
    id                BIGSERIAL PRIMARY KEY,
    title             VARCHAR(200) NOT NULL,
    trigger           VARCHAR(30) NOT NULL DEFAULT 'scheduled',  -- scheduled, on_demand
    rating            VARCHAR(20) NOT NULL,                       -- strong_buy, buy, hold, sell, strong_sell
    confidence        DECIMAL(5, 3),
    regime            VARCHAR(20),
    symbol            VARCHAR(20) DEFAULT 'BTCUSDT',
    btc_price         DECIMAL(20, 2),
    -- 분석 요약 섹션
    technical_summary TEXT,
    sentiment_summary TEXT,
    bull_summary      TEXT,
    bear_summary      TEXT,
    debate_conclusion TEXT,
    risk_assessment   TEXT,
    reasoning         TEXT,
    -- 추천 액션
    weight_adjustments JSONB,
    risk_flags         JSONB,
    -- 6시간 자산 리포트 (한국어 서술형)
    asset_report       TEXT,
    -- 메타
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_reports_created ON llm_reports(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_reports_symbol ON llm_reports(symbol, created_at DESC);
