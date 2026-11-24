-- 016_supertrend_signals.sql — Supertrend per-bar signal log
--
-- Records what the strategy computed at each confirmed 4h candle close:
-- expected entry/exit decision, indicator values, and allocated capital.
-- Used by the dashboard to compare expected-vs-actual mainnet execution.

CREATE TABLE IF NOT EXISTS supertrend_signals (
    id                 BIGSERIAL PRIMARY KEY,
    bar_ts             TIMESTAMPTZ NOT NULL,        -- 4h candle open time (bar timestamp, UTC)
    computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    st_dir             SMALLINT    NOT NULL,         -- +1 uptrend, -1 downtrend, 0 undefined
    fast_ema           DOUBLE PRECISION NOT NULL,    -- EMA(7)
    slow_ema           DOUBLE PRECISION NOT NULL,    -- EMA(27)
    dir_ema            DOUBLE PRECISION NOT NULL,    -- EMA(230)
    price              DOUBLE PRECISION NOT NULL,    -- candle close price
    atr_14             DOUBLE PRECISION NOT NULL,    -- ATR(14)
    allocated_capital  DOUBLE PRECISION NOT NULL,    -- orchestrator allocation at this bar
    had_position       BOOLEAN NOT NULL,             -- position state BEFORE this bar's decision
    entry_ok           BOOLEAN NOT NULL,             -- all 3 entry conditions satisfied
    exit_signal        BOOLEAN NOT NULL,
    exit_reason        VARCHAR(20),                  -- 'ema_cross' | 'atr_distance' | NULL
    expected_action    VARCHAR(10) NOT NULL,         -- 'enter' | 'exit' | 'hold'
    expected_qty       DOUBLE PRECISION,             -- BTC qty when entering, NULL otherwise
    expected_stop_loss DOUBLE PRECISION              -- SL price when entering, NULL otherwise
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_supertrend_signals_bar_ts
    ON supertrend_signals (bar_ts);

CREATE INDEX IF NOT EXISTS idx_supertrend_signals_computed_at
    ON supertrend_signals (computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_supertrend_signals_action
    ON supertrend_signals (expected_action, bar_ts DESC);
