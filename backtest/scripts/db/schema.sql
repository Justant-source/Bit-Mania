-- PostgreSQL schema for strategy backtesting results
-- All CREATE statements use IF NOT EXISTS for idempotency

CREATE TABLE IF NOT EXISTS st_sweeps (
  sweep_id TEXT PRIMARY KEY,
  description TEXT,
  leverage NUMERIC NOT NULL DEFAULT 3,
  variant TEXT DEFAULT 'long_only',
  grid_json JSONB,
  n_combos INT,
  created_at TIMESTAMPTZ DEFAULT now(),
  source_csv TEXT
);

CREATE TABLE IF NOT EXISTS st_combos (
  pk BIGSERIAL PRIMARY KEY,
  sweep_id TEXT REFERENCES st_sweeps(sweep_id),
  combo_id INT NOT NULL,
  st_factor NUMERIC,
  st_period INT,
  fast_ema_len INT,
  slow_ema_len INT,
  direction_ema_len INT,
  atr_mult NUMERIC,
  sl_margin_pct NUMERIC DEFAULT 0,
  tp_atr_mult NUMERIC,
  sl_atr_mult NUMERIC,
  n_complete INT,
  n_positive INT,
  mean_cagr NUMERIC,
  std_cagr NUMERIC,
  worst_window NUMERIC,
  worst_mdd NUMERIC,
  mean_mdd NUMERIC,
  total_trades INT,
  liquidated BOOL,
  worst_mdd_recent NUMERIC,
  mean_cagr_recent NUMERIC,
  tier1 BOOL,
  tier2 BOOL,
  tier3 BOOL,
  tier4 BOOL,
  tier_pass BOOL,
  tier_a BOOL,
  tier_b BOOL,
  tier_c BOOL,
  final_tier TEXT,
  safety_score NUMERIC,
  plateau_quality TEXT,
  plateau_score NUMERIC,
  sweet_spot_score NUMERIC,
  cross_val_status TEXT,
  xref_json JSONB,
  raw_json JSONB,
  UNIQUE(sweep_id, combo_id)
);

CREATE INDEX IF NOT EXISTS st_combos_sweep_idx ON st_combos(sweep_id);
CREATE INDEX IF NOT EXISTS st_combos_ss_idx ON st_combos(sweet_spot_score DESC);

CREATE TABLE IF NOT EXISTS st_window_results (
  pk BIGSERIAL PRIMARY KEY,
  combo_pk BIGINT REFERENCES st_combos(pk) ON DELETE CASCADE,
"window" TEXT NOT NULL,
  complete BOOL,
  cagr_raw NUMERIC,
  mdd_raw NUMERIC,
  cagr_adj NUMERIC,
  mdd_adj NUMERIC,
  sharpe NUMERIC,
  trades_count INT,
  liquidated BOOL,
  finishing_balance NUMERIC,
  UNIQUE(combo_pk, "window")
);

CREATE INDEX IF NOT EXISTS st_window_results_combo_idx ON st_window_results(combo_pk);

-- ── OHLCV candle tables (backtest analytics — separate from Jesse's internal candles) ──

CREATE TABLE IF NOT EXISTS ohlcv_1m (
  id        BIGSERIAL PRIMARY KEY,
  exchange  TEXT    NOT NULL DEFAULT 'Bybit Perpetual',
  symbol    TEXT    NOT NULL DEFAULT 'BTCUSDT',
  timestamp BIGINT  NOT NULL,   -- milliseconds since epoch (UTC)
  open      FLOAT8  NOT NULL,
  high      FLOAT8  NOT NULL,
  low       FLOAT8  NOT NULL,
  close     FLOAT8  NOT NULL,
  volume    FLOAT8  NOT NULL,
  UNIQUE (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ohlcv_1m_sym_ts_idx ON ohlcv_1m (symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS ohlcv_4h (
  id        BIGSERIAL PRIMARY KEY,
  exchange  TEXT    NOT NULL DEFAULT 'Bybit Perpetual',
  symbol    TEXT    NOT NULL DEFAULT 'BTCUSDT',
  timestamp BIGINT  NOT NULL,   -- milliseconds since epoch (UTC), bar open time
  open      FLOAT8  NOT NULL,
  high      FLOAT8  NOT NULL,
  low       FLOAT8  NOT NULL,
  close     FLOAT8  NOT NULL,
  volume    FLOAT8  NOT NULL,
  UNIQUE (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ohlcv_4h_sym_ts_idx ON ohlcv_4h (symbol, timestamp DESC);
