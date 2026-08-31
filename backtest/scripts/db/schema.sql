-- backtest-postgres schema (jesse_db, port 5433)
-- All statements use IF NOT EXISTS for idempotency.
-- Apply: psql -h 127.0.0.1 -p 5433 -U jesse -d jesse_db -f schema.sql

-- ── Sweep results ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS st_sweeps (
  sweep_id    TEXT PRIMARY KEY,
  description TEXT,
  leverage    NUMERIC NOT NULL DEFAULT 3,
  variant     TEXT DEFAULT 'long_only',
  grid_json   JSONB,
  n_combos    INT,
  created_at  TIMESTAMPTZ DEFAULT now(),
  source_csv  TEXT
);

CREATE TABLE IF NOT EXISTS st_combos (
  pk                 BIGSERIAL PRIMARY KEY,
  sweep_id           TEXT REFERENCES st_sweeps(sweep_id),
  combo_id           INT NOT NULL,
  st_factor          NUMERIC,
  st_period          INT,
  fast_ema_len       INT,
  slow_ema_len       INT,
  direction_ema_len  INT,
  atr_mult           NUMERIC,
  sl_margin_pct      NUMERIC DEFAULT 0,
  tp_atr_mult        NUMERIC,
  sl_atr_mult        NUMERIC,
  n_complete         INT,
  n_positive         INT,
  mean_cagr          NUMERIC,
  std_cagr           NUMERIC,
  worst_window       NUMERIC,
  worst_mdd          NUMERIC,
  mean_mdd           NUMERIC,
  total_trades       INT,
  liquidated         BOOL,
  worst_mdd_recent   NUMERIC,
  mean_cagr_recent   NUMERIC,
  tier1              BOOL,
  tier2              BOOL,
  tier3              BOOL,
  tier4              BOOL,
  tier_pass          BOOL,
  tier_a             BOOL,
  tier_b             BOOL,
  tier_c             BOOL,
  final_tier         TEXT,
  safety_score       NUMERIC,
  plateau_quality    TEXT,
  plateau_score      NUMERIC,
  sweet_spot_score   NUMERIC,
  cross_val_status   TEXT,
  xref_json          JSONB,
  raw_json           JSONB,
  UNIQUE(sweep_id, combo_id)
);

CREATE INDEX IF NOT EXISTS st_combos_sweep_idx ON st_combos(sweep_id);
CREATE INDEX IF NOT EXISTS st_combos_ss_idx    ON st_combos(sweet_spot_score DESC);

CREATE TABLE IF NOT EXISTS st_window_results (
  pk             BIGSERIAL PRIMARY KEY,
  combo_pk       BIGINT REFERENCES st_combos(pk) ON DELETE CASCADE,
  "window"       TEXT NOT NULL,
  complete       BOOL,
  cagr_raw       NUMERIC,
  mdd_raw        NUMERIC,
  cagr_adj       NUMERIC,
  mdd_adj        NUMERIC,
  sharpe         NUMERIC,
  trades_count   INT,
  liquidated     BOOL,
  finishing_balance NUMERIC,
  UNIQUE(combo_pk, "window")
);

CREATE INDEX IF NOT EXISTS st_window_results_combo_idx ON st_window_results(combo_pk);

-- ── OHLCV candle tables (analytics — separate from Jesse internal candles) ──
-- 출처: Binance Vision SPOT (data.binance.vision/data/spot), download_binance_vision.py /
--       fetch_binance_vision_1m_to_pg.py 로 적재. 전 구간 SPOT 임을 실측 확인했다
--       (2020-03-12 / 2022-06-15 / 2026-08-28 각 1,440봉 spot 100% 일치, perp 0%).
-- ⚠️ 2026-08-31 이전에는 이 컬럼이 'Bybit Perpetual'로 잘못 라벨링돼 있었다(다운로드
--    스크립트의 하드코딩 상수). 라이브(cryptoengine.ohlcv_history)는 Bybit USDT
--    무기한이다 — 거래소도 상품 종류도 다르며(현물 vs 무기한) 종가 기준 평균 +0.05%
--    차이가 난다. 두 소스를 섞지 말 것. 무기한의 펀딩비는 이 캔들에 반영돼 있지 않다.
--    상세: backtest/results/2026-08-31/csv_ohlcv_drift.md

CREATE TABLE IF NOT EXISTS ohlcv_1m (
  id        BIGSERIAL PRIMARY KEY,
  exchange  TEXT   NOT NULL DEFAULT 'Binance Spot',
  symbol    TEXT   NOT NULL DEFAULT 'BTCUSDT',
  timestamp BIGINT NOT NULL,   -- milliseconds since epoch (UTC)
  open      FLOAT8 NOT NULL,
  high      FLOAT8 NOT NULL,
  low       FLOAT8 NOT NULL,
  close     FLOAT8 NOT NULL,
  volume    FLOAT8 NOT NULL,
  UNIQUE (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ohlcv_1m_sym_ts_idx ON ohlcv_1m (symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS ohlcv_4h (
  id        BIGSERIAL PRIMARY KEY,
  exchange  TEXT   NOT NULL DEFAULT 'Binance Spot',
  symbol    TEXT   NOT NULL DEFAULT 'BTCUSDT',
  timestamp BIGINT NOT NULL,   -- milliseconds since epoch (UTC), bar open time
  open      FLOAT8 NOT NULL,
  high      FLOAT8 NOT NULL,
  low       FLOAT8 NOT NULL,
  close     FLOAT8 NOT NULL,
  volume    FLOAT8 NOT NULL,
  UNIQUE (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ohlcv_4h_sym_ts_idx ON ohlcv_4h (symbol, timestamp DESC);

-- ── Funding rate history ─────────────────────────────────────────────────────

-- funding_8h 의 출처는 미검증이다 — 현재 트리에 적재 스크립트가 없어
-- 'Bybit Perpetual' 라벨의 진위를 확인하지 못했다(2026-08-31 조사).
CREATE TABLE IF NOT EXISTS funding_8h (
  id           BIGSERIAL PRIMARY KEY,
  exchange     TEXT   NOT NULL DEFAULT 'Bybit Perpetual',
  symbol       TEXT   NOT NULL DEFAULT 'BTCUSDT',
  timestamp    BIGINT NOT NULL,   -- milliseconds since epoch (UTC), settlement time
  funding_rate FLOAT8 NOT NULL,
  UNIQUE (exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS funding_8h_sym_ts_idx ON funding_8h (symbol, timestamp DESC);

-- v11 robust columns
ALTER TABLE st_combos
  ADD COLUMN IF NOT EXISTS lg_mean NUMERIC,
  ADD COLUMN IF NOT EXISTS lg_std NUMERIC,
  ADD COLUMN IF NOT EXISTS lg_cv NUMERIC,
  ADD COLUMN IF NOT EXISTS robust_score NUMERIC,
  ADD COLUMN IF NOT EXISTS smooth_score NUMERIC,
  ADD COLUMN IF NOT EXISTS n_neighbors INT,
  ADD COLUMN IF NOT EXISTS nb_feasible_ratio NUMERIC,
  ADD COLUMN IF NOT EXISTS pct_mean NUMERIC,
  ADD COLUMN IF NOT EXISTS pct_std NUMERIC,
  ADD COLUMN IF NOT EXISTS pct_max NUMERIC,
  ADD COLUMN IF NOT EXISTS pareto BOOL,
  ADD COLUMN IF NOT EXISTS constraint_pass BOOL,
  ADD COLUMN IF NOT EXISTS robust_ctx TEXT,
  ADD COLUMN IF NOT EXISTS robust_json JSONB;
CREATE INDEX IF NOT EXISTS st_combos_smooth_idx ON st_combos(smooth_score DESC);

