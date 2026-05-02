-- 014_dashboard_performance_indexes.sql — 대시보드 쿼리 성능 최적화 인덱스
-- Wave 2 대시보드(Task 3~6)에서 자주 사용하는 쿼리 가속화

-- A. Overview 대시보드 (Task 3) — 최신 포트폴리오 스냅샷 빠른 조회
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_at_desc
  ON portfolio_snapshots (snapshot_at DESC);

-- B. Performance & Risk 대시보드 (Task 4) — 시계열 범위 쿼리
CREATE INDEX IF NOT EXISTS idx_daily_reports_date_range
  ON daily_reports (date DESC);

CREATE INDEX IF NOT EXISTS idx_trades_created_range
  ON trades (created_at DESC) WHERE filled_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_regime_transitions_detected
  ON regime_transitions (detected_at DESC);

-- C. Strategies & Positions 대시보드 (Task 5) — 오픈 포지션 필터
CREATE INDEX IF NOT EXISTS idx_positions_closed_at_filter
  ON positions (closed_at) WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_positions_symbol_open
  ON positions (symbol, closed_at) WHERE closed_at IS NULL;

-- D. Operations 대시보드 (Task 6) — 서비스 로그 타임시리즈
CREATE INDEX IF NOT EXISTS idx_service_logs_service_time
  ON service_logs (service, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_service_logs_error_filter
  ON service_logs (timestamp DESC) WHERE level_no >= 40;

-- 공통 — 대시보드 제너럴 시계열 쿼리
CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp_range
  ON ohlcv (timestamp DESC) WHERE timeframe = '1h';

CREATE INDEX IF NOT EXISTS idx_funding_payments_collected
  ON funding_payments (collected_at DESC);
