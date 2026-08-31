-- verify_ohlcv_gate.sql
-- Idempotent diagnostic gate after 1m/4h OHLCV splice (S6). SELECT only — no writes.
--
-- Splice cutoff: last canonical native 4h bar open = 2026-04-30 20:00:00 UTC
-- Expected: ohlcv_4h rows with timestamp <= that cutoff remain 19057.

\set ON_ERROR_STOP on

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Count and max timestamp (ohlcv_1m, ohlcv_4h)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  'ohlcv_1m'::text AS table_name,
  COUNT(*) AS row_count,
  MAX(timestamp) AS max_ts_ms,
  to_timestamp(MAX(timestamp) / 1000.0) AT TIME ZONE 'UTC' AS max_ts_utc
FROM ohlcv_1m
UNION ALL
SELECT
  'ohlcv_4h'::text,
  COUNT(*),
  MAX(timestamp),
  to_timestamp(MAX(timestamp) / 1000.0) AT TIME ZONE 'UTC'
FROM ohlcv_4h;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Count 1m rows on 2026-04-30 UTC (full calendar day)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  COUNT(*) AS ohlcv_1m_rows_2026_04_30_utc
FROM ohlcv_1m
WHERE timestamp >= (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-04-30 00:00:00+00') * 1000)::bigint
  AND timestamp <  (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-05-01 00:00:00+00') * 1000)::bigint;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Count 4h rows with timestamp <= 2026-04-30 20:00 UTC
--    Expected: 19057 (must not change after splice)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  COUNT(*) AS ohlcv_4h_rows_at_or_before_splice,
  19057 AS expected_count,
  (COUNT(*) = 19057) AS matches_expected
FROM ohlcv_4h
WHERE timestamp <= (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-04-30 20:00:00+00') * 1000)::bigint;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Count 4h rows after the splice cutoff
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
  COUNT(*) AS ohlcv_4h_rows_after_splice
FROM ohlcv_4h
WHERE timestamp > (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-04-30 20:00:00+00') * 1000)::bigint;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Sample 3 rows at the splice: last two old bars + first new bar
--    Last old bar open = 2026-04-30 20:00 UTC; first new bar is the next open.
-- ─────────────────────────────────────────────────────────────────────────────
(
  SELECT
    timestamp AS ts_ms,
    to_timestamp(timestamp / 1000.0) AT TIME ZONE 'UTC' AS ts_utc,
    'old'::text AS side,
    open, high, low, close, volume
  FROM ohlcv_4h
  WHERE timestamp <= (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-04-30 20:00:00+00') * 1000)::bigint
  ORDER BY timestamp DESC
  LIMIT 2
)
UNION ALL
(
  SELECT
    timestamp AS ts_ms,
    to_timestamp(timestamp / 1000.0) AT TIME ZONE 'UTC' AS ts_utc,
    'new'::text AS side,
    open, high, low, close, volume
  FROM ohlcv_4h
  WHERE timestamp > (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-04-30 20:00:00+00') * 1000)::bigint
  ORDER BY timestamp ASC
  LIMIT 1
)
ORDER BY ts_ms;
