#!/bin/sh
# scripts/ohlcv_retention.sh — ohlcv_history 롤링 보존 정책 SQL 실행
# pg-backup과 동일한 postgres:16-alpine 이미지에서 psql로 실행
#
# 4h는 운영 전략 정본이므로 삭제하지 않는다.
# 1m/5m/15m/1h는 Bybit 본선에서 더 이상 수집하지 않음 — 잔여 행만 정리.

set -e

RETENTION_DAYS="${OHLCV_RETENTION_DAYS:-7}"
case "${RETENTION_DAYS}" in
  ''|*[!0-9]*) RETENTION_DAYS=7 ;;
esac

PGPASSWORD="${DB_PASSWORD}" psql \
  -h "${DB_HOST:-postgres}" \
  -U "${DB_USER:-cryptoengine}" \
  -d "${DB_NAME:-cryptoengine}" \
  -v ON_ERROR_STOP=1 \
  <<SQL
\echo '[ohlcv_retention] 단기 봉 롤링 보존 시작 (retention: ${RETENTION_DAYS} days, 4h 제외)'

DELETE FROM ohlcv_history
 WHERE timeframe <> '4h'
   AND timestamp < NOW() - (${RETENTION_DAYS}::int * INTERVAL '1 day');
\echo '[ohlcv_retention] 단기 봉 삭제 완료 (4h 보존)'

VACUUM ANALYZE ohlcv_history;
\echo '[ohlcv_retention] VACUUM ANALYZE 완료'
SQL

echo "[$(date)] ohlcv-retention 완료"
