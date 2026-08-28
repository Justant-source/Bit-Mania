#!/bin/sh
# scripts/ohlcv_retention.sh — ohlcv_history 롤링 보존 정책 SQL 실행
# pg-backup과 동일한 postgres:16-alpine 이미지에서 psql로 실행
#
# 매일 03:00 KST (18:00 UTC)에 OHLCV_RETENTION_DAYS 이상 된 레코드 삭제
# (기본값: 7일)

set -e

RETENTION_DAYS="${OHLCV_RETENTION_DAYS:-7}"

PGPASSWORD="${DB_PASSWORD}" psql \
  -h "${DB_HOST:-postgres}" \
  -U "${DB_USER:-cryptoengine}" \
  -d "${DB_NAME:-cryptoengine}" \
  -v ON_ERROR_STOP=1 \
  <<SQL
\echo '[ohlcv_retention] ohlcv_history 롤링 보존 정책 실행 시작 (retention: ${RETENTION_DAYS} days)'

DELETE FROM ohlcv_history WHERE timestamp < NOW() - INTERVAL '${RETENTION_DAYS} days';
\echo '[ohlcv_retention] ohlcv_history 삭제 완료'

VACUUM ANALYZE ohlcv_history;
\echo '[ohlcv_retention] VACUUM ANALYZE 완료'
SQL

echo "[$(date)] ohlcv-retention 완료"
