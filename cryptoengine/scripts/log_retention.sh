#!/bin/sh
# scripts/log_retention.sh — service_logs 보존 정책 SQL 실행
# pg-backup와 동일한 postgres:16-alpine 이미지에서 psql로 실행

set -e

PGPASSWORD="${DB_PASSWORD}" psql \
  -h "${DB_HOST:-postgres}" \
  -U "${DB_USER:-cryptoengine}" \
  -d "${DB_NAME:-cryptoengine}" \
  -v ON_ERROR_STOP=1 \
  <<'SQL'
\echo '[retention] service_logs 보존 정책 실행 시작'

-- DEBUG (level_no=10): 7일
DELETE FROM service_logs WHERE level_no = 10 AND timestamp < NOW() - INTERVAL '7 days';
\echo '[retention] DEBUG 삭제 완료'

-- INFO (level_no=20): 30일
DELETE FROM service_logs WHERE level_no = 20 AND timestamp < NOW() - INTERVAL '30 days';
\echo '[retention] INFO 삭제 완료'

-- WARNING (level_no=30): 90일
DELETE FROM service_logs WHERE level_no = 30 AND timestamp < NOW() - INTERVAL '90 days';
\echo '[retention] WARNING 삭제 완료'

-- ERROR (level_no=40): 365일
DELETE FROM service_logs WHERE level_no = 40 AND timestamp < NOW() - INTERVAL '365 days';
\echo '[retention] ERROR 삭제 완료'

-- CRITICAL (level_no=50): 영구 보존 (삭제 없음)

VACUUM ANALYZE service_logs;
\echo '[retention] VACUUM ANALYZE 완료'
SQL

echo "[$(date)] log-retention 완료"
