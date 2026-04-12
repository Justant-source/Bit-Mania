#!/bin/sh
# log-retention container entrypoint
# 매일 03:00 KST (18:00 UTC) 에 service_logs 보존 정책 실행

set -e

CRON_SCHEDULE="${LOG_RETENTION_CRON:-0 18 * * *}"

# crontab 설정
echo "${CRON_SCHEDULE} /scripts/log_retention.sh >> /var/log/log_retention.log 2>&1" \
  > /var/spool/cron/crontabs/root

echo "[$(date)] log-retention service started"
echo "[$(date)] Schedule: ${CRON_SCHEDULE}"

# 컨테이너 시작 시 즉시 한 번 실행 (최초 기동 확인용)
if [ "${RETENTION_ON_STARTUP:-false}" = "true" ]; then
  echo "[$(date)] Running initial retention on startup..."
  /scripts/log_retention.sh
fi

# crond 포그라운드 실행
exec crond -f -l 8
