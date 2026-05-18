#!/bin/sh
# ohlcv-retention container entrypoint
# 매일 03:00 KST (18:00 UTC) 에 ohlcv_history 롤링 보존 정책 실행

set -e

CRON_SCHEDULE="${OHLCV_RETENTION_CRON:-0 18 * * *}"

# crontab 설정
echo "${CRON_SCHEDULE} /scripts/ohlcv_retention.sh >> /var/log/ohlcv_retention.log 2>&1" \
  > /var/spool/cron/crontabs/root

echo "[$(date)] ohlcv-retention service started"
echo "[$(date)] Schedule: ${CRON_SCHEDULE}"
echo "[$(date)] Retention days: ${OHLCV_RETENTION_DAYS:-7}"

# 컨테이너 시작 시 즉시 한 번 실행 (최초 기동 확인용)
if [ "${OHLCV_RETENTION_ON_STARTUP:-false}" = "true" ]; then
  echo "[$(date)] Running initial retention on startup..."
  /scripts/ohlcv_retention.sh
fi

# crond 포그라운드 실행
exec crond -f -l 8
