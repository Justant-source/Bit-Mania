#!/bin/sh
# Walk-Forward 월간 스케줄러 진입점
# 환경변수 MONTHLY_WF_CRON으로 실행 주기 제어 (기본: 매월 1일 02:00 KST = UTC 17:00)

set -e

echo "WF Scheduler 시작: cron=${MONTHLY_WF_CRON}"

if [ "${WF_ON_STARTUP}" = "true" ]; then
    echo "WF_ON_STARTUP=true: 즉시 Walk-Forward 실행"
    python /app/scripts/analysis/walk_forward.py
fi

# crond 기반 주기 실행 (busybox crond)
CRON_SCHEDULE="${MONTHLY_WF_CRON:-0 17 1 * *}"
echo "${CRON_SCHEDULE} python /app/scripts/analysis/walk_forward.py >> /proc/1/fd/1 2>&1" > /var/spool/cron/crontabs/root
chmod 600 /var/spool/cron/crontabs/root

echo "크론 등록: ${CRON_SCHEDULE}"
exec crond -f -l 2
