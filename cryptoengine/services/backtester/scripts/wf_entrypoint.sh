#!/bin/sh
# wf-scheduler container entrypoint
# 매월 1일 02:00 KST (17:00 UTC) 에 Walk-Forward 분석 실행

set -e

CRON_SCHEDULE="${MONTHLY_WF_CRON:-0 17 1 * *}"
RUNNER="/app/scripts/monthly_wf_runner.py"

echo "[$(date)] wf-scheduler service started"
echo "[$(date)] Schedule: ${CRON_SCHEDULE}"
echo "[$(date)] Symbol:   ${WF_SYMBOL:-BTCUSDT}"
echo "[$(date)] Lookback: ${WF_LOOKBACK_DAYS:-180} days"

# crontab 설정 — 실행 후 로그 기록
echo "${CRON_SCHEDULE} python ${RUNNER} >> /var/log/wf_runner.log 2>&1" \
  > /var/spool/cron/crontabs/root

# WF_ON_STARTUP=true 이면 즉시 실행
if [ "${WF_ON_STARTUP:-false}" = "true" ]; then
  echo "[$(date)] WF_ON_STARTUP=true — running immediately..."
  python "${RUNNER}" 2>&1 | tee -a /var/log/wf_runner.log
fi

# crond 포그라운드 실행
exec crond -f -l 8
