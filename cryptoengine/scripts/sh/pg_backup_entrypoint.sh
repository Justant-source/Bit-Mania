#!/bin/sh
# pg-backup container entrypoint
# crond를 통해 매일 02:00 KST (17:00 UTC) 에 백업 실행

set -e

BACKUP_DIR="/backups"
CRON_SCHEDULE="${BACKUP_CRON:-0 17 * * *}"

mkdir -p "${BACKUP_DIR}"

# pg_dump 비밀번호를 pgpass에 저장 (비밀번호 프롬프트 방지)
echo "${DB_HOST}:5432:${DB_NAME}:${DB_USER}:${DB_PASSWORD}" > /root/.pgpass
chmod 600 /root/.pgpass

# crontab 설정
echo "${CRON_SCHEDULE} /scripts/pg_backup.sh >> /var/log/pg_backup.log 2>&1" > /var/spool/cron/crontabs/root

echo "[$(date)] pg-backup service started"
echo "[$(date)] Schedule: ${CRON_SCHEDULE}"
echo "[$(date)] Backup directory: ${BACKUP_DIR}"

# 컨테이너 시작 시 즉시 한 번 백업 (최초 기동 확인용)
if [ "${RUN_ON_STARTUP:-false}" = "true" ]; then
  echo "[$(date)] Running initial backup on startup..."
  /scripts/pg_backup.sh
fi

# crond 포그라운드 실행
exec crond -f -l 8
