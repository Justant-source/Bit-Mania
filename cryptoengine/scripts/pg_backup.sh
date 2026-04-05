#!/bin/sh
# PostgreSQL daily backup script
# Runs inside pg-backup container via crond

set -e

BACKUP_DIR="/backups"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="cryptoengine_${DATE}.sql.gz"
KEEP_DAYS=7

echo "[$(date)] Starting backup: ${FILENAME}"

pg_dump \
  -h "${DB_HOST}" \
  -U "${DB_USER}" \
  -d "${DB_NAME}" \
  --no-password \
  | gzip > "${BACKUP_DIR}/${FILENAME}"

SIZE=$(du -sh "${BACKUP_DIR}/${FILENAME}" | cut -f1)
echo "[$(date)] Backup complete: ${FILENAME} (${SIZE})"

# 7일 이상 된 백업 삭제
find "${BACKUP_DIR}" -name "cryptoengine_*.sql.gz" -mtime "+${KEEP_DAYS}" -delete
echo "[$(date)] Cleaned up backups older than ${KEEP_DAYS} days"

# 현재 보유 백업 목록
echo "[$(date)] Current backups:"
ls -lh "${BACKUP_DIR}"/cryptoengine_*.sql.gz 2>/dev/null || echo "  (none)"
