"""
scripts/log_retention.py — 서비스 로그 보존 정책 실행 스크립트

실행 방법:
    docker compose exec postgres psql -U cryptoengine -d cryptoengine -f /scripts/log_retention.py
    또는 Python으로 직접 실행 (asyncpg 사용)

보존 정책:
    DEBUG (10): 7일
    INFO (20): 30일
    WARNING (30): 90일
    ERROR (40): 365일
    CRITICAL (50): 영구 보존
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")

RETENTION_POLICY = {
    10: 7,    # DEBUG: 7일
    20: 30,   # INFO: 30일
    30: 90,   # WARNING: 90일
    40: 365,  # ERROR: 1년
    50: None, # CRITICAL: 영구 보존
}

async def run_retention():
    import asyncpg

    db_host = os.getenv("DB_HOST", "postgres")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "cryptoengine")
    db_user = os.getenv("DB_USER", "cryptoengine")
    db_pass = os.getenv("DB_PASSWORD", "")

    conn = await asyncpg.connect(
        host=db_host, port=db_port,
        database=db_name, user=db_user, password=db_pass
    )

    total_deleted = 0
    try:
        for level_no, days in RETENTION_POLICY.items():
            if days is None:
                continue
            result = await conn.execute(
                """
                DELETE FROM service_logs
                WHERE level_no = $1
                  AND timestamp < NOW() - INTERVAL '1 day' * $2
                """,
                level_no, days
            )
            deleted = int(result.split()[-1])
            total_deleted += deleted
            print(f"  level_no={level_no}: {deleted}개 삭제 ({days}일 초과)")

        await conn.execute("VACUUM ANALYZE service_logs")
        print(f"\n총 {total_deleted}개 로그 삭제 완료. VACUUM ANALYZE 실행.")
    finally:
        await conn.close()

if __name__ == "__main__":
    from shared.timezone_utils import format_kst
    print(f"로그 보존 정책 실행: {format_kst(datetime.now(timezone.utc))}")
    asyncio.run(run_retention())
