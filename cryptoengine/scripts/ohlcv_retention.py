"""
scripts/ohlcv_retention.py — OHLCV 캔들 데이터 보존 정책 실행 스크립트

실행 방법:
    docker compose exec strategy-orchestrator python /app/scripts/ohlcv_retention.py

    또는 크론탭(컨테이너 외부, 주 1회):
    0 3 * * 0 docker compose -f /path/to/docker-compose.yml exec -T strategy-orchestrator \
        python /app/scripts/ohlcv_retention.py

보존 정책:
    1m  (1분봉):  30일  — 매일 1,440건 × 30일 = 최대 43,200건 유지
    5m  (5분봉):  90일
    15m (15분봉): 180일
    1h  (1시간봉): 365일
    4h  (4시간봉): 730일 (2년)
"""

import asyncio
import os
import sys
from datetime import datetime, timezone

# 보존 기간 (일). None = 영구 보존
RETENTION_DAYS: dict[str, int | None] = {
    "1m":  30,
    "5m":  90,
    "15m": 180,
    "1h":  365,
    "4h":  730,
}


async def run_retention() -> None:
    import asyncpg

    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "cryptoengine"),
        user=os.getenv("DB_USER", "cryptoengine"),
        password=os.getenv("DB_PASSWORD", ""),
    )

    total_deleted = 0
    try:
        for tf, days in RETENTION_DAYS.items():
            if days is None:
                print(f"  {tf}: 영구 보존, 건너뜀")
                continue
            result = await conn.execute(
                """
                DELETE FROM ohlcv_history
                WHERE timeframe = $1
                  AND timestamp < NOW() - INTERVAL '1 day' * $2
                """,
                tf,
                days,
            )
            deleted = int(result.split()[-1])
            total_deleted += deleted
            print(f"  {tf}: {deleted}건 삭제 ({days}일 초과)")

        await conn.execute("VACUUM ANALYZE ohlcv_history")
        print(f"\n총 {total_deleted}건 삭제 완료. VACUUM ANALYZE 실행.")
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.path.insert(0, "/app") if "/app" not in sys.path else None
    from shared.timezone_utils import format_kst
    print(f"OHLCV 보존 정책 실행: {format_kst(datetime.now(timezone.utc))}")
    asyncio.run(run_retention())
