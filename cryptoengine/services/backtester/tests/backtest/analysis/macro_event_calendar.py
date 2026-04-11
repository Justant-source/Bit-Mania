"""analysis/macro_event_calendar.py
매크로 이벤트 (FOMC, CPI) 캘린더 DB 저장.

하드코딩된 FOMC 일정 및 CPI 발표일을 macro_events 테이블에 저장.

실행:
    python tests/backtest/analysis/macro_event_calendar.py --backfill
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone

import asyncpg

sys.path.insert(0, "/app")
from tests.backtest.core import make_pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = timezone.utc

# FOMC 회의 일정 (2024-01 ~ 2026-06)
# 출처: Fed 공식 일정 (policy decision date)
FOMC_DATES = [
    # 2024
    "2024-01-31",
    "2024-03-20",
    "2024-05-01",
    "2024-06-12",
    "2024-07-31",
    "2024-09-18",
    "2024-11-07",
    "2024-12-18",
    # 2025
    "2025-01-29",
    "2025-03-19",
    "2025-05-07",
    "2025-06-18",
    "2025-07-30",
    "2025-09-17",
    "2025-11-05",
    "2025-12-17",
    # 2026
    "2026-01-28",
    "2026-03-18",
    "2026-04-29",
    "2026-06-17",
]

# CPI 발표일 (매월 둘째 주 또는 셋째 주 수요일, US 9:30 ET)
# 패턴: 2024년부터 대부분 둘째 주 수요일, 일부 예외 있음
# 정확한 일정은 BLS 캘린더에서 확인 필요 (여기서는 근사 배치)
CPI_DATES = [
    # 2024
    "2024-01-10",  # Wed, 2nd week (release date)
    "2024-02-14",  # Wed, 2nd week
    "2024-03-12",  # Tue, 2nd week
    "2024-04-10",  # Wed, 2nd week
    "2024-05-15",  # Wed, 3rd week
    "2024-06-12",  # Wed, 2nd week
    "2024-07-10",  # Wed, 2nd week
    "2024-08-14",  # Wed, 2nd week
    "2024-09-11",  # Wed, 2nd week
    "2024-10-09",  # Wed, 2nd week
    "2024-11-13",  # Wed, 2nd week
    "2024-12-11",  # Wed, 2nd week
    # 2025
    "2025-01-15",  # Wed, 3rd week
    "2025-02-12",  # Wed, 2nd week
    "2025-03-12",  # Wed, 2nd week
    "2025-04-09",  # Wed, 2nd week
    "2025-05-14",  # Wed, 2nd week
    "2025-06-11",  # Wed, 2nd week
    "2025-07-09",  # Wed, 2nd week
    "2025-08-13",  # Wed, 2nd week
    "2025-09-10",  # Wed, 2nd week
    "2025-10-08",  # Wed, 2nd week
    "2025-11-12",  # Wed, 2nd week
    "2025-12-10",  # Wed, 2nd week
    # 2026
    "2026-01-14",  # Wed, 2nd week
    "2026-02-11",  # Wed, 2nd week
    "2026-03-11",  # Wed, 2nd week
    "2026-04-08",  # Wed, 2nd week
    "2026-05-13",  # Wed, 2nd week
]


async def backfill_macro_events(pool: asyncpg.Pool) -> None:
    """FOMC, CPI 일정을 macro_events 테이블에 저장."""
    events = []

    # FOMC 추가
    for date_str in FOMC_DATES:
        events.append((date_str, "FOMC", 3))

    # CPI 추가
    for date_str in CPI_DATES:
        events.append((date_str, "CPI", 3))

    logger.info(f"매크로 이벤트 {len(events)}개 저장 준비")

    async with pool.acquire() as conn:
        for date_str, event_type, impact_level in events:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

            # UPSERT (중복 무시)
            await conn.execute(
                """
                INSERT INTO macro_events (event_date, event_type, impact_level)
                VALUES ($1, $2, $3)
                ON CONFLICT(event_date, event_type) DO NOTHING
                """,
                date_obj, event_type, impact_level,
            )

    logger.info(f"매크로 이벤트 저장 완료")


async def main(args):
    pool = await make_pool()
    try:
        if args.backfill:
            await backfill_macro_events(pool)

        # 통계 출력
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM macro_events")
            by_type = await conn.fetch(
                "SELECT event_type, COUNT(*) as cnt FROM macro_events GROUP BY event_type"
            )

        logger.info(f"Macro Events 테이블 통계:")
        logger.info(f"  - 총 행: {count}")
        logger.info(f"  - 이벤트별:")
        for row in by_type:
            logger.info(f"      {row['event_type']}: {row['cnt']} 건")
    finally:
        await pool.close()


def _parse():
    import argparse
    p = argparse.ArgumentParser(description="Macro Event Calendar")
    p.add_argument("--backfill", action="store_true", help="2024-01부터 2026-06까지 백필")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse()))
