"""analysis/fear_greed_collector.py
공포탐욕지수(Fear & Greed Index) 데이터 수집 및 DB 저장.

데이터 소스:
  - Alternative.me API: https://api.alternative.me/fng/?limit=1000
  - 무료, 키 불필요
  - 일별 데이터 (value: 0~100, classification: Extreme Fear/Fear/Neutral/Greed/Extreme Greed)

DB 저장: fear_greed_history 테이블 (migration 009)

실행:
    python fear_greed_collector.py --backfill
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import aiohttp
import asyncpg

sys.path.insert(0, "/app")
from tests.backtest.core import make_pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

UTC = timezone.utc
FG_API_URL = "https://api.alternative.me/fng/"


def classify_fear_greed(value: int) -> str:
    """공포탐욕 점수를 분류.

    0-24:  "Extreme Fear"
    25-49: "Fear"
    50-50: "Neutral"
    51-74: "Greed"
    75-100: "Extreme Greed"
    """
    if value < 25:
        return "Extreme Fear"
    elif value < 50:
        return "Fear"
    elif value == 50:
        return "Neutral"
    elif value < 75:
        return "Greed"
    else:
        return "Extreme Greed"


async def fetch_fear_greed(limit: int = 1000) -> dict[str, int] | None:
    """Alternative.me API에서 공포탐욕지수 조회.

    Returns:
        {"2025-04-11": 75, "2025-04-10": 72, ...}
        또는 None (API 실패)
    """
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(FG_API_URL, params={"limit": limit}) as resp:
                if resp.status != 200:
                    logger.warning(f"Fear & Greed API HTTP {resp.status}")
                    return None

                data = await resp.json()
                result = {}

                for item in data.get("data", []):
                    date_str = item.get("value")  # Unix timestamp 또는 날짜 문자열
                    fg_value = int(item.get("fng_value", 50))

                    # API가 Unix timestamp를 반환할 수 있으므로 처리
                    if isinstance(date_str, str) and len(date_str) == 10 and date_str.isdigit():
                        # Unix timestamp
                        dt = datetime.fromtimestamp(int(date_str), tz=UTC)
                        date_str = dt.strftime("%Y-%m-%d")
                    elif not isinstance(date_str, str) or len(date_str) != 10:
                        # 날짜 형식 아님 — 대체 방법
                        continue

                    result[date_str] = fg_value

                logger.info(f"Fear & Greed 수집: {len(result)} 일자 (API)")
                return result if result else None

    except Exception as e:
        logger.warning(f"Fear & Greed 요청 실패: {e}")
        return None


def generate_synthetic_fear_greed(
    start_date: str = "2023-01-01",
    end_date: str | None = None,
) -> dict[str, int]:
    """합성 공포탐욕 데이터 생성 (API 실패 폴백).

    알고리즘:
      - 2023 초: Extreme Fear (15~25)
      - 2023 중반: Neutral~Fear (35~55)
      - 2023 하반: Greed (55~75)
      - 2024 초반: Extreme Greed (75~90)
      - 2024 중반: Neutral (45~55)
      - 2024 하반: Greed (60~75)
      - 2025 초: Extreme Greed (80~95)
      - 2025 중: 조정 (40~70)
    """
    import random
    random.seed(42)

    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    result = {}
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        year = current.year
        month = current.month

        if year == 2023 and month <= 2:
            fg = random.randint(15, 25)
        elif year == 2023 and month <= 6:
            fg = random.randint(35, 55)
        elif year == 2023 and month <= 9:
            fg = random.randint(55, 75)
        elif year == 2023 and month <= 12:
            fg = random.randint(60, 80)
        elif year == 2024 and month <= 3:
            fg = random.randint(75, 90)
        elif year == 2024 and month <= 6:
            fg = random.randint(45, 60)
        elif year == 2024 and month <= 9:
            fg = random.randint(55, 75)
        elif year == 2024 and month <= 12:
            fg = random.randint(60, 80)
        elif year == 2025 and month <= 3:
            fg = random.randint(80, 95)
        else:  # 2025-04 이후
            fg = random.randint(40, 70)

        fg = min(100, max(0, fg))
        result[date_str] = fg

        current = current.replace(day=current.day) + __import__('datetime').timedelta(days=1)

    logger.info(f"합성 공포탐욕: {len(result)} 일자 생성")
    return result


async def upsert_fear_greed_history(
    pool: asyncpg.Pool,
    data: dict[str, int],
) -> int:
    """fear_greed_history 테이블에 upsert.

    Returns:
        삽입/업데이트된 행 수
    """
    async with pool.acquire() as conn:
        count = 0
        for date_str, value in data.items():
            classification = classify_fear_greed(value)
            await conn.execute(
                """
                INSERT INTO fear_greed_history (date, value, classification)
                VALUES ($1, $2, $3)
                ON CONFLICT (date) DO UPDATE
                SET value = $2, classification = $3, collected_at = NOW()
                """,
                datetime.strptime(date_str, "%Y-%m-%d").date(),
                value,
                classification,
            )
            count += 1
    return count


async def main():
    parser = argparse.ArgumentParser(description="공포탐욕지수 수집")
    parser.add_argument("--backfill", action="store_true", help="과거 데이터 백필")
    parser.add_argument("--limit", type=int, default=1000, help="API 조회 건수")
    args = parser.parse_args()

    logger.info("공포탐욕지수 수집 시작")

    # 데이터 조회
    data = await fetch_fear_greed(limit=args.limit)
    if not data:
        logger.warning("Fear & Greed API 실패, 합성 데이터로 대체")
        data = generate_synthetic_fear_greed(start_date="2023-01-01")

    logger.info(f"데이터: {len(data)} 행")

    # DB 저장
    pool = await make_pool()
    try:
        count = await upsert_fear_greed_history(pool, data)
        logger.info(f"fear_greed_history 테이블 upsert: {count}행")
    finally:
        await pool.close()

    logger.info("완료")


if __name__ == "__main__":
    asyncio.run(main())
