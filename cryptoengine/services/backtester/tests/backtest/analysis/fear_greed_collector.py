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


async def fetch_fear_greed(limit: int = 0) -> dict[str, int]:
    """Alternative.me API에서 공포탐욕지수 조회 (전체 히스토리).

    Args:
        limit: 조회 건수. 0 = 전체 히스토리 (2018-02-01 이후). API는 무한 조회 지원.

    Returns:
        {"2025-04-11": 75, "2025-04-10": 72, ..., "2018-02-01": 8}
        조회 실패 시 RuntimeError 발생 (synthetic fallback 없음).

    API Response:
        {
          "data": [
            {"value": "45", "value_classification": "Fear", "timestamp": "1519776000", "time_until_update": "..."},
            ...
          ]
        }
    """
    # limit=0은 전체 히스토리를 반환하도록 API 설정
    url = FG_API_URL
    params = {} if limit == 0 else {"limit": limit}

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Fear & Greed API HTTP {resp.status}: {await resp.text()}")

                data = await resp.json()
                result = {}

                for item in data.get("data", []):
                    timestamp = item.get("timestamp")
                    fg_value = int(item.get("value", 50))

                    # timestamp는 Unix timestamp 문자열
                    if isinstance(timestamp, str) and timestamp.isdigit():
                        dt = datetime.fromtimestamp(int(timestamp), tz=UTC)
                        date_str = dt.strftime("%Y-%m-%d")
                        result[date_str] = fg_value
                    else:
                        logger.warning(f"Invalid timestamp in Fear & Greed API: {timestamp}")
                        continue

                if not result:
                    raise RuntimeError("Fear & Greed API returned empty data")

                logger.info(f"Fear & Greed 수집: {len(result)} 일자 (API)")

                # 검증: 2018년 데이터가 포함되어 있는지 확인
                dates_in_result = [datetime.strptime(d, "%Y-%m-%d") for d in result.keys()]
                min_date = min(dates_in_result) if dates_in_result else None

                if min_date and min_date >= datetime(2019, 1, 1, tzinfo=UTC):
                    logger.warning(f"Warning: Oldest data is {min_date.date()}, expected to include data from 2018-02-01 onwards")

                return result

    except RuntimeError:
        raise  # Re-raise RuntimeError
    except Exception as e:
        raise RuntimeError(f"Fear & Greed API 요청 실패: {e}")




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
    parser = argparse.ArgumentParser(description="공포탐욕지수 수집 (Alternative.me API)")
    parser.add_argument("--backfill", action="store_true", help="과거 데이터 백필 (전체 히스토리 조회)")
    args = parser.parse_args()

    logger.info("공포탐욕지수 수집 시작 (limit=0: 전체 히스토리)")

    # 데이터 조회 (실패 시 RuntimeError 발생)
    try:
        data = await fetch_fear_greed(limit=0)  # 0 = fetch all history
        logger.info(f"데이터: {len(data)} 행")
    except RuntimeError as e:
        logger.error(f"FATAL: Fear & Greed API 실패: {e}")
        logger.error("Real data is required. No synthetic fallback available.")
        raise

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
