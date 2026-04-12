"""analysis/etf_flow_collector.py
ETF 플로우 데이터 수집 및 DB 저장.

데이터 소스:
  1. Farside Investors 스크래핑 (https://farside.co.uk/bitcoin-etf-flow-all-data/)
  2. SoSoValue API 폴백
  3. 합성 프록시 데이터 (BTC 가격 기반 파생)

실행:
    python tests/backtest/analysis/etf_flow_collector.py --backfill
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import aiohttp
import asyncpg
import pandas as pd

sys.path.insert(0, "/app")
from tests.backtest.core import make_pool, load_ohlcv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UTC = timezone.utc
KST = timezone(timedelta(hours=9))

# ETF 승인일 이후부터
ETF_APPROVAL_DATE = datetime(2024, 1, 11, tzinfo=UTC).date()
TODAY = datetime.now(tz=UTC).date()


async def fetch_etf_flow_farside() -> dict[str, float] | None:
    """Farside Investors에서 ETF 플로우 스크래핑 (2024-01-11 이후).

    Returns:
        {"2024-01-11": 1234.56, ...} (USD) 또는 None (실패)
    """
    url = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(f"Farside HTTP {resp.status}")
                    return None
                html = await resp.text()

                # 간단한 JSON 패턴 추출 (실제로는 더 정교한 파싱 필요)
                if "window.__NUXT__" in html:
                    # 현대적 웹 사이트는 보통 JS로 로드되므로 직접 파싱 어려움
                    logger.warning("Farside 웹페이지 JS 렌더링 필요 (스크래핑 불가)")
                    return None

                logger.info("Farside 데이터 추출 불가 (JS 렌더링 필요)")
                return None
    except Exception as e:
        logger.warning(f"Farside 요청 실패: {e}")
        return None


async def fetch_etf_flow_sosovaluе() -> dict[str, float] | None:
    """SoSoValue API에서 ETF 플로우 조회.

    SoSoValue는 선택적 API이며, 무료 Tier에서는 제한적일 수 있음.
    """
    try:
        url = "https://api.sosovaluе.com/bitcoin/etf-flow-data"
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if "data" in data:
                    return {
                        item["date"]: float(item["flow_usd"])
                        for item in data["data"]
                    }
        return None
    except Exception as e:
        logger.warning(f"SoSoValue 요청 실패: {e}")
        return None


async def generate_synthetic_etf_flow(
    pool: asyncpg.Pool,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, float]:
    """BTC 1d OHLCV에서 합성 ETF 플로우 데이터 생성 (폴백용).

    알고리즘:
      - 가격 +1% 이상 & 거래량 증가 → 양의 플로우 (+50M ~ +500M)
      - 가격 -1% 이상 & 거래량 증가 → 음의 플로우 (-200M ~ -50M)
      - 기본: 거래량 변화율 × 무작위 배율

    Returns:
        {"2024-01-11": 1234.56, ...} (USD)
    """
    logger.info("BTC 1d 데이터에서 합성 ETF 플로우 생성")

    ohlcv = await load_ohlcv(pool, "BTCUSDT", "1d", start_date, end_date)
    if ohlcv.empty:
        logger.error("BTC 1d 데이터 없음")
        return {}

    flows = {}
    for i in range(1, len(ohlcv)):
        prev_row = ohlcv.iloc[i-1]
        curr_row = ohlcv.iloc[i]

        date_key = curr_row.name.date()

        price_change = (float(curr_row["close"]) - float(prev_row["close"])) / float(prev_row["close"])
        vol_change = (float(curr_row["volume"]) - float(prev_row["volume"])) / float(prev_row["volume"])

        # 기본 플로우 (거래량 변화 기반)
        base_flow = vol_change * 100_000_000  # $100M 스케일

        # 가격 방향에 따른 조정
        if price_change > 0.01 and vol_change > 0:
            # 상승장 + 거래량 증가 → 주로 매수 플로우
            flow_usd = base_flow + abs(price_change) * 200_000_000
        elif price_change < -0.01 and vol_change > 0:
            # 하락장 + 거래량 증가 → 주로 매도 플로우
            flow_usd = base_flow - abs(price_change) * 150_000_000
        else:
            # 중립
            flow_usd = base_flow * 0.5

        # 범위 제한
        flow_usd = max(-300_000_000, min(500_000_000, flow_usd))
        flows[str(date_key)] = round(flow_usd, 2)

    logger.info(f"합성 ETF 플로우 {len(flows)}개 데이터 생성")
    return flows


async def save_etf_flows(
    pool: asyncpg.Pool,
    flows: dict[str, float],
    source: str,
) -> None:
    """ETF 플로우를 DB에 저장."""
    if not flows:
        logger.warning("저장할 플로우 데이터 없음")
        return

    async with pool.acquire() as conn:
        for date_str, flow_usd in flows.items():
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

            # 기존 데이터 존재 여부 확인
            existing = await conn.fetchval(
                "SELECT total_flow_usd FROM etf_flow_history WHERE date = $1",
                date_obj,
            )

            if existing is None:
                # 신규 삽입
                await conn.execute(
                    """
                    INSERT INTO etf_flow_history
                    (date, total_flow_usd, source)
                    VALUES ($1, $2, $3)
                    """,
                    date_obj, flow_usd, source,
                )

    logger.info(f"{source}에서 {len(flows)}개 ETF 플로우 저장 완료")


async def compute_cumulative_flows(pool: asyncpg.Pool) -> None:
    """ETF 플로우 누적합 계산 및 저장."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date, total_flow_usd FROM etf_flow_history ORDER BY date ASC"
        )

    if not rows:
        logger.warning("누적 계산할 플로우 데이터 없음")
        return

    cumulative = 0.0
    updates = []

    for row in rows:
        cumulative += float(row["total_flow_usd"])
        updates.append((row["date"], cumulative))

    async with pool.acquire() as conn:
        for date, cum_value in updates:
            await conn.execute(
                "UPDATE etf_flow_history SET cumulative_flow_usd = $1 WHERE date = $2",
                cum_value, date,
            )

    logger.info(f"누적 플로우 계산 {len(updates)}개 행")


async def backfill_etf_flows(pool: asyncpg.Pool) -> None:
    """ETF 플로우 백필: 2024-01-11 ~ 현재.

    Note: ETF data only available from 2024-01-11 (Bitcoin spot ETF approval date).
    Prior to this date, no real ETF flow data exists.
    """
    start_date = datetime(2024, 1, 11, tzinfo=UTC)
    end_date = datetime.now(tz=UTC)

    logger.info(f"ETF 플로우 백필: {start_date.date()} ~ {end_date.date()}")

    # 1단계: Farside Investors 시도
    flows = await fetch_etf_flow_farside()
    if flows:
        await save_etf_flows(pool, flows, "farside")
        await compute_cumulative_flows(pool)
        return

    # 2단계: SoSoValue API 시도
    flows = await fetch_etf_flow_sosovaluе()
    if flows:
        await save_etf_flows(pool, flows, "sosovaluе")
        await compute_cumulative_flows(pool)
        return

    # No synthetic fallback - ETF data is real data only
    raise RuntimeError(
        "Failed to fetch ETF flow data from Farside and SoSoValue APIs. "
        "ETF data is only available from 2024-01-11 onwards (Bitcoin spot ETF approval). "
        "Options:\n"
        "1. Try alternative sources: Farside (farside.co.uk), SoSoValue, CoinGlass\n"
        "2. For backtests before 2024-01-11, consider a strategy that doesn't depend on ETF flow\n"
        "3. Do not use synthetic fallback for ETF flow data - it would be meaningless\n"
    )


async def main(args):
    pool = await make_pool()
    try:
        if args.backfill:
            await backfill_etf_flows(pool)

        # 통계 출력
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM etf_flow_history")
            min_max = await conn.fetchrow(
                "SELECT MIN(date), MAX(date) FROM etf_flow_history"
            )
            sources = await conn.fetch(
                "SELECT source, COUNT(*) as cnt FROM etf_flow_history GROUP BY source"
            )

        logger.info(f"ETF Flow 테이블 통계:")
        logger.info(f"  - 총 행: {count}")
        logger.info(f"  - 기간: {min_max['min']} ~ {min_max['max']}")
        logger.info(f"  - 소스별:")
        for row in sources:
            logger.info(f"      {row['source']}: {row['cnt']} 행")
    finally:
        await pool.close()


def _parse():
    import argparse
    p = argparse.ArgumentParser(description="ETF Flow Data Collector")
    p.add_argument("--backfill", action="store_true", help="2024-01-11부터 현재까지 백필")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse()))
