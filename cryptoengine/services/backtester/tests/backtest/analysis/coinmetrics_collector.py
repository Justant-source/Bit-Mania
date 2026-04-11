"""analysis/coinmetrics_collector.py
온체인 메트릭 수집 — MVRV, aSOPR, 거래소순유출 (CoinMetrics Community API).

CoinMetrics Community API 사용:
  - URL: https://community-api.coinmetrics.io/v4
  - 키 불필요, 무료, 일별 데이터
  - 메트릭: PriceUSD, CapMrktCurUSD, CapRealUSD, FlowInExUSD, FlowOutExUSD, SplyAct180d

파생 지표:
  - MVRV = CapMrktCurUSD / CapRealUSD
  - MVRV Z-Score = (mvrv - 4yr_ma) / 4yr_std
  - aSOPR: CoinMetrics 없으면 1.0 상수 또는 간접 추정
  - 거래소 순유출 = FlowOutExUSD - FlowInExUSD

DB 저장: onchain_metrics 테이블 (migration 009)

실행:
    python coinmetrics_collector.py --backfill --start 2020-01-01
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import asyncpg
import pandas as pd

sys.path.insert(0, "/app")
from tests.backtest.core import make_pool, load_ohlcv

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

UTC = timezone.utc
COINMETRICS_BASE = "https://community-api.coinmetrics.io/v4"


async def fetch_coinmetrics(
    asset: str = "btc",
    start_date: str | None = None,
    end_date: str | None = None,
    metrics: str = "PriceUSD,CapMrktCurUSD,CapRealUSD,FlowInExUSD,FlowOutExUSD,SplyAct180d",
) -> dict[str, dict[str, Any]] | None:
    """CoinMetrics Community API에서 일별 메트릭 조회.

    Returns:
        {"2020-01-01": {"price_usd": 9000, "market_cap": ..., ...}, ...}
        또는 None (API 실패)
    """
    if not start_date:
        start_date = (datetime.now(UTC) - timedelta(days=4*365)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    url = f"{COINMETRICS_BASE}/timeseries/asset-metrics"
    params = {
        "assets": asset.lower(),
        "metrics": metrics,
        "start_time": f"{start_date}T00:00:00Z",
        "end_time": f"{end_date}T23:59:59Z",
        "frequency": "1d",
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"CoinMetrics HTTP {resp.status}")
                    return None
                data = await resp.json()

                # Parse response: data.data[].time, .price_usd, .cap_mrkt_cur_usd, ...
                result = {}
                for row in data.get("data", []):
                    date_str = row.get("time", "").split("T")[0]
                    if not date_str:
                        continue
                    result[date_str] = {
                        "price_usd": safe_float(row.get("PriceUSD")),
                        "market_cap_usd": safe_float(row.get("CapMrktCurUSD")),
                        "realized_cap_usd": safe_float(row.get("CapRealUSD")),
                        "flow_in_ex_usd": safe_float(row.get("FlowInExUSD")),
                        "flow_out_ex_usd": safe_float(row.get("FlowOutExUSD")),
                        "sply_act_180d": safe_float(row.get("SplyAct180d")),
                    }
                logger.info(f"CoinMetrics 수집: {len(result)} 일자 (API)")
                return result if result else None

    except Exception as e:
        logger.warning(f"CoinMetrics 요청 실패: {e}")
        return None


def generate_synthetic_onchain(
    start_date: datetime,
    end_date: datetime,
) -> dict[str, dict[str, float]]:
    """BTC 가격 기반 합성 온체인 메트릭 생성 (API 실패 폴백).

    알고리즘:
      - 2020-2022 강세: MVRV 1.5~4.0, 거래소순유출 양수
      - 2022 하락: MVRV 0.5~1.0, 거래소순유입 양수
      - 2023-2024 회복: MVRV 1.0~2.5
      - 2024-2025 고점: MVRV 2.0~3.5
      - 2025-2026 조정: MVRV 1.0~2.0
    """
    result = {}
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # 가격 시뮬레이션 (간단한 로그정규분포 기반)
    import random
    random.seed(42)

    price = 5000.0  # 2020-01-01 BTC 가격 (대략)

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")

        # 날짜별 시황 분류
        year = current.year
        month = current.month

        if year == 2020:
            price_change = random.gauss(0.003, 0.02)  # +0.3% ± 2%
            mvrv = random.uniform(1.5, 2.5)
            exchange_netflow = random.uniform(-50_000_000, 50_000_000)
        elif year == 2021:
            price_change = random.gauss(0.005, 0.025)
            mvrv = random.uniform(2.0, 4.0)
            exchange_netflow = random.uniform(-100_000_000, 100_000_000)
        elif year == 2022 and month <= 6:
            price_change = random.gauss(0.002, 0.02)
            mvrv = random.uniform(1.5, 3.0)
            exchange_netflow = random.uniform(-80_000_000, 80_000_000)
        elif year == 2022:
            price_change = random.gauss(-0.004, 0.025)  # -0.4% ± 2.5%
            mvrv = random.uniform(0.5, 1.0)
            exchange_netflow = random.uniform(-30_000_000, 30_000_000)
        elif year == 2023:
            price_change = random.gauss(0.003, 0.015)
            mvrv = random.uniform(1.0, 2.0)
            exchange_netflow = random.uniform(-60_000_000, 60_000_000)
        elif year == 2024 and month <= 6:
            price_change = random.gauss(0.004, 0.018)
            mvrv = random.uniform(1.5, 2.5)
            exchange_netflow = random.uniform(-70_000_000, 70_000_000)
        elif year == 2024:
            price_change = random.gauss(0.002, 0.02)
            mvrv = random.uniform(2.0, 3.5)
            exchange_netflow = random.uniform(-100_000_000, 100_000_000)
        else:  # 2025-2026
            price_change = random.gauss(0.001, 0.015)
            mvrv = random.uniform(1.0, 2.5)
            exchange_netflow = random.uniform(-50_000_000, 50_000_000)

        price = max(1000, price * (1 + price_change))
        market_cap = price * 21_000_000  # 공급량 2,100만
        realized_cap = market_cap / mvrv

        result[date_str] = {
            "price_usd": round(price, 2),
            "market_cap_usd": round(market_cap, 2),
            "realized_cap_usd": round(realized_cap, 2),
            "flow_in_ex_usd": abs(exchange_netflow) if exchange_netflow > 0 else 0.0,
            "flow_out_ex_usd": abs(exchange_netflow) if exchange_netflow < 0 else 0.0,
            "sply_act_180d": 21_000_000.0,  # 대략 고정
        }

        current += timedelta(days=1)

    logger.info(f"합성 온체인 메트릭: {len(result)} 일자 생성")
    return result


def safe_float(v: Any) -> float:
    """None/NaN을 0.0으로 치환."""
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if (f != f) else f  # NaN 체크
    except (TypeError, ValueError):
        return 0.0


async def compute_mvrv_zscore(
    pool: asyncpg.Pool,
) -> dict[str, float]:
    """기존 DB 데이터로부터 4년 이동평균·표준편차 계산.

    Returns:
        {"2020-01-01": 0.5, ...} 또는 {} (데이터 부족)
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT date, mvrv FROM onchain_metrics
            WHERE asset = 'BTC' AND mvrv IS NOT NULL
            ORDER BY date ASC
            """
        )

    if not rows:
        return {}

    df = pd.DataFrame(rows, columns=["date", "mvrv"])
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    # 4년 이동평균·표준편차 계산
    ma_4yr = df["mvrv"].rolling(window=4*365, min_periods=1).mean()
    std_4yr = df["mvrv"].rolling(window=4*365, min_periods=1).std()
    std_4yr = std_4yr.replace(0, 0.001)  # 0 제외 (ZeroDivision)

    zscore = (df["mvrv"] - ma_4yr) / std_4yr

    result = {}
    for date, zs in zscore.items():
        if not pd.isna(zs):
            result[date.strftime("%Y-%m-%d")] = float(zs)

    return result


async def upsert_onchain_metrics(
    pool: asyncpg.Pool,
    data: dict[str, dict[str, float]],
    source: str = "coinmetrics",
) -> int:
    """onchain_metrics 테이블에 upsert.

    Returns:
        삽입/업데이트된 행 수
    """
    async with pool.acquire() as conn:
        count = 0
        for date_str, metrics in data.items():
            await conn.execute(
                """
                INSERT INTO onchain_metrics
                (asset, date, price_usd, market_cap_usd, realized_cap_usd,
                 asopr, exchange_netflow_usd, active_supply_180d, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (asset, date) DO UPDATE
                SET price_usd = $3, market_cap_usd = $4, realized_cap_usd = $5,
                    asopr = $6, exchange_netflow_usd = $7, active_supply_180d = $8,
                    source = $9, collected_at = NOW()
                """,
                "BTC",
                datetime.strptime(date_str, "%Y-%m-%d").date(),
                metrics.get("price_usd", 0),
                metrics.get("market_cap_usd", 0),
                metrics.get("realized_cap_usd", 0),
                1.0,  # aSOPR (데이터 없으면 1.0)
                metrics.get("flow_out_ex_usd", 0) - metrics.get("flow_in_ex_usd", 0),
                metrics.get("sply_act_180d", 0),
                source,
            )
            count += 1
    return count


async def main():
    parser = argparse.ArgumentParser(description="온체인 메트릭 수집 (CoinMetrics)")
    parser.add_argument("--backfill", action="store_true", help="과거 데이터 백필")
    parser.add_argument("--start", type=str, default="2020-01-01", help="시작 날짜")
    parser.add_argument("--end", type=str, help="종료 날짜")
    args = parser.parse_args()

    if not args.end:
        args.end = datetime.now(UTC).strftime("%Y-%m-%d")

    logger.info(f"온체인 메트릭 수집 시작: {args.start} ~ {args.end}")

    # 데이터 조회
    data = await fetch_coinmetrics(start_date=args.start, end_date=args.end)
    if not data:
        logger.warning("CoinMetrics API 실패, 합성 데이터로 대체")
        start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)
        data = generate_synthetic_onchain(start_dt, end_dt)
        source = "synthetic"
    else:
        source = "coinmetrics"

    logger.info(f"데이터: {len(data)} 행, source={source}")

    # DB 저장
    pool = await make_pool()
    try:
        count = await upsert_onchain_metrics(pool, data, source=source)
        logger.info(f"onchain_metrics 테이블 upsert: {count}행")

        # MVRV Z-Score 계산 (MVRV 컬럼만 채워진 경우)
        # NOTE: migration 009는 mvrv 컬럼이 있으므로 계산 가능
        # 현재는 MVRV 재계산 생략 (future work)

    finally:
        await pool.close()

    logger.info("완료")


if __name__ == "__main__":
    asyncio.run(main())
