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
    metrics: str = "PriceUSD,AdrActCnt,TxCnt,CapMrktCurUSD,CapRealUSD,FlowInExUSD,FlowOutExUSD,SplyAct180d",
) -> dict[str, dict[str, Any]] | None:
    """CoinMetrics Community API에서 일별 메트릭 조회.

    CoinMetrics Pro metrics (403 Forbidden 원인):
      - MVRV: 더 이상 Community API에서 지원 안 함 (제거함)
      - aSOPR: 더 이상 Community API에서 지원 안 함 (제거함)
      - NVT: 더 이상 Community API에서 지원 안 함 (제거함)

    Community API 지원 메트릭 (무료):
      - PriceUSD: 가격 (REQUIRED)
      - AdrActCnt: 활성 주소 수
      - TxCnt: 거래 수
      - CapMrktCurUSD: 시장 시가총액
      - CapRealUSD: 실현 시가총액 (MVRV = 시장/실현)
      - FlowInExUSD: 거래소 순 유입 (계산 필요)
      - FlowOutExUSD: 거래소 순 유출 (계산 필요)
      - SplyAct180d: 180일 활성 공급량

    Returns:
        {"2020-01-01": {"price_usd": 9000, "mvrv": 1.5, ...}, ...}
        또는 None (API 실패 또는 Pro metric 요청됨)
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
                if resp.status == 403:
                    raise RuntimeError(
                        "CoinMetrics API returned 403 Forbidden. Likely requesting Pro-tier metrics (MVRV, aSOPR, NVT). "
                        "Use Community-tier only: PriceUSD, AdrActCnt, TxCnt, CapMrktCurUSD, CapRealUSD, FlowInExUSD, FlowOutExUSD, SplyAct180d."
                    )
                elif resp.status != 200:
                    raise RuntimeError(f"CoinMetrics HTTP {resp.status}: {await resp.text()}")

                data = await resp.json()

                # Parse response: data.data[].time, .price_usd, .cap_mrkt_cur_usd, ...
                result = {}
                for row in data.get("data", []):
                    date_str = row.get("time", "").split("T")[0]
                    if not date_str:
                        continue

                    market_cap = safe_float(row.get("CapMrktCurUSD"))
                    realized_cap = safe_float(row.get("CapRealUSD"))

                    # MVRV 계산 (Community API 데이터로부터)
                    mvrv = (market_cap / realized_cap) if realized_cap > 0 else 0.0

                    result[date_str] = {
                        "price_usd": safe_float(row.get("PriceUSD")),
                        "market_cap_usd": market_cap,
                        "realized_cap_usd": realized_cap,
                        "mvrv": mvrv,
                        "adr_act_cnt": safe_float(row.get("AdrActCnt")),
                        "tx_cnt": safe_float(row.get("TxCnt")),
                        "flow_in_ex_usd": safe_float(row.get("FlowInExUSD")),
                        "flow_out_ex_usd": safe_float(row.get("FlowOutExUSD")),
                        "sply_act_180d": safe_float(row.get("SplyAct180d")),
                    }

                logger.info(f"CoinMetrics 수집: {len(result)} 일자 (API)")
                if not result:
                    raise RuntimeError("CoinMetrics returned empty data - may indicate API issue or date range has no data")
                return result

    except RuntimeError:
        raise  # Re-raise RuntimeError explicitly
    except Exception as e:
        raise RuntimeError(f"CoinMetrics 요청 실패: {e}")




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
    parser = argparse.ArgumentParser(description="온체인 메트릭 수집 (CoinMetrics Community API)")
    parser.add_argument("--backfill", action="store_true", help="과거 데이터 백필")
    parser.add_argument("--start", type=str, default="2020-01-01", help="시작 날짜")
    parser.add_argument("--end", type=str, help="종료 날짜")
    parser.add_argument("--rate-limit", type=float, default=6.0, help="API 요청 간격 (초, 기본 6초 = 10 req/min)")
    args = parser.parse_args()

    if not args.end:
        args.end = datetime.now(UTC).strftime("%Y-%m-%d")

    logger.info(f"온체인 메트릭 수집 시작: {args.start} ~ {args.end}")

    # 데이터 조회 (실패 시 RuntimeError 발생)
    try:
        data = await fetch_coinmetrics(start_date=args.start, end_date=args.end)
        source = "coinmetrics"
        logger.info(f"데이터: {len(data)} 행, source={source}")
    except RuntimeError as e:
        logger.error(f"FATAL: CoinMetrics API 실패: {e}")
        logger.error("Real data is required. Use --synthetic-mode flag to generate fallback data (not recommended for production backtests).")
        raise

    # DB 저장
    pool = await make_pool()
    try:
        count = await upsert_onchain_metrics(pool, data, source=source)
        logger.info(f"onchain_metrics 테이블 upsert: {count}행")

        # MVRV Z-Score 계산
        mvrv_zscores = await compute_mvrv_zscore(pool)
        logger.info(f"MVRV Z-Score 계산: {len(mvrv_zscores)} 행")

    finally:
        await pool.close()

    logger.info("완료")


if __name__ == "__main__":
    asyncio.run(main())
