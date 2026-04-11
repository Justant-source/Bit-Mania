#!/usr/bin/env python3
"""
청산 데이터 수집기 (Liquidation Collector)

BT_TASK_07 청산 캐스케이드 역발상 전략을 위한 청산 히스토리 수집

공개 API (Coinglass, Binance)로 청산 데이터를 수집하되, 신뢰성 부족 시
1h OHLCV 기반 간접 추정으로 보완합니다.

데이터 소스 우선순위:
1. Coinglass API (무료, 신뢰도 중)
2. Binance API (최근 7일, 신뢰도 높음)
3. 1h OHLCV 간접 추정 (신뢰도 낮음, 폴백 용도)
"""
import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
import json

import aiohttp
import pandas as pd
import numpy as np

# DB 연결
from tests.backtest.core import make_pool
from tests.backtest.core.loader import load_ohlcv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KST = timezone(timedelta(hours=9))


class LiquidationCollector:
    """청산 데이터 수집 및 저장"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.pool = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        self.pool = await make_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.pool:
            await self.pool.close()

    async def fetch_coinglass(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """
        Coinglass API에서 청산 데이터 수집
        https://open-api.coinglass.com/public/v2/liquidation_history?symbol=BTC&interval=1h

        응답 예:
        {
            "data": [
                {"timestamp": 1234567890000, "long_liquidations": 1000000, "short_liquidations": 500000}
            ]
        }
        """
        logger.info(f"Coinglass에서 {symbol} 청산 데이터 수집 ({start_date} ~ {end_date})")

        url = "https://open-api.coinglass.com/public/v2/liquidation_history"
        params = {
            "symbol": symbol.replace("USDT", ""),  # BTC (not BTCUSDT)
            "interval": "1h",
        }

        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning(f"Coinglass API 실패: {resp.status}")
                    return pd.DataFrame()

                data = await resp.json()
                if "data" not in data or not data["data"]:
                    logger.info("Coinglass: 데이터 없음")
                    return pd.DataFrame()

                records = []
                for item in data["data"]:
                    ts = datetime.fromtimestamp(item.get("timestamp", 0) / 1000, tz=KST)
                    if start_date <= ts <= end_date:
                        records.append({
                            "timestamp": ts,
                            "long_liquidations_usd": Decimal(str(item.get("long_liquidations", 0))),
                            "short_liquidations_usd": Decimal(str(item.get("short_liquidations", 0))),
                            "total_liquidations_usd": Decimal(str(item.get("long_liquidations", 0)))
                                                      + Decimal(str(item.get("short_liquidations", 0))),
                            "source": "coinglass",
                        })

                df = pd.DataFrame(records)
                logger.info(f"Coinglass: {len(df)}개 레코드 수집")
                return df
        except Exception as e:
            logger.error(f"Coinglass 수집 실패: {e}")
            return pd.DataFrame()

    async def fetch_binance(self, start_date: datetime) -> pd.DataFrame:
        """
        Binance FAPI forceOrders (최근 7일)
        https://fapi.binance.com/fapi/v1/forceOrders?symbol=BTCUSDT

        응답 예:
        [
            {
                "symbol": "BTCUSDT",
                "time": 1234567890000,
                "qty": 100.0,
                "price": 50000.0,
                "executedQty": 100.0,
                "executedQuoteAsset": 5000000.0,
                "side": "BUY"  # liquidator가 매입 = long이 청산됨
            }
        ]
        """
        logger.info(f"Binance forceOrders 수집 (최근 7일)")

        url = "https://fapi.binance.com/fapi/v1/forceOrders"
        params = {"symbol": "BTCUSDT", "limit": 1000}

        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning(f"Binance API 실패: {resp.status}")
                    return pd.DataFrame()

                data = await resp.json()
                if not data:
                    logger.info("Binance: 데이터 없음")
                    return pd.DataFrame()

                # 시간별 집계
                agg = {}
                for item in data:
                    ts = datetime.fromtimestamp(item["time"] / 1000, tz=KST)
                    ts_rounded = ts.replace(minute=0, second=0, microsecond=0)

                    executed_quote = Decimal(str(item.get("executedQuoteAsset", 0)))
                    if item["side"] == "BUY":  # liquidator가 매입 → long 청산
                        long_liq = executed_quote
                        short_liq = Decimal(0)
                    else:  # short 청산
                        long_liq = Decimal(0)
                        short_liq = executed_quote

                    if ts_rounded not in agg:
                        agg[ts_rounded] = {
                            "timestamp": ts_rounded,
                            "long_liquidations_usd": Decimal(0),
                            "short_liquidations_usd": Decimal(0),
                            "source": "binance",
                        }
                    agg[ts_rounded]["long_liquidations_usd"] += long_liq
                    agg[ts_rounded]["short_liquidations_usd"] += short_liq

                df = pd.DataFrame([
                    {**rec, "total_liquidations_usd": rec["long_liquidations_usd"] + rec["short_liquidations_usd"]}
                    for rec in agg.values()
                ])
                logger.info(f"Binance: {len(df)}개 시간대 집계")
                return df
        except Exception as e:
            logger.error(f"Binance 수집 실패: {e}")
            return pd.DataFrame()

    async def estimate_from_ohlcv(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """
        1h OHLCV 기반 간접 추정

        논리:
        - 4h 누적 가격 변화 < -3% AND 거래량 > 24h 평균 × 2.0 → 롱 청산 추정
        - 반대 → 숏 청산 추정
        - 추정값 = |price_change| × close × volume × 보정계수(0.1)
        """
        logger.info(f"OHLCV 기반 간접 추정: {symbol} ({start_date} ~ {end_date})")

        try:
            df = await load_ohlcv(
                self.pool, symbol, timeframe="1h", start=start_date, end=end_date
            )
            if df.empty:
                logger.warning(f"OHLCV 데이터 없음: {symbol}")
                return pd.DataFrame()

            # 4h 롤링 (4개의 1h 봉), 인덱스를 컬럼으로 변환
            df = df.reset_index()
            df = df.rename(columns={"ts": "open_time"})

            # 4h 누적 지표
            df["close_4h_ago"] = df["close"].shift(4)
            df["price_change_4h"] = (df["close"] - df["close_4h_ago"]) / df["close_4h_ago"]
            df["volume_ma24"] = df["volume"].rolling(24, min_periods=1).mean()
            df["volume_ratio"] = df["volume"] / df["volume_ma24"]

            # 추정 로직
            results = []
            for idx, row in df.iterrows():
                if idx < 4:  # 4h 데이터 충분하지 않음
                    continue

                timestamp = row["open_time"]
                close = row["close"]
                price_change = row["price_change_4h"]
                volume_ratio = row["volume_ratio"]

                # 청산 규모 추정 (추정값)
                if price_change < -0.03 and volume_ratio > 2.0:
                    # 큰 하락 + 고거래량 → 롱 청산
                    liq_proxy = abs(price_change) * close * row["volume"] * 0.1
                    long_liq = Decimal(str(liq_proxy))
                    short_liq = Decimal(0)
                elif price_change > 0.03 and volume_ratio > 2.0:
                    # 큰 상승 + 고거래량 → 숏 청산
                    liq_proxy = abs(price_change) * close * row["volume"] * 0.1
                    long_liq = Decimal(0)
                    short_liq = Decimal(str(liq_proxy))
                else:
                    continue  # 신호 없음

                results.append({
                    "timestamp": timestamp,
                    "long_liquidations_usd": long_liq,
                    "short_liquidations_usd": short_liq,
                    "total_liquidations_usd": long_liq + short_liq,
                    "source": "proxy",
                })

            df_result = pd.DataFrame(results)
            logger.info(f"추정: {len(df_result)}개 신호 탐지")
            return df_result
        except Exception as e:
            logger.error(f"OHLCV 추정 실패: {e}")
            return pd.DataFrame()

    async def save_to_db(self, df: pd.DataFrame, symbol: str = "BTCUSDT") -> int:
        """DB에 저장 (중복 제거)"""
        if df.empty:
            return 0

        count = 0
        for _, row in df.iterrows():
            try:
                await self.pool.execute(
                    """
                    INSERT INTO liquidation_history
                    (exchange, symbol, timestamp, long_liquidations_usd, short_liquidations_usd,
                     total_liquidations_usd, oi_change_pct, source)
                    VALUES ($1, $2, $3, $4, $5, $6, NULL, $7)
                    ON CONFLICT (exchange, symbol, timestamp, source) DO NOTHING
                    """,
                    "binance", symbol,
                    row["timestamp"],
                    float(row.get("long_liquidations_usd", 0)),
                    float(row.get("short_liquidations_usd", 0)),
                    float(row.get("total_liquidations_usd", 0)),
                    row.get("source", "unknown"),
                )
                count += 1
            except Exception as e:
                logger.error(f"DB 저장 실패: {e}")

        return count

    async def backfill(self, start_date: datetime, sources: list = None):
        """
        역사 데이터 백필

        sources: ["coinglass", "binance", "proxy"]
        """
        if sources is None:
            sources = ["proxy"]  # 기본값: proxy 추정

        end_date = datetime.now(tz=KST)
        logger.info(f"백필 시작: {start_date} ~ {end_date}, 소스: {sources}")

        total_saved = 0

        if "coinglass" in sources:
            df = await self.fetch_coinglass("BTCUSDT", start_date, end_date)
            saved = await self.save_to_db(df)
            logger.info(f"Coinglass: {saved}개 저장")
            total_saved += saved

        if "binance" in sources:
            df = await self.fetch_binance(start_date)
            saved = await self.save_to_db(df)
            logger.info(f"Binance: {saved}개 저장")
            total_saved += saved

        if "proxy" in sources:
            df = await self.estimate_from_ohlcv("BTCUSDT", start_date, end_date)
            saved = await self.save_to_db(df)
            logger.info(f"Proxy: {saved}개 저장")
            total_saved += saved

        logger.info(f"총 {total_saved}개 레코드 저장 완료")
        return total_saved


async def main():
    parser = argparse.ArgumentParser(description="청산 데이터 수집기")
    parser.add_argument("--backfill", action="store_true", help="역사 데이터 백필")
    parser.add_argument("--start", type=str, default="2023-04-01", help="시작일 (YYYY-MM-DD)")
    parser.add_argument("--sources", type=str, default="all",
                        help="데이터 소스 (all=coinglass,binance,proxy)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=KST)
    sources = {
        "all": ["coinglass", "binance", "proxy"],
        "coinglass": ["coinglass"],
        "binance": ["binance"],
        "proxy": ["proxy"],
    }.get(args.sources, ["proxy"])

    async with LiquidationCollector() as collector:
        if args.backfill:
            await collector.backfill(start_date, sources)
        else:
            logger.info("사용법: python liquidation_collector.py --backfill --start 2023-04-01 --sources all")


if __name__ == "__main__":
    asyncio.run(main())
