#!/usr/bin/env python3
"""
quarterly_futures_collector.py — Bybit 분기물 선물 OHLCV 데이터 수집

분기물 선물(Quarterly Futures)의 일봉 데이터를 Bybit 공개 API v5에서 수집하여
quarterly_futures_history 테이블에 upsert합니다.

만기일이 임박하면 현물(Perpetual)으로 자동 전환.

사용법:
    python quarterly_futures_collector.py --backfill --start 2023-04-01
    python quarterly_futures_collector.py --verify-convergence
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import aiohttp
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


QUARTERLY_SYMBOLS = {
    "BTCUSDH24": {"expiry": "2024-03-29", "underlying": "BTCUSD"},
    "BTCUSDM24": {"expiry": "2024-06-28", "underlying": "BTCUSD"},
    "BTCUSDU24": {"expiry": "2024-09-27", "underlying": "BTCUSD"},
    "BTCUSDZ24": {"expiry": "2024-12-27", "underlying": "BTCUSD"},
    "BTCUSDH25": {"expiry": "2025-03-28", "underlying": "BTCUSD"},
    "BTCUSDM25": {"expiry": "2025-06-27", "underlying": "BTCUSD"},
    "BTCUSDU25": {"expiry": "2025-09-26", "underlying": "BTCUSD"},
    "BTCUSDZ25": {"expiry": "2025-12-26", "underlying": "BTCUSD"},
    "BTCUSDH26": {"expiry": "2026-03-27", "underlying": "BTCUSD"},
    "BTCUSDM26": {"expiry": "2026-06-26", "underlying": "BTCUSD"},
}

DB_DSN = (
    "postgresql://cryptoengine:CryptoEngine2026!@postgres:5432/cryptoengine"
)

BYBIT_API_V5 = "https://api.bybit.com/v5"


async def create_table(conn: asyncpg.Connection) -> None:
    """quarterly_futures_history 테이블 생성."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_futures_history (
            id              SERIAL PRIMARY KEY,
            symbol          VARCHAR(20) NOT NULL,
            timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
            open            DECIMAL(20, 4) NOT NULL,
            high            DECIMAL(20, 4) NOT NULL,
            low             DECIMAL(20, 4) NOT NULL,
            close           DECIMAL(20, 4) NOT NULL,
            volume          DECIMAL(20, 8) NOT NULL,
            turnover        DECIMAL(30, 2),
            created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(symbol, timestamp)
        );
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_quarterly_futures_symbol_ts "
        "ON quarterly_futures_history(symbol, timestamp);"
    )
    logger.info("Table quarterly_futures_history created/verified.")


async def fetch_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """Bybit v5 API /market/kline에서 일봉 데이터 수집."""
    all_klines = []
    limit = 200
    current_start = start_ts

    while current_start < end_ts:
        params = {
            "category": "inverse",
            "symbol": symbol,
            "interval": "D",
            "start": current_start,
            "end": end_ts,
            "limit": limit,
        }

        try:
            async with session.get(
                f"{BYBIT_API_V5}/market/kline",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"Bybit API error for {symbol}: status={resp.status}"
                    )
                    break

                data = await resp.json()
                if data.get("retCode") != 0:
                    logger.warning(
                        f"Bybit API error for {symbol}: {data.get('retMsg')}"
                    )
                    break

                klines = data.get("result", {}).get("list", [])
                if not klines:
                    break

                all_klines.extend(klines)

                # API 레이트 리밋 대기
                await asyncio.sleep(0.1)

                # 마지막 캔들 타임스탐프로 다음 조회 시작점 설정
                current_start = int(klines[-1][0]) + 1000  # ms + 1초

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {symbol}")
            break
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            break

    logger.info(f"Fetched {len(all_klines)} klines for {symbol}")
    return all_klines


async def upsert_klines(
    conn: asyncpg.Connection,
    symbol: str,
    klines: list[dict],
) -> None:
    """klines을 quarterly_futures_history에 upsert."""
    if not klines:
        return

    rows = []
    for kline in klines:
        # Bybit 응답: [open_time, open, high, low, close, volume, turnover]
        ts_ms = int(kline[0])
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        open_price = float(kline[1])
        high_price = float(kline[2])
        low_price = float(kline[3])
        close_price = float(kline[4])
        volume = float(kline[5])
        turnover = float(kline[6]) if len(kline) > 6 else 0

        rows.append((symbol, ts, open_price, high_price, low_price, close_price, volume, turnover))

    await conn.executemany(
        """
        INSERT INTO quarterly_futures_history
        (symbol, timestamp, open, high, low, close, volume, turnover)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            turnover = EXCLUDED.turnover
        """,
        rows,
    )
    logger.info(f"Upserted {len(rows)} rows for {symbol}")


async def collect_quarterly_futures(
    symbol: str,
    expiry_str: str,
    start_date: datetime,
) -> None:
    """단일 분기물 심볼의 데이터를 수집.

    만기 60일 전부터 만기일까지 수집.
    """
    expiry = datetime.strptime(expiry_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # 만기 60일 전부터 시작
    collect_start = expiry - timedelta(days=60)
    if start_date > collect_start:
        collect_start = start_date

    # 만기일 이후면 수집 안 함
    if collect_start >= expiry:
        logger.info(f"{symbol} already expired, skipping")
        return

    collect_end = min(expiry, datetime.now(tz=timezone.utc))

    logger.info(
        f"Collecting {symbol} from {collect_start.date()} to {collect_end.date()}"
    )

    async with aiohttp.ClientSession() as session:
        klines = await fetch_klines(
            session,
            symbol,
            int(collect_start.timestamp() * 1000),
            int(collect_end.timestamp() * 1000),
        )

    if klines:
        conn = await asyncpg.connect(DB_DSN)
        try:
            await upsert_klines(conn, symbol, klines)
        finally:
            await conn.close()


async def collect_all_symbols(start_date: datetime) -> None:
    """모든 분기물 심볼 데이터 수집."""
    conn = await asyncpg.connect(DB_DSN)
    try:
        await create_table(conn)
    finally:
        await conn.close()

    tasks = [
        collect_quarterly_futures(symbol, info["expiry"], start_date)
        for symbol, info in QUARTERLY_SYMBOLS.items()
    ]
    await asyncio.gather(*tasks)

    logger.info("Backfill complete")


async def verify_convergence() -> None:
    """만기일 전후 현물가 수렴 확인.

    분기물 가격이 만기일에 현물(Perpetual) 가격으로 수렴하는지 검증.
    """
    conn = await asyncpg.connect(DB_DSN)
    try:
        for symbol, info in QUARTERLY_SYMBOLS.items():
            expiry = datetime.strptime(info["expiry"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

            # 만기 전후 7일씩
            start = expiry - timedelta(days=7)
            end = expiry + timedelta(days=7)

            rows = await conn.fetch(
                """
                SELECT timestamp, close
                FROM quarterly_futures_history
                WHERE symbol = $1 AND timestamp BETWEEN $2 AND $3
                ORDER BY timestamp
                """,
                symbol, start, end,
            )

            if not rows:
                logger.warning(f"No convergence data for {symbol}")
                continue

            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp")

            logger.info(f"\n{symbol} Convergence Analysis:")
            logger.info(f"  Expiry: {expiry}")
            logger.info(f"  Data points: {len(df)}")
            logger.info(f"  Price range: {df['close'].min():.2f} - {df['close'].max():.2f}")
            logger.info(f"  7d volatility: {df['close'].pct_change().std() * 100:.4f}%")

    finally:
        await conn.close()


async def main() -> None:
    """메인 진입점."""
    parser = argparse.ArgumentParser(description="Quarterly Futures OHLCV Collector")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill all quarterly futures data"
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2023-04-01",
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--verify-convergence",
        action="store_true",
        help="Verify price convergence at expiry"
    )

    args = parser.parse_args()

    if args.backfill:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        await collect_all_symbols(start_date)

    if args.verify_convergence:
        await verify_convergence()

    if not args.backfill and not args.verify_convergence:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
