"""fetch_real_ohlcv.py — Bybit 공개 REST API로 실제 OHLCV / 펀딩비 데이터 수집.

인증 불필요 (공개 마켓 데이터 엔드포인트 사용).
테스트넷 API 키로 메인넷 히스토리 데이터를 받을 수 없는 문제를 우회.

기본 범위: 3년 (2023-04-01 ~ 2026-04-01), BTCUSDT, 1h/4h/15m

사용 예:
    # 기본 (3년, BTCUSDT, 1h/4h/15m)
    python scripts/fetch_real_ohlcv.py

    # 커스텀 기간
    python scripts/fetch_real_ohlcv.py --start 2023-04-01 --end 2026-04-01

    # 심볼 / 타임프레임 지정
    python scripts/fetch_real_ohlcv.py --symbol ETHUSDT --timeframes 1h,4h
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone

import aiohttp
import asyncpg

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

BYBIT_PUBLIC = "https://api.bybit.com"
SYMBOL = "BTCUSDT"
START = "2023-04-01"
END   = "2026-04-01"
TIMEFRAMES = ["1h", "4h", "15m"]

# Bybit interval 표기 (1d=D, 6h=360, 4h=240, 1h=60 ...)
TF_TO_INTERVAL = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240", "6h": "360", "1d": "D"}

UPSERT_OHLCV = """
INSERT INTO ohlcv_history (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (exchange, symbol, timeframe, timestamp) DO UPDATE
    SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
        close=EXCLUDED.close, volume=EXCLUDED.volume;
"""

UPSERT_FUNDING = """
INSERT INTO funding_rate_history (exchange, symbol, timestamp, rate)
VALUES ($1, $2, $3, $4)
ON CONFLICT (exchange, symbol, timestamp) DO UPDATE
    SET rate = EXCLUDED.rate;
"""


async def fetch_ohlcv(session: aiohttp.ClientSession, pool: asyncpg.Pool,
                      symbol: str, timeframe: str, start_ms: int, end_ms: int) -> int:
    interval = TF_TO_INTERVAL.get(timeframe)
    if not interval:
        print(f"[WARN] 지원하지 않는 타임프레임: {timeframe}")
        return 0

    total = 0
    since_ms = start_ms

    while since_ms < end_ms:
        url = f"{BYBIT_PUBLIC}/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "start": since_ms,
            "end": min(since_ms + 200 * _tf_ms(timeframe), end_ms),
            "limit": 200,
        }

        async with session.get(url, params=params) as resp:
            data = await resp.json()

        if data.get("retCode") != 0:
            print(f"[ERROR] {data}")
            break

        candles = data["result"]["list"]  # 최신→과거 순서
        if not candles:
            break

        # 오래된→최신 순으로 역정렬
        candles = sorted(candles, key=lambda c: int(c[0]))

        rows = [
            (
                "bybit", symbol, timeframe,
                datetime.fromtimestamp(int(c[0]) / 1000, tz=timezone.utc),
                float(c[1]),  # open
                float(c[2]),  # high
                float(c[3]),  # low
                float(c[4]),  # close
                float(c[5]),  # volume
            )
            for c in candles
            if int(c[0]) < end_ms
        ]

        if not rows:
            break

        async with pool.acquire() as conn:
            await conn.executemany(UPSERT_OHLCV, rows)

        total += len(rows)
        last_ts_ms = int(candles[-1][0])
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)
        print(f"  [{symbol} {timeframe}] {total}개 저장 (마지막: {last_dt.astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%dT%H:%M:%S+09:00')})")

        if last_ts_ms >= end_ms - _tf_ms(timeframe):
            break
        since_ms = last_ts_ms + _tf_ms(timeframe)
        await asyncio.sleep(0.1)

    return total


async def fetch_funding(session: aiohttp.ClientSession, pool: asyncpg.Pool,
                        symbol: str, start_ms: int, end_ms: int) -> int:
    total = 0
    since_ms = start_ms

    while since_ms < end_ms:
        url = f"{BYBIT_PUBLIC}/v5/market/funding/history"
        params = {
            "category": "linear",
            "symbol": symbol,
            "startTime": since_ms,
            "endTime": min(since_ms + 200 * 8 * 3600 * 1000, end_ms),
            "limit": 200,
        }

        async with session.get(url, params=params) as resp:
            data = await resp.json()

        if data.get("retCode") != 0:
            print(f"[ERROR] funding {data}")
            break

        records = data["result"]["list"]
        if not records:
            break

        records = sorted(records, key=lambda r: int(r["fundingRateTimestamp"]))

        rows = [
            (
                "bybit", symbol,
                datetime.fromtimestamp(int(r["fundingRateTimestamp"]) / 1000, tz=timezone.utc),
                float(r["fundingRate"]),
            )
            for r in records
            if int(r["fundingRateTimestamp"]) < end_ms
        ]

        if not rows:
            break

        async with pool.acquire() as conn:
            await conn.executemany(UPSERT_FUNDING, rows)

        total += len(rows)
        last_ts_ms = int(records[-1]["fundingRateTimestamp"])
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)
        print(f"  [{symbol} funding] {total}개 저장 (마지막: {last_dt.astimezone(timezone(timedelta(hours=9))).strftime('%Y-%m-%dT%H:%M:%S+09:00')})")

        if last_ts_ms >= end_ms - 8 * 3600 * 1000:
            break
        since_ms = last_ts_ms + 1
        await asyncio.sleep(0.1)

    return total


def _tf_ms(timeframe: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    if timeframe == "1d":
        return 86_400_000
    return int(timeframe[:-1]) * units[timeframe[-1]]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bybit 공개 REST API로 실제 OHLCV / 펀딩비 데이터 수집"
    )
    parser.add_argument(
        "--start",
        default=START,
        metavar="YYYY-MM-DD",
        help=f"수집 시작일 (기본: {START})",
    )
    parser.add_argument(
        "--end",
        default=END,
        metavar="YYYY-MM-DD",
        help=f"수집 종료일 (기본: {END})",
    )
    parser.add_argument(
        "--symbol",
        default=SYMBOL,
        help=f"거래 심볼 (기본: {SYMBOL})",
    )
    parser.add_argument(
        "--timeframes",
        default=",".join(TIMEFRAMES),
        metavar="TF1,TF2,...",
        help=f"쉼표 구분 타임프레임 (기본: {','.join(TIMEFRAMES)})",
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    start_ms = int(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    symbol = args.symbol
    timeframes = [tf.strip() for tf in args.timeframes.split(",") if tf.strip()]

    print(f"[INFO] DB 연결 중...")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    async with aiohttp.ClientSession() as session:
        # OHLCV
        for tf in timeframes:
            print(f"\n[INFO] OHLCV {tf} 다운로드 중...")
            count = await fetch_ohlcv(session, pool, symbol, tf, start_ms, end_ms)
            print(f"[INFO] {tf} 완료: {count}개")

        # 펀딩비
        print(f"\n[INFO] 펀딩비 히스토리 다운로드 중...")
        count = await fetch_funding(session, pool, symbol, start_ms, end_ms)
        print(f"[INFO] 펀딩비 완료: {count}개")

    await pool.close()
    print("\n[DONE] 모든 데이터 다운로드 완료!")


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(args))
