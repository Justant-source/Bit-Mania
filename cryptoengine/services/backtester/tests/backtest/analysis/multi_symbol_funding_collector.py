"""multi_symbol_funding_collector.py — 멀티심볼 펀딩비 + OHLCV 수집기.

15개 알트코인의 3년치 펀딩비 + 1d OHLCV 데이터를 Bybit 공개 API로 수집.
기존 scripts/fetch_real_ohlcv.py의 패턴 재사용.

사용 예:
    python tests/backtest/analysis/multi_symbol_funding_collector.py --backfill --start 2023-04-01

심볼별 상장일:
    - BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT, DOGEUSDT, ADAUSDT, AVAXUSDT, LINKUSDT, MATICUSDT,
      DOTUSDT, LTCUSDT, NEARUSDT: 모두 2023-04-01 이전 거래 가능
    - APTUSDT: 2022-10 상장 (2023-04-01부터 데이터 수집)
    - ARBUSDT: 2023-03 상장 (2023-04-01부터 데이터 수집)
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

UNIVERSE = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "MATICUSDT",
    "DOTUSDT", "LTCUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT",
]

# Bybit interval 표기
TF_TO_INTERVAL = {"1d": "D"}

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


async def fetch_ohlcv_1d(session: aiohttp.ClientSession, pool: asyncpg.Pool,
                          symbol: str, start_ms: int, end_ms: int) -> int:
    """1d OHLCV 데이터 수집."""
    total = 0
    since_ms = start_ms

    while since_ms < end_ms:
        url = f"{BYBIT_PUBLIC}/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": "D",
            "start": since_ms,
            "end": min(since_ms + 200 * 86_400_000, end_ms),
            "limit": 200,
        }

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
        except asyncio.TimeoutError:
            print(f"  [TIMEOUT] {symbol} 1d OHLCV")
            break
        except Exception as e:
            print(f"  [ERROR] {symbol} 1d OHLCV: {e}")
            break

        if data.get("retCode") != 0:
            print(f"  [API ERROR] {symbol} 1d: {data.get('retMsg', 'Unknown')}")
            break

        candles = data.get("result", {}).get("list", [])
        if not candles:
            break

        # 오래된→최신 순으로 역정렬
        candles = sorted(candles, key=lambda c: int(c[0]))

        rows = [
            (
                "bybit", symbol, "1d",
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

        try:
            async with pool.acquire() as conn:
                await conn.executemany(UPSERT_OHLCV, rows)
        except Exception as e:
            print(f"  [DB ERROR] {symbol} 1d OHLCV insert: {e}")
            break

        total += len(rows)
        last_ts_ms = int(candles[-1][0])
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)
        print(f"  [{symbol} 1d] {total}개 저장 (마지막: {last_dt.strftime('%Y-%m-%d')})")

        if last_ts_ms >= end_ms - 86_400_000:
            break
        since_ms = last_ts_ms + 86_400_000
        await asyncio.sleep(0.5)

    return total


async def fetch_funding(session: aiohttp.ClientSession, pool: asyncpg.Pool,
                        symbol: str, start_ms: int, end_ms: int) -> int:
    """펀딩비 데이터 수집 (8h 정산)."""
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

        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
        except asyncio.TimeoutError:
            print(f"  [TIMEOUT] {symbol} funding")
            break
        except Exception as e:
            print(f"  [ERROR] {symbol} funding: {e}")
            break

        if data.get("retCode") != 0:
            print(f"  [API ERROR] {symbol} funding: {data.get('retMsg', 'Unknown')}")
            break

        records = data.get("result", {}).get("list", [])
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

        try:
            async with pool.acquire() as conn:
                await conn.executemany(UPSERT_FUNDING, rows)
        except Exception as e:
            print(f"  [DB ERROR] {symbol} funding insert: {e}")
            break

        total += len(rows)
        last_ts_ms = int(records[-1]["fundingRateTimestamp"])
        last_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc)
        print(f"  [{symbol} funding] {total}개 저장 (마지막: {last_dt.strftime('%Y-%m-%d %H:%M')})")

        if last_ts_ms >= end_ms - 8 * 3600 * 1000:
            break
        since_ms = last_ts_ms + 1
        await asyncio.sleep(0.5)

    return total


async def main():
    parser = argparse.ArgumentParser(description="멀티심볼 펀딩비 + OHLCV 수집기")
    parser.add_argument("--backfill", action="store_true", help="2023-04-01부터 현재까지 백필")
    parser.add_argument("--start", default="2023-04-01", help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="종료 날짜 (YYYY-MM-DD, 기본: 오늘)")
    args = parser.parse_args()

    if not args.backfill:
        print("--backfill 옵션이 없으면 실행하지 않습니다.")
        return

    # 날짜 파싱
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(tz=timezone.utc)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print(f"범위: {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')} ({len(UNIVERSE)}개 심볼)")
    print()

    # DB 연결
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as e:
        print(f"[ERROR] DB 연결 실패: {e}")
        return

    async with aiohttp.ClientSession() as session:
        for symbol in UNIVERSE:
            print(f"[{symbol}]")
            try:
                # 펀딩비 수집
                funding_count = await fetch_funding(session, pool, symbol, start_ms, end_ms)
                # OHLCV 수집
                ohlcv_count = await fetch_ohlcv_1d(session, pool, symbol, start_ms, end_ms)
                print(f"  → 완료: 펀딩비 {funding_count}개, OHLCV 1d {ohlcv_count}개")
            except Exception as e:
                print(f"  [ERROR] {symbol}: {e}")
            print()
            await asyncio.sleep(1)

    await pool.close()
    print("✓ 모든 심볼 수집 완료")


if __name__ == "__main__":
    asyncio.run(main())
