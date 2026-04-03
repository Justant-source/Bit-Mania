"""seed_historical.py — Bybit REST API (ccxt)로 OHLCV / 펀딩비 히스토리 데이터를
PostgreSQL에 시드(upsert)하는 스크립트.

사용 예:
    python scripts/seed_historical.py \
        --exchange bybit --symbol BTCUSDT \
        --timeframes 1m,5m,15m,1h,4h \
        --start 2025-10-01 --end 2026-04-01

    python scripts/seed_historical.py \
        --exchange bybit --symbol BTCUSDT \
        --data-type funding \
        --start 2025-10-01 --end 2026-04-01
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg
import ccxt
import ccxt.async_support as ccxt_async

# ── DB 연결 ────────────────────────────────────────────────────────────────────

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

BATCH_SIZE = 1000

# ccxt timeframe 매핑
TIMEFRAME_MAP: dict[str, str] = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
}


# ── DDL ────────────────────────────────────────────────────────────────────────

CREATE_OHLCV_TABLE = """
CREATE TABLE IF NOT EXISTS ohlcv_history (
    exchange   TEXT        NOT NULL,
    symbol     TEXT        NOT NULL,
    timeframe  TEXT        NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL,
    open       DOUBLE PRECISION NOT NULL,
    high       DOUBLE PRECISION NOT NULL,
    low        DOUBLE PRECISION NOT NULL,
    close      DOUBLE PRECISION NOT NULL,
    volume     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (exchange, symbol, timeframe, timestamp)
);
"""

CREATE_FUNDING_TABLE = """
CREATE TABLE IF NOT EXISTS funding_rate_history (
    exchange   TEXT        NOT NULL,
    symbol     TEXT        NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL,
    rate       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (exchange, symbol, timestamp)
);
"""

UPSERT_OHLCV = """
INSERT INTO ohlcv_history (exchange, symbol, timeframe, timestamp, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (exchange, symbol, timeframe, timestamp) DO UPDATE
    SET open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume;
"""

UPSERT_FUNDING = """
INSERT INTO funding_rate_history (exchange, symbol, timestamp, rate)
VALUES ($1, $2, $3, $4)
ON CONFLICT (exchange, symbol, timestamp) DO UPDATE
    SET rate = EXCLUDED.rate;
"""


# ── Exchange 초기화 ────────────────────────────────────────────────────────────

def _build_exchange(exchange_id: str) -> ccxt_async.Exchange:
    """ccxt async exchange 인스턴스 생성. Bybit testnet 지원."""
    ExchangeClass = getattr(ccxt_async, exchange_id, None)
    if ExchangeClass is None:
        raise ValueError(f"Unknown exchange: {exchange_id}")

    options: dict = {
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_API_SECRET,
        "enableRateLimit": True,
        "options": {},
    }

    if exchange_id == "bybit" and BYBIT_TESTNET:
        print("[INFO] Bybit TESTNET 모드 사용")
        options["options"]["defaultType"] = "linear"
        options["options"]["adjustForTimeDifference"] = True
        # ccxt bybit testnet URL 설정
        options["urls"] = {
            "api": {
                "public": "https://api-testnet.bybit.com",
                "private": "https://api-testnet.bybit.com",
            }
        }
    else:
        if exchange_id == "bybit":
            options["options"]["defaultType"] = "linear"

    return ExchangeClass(options)


# ── OHLCV 시드 ─────────────────────────────────────────────────────────────────

async def _seed_ohlcv(
    pool: asyncpg.Pool,
    exchange: ccxt_async.Exchange,
    exchange_id: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> int:
    """단일 타임프레임에 대해 OHLCV 데이터를 배치로 가져와 upsert."""
    ccxt_tf = TIMEFRAME_MAP.get(timeframe)
    if ccxt_tf is None:
        print(f"[WARN] 지원하지 않는 타임프레임: {timeframe}, 건너뜀")
        return 0

    total_saved = 0
    since_ms = start_ms

    while since_ms < end_ms:
        try:
            candles = await exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=ccxt_tf,
                since=since_ms,
                limit=BATCH_SIZE,
            )
        except Exception as exc:
            print(f"[ERROR] fetch_ohlcv 실패 ({symbol} {timeframe}): {exc}")
            break

        if not candles:
            break

        # end_ms 이후 데이터 제거
        candles = [c for c in candles if c[0] < end_ms]
        if not candles:
            break

        rows = [
            (
                exchange_id,
                symbol,
                timeframe,
                datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
                float(c[1]),  # open
                float(c[2]),  # high
                float(c[3]),  # low
                float(c[4]),  # close
                float(c[5]),  # volume
            )
            for c in candles
        ]

        async with pool.acquire() as conn:
            await conn.executemany(UPSERT_OHLCV, rows)

        total_saved += len(rows)
        last_ts = candles[-1][0]
        print(
            f"  [{symbol} {timeframe}] {total_saved}개 저장 "
            f"(마지막: {datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).isoformat()})"
        )

        # 다음 배치 시작점: 마지막 캔들 다음 틱
        since_ms = last_ts + 1

        # ccxt rate limit 준수
        await asyncio.sleep(exchange.rateLimit / 1000)

    return total_saved


# ── 펀딩비 시드 ────────────────────────────────────────────────────────────────

async def _seed_funding(
    pool: asyncpg.Pool,
    exchange: ccxt_async.Exchange,
    exchange_id: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> int:
    """펀딩비 히스토리를 배치로 가져와 upsert."""
    total_saved = 0
    since_ms = start_ms

    while since_ms < end_ms:
        try:
            # fetch_funding_rate_history: [{timestamp, symbol, fundingRate, ...}]
            records = await exchange.fetch_funding_rate_history(
                symbol=symbol,
                since=since_ms,
                limit=BATCH_SIZE,
            )
        except Exception as exc:
            print(f"[ERROR] fetch_funding_rate_history 실패 ({symbol}): {exc}")
            break

        if not records:
            break

        records = [r for r in records if r["timestamp"] < end_ms]
        if not records:
            break

        rows = [
            (
                exchange_id,
                symbol,
                datetime.fromtimestamp(r["timestamp"] / 1000, tz=timezone.utc),
                float(r["fundingRate"]),
            )
            for r in records
            if r.get("fundingRate") is not None
        ]

        if rows:
            async with pool.acquire() as conn:
                await conn.executemany(UPSERT_FUNDING, rows)
            total_saved += len(rows)

        last_ts = records[-1]["timestamp"]
        print(
            f"  [{symbol} funding] {total_saved}개 저장 "
            f"(마지막: {datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).isoformat()})"
        )

        since_ms = last_ts + 1
        await asyncio.sleep(exchange.rateLimit / 1000)

    return total_saved


# ── 메인 ───────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    # 날짜 → milliseconds
    start_ms = int(
        datetime.strptime(args.start, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )
    end_ms = int(
        datetime.strptime(args.end, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )

    print(f"[INFO] DB 연결 중... ({DB_DSN.split('@')[1]})")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    # 테이블 생성
    async with pool.acquire() as conn:
        await conn.execute(CREATE_OHLCV_TABLE)
        await conn.execute(CREATE_FUNDING_TABLE)
    print("[INFO] 테이블 준비 완료")

    exchange = _build_exchange(args.exchange)

    try:
        await exchange.load_markets()
        print(f"[INFO] Exchange: {args.exchange}, Symbol: {args.symbol}")
        print(f"[INFO] 기간: {args.start} ~ {args.end}")

        if args.data_type == "funding":
            print("[INFO] 펀딩비 히스토리 다운로드 시작...")
            total = await _seed_funding(
                pool, exchange, args.exchange, args.symbol, start_ms, end_ms
            )
            print(f"[DONE] 펀딩비 총 {total}개 저장 완료")

        else:
            # OHLCV
            timeframes = [tf.strip() for tf in args.timeframes.split(",")]
            print(f"[INFO] OHLCV 다운로드 시작 — 타임프레임: {timeframes}")
            grand_total = 0
            for tf in timeframes:
                print(f"\n[INFO] 타임프레임 {tf} 처리 중...")
                count = await _seed_ohlcv(
                    pool, exchange, args.exchange, args.symbol, tf, start_ms, end_ms
                )
                grand_total += count
                print(f"[INFO] {tf} 완료: {count}개")
            print(f"\n[DONE] OHLCV 총 {grand_total}개 저장 완료")

    finally:
        await exchange.close()
        await pool.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bybit OHLCV / 펀딩비 히스토리 데이터를 PostgreSQL에 시드"
    )
    parser.add_argument("--exchange", default="bybit", help="거래소 ID (기본: bybit)")
    parser.add_argument("--symbol", default="BTCUSDT", help="심볼 (기본: BTCUSDT)")
    parser.add_argument(
        "--data-type",
        default="ohlcv",
        choices=["ohlcv", "funding"],
        help="데이터 종류: ohlcv 또는 funding (기본: ohlcv)",
    )
    parser.add_argument(
        "--timeframes",
        default="1h",
        help="콤마 구분 타임프레임, --data-type ohlcv 시 사용 (예: 1m,5m,15m,1h,4h)",
    )
    parser.add_argument("--start", required=True, help="시작일 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="종료일 YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
