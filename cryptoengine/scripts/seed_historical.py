#!/usr/bin/env python3
"""Seed historical OHLCV and funding-rate data from Bybit REST API.

Downloads the last 6 months of data (configurable) and upserts into
the ``ohlcv_history`` and ``funding_rate_history`` tables.

Usage:
  python scripts/seed_historical.py
  python scripts/seed_historical.py --symbols BTCUSDT ETHUSDT --months 3
  python scripts/seed_historical.py --timeframes 1h 4h --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import logging

import asyncpg
import structlog
from shared.timezone_utils import kst_timestamper
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        kst_timestamper,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("seed_historical")

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_TIMEFRAMES = ["1h"]
EXCHANGE = "bybit"

# Bybit REST API limits
OHLCV_BATCH_SIZE = 200
FUNDING_BATCH_SIZE = 200

# Timeframe to milliseconds
TF_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)


# ------------------------------------------------------------------
# Bybit REST client (minimal, no ccxt dependency)
# ------------------------------------------------------------------

async def _fetch_json(session: Any, url: str, params: dict[str, Any]) -> dict[str, Any]:
    """Fetch JSON from Bybit REST API with retries."""
    import aiohttp

    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        return data.get("result", {})
                    log.warning("bybit_api_error", code=data.get("retCode"), msg=data.get("retMsg"))
                else:
                    log.warning("bybit_http_error", status=resp.status)
        except Exception as exc:
            log.warning("bybit_request_error", attempt=attempt + 1, error=str(exc))
        await asyncio.sleep(1 * (attempt + 1))

    return {}


async def _fetch_ohlcv(
    session: Any,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """Fetch OHLCV candles from Bybit v5 API in batches."""
    url = "https://api.bybit.com/v5/market/kline"
    all_candles: list[dict[str, Any]] = []
    cursor_ms = start_ms

    interval_map = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}
    interval = interval_map.get(timeframe, "60")
    tf_ms = TF_MS.get(timeframe, 3_600_000)

    while cursor_ms < end_ms:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "start": cursor_ms,
            "end": min(cursor_ms + tf_ms * OHLCV_BATCH_SIZE, end_ms),
            "limit": OHLCV_BATCH_SIZE,
        }
        result = await _fetch_json(session, url, params)
        candles = result.get("list", [])

        if not candles:
            break

        for c in candles:
            # Bybit returns [ts, open, high, low, close, volume, turnover]
            ts = int(c[0])
            if start_ms <= ts <= end_ms:
                all_candles.append({
                    "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                })

        # Bybit returns newest first, so advance cursor
        oldest_ts = min(int(c[0]) for c in candles)
        newest_ts = max(int(c[0]) for c in candles)

        if newest_ts <= cursor_ms:
            break
        cursor_ms = newest_ts + tf_ms

        # Rate limiting
        await asyncio.sleep(0.1)

    # Deduplicate by timestamp
    seen: set[datetime] = set()
    deduped: list[dict[str, Any]] = []
    for c in all_candles:
        if c["timestamp"] not in seen:
            seen.add(c["timestamp"])
            deduped.append(c)

    deduped.sort(key=lambda x: x["timestamp"])
    return deduped


async def _fetch_funding_rates(
    session: Any,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    """Fetch historical funding rates from Bybit v5 API."""
    url = "https://api.bybit.com/v5/market/funding/history"
    all_rates: list[dict[str, Any]] = []
    cursor_ms = start_ms

    while cursor_ms < end_ms:
        params = {
            "category": "linear",
            "symbol": symbol,
            "startTime": cursor_ms,
            "endTime": min(cursor_ms + 86_400_000 * 30, end_ms),  # 30 days
            "limit": FUNDING_BATCH_SIZE,
        }
        result = await _fetch_json(session, url, params)
        rates = result.get("list", [])

        if not rates:
            break

        for r in rates:
            ts = int(r.get("fundingRateTimestamp", 0))
            if start_ms <= ts <= end_ms:
                all_rates.append({
                    "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    "rate": float(r.get("fundingRate", 0)),
                    "predicted_rate": None,
                })

        oldest_ts = min(int(r.get("fundingRateTimestamp", 0)) for r in rates)
        newest_ts = max(int(r.get("fundingRateTimestamp", 0)) for r in rates)

        if newest_ts <= cursor_ms:
            break
        cursor_ms = newest_ts + 1

        await asyncio.sleep(0.1)

    # Deduplicate
    seen_ts: set[datetime] = set()
    deduped: list[dict[str, Any]] = []
    for r in all_rates:
        if r["timestamp"] not in seen_ts:
            seen_ts.add(r["timestamp"])
            deduped.append(r)

    deduped.sort(key=lambda x: x["timestamp"])
    return deduped


# ------------------------------------------------------------------
# Database insertion
# ------------------------------------------------------------------

async def _upsert_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    candles: list[dict[str, Any]],
) -> int:
    """Upsert OHLCV candles into ohlcv_history table."""
    if not candles:
        return 0

    async with pool.acquire() as conn:
        count = 0
        # Batch insert with ON CONFLICT
        for batch_start in range(0, len(candles), 100):
            batch = candles[batch_start : batch_start + 100]
            records = [
                (
                    EXCHANGE,
                    symbol,
                    timeframe,
                    c["open"],
                    c["high"],
                    c["low"],
                    c["close"],
                    c["volume"],
                    c["timestamp"],
                )
                for c in batch
            ]
            await conn.executemany(
                """
                INSERT INTO ohlcv_history
                    (exchange, symbol, timeframe, open, high, low, close, volume, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (exchange, symbol, timeframe, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                records,
            )
            count += len(batch)

    return count


async def _upsert_funding_rates(
    pool: asyncpg.Pool,
    symbol: str,
    rates: list[dict[str, Any]],
) -> int:
    """Upsert funding rates into funding_rate_history table."""
    if not rates:
        return 0

    async with pool.acquire() as conn:
        count = 0
        for batch_start in range(0, len(rates), 100):
            batch = rates[batch_start : batch_start + 100]
            records = [
                (EXCHANGE, symbol, r["rate"], r["predicted_rate"], r["timestamp"])
                for r in batch
            ]
            await conn.executemany(
                """
                INSERT INTO funding_rate_history
                    (exchange, symbol, rate, predicted_rate, timestamp)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (exchange, symbol, timestamp) DO UPDATE SET
                    rate = EXCLUDED.rate,
                    predicted_rate = EXCLUDED.predicted_rate
                """,
                records,
            )
            count += len(batch)

    return count


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def seed(
    symbols: list[str],
    timeframes: list[str],
    months: int,
    dry_run: bool = False,
) -> None:
    """Seed historical data for all symbol/timeframe combinations."""
    import aiohttp

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=months * 30)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    log.info(
        "seeding_historical_data",
        symbols=symbols,
        timeframes=timeframes,
        start=start.isoformat(),
        end=now.isoformat(),
        months=months,
    )

    pool: asyncpg.Pool | None = None
    if not dry_run:
        pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=5)
        log.info("database_connected")

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            # Fetch funding rates
            log.info("fetching_funding_rates", symbol=symbol)
            funding = await _fetch_funding_rates(session, symbol, start_ms, end_ms)
            log.info("funding_rates_fetched", symbol=symbol, count=len(funding))

            if not dry_run and pool:
                count = await _upsert_funding_rates(pool, symbol, funding)
                log.info("funding_rates_inserted", symbol=symbol, count=count)

            # Fetch OHLCV per timeframe
            for tf in timeframes:
                log.info("fetching_ohlcv", symbol=symbol, timeframe=tf)
                candles = await _fetch_ohlcv(session, symbol, tf, start_ms, end_ms)
                log.info("ohlcv_fetched", symbol=symbol, timeframe=tf, count=len(candles))

                if not dry_run and pool:
                    count = await _upsert_ohlcv(pool, symbol, tf, candles)
                    log.info("ohlcv_inserted", symbol=symbol, timeframe=tf, count=count)

    if pool:
        await pool.close()

    log.info("seed_complete")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed historical OHLCV and funding rate data from Bybit"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Trading pairs to fetch (default: BTCUSDT ETHUSDT SOLUSDT)",
    )
    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=DEFAULT_TIMEFRAMES,
        help="OHLCV timeframes (default: 1h)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of months of history to fetch (default: 6)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write to database",
    )
    args = parser.parse_args()

    asyncio.run(seed(args.symbols, args.timeframes, args.months, args.dry_run))


if __name__ == "__main__":
    main()
