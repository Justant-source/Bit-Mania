"""Open Interest data collector from Bybit API.

Collects hourly OI data for BTC and ETH, with optional backfill capability.
Stores in open_interest_history table.

Usage:
    python oi_collector.py --backfill --start 2023-04-01
    python oi_collector.py --update (fetch latest hour)
"""

import asyncio
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
import ccxt.async_support as ccxt

sys.path.insert(0, "/app")
from shared.db.connection import create_pool, get_pool, close_pool
from shared.logging_config import setup_logging

log = logging.getLogger(__name__)
setup_logging("oi_collector")

# Bybit USDT perpetual API endpoint
BYBIT_API_URL = "https://api.bybit.com"
SYMBOL = "BTCUSDT"
INTERVAL = "1h"  # 1-hour interval OI

# Data retention: store 3+ years (2023-04-01 to 2026-04-11)
START_BACKFILL = datetime(2023, 4, 1, tzinfo=timezone.utc)
END_BACKFILL = datetime(2026, 4, 11, tzinfo=timezone.utc)


async def fetch_oi_bybit(
    symbol: str,
    interval: str = "1h",
    limit: int = 200,
    cursor: Optional[str] = None,
) -> dict:
    """Fetch OI data from Bybit API.

    Args:
        symbol: Trading pair (e.g., 'BTCUSDT')
        interval: Time interval ('5min', '15min', '30min', '1h', '4h', '1d')
        limit: Max records per request (1-200)
        cursor: Pagination cursor for next batch

    Returns:
        dict with 'list' (OI records) and 'nextPageCursor'
    """
    try:
        exchange = ccxt.bybit({"enableRateLimit": True})
        # Bybit doesn't have public OI endpoint in CCXT
        # Fallback: fetch from web API directly
        import aiohttp

        url = "https://api.bybit.com/v5/market/open-interest"
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": interval,
            "limit": min(limit, 200),
        }
        if cursor:
            params["cursor"] = cursor

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    log.error(f"Bybit API error: {resp.status}")
                    return {"list": [], "nextPageCursor": None}
                data = await resp.json()

        result = data.get("result", {})
        return {
            "list": result.get("openInterestList", []),
            "nextPageCursor": result.get("nextPageCursor", None),
        }
    except Exception as e:
        log.error(f"fetch_oi_bybit error: {e}")
        return {"list": [], "nextPageCursor": None}


async def upsert_oi_records(
    pool: asyncpg.Pool,
    exchange: str,
    symbol: str,
    records: list[dict],
) -> int:
    """Upsert OI records to database.

    Args:
        pool: asyncpg connection pool
        exchange: Exchange name ('bybit')
        symbol: Trading pair ('BTCUSDT')
        records: List of {timestamp (ms), openInterest (value)}

    Returns:
        Number of records inserted/updated
    """
    if not records:
        return 0

    async with pool.acquire() as conn:
        rows = []
        for rec in records:
            try:
                ts_ms = int(rec.get("timestamp", 0))
                oi_val = float(rec.get("openInterest", 0))

                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

                # INSERT ... ON CONFLICT DO UPDATE
                await conn.execute(
                    """
                    INSERT INTO open_interest_history (exchange, symbol, timestamp, oi_usd)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (exchange, symbol, timestamp)
                    DO UPDATE SET oi_usd = $4
                    """,
                    exchange, symbol, ts, oi_val,
                )
                rows.append(1)
            except Exception as e:
                log.warning(f"Failed to parse record {rec}: {e}")

        return len(rows)


async def backfill_oi(
    pool: asyncpg.Pool,
    symbol: str = SYMBOL,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    interval: str = INTERVAL,
) -> None:
    """Backfill OI data from start_date to end_date.

    Args:
        pool: asyncpg pool
        symbol: Trading pair
        start_date: Start date (default 2023-04-01)
        end_date: End date (default today)
        interval: OI interval ('1h', '4h', '1d')
    """
    start_date = start_date or START_BACKFILL
    end_date = end_date or datetime.now(timezone.utc)

    log.info(
        f"Starting OI backfill: {symbol} from {start_date} to {end_date}, interval={interval}"
    )

    # Estimate total requests needed
    # Bybit limit=200 per request, for hourly data
    hours_total = int((end_date - start_date).total_seconds() / 3600)
    total_requests = (hours_total + 199) // 200
    log.info(f"Estimated {total_requests} API requests needed")

    cursor = None
    processed = 0

    try:
        while processed < hours_total:
            result = await fetch_oi_bybit(symbol, interval=interval, cursor=cursor)
            records = result.get("list", [])

            if not records:
                log.warning("No more records from API")
                break

            # Filter to date range
            filtered = [
                r for r in records
                if start_date <= datetime.fromtimestamp(
                    int(r.get("timestamp", 0)) / 1000, tz=timezone.utc
                ) <= end_date
            ]

            if filtered:
                inserted = await upsert_oi_records(pool, "bybit", symbol, filtered)
                processed += inserted
                log.info(f"Inserted {inserted} OI records (total: {processed})")

            cursor = result.get("nextPageCursor")
            if not cursor:
                log.info("Cursor exhausted, backfill complete")
                break

            # Rate limit: 10 req/sec from Bybit
            await asyncio.sleep(0.15)

    except Exception as e:
        log.error(f"Backfill interrupted: {e}")

    log.info(f"Backfill complete: {processed} total records")


async def update_latest_oi(
    pool: asyncpg.Pool,
    symbol: str = SYMBOL,
) -> None:
    """Fetch and store the latest OI data.

    Args:
        pool: asyncpg pool
        symbol: Trading pair
    """
    log.info(f"Fetching latest OI for {symbol}")

    try:
        result = await fetch_oi_bybit(symbol, interval="1h", limit=1)
        records = result.get("list", [])

        if records:
            inserted = await upsert_oi_records(pool, "bybit", symbol, records)
            log.info(f"Updated {inserted} latest OI records")
        else:
            log.warning("No data returned from API")

    except Exception as e:
        log.error(f"Update failed: {e}")


async def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Collect BTC OI data from Bybit")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill historical data from 2023-04-01",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Backfill start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Fetch latest hour only",
    )
    args = parser.parse_args()

    pool = await create_pool()

    try:
        if args.backfill:
            start = None
            if args.start:
                start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            await backfill_oi(pool, start_date=start)
        elif args.update:
            await update_latest_oi(pool)
        else:
            parser.print_help()

    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
