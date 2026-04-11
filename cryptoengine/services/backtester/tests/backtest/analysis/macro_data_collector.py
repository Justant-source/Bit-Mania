"""Macro indicators data collector from FRED API.

Collects daily DXY, VIX-like, US 10Y yield data.
Stores in macro_indicators table.

Uses FRED (https://fred.stlouisfed.org/) which is free and requires no API key.

Usage:
    python macro_data_collector.py --backfill --start 2020-01-01
    python macro_data_collector.py --update (fetch latest)

Indicators collected:
    - DXY (US Dollar Index): FRED series DTWEXBGS
    - VIX equivalent: Cannot use VIX directly from FRED, use VIXCLS from alternative
    - US 10Y Treasury Yield: FRED series DGS10
    - US 2Y Treasury Yield: FRED series DGS2
"""

import asyncio
import argparse
import csv
import io
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg

sys.path.insert(0, "/app")
from shared.db.connection import create_pool, get_pool, close_pool
from shared.logging_config import setup_logging

log = logging.getLogger(__name__)
setup_logging("macro_data_collector")

# FRED API Base URL - CSV download (no authentication required)
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# Macro indicators to collect
FRED_SERIES_MAP = {
    "dxy": "DTWEXBGS",           # US Dollar Index
    "us10y": "DGS10",            # US 10Y Treasury Yield
    "us2y": "DGS2",              # US 2Y Treasury Yield
    "vix": "VIXCLS",             # VIX Index (Volatility)
    "dff": "DFF",                # Effective Federal Funds Rate
    "cpi": "CPIAUCSL",           # CPI All Urban Consumers
}

# Data retention: store from 2020-01-01 onwards
START_BACKFILL = datetime(2020, 1, 1, tzinfo=timezone.utc).date()
END_BACKFILL = datetime.now(timezone.utc).date()


async def fetch_fred_csv(
    series_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> list[tuple[str, float]]:
    """Fetch FRED data as CSV.

    Args:
        series_id: FRED series ID (e.g., 'DTWEXBGS')
        start_date: Start date
        end_date: End date

    Returns:
        List of (date_str, value) tuples
    """
    try:
        import aiohttp

        start_date = start_date or START_BACKFILL
        end_date = end_date or END_BACKFILL

        params = {
            "id": series_id,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(FRED_CSV_URL, params=params) as resp:
                if resp.status != 200:
                    log.error(f"FRED API error for {series_id}: HTTP {resp.status}")
                    return []

                content = await resp.text()

        # Parse CSV
        records = []
        reader = csv.reader(io.StringIO(content))
        next(reader)  # Skip header

        for row in reader:
            if len(row) < 2:
                continue

            date_str = row[0]
            value_str = row[1]

            if value_str == ".":  # Missing data
                continue

            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if date < start_date or date > end_date:
                    continue

                value = float(value_str)
                records.append((date_str, value))

            except (ValueError, IndexError):
                continue

        log.info(f"Fetched {len(records)} records for {series_id}")
        return records

    except Exception as e:
        log.error(f"fetch_fred_csv error for {series_id}: {e}")
        return []


async def upsert_macro_records(
    pool: asyncpg.Pool,
    indicator: str,
    records: list[tuple[str, float]],
) -> int:
    """Upsert macro indicator records to database.

    Args:
        pool: asyncpg connection pool
        indicator: Indicator name ('dxy', 'us10y', etc.)
        records: List of (date_str, value) tuples

    Returns:
        Number of records inserted/updated
    """
    if not records:
        return 0

    async with pool.acquire() as conn:
        count = 0
        for date_str, value in records:
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").date()

                # INSERT ... ON CONFLICT DO UPDATE
                await conn.execute(
                    """
                    INSERT INTO macro_indicators (date, indicator, value, source)
                    VALUES ($1, $2, $3, 'fred')
                    ON CONFLICT (date, indicator)
                    DO UPDATE SET value = $3
                    """,
                    date, indicator, value,
                )
                count += 1
            except Exception as e:
                log.warning(f"Failed to insert {indicator} {date_str}: {e}")

        return count


async def backfill_all_indicators(
    pool: asyncpg.Pool,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> None:
    """Backfill all macro indicators.

    Args:
        pool: asyncpg pool
        start_date: Start date
        end_date: End date
    """
    start_date = start_date or START_BACKFILL
    end_date = end_date or END_BACKFILL

    log.info(
        f"Starting macro backfill from {start_date} to {end_date}"
    )

    for indicator, series_id in FRED_SERIES_MAP.items():
        log.info(f"Fetching {indicator} ({series_id})")

        records = await fetch_fred_csv(series_id, start_date, end_date)

        if records:
            inserted = await upsert_macro_records(pool, indicator, records)
            log.info(f"Inserted {inserted} records for {indicator}")
        else:
            log.warning(f"No data for {indicator}")

        # Rate limit: be respectful to FRED servers
        await asyncio.sleep(0.5)

    log.info("Macro backfill complete")


async def update_latest(
    pool: asyncpg.Pool,
) -> None:
    """Fetch and store latest macro data.

    Args:
        pool: asyncpg pool
    """
    log.info("Fetching latest macro indicators")

    today = datetime.now(timezone.utc).date()

    for indicator, series_id in FRED_SERIES_MAP.items():
        # Fetch last 7 days to catch latest available
        start = today - timedelta(days=7)
        records = await fetch_fred_csv(series_id, start, today)

        if records:
            inserted = await upsert_macro_records(pool, indicator, records)
            log.info(f"Updated {inserted} latest records for {indicator}")
        else:
            log.warning(f"No update for {indicator}")

        await asyncio.sleep(0.5)


async def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Collect macro indicators from FRED"
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill historical data from 2020-01-01",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Backfill start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="Backfill end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Fetch latest data only",
    )
    args = parser.parse_args()

    pool = await create_pool()

    try:
        if args.backfill:
            start = None
            if args.start:
                start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end = None
            if args.end:
                end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            await backfill_all_indicators(pool, start, end)
        elif args.update:
            await update_latest(pool)
        else:
            parser.print_help()

    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
