#!/usr/bin/env python3
"""Export trades table to CSV with optional date-range filter.

Writes to stdout by default so that it can be piped::

    python scripts/export_trades.py --start 2026-01-01 > trades.csv
    python scripts/export_trades.py -o trades_jan.csv --start 2026-01-01 --end 2026-02-01
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import os
import sys
from datetime import datetime, timezone
from typing import TextIO

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
log = structlog.get_logger("export_trades")

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CSV_COLUMNS = [
    "id",
    "strategy_id",
    "exchange",
    "symbol",
    "side",
    "order_type",
    "quantity",
    "price",
    "fee",
    "fee_currency",
    "pnl",
    "order_id",
    "request_id",
    "status",
    "created_at",
    "filled_at",
]


async def _fetch_trades(
    dsn: str,
    start: datetime | None,
    end: datetime | None,
    strategy: str | None,
    symbol: str | None,
    limit: int,
) -> list[asyncpg.Record]:
    """Query trades with optional filters."""
    conn = await asyncpg.connect(dsn)

    conditions: list[str] = []
    params: list = []
    idx = 1

    if start:
        conditions.append(f"created_at >= ${idx}")
        params.append(start)
        idx += 1

    if end:
        conditions.append(f"created_at <= ${idx}")
        params.append(end)
        idx += 1

    if strategy:
        conditions.append(f"strategy_id = ${idx}")
        params.append(strategy)
        idx += 1

    if symbol:
        conditions.append(f"symbol = ${idx}")
        params.append(symbol)
        idx += 1

    where = " AND ".join(conditions)
    query = f"""
        SELECT {', '.join(CSV_COLUMNS)}
        FROM trades
        {"WHERE " + where if where else ""}
        ORDER BY created_at ASC
        LIMIT ${idx}
    """
    params.append(limit)

    try:
        rows = await conn.fetch(query, *params)
    finally:
        await conn.close()

    return rows


def _write_csv(rows: list[asyncpg.Record], output: TextIO) -> int:
    """Write records as CSV.  Returns number of rows written."""
    writer = csv.writer(output)
    writer.writerow(CSV_COLUMNS)

    count = 0
    for row in rows:
        writer.writerow([
            str(row[col]) if row[col] is not None else ""
            for col in CSV_COLUMNS
        ])
        count += 1

    return count


async def export_trades(
    dsn: str,
    output_path: str | None,
    start: datetime | None,
    end: datetime | None,
    strategy: str | None,
    symbol: str | None,
    limit: int,
) -> int:
    """Fetch and export trades. Returns count."""
    log.info(
        "exporting_trades",
        start=start.isoformat() if start else "all",
        end=end.isoformat() if end else "all",
        strategy=strategy or "all",
        symbol=symbol or "all",
        output=output_path or "stdout",
    )

    rows = await _fetch_trades(dsn, start, end, strategy, symbol, limit)

    if not rows:
        log.warning("no_trades_found")
        return 0

    if output_path:
        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            count = _write_csv(rows, fh)
        log.info("trades_exported_to_file", path=output_path, count=count)
    else:
        count = _write_csv(rows, sys.stdout)
        # Log to stderr so it doesn't mix with CSV output
        print(f"\n# Exported {count} trades", file=sys.stderr)

    return count


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD into a timezone-aware datetime."""
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export CryptoEngine trades to CSV")
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default=None,
        help="Filter by strategy_id",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Filter by trading pair (e.g. BTCUSDT)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100_000,
        help="Maximum number of rows (default: 100000)",
    )
    parser.add_argument(
        "--dsn",
        type=str,
        default=DB_DSN,
        help="PostgreSQL connection string",
    )
    args = parser.parse_args()

    start_dt = _parse_date(args.start) if args.start else None
    end_dt = _parse_date(args.end) if args.end else None

    count = asyncio.run(
        export_trades(
            args.dsn, args.output, start_dt, end_dt,
            args.strategy, args.symbol, args.limit,
        )
    )
    sys.exit(0 if count > 0 else 1)


if __name__ == "__main__":
    main()
