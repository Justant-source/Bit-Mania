"""export_pg_to_parquet.py — Export PostgreSQL data to Parquet for Jesse framework.

Exports historical market data from PostgreSQL to Parquet format with ZSTD compression.
- Tables: ohlcv_history, funding_rate_history
- Chunking by symbol and timeframe for memory efficiency
- CLI args for flexible symbol and timeframe selection
- Jesse-compatible schema

Usage:
    # Basic: export all data
    python scripts/export_pg_to_parquet.py

    # Custom symbols and timeframes
    python scripts/export_pg_to_parquet.py \
        --symbols BTCUSDT,ETHUSDT \
        --timeframes 1h,4h,1d

    # Custom output directory
    python scripts/export_pg_to_parquet.py \
        --output-dir /path/to/data \
        --symbols BTCUSDT
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

# ── Configuration ────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "cryptoengine"),
    "user": os.getenv("DB_USER", "cryptoengine"),
    "password": os.getenv("DB_PASSWORD", "cryptoengine"),
}

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_TIMEFRAMES = ["1h", "4h", "1d"]
OUTPUT_DIR = "data"
BATCH_SIZE = 10000

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Database Connection ─────────────────────────────────────────────────────

def _get_connection():
    """Create PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)


# ── Helper Functions ────────────────────────────────────────────────────────

def _ensure_output_dir(output_dir: str) -> None:
    """Create output directory if it doesn't exist."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")


def _export_ohlcv(conn, symbols: list[str], timeframes: list[str], output_dir: str) -> int:
    """Export OHLCV data from ohlcv_history table."""
    logger.info("Exporting OHLCV data...")

    ohlcv_dir = Path(output_dir) / "ohlcv"
    ohlcv_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    for symbol in symbols:
        for timeframe in timeframes:
            try:
                # Query OHLCV data
                query = """
                    SELECT exchange, symbol, timeframe, timestamp,
                           open, high, low, close, volume
                    FROM ohlcv_history
                    WHERE symbol = %s AND timeframe = %s
                    ORDER BY timestamp ASC
                """
                cur.execute(query, (symbol, timeframe))

                rows = cur.fetchall()
                if not rows:
                    logger.warning(f"No OHLCV data for {symbol} {timeframe}")
                    continue

                # Convert to DataFrame
                df = pd.DataFrame(rows)
                df.columns = ['exchange', 'symbol', 'timeframe', 'timestamp',
                             'open', 'high', 'low', 'close', 'volume']

                # Ensure timestamp is datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                # Save to Parquet
                output_file = ohlcv_dir / f"pg_ohlcv_{symbol}_{timeframe}.parquet"
                df.to_parquet(
                    output_file,
                    engine='pyarrow',
                    compression='zstd',
                    index=False
                )

                count = len(df)
                total_rows += count
                logger.info(f"  {symbol} {timeframe}: {count} rows → {output_file}")

            except psycopg2.Error as e:
                logger.error(f"Error exporting {symbol} {timeframe}: {e}")

    cur.close()
    return total_rows


def _export_funding(conn, symbols: list[str], output_dir: str) -> int:
    """Export funding rate data from funding_rate_history table."""
    logger.info("Exporting funding rate data...")

    funding_dir = Path(output_dir) / "funding"
    funding_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch all funding data for specified symbols
    all_rows = []
    for symbol in symbols:
        try:
            query = """
                SELECT exchange, symbol, timestamp, rate
                FROM funding_rate_history
                WHERE symbol = %s
                ORDER BY timestamp ASC
            """
            cur.execute(query, (symbol,))
            rows = cur.fetchall()

            if not rows:
                logger.warning(f"No funding data for {symbol}")
                continue

            all_rows.extend(rows)
            logger.info(f"  {symbol}: {len(rows)} rows")

        except psycopg2.Error as e:
            logger.error(f"Error exporting funding for {symbol}: {e}")

    cur.close()

    if all_rows:
        df = pd.DataFrame(all_rows)
        df.columns = ['exchange', 'symbol', 'timestamp', 'rate']
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        output_file = funding_dir / "pg_funding.parquet"
        df.to_parquet(
            output_file,
            engine='pyarrow',
            compression='zstd',
            index=False
        )

        total_rows = len(df)
        logger.info(f"Saved funding data: {total_rows} rows → {output_file}")

    return total_rows


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    _ensure_output_dir(args.output_dir)

    logger.info("PostgreSQL → Parquet Exporter")
    logger.info(f"Database: {DB_CONFIG['database']} @ {DB_CONFIG['host']}")
    logger.info(f"Symbols: {', '.join(args.symbols)}")
    logger.info(f"Timeframes: {', '.join(args.timeframes)}")

    conn = _get_connection()

    try:
        # Export OHLCV
        ohlcv_rows = _export_ohlcv(conn, args.symbols, args.timeframes, args.output_dir)

        # Export funding rates
        funding_rows = _export_funding(conn, args.symbols, args.output_dir)

        logger.info(f"\n[DONE] Export complete!")
        logger.info(f"  OHLCV rows: {ohlcv_rows}")
        logger.info(f"  Funding rows: {funding_rows}")
        logger.info(f"  Total: {ohlcv_rows + funding_rows} rows")

    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL data to Parquet format for Jesse"
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        metavar="SYM1,SYM2,...",
        help=f"Trading symbols (default: {','.join(DEFAULT_SYMBOLS)})"
    )
    parser.add_argument(
        "--timeframes",
        default=",".join(DEFAULT_TIMEFRAMES),
        metavar="1h,4h,1d",
        help=f"Timeframes (default: {','.join(DEFAULT_TIMEFRAMES)})"
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        metavar="PATH",
        help=f"Output directory (default: {OUTPUT_DIR})"
    )
    parser.add_argument(
        "--db-host",
        default=DB_CONFIG["host"],
        metavar="HOSTNAME",
        help=f"Database host (default: {DB_CONFIG['host']})"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=DB_CONFIG["port"],
        metavar="PORT",
        help=f"Database port (default: {DB_CONFIG['port']})"
    )
    parser.add_argument(
        "--db-name",
        default=DB_CONFIG["database"],
        metavar="NAME",
        help=f"Database name (default: {DB_CONFIG['database']})"
    )
    parser.add_argument(
        "--db-user",
        default=DB_CONFIG["user"],
        metavar="USER",
        help=f"Database user (default: {DB_CONFIG['user']})"
    )

    args = parser.parse_args()

    # Parse comma-separated values
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.timeframes = [tf.strip() for tf in args.timeframes.split(",") if tf.strip()]

    # Update DB_CONFIG with command-line args
    DB_CONFIG["host"] = args.db_host
    DB_CONFIG["port"] = args.db_port
    DB_CONFIG["database"] = args.db_name
    DB_CONFIG["user"] = args.db_user

    return args


if __name__ == "__main__":
    try:
        args = _parse_args()
        _main(args)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
