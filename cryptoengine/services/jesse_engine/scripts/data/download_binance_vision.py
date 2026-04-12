"""download_binance_vision.py — Downloads BTC/ETH OHLCV from Binance Vision.

Fetches real market data from data.binance.vision for multiple symbols and intervals.
- Symbols: BTCUSDT, ETHUSDT (customizable)
- Intervals: 1h, 4h, 1d (customizable)
- Date range: 2019-01-01 to today (customizable)
- Format: CSV inside ZIP files
- Output: Parquet files with ZSTD compression in data/ohlcv/

Usage:
    # Basic: 6 years, BTCUSDT + ETHUSDT, 1h/4h/1d
    python scripts/download_binance_vision.py

    # Custom date range
    python scripts/download_binance_vision.py --start 2022-01-01 --end 2024-12-31

    # Custom symbols and intervals
    python scripts/download_binance_vision.py --symbols BTCUSDT,ETHUSDT --intervals 1h,4h
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

# ── Configuration ────────────────────────────────────────────────────────────

BINANCE_VISION_BASE = "https://data.binance.vision/data/futures/um/daily/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVALS = ["1h", "4h", "1d"]
START_DATE = "2019-01-01"
END_DATE = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
OUTPUT_DIR = "data/ohlcv"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# CSV columns from Binance
BINANCE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

PARQUET_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Helper Functions ────────────────────────────────────────────────────────

def _ensure_output_dir() -> None:
    """Create output directory if it doesn't exist."""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {OUTPUT_DIR}")


def _download_with_retry(url: str) -> bytes | None:
    """Download file with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Downloading: {url}")
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY ** (attempt + 1)
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to download {url} after {MAX_RETRIES} attempts.")
    return None


def _parse_date_range(start_str: str, end_str: str) -> list[str]:
    """Generate date list from start to end (inclusive)."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _download_symbol_interval(symbol: str, interval: str, dates: list[str]) -> int:
    """Download and save OHLCV data for a symbol and interval.

    Returns:
        Number of rows saved.
    """
    logger.info(f"Processing {symbol} {interval} ({len(dates)} dates)...")

    all_candles = []
    skipped = 0

    for date in dates:
        url = f"{BINANCE_VISION_BASE}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        content = _download_with_retry(url)

        if content is None:
            skipped += 1
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Extract CSV file inside ZIP
                csv_files = [n for n in zf.namelist() if n.endswith('.csv')]
                if not csv_files:
                    logger.warning(f"No CSV found in {url}")
                    continue

                with zf.open(csv_files[0]) as f:
                    df = pd.read_csv(f, names=BINANCE_COLUMNS, dtype={'open_time': 'int64'})
                    # Convert milliseconds to datetime
                    df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
                    # Select and rename columns
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    all_candles.append(df)

        except Exception as e:
            logger.warning(f"Error processing {url}: {e}")

    if not all_candles:
        logger.warning(f"No data collected for {symbol} {interval}")
        return 0

    # Combine all data
    df_combined = pd.concat(all_candles, ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['timestamp'])
    df_combined = df_combined.sort_values('timestamp').reset_index(drop=True)

    # Save to Parquet
    output_file = Path(OUTPUT_DIR) / f"{symbol}_{interval}.parquet"
    df_combined.to_parquet(
        output_file,
        engine='pyarrow',
        compression='zstd',
        index=False
    )

    count = len(df_combined)
    logger.info(f"  {symbol} {interval}: {count} candles saved → {output_file}")
    return count


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    _ensure_output_dir()

    logger.info(f"Binance Vision OHLCV Downloader")
    logger.info(f"Date range: {args.start} to {args.end}")
    logger.info(f"Symbols: {', '.join(args.symbols)}")
    logger.info(f"Intervals: {', '.join(args.intervals)}")

    dates = _parse_date_range(args.start, args.end)
    logger.info(f"Total dates to process: {len(dates)}")

    total_rows = 0
    for symbol in args.symbols:
        for interval in args.intervals:
            rows = _download_symbol_interval(symbol, interval, dates)
            total_rows += rows

    logger.info(f"\n[DONE] Total rows saved: {total_rows}")


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download OHLCV data from Binance Vision"
    )
    parser.add_argument(
        "--start",
        default=START_DATE,
        metavar="YYYY-MM-DD",
        help=f"Start date (default: {START_DATE})"
    )
    parser.add_argument(
        "--end",
        default=END_DATE,
        metavar="YYYY-MM-DD",
        help=f"End date (default: {END_DATE})"
    )
    parser.add_argument(
        "--symbols",
        default=",".join(SYMBOLS),
        metavar="SYM1,SYM2,...",
        help=f"Symbols (default: {','.join(SYMBOLS)})"
    )
    parser.add_argument(
        "--intervals",
        default=",".join(INTERVALS),
        metavar="1h,4h,1d",
        help=f"Intervals (default: {','.join(INTERVALS)})"
    )
    args = parser.parse_args()
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
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
