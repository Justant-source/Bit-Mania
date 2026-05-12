"""download_binance_vision.py — Downloads BTC OHLCV from Binance Vision.

BTC 단일 운영 — multi-symbol 거래 금지.

Fetches real market data from data.binance.vision for BTC.
- Symbol: BTCUSDT
- Intervals: 1h, 4h, 1d (customizable)
- Date range: 2017-08-01 to today (customizable)
- Source: auto (perp from 2019-09-08, spot before), spot, or perp
- Format: CSV inside ZIP files
- Output: Parquet files with ZSTD compression in data/ohlcv/SYMBOL/INTERVAL/YYYY/MM.parquet

Usage:
    # Full backfill (2017-08-01 ~ today, 1h/4h/1d, auto spot→perp)
    python scripts/data/download_binance_vision.py

    # 특정 구간만 (spot 강제)
    python scripts/data/download_binance_vision.py --start 2017-08-01 --end 2019-09-07 --source spot

    # 4h만 다운로드
    python scripts/data/download_binance_vision.py --intervals 4h
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

# _paths.py로부터 절대경로 import
try:
    from _paths import OHLCV_ROOT
except ImportError:
    # fallback: 스크립트 루트 기준 상대경로
    OHLCV_ROOT = Path(__file__).parent.parent.parent / "data" / "ohlcv"

# ── Configuration ────────────────────────────────────────────────────────────

FUTURES_BASE = "https://data.binance.vision/data/futures/um/daily/klines"
SPOT_BASE    = "https://data.binance.vision/data/spot/daily/klines"

# BTC perp(BTCUSDT) 는 2019-09-08 이후에만 존재
PERP_START_DATE = "2019-09-08"

SYMBOLS = ["BTCUSDT"]
INTERVALS = ["1h", "4h", "1d"]
START_DATE = "2017-08-01"
END_DATE = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
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

def _get_base_url(date_str: str, source: str) -> str:
    """Return Binance Vision base URL based on source and date."""
    if source == "spot":
        return SPOT_BASE
    if source == "perp":
        return FUTURES_BASE
    # auto: spot if before perp launch date
    return SPOT_BASE if date_str < PERP_START_DATE else FUTURES_BASE


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


def _download_symbol_interval(symbol: str, interval: str, dates: list[str], source: str) -> int:
    """Download OHLCV data for a symbol/interval and save as monthly Parquet files.

    Output structure: OHLCV_ROOT / symbol / interval / YYYY / MM.parquet
    Adds `source` column ('spot' or 'perp') to each row.

    Returns:
        Total number of rows saved.
    """
    logger.info(f"Processing {symbol} {interval} ({len(dates)} dates, source={source})...")

    # Group dates by YYYY/MM for monthly Parquet files
    monthly: dict[str, list[pd.DataFrame]] = {}
    skipped = 0

    for date in dates:
        base = _get_base_url(date, source)
        actual_source = "spot" if base == SPOT_BASE else "perp"
        url = f"{base}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        content = _download_with_retry(url)

        if content is None:
            skipped += 1
            continue

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_files = [n for n in zf.namelist() if n.endswith('.csv')]
                if not csv_files:
                    logger.warning(f"No CSV in {url}")
                    continue
                with zf.open(csv_files[0]) as f:
                    df = pd.read_csv(f, names=BINANCE_COLUMNS, dtype={'open_time': 'int64'})
                    df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
                    df['source'] = actual_source
                    ym = date[:7]  # "YYYY-MM"
                    monthly.setdefault(ym, []).append(df)
        except Exception as e:
            logger.warning(f"Error processing {url}: {e}")

    if not monthly:
        logger.warning(f"No data collected for {symbol} {interval}")
        return 0

    total = 0
    for ym, frames in sorted(monthly.items()):
        year, month = ym.split("-")
        out_dir = OHLCV_ROOT / symbol / interval / year
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{month}.parquet"

        df_month = pd.concat(frames, ignore_index=True)
        df_month = df_month.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
        df_month.to_parquet(out_file, engine='pyarrow', compression='zstd', index=False)
        total += len(df_month)

    logger.info(f"  {symbol} {interval}: {total} rows ({len(monthly)} months), skipped={skipped}")
    return total


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    OHLCV_ROOT.mkdir(parents=True, exist_ok=True)
    logger.info(f"Binance Vision OHLCV Downloader")
    logger.info(f"Date range: {args.start} to {args.end}")
    logger.info(f"Source: {args.source}  (auto=spot before {PERP_START_DATE}, perp after)")
    logger.info(f"Symbols: {', '.join(args.symbols)} | Intervals: {', '.join(args.intervals)}")
    logger.info(f"Output: {OHLCV_ROOT}")

    dates = _parse_date_range(args.start, args.end)
    logger.info(f"Total dates: {len(dates)}")

    total_rows = 0
    for symbol in args.symbols:
        for interval in args.intervals:
            rows = _download_symbol_interval(symbol, interval, dates, args.source)
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
    parser.add_argument(
        "--source",
        default="auto",
        choices=["auto", "spot", "perp"],
        help="Data source: auto=spot before 2019-09-08 / perp after (default: auto)"
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
