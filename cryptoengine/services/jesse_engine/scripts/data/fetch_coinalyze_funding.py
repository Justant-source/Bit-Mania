"""fetch_coinalyze_funding.py — Fetch funding rates, open interest, and liquidations from Coinalyze.

Fetches real market microstructure data from Coinalyze API.
- Endpoints: funding history, open interest, liquidations
- Symbols: BTCUSDT_PERP.A, ETHUSDT_PERP.A (Bybit)
- API key: optional (read from COINALYZE_API_KEY env var)
- Rate limit: 1 request/second
- Chunked requests: 30-day windows to avoid rate limits
- Output: Parquet files with ZSTD compression

Usage:
    # Basic: 3 years (2023-04-01 to 2026-04-01)
    python scripts/fetch_coinalyze_funding.py

    # Custom date range
    python scripts/fetch_coinalyze_funding.py --start 2022-01-01 --end 2024-12-31

    # Specific data types
    python scripts/fetch_coinalyze_funding.py --types funding,oi
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ── Configuration ────────────────────────────────────────────────────────────

COINALYZE_BASE = "https://api.coinalyze.net/v1"
API_KEY = os.getenv("COINALYZE_API_KEY", "")
SYMBOLS = ["BTCUSDT_PERP.A", "ETHUSDT_PERP.A"]
START_DATE = "2023-04-01"
END_DATE = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
OUTPUT_DIR = "data"
DATA_TYPES = ["funding", "oi", "liquidations"]
CHUNK_DAYS = 30
RATE_LIMIT_DELAY = 1.0  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 2

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ── Helper Functions ────────────────────────────────────────────────────────

def _ensure_output_dirs() -> None:
    """Create output directories if they don't exist."""
    for subdir in ["funding", "liquidations", "oi"]:
        (Path(OUTPUT_DIR) / subdir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directories created under: {OUTPUT_DIR}")


def _timestamp_to_unix(date_str: str) -> int:
    """Convert YYYY-MM-DD to Unix timestamp (seconds)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _request_with_retry(url: str, params: dict[str, Any]) -> dict[str, Any] | None:
    """Make HTTP request with retry logic."""
    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"GET {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            resp = requests.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY ** (attempt + 1)
                logger.warning(f"Request failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts: {e}")
    return None


def _parse_date_chunks(start_str: str, end_str: str, chunk_days: int) -> list[tuple[str, str]]:
    """Split date range into chunks."""
    start = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunks.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        current = chunk_end + timedelta(days=1)
    return chunks


def _fetch_funding_history(symbol: str, chunks: list[tuple[str, str]]) -> pd.DataFrame:
    """Fetch funding rate history."""
    logger.info(f"Fetching funding history for {symbol}...")
    all_records = []

    for start_date, end_date in chunks:
        ts_from = _timestamp_to_unix(start_date)
        ts_to = _timestamp_to_unix(end_date)

        params = {
            "symbols": symbol,
            "interval": "last",
            "from": ts_from,
            "to": ts_to
        }

        data = _request_with_retry(f"{COINALYZE_BASE}/funding-rate-history", params)
        time.sleep(RATE_LIMIT_DELAY)

        if not data or "data" not in data:
            logger.warning(f"No data for {symbol} {start_date}~{end_date}")
            continue

        for item in data.get("data", []):
            try:
                all_records.append({
                    "timestamp": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc),
                    "symbol": symbol,
                    "funding_rate": float(item.get("rate", 0)),
                })
            except Exception as e:
                logger.warning(f"Error parsing funding record: {e}")

        logger.info(f"  {start_date}~{end_date}: {len(all_records)} records")

    if not all_records:
        return pd.DataFrame(columns=["timestamp", "symbol", "funding_rate"])

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _fetch_open_interest(symbol: str, chunks: list[tuple[str, str]]) -> pd.DataFrame:
    """Fetch open interest history."""
    logger.info(f"Fetching open interest for {symbol}...")
    all_records = []

    for start_date, end_date in chunks:
        ts_from = _timestamp_to_unix(start_date)
        ts_to = _timestamp_to_unix(end_date)

        params = {
            "symbols": symbol,
            "interval": "1hour",
            "from": ts_from,
            "to": ts_to
        }

        data = _request_with_retry(f"{COINALYZE_BASE}/open-interest-history", params)
        time.sleep(RATE_LIMIT_DELAY)

        if not data or "data" not in data:
            logger.warning(f"No OI data for {symbol} {start_date}~{end_date}")
            continue

        for item in data.get("data", []):
            try:
                all_records.append({
                    "timestamp": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc),
                    "symbol": symbol,
                    "open_interest": float(item.get("openInterest", 0)),
                })
            except Exception as e:
                logger.warning(f"Error parsing OI record: {e}")

        logger.info(f"  {start_date}~{end_date}: {len(all_records)} records")

    if not all_records:
        return pd.DataFrame(columns=["timestamp", "symbol", "open_interest"])

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _fetch_liquidations(symbol: str, chunks: list[tuple[str, str]]) -> pd.DataFrame:
    """Fetch liquidation data."""
    logger.info(f"Fetching liquidations for {symbol}...")
    all_records = []

    for start_date, end_date in chunks:
        ts_from = _timestamp_to_unix(start_date)
        ts_to = _timestamp_to_unix(end_date)

        params = {
            "symbols": symbol,
            "interval": "1hour",
            "from": ts_from,
            "to": ts_to
        }

        data = _request_with_retry(f"{COINALYZE_BASE}/liquidation-history", params)
        time.sleep(RATE_LIMIT_DELAY)

        if not data or "data" not in data:
            logger.warning(f"No liquidation data for {symbol} {start_date}~{end_date}")
            continue

        for item in data.get("data", []):
            try:
                all_records.append({
                    "timestamp": datetime.fromtimestamp(item["timestamp"], tz=timezone.utc),
                    "symbol": symbol,
                    "liquidation_volume": float(item.get("liquidationVolume", 0)),
                    "liquidation_count": int(item.get("liquidationCount", 0)),
                })
            except Exception as e:
                logger.warning(f"Error parsing liquidation record: {e}")

        logger.info(f"  {start_date}~{end_date}: {len(all_records)} records")

    if not all_records:
        return pd.DataFrame(columns=["timestamp", "symbol", "liquidation_volume", "liquidation_count"])

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=["timestamp", "symbol"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    _ensure_output_dirs()

    logger.info(f"Coinalyze Data Fetcher")
    logger.info(f"Date range: {args.start} to {args.end}")
    logger.info(f"Symbols: {', '.join(args.symbols)}")
    logger.info(f"Data types: {', '.join(args.types)}")
    logger.info(f"API key: {'configured' if API_KEY else 'not configured (public endpoints only)'}")

    chunks = _parse_date_chunks(args.start, args.end, CHUNK_DAYS)
    logger.info(f"Processing in {len(chunks)} chunks ({CHUNK_DAYS}-day windows)")

    all_funding = []
    all_oi = []
    all_liquidations = []

    for symbol in args.symbols:
        if "funding" in args.types:
            df = _fetch_funding_history(symbol, chunks)
            if not df.empty:
                all_funding.append(df)

        if "oi" in args.types:
            df = _fetch_open_interest(symbol, chunks)
            if not df.empty:
                all_oi.append(df)

        if "liquidations" in args.types:
            df = _fetch_liquidations(symbol, chunks)
            if not df.empty:
                all_liquidations.append(df)

    # Save combined data
    if all_funding:
        df_funding = pd.concat(all_funding, ignore_index=True)
        output_file = Path(OUTPUT_DIR) / "funding" / "coinalyze_funding.parquet"
        df_funding.to_parquet(output_file, engine='pyarrow', compression='zstd', index=False)
        logger.info(f"Saved {len(df_funding)} funding records → {output_file}")

    if all_oi:
        df_oi = pd.concat(all_oi, ignore_index=True)
        output_file = Path(OUTPUT_DIR) / "oi" / "coinalyze_oi.parquet"
        df_oi.to_parquet(output_file, engine='pyarrow', compression='zstd', index=False)
        logger.info(f"Saved {len(df_oi)} OI records → {output_file}")

    if all_liquidations:
        df_liq = pd.concat(all_liquidations, ignore_index=True)
        output_file = Path(OUTPUT_DIR) / "liquidations" / "coinalyze_liquidations.parquet"
        df_liq.to_parquet(output_file, engine='pyarrow', compression='zstd', index=False)
        logger.info(f"Saved {len(df_liq)} liquidation records → {output_file}")

    logger.info("\n[DONE] All data fetched successfully!")


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch market microstructure data from Coinalyze"
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
        "--types",
        default=",".join(DATA_TYPES),
        metavar="funding,oi,liquidations",
        help=f"Data types to fetch (default: {','.join(DATA_TYPES)})"
    )
    args = parser.parse_args()
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.types = [t.strip() for t in args.types.split(",") if t.strip()]
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
