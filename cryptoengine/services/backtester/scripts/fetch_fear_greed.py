"""fetch_fear_greed.py — Download Fear & Greed Index history from Alternative.me.

Fetches complete Fear & Greed Index history since 2018.
- API: https://api.alternative.me/fng/
- limit=0 returns ALL historical data
- Output: Parquet file with ZSTD compression + CSV for inspection
- Columns: timestamp, value, value_classification

Usage:
    # Basic: fetch all available history
    python scripts/fetch_fear_greed.py

    # Output to custom directory
    python scripts/fetch_fear_greed.py --output-dir custom/path
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# ── Configuration ────────────────────────────────────────────────────────────

FNG_API_URL = "https://api.alternative.me/fng/"
OUTPUT_DIR = "data/sentiment"
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

def _ensure_output_dir(output_dir: str) -> None:
    """Create output directory if it doesn't exist."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")


def _fetch_with_retry(url: str, params: dict) -> dict | None:
    """Fetch JSON with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Fetching {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            resp = requests.get(url, params=params, timeout=15)
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


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    _ensure_output_dir(args.output_dir)

    logger.info("Fear & Greed Index Fetcher")
    logger.info(f"API: {FNG_API_URL}")

    # Fetch all historical data
    logger.info("Fetching all Fear & Greed Index history...")
    data = _fetch_with_retry(FNG_API_URL, {"limit": 0, "format": "json"})

    if not data or "data" not in data:
        logger.error("Failed to fetch Fear & Greed Index data")
        sys.exit(1)

    records = data.get("data", [])
    logger.info(f"Received {len(records)} records")

    if not records:
        logger.error("No data received from API")
        sys.exit(1)

    # Parse records
    all_rows = []
    for item in records:
        try:
            timestamp = datetime.fromtimestamp(
                int(item["timestamp"]),
                tz=timezone.utc
            )
            all_rows.append({
                "timestamp": timestamp,
                "value": int(item["value"]),
                "value_classification": item.get("value_classification", ""),
            })
        except Exception as e:
            logger.warning(f"Error parsing record: {e}")

    if not all_rows:
        logger.error("No valid records after parsing")
        sys.exit(1)

    # Create DataFrame
    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    logger.info(f"Total unique records: {len(df)}")
    logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    # Save as Parquet
    parquet_file = Path(args.output_dir) / "fear_greed.parquet"
    df.to_parquet(
        parquet_file,
        engine='pyarrow',
        compression='zstd',
        index=False
    )
    logger.info(f"Saved Parquet → {parquet_file}")

    # Save as CSV for inspection
    csv_file = Path(args.output_dir) / "fear_greed.csv"
    df.to_csv(csv_file, index=False)
    logger.info(f"Saved CSV → {csv_file}")

    # Show summary
    logger.info("\nSummary:")
    logger.info(f"  Total records: {len(df)}")
    logger.info(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    logger.info(f"  Value range: {df['value'].min()} to {df['value'].max()}")
    logger.info(f"  Classifications: {df['value_classification'].unique().tolist()}")

    logger.info("\n[DONE] Fear & Greed Index data fetched successfully!")


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch Fear & Greed Index history from Alternative.me"
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        metavar="PATH",
        help=f"Output directory (default: {OUTPUT_DIR})"
    )
    return parser.parse_args()


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
