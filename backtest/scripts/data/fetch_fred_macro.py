"""fetch_fred_macro.py — Fetch macro data from FRED API (Federal Reserve Economic Data).

Downloads key macroeconomic indicators from St. Louis Federal Reserve.
- No API key required for public series (CSV endpoint)
- Series: DFF (Fed Funds Rate), DGS10 (10Y Treasury), WALCL (Fed Balance Sheet), CPIAUCSL (CPI)
- Forward-fill to daily frequency
- Output: Individual Parquet files + combined macro file with ZSTD compression

Usage:
    # Basic: fetch all default series
    python scripts/fetch_fred_macro.py

    # Custom series
    python scripts/fetch_fred_macro.py --series DFF,DGS10,CPIAUCSL

    # Output to custom directory
    python scripts/fetch_fred_macro.py --output-dir custom/path
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# ── Configuration ────────────────────────────────────────────────────────────

FRED_CSV_URL_TEMPLATE = "https://fred.stlouisfed.org/graph/fredgraph.csv"
SERIES = {
    "DFF": "Federal Funds Rate",
    "DGS10": "10-Year Treasury Bond Yield",
    "WALCL": "Federal Reserve Balance Sheet",
    "CPIAUCSL": "Consumer Price Index - All Urban Consumers",
}
OUTPUT_DIR = "data/macro"
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


def _fetch_with_retry(url: str, params: dict) -> Optional[str]:
    """Fetch CSV data with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Fetching {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY ** (attempt + 1)
                logger.warning(f"Request failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {MAX_RETRIES} attempts: {e}")
    return None


def _fetch_series(series_id: str, description: str) -> Optional[pd.DataFrame]:
    """Fetch a single FRED series."""
    logger.info(f"Fetching {series_id} ({description})...")

    params = {"id": series_id}
    csv_data = _fetch_with_retry(FRED_CSV_URL_TEMPLATE, params)

    if not csv_data:
        logger.error(f"Failed to fetch {series_id}")
        return None

    try:
        # Parse CSV from FRED (date, value columns)
        df = pd.read_csv(pd.io.common.StringIO(csv_data))

        # Handle missing values indicated as '.' or 'ND'
        df = df.replace({'.': None, 'ND': None})

        # Rename columns
        df.columns = ['date', 'value']
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        # Remove rows with NaN values
        df = df.dropna(subset=['value'])

        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)

        logger.info(f"  {series_id}: {len(df)} records, "
                   f"date range {df['date'].min().date()} to {df['date'].max().date()}")

        return df

    except Exception as e:
        logger.error(f"Error parsing {series_id}: {e}")
        return None


def _forward_fill_to_daily(df: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """Forward-fill to daily frequency."""
    # Set date as index
    df = df.set_index('date')

    # Create daily date range
    date_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
    df = df.reindex(date_range)

    # Forward-fill missing values
    df['value'] = df['value'].ffill()

    # Reset index
    df = df.reset_index()
    df.columns = ['date', series_id]

    return df


def _main(args: argparse.Namespace) -> None:
    """Main entry point."""
    _ensure_output_dir(args.output_dir)

    logger.info("FRED Macroeconomic Data Fetcher")
    logger.info(f"Series: {', '.join(args.series)}")

    all_dfs = []
    fetched_series = []

    for series_id in args.series:
        description = SERIES.get(series_id, "Unknown")
        df = _fetch_series(series_id, description)

        if df is None or df.empty:
            logger.warning(f"Skipping {series_id} due to fetch error")
            continue

        # Forward-fill to daily frequency
        df_daily = _forward_fill_to_daily(df, series_id)

        # Save individual series
        output_file = Path(args.output_dir) / f"{series_id}.parquet"
        df_daily.to_parquet(
            output_file,
            engine='pyarrow',
            compression='zstd',
            index=False
        )
        logger.info(f"  Saved → {output_file}")

        all_dfs.append(df_daily)
        fetched_series.append(series_id)

    if not all_dfs:
        logger.error("No series fetched successfully")
        sys.exit(1)

    # Merge all series into combined macro file
    logger.info("Creating combined macro file...")
    df_combined = all_dfs[0]
    for df_next in all_dfs[1:]:
        df_combined = pd.merge(df_combined, df_next, on='date', how='outer')

    # Sort by date
    df_combined = df_combined.sort_values('date').reset_index(drop=True)

    # Forward-fill any remaining gaps from merge
    numeric_cols = df_combined.select_dtypes(include=['number']).columns
    df_combined[numeric_cols] = df_combined[numeric_cols].ffill()

    # Save combined file
    combined_file = Path(args.output_dir) / "macro_combined.parquet"
    df_combined.to_parquet(
        combined_file,
        engine='pyarrow',
        compression='zstd',
        index=False
    )
    logger.info(f"Saved combined → {combined_file}")

    # Show summary
    logger.info("\nSummary:")
    logger.info(f"  Fetched series: {', '.join(fetched_series)}")
    logger.info(f"  Combined records: {len(df_combined)}")
    logger.info(f"  Date range: {df_combined['date'].min().date()} "
               f"to {df_combined['date'].max().date()}")
    logger.info(f"  Columns: {', '.join(df_combined.columns.tolist())}")

    logger.info("\n[DONE] Macro data fetched successfully!")


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch macroeconomic data from FRED (Federal Reserve Economic Data)"
    )
    parser.add_argument(
        "--series",
        default=",".join(SERIES.keys()),
        metavar="SER1,SER2,...",
        help=f"FRED series IDs (default: {','.join(SERIES.keys())})"
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        metavar="PATH",
        help=f"Output directory (default: {OUTPUT_DIR})"
    )
    args = parser.parse_args()
    args.series = [s.strip().upper() for s in args.series.split(",") if s.strip()]
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
