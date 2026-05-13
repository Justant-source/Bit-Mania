#!/usr/bin/env python3
"""Fetch Binance BTCUSDT perpetual funding rates (2019-09-08 ~ 2020-03-25) via public API.

Binance Futures BTCUSDT launched 2019-09-08. This script fills the gap in local funding
history (pre-Bybit era) by fetching from the public API endpoint.
Bybit BTCUSDT linear perp funding data starts 2020-03-25, so Binance covers the gap.

Output: parquet with columns [timestamp (int64 ms), funding_rate (float64)]
"""
from __future__ import annotations
import json
import sys
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
OUT_DIR = ROOT / 'data' / 'funding' / 'binance_api'
OUT_FILE = OUT_DIR / 'BTCUSDT_2019.parquet'
FAIL_MARKER = OUT_DIR / 'FETCH_FAILED.marker'

SYMBOL = 'BTCUSDT'
BASE_URL = 'https://fapi.binance.com/fapi/v1/fundingRate'
LIMIT = 1000

# 2019-09-08 00:00:00 UTC to 2020-03-25 16:00:00 UTC (Bybit data starts 2020-03-26)
START_MS = int(datetime(2019, 9, 8, tzinfo=timezone.utc).timestamp() * 1000)
END_MS = int(datetime(2020, 3, 26, tzinfo=timezone.utc).timestamp() * 1000) - 1


def fetch_page(start_ms: int, end_ms: int) -> list[dict]:
    """Fetch one page of funding rates from Binance API.

    Args:
        start_ms: start time in milliseconds (inclusive)
        end_ms: end time in milliseconds (inclusive)

    Returns:
        List of funding rate records. Empty list if error or no data.
        Returns None on fatal error (to signal early exit).
    """
    params = {
        'symbol': SYMBOL,
        'startTime': str(start_ms),
        'endTime': str(end_ms),
        'limit': str(LIMIT),
    }
    query_string = urllib.parse.urlencode(params)
    url = f'{BASE_URL}?{query_string}'

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            if isinstance(data, list):
                return data
            else:
                print(f'[fetch] Unexpected response format: {type(data)}', file=sys.stderr)
                return None
    except urllib.error.HTTPError as e:
        print(f'[fetch] HTTP error {e.code}: {e.reason}', file=sys.stderr)
        return None
    except urllib.error.URLError as e:
        print(f'[fetch] URL error: {e.reason}', file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f'[fetch] JSON parse error: {e}', file=sys.stderr)
        return None
    except Exception as e:
        print(f'[fetch] Unexpected error: {e}', file=sys.stderr)
        return None


def fetch_all() -> list[dict]:
    """Fetch all funding rates from START_MS to END_MS.

    Uses pagination with sliding startTime window. Each page's last
    fundingTime becomes the next page's startTime + 1.

    Returns:
        List of all funding rate records (may be empty).
    """
    all_rows = []
    current_start = START_MS

    while current_start <= END_MS:
        page = fetch_page(current_start, END_MS)

        if page is None:
            # Fatal error during fetch
            return None

        if not page:
            # No data in this page; stop pagination
            break

        all_rows.extend(page)
        last_funding_time = int(page[-1]['fundingTime'])

        # Move start time to just after last record
        current_start = last_funding_time + 1

        print(f'[fetch] Fetched {len(page)} rows ({current_start - 1} → {END_MS})', flush=True)

    return all_rows


def write_failure_marker(error_msg: str) -> None:
    """Write failure marker file with error message and timestamp."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()
    content = f'{timestamp}\n{error_msg}\n'
    FAIL_MARKER.write_text(content)


def main() -> None:
    """Main entry point."""
    print(f'[fetch] Fetching {SYMBOL} funding rates from {START_MS} to {END_MS}', flush=True)

    # Ensure output directory exists
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Fetch all pages
    rows = fetch_all()

    if rows is None:
        # Fatal error during fetch
        error_msg = 'Fatal error during API fetch (see stderr)'
        write_failure_marker(error_msg)
        sys.exit(0)

    if not rows:
        # No data returned
        error_msg = 'No data returned from Binance API'
        write_failure_marker(error_msg)
        sys.exit(0)

    print(f'[fetch] Total rows fetched: {len(rows)}', flush=True)

    # Convert to DataFrame
    df = pd.DataFrame(rows)

    # Rename and extract columns
    df = df[['fundingTime', 'fundingRate']].rename(
        columns={'fundingTime': 'timestamp', 'fundingRate': 'funding_rate'}
    )

    # Convert timestamp to int64 (already in ms)
    df['timestamp'] = df['timestamp'].astype('int64')

    # Convert funding_rate to float64
    df['funding_rate'] = df['funding_rate'].astype('float64')

    # Sort by timestamp and remove duplicates
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

    print(f'[fetch] Writing {len(df)} rows to {OUT_FILE}', flush=True)

    # Write parquet
    df.to_parquet(OUT_FILE, engine='pyarrow', index=False)

    print(f'[fetch] Done. Output: {OUT_FILE}', flush=True)


if __name__ == '__main__':
    main()
