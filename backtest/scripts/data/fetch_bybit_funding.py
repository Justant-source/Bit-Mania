#!/usr/bin/env python3
"""Fetch Bybit BTCUSDT USDT-perp funding rate history (public REST, no auth) and
cache to CSV for the replay engine's opt-in --funding-csv flag.

Endpoint: https://api.bybit.com/v5/market/funding/history (category=linear, symbol=BTCUSDT)
requires both startTime and endTime per call; max 200 rows/call (~66 days @ 8h interval).
Confirmed earliest available record: ~2020-03-25 (before that: API returns empty, not an
error — the product likely didn't exist yet; see backtest/results/funding/PROVENANCE.md
for the exact discovery timestamp). Idempotent: reads any existing CSV, resumes from
(last_timestamp + 1ms) instead of re-fetching from scratch.

Not used: jesse_db.funding_8h (schema.sql) — that table's source is unverified and has no
loader script in the tree (see backtest/results/2026-08-31/csv_ohlcv_drift.md §7). This
script fetches a freshly verified series into a CSV instead, following the same "don't
relabel an unverified source, re-fetch a verified one" precedent that fixed the OHLCV
provenance mixup.

Usage:
    python backtest/scripts/data/fetch_bybit_funding.py
    python backtest/scripts/data/fetch_bybit_funding.py --out backtest/results/funding/bybit_btcusdt_funding_8h.csv
    python backtest/scripts/data/fetch_bybit_funding.py --end 2020-06-01   # partial fetch, for testing resume
"""
from __future__ import annotations

import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://api.bybit.com/v5/market/funding/history"
SYMBOL = "BTCUSDT"
CATEGORY = "linear"
WINDOW_MS = 66 * 86_400_000  # ~200 records/call at 8h spacing, stay under limit=200
DEFAULT_OUT = Path(__file__).resolve().parents[2] / "results" / "funding" / "bybit_btcusdt_funding_8h.csv"
# earliest confirmed non-empty response as of the 2026-09-02 discovery run; fetch still
# probes from here rather than hardcoding a "true" launch date it never verified further back
EARLIEST_PROBE = int(datetime(2020, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds; delay = RETRY_DELAY ** (attempt + 1)


def _get(session: requests.Session, params: dict) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(URL, params=params, timeout=15)
            r.raise_for_status()
            payload = r.json()
            if payload["retCode"] != 0:
                raise RuntimeError(f"Bybit API error {payload['retCode']}: {payload['retMsg']}")
            return payload
        except (requests.RequestException, RuntimeError) as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY ** (attempt + 1)
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                raise


def _fetch_window(start_ms: int, end_ms: int, session: requests.Session) -> list[dict]:
    out = []
    cursor = start_ms
    while cursor < end_ms:
        hi = min(cursor + WINDOW_MS, end_ms)
        payload = _get(session, {
            "category": CATEGORY, "symbol": SYMBOL,
            "startTime": cursor, "endTime": hi, "limit": 200,
        })
        rows = payload["result"]["list"]
        out.extend(rows)
        cursor = hi
        time.sleep(0.2)  # polite pacing, public endpoint has no auth budget to protect
    return out


def load_existing(path: Path) -> tuple[list[tuple[int, float]], int | None]:
    if not path.exists():
        return [], None
    rows = []
    with path.open() as f:
        for row in csv.DictReader(f):
            rows.append((int(row["timestamp_ms"]), float(row["funding_rate"])))
    rows.sort()
    return rows, (rows[-1][0] if rows else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--end", default=None, help="YYYY-MM-DD UTC, default: now")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    existing, last_ts = load_existing(out_path)

    end_ms = (int(datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
              if args.end else int(datetime.now(timezone.utc).timestamp() * 1000))
    start_ms = (last_ts + 1) if last_ts is not None else EARLIEST_PROBE

    if start_ms >= end_ms:
        print(f"nothing to do: start={start_ms} >= end={end_ms}")
        return

    with requests.Session() as s:
        fetched = _fetch_window(start_ms, end_ms, s)

    by_ts = {int(r["fundingRateTimestamp"]): float(r["fundingRate"]) for r in fetched}
    merged = {ts: rate for ts, rate in existing}
    merged.update(by_ts)
    rows = sorted(merged.items())

    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_ms", "funding_rate"])
        for ts, rate in rows:
            w.writerow([ts, rate])

    if rows:
        first_dt = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
        last_dt = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
        print(f"wrote {len(rows)} rows -> {out_path}")
        print(f"coverage: {first_dt} .. {last_dt}")
        print(f"newly fetched this run: {len(by_ts)}")
    else:
        print("no data returned — check date range / API availability")


if __name__ == "__main__":
    main()
