"""
Phase 8.2 — Build Macro Event Calendar for FOMC/CPI Strategy.

Sources:
  1. Hardcoded known events 2023-2026 (reliable baseline)
  2. FRED API (FOMC dates from federal funds rate decisions) — requires API key
  3. Manual verification recommended

Output: /data/macro_events/fomc_cpi_calendar.csv

Usage:
    # Basic (hardcoded events only)
    python scripts/data/build_macro_calendar.py

    # With FRED API (more accurate)
    FRED_API_KEY=your_key python scripts/data/build_macro_calendar.py \
        --start 2023-01-01 --end 2026-12-31 --use-fred
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(os.environ.get("DATA_DIR", "/data")) / "macro_events"
OUTPUT_FILE = OUTPUT_DIR / "fomc_cpi_calendar.csv"

# ─── Hardcoded event database (2023-2026) ─────────────────────────────────────
# FOMC decision times: typically 18:00 UTC (2pm ET)
# CPI release times: typically 12:30 UTC (8:30am ET)

KNOWN_EVENTS = [
    # ── 2023 FOMC ─────────────────────────────────────────────────────────────
    ("FOMC", "2023-02-01 19:00", "Feb FOMC rate decision (+25bp)"),
    ("FOMC", "2023-03-22 18:00", "Mar FOMC rate decision (+25bp)"),
    ("FOMC", "2023-05-03 18:00", "May FOMC rate decision (+25bp)"),
    ("FOMC", "2023-06-14 18:00", "Jun FOMC rate decision (hold)"),
    ("FOMC", "2023-07-26 18:00", "Jul FOMC rate decision (+25bp)"),
    ("FOMC", "2023-09-20 18:00", "Sep FOMC rate decision (hold)"),
    ("FOMC", "2023-11-01 18:00", "Nov FOMC rate decision (hold)"),
    ("FOMC", "2023-12-13 19:00", "Dec FOMC rate decision (hold, dot plot)"),
    # ── 2023 CPI ──────────────────────────────────────────────────────────────
    ("CPI", "2023-01-12 13:30", "Dec 2022 CPI"),
    ("CPI", "2023-02-14 13:30", "Jan 2023 CPI"),
    ("CPI", "2023-03-14 12:30", "Feb 2023 CPI"),
    ("CPI", "2023-04-12 12:30", "Mar 2023 CPI"),
    ("CPI", "2023-05-10 12:30", "Apr 2023 CPI"),
    ("CPI", "2023-06-13 12:30", "May 2023 CPI"),
    ("CPI", "2023-07-12 12:30", "Jun 2023 CPI"),
    ("CPI", "2023-08-10 12:30", "Jul 2023 CPI"),
    ("CPI", "2023-09-13 12:30", "Aug 2023 CPI"),
    ("CPI", "2023-10-12 12:30", "Sep 2023 CPI"),
    ("CPI", "2023-11-14 13:30", "Oct 2023 CPI"),
    ("CPI", "2023-12-12 13:30", "Nov 2023 CPI"),
    # ── 2024 FOMC ─────────────────────────────────────────────────────────────
    ("FOMC", "2024-01-31 19:00", "Jan 2024 FOMC (hold)"),
    ("FOMC", "2024-03-20 18:00", "Mar 2024 FOMC (hold)"),
    ("FOMC", "2024-05-01 18:00", "May 2024 FOMC (hold)"),
    ("FOMC", "2024-06-12 18:00", "Jun 2024 FOMC (hold)"),
    ("FOMC", "2024-07-31 18:00", "Jul 2024 FOMC (hold)"),
    ("FOMC", "2024-09-18 18:00", "Sep 2024 FOMC (-50bp, first cut)"),
    ("FOMC", "2024-11-07 19:00", "Nov 2024 FOMC (-25bp)"),
    ("FOMC", "2024-12-18 19:00", "Dec 2024 FOMC (-25bp, hawkish dot plot)"),
    # ── 2024 CPI ──────────────────────────────────────────────────────────────
    ("CPI", "2024-01-11 13:30", "Dec 2023 CPI"),
    ("CPI", "2024-02-13 13:30", "Jan 2024 CPI"),
    ("CPI", "2024-03-12 12:30", "Feb 2024 CPI"),
    ("CPI", "2024-04-10 12:30", "Mar 2024 CPI (surprise +3.5%)"),
    ("CPI", "2024-05-15 12:30", "Apr 2024 CPI"),
    ("CPI", "2024-06-12 12:30", "May 2024 CPI"),
    ("CPI", "2024-07-11 12:30", "Jun 2024 CPI"),
    ("CPI", "2024-08-14 12:30", "Jul 2024 CPI"),
    ("CPI", "2024-09-11 12:30", "Aug 2024 CPI"),
    ("CPI", "2024-10-10 12:30", "Sep 2024 CPI"),
    ("CPI", "2024-11-13 13:30", "Oct 2024 CPI"),
    ("CPI", "2024-12-11 13:30", "Nov 2024 CPI"),
    # ── 2025 FOMC ─────────────────────────────────────────────────────────────
    ("FOMC", "2025-01-29 19:00", "Jan 2025 FOMC (hold)"),
    ("FOMC", "2025-03-19 18:00", "Mar 2025 FOMC (hold, tariff uncertainty)"),
    ("FOMC", "2025-05-07 18:00", "May 2025 FOMC (hold)"),
    ("FOMC", "2025-06-18 18:00", "Jun 2025 FOMC"),
    ("FOMC", "2025-07-30 18:00", "Jul 2025 FOMC"),
    ("FOMC", "2025-09-17 18:00", "Sep 2025 FOMC"),
    ("FOMC", "2025-10-29 18:00", "Oct 2025 FOMC"),
    ("FOMC", "2025-12-10 19:00", "Dec 2025 FOMC"),
    # ── 2025 CPI ──────────────────────────────────────────────────────────────
    ("CPI", "2025-01-15 13:30", "Dec 2024 CPI"),
    ("CPI", "2025-02-12 13:30", "Jan 2025 CPI"),
    ("CPI", "2025-03-12 12:30", "Feb 2025 CPI"),
    ("CPI", "2025-04-10 12:30", "Mar 2025 CPI"),
    ("CPI", "2025-05-13 12:30", "Apr 2025 CPI"),
    ("CPI", "2025-06-11 12:30", "May 2025 CPI"),
    ("CPI", "2025-07-15 12:30", "Jun 2025 CPI"),
    ("CPI", "2025-08-12 12:30", "Jul 2025 CPI"),
    ("CPI", "2025-09-10 12:30", "Aug 2025 CPI"),
    ("CPI", "2025-10-14 12:30", "Sep 2025 CPI"),
    ("CPI", "2025-11-12 13:30", "Oct 2025 CPI"),
    ("CPI", "2025-12-10 13:30", "Nov 2025 CPI"),
    # ── 2026 projected ────────────────────────────────────────────────────────
    ("FOMC", "2026-01-28 19:00", "Jan 2026 FOMC (projected)"),
    ("FOMC", "2026-03-18 18:00", "Mar 2026 FOMC (projected)"),
    ("CPI",  "2026-01-14 13:30", "Dec 2025 CPI (projected)"),
    ("CPI",  "2026-02-11 13:30", "Jan 2026 CPI (projected)"),
    ("CPI",  "2026-03-11 12:30", "Feb 2026 CPI (projected)"),
    ("CPI",  "2026-04-08 12:30", "Mar 2026 CPI (projected)"),
]


def fetch_fred_fomc_dates(api_key: str, start: str, end: str) -> list[tuple]:
    """
    Fetch FOMC decision dates from FRED API.
    Series: DFEDTARL (daily fed funds rate lower bound).
    Returns list of (event_type, timestamp_utc_str, description).
    """
    try:
        import urllib.request
        import json

        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id=DFEDTARL&observation_start={start}&observation_end={end}"
            f"&api_key={api_key}&file_type=json&frequency=d"
        )
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        obs = data.get("observations", [])

        # FOMC dates = dates where fed funds rate changed
        events = []
        prev_val = None
        for ob in obs:
            val = ob.get("value", ".")
            if val != "." and val != prev_val and prev_val is not None:
                date_str = ob["date"]  # YYYY-MM-DD
                ts_utc = f"{date_str} 18:00"  # approximate decision time
                events.append(("FOMC", ts_utc, f"FRED: rate change to {val}%"))
            if val != ".":
                prev_val = val
        return events
    except Exception as exc:
        print(f"  WARN: FRED fetch failed: {exc}", file=sys.stderr)
        return []


def main():
    p = argparse.ArgumentParser(description="Build FOMC/CPI macro event calendar")
    p.add_argument("--start",    default="2023-01-01")
    p.add_argument("--end",      default="2026-12-31")
    p.add_argument("--use-fred", action="store_true")
    p.add_argument("--output",   default=str(OUTPUT_FILE))
    args = p.parse_args()

    events = list(KNOWN_EVENTS)
    print(f"[build_macro_calendar] Loaded {len(events)} hardcoded events")

    if args.use_fred:
        api_key = os.environ.get("FRED_API_KEY", "")
        if not api_key:
            print("  WARN: FRED_API_KEY not set, skipping FRED fetch", file=sys.stderr)
        else:
            fred_events = fetch_fred_fomc_dates(api_key, args.start, args.end)
            print(f"  FRED: {len(fred_events)} additional events")
            events.extend(fred_events)

    # Filter by date range
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    filtered = []
    for ev_type, ts_str, desc in events:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                if start_dt <= dt <= end_dt:
                    filtered.append((ev_type, ts_str, desc))
                break
            except ValueError:
                continue

    # Deduplicate and sort
    seen = set()
    unique = []
    for row in filtered:
        key = (row[0], row[1])
        if key not in seen:
            seen.add(key)
            unique.append(row)
    unique.sort(key=lambda x: x[1])

    # Write CSV
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_type", "timestamp_utc", "description"])
        for row in unique:
            writer.writerow(row)

    print(f"  Written {len(unique)} events to {out_path}")

    # Summary
    fomc_count = sum(1 for r in unique if r[0] == "FOMC")
    cpi_count  = sum(1 for r in unique if r[0] == "CPI")
    print(f"  FOMC: {fomc_count}, CPI: {cpi_count}, Total: {len(unique)}")


if __name__ == "__main__":
    main()
