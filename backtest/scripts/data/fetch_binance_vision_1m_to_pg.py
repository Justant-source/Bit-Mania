#!/usr/bin/env python3
"""Fetch Binance Vision 1m klines into jesse_db ohlcv_1m, then resample new bars to ohlcv_4h.

Extends existing ohlcv through a date range (default 2026-05-01 .. 2026-08-28).
Existing 4h rows (through 2026-04-30) are not rewritten: INSERT … ON CONFLICT DO NOTHING.

Vision URLs / retry / CSV columns / µs→ms follow a88a22ac download_binance_vision.py.
4h resample follows a88a22ac import_parquet_to_pg.py:
    bar_open = (timestamp // 14_400_000) * 14_400_000
    open=first, high=max, low=min, close=last, volume=sum

Usage:
    python backtest/scripts/data/fetch_binance_vision_1m_to_pg.py \\
        --start 2026-05-01 --end 2026-08-28 --source perp \\
        --verify-overlap-day 2026-04-30
    python backtest/scripts/data/fetch_binance_vision_1m_to_pg.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import zipfile
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

# DB connect: same pattern as backtest/scripts/optimization/pg_*.py (no password defaults)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "db"))
from _common import connect  # noqa: E402

EXCHANGE = "Bybit Perpetual"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"

FUTURES_MONTHLY = "https://data.binance.vision/data/futures/um/monthly/klines"
FUTURES_DAILY = "https://data.binance.vision/data/futures/um/daily/klines"
SPOT_MONTHLY = "https://data.binance.vision/data/spot/monthly/klines"
SPOT_DAILY = "https://data.binance.vision/data/spot/daily/klines"

BINANCE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds; delay = RETRY_DELAY ** (attempt + 1)
BATCH_SIZE = 50_000
OVERLAP_MATCH_THRESHOLD = 0.999
CLOSE_REL_TOL = 1e-6
MIN_BARS_PER_DAY = 1440
FOUR_H_MS = 14_400_000
US_THRESHOLD = 1e14  # open_time > this is microseconds, convert → ms


def _day_start_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _next_day(d: date) -> date:
    return d + timedelta(days=1)


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _bases(source: str) -> tuple[str, str]:
    if source == "spot":
        return SPOT_MONTHLY, SPOT_DAILY
    if source == "perp":
        return FUTURES_MONTHLY, FUTURES_DAILY
    raise ValueError(f"unknown source {source!r}")


def _download_with_retry(url: str) -> bytes:
    """Download file with exponential backoff (a88a22ac)."""
    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  GET {url}", flush=True)
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY ** (attempt + 1)
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retry in {delay}s...")
                time.sleep(delay)
            else:
                print(f"  Failed to download {url} after {MAX_RETRIES} attempts.")
    raise RuntimeError(f"download failed: {url}") from last_err


def _open_time_to_ms(raw: str) -> int | None:
    """Parse Binance open_time; µs (>1e14) → ms (a88a22ac Vision CSVs)."""
    raw = raw.strip()
    if not raw or not raw.replace(".", "", 1).isdigit():
        return None
    ts = int(float(raw))
    if ts > US_THRESHOLD:
        ts = ts // 1000
    return ts


def _parse_zip_csv(content: bytes) -> list[tuple[int, float, float, float, float, float]]:
    """Parse Vision kline ZIP → (timestamp_ms, open, high, low, close, volume)."""
    rows: list[tuple[int, float, float, float, float, float]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_files:
            raise RuntimeError("no CSV in zip")
        with zf.open(csv_files[0]) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
            reader = csv.reader(text)
            first = True
            for rec in reader:
                if not rec:
                    continue
                if first:
                    first = False
                    # Skip header row (newer Vision CSVs include column names)
                    if rec[0].strip().lower() in ("open_time", "open time") or _open_time_to_ms(rec[0]) is None:
                        continue
                if len(rec) < 6:
                    continue
                ts = _open_time_to_ms(rec[0])
                if ts is None:
                    continue
                try:
                    o, h, l, c, v = (float(rec[1]), float(rec[2]), float(rec[3]), float(rec[4]), float(rec[5]))
                except (ValueError, TypeError):
                    continue
                rows.append((ts, o, h, l, c, v))
    return rows


def _filter_range(
    rows: list[tuple[int, float, float, float, float, float]],
    start_ms: int,
    end_exclusive_ms: int,
) -> list[tuple[int, float, float, float, float, float]]:
    return [r for r in rows if start_ms <= r[0] < end_exclusive_ms]


def _dedupe_sort(
    rows: list[tuple[int, float, float, float, float, float]],
) -> list[tuple[int, float, float, float, float, float]]:
    by_ts: dict[int, tuple[int, float, float, float, float, float]] = {}
    for r in rows:
        by_ts.setdefault(r[0], r)
    return [by_ts[k] for k in sorted(by_ts)]


def _month_jobs(start: date, end: date) -> list[tuple[str, str]]:
    """Complete months → monthly zip; partial months → daily zips."""
    jobs: list[tuple[str, str]] = []
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        last_dom = monthrange(y, m)[1]
        month_start = date(y, m, 1)
        month_end = date(y, m, last_dom)
        span_start = max(start, month_start)
        span_end = min(end, month_end)
        if span_start == month_start and span_end == month_end:
            jobs.append(("monthly", f"{y:04d}-{m:02d}"))
        else:
            d = span_start
            while d <= span_end:
                jobs.append(("daily", d.isoformat()))
                d += timedelta(days=1)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return jobs


def _job_url(kind: str, key: str, source: str) -> str:
    monthly_base, daily_base = _bases(source)
    if kind == "monthly":
        return f"{monthly_base}/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{key}.zip"
    return f"{daily_base}/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{key}.zip"


def _closes_match(a: float, b: float) -> bool:
    if a == b:
        return True
    return abs(a - b) / max(abs(a), 1e-12) < CLOSE_REL_TOL


def overlap_gate(
    conn,
    overlap_day: date,
    source: str,
) -> None:
    """Compare Vision 1m closes vs DB ohlcv_1m for one calendar day. Exit 2 on fail."""
    url = _job_url("daily", overlap_day.isoformat(), source)
    print(f"\n[overlap] {overlap_day.isoformat()} source={source}")
    rows = _dedupe_sort(_parse_zip_csv(_download_with_retry(url)))
    day_ms = _day_start_ms(overlap_day)
    next_ms = _day_start_ms(_next_day(overlap_day))
    vision = {r[0]: r[4] for r in rows if day_ms <= r[0] < next_ms}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, close
            FROM ohlcv_1m
            WHERE exchange = %s AND symbol = %s
              AND timestamp >= %s AND timestamp < %s
            """,
            (EXCHANGE, SYMBOL, day_ms, next_ms),
        )
        db_rows = cur.fetchall()
    db = {int(ts): float(close) for ts, close in db_rows}

    overlap_ts = sorted(set(vision) & set(db))
    n_overlap = len(overlap_ts)
    n_match = sum(1 for ts in overlap_ts if _closes_match(vision[ts], db[ts]))
    rate = (n_match / n_overlap) if n_overlap else 0.0
    print(
        f"  vision_day={len(vision)} db_day={len(db)} "
        f"overlap={n_overlap} match={n_match} rate={rate:.6%}"
    )
    if n_overlap == 0 or rate < OVERLAP_MATCH_THRESHOLD:
        print("try other --source")
        sys.exit(2)


def resample_to_4h(
    rows_1m: list[tuple[int, float, float, float, float, float]],
) -> list[tuple[int, float, float, float, float, float]]:
    """1m (timestamp ms) → 4h OHLCV at bar-open timestamp (a88a22ac)."""
    groups: dict[int, list[float]] = {}
    for ts, o, h, l, c, v in rows_1m:
        ts4h = (ts // FOUR_H_MS) * FOUR_H_MS
        if ts4h not in groups:
            groups[ts4h] = [o, h, l, c, v]
        else:
            g = groups[ts4h]
            if h > g[1]:
                g[1] = h
            if l < g[2]:
                g[2] = l
            g[3] = c
            g[4] += v
    return [(ts4h, g[0], g[1], g[2], g[3], g[4]) for ts4h, g in sorted(groups.items())]


def _count_by_day(
    rows: list[tuple[int, float, float, float, float, float]],
    start: date,
    end: date,
) -> dict[date, int]:
    counts: dict[date, int] = defaultdict(int)
    for ts, *_ in rows:
        d = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).date()
        if start <= d <= end:
            counts[d] += 1
    return counts


def _validate_full_days(
    rows: list[tuple[int, float, float, float, float, float]],
    start: date,
    end: date,
) -> None:
    counts = _count_by_day(rows, start, end)
    missing: list[str] = []
    short: list[str] = []
    d = start
    while d <= end:
        n = counts.get(d, 0)
        if n < MIN_BARS_PER_DAY:
            msg = f"{d.isoformat()} n={n} (need {MIN_BARS_PER_DAY})"
            if n == 0:
                missing.append(msg)
            else:
                short.append(msg)
        d = _next_day(d)
    if missing or short:
        print("[error] days with < 1440 1m rows (BTCUSDT perp is 24/7):")
        for line in missing + short:
            print(f"  {line}")
        sys.exit(1)


def _insert_ohlcv(conn, table: str, rows: list[tuple[int, float, float, float, float, float]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {table} (exchange, symbol, timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
    """
    attempted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            payload = [(EXCHANGE, SYMBOL, ts, o, h, l, c, v) for ts, o, h, l, c, v in batch]
            cur.executemany(sql, payload)
            attempted += len(payload)
            print(f"    {table}: attempted {attempted:,}/{len(rows):,}", flush=True)
    conn.commit()
    return attempted


def fetch_range(start: date, end: date, source: str) -> list[tuple[int, float, float, float, float, float]]:
    start_ms = _day_start_ms(start)
    end_excl = _day_start_ms(_next_day(end))
    collected: list[tuple[int, float, float, float, float, float]] = []
    jobs = _month_jobs(start, end)
    print(f"\n[download] {len(jobs)} Vision files (source={source})")
    for kind, key in jobs:
        url = _job_url(kind, key, source)
        parsed = _parse_zip_csv(_download_with_retry(url))
        collected.extend(_filter_range(parsed, start_ms, end_excl))
    return _dedupe_sort(_filter_range(collected, start_ms, end_excl))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Binance Vision 1m → jesse_db ohlcv_1m + resample ohlcv_4h (new bars only)"
    )
    p.add_argument("--start", default="2026-05-01", metavar="YYYY-MM-DD")
    p.add_argument("--end", default="2026-08-28", metavar="YYYY-MM-DD")
    p.add_argument(
        "--source",
        default="spot",
        choices=["perp", "spot"],
        help="Vision market. Existing jesse_db 1m matches spot (perp overlap 0%% on 2026-04-30).",
    )
    p.add_argument("--verify-overlap-day", default="2026-04-30", metavar="YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true", help="overlap + parse only; no INSERT")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    start = _parse_ymd(args.start)
    end = _parse_ymd(args.end)
    overlap_day = _parse_ymd(args.verify_overlap_day)
    if end < start:
        print("--end must be >= --start")
        sys.exit(1)

    print(f"Binance Vision 1m → PG  start={start} end={end} source={args.source}")
    print(f"  labels exchange={EXCHANGE!r} symbol={SYMBOL}")
    print(f"  overlap_day={overlap_day} dry_run={args.dry_run}")

    conn = connect()
    try:
        overlap_gate(conn, overlap_day, args.source)
        rows_1m = fetch_range(start, end, args.source)
        print(f"\n[parse] {len(rows_1m):,} 1m rows in [{start}, {end}] (23:59 inclusive)")
        _validate_full_days(rows_1m, start, end)
        rows_4h = resample_to_4h(rows_1m)
        print(f"[resample] {len(rows_4h):,} 4h bars from NEW 1m only")

        if args.dry_run:
            print(f"\n[dry-run] no INSERT")
            print(f"  ohlcv_1m attempted={len(rows_1m)}")
            print(f"  ohlcv_4h attempted={len(rows_4h)}")
            return

        print("\n[insert] ON CONFLICT DO NOTHING")
        n1m = _insert_ohlcv(conn, "ohlcv_1m", rows_1m)
        n4h = _insert_ohlcv(conn, "ohlcv_4h", rows_4h)
        print(f"\n[done] ohlcv_1m attempted={n1m}  ohlcv_4h attempted={n4h}")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
