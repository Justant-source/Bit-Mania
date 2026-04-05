"""Test O — Funding Rate Time-of-Day and Day-of-Week Analysis

Pure analytics — no FA simulation. Examines funding rate patterns across:
  1. Hour-of-day (highlighting settlement hours 00:00, 08:00, 16:00 UTC)
  2. Day-of-week
  3. Settlement-time comparison (00:00 vs 08:00 vs 16:00 UTC)
  4. Price volatility around settlement bars vs non-settlement bars
  5. Consecutive positive funding streaks (validates consecutive_intervals=3)

Results saved as a single row to strategy_variant_results
(test_name='test_o_funding_time').

Usage:
    python tests/backtest/bt_funding_time_analysis.py \\
        --start 2020-04-01 --end 2026-03-31
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
# ── sys.path: allow importing from parent backtester directory ────────────────
_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))   # .../backtester/
sys.path.insert(0, _PARENT_DIR)

log = structlog.get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL      = "BTCUSDT"
TIMEFRAME   = "1h"
TEST_NAME   = "test_o_funding_time"

SETTLEMENT_HOURS = {0, 8, 16}          # Bybit funding settlement UTC hours
WEEKDAY_NAMES    = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


# =============================================================================
# DB helpers
# =============================================================================

async def load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = $2
              AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            """,
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


async def load_funding(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    df["rate"] = df["rate"].astype(float)
    return df


def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


async def save_summary(
    pool: asyncpg.Pool,
    summary: dict[str, Any],
    data_range: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO strategy_variant_results
                (test_name, variant_name, data_range,
                 total_return, sharpe_ratio, max_drawdown,
                 trade_count, win_rate, profit_factor,
                 monthly_returns, params)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
            ON CONFLICT DO NOTHING
            """,
            TEST_NAME,
            "funding_time_analysis",
            data_range,
            0.0, 0.0, 0.0,   # not applicable for analytics
            0, 0.0, 0.0,
            json.dumps({}),
            json.dumps(summary),
        )
    log.info("summary_saved", test=TEST_NAME, data_range=data_range)


# =============================================================================
# Analysis functions
# =============================================================================

def _print_section(title: str) -> None:
    width = 70
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def analyze_hour_of_day(funding: pd.DataFrame) -> dict[str, Any]:
    """1. Hour-of-day analysis. Returns stats dict keyed by hour."""
    _print_section("1. Hour-of-Day Analysis")

    funding = funding.copy()
    funding["hour"] = funding.index.hour

    header = f"{'Hour':>5} {'Count':>7} {'Mean%':>9} {'Std%':>9} {'Median%':>9} {'PctPos%':>8}  {'Note'}"
    print(header)
    print("-" * len(header))

    stats: dict[str, Any] = {}
    for h in range(24):
        subset = funding[funding["hour"] == h]["rate"]
        count  = len(subset)
        if count == 0:
            mean_pct = std_pct = med_pct = pos_pct = 0.0
        else:
            mean_pct = float(subset.mean()) * 100
            std_pct  = float(subset.std())  * 100 if len(subset) > 1 else 0.0
            med_pct  = float(subset.median()) * 100
            pos_pct  = float((subset > 0).sum()) / count * 100

        note = "  <-- SETTLEMENT" if h in SETTLEMENT_HOURS else ""
        print(
            f"{h:>5}  {count:>7}  {mean_pct:>+8.4f}%  {std_pct:>8.4f}%  "
            f"{med_pct:>+8.4f}%  {pos_pct:>7.2f}%{note}"
        )
        stats[str(h)] = {
            "count": count,
            "mean_pct": round(mean_pct, 6),
            "std_pct":  round(std_pct, 6),
            "median_pct": round(med_pct, 6),
            "positive_pct": round(pos_pct, 4),
        }
    return stats


def analyze_day_of_week(funding: pd.DataFrame) -> dict[str, Any]:
    """2. Day-of-week analysis."""
    _print_section("2. Day-of-Week Analysis")

    funding = funding.copy()
    funding["weekday"] = funding.index.weekday  # 0=Mon, 6=Sun

    header = f"{'Day':>5} {'Name':>5} {'Count':>7} {'Mean%':>9} {'Std%':>9} {'PctPos%':>8}"
    print(header)
    print("-" * len(header))

    stats: dict[str, Any] = {}
    for d in range(7):
        subset = funding[funding["weekday"] == d]["rate"]
        count  = len(subset)
        if count == 0:
            mean_pct = std_pct = pos_pct = 0.0
        else:
            mean_pct = float(subset.mean()) * 100
            std_pct  = float(subset.std())  * 100 if len(subset) > 1 else 0.0
            pos_pct  = float((subset > 0).sum()) / count * 100

        print(
            f"{d:>5}  {WEEKDAY_NAMES[d]:>5}  {count:>7}  {mean_pct:>+8.4f}%  "
            f"{std_pct:>8.4f}%  {pos_pct:>7.2f}%"
        )
        stats[WEEKDAY_NAMES[d]] = {
            "weekday_int": d,
            "count": count,
            "mean_pct": round(mean_pct, 6),
            "std_pct":  round(std_pct, 6),
            "positive_pct": round(pos_pct, 4),
        }
    return stats


def analyze_settlement_comparison(funding: pd.DataFrame) -> dict[str, Any]:
    """3. Settlement-time comparison: 00:00 vs 08:00 vs 16:00 UTC."""
    _print_section("3. Settlement-Time Comparison (00:00 vs 08:00 vs 16:00 UTC)")

    funding = funding.copy()
    funding["hour"] = funding.index.hour
    settlement_df = funding[funding["hour"].isin(SETTLEMENT_HOURS)]

    header = f"{'Settlement':>12} {'Count':>7} {'Mean%':>9} {'Std%':>9} {'Max%':>9} {'PctPos%':>8}"
    print(header)
    print("-" * len(header))

    stats: dict[str, Any] = {}
    for h in sorted(SETTLEMENT_HOURS):
        subset = settlement_df[settlement_df["hour"] == h]["rate"]
        count  = len(subset)
        if count == 0:
            mean_pct = std_pct = max_pct = pos_pct = 0.0
        else:
            mean_pct = float(subset.mean()) * 100
            std_pct  = float(subset.std())  * 100 if len(subset) > 1 else 0.0
            max_pct  = float(subset.max())  * 100
            pos_pct  = float((subset > 0).sum()) / count * 100

        key = f"{h:02d}:00 UTC"
        print(
            f"{key:>12}  {count:>7}  {mean_pct:>+8.4f}%  {std_pct:>8.4f}%  "
            f"{max_pct:>+8.4f}%  {pos_pct:>7.2f}%"
        )
        stats[key] = {
            "count": count,
            "mean_pct": round(mean_pct, 6),
            "std_pct":  round(std_pct, 6),
            "max_pct":  round(max_pct, 6),
            "positive_pct": round(pos_pct, 4),
        }
    return stats


def analyze_price_volatility(ohlcv: pd.DataFrame) -> dict[str, Any]:
    """4. Price volatility around settlement bars vs non-settlement bars."""
    _print_section("4. Price Volatility: Settlement Bars vs Non-Settlement Bars")

    if ohlcv.empty:
        print("  [SKIP] No OHLCV data available.")
        return {}

    df = ohlcv.copy()
    df["price_change_pct"] = ((df["close"] - df["open"]).abs() / df["open"] * 100)
    df["is_settlement"]    = df.index.hour.isin(SETTLEMENT_HOURS)

    settle_vol    = df[df["is_settlement"]]["price_change_pct"]
    nonsettle_vol = df[~df["is_settlement"]]["price_change_pct"]

    for label, series in [
        ("Settlement bars", settle_vol),
        ("Non-settlement bars", nonsettle_vol),
    ]:
        count  = len(series)
        mean_v = float(series.mean()) if count > 0 else 0.0
        std_v  = float(series.std())  if count > 1 else 0.0
        max_v  = float(series.max())  if count > 0 else 0.0
        print(
            f"  {label:<22}  count={count:>7}  "
            f"mean={mean_v:>7.4f}%  std={std_v:>7.4f}%  max={max_v:>7.4f}%"
        )

    # Per-settlement-hour breakdown
    print()
    header = f"  {'Hour':>5} {'Count':>7} {'Mean%':>9} {'Std%':>9}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    per_hour: dict[str, Any] = {}
    for h in sorted(SETTLEMENT_HOURS):
        subset = df[df.index.hour == h]["price_change_pct"]
        count  = len(subset)
        mean_v = float(subset.mean()) if count > 0 else 0.0
        std_v  = float(subset.std())  if count > 1 else 0.0
        print(f"  {h:>5}  {count:>7}  {mean_v:>+8.4f}%  {std_v:>8.4f}%")
        per_hour[f"{h:02d}:00"] = {
            "count": count,
            "mean_price_change_pct": round(mean_v, 6),
            "std_price_change_pct":  round(std_v, 6),
        }

    return {
        "settlement_mean_vol_pct":    round(float(settle_vol.mean())    if len(settle_vol)    > 0 else 0.0, 6),
        "nonsettlement_mean_vol_pct": round(float(nonsettle_vol.mean()) if len(nonsettle_vol) > 0 else 0.0, 6),
        "per_settlement_hour": per_hour,
    }


def analyze_positive_streaks(funding: pd.DataFrame) -> dict[str, Any]:
    """5. Consecutive positive funding streaks (only at settlement hours)."""
    _print_section("5. Consecutive Positive Funding Streaks (settlement hours only)")

    # Filter to settlement hours only (where entry logic actually runs)
    settle = funding[funding.index.hour.isin(SETTLEMENT_HOURS)].copy()
    settle = settle.sort_index()

    MIN_RATE = 0.0001  # same as FA_PARAMS["min_funding_rate"]

    rates = settle["rate"].values

    # Build streak lengths
    streaks: list[int] = []
    current = 0
    for r in rates:
        if float(r) >= MIN_RATE:
            current += 1
        else:
            if current > 0:
                streaks.append(current)
            current = 0
    if current > 0:
        streaks.append(current)

    total_streaks = len(streaks)
    if total_streaks == 0:
        print("  No positive funding streaks found.")
        return {}

    streak_arr = np.array(streaks, dtype=float)

    avg_len      = float(np.mean(streak_arr))
    median_len   = float(np.median(streak_arr))
    max_len      = int(np.max(streak_arr))
    pct_ge_3     = float(np.sum(streak_arr >= 3)) / total_streaks * 100
    pct_ge_5     = float(np.sum(streak_arr >= 5)) / total_streaks * 100

    # Distribution table
    from collections import Counter
    dist = Counter(int(s) for s in streak_arr)
    max_display = min(max_len, 20)

    print(f"  Total streaks           : {total_streaks}")
    print(f"  Average streak length   : {avg_len:.2f} intervals")
    print(f"  Median streak length    : {median_len:.2f} intervals")
    print(f"  Max streak length       : {max_len} intervals")
    print(f"  Streaks >= 3 intervals  : {pct_ge_3:.2f}%  "
          f"(consecutive_intervals=3 would catch these)")
    print(f"  Streaks >= 5 intervals  : {pct_ge_5:.2f}%")
    print()
    print(f"  {'Length':>8} {'Count':>8} {'Pct':>8}")
    print("  " + "-" * 28)
    for length in range(1, max_display + 1):
        cnt = dist.get(length, 0)
        pct = cnt / total_streaks * 100
        print(f"  {length:>8}  {cnt:>7}  {pct:>7.2f}%")
    if max_len > max_display:
        remainder = sum(v for k, v in dist.items() if k > max_display)
        print(f"  {'> ' + str(max_display):>8}  {remainder:>7}  "
              f"{remainder/total_streaks*100:>7.2f}%")

    return {
        "total_streaks":     total_streaks,
        "avg_streak_length": round(avg_len, 4),
        "median_streak_len": round(median_len, 4),
        "max_streak_length": max_len,
        "pct_streaks_ge_3":  round(pct_ge_3, 4),
        "pct_streaks_ge_5":  round(pct_ge_5, 4),
    }


# =============================================================================
# Main
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test O — Funding Rate Time-of-Day / Day-of-Week Analysis"
    )
    p.add_argument("--start",  default="2020-04-01", help="YYYY-MM-DD")
    p.add_argument("--end",    default="2026-03-31", help="YYYY-MM-DD")
    p.add_argument("--symbol", default=SYMBOL)
    return p.parse_args()


async def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            kst_timestamper,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    args = _parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    data_range = f"{args.start}~{args.end}"

    print(f"\n[Test O] Funding Time Analysis  |  {args.symbol}  |  {data_range}")
    print(f"DB: {DB_DSN.split('@')[-1]}")

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    log.info("loading_data", symbol=args.symbol, start=args.start, end=args.end)
    ohlcv   = await load_ohlcv(pool, args.symbol, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, args.symbol, start_dt, end_dt)

    if funding.empty:
        log.error(
            "no_funding_data",
            hint="먼저 seed_historical.py --data-type funding 을 실행하세요.",
        )
        await pool.close()
        return

    log.info(
        "data_loaded",
        ohlcv_bars=len(ohlcv),
        funding_rows=len(funding),
        funding_start=str(funding.index[0]) if len(funding) > 0 else "N/A",
        funding_end=str(funding.index[-1])   if len(funding) > 0 else "N/A",
    )

    # ── Run analyses ──────────────────────────────────────────────────────────
    hour_stats       = analyze_hour_of_day(funding)
    weekday_stats    = analyze_day_of_week(funding)
    settlement_stats = analyze_settlement_comparison(funding)
    volatility_stats = analyze_price_volatility(ohlcv)
    streak_stats     = analyze_positive_streaks(funding)

    # ── Derive top-level summary ──────────────────────────────────────────────
    hour_means = {
        int(h): s["mean_pct"] for h, s in hour_stats.items()
    }
    best_hour  = max(hour_means, key=hour_means.get)
    worst_hour = min(hour_means, key=hour_means.get)

    weekday_means = {
        name: s["mean_pct"] for name, s in weekday_stats.items()
    }
    best_weekday  = max(weekday_means, key=weekday_means.get)
    worst_weekday = min(weekday_means, key=weekday_means.get)

    s00 = settlement_stats.get("00:00 UTC", {})
    s08 = settlement_stats.get("08:00 UTC", {})
    s16 = settlement_stats.get("16:00 UTC", {})

    summary: dict[str, Any] = {
        # Hour-level
        "best_hour":          best_hour,
        "best_hour_mean":     round(hour_means[best_hour], 6),
        "worst_hour":         worst_hour,
        "worst_hour_mean":    round(hour_means[worst_hour], 6),
        # Settlement comparison
        "settlement_00_mean": _safe_float(s00.get("mean_pct")),
        "settlement_08_mean": _safe_float(s08.get("mean_pct")),
        "settlement_16_mean": _safe_float(s16.get("mean_pct")),
        "settlement_00_positive_pct": _safe_float(s00.get("positive_pct")),
        "settlement_08_positive_pct": _safe_float(s08.get("positive_pct")),
        "settlement_16_positive_pct": _safe_float(s16.get("positive_pct")),
        # Weekday
        "best_weekday":       best_weekday,
        "best_weekday_mean":  round(weekday_means[best_weekday], 6),
        "worst_weekday":      worst_weekday,
        "worst_weekday_mean": round(weekday_means[worst_weekday], 6),
        # Streaks
        "avg_streak_length":  _safe_float(streak_stats.get("avg_streak_length")),
        "pct_streaks_ge_3":   _safe_float(streak_stats.get("pct_streaks_ge_3")),
        "pct_streaks_ge_5":   _safe_float(streak_stats.get("pct_streaks_ge_5")),
        "max_streak_length":  streak_stats.get("max_streak_length", 0),
        # Volatility
        "settlement_mean_vol_pct":    _safe_float(volatility_stats.get("settlement_mean_vol_pct")),
        "nonsettlement_mean_vol_pct": _safe_float(volatility_stats.get("nonsettlement_mean_vol_pct")),
        # Detail blobs (for reference)
        "hour_stats":       hour_stats,
        "weekday_stats":    weekday_stats,
        "settlement_stats": settlement_stats,
        "streak_stats":     streak_stats,
        "volatility_stats": volatility_stats,
    }

    # ── Print final summary ───────────────────────────────────────────────────
    _print_section("Summary")
    print(f"  Best hour   : {best_hour:02d}:00 UTC  (mean {hour_means[best_hour]:+.4f}%)")
    print(f"  Worst hour  : {worst_hour:02d}:00 UTC  (mean {hour_means[worst_hour]:+.4f}%)")
    print(f"  Best weekday: {best_weekday}  (mean {weekday_means[best_weekday]:+.4f}%)")
    print(f"  Worst weekday: {worst_weekday}  (mean {weekday_means[worst_weekday]:+.4f}%)")
    print()
    print(f"  Settlement means: 00:00={s00.get('mean_pct',0):+.4f}%  "
          f"08:00={s08.get('mean_pct',0):+.4f}%  "
          f"16:00={s16.get('mean_pct',0):+.4f}%")
    print()
    avg_streak = streak_stats.get("avg_streak_length", 0)
    pct_ge3    = streak_stats.get("pct_streaks_ge_3", 0)
    print(f"  Avg streak length : {avg_streak:.2f} intervals")
    print(f"  Pct streaks >= 3  : {pct_ge3:.2f}%  "
          f"{'[OK — entry trigger reachable]' if pct_ge3 > 20 else '[WARNING — few streaks reach 3]'}")

    # ── Save to DB ────────────────────────────────────────────────────────────
    await save_summary(pool, summary, data_range)
    await pool.close()

    print("\n[DONE] Test O complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
