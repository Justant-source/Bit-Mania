#!/usr/bin/env python3
"""Backfill supertrend_signals table from historical ohlcv_history.

Replays confirmed 4h candles through the *exact same* indicator logic as
the live strategy (imports real indicators.py), writing one row per bar.
Run once after deploying strategy.py signal-logging; skip already-existing
bars with ON CONFLICT DO NOTHING.

Usage:
    python cryptoengine/scripts/backfill_supertrend_signals.py [--dry-run]

Environment: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (or DATABASE_URL)
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import deque
from datetime import timezone

import pandas as pd
import psycopg2
import psycopg2.extras

# ── Path setup: import live indicators.py verbatim ────────────────────
_STRAT_DIR = os.path.join(
    os.path.dirname(__file__), "..", "services", "strategies", "supertrend"
)
sys.path.insert(0, _STRAT_DIR)

from indicators import compute_atr, compute_ema, compute_supertrend  # noqa: E402

# ── Combo #7908 parameters (must match strategy.py) ───────────────────
ST_FACTOR = 2.6
ST_PERIOD = 9
FAST_EMA_LEN = 7
SLOW_EMA_LEN = 29
DIR_EMA_LEN = 240
ATR_MULT = 3.3
LEVERAGE = 3
CANDLE_LOOKBACK = 300
_4H_MS = 14_400_000  # 4 hours in milliseconds

MIN_BARS = max(ST_PERIOD, DIR_EMA_LEN, SLOW_EMA_LEN) + 20


def get_db_conn() -> psycopg2.extensions.connection:
    url = os.environ.get("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "cryptoengine"),
        user=os.environ.get("DB_USER", "cryptoengine"),
        password=os.environ.get("DB_PASSWORD", "cryptoengine"),
    )


def get_allocated_capital_at(cur, bar_ts_utc) -> float:
    """Approximate allocated capital at bar_ts from orchestrator service_logs."""
    cur.execute(
        """
        SELECT (context->>'capital')::float AS capital
        FROM service_logs
        WHERE event = 'orch_capital_allocated'
          AND context->>'strategy' = 'supertrend'
          AND timestamp <= %s
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (bar_ts_utc,),
    )
    row = cur.fetchone()
    if row and row.get("capital"):
        return float(row["capital"])
    # Fallback: latest strategy_states allocation
    cur.execute(
        "SELECT allocated_capital FROM strategy_states WHERE strategy_id = 'supertrend' LIMIT 1"
    )
    row = cur.fetchone()
    if row and row.get("allocated_capital"):
        return float(row["allocated_capital"])
    return 60.0  # ranging-regime default if no data


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill supertrend_signals")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rows instead of inserting",
    )
    args = parser.parse_args()

    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("[backfill] Fetching 4h OHLCV history from DB...")
    cur.execute(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv_history
        WHERE exchange = 'bybit' AND symbol = 'BTCUSDT' AND timeframe = '4h'
        ORDER BY timestamp ASC
        """
    )
    rows = cur.fetchall()
    print(f"[backfill] {len(rows)} bars found")

    if len(rows) < MIN_BARS:
        print(f"[backfill] Not enough bars (need {MIN_BARS}), aborting")
        sys.exit(1)

    # State machine mirrors strategy.py tick() exactly
    candle_buf: deque = deque(maxlen=CANDLE_LOOKBACK)
    has_position = False
    entry_price = 0.0
    last_liquidation_ts = 0
    atr_cooldown_until = 0
    last_bar_ts = 0

    inserted = 0
    skipped = 0

    # Pre-load first CANDLE_LOOKBACK bars without inserting (warm-up)
    warm_up = min(CANDLE_LOOKBACK, len(rows))
    for row in rows[:warm_up]:
        ts_ms = int(row["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        candle_buf.append(
            {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "ts": ts_ms,
            }
        )

    print(f"[backfill] Warm-up complete ({warm_up} bars). Processing signal bars...")

    for row in rows[warm_up:]:
        ts_ms = int(row["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        candle_buf.append(
            {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "ts": ts_ms,
            }
        )
        last_bar_ts = ts_ms

        if len(candle_buf) < MIN_BARS:
            continue

        df = pd.DataFrame(list(candle_buf))
        try:
            st_dir, st_line = compute_supertrend(df, ST_PERIOD, ST_FACTOR)
            fast_ema = float(compute_ema(df, FAST_EMA_LEN).iloc[-1])
            slow_ema = float(compute_ema(df, SLOW_EMA_LEN).iloc[-1])
            dir_ema = float(compute_ema(df, DIR_EMA_LEN).iloc[-1])
            price = float(df["close"].iloc[-1])
            atr_14 = float(compute_atr(df, 14))
        except Exception as exc:
            print(f"[backfill] indicator error at {row['timestamp']}: {exc}")
            continue

        # Allocated capital approximation for historical bars
        allocated_capital = get_allocated_capital_at(cur, row["timestamp"])

        # Mirror strategy.py tick() logic exactly
        had_position = has_position
        entry_ok = False
        exit_signal = False
        exit_reason = None
        expected_action = "hold"
        expected_qty = None
        expected_stop_loss = None

        if not had_position:
            entry_ok = (
                st_dir == 1
                and fast_ema > slow_ema
                and price > dir_ema
                and last_bar_ts > last_liquidation_ts
                and last_bar_ts > atr_cooldown_until
            )
            if entry_ok:
                expected_action = "enter"
                expected_qty = (allocated_capital * 0.95 * LEVERAGE) / price
                expected_stop_loss = price * (1 - 0.70 / LEVERAGE)
                # Advance state
                has_position = True
                entry_price = price
        else:
            ema_cross_exit = fast_ema < slow_ema
            atr_stop = atr_14 * ATR_MULT
            atr_distance_exit = abs(price - entry_price) >= atr_stop
            if ema_cross_exit or atr_distance_exit:
                exit_signal = True
                exit_reason = "ema_cross" if ema_cross_exit else "atr_distance"
                expected_action = "exit"
                # Advance state
                has_position = False
                if exit_reason == "atr_distance":
                    atr_cooldown_until = last_bar_ts + _4H_MS
                last_liquidation_ts = last_bar_ts

        if args.dry_run:
            print(
                f"  {row['timestamp']}  {expected_action:5s}  "
                f"st={st_dir:+d}  f={fast_ema:.0f}  s={slow_ema:.0f}  "
                f"p={price:.0f}  alloc={allocated_capital:.1f}"
            )
            inserted += 1
            continue

        cur.execute(
            """
            INSERT INTO supertrend_signals (
                bar_ts, computed_at, st_dir, st_line, fast_ema, slow_ema, dir_ema,
                price, atr_14, allocated_capital, had_position, entry_ok,
                exit_signal, exit_reason, expected_action,
                expected_qty, expected_stop_loss
            ) VALUES (
                %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (bar_ts) DO UPDATE SET
                st_dir  = EXCLUDED.st_dir,
                st_line = EXCLUDED.st_line
            """,
            (
                row["timestamp"],
                st_dir,
                st_line,
                fast_ema,
                slow_ema,
                dir_ema,
                price,
                atr_14,
                allocated_capital,
                had_position,
                entry_ok,
                exit_signal,
                exit_reason,
                expected_action,
                expected_qty,
                expected_stop_loss,
            ),
        )
        inserted += 1

        if (inserted + skipped) % 500 == 0:
            conn.commit()
            print(f"[backfill] checkpoint: {inserted} inserted, {skipped} skipped")

    if not args.dry_run:
        conn.commit()
        print(f"[backfill] Done. upserted={inserted} (st_dir + st_line overwritten with canonical)")
    else:
        print(f"[backfill] Dry-run complete. Would insert {inserted} rows.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
