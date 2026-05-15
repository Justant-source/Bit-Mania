#!/usr/bin/env python3
"""
v3_generate_grid.py — 1,620 combo fine-grid DB 생성 (sl=-25 고정)

v2 4,374 전수 탐색에서 robust winner combo_1390 인근을 3×5×4×3×3×3 = 1,620 fine-grid로 재탐색.
sl_margin_pct=-25.0 고정 (v2에서 best level 확정됨).

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v3_generate_grid.py --db /result/v3_optimization/queue.sqlite3
"""
from __future__ import annotations

import argparse
import itertools
import sqlite3
from pathlib import Path

GRID = {
    'st_factor':          [2.3, 2.4, 2.5],
    'st_period':          [5, 6, 7, 8, 9],
    'fast_ema_len':       [5, 7, 9, 11],
    'slow_ema_len':       [25, 30, 35],
    'direction_ema_len':  [200, 230, 260],
    'atr_mult':           [2.5, 3.0, 3.5],
}
SL_FIXED = -25.0  # v2 검증됨 — best level

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    combo_id         INTEGER PRIMARY KEY,
    st_factor        REAL,
    st_period        INTEGER,
    fast_ema_len     INTEGER,
    slow_ema_len     INTEGER,
    direction_ema_len INTEGER,
    atr_mult         REAL,
    sl_margin_pct    REAL,
    status           TEXT DEFAULT 'pending',
    claimed_at       REAL,
    finished_at      REAL,
    cagr             REAL,
    mdd              REAL,
    sharpe           REAL,
    trades           INTEGER,
    multiplier       REAL,
    win_rate         REAL,
    error            TEXT
);
CREATE INDEX IF NOT EXISTS idx_status ON jobs(status);
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v3_optimization/queue.sqlite3')
    p.add_argument('--reset', action='store_true', help='Drop and recreate (not recommended mid-sweep)')
    args = p.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.executescript(SCHEMA)

    # Check existing
    existing = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    if existing > 0 and not args.reset:
        pending = conn.execute("SELECT count(*) FROM jobs WHERE status='pending'").fetchone()[0]
        done    = conn.execute("SELECT count(*) FROM jobs WHERE status='done'").fetchone()[0]
        print(f"DB already has {existing} jobs (done={done}, pending={pending}). Skipping insert.")
        conn.close()
        return 0

    if args.reset:
        conn.execute("DELETE FROM jobs")
        conn.commit()

    keys = list(GRID.keys())
    levels = [GRID[k] for k in keys]
    total_expected = 1
    for lv in levels:
        total_expected *= len(lv)

    inserted = 0
    for combo_id, values in enumerate(itertools.product(*levels)):
        params = dict(zip(keys, values))
        conn.execute(
            """INSERT OR IGNORE INTO jobs
               (combo_id, st_factor, st_period, fast_ema_len, slow_ema_len,
                direction_ema_len, atr_mult, sl_margin_pct, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
            (combo_id, params['st_factor'], params['st_period'], params['fast_ema_len'],
             params['slow_ema_len'], params['direction_ema_len'], params['atr_mult'], SL_FIXED)
        )
        inserted += conn.execute("SELECT changes()").fetchone()[0]

    conn.commit()
    total = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    conn.close()

    print(f"Grid: {' × '.join(str(len(v)) for v in levels)} = {total_expected} expected")
    print(f"Total in DB: {total} (inserted {inserted})")
    assert total == total_expected, f"Expected {total_expected} rows, got {total}"
    print("Grid generation OK.")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
