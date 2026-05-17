#!/usr/bin/env python3
"""
v4_generate_grid.py — 216 combo × 8 window = 1,728 job SQLite 큐 생성

combo_18 (st=2.5/p=6/fe=7/se=20/de=200/atr=3.0, SL없음) 인근
3×3×2×2×2×3 = 216 fine-grid. sl_margin_pct=0.0 고정.
각 (combo, window) 쌍을 1 job으로 삽입 → 1,728 jobs.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v4_generate_grid.py \
        --db /result/v4_optimization/queue.sqlite3
"""
from __future__ import annotations

import argparse
import itertools
import sqlite3
from pathlib import Path

GRID = {
    'st_factor':          [2.3, 2.5, 2.7],
    'st_period':          [5, 6, 7],
    'fast_ema_len':       [7, 9],
    'slow_ema_len':       [20, 25],
    'direction_ema_len':  [200, 230],
    'atr_mult':           [2.5, 3.0, 3.5],
}
SL_FIXED = 0.0  # combo_18은 SL 없음 — 전체 그리드 동일 조건

WINDOWS_8 = [
    ('W1', '2017-08-18', '2018-09-19'),
    ('W2', '2018-09-19', '2019-10-22'),
    ('W3', '2019-10-22', '2020-11-23'),
    ('W4', '2020-11-23', '2021-12-26'),
    ('W5', '2021-12-26', '2023-01-28'),
    ('W6', '2023-01-28', '2024-03-01'),
    ('W7', '2024-03-01', '2025-04-03'),
    ('W8', '2025-04-03', '2026-04-30'),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    combo_id          INTEGER NOT NULL,
    window_name       TEXT NOT NULL,
    w_start           TEXT NOT NULL,
    w_end             TEXT NOT NULL,
    st_factor         REAL,
    st_period         INTEGER,
    fast_ema_len      INTEGER,
    slow_ema_len      INTEGER,
    direction_ema_len INTEGER,
    atr_mult          REAL,
    sl_margin_pct     REAL,
    status            TEXT DEFAULT 'pending',
    claimed_at        REAL,
    finished_at       REAL,
    cagr              REAL,
    mdd               REAL,
    sharpe            REAL,
    trades            INTEGER,
    multiplier        REAL,
    win_rate          REAL,
    error             TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_combo_window ON jobs(combo_id, window_name);
CREATE INDEX IF NOT EXISTS idx_status ON jobs(status);
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v4_optimization/queue.sqlite3')
    p.add_argument('--reset', action='store_true', help='Drop and recreate (mid-sweep 비권장)')
    args = p.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.executescript(SCHEMA)

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

    keys   = list(GRID.keys())
    levels = [GRID[k] for k in keys]
    n_combos = 1
    for lv in levels:
        n_combos *= len(lv)
    total_expected = n_combos * len(WINDOWS_8)

    inserted = 0
    for combo_id, values in enumerate(itertools.product(*levels)):
        params = dict(zip(keys, values))
        for w_name, w_start, w_end in WINDOWS_8:
            conn.execute(
                """INSERT OR IGNORE INTO jobs
                   (combo_id, window_name, w_start, w_end,
                    st_factor, st_period, fast_ema_len, slow_ema_len,
                    direction_ema_len, atr_mult, sl_margin_pct, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
                (combo_id, w_name, w_start, w_end,
                 params['st_factor'], params['st_period'], params['fast_ema_len'],
                 params['slow_ema_len'], params['direction_ema_len'], params['atr_mult'], SL_FIXED)
            )
            inserted += conn.execute("SELECT changes()").fetchone()[0]

    conn.commit()
    total = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    conn.close()

    print(f"Grid: {' × '.join(str(len(v)) for v in levels)} = {n_combos} combos × {len(WINDOWS_8)} windows = {total_expected} expected")
    print(f"Total in DB: {total} (inserted {inserted})")
    assert total == total_expected, f"Expected {total_expected} rows, got {total}"
    print("Grid generation OK.")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
