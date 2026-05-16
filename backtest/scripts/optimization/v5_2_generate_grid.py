#!/usr/bin/env python3
"""
v5_2_generate_grid.py — 1,296 combo × 8 window = 10,368 job SQLite 큐 생성

v4/v5 sparse-grid가 놓친 사이값 + dir_ema=270 확장 (v6 신호).
v7(1x/2x) carrier 발굴용 — 3x archive 결정(v6)은 불변.

v5_generate_grid.py 복제 + GRID 교체 + v6 WAL/stuck-recovery 버그픽스 적용.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v5_2_generate_grid.py \
        --db /result/v5_2_optimization/queue.sqlite3
"""
from __future__ import annotations

import argparse
import itertools
import sqlite3
from pathlib import Path

GRID = {
    'st_factor':          [2.4, 2.5, 2.6, 2.7],   # 사이값 2.4/2.6 추가, 2.3 제외
    'st_period':          [6, 7, 8, 9],            # 9 확장 (v5 미탐색)
    'fast_ema_len':       [7, 8, 9],               # 사이값 8 추가
    'slow_ema_len':       [25, 27, 30],            # 사이값 27 추가
    'direction_ema_len':  [230, 250, 270],         # 270 확장 (v6 dir_ema 신호)
    'atr_mult':           [3.0, 3.1, 3.2],         # 사이값 3.1 추가
}
SL_FIXED = 0.0  # v5와 동일 조건

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
    p.add_argument('--db', type=str, default='/result/v5_2_optimization/queue.sqlite3')
    p.add_argument('--reset', action='store_true', help='Drop and recreate (mid-sweep 비권장)')
    args = p.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")  # 워커 시작 전 WAL 확보 (v6 버그픽스)
    conn.executescript(SCHEMA)

    existing = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    if existing > 0 and not args.reset:
        # 이전 중단으로 stuck된 'running' job을 'pending'으로 복구 (v6 버그픽스)
        running = conn.execute("SELECT count(*) FROM jobs WHERE status='running'").fetchone()[0]
        if running > 0:
            conn.execute("UPDATE jobs SET status='pending', claimed_at=NULL WHERE status='running'")
            conn.commit()
            print(f"Recovered {running} stuck 'running' jobs → 'pending'")
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

    print(f"Grid: {' × '.join(str(len(v)) for v in levels)} = {n_combos} combos "
          f"× {len(WINDOWS_8)} windows = {total_expected} expected")
    print(f"Total in DB: {total} (inserted {inserted})")
    assert total == total_expected, f"Expected {total_expected} rows, got {total}"
    assert total == 10368, f"Expected 10368 rows, got {total}"
    print("Grid generation OK.")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
