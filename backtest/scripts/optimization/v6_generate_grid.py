#!/usr/bin/env python3
"""
v6_generate_grid.py — 15 carrier × 15 TP/SL × 8 window = 1,800 job SQLite 큐 생성

v5 sweet-spot top-15 carrier에 비대칭 TP/SL grid 적용.
Carrier: /result/v5_optimization/v6_input_combos.csv 상위 15행 (sweet_score 순)
TP: [2.5, 3.0, 3.5] × SL: [1.5, 1.8, 2.0, 2.5, 3.0] = 15 조합
Sanity case: carrier v5_atr_mult=3.0 × TP=3.0/SL=3.0 → v5 symmetric 재현 (환경 검증)

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v6_generate_grid.py \
        --db /result/v6_optimization/queue.sqlite3
"""
from __future__ import annotations

import argparse
import csv
import itertools
import sqlite3
from pathlib import Path

TP_LEVELS = [2.5, 3.0, 3.5]
SL_LEVELS = [1.5, 1.8, 2.0, 2.5, 3.0]

V5_INPUT_CSV = '/result/v5_optimization/v6_input_combos.csv'

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

N_CARRIERS    = 15
N_TPSL        = len(TP_LEVELS) * len(SL_LEVELS)   # 15
TOTAL_COMBOS  = N_CARRIERS * N_TPSL               # 225
TOTAL_JOBS    = TOTAL_COMBOS * len(WINDOWS_8)      # 1,800

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    combo_id          INTEGER NOT NULL,
    carrier_id        INTEGER NOT NULL,
    v5_combo_id       INTEGER NOT NULL,
    window_name       TEXT NOT NULL,
    w_start           TEXT NOT NULL,
    w_end             TEXT NOT NULL,
    st_factor         REAL,
    st_period         INTEGER,
    fast_ema_len      INTEGER,
    slow_ema_len      INTEGER,
    direction_ema_len INTEGER,
    tp_atr_mult       REAL,
    sl_atr_mult       REAL,
    sl_margin_pct     REAL DEFAULT 0.0,
    v5_atr_mult       REAL,
    v5_mean_cagr      REAL,
    v5_worst_mdd      REAL,
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
CREATE INDEX IF NOT EXISTS idx_carrier ON jobs(carrier_id);
"""


def load_carriers(csv_path: str) -> list[dict]:
    carriers = []
    with open(csv_path, newline='') as f:
        for i, row in enumerate(csv.DictReader(f)):
            if i >= 15:
                break
            carriers.append({
                'carrier_id':        i,
                'v5_combo_id':       int(float(row['combo_id'])),
                'st_factor':         float(row['st_factor']),
                'st_period':         int(float(row['st_period'])),
                'fast_ema_len':      int(float(row['fast_ema_len'])),
                'slow_ema_len':      int(float(row['slow_ema_len'])),
                'direction_ema_len': int(float(row['direction_ema_len'])),
                'v5_atr_mult':       float(row['atr_mult']),
                'v5_mean_cagr':      float(row['mean_cagr']),
                'v5_worst_mdd':      float(row['worst_mdd']),
            })
    assert len(carriers) == 15, f"Expected 15 carriers, got {len(carriers)}"
    return carriers


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v6_optimization/queue.sqlite3')
    p.add_argument('--reset', action='store_true', help='Drop and recreate (mid-sweep 비권장)')
    args = p.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")  # 워커 시작 전 WAL 확보
    conn.executescript(SCHEMA)

    existing = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    if existing > 0 and not args.reset:
        # 이전 중단으로 stuck된 'running' job을 'pending'으로 복구
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

    carriers = load_carriers(V5_INPUT_CSV)
    print(f"Loaded {len(carriers)} carriers from {V5_INPUT_CSV}")
    for c in carriers:
        print(f"  carrier_id={c['carrier_id']} v5_combo={c['v5_combo_id']} "
              f"st={c['st_factor']}/p={c['st_period']}/fe={c['fast_ema_len']}/"
              f"se={c['slow_ema_len']}/de={c['direction_ema_len']} "
              f"v5_atr={c['v5_atr_mult']} v5_cagr={c['v5_mean_cagr']:.1f}%")

    inserted  = 0
    combo_id  = 0
    for carrier in carriers:
        for tp, sl in itertools.product(TP_LEVELS, SL_LEVELS):
            for w_name, w_start, w_end in WINDOWS_8:
                conn.execute(
                    """INSERT OR IGNORE INTO jobs
                       (combo_id, carrier_id, v5_combo_id, window_name, w_start, w_end,
                        st_factor, st_period, fast_ema_len, slow_ema_len, direction_ema_len,
                        tp_atr_mult, sl_atr_mult, sl_margin_pct,
                        v5_atr_mult, v5_mean_cagr, v5_worst_mdd, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, ?, ?, ?, 'pending')""",
                    (combo_id, carrier['carrier_id'], carrier['v5_combo_id'],
                     w_name, w_start, w_end,
                     carrier['st_factor'], carrier['st_period'],
                     carrier['fast_ema_len'], carrier['slow_ema_len'],
                     carrier['direction_ema_len'],
                     tp, sl,
                     carrier['v5_atr_mult'], carrier['v5_mean_cagr'], carrier['v5_worst_mdd'])
                )
                inserted += conn.execute("SELECT changes()").fetchone()[0]
            combo_id += 1

    conn.commit()
    total = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    conn.close()

    print(f"\nGrid: {N_CARRIERS} carriers × {N_TPSL} TP/SL = {combo_id} combos "
          f"× {len(WINDOWS_8)} windows = {TOTAL_JOBS} expected")
    print(f"Total in DB: {total} (inserted {inserted})")
    assert total == TOTAL_JOBS, f"Expected {TOTAL_JOBS} rows, got {total}"
    print("Grid generation OK.")

    sanity_carriers = [c for c in carriers if abs(c['v5_atr_mult'] - 3.0) < 0.01]
    print(f"\n=== Sanity cases (TP=3.0, SL=3.0, carrier v5_atr=3.0): {len(sanity_carriers)} carriers ===")
    for c in sanity_carriers:
        print(f"  carrier_id={c['carrier_id']} v5_combo={c['v5_combo_id']} "
              f"v5_cagr={c['v5_mean_cagr']:.1f}% v5_mdd={c['v5_worst_mdd']:.1f}%")

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
