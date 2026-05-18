#!/usr/bin/env python3
"""
p1_generate_grid.py — SQLite 작업 큐 생성 (4,374 combos)

Usage:
    python3 p1_generate_grid.py [--db /path/to/queue.sqlite3] [--force]
"""
from __future__ import annotations

import argparse
import itertools
import sqlite3
import time
from pathlib import Path

# ── 그리드 정의 ──────────────────────────────────────────────────────────────
GRID = {
    'st_factor':         [2.3, 2.5, 2.8],
    'st_period':         [5, 6, 8],
    'fast_ema_len':      [5, 7, 10],
    'slow_ema_len':      [15, 20, 30],
    'direction_ema_len': [150, 200, 250],
    'atr_mult':          [2.5, 3.0, 4.0],
    'sl_margin_pct':     [0.0, -10.0, -15.0, -20.0, -25.0, -33.0],
}

CHAMPION = {
    'st_factor': 2.5,
    'st_period': 6,
    'fast_ema_len': 7,
    'slow_ema_len': 20,
    'direction_ema_len': 200,
    'atr_mult': 3.0,
}

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


def generate_combos() -> list[dict]:
    keys = list(GRID.keys())
    values = [GRID[k] for k in keys]
    combos = []
    for i, combo in enumerate(itertools.product(*values)):
        row = dict(zip(keys, combo))
        row['combo_id'] = i
        combos.append(row)
    return combos


def create_db(db_path: Path, force: bool = False) -> sqlite3.Connection:
    if force and db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def insert_combos(conn: sqlite3.Connection, combos: list[dict]) -> int:
    existing = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    if existing > 0:
        print(f"  DB already has {existing} rows — skipping insert (use --force to reset)")
        return existing

    rows = [
        (
            c['combo_id'], c['st_factor'], c['st_period'], c['fast_ema_len'],
            c['slow_ema_len'], c['direction_ema_len'], c['atr_mult'], c['sl_margin_pct'],
            'pending', None, None, None, None, None, None, None, None, None,
        )
        for c in combos
    ]
    conn.executemany(
        "INSERT INTO jobs (combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct, status, claimed_at, finished_at, "
        "cagr, mdd, sharpe, trades, multiplier, win_rate, error) VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def print_stats(conn: sqlite3.Connection, combos: list[dict]) -> None:
    n = conn.execute("SELECT count(*) FROM jobs").fetchone()[0]
    # find champion combo_id
    champ_id = None
    for c in combos:
        if (c['st_factor'] == CHAMPION['st_factor'] and
                c['st_period'] == CHAMPION['st_period'] and
                c['fast_ema_len'] == CHAMPION['fast_ema_len'] and
                c['slow_ema_len'] == CHAMPION['slow_ema_len'] and
                c['direction_ema_len'] == CHAMPION['direction_ema_len'] and
                c['atr_mult'] == CHAMPION['atr_mult'] and
                c['sl_margin_pct'] == 0.0):
            champ_id = c['combo_id']
            break

    total = 1
    for v in GRID.values():
        total *= len(v)

    print(f"\n  Grid combos: {total}  (3^6 × 6 = 4,374)")
    print(f"  DB rows:     {n}")
    print(f"  Champion combo_id: {champ_id}")
    print(f"  Champion HP: {CHAMPION} | sl=0.0")


def main():
    p = argparse.ArgumentParser(description='Generate SQLite job queue for p1 optimization')
    p.add_argument('--db', type=str,
                   default='backtest/results/p1_optimization/queue.sqlite3')
    p.add_argument('--force', action='store_true', help='Drop existing DB and rebuild')
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.is_absolute():
        repo_root = Path(__file__).parent.parent.parent.parent
        db_path = repo_root / db_path

    print(f"Generating grid → {db_path}")
    combos = generate_combos()
    print(f"  Total combos: {len(combos)}")

    conn = create_db(db_path, force=args.force)
    n = insert_combos(conn, combos)
    print_stats(conn, combos)
    conn.close()
    print(f"\nDone. DB: {db_path}")
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
