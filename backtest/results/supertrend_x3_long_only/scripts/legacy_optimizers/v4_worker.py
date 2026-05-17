#!/usr/bin/env python3
"""
v4_worker.py — v4 216×8 = 1,728 (combo,window) SQLite claim-and-run 워커

v3_worker.py와 동일한 패턴. job 단위가 (combo_id, window_name) 쌍으로 변경.
sl_margin_pct=0.0 고정 (combo_18 SL 없음 조건).

Usage (v4_master.py 내부에서 호출):
    python3 /app/scripts/optimization/v4_worker.py \
        --db /result/v4_optimization/queue.sqlite3 \
        --output-dir /result/v4_optimization
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DB_TIMEOUT       = 30
BACKTEST_TIMEOUT = 600  # 10 min per window (단일 윈도우 ~1년)


def claim_job(conn: sqlite3.Connection) -> dict | None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT job_id, combo_id, window_name, w_start, w_end, "
            "st_factor, st_period, fast_ema_len, slow_ema_len, "
            "direction_ema_len, atr_mult, sl_margin_pct FROM jobs "
            "WHERE status='pending' ORDER BY job_id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return None
        job_id = row[0]
        conn.execute(
            "UPDATE jobs SET status='running', claimed_at=? WHERE job_id=?",
            (time.time(), job_id)
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    keys = ['job_id', 'combo_id', 'window_name', 'w_start', 'w_end',
            'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
            'direction_ema_len', 'atr_mult', 'sl_margin_pct']
    return dict(zip(keys, row))


def build_hp_json(job: dict) -> str:
    hp = {k: job[k] for k in ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
                               'direction_ema_len', 'atr_mult', 'sl_margin_pct']}
    return json.dumps(hp)


def run_backtest(job: dict, output_dir: Path) -> bool:
    combo_id = job['combo_id']
    w_name   = job['window_name']
    combo_out = output_dir / f"combo_{combo_id}_{w_name}"
    hp_json = build_hp_json(job)

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', job['w_start'],
        '--end', job['w_end'],
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', hp_json,
        '--output', str(combo_out),
    ]

    run_dir = Path(tempfile.mkdtemp(prefix=f'jesse_v4c{combo_id}{w_name}_'))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        return (combo_out / 'stats.json').exists()
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT combo={combo_id} {w_name}]", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[ERROR combo={combo_id} {w_name}] {e}", file=sys.stderr)
        return False
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def parse_stats(output_dir: Path, combo_id: int, w_name: str) -> dict | None:
    stats_path = output_dir / f"combo_{combo_id}_{w_name}" / 'stats.json'
    if not stats_path.exists():
        return None
    try:
        with open(stats_path) as f:
            stats = json.load(f)
        raw = stats.get('raw_metrics', {})
        balance = raw.get('finishing_balance', stats.get('starting_balance', 10000))
        return {
            'cagr':       stats.get('cagr_pct'),
            'mdd':        stats.get('max_drawdown_pct'),
            'sharpe':     stats.get('sharpe_ratio'),
            'trades':     stats.get('total_trades'),
            'multiplier': balance / 10000.0 if balance else 0.0,
            'win_rate':   stats.get('win_rate_pct'),
        }
    except Exception as e:
        print(f"[PARSE ERROR combo={combo_id} {w_name}] {e}", file=sys.stderr)
        return None


def mark_done(conn: sqlite3.Connection, job_id: int, stats: dict) -> None:
    conn.execute("BEGIN")
    conn.execute(
        """UPDATE jobs SET status='done', finished_at=?,
           cagr=?, mdd=?, sharpe=?, trades=?, multiplier=?, win_rate=?
           WHERE job_id=?""",
        (time.time(), stats.get('cagr'), stats.get('mdd'), stats.get('sharpe'),
         stats.get('trades'), stats.get('multiplier'), stats.get('win_rate'), job_id)
    )
    conn.execute("COMMIT")


def mark_error(conn: sqlite3.Connection, job_id: int, error: str) -> None:
    conn.execute("BEGIN")
    conn.execute(
        "UPDATE jobs SET status='error', finished_at=?, error=? WHERE job_id=?",
        (time.time(), error[:500], job_id)
    )
    conn.execute("COMMIT")


def main():
    p = argparse.ArgumentParser(description='v4 (combo,window) worker')
    p.add_argument('--db', type=str, required=True)
    p.add_argument('--output-dir', type=str, required=True)
    args = p.parse_args()

    db_path = args.db
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=DB_TIMEOUT, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")

    processed = 0
    while True:
        job = claim_job(conn)
        if job is None:
            break

        job_id   = job['job_id']
        combo_id = job['combo_id']
        w_name   = job['window_name']
        t0 = time.time()

        success = run_backtest(job, output_dir)
        if success:
            stats = parse_stats(output_dir, combo_id, w_name)
            if stats:
                mark_done(conn, job_id, stats)
                elapsed = time.time() - t0
                print(f"[c={combo_id} {w_name}] done cagr={stats['cagr']:.1f}% "
                      f"mdd={stats['mdd']:.1f}% trades={stats['trades']} "
                      f"elapsed={elapsed:.0f}s", flush=True)
            else:
                mark_error(conn, job_id, 'stats parse failed')
                print(f"[c={combo_id} {w_name}] error: stats parse failed", flush=True)
        else:
            mark_error(conn, job_id, 'backtest failed')
            print(f"[c={combo_id} {w_name}] error: backtest failed", flush=True)

        processed += 1

    conn.close()
    print(f"Worker done. Processed {processed} jobs.", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
