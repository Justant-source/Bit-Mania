#!/usr/bin/env python3
"""
p1_worker.py — SQLite claim-and-run 워커 (단일 프로세스, p1_master.py가 N개 spawn)

SQLite에서 pending job을 atomic claim하고, run_intrabar_backtest.py를 subprocess로 실행,
결과를 DB에 기록한다. 더 이상 pending이 없으면 종료.

Usage (p1_master.py 내부에서 호출):
    python3 /app/scripts/optimization/p1_worker.py \
        --db /result/p1_optimization/queue.sqlite3 \
        --output-dir /result/p1_optimization
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

DB_TIMEOUT = 30  # SQLite busy timeout (seconds)
BACKTEST_TIMEOUT = 1200  # 20 min per combo


def claim_job(conn: sqlite3.Connection) -> dict | None:
    # BEGIN IMMEDIATE prevents two workers from claiming the same job
    # (avoids RETURNING which requires SQLite >= 3.35)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
            "direction_ema_len, atr_mult, sl_margin_pct FROM jobs "
            "WHERE status='pending' ORDER BY combo_id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return None
        combo_id = row[0]
        conn.execute(
            "UPDATE jobs SET status='running', claimed_at=? WHERE combo_id=?",
            (time.time(), combo_id)
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    keys = ['combo_id', 'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
            'direction_ema_len', 'atr_mult', 'sl_margin_pct']
    return dict(zip(keys, row))


def build_hp_json(job: dict) -> str:
    hp = {k: job[k] for k in ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
                               'direction_ema_len', 'atr_mult', 'sl_margin_pct']}
    return json.dumps(hp)


def run_backtest(job: dict, output_dir: Path) -> bool:
    combo_id = job['combo_id']
    combo_out = output_dir / f"combo_{combo_id}"
    hp_json = build_hp_json(job)

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', '2017-08-18',
        '--end', '2026-04-30',
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', hp_json,
        '--output', str(combo_out),
    ]

    # Isolated CWD to prevent is_jesse_project() from triggering Redis connection
    run_dir = Path(tempfile.mkdtemp(prefix=f'jesse_combo{combo_id}_'))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        return (combo_out / 'stats.json').exists()
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT combo={combo_id}]", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[ERROR combo={combo_id}] {e}", file=sys.stderr)
        return False
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def parse_stats(output_dir: Path, combo_id: int) -> dict | None:
    stats_path = output_dir / f"combo_{combo_id}" / 'stats.json'
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
        print(f"[PARSE ERROR combo={combo_id}] {e}", file=sys.stderr)
        return None


def mark_done(conn: sqlite3.Connection, combo_id: int, stats: dict) -> None:
    conn.execute("BEGIN")
    conn.execute(
        """UPDATE jobs SET status='done', finished_at=?,
           cagr=?, mdd=?, sharpe=?, trades=?, multiplier=?, win_rate=?
           WHERE combo_id=?""",
        (time.time(), stats.get('cagr'), stats.get('mdd'), stats.get('sharpe'),
         stats.get('trades'), stats.get('multiplier'), stats.get('win_rate'), combo_id)
    )
    conn.execute("COMMIT")


def mark_error(conn: sqlite3.Connection, combo_id: int, error: str) -> None:
    conn.execute("BEGIN")
    conn.execute(
        "UPDATE jobs SET status='error', finished_at=?, error=? WHERE combo_id=?",
        (time.time(), error[:500], combo_id)
    )
    conn.execute("COMMIT")


def main():
    p = argparse.ArgumentParser(description='p1 optimization worker')
    p.add_argument('--db', type=str, required=True)
    p.add_argument('--output-dir', type=str, required=True)
    args = p.parse_args()

    db_path = args.db
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # isolation_level=None → autocommit; all transactions are explicit
    conn = sqlite3.connect(db_path, timeout=DB_TIMEOUT, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")

    processed = 0
    while True:
        job = claim_job(conn)
        if job is None:
            break  # no more pending jobs

        combo_id = job['combo_id']
        t0 = time.time()

        success = run_backtest(job, output_dir)
        if success:
            stats = parse_stats(output_dir, combo_id)
            if stats:
                mark_done(conn, combo_id, stats)
                elapsed = time.time() - t0
                print(f"[combo={combo_id}] done cagr={stats['cagr']:.1f}% "
                      f"mdd={stats['mdd']:.1f}% trades={stats['trades']} "
                      f"mult={stats['multiplier']:.3f}x elapsed={elapsed:.0f}s",
                      flush=True)
            else:
                mark_error(conn, combo_id, 'stats parse failed')
                print(f"[combo={combo_id}] error: stats parse failed", flush=True)
        else:
            mark_error(conn, combo_id, 'backtest execution failed')
            print(f"[combo={combo_id}] error: backtest failed", flush=True)

        processed += 1

    conn.close()
    print(f"Worker done. Processed {processed} jobs.", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
