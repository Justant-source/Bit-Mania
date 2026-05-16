#!/usr/bin/env python3
"""
v5_2_master.py — v5_2 1,296 combo × 8 window = 10,368 job 스윕 마스터 (SQLite 큐)

v5_master.py와 동일 패턴. 그리드/경로만 v5_2로 변경.
목적: v7(1x/2x) carrier 발굴 + dir_ema=270 검증 + 3-way cross-val.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v5_2_master.py --workers 6
    python3 /app/scripts/optimization/v5_2_master.py --workers 4 --skip-post
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

DB_PATH    = '/result/v5_2_optimization/queue.sqlite3'
OUTPUT_DIR = '/result/v5_2_optimization'
TOTAL      = 10368  # 1,296 combos × 8 windows


def bootstrap_jesse_env() -> None:
    storage_dir = Path('/app/storage')
    storage_dir.mkdir(exist_ok=True)
    env_file = Path('/app/.env')
    if not env_file.exists():
        env_file.write_text(
            f"POSTGRES_HOST={os.environ.get('JESSE_DB_HOST', 'backtest-postgres')}\n"
            f"POSTGRES_NAME={os.environ.get('JESSE_DB_NAME', 'jesse_db')}\n"
            f"POSTGRES_PORT={os.environ.get('JESSE_DB_PORT', '5432')}\n"
            f"POSTGRES_USERNAME={os.environ.get('JESSE_DB_USER', 'jesse')}\n"
            f"POSTGRES_PASSWORD={os.environ.get('JESSE_DB_PASSWORD', '')}\n"
            "REDIS_HOST=localhost\nREDIS_PORT=6379\nREDIS_PASSWORD=\nREDIS_DB=0\n"
            "PASSWORD=backtest\nAPP_PORT=9000\nIS_DEV_ENV=false\nLSP_PORT=9001\n"
        )
    print('Jesse env ready.', flush=True)

    print('Pre-compiling .pyc cache...', flush=True)
    subprocess.run(
        ['python3', '-m', 'compileall', '-q', '-j4',
         '/jesse-docker/jesse/', '/app/strategies/'],
        cwd='/app', capture_output=True
    )
    print('Pre-compile done.', flush=True)


def generate_grid() -> None:
    result = subprocess.run(
        ['python3', '/app/scripts/optimization/v5_2_generate_grid.py',
         '--db', DB_PATH],
        cwd='/app', capture_output=False
    )
    if result.returncode != 0:
        print('[ERROR] v5_2_generate_grid.py failed', file=sys.stderr)
        sys.exit(1)


def get_progress(db_path: str) -> tuple[int, int, int]:
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        done = conn.execute("SELECT count(*) FROM jobs WHERE status='done'").fetchone()[0]
        err  = conn.execute("SELECT count(*) FROM jobs WHERE status='error'").fetchone()[0]
        pend = conn.execute("SELECT count(*) FROM jobs WHERE status='pending'").fetchone()[0]
        conn.close()
        return done, err, pend
    except Exception:
        return 0, 0, TOTAL


def run_worker(_args: tuple) -> int:
    db_path, output_dir = _args
    result = subprocess.run(
        ['python3', '/app/scripts/optimization/v5_2_worker.py',
         '--db', db_path,
         '--output-dir', output_dir],
        cwd='/app'
    )
    return result.returncode


def monitor_loop(t_start: float) -> None:
    while True:
        time.sleep(30)
        done, err, pend = get_progress(DB_PATH)
        elapsed = (time.time() - t_start) / 60.0
        total_done = done + err
        if total_done > 0:
            rate = total_done / elapsed
            eta  = pend / rate if rate > 0 else float('inf')
        else:
            rate = 0.0
            eta  = float('inf')
        print(f"  [{total_done}/{TOTAL}] done={done} err={err} pend={pend} "
              f"elapsed={elapsed:.1f}m rate={rate:.2f}/min ETA={eta:.0f}m",
              flush=True)
        if pend == 0:
            break


def run_post_processing() -> None:
    print('\nRunning v5_2_aggregate.py...', flush=True)
    subprocess.run(
        ['python3', '/app/scripts/optimization/v5_2_aggregate.py',
         '--db', DB_PATH,
         '--output-dir', OUTPUT_DIR],
        cwd='/app'
    )


def main():
    p = argparse.ArgumentParser(description='v5_2 gap-filling master (10,368 jobs)')
    p.add_argument('--workers', type=int, default=6)
    p.add_argument('--skip-post', action='store_true', help='Skip v5_2_aggregate.py')
    args = p.parse_args()

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    print(f'v5_2 Master — {TOTAL} jobs (1,296 combos × 8 windows), {args.workers} workers')
    print(f'DB: {DB_PATH}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')

    bootstrap_jesse_env()
    generate_grid()

    t_start = time.time()
    tasks   = [(DB_PATH, OUTPUT_DIR)] * args.workers
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_worker, t) for t in tasks]

        mon = threading.Thread(target=monitor_loop, args=(t_start,), daemon=True)
        mon.start()

        for f in as_completed(futures):
            try:
                rc = f.result()
                if rc != 0:
                    print(f'[WARN] worker exited with code {rc}', flush=True)
            except Exception as e:
                print(f'[WARN] worker exception: {e}', flush=True)

        mon.join(timeout=5)

    done, err, _ = get_progress(DB_PATH)
    elapsed = (time.time() - t_start) / 60.0
    print(f'\nSweep complete: done={done} error={err} elapsed={elapsed:.1f}m')
    print(f'End: {datetime.now(timezone.utc).isoformat()}')

    if not args.skip_post:
        run_post_processing()

    return 0


if __name__ == '__main__':
    sys.exit(main())
