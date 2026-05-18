#!/usr/bin/env python3
"""
p1_master.py — 4,374 combo 그리드 스윕 마스터 (6 workers, SQLite 큐)

1. SQLite DB 생성 (p1_generate_grid.py 호출)
2. Jesse 환경 부트스트랩 (/app/storage, /app/.env)
3. ProcessPoolExecutor(workers=6)로 p1_worker.py N개 spawn
4. 진행 상황 모니터링 (매 30초)
5. 완료 후 p2_aggregate.py + p2_pareto_plot.py 자동 호출

Usage (inside Jesse container):
    python3 /app/scripts/optimization/p1_master.py --workers 6
    python3 /app/scripts/optimization/p1_master.py --workers 4 --skip-post
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = '/result/p1_optimization/queue.sqlite3'
OUTPUT_DIR = '/result/p1_optimization'
TOTAL = 4374


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
        ['python3', '/app/scripts/optimization/p1_generate_grid.py',
         '--db', DB_PATH],
        cwd='/app', capture_output=False
    )
    if result.returncode != 0:
        print('[ERROR] p1_generate_grid.py failed', file=sys.stderr)
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
        ['python3', '/app/scripts/optimization/p1_worker.py',
         '--db', db_path,
         '--output-dir', output_dir],
        cwd='/app'
    )
    return result.returncode


def monitor_loop(t_start: float, workers: int) -> None:
    while True:
        time.sleep(30)
        done, err, pend = get_progress(DB_PATH)
        elapsed = (time.time() - t_start) / 60.0
        total_done = done + err
        if total_done > 0:
            rate = total_done / elapsed  # combos/min
            eta = pend / rate if rate > 0 else float('inf')
        else:
            eta = float('inf')
        print(f"  [{total_done}/{TOTAL}] done={done} err={err} pend={pend} "
              f"elapsed={elapsed:.1f}m ETA={eta:.0f}m",
              flush=True)
        if pend == 0:
            break


def run_post_processing() -> None:
    print('\nRunning p2_aggregate.py...', flush=True)
    subprocess.run(
        ['python3', '/app/scripts/optimization/p2_aggregate.py',
         '--db', DB_PATH,
         '--output-dir', OUTPUT_DIR],
        cwd='/app'
    )
    print('Running p2_pareto_plot.py...', flush=True)
    subprocess.run(
        ['python3', '/app/scripts/optimization/p2_pareto_plot.py',
         '--csv', f'{OUTPUT_DIR}/all_results.csv',
         '--output', f'{OUTPUT_DIR}/pareto.png'],
        cwd='/app'
    )


def main():
    p = argparse.ArgumentParser(description='p1 optimization master (4,374 combos)')
    p.add_argument('--workers', type=int, default=6)
    p.add_argument('--skip-post', action='store_true', help='Skip p2 post-processing')
    args = p.parse_args()

    print(f'p1 Master — {TOTAL} combos, {args.workers} workers')
    print(f'DB: {DB_PATH}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')

    bootstrap_jesse_env()
    generate_grid()

    t_start = time.monotonic()

    # spawn workers
    tasks = [(DB_PATH, OUTPUT_DIR)] * args.workers

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_worker, t) for t in tasks]

        # monitor in a background thread
        import threading
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
    elapsed = (time.monotonic() - t_start) / 60.0
    print(f'\nSweep complete: done={done} error={err} elapsed={elapsed:.1f}m')
    print(f'End: {datetime.now(timezone.utc).isoformat()}')

    if not args.skip_post:
        run_post_processing()

    return 0


if __name__ == '__main__':
    sys.exit(main())
