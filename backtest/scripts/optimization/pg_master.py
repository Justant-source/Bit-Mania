#!/usr/bin/env python3
"""
pg_master.py — Master orchestrator for PostgreSQL-native backtest pipeline.

Runs pg_worker.py with multiple parallel processes for a sweep.
Monitors progress, then optionally runs pg_aggregate.py.

Usage:
    python3 pg_master.py --sweep <sweep_id> [--workers 6] [--no-aggregate]
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# Add db module to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'db'))
from _common import connect


def get_progress(sweep_id: str) -> tuple[int, int]:
    """Get (completed, pending) counts for the sweep."""
    try:
        conn = connect()
        with conn.cursor() as cur:
            # Count (combo, window) pairs with window_results
            cur.execute("""
                SELECT COUNT(DISTINCT (c.pk, w.name))
                FROM st_combos c
                CROSS JOIN (
                    VALUES ('W1'), ('W2'), ('W3'), ('W4'),
                           ('W5'), ('W6'), ('W7'), ('W8')
                ) AS w(name)
                WHERE c.sweep_id = %s
                AND EXISTS (
                    SELECT 1 FROM st_window_results wr
                    WHERE wr.combo_pk = c.pk AND wr.window = w.name
                )
            """, (sweep_id,))
            completed = cur.fetchone()[0]

            # Count total (combo, window) pairs
            cur.execute("""
                SELECT COUNT(*) * 8
                FROM st_combos
                WHERE sweep_id = %s
            """, (sweep_id,))
            total_expected = cur.fetchone()[0]

        conn.close()
        pending = total_expected - completed
        return completed, pending
    except Exception as e:
        print(f'[PROGRESS ERROR] {e}', file=sys.stderr)
        return 0, 0


def run_worker(args: tuple) -> int:
    """Run pg_worker as subprocess."""
    sweep_id, worker_id = args
    result = subprocess.run(
        ['python3', '/app/scripts/optimization/pg_worker.py',
         '--sweep', sweep_id,
         '--worker-id', str(worker_id)],
        cwd='/app'
    )
    return result.returncode


def monitor_loop(sweep_id: str, total_pairs: int, t_start: float) -> None:
    """Monitor progress and print status every 30 seconds."""
    while True:
        time.sleep(30)
        completed, pending = get_progress(sweep_id)
        elapsed_min = (time.time() - t_start) / 60.0

        if completed > 0:
            rate = completed / elapsed_min
            eta_min = pending / rate if rate > 0 else float('inf')
        else:
            rate = 0.0
            eta_min = float('inf')

        print(f'  [{completed}/{total_pairs}] completed={completed} pending={pending} '
              f'elapsed={elapsed_min:.1f}m rate={rate:.2f}/min ETA={eta_min:.0f}m',
              flush=True)

        if pending == 0:
            break


def main():
    p = argparse.ArgumentParser(
        description='Master orchestrator for PostgreSQL backtest pipeline'
    )
    p.add_argument('--sweep', type=str, required=True, help='Sweep ID')
    p.add_argument('--workers', type=int, default=6, help='Number of worker processes')
    p.add_argument('--no-aggregate', action='store_true', help='Skip pg_aggregate.py')
    args = p.parse_args()

    sweep_id = args.sweep
    n_workers = args.workers

    # Calculate expected total
    try:
        conn = connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM st_combos WHERE sweep_id = %s
            """, (sweep_id,))
            n_combos = cur.fetchone()[0]
        conn.close()

        if n_combos == 0:
            print(f'ERROR: Sweep {sweep_id} has no combos. Run pg_generate_grid.py first.',
                  file=sys.stderr)
            return 1

        total_pairs = n_combos * 8  # 8 windows
    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1

    print(f'Master orchestrator for PostgreSQL backtest pipeline')
    print(f'Sweep: {sweep_id}')
    print(f'Combos: {n_combos}')
    print(f'Total (combo, window) pairs: {total_pairs}')
    print(f'Workers: {n_workers}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')

    t_start = time.time()

    # Launch workers
    tasks = [(sweep_id, i) for i in range(n_workers)]
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(run_worker, t) for t in tasks]

        # Start monitor thread
        mon = threading.Thread(
            target=monitor_loop,
            args=(sweep_id, total_pairs, t_start),
            daemon=True
        )
        mon.start()

        # Wait for all workers
        for f in as_completed(futures):
            try:
                rc = f.result()
                if rc != 0:
                    print(f'[WARN] worker exited with code {rc}', flush=True)
            except Exception as e:
                print(f'[WARN] worker exception: {e}', flush=True)

        # Wait for monitor to finish
        mon.join(timeout=5)

    elapsed_min = (time.time() - t_start) / 60.0
    completed, pending = get_progress(sweep_id)

    print(f'\nSweep complete: completed={completed} pending={pending} '
          f'elapsed={elapsed_min:.1f}m')
    print(f'End: {datetime.now(timezone.utc).isoformat()}')

    # Run aggregation if not skipped
    if not args.no_aggregate:
        print('\nRunning pg_aggregate.py...', flush=True)
        result = subprocess.run(
            ['python3', '/app/scripts/optimization/pg_aggregate.py',
             '--sweep', sweep_id],
            cwd='/app'
        )
        if result.returncode != 0:
            print('[WARN] pg_aggregate.py failed', file=sys.stderr)
            return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
