#!/usr/bin/env python3
"""
pg_worker.py — Claim and run pending combo×window backtests from PostgreSQL.

Worker loops: claim one pending (combo, window) pair using FOR UPDATE SKIP LOCKED,
run backtest, insert result, repeat until no more pending work.

Usage (called by pg_master.py, or directly):
    python3 pg_worker.py --sweep <sweep_id> [--worker-id 0]
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Add db module to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'db'))
from _common import connect


WINDOWS_DICT = {
    'W1': ('2017-08-18', '2018-12-15'),
    'W2': ('2018-12-15', '2019-10-22'),
    'W3': ('2019-10-22', '2021-02-21'),
    'W4': ('2021-02-21', '2021-11-10'),
    'W5': ('2021-11-10', '2023-01-01'),
    'W6': ('2023-01-01', '2024-03-01'),
    'W7': ('2024-03-01', '2025-04-03'),
    'W8': ('2025-04-03', '2026-04-30'),
}

BACKTEST_TIMEOUT = 600  # 10 minutes per window
STRATEGY = 'SupertrendStrategyWithSL'
TIMEFRAME = '4h'
VARIANT = 'long_only'
LEVERAGE = 3
STARTING_BALANCE = 10000
FEE = 0.00055


def claim_pending_job(conn, sweep_id: str, worker_id: int) -> dict | None:
    """
    Claim one pending (combo, window) pair atomically.

    Selects a pending pair (no row in st_window_results) and immediately
    inserts a placeholder row (complete=NULL) within the same transaction.
    This prevents other workers from double-claiming the same pair between
    SELECT and backtest completion.

    Returns job dict or None if no pending work.
    """
    for _ in range(10):
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")

                cur.execute("""
                    SELECT c.pk, c.combo_id, c.st_factor, c.st_period, c.fast_ema_len,
                           c.slow_ema_len, c.direction_ema_len, c.atr_mult,
                           w.name as window_name, w.start_date, w.end_date
                    FROM (VALUES ('W1','2017-08-18','2018-12-15'),
                                ('W2','2018-12-15','2019-10-22'),
                                ('W3','2019-10-22','2021-02-21'),
                                ('W4','2021-02-21','2021-11-10'),
                                ('W5','2021-11-10','2023-01-01'),
                                ('W6','2023-01-01','2024-03-01'),
                                ('W7','2024-03-01','2025-04-03'),
                                ('W8','2025-04-03','2026-04-30')) AS w(name,start_date,end_date)
                    JOIN st_combos c ON c.sweep_id = %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM st_window_results wr
                        WHERE wr.combo_pk = c.pk AND wr."window" = w.name
                    )
                    ORDER BY c.combo_id, w.name
                    LIMIT 1
                    FOR UPDATE OF c SKIP LOCKED
                """, (sweep_id,))

                row = cur.fetchone()
                if row is None:
                    cur.execute("COMMIT")
                    return None

                pk, combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, \
                    direction_ema_len, atr_mult, window_name, start_date, end_date = row

                # Insert placeholder atomically — if another worker raced and already inserted,
                # RETURNING returns nothing; we rollback and try the next available pair.
                cur.execute("""
                    INSERT INTO st_window_results(combo_pk, "window", complete)
                    VALUES (%s, %s, NULL)
                    ON CONFLICT(combo_pk, "window") DO NOTHING
                    RETURNING pk
                """, (pk, window_name))

                if cur.fetchone() is None:
                    # Race: another worker claimed this pair first
                    cur.execute("ROLLBACK")
                    continue

                cur.execute("COMMIT")

                return {
                    'combo_pk': pk,
                    'combo_id': combo_id,
                    'window_name': window_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'st_factor': st_factor,
                    'st_period': st_period,
                    'fast_ema_len': fast_ema_len,
                    'slow_ema_len': slow_ema_len,
                    'direction_ema_len': direction_ema_len,
                    'atr_mult': atr_mult,
                }

        except Exception as e:
            print(f'[CLAIM ERROR worker={worker_id}] {e}', file=sys.stderr, flush=True)
            try:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK")
            except Exception:
                pass
            return None

    return None


def build_hp_json(job: dict) -> str:
    """Build hyperparameter JSON for backtest."""
    hp = {
        'st_factor': float(job['st_factor']),
        'st_period': int(job['st_period']),
        'fast_ema_len': int(job['fast_ema_len']),
        'slow_ema_len': int(job['slow_ema_len']),
        'direction_ema_len': int(job['direction_ema_len']),
        'atr_mult': float(job['atr_mult']),
        'sl_margin_pct': 0.0,
    }
    return json.dumps(hp)


def run_backtest(job: dict, output_dir: Path) -> bool:
    """Run backtest subprocess, return True if successful."""
    combo_id = job['combo_id']
    window_name = job['window_name']
    combo_out = output_dir / f"combo_{combo_id}_{window_name}"
    combo_out.mkdir(parents=True, exist_ok=True)

    hp_json = build_hp_json(job)

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', STRATEGY,
        '--timeframe', TIMEFRAME,
        '--variant', VARIANT,
        '--leverage', str(LEVERAGE),
        '--start', job['start_date'],
        '--end', job['end_date'],
        '--balance', str(STARTING_BALANCE),
        '--fee', str(FEE),
        '--hp-json', hp_json,
        '--output', str(combo_out),
    ]

    # Create temp dir with strategies symlink
    run_dir = Path(tempfile.mkdtemp(prefix=f'pg_bt_{combo_id}_{window_name}_'))
    try:
        (run_dir / 'strategies').symlink_to('/app/strategies')
    except Exception as e:
        print(f'[ERROR combo={combo_id} {window_name}] symlink failed: {e}',
              file=sys.stderr, flush=True)
        shutil.rmtree(run_dir, ignore_errors=True)
        return False

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        return (combo_out / 'stats.json').exists()
    except subprocess.TimeoutExpired:
        print(f'[TIMEOUT combo={combo_id} {window_name}]',
              file=sys.stderr, flush=True)
        return False
    except Exception as e:
        print(f'[ERROR combo={combo_id} {window_name}] {e}',
              file=sys.stderr, flush=True)
        return False
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def parse_stats(output_dir: Path, combo_id: int, window_name: str) -> dict | None:
    """Parse stats.json from backtest output."""
    stats_path = output_dir / f"combo_{combo_id}_{window_name}" / 'stats.json'
    if not stats_path.exists():
        return None

    try:
        with open(stats_path) as f:
            stats = json.load(f)

        raw = stats.get('raw_metrics', {})
        balance = raw.get('finishing_balance', STARTING_BALANCE)

        return {
            'cagr_pct': stats.get('cagr_pct'),
            'max_drawdown_pct': stats.get('max_drawdown_pct'),
            'sharpe_ratio': stats.get('sharpe_ratio'),
            'total_trades': stats.get('total_trades', 0),
            'finishing_balance': balance,
        }
    except Exception as e:
        print(f'[PARSE ERROR combo={combo_id} {window_name}] {e}',
              file=sys.stderr, flush=True)
        return None


def insert_window_result(conn, job: dict, stats: dict | None, success: bool) -> None:
    """Update placeholder row in st_window_results with actual backtest result."""
    combo_pk = job['combo_pk']
    window_name = job['window_name']

    try:
        with conn.cursor() as cur:
            if success and stats:
                cur.execute("""
                    INSERT INTO st_window_results(
                      combo_pk, "window", complete, cagr_raw, mdd_raw, cagr_adj, mdd_adj,
                      sharpe, trades_count, liquidated, finishing_balance
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(combo_pk, "window") DO UPDATE SET
                      complete=EXCLUDED.complete,
                      cagr_raw=EXCLUDED.cagr_raw,
                      mdd_raw=EXCLUDED.mdd_raw,
                      cagr_adj=EXCLUDED.cagr_adj,
                      mdd_adj=EXCLUDED.mdd_adj,
                      sharpe=EXCLUDED.sharpe,
                      trades_count=EXCLUDED.trades_count,
                      liquidated=EXCLUDED.liquidated,
                      finishing_balance=EXCLUDED.finishing_balance
                    WHERE st_window_results.complete IS NULL
                """, (
                    combo_pk,
                    window_name,
                    True,
                    stats.get('cagr_pct'),
                    stats.get('max_drawdown_pct'),
                    stats.get('cagr_pct'),
                    stats.get('max_drawdown_pct'),
                    stats.get('sharpe_ratio'),
                    stats.get('total_trades'),
                    False,
                    stats.get('finishing_balance'),
                ))
            else:
                cur.execute("""
                    INSERT INTO st_window_results(
                      combo_pk, "window", complete
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT(combo_pk, "window") DO UPDATE SET
                      complete=EXCLUDED.complete
                    WHERE st_window_results.complete IS NULL
                """, (combo_pk, window_name, False))
        conn.commit()
    except Exception as e:
        print(f'[INSERT ERROR combo={job["combo_id"]} {window_name}] {e}',
              file=sys.stderr, flush=True)


def main():
    p = argparse.ArgumentParser(description='PG worker for combo×window backtests')
    p.add_argument('--sweep', type=str, required=True, help='Sweep ID')
    p.add_argument('--worker-id', type=int, default=0, help='Worker ID (for logging)')
    args = p.parse_args()

    sweep_id = args.sweep
    worker_id = args.worker_id

    output_dir = Path('/result')
    output_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    t_start = time.time()

    while True:
        # Fresh connection for each claim
        conn = connect()
        conn.autocommit = False

        job = claim_pending_job(conn, sweep_id, worker_id)
        if job is None:
            conn.close()
            break

        combo_id = job['combo_id']
        window_name = job['window_name']
        job_start = time.time()

        # Run backtest
        success = run_backtest(job, output_dir)

        # Parse stats
        stats = None
        if success:
            stats = parse_stats(output_dir, combo_id, window_name)
            if not stats:
                success = False

        # Insert result
        insert_window_result(conn, job, stats, success)
        conn.close()

        elapsed = time.time() - job_start
        if stats:
            print(f'[w{worker_id} c={combo_id} {window_name}] done '
                  f'cagr={stats.get("cagr_pct", 0):.1f}% '
                  f'mdd={stats.get("max_drawdown_pct", 0):.1f}% '
                  f'trades={stats.get("total_trades", 0)} '
                  f'elapsed={elapsed:.0f}s', flush=True)
        else:
            print(f'[w{worker_id} c={combo_id} {window_name}] error elapsed={elapsed:.0f}s',
                  flush=True)

        processed += 1

    total_elapsed = time.time() - t_start
    print(f'Worker {worker_id} done. Processed {processed} jobs in {total_elapsed:.0f}s.',
          flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
