#!/usr/bin/env python3
"""
v2_hybrid_sweep.py — Main orchestrator for Supertrend+TrendType hybrid v2.0 filter mask sweep.

Runs all 128 filter masks (0-127) as backtests, collates results into CSV, then calls report generator.

Key features:
  - Idempotent: skips masks with valid stats.json
  - Parallel: uses ProcessPoolExecutor (default 4 workers)
  - Robust: continues on individual mask failures, warns if >10% error rate
  - Collates: writes all_results.csv with bit analysis (popcount, bit names)

Usage (inside Jesse container):
    python3 v2_hybrid_sweep.py --workers 4 --output-dir /result/v2_hybrid/
    python3 v2_hybrid_sweep.py --dry-run --output-dir /result/v2_hybrid/
    python3 v2_hybrid_sweep.py --workers 1 --skip-report

Output:
    <output-dir>/mask_*/stats.json       (individual backtest results)
    <output-dir>/all_results.csv          (collated summary)
    <output-dir>/all_results.csv          (also copied to /result/v2_hybrid/ inside container)

CSV columns: mask, bits_on, bit_names, multiplier, mdd, sharpe, trades, cagr, status
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


# ── Constants ──────────────────────────────────────────────────────────────────

RESULT_DIR = Path('/result/v2_hybrid')
N_MASKS = 128

# Bit index to filter name mapping (LSB = bit 0)
BIT_NAMES = {
    0: 'F0_tt_up',
    1: 'F1_not_sideways',
    2: 'F2_adx25',
    3: 'F3_di_pos',
    4: 'F4_ema200',
    5: 'F5_atr_expand',
    6: 'F6_ema200_slope',
}


# ── Helper functions ──────────────────────────────────────────────────────────

def _mask_to_bits(mask: int) -> tuple[int, list[str]]:
    """
    Convert mask to (popcount, list of bit names).

    Example: mask=5 (binary 0b101) -> bits_on=2, bit_names=['F0_tt_up', 'F2_adx25']
    """
    bits_on = bin(mask).count('1')
    bit_names = []
    for bit_idx in range(7):  # 7 bits total
        if mask & (1 << bit_idx):
            bit_names.append(BIT_NAMES.get(bit_idx, f'F{bit_idx}'))
    return bits_on, bit_names


def _extract_multiplier(finishing_balance: float | None) -> float:
    """
    Calculate multiplier as finishing_balance / starting_balance.
    Returns 0.0 if balance is missing or zero.
    """
    if finishing_balance is None or finishing_balance == 0:
        return 0.0
    return finishing_balance / 10000.0


def _run_single_mask(args: tuple) -> dict:
    """
    Run a single mask via v2_hybrid_worker.py subprocess.
    Returns a dict with: mask, status, data (or error).
    """
    mask, output_dir, dry_run = args

    cmd = [
        'python3', '/app/scripts/sweep/v2_hybrid_worker.py',
        '--mask', str(mask),
        '--output-dir', output_dir,
    ]
    if dry_run:
        cmd.append('--dry-run')

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout per mask
            cwd='/app'
        )

        # Parse last JSON line from stdout (worker may emit backtest logs before the JSON)
        stdout = result.stdout.strip()
        if stdout:
            for line in reversed(stdout.split('\n')):
                line = line.strip()
                if line.startswith('{'):
                    try:
                        return json.loads(line)
                    except json.JSONDecodeError:
                        pass
        return {
            'mask': mask,
            'status': 'error',
            'error': 'no valid JSON line in stdout',
        }

    except subprocess.TimeoutExpired:
        return {
            'mask': mask,
            'status': 'error',
            'error': 'timeout (1h)',
        }
    except Exception as e:
        return {
            'mask': mask,
            'status': 'error',
            'error': str(e),
        }


def _collate_results(results: list[dict]) -> list[dict]:
    """
    Convert worker results into CSV rows.
    Each row contains: mask, bits_on, bit_names, multiplier, mdd, sharpe, trades, cagr, status
    """
    rows = []
    for r in results:
        mask = r.get('mask')
        status = r.get('status')

        bits_on, bit_names = _mask_to_bits(mask)
        bit_names_str = ','.join(bit_names)

        if status == 'ok':
            data = r.get('data', {})
            multiplier = _extract_multiplier(data.get('finishing_balance'))
            mdd = data.get('max_drawdown_pct', float('nan'))
            sharpe = data.get('sharpe_ratio', float('nan'))
            trades = data.get('number_of_trades', 0)
            cagr = data.get('cagr', float('nan'))
            row_status = 'ok'
        else:
            multiplier = float('nan')
            mdd = float('nan')
            sharpe = float('nan')
            trades = 0
            cagr = float('nan')
            row_status = 'error'

        rows.append({
            'mask': mask,
            'bits_on': bits_on,
            'bit_names': bit_names_str,
            'multiplier': multiplier,
            'mdd': mdd,
            'sharpe': sharpe,
            'trades': trades,
            'cagr': cagr,
            'status': row_status,
        })

    return rows


def _write_csv(output_dir: Path, rows: list[dict]) -> Path:
    """Write results to CSV. Returns path to CSV file."""
    csv_path = output_dir / 'all_results.csv'
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['mask', 'bits_on', 'bit_names', 'multiplier', 'mdd', 'sharpe', 'trades', 'cagr', 'status']
        )
        writer.writeheader()
        writer.writerows(rows)

    return csv_path


def _find_best_mask(rows: list[dict]) -> dict | None:
    """Find mask with highest multiplier (excluding errors). Returns row or None."""
    valid_rows = [r for r in rows if r['status'] == 'ok']
    if not valid_rows:
        return None
    return max(valid_rows, key=lambda r: r['multiplier'])


def _print_summary(rows: list[dict]) -> None:
    """Print summary statistics and best result."""
    total = len(rows)
    ok_count = sum(1 for r in rows if r['status'] == 'ok')
    error_count = total - ok_count

    best = _find_best_mask(rows)
    baseline = next((r for r in rows if r['mask'] == 0), None)

    print(f'\n--- v2 Hybrid Sweep Summary ---')
    print(f'  Total masks: {total}')
    print(f'  OK: {ok_count}  Error: {error_count}')
    if error_count > 0:
        error_pct = (error_count / total) * 100
        print(f'  Error rate: {error_pct:.1f}%')
        if error_pct > 10.0:
            print(f'  WARNING: >10% error rate detected')

    if best:
        print(f'\n  Best mask: {best["mask"]} (bits_on={best["bits_on"]})')
        print(f'    Multiplier: {best["multiplier"]:.4f}')
        print(f'    CAGR: {best["cagr"]:.2f}%')
        print(f'    Sharpe: {best["sharpe"]:.3f}')
        print(f'    MDD: {best["mdd"]:.2f}%')
        print(f'    Trades: {best["trades"]}')
        print(f'    Bits: {best["bit_names"]}')

    if baseline:
        print(f'\n  Baseline (mask=0):')
        print(f'    Multiplier: {baseline["multiplier"]:.4f}')
        print(f'    CAGR: {baseline["cagr"]:.2f}%')
        print(f'    Sharpe: {baseline["sharpe"]:.3f}')
        print(f'    MDD: {baseline["mdd"]:.2f}%')

        if best and baseline['status'] == 'ok':
            improvement = ((best['multiplier'] / baseline['multiplier']) - 1.0) * 100
            print(f'\n  vs. Baseline: {improvement:+.1f}%')


def _call_report_generator(csv_path: Path) -> bool:
    """Call v2_report_gen.py to generate markdown report. Returns True on success."""
    cmd = [
        'python3', '/app/scripts/sweep/v2_report_gen.py',
        '--results', str(csv_path),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, cwd='/app')
        if result.returncode == 0:
            print(f'\nReport generator completed successfully')
            return True
        else:
            print(f'\nWARNING: Report generator exited with code {result.returncode}')
            if result.stderr:
                print(f'  stderr: {result.stderr[:200]}')
            return False
    except subprocess.TimeoutExpired:
        print(f'\nWARNING: Report generator timeout (10m)')
        return False
    except Exception as e:
        print(f'\nWARNING: Report generator failed: {e}')
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Supertrend+TrendType hybrid v2.0 filter mask sweep (128 masks)'
    )
    parser.add_argument(
        '--workers', type=int, default=4,
        help='Number of parallel workers (default: 4)'
    )
    parser.add_argument(
        '--output-dir', type=str, default='/result/v2_hybrid/',
        help='Output directory for results (default: /result/v2_hybrid/)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Print commands only, do not execute'
    )
    parser.add_argument(
        '--skip-report', action='store_true',
        help='Skip report generation after sweep'
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f'v2 Hybrid Sweep — 128 masks, workers={args.workers}')
    print(f'Output directory: {output_dir}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')
    if args.dry_run:
        print(f'[DRY-RUN mode]')
    print()

    if not args.dry_run:
        # Jesse creates /app/storage/ mid-run, making is_jesse_project()=True.
        # Pre-create storage/ and .env so that ALL subprocess batches see a
        # consistent jesse project state from the very first import — preventing
        # env.py from calling os._exit(1) on an empty .env mid-sweep.
        import os as _os
        storage_dir = Path('/app/storage')
        storage_dir.mkdir(exist_ok=True)
        env_file = Path('/app/.env')
        if not env_file.exists():
            env_file.write_text(
                f"POSTGRES_HOST={_os.environ.get('JESSE_DB_HOST', 'backtest-postgres')}\n"
                f"POSTGRES_NAME={_os.environ.get('JESSE_DB_NAME', 'jesse_db')}\n"
                f"POSTGRES_PORT={_os.environ.get('JESSE_DB_PORT', '5432')}\n"
                f"POSTGRES_USERNAME={_os.environ.get('JESSE_DB_USER', 'jesse')}\n"
                f"POSTGRES_PASSWORD={_os.environ.get('JESSE_DB_PASSWORD', '')}\n"
                f"REDIS_HOST=localhost\nREDIS_PORT=6379\nREDIS_PASSWORD=\nREDIS_DB=0\n"
                f"PASSWORD=backtest\nAPP_PORT=9000\nIS_DEV_ENV=false\nLSP_PORT=9001\n"
            )
        print('Jesse project env pre-configured (/app/storage + /app/.env).', flush=True)

        # Pre-compile Jesse and strategy .pyc files to prevent circular import
        # race when workers=N subprocesses all import Jesse simultaneously.
        print('Pre-compiling Jesse .pyc cache...', flush=True)
        subprocess.run(
            ['python3', '-m', 'compileall', '-q', '-j4',
             '/jesse-docker/jesse/', '/app/strategies/'],
            cwd='/app', capture_output=True
        )
        print('Pre-compile done.', flush=True)

    # Build task list (mask, output_dir, dry_run)
    tasks = [
        (mask, str(output_dir), args.dry_run)
        for mask in range(N_MASKS)
    ]

    # Track results
    results_list = []
    t_start = time.monotonic()
    processed = 0

    # Run tasks
    if args.workers <= 1:
        # Sequential execution
        for i, task in enumerate(tasks, 1):
            mask = task[0]
            print(f'  [{i}/{N_MASKS}] mask={mask} ...', flush=True)
            result = _run_single_mask(task)
            status = result.get('status', 'unknown')
            results_list.append(result)
            processed += 1

            # Print progress with key metrics
            if status == 'ok':
                data = result.get('data', {})
                mult = _extract_multiplier(data.get('finishing_balance'))
                mdd = data.get('max_drawdown_pct', 0.0)
                trades = data.get('number_of_trades', 0)
                elapsed = time.monotonic() - t_start
                print(f'    → status=ok multiplier={mult:.4f} mdd={mdd:.2f}% trades={trades} '
                      f'elapsed={elapsed:.0f}s', flush=True)
            else:
                error = result.get('error', 'unknown error')
                elapsed = time.monotonic() - t_start
                print(f'    → status=error {error} elapsed={elapsed:.0f}s', flush=True)

    else:
        # Parallel execution
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_run_single_mask, task): task[0] for task in tasks}

            for future in as_completed(futures):
                mask = futures[future]
                processed += 1

                try:
                    result = future.result()
                    results_list.append(result)
                    status = result.get('status', 'unknown')

                    # Print progress
                    if status == 'ok':
                        data = result.get('data', {})
                        mult = _extract_multiplier(data.get('finishing_balance'))
                        mdd = data.get('max_drawdown_pct', 0.0)
                        trades = data.get('number_of_trades', 0)
                        elapsed = time.monotonic() - t_start
                        print(f'  [{processed}/{N_MASKS}] mask={mask} status=ok multiplier={mult:.4f} '
                              f'mdd={mdd:.2f}% trades={trades} elapsed={elapsed:.0f}s', flush=True)
                    else:
                        error = result.get('error', 'unknown error')
                        elapsed = time.monotonic() - t_start
                        print(f'  [{processed}/{N_MASKS}] mask={mask} status=error {error} '
                              f'elapsed={elapsed:.0f}s', flush=True)

                except Exception as e:
                    print(f'  [{processed}/{N_MASKS}] mask={mask} EXCEPTION {e}', flush=True)
                    results_list.append({'mask': mask, 'status': 'error', 'error': str(e)})

    # Collate and write results
    print(f'\nCollating results...', flush=True)
    rows = _collate_results(results_list)
    csv_path = _write_csv(output_dir, rows)
    print(f'Results written: {csv_path}')

    # Print summary
    _print_summary(rows)

    # Call report generator (unless skipped)
    if not args.skip_report:
        print(f'\nGenerating report...', flush=True)
        _call_report_generator(csv_path)

    elapsed = time.monotonic() - t_start
    print(f'\nEnd: {datetime.now(timezone.utc).isoformat()}')
    print(f'Wall time: {elapsed/60:.1f}m')


if __name__ == '__main__':
    main()
