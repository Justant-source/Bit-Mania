#!/usr/bin/env python3
"""
replay_with_intrabar.py — 219개 기존게이트 통과 후보 intrabar 재실행

219개 기존게이트 통과 후보를 실제 Binance 1m OHLC로 재백테스트하여
intra-bar wick 기반 MDD를 측정한다.

비교 대상:
- 원본: param_sweep/{v2|v3}/{strat}/{tf}/{variant}/combo_{i}/summary.json의 MDD
- intrabar: results/intrabar/{strat}/{tf}/{variant}/combo_{i}/{period}/stats.json의 MDD

사용 방법 (Docker내부):
    python3 /app/scripts/sweep/replay_with_intrabar.py --workers 6
    python3 /app/scripts/sweep/replay_with_intrabar.py --dry-run
    python3 /app/scripts/sweep/replay_with_intrabar.py --strategies supertrend momentum_ma

출력:
    /result/intrabar/{strat}/{tf}/{variant}/combo_{i}/{period}/stats.json
    - idempotent: 이미 존재하면 스킵
"""
import sys
import json
import os
import subprocess
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Any
import argparse

# Paths inside Docker container
SWEEP_BASE   = Path('/result/param_sweep')
INTRABAR_BASE = Path('/result/intrabar')
RUNNER       = '/app/scripts/runners/run_intrabar_backtest.py'
PYTHONPATH   = '/app:/app/scripts/runners'

# 5구간 정의 (기존 param_sweep과 동일)
PERIODS = {
    'p0': ('2018-04-01', '2020-06-30'),
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

# Gating thresholds (기존 param_sweep 기준)
GATE_MDD = -35.0
GATE_TRADES = 5


def collect_survivors() -> List[Dict[str, Any]]:
    """219개 기존게이트 통과 combo 수집"""
    survivors = []

    for ver in ('v2', 'v3'):
        base = SWEEP_BASE / ver
        if not base.exists():
            continue

        for summary_path in sorted(base.rglob('summary.json')):
            try:
                s = json.loads(summary_path.read_text())
                # score > -998 means it passed gating
                if s.get('score', -999) <= -998:
                    continue

                parts = summary_path.parts
                variant   = parts[-3]
                tf        = parts[-4]
                strat     = parts[-5]
                combo_idx = int(parts[-2].replace('combo_', ''))

                survivors.append({
                    'version': ver,
                    'strat': strat,
                    'tf': tf,
                    'variant': variant,
                    'combo': combo_idx,
                    'hp': s.get('hp', {}),
                    'summary_path': str(summary_path),
                })
            except Exception as e:
                print(f'Warning: Failed to parse {summary_path}: {e}', file=sys.stderr)

    return survivors


def get_strategy_class_name(strat: str) -> str:
    """전략 클래스 이름 반환

    Note: variant는 run_intrabar_backtest.py의 --variant 인수로 전달된다.
    """
    # Base strategy class names
    CLASS_NAMES = {
        'supertrend': 'SupertrendStrategy',
        'supertrend_trendtype': 'SupertrendTrendTypeStrategy',
        'tradeiq_psar_ha': 'TradeIQPsarHaStrategy',
        'tradeiq_cci_ce': 'TradeIQCciCeStrategy',
        'trendtype': 'TrendTypeStrategy',
        'stoch': 'StochStrategy',
        'momentum_ma': 'MomentumMAStrategy',
    }

    return CLASS_NAMES.get(strat, 'SupertrendStrategy')


def run_one_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """단일 combo × period 백테스트 실행"""
    strat   = job['strat']
    tf      = job['tf']
    variant = job['variant']
    combo   = job['combo']
    period  = job['period']
    p_start, p_end = PERIODS[period]
    hp      = job['hp']

    out_dir = INTRABAR_BASE / strat / tf / variant / f'combo_{combo}' / period
    out_dir.mkdir(parents=True, exist_ok=True)
    stats_path = out_dir / 'stats.json'

    # Idempotent check
    if stats_path.exists():
        return {
            'label': job['label'],
            'status': 'SKIP',
            'path': str(stats_path)
        }

    strategy_cls = get_strategy_class_name(strat)

    hp_json = json.dumps(job.get('hp', {}))
    cmd = [
        'python3', '-u', RUNNER,
        '--strategy', strategy_cls,
        '--timeframe', tf,
        '--variant', variant,
        '--start', p_start,
        '--end', p_end,
        '--output', str(out_dir),
        '--hp-json', hp_json,
    ]

    env = {**os.environ, 'PYTHONPATH': PYTHONPATH}

    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=600  # 10분 timeout
        )
        elapsed = time.time() - t0

        if result.returncode != 0:
            return {
                'label': job['label'],
                'status': 'FAIL',
                'returncode': result.returncode,
                'stderr': result.stderr[-500:] if result.stderr else '',
                'elapsed': elapsed
            }

        # Check if output was created
        if stats_path.exists():
            return {
                'label': job['label'],
                'status': 'OK',
                'elapsed': elapsed
            }
        else:
            # Sometimes Jesse writes different filenames
            json_files = list(out_dir.glob('*.json'))
            return {
                'label': job['label'],
                'status': 'NO_OUTPUT',
                'files_found': len(json_files),
                'elapsed': elapsed
            }
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        return {
            'label': job['label'],
            'status': 'TIMEOUT',
            'elapsed': elapsed
        }
    except Exception as e:
        elapsed = time.time() - t0
        return {
            'label': job['label'],
            'status': 'ERROR',
            'error': str(e),
            'elapsed': elapsed
        }


def main():
    parser = argparse.ArgumentParser(
        description='Replay 219 survivors with intrabar 1m OHLC'
    )
    parser.add_argument('--workers', type=int, default=6,
                        help='Number of parallel workers (default: 6)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Collect jobs but do not run them')
    parser.add_argument('--strategies', nargs='+', default=None,
                        help='Filter by strategy names (default: all)')
    parser.add_argument('--tf', nargs='+', default=None,
                        help='Filter by timeframes (default: all)')
    args = parser.parse_args()

    print('=' * 80)
    print('Intrabar Replay: 219 Survivors with Real 1m OHLC')
    print('=' * 80)

    # Collect survivors
    survivors = collect_survivors()
    print(f'\nCollected: {len(survivors)} survivors')

    if not survivors:
        print('ERROR: No survivors found')
        return 1

    # Apply filters
    filtered = survivors
    if args.strategies:
        filtered = [s for s in filtered if s['strat'] in args.strategies]
        print(f'After strategy filter: {len(filtered)}')

    if args.tf:
        filtered = [s for s in filtered if s['tf'] in args.tf]
        print(f'After timeframe filter: {len(filtered)}')

    # Create jobs: each (survivor, period) pair
    jobs = []
    for row in filtered:
        for period in PERIODS:
            label = f"{row['strat']}/{row['tf']}/{row['variant']}/combo_{row['combo']}/{period}"
            jobs.append({
                **row,
                'period': period,
                'label': label
            })

    print(f'Total jobs: {len(jobs)}')

    # Idempotent check: count already-done
    skip_count = 0
    run_jobs = []
    for job in jobs:
        out = (INTRABAR_BASE / job['strat'] / job['tf'] / job['variant'] /
               f"combo_{job['combo']}" / job['period'] / 'stats.json')
        if out.exists():
            skip_count += 1
        else:
            run_jobs.append(job)

    print(f'Idempotent check: {len(run_jobs)} to run, {skip_count} already done')

    if args.dry_run:
        print('\nDRY-RUN mode: exiting without running jobs')
        return 0

    if len(run_jobs) == 0:
        print('\nAll jobs already completed.')
        return 0

    # Parallel execution
    print(f'\nStarting parallel execution with {args.workers} workers...\n')

    ok_count = 0
    fail_count = 0
    skip_count_actual = skip_count

    t_start = time.time()

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_one_job, job): job for job in run_jobs}
        total = len(futures)
        done = 0

        for fut in as_completed(futures):
            res = fut.result()
            done += 1
            status = res.get('status', 'FAIL')
            elapsed = res.get('elapsed', 0)
            total_elapsed = time.time() - t_start

            # Print progress
            status_str = f'{status:10s}'
            if status == 'OK':
                ok_count += 1
                status_str = f'\033[92m{status:10s}\033[0m'  # Green
            elif status == 'SKIP':
                skip_count_actual += 1
                status_str = f'\033[94m{status:10s}\033[0m'  # Blue
            elif status in ('FAIL', 'ERROR', 'TIMEOUT', 'NO_OUTPUT'):
                fail_count += 1
                status_str = f'\033[91m{status:10s}\033[0m'  # Red

            print(
                f'[{done:4d}/{total}] {status_str} {res["label"]:60s} '
                f'({elapsed:6.1f}s, total {total_elapsed/60:6.1f}m)'
            )

            # Log errors
            if 'stderr' in res and res['stderr']:
                print(f'         └─ STDERR: {res["stderr"][:100]}')
            if 'error' in res:
                print(f'         └─ Error: {res["error"][:100]}')

    total_elapsed = time.time() - t_start
    print('\n' + '=' * 80)
    print(f'Completion Summary:')
    print(f'  OK:     {ok_count:4d}')
    print(f'  SKIP:   {skip_count_actual:4d}')
    print(f'  FAIL:   {fail_count:4d}')
    print(f'  Total time: {total_elapsed/60:.1f} minutes')
    print('=' * 80)

    return 0 if fail_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
