#!/usr/bin/env python3
"""
V4 Multi-Timeframe Orchestrator (자동 생성 금지 — Python 전용)

76개 백테스트 (4 TF × 19 combinations) 를 단일 컨테이너에서 실행.
각 백테스트는 subprocess로 run_external_backtest.py 를 호출.
idempotent: EXECUTION_SUCCESS.marker 있으면 SKIP.
완료 후 v4_generate_report.py 자동 실행.

Usage:
    python /jesse-project/scripts/v4_run_all.py [--workers N] [--tf TF]

Options:
    --workers N  병렬 워커 수 (기본: 1 = 직렬). RAM 4GB+/워커 필요.
    --tf TF      특정 TF만 실행 (1h|2h|4h|1D). 생략 시 전체.
    --dry-run    실행 없이 job 목록만 출력.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

RESULT_DIR = Path('/result/v4')
SCRIPTS    = Path('/jesse-project/scripts')

TIMEFRAMES = ['1h', '2h', '4h', '1D']

STRATEGIES = [
    ('BBPBStrategy',               'bbpb'),
    ('BBWPStrategy',               'bbwp'),
    ('StochStrategy',              'stoch'),
    ('MomentumMAStrategy',         'momentum_ma'),
    ('SupertrendStrategy',         'supertrend'),
    ('TradeIQ220320Strategy',      'tradeiq_220320'),
    ('TrendTypeStrategy',          'trendtype'),
    ('SupertrendTrendTypeStrategy','supertrend_trendtype'),
    ('TradeIQ220323Strategy',      'tradeiq_220323'),
]

VARIANTS = ['bidirectional', 'long_only']


def _jobs(tf_filter: str | None = None) -> list[dict]:
    """Return ordered list of job dicts."""
    jobs = []
    tfs = [tf_filter] if tf_filter else TIMEFRAMES
    for tf in tfs:
        # BuyAndHold first (benchmark)
        bnh_out = RESULT_DIR / tf / 'buy_and_hold'
        jobs.append({
            'tf':     tf,
            'cls':    'BuyAndHoldStrategy',
            'dir':    'buy_and_hold',
            'var':    'buy_and_hold',
            'out':    str(bnh_out),
            'label':  f'[{tf}] buy_and_hold',
        })
        # Strategy variants
        for cls, d in STRATEGIES:
            for var in VARIANTS:
                out = RESULT_DIR / tf / d / var
                jobs.append({
                    'tf':    tf,
                    'cls':   cls,
                    'dir':   d,
                    'var':   var,
                    'out':   str(out),
                    'label': f'[{tf}] {d}/{var}',
                })
    return jobs


def _already_done(out: str) -> bool:
    return (Path(out) / 'EXECUTION_SUCCESS.marker').exists()


def _write_failure_marker(out: str, reason: str) -> None:
    p = Path(out)
    p.mkdir(parents=True, exist_ok=True)
    (p / 'EXECUTION_FAILED.marker').write_text(
        f'status: FAILED\nreason: {reason}\n'
        f'executed_at: {datetime.now(timezone.utc).isoformat()}\n'
    )


def run_one(job: dict) -> dict:
    """Run a single backtest. Returns result summary dict."""
    out = job['out']
    label = job['label']

    if _already_done(out):
        return {'label': label, 'status': 'SKIP', 'elapsed': 0}

    Path(out).mkdir(parents=True, exist_ok=True)

    cmd = [
        'python', str(SCRIPTS / 'run_external_backtest.py'),
        '--strategy', job['cls'],
        '--variant',  job['var'],
        '--balance',  '10000',
        '--leverage', '1',
        '--start',    '2020-01-01',
        '--end',      '2025-12-31',
        '--no-upsample',
        '--timeframe', job['tf'],
        '--output',   out,
    ]

    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd, timeout=900, capture_output=True, text=True
        )
        elapsed = time.monotonic() - t0
        status = 'OK' if result.returncode == 0 else 'FAIL'
        if status == 'FAIL' and not _already_done(out):
            _write_failure_marker(out, f'exit={result.returncode}')
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - t0
        status = 'TIMEOUT'
        _write_failure_marker(out, 'timeout 900s')
    except Exception as e:
        elapsed = time.monotonic() - t0
        status = 'ERROR'
        _write_failure_marker(out, str(e))

    return {'label': label, 'status': status, 'elapsed': elapsed}


def _run_serial(jobs: list[dict]) -> list[dict]:
    results = []
    total = len(jobs)
    t_global = time.monotonic()
    for i, job in enumerate(jobs, 1):
        out = job['out']
        if _already_done(out):
            print(f'  [{i}/{total}] SKIP  {job["label"]}', flush=True)
            results.append({'label': job['label'], 'status': 'SKIP', 'elapsed': 0})
            continue
        print(f'  [{i}/{total}] START {job["label"]}', flush=True)
        r = run_one(job)
        elapsed_str = f'{r["elapsed"]:.0f}s'
        elapsed_total = time.monotonic() - t_global
        print(f'  [{i}/{total}] {r["status"]:7s} {job["label"]}  ({elapsed_str}, '
              f'total {elapsed_total/60:.1f}m)', flush=True)
        results.append(r)
    return results


def _run_parallel(jobs: list[dict], workers: int) -> list[dict]:
    results_map: dict[str, dict] = {}
    total = len(jobs)
    done = 0
    t_global = time.monotonic()

    # Pre-check skips
    pending = []
    for job in jobs:
        if _already_done(job['out']):
            results_map[job['label']] = {'label': job['label'], 'status': 'SKIP', 'elapsed': 0}
            done += 1
        else:
            pending.append(job)

    print(f'  Pre-skipped: {done}/{total}. Running {len(pending)} jobs with {workers} workers.', flush=True)

    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_to_label = {pool.submit(run_one, j): j['label'] for j in pending}
        for fut in as_completed(future_to_label):
            r = fut.result()
            done += 1
            elapsed_total = time.monotonic() - t_global
            print(f'  [{done}/{total}] {r["status"]:7s} {r["label"]}  '
                  f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)
            results_map[r['label']] = r

    return [results_map[j['label']] for j in jobs]


def _print_summary(results: list[dict]) -> None:
    counts = {'OK': 0, 'SKIP': 0, 'FAIL': 0, 'TIMEOUT': 0, 'ERROR': 0}
    for r in results:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    total_elapsed = sum(r['elapsed'] for r in results)
    print('\n--- V4 Summary ---')
    for k, v in counts.items():
        if v:
            print(f'  {k}: {v}')
    print(f'  Total elapsed: {total_elapsed/60:.1f}m')

    failures = [r for r in results if r['status'] not in ('OK', 'SKIP')]
    if failures:
        print('\nFailed jobs:')
        for r in failures:
            print(f'  {r["status"]:7s} {r["label"]}')


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--workers', type=int, default=1, help='Parallel workers (default: 1=serial)')
    p.add_argument('--tf', choices=['1h', '2h', '4h', '1D'], default=None, help='Run single TF only')
    p.add_argument('--dry-run', action='store_true', help='Print jobs without running')
    args = p.parse_args()

    jobs = _jobs(args.tf)
    print(f'V4 Orchestrator: {len(jobs)} jobs, workers={args.workers}', flush=True)
    print(f'Output: {RESULT_DIR}', flush=True)
    print(f'Start: {datetime.now(timezone.utc).isoformat()}', flush=True)
    print('', flush=True)

    if args.dry_run:
        for j in jobs:
            done = '✓' if _already_done(j['out']) else ' '
            print(f'  {done} {j["label"]}')
        return

    t0 = time.monotonic()
    if args.workers <= 1:
        results = _run_serial(jobs)
    else:
        results = _run_parallel(jobs, args.workers)

    _print_summary(results)
    total_time = time.monotonic() - t0
    print(f'\nTotal wall time: {total_time/60:.1f}m', flush=True)
    print(f'End: {datetime.now(timezone.utc).isoformat()}', flush=True)

    # Auto-generate report
    print('\nGenerating V4 reports...', flush=True)
    try:
        subprocess.run(
            ['python', str(SCRIPTS / 'v4_generate_report.py')],
            timeout=300, check=True
        )
    except Exception as e:
        print(f'  [warn] Report generation failed: {e}')
        print('  Run manually: python /jesse-project/scripts/v4_generate_report.py')


if __name__ == '__main__':
    main()
