#!/usr/bin/env python3
"""
전구간(2017-2026) Liquidation Risk + Equity Stop 분석 런처.

Step 1: run_liquidation_risk_analysis.py  → liq 위험 전수 검사 (5 combos)
Step 2: _equity_stop_worker.py × 10 workers → equity stop 성과 비교 (병렬)
Step 3: _aggregate_results.py              → 통합 리포트 생성

실행:
  DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
  $DC python3 /result/supertrend_x3_long_only/covid_crash_analysis/run_full_period_analysis.py
"""
from __future__ import annotations

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path('/result/supertrend_x3_long_only/covid_crash_analysis')
WORKERS_DIR = SCRIPT_DIR / 'workers'
WORKERS_DIR.mkdir(exist_ok=True)

# 5 combos × 4 variants = 20 runs → 10 parallel workers (2 variants each)
# 각 worker는 1 combo × 2 variants 담당
WORKER_PLAN = [
    # (worker_id, combo_id, variants)
    ('w01', 'default',   'no_stop,eq_stop_70'),
    ('w02', 'default',   'eq_stop_75,eq_stop_80'),
    ('w03', 'v2',        'no_stop,eq_stop_70'),
    ('w04', 'v2',        'eq_stop_75,eq_stop_80'),
    ('w05', 'combo_164', 'no_stop,eq_stop_70'),
    ('w06', 'combo_164', 'eq_stop_75,eq_stop_80'),
    ('w07', 'combo_173', 'no_stop,eq_stop_70'),
    ('w08', 'combo_173', 'eq_stop_75,eq_stop_80'),
    ('w09', 'combo_176', 'no_stop,eq_stop_70'),
    ('w10', 'combo_176', 'eq_stop_75,eq_stop_80'),
]
PARALLELISM = 10
TIMEOUT_S   = 3600  # 1h per worker


def run_step1():
    """Step 1: 전수 liq 위험 검사"""
    print('\n' + '='*65)
    print('Step 1: Liquidation Risk Analysis (2017-2026, 5 combos)')
    print('='*65)
    t0  = time.time()
    cmd = ['python3', str(SCRIPT_DIR / 'run_liquidation_risk_analysis.py')]
    ret = subprocess.run(cmd, check=False, timeout=TIMEOUT_S * 2)
    elapsed = time.time() - t0
    ok = ret.returncode == 0
    print(f'\nStep 1 완료: {"OK" if ok else "FAIL"}  {elapsed:.0f}s')
    return ok


def run_worker(wid: str, combo: str, variants: str) -> dict:
    out_path = WORKERS_DIR / f'{wid}.json'
    cmd = [
        'python3', str(SCRIPT_DIR / '_equity_stop_worker.py'),
        '--combo-id',  combo,
        '--variants',  variants,
        '--out',       str(out_path),
        '--worker-id', wid,
    ]
    t0 = time.time()
    print(f'[START] {wid}: {combo} / {variants}', flush=True)
    try:
        ret = subprocess.run(cmd, check=False, timeout=TIMEOUT_S)
        ok  = ret.returncode == 0 and out_path.exists()
    except subprocess.TimeoutExpired:
        print(f'[TIMEOUT] {wid}', flush=True)
        return {'wid': wid, 'ok': False}
    except Exception as e:
        print(f'[ERROR] {wid}: {e}', flush=True)
        return {'wid': wid, 'ok': False}
    elapsed = time.time() - t0
    status  = 'OK' if ok else 'FAIL'
    print(f'[{status}] {wid}: {combo} / {variants}  {elapsed:.0f}s', flush=True)
    return {'wid': wid, 'ok': ok}


def run_step2():
    """Step 2: 10 workers 병렬 실행"""
    print('\n' + '='*65)
    print(f'Step 2: Equity Stop Backtest (10 workers, {PARALLELISM} 병렬)')
    print('='*65)
    t0 = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=PARALLELISM) as exe:
        futs = {exe.submit(run_worker, wid, combo, variants): wid
                for wid, combo, variants in WORKER_PLAN}
        for fut in as_completed(futs):
            results.append(fut.result())

    elapsed = time.time() - t0
    ok_count = sum(1 for r in results if r['ok'])
    print(f'\nStep 2 완료: {ok_count}/{len(WORKER_PLAN)} 성공  {elapsed:.0f}s')
    failed = [r['wid'] for r in results if not r['ok']]
    if failed:
        print(f'실패한 worker: {failed}')
    return ok_count == len(WORKER_PLAN)


def run_step3():
    """Step 3: 집계 + 리포트"""
    print('\n' + '='*65)
    print('Step 3: 결과 집계 + 리포트 생성')
    print('='*65)
    t0  = time.time()
    cmd = ['python3', str(SCRIPT_DIR / '_aggregate_results.py')]
    ret = subprocess.run(cmd, check=False, timeout=300)
    elapsed = time.time() - t0
    ok = ret.returncode == 0
    print(f'Step 3 완료: {"OK" if ok else "FAIL"}  {elapsed:.0f}s')
    return ok


def main():
    print('='*65)
    print('전구간(2017-2026) Liquidation Risk + Equity Stop 분석')
    print('대상: default / v2 / combo_164 / combo_173 / combo_176')
    print('='*65)
    t_total = time.time()

    # Step 1: liq risk (순차)
    ok1 = run_step1()

    # Step 2: equity stop workers (병렬)
    ok2 = run_step2()

    # Step 3: aggregate (순차)
    ok3 = run_step3()

    total_elapsed = time.time() - t_total
    print(f'\n{"="*65}')
    print(f'전체 완료: Step1={ok1} Step2={ok2} Step3={ok3}  총 {total_elapsed:.0f}s')
    print(f'{"="*65}')
    return 0 if (ok1 and ok2 and ok3) else 1


if __name__ == '__main__':
    sys.exit(main())
