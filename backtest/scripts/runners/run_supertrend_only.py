#!/usr/bin/env python3
"""SupertrendStrategy 16개 변형만 실행 (멀티에이전트 분할용)."""
from __future__ import annotations

import json, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

START       = '2017-08-18'
END         = '2026-04-30'
BALANCE     = '10000'
FEE         = '0.00055'
PARALLELISM = 2
TIMEOUT_S   = 1800
BASE_RESULT = Path('/result/7-strategies')

HP_ST   = json.dumps({'st_factor': 3.0, 'st_period': 7, 'fast_ema_len': 7,
                      'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0})
HP_ST_V2 = json.dumps({'st_factor': 2.3, 'st_period': 8, 'fast_ema_len': 10,
                       'slow_ema_len': 20, 'direction_ema_len': 250, 'atr_mult': 3.0})

JOBS = [
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only',        1, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x2',     2, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x3',     3, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_v2',     1, HP_ST_V2, 'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x3_v2',  3, HP_ST_V2, 'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional',    1, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional_x2', 2, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional_x3', 3, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1h', 'long_only',        1, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '1h', 'bidirectional',    1, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only',        1, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only_x2',     2, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only_x3',     3, HP_ST,    'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional',    1, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional_x2', 2, HP_ST,    'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional_x3', 3, HP_ST,    'bidirectional'),
]


def run_one(job):
    cls, strat_dir, tf, variant_name, leverage, hp_json, jesse_variant = job
    out_dir = BASE_RESULT / strat_dir / tf / variant_name
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f'{strat_dir}/{tf}/{variant_name}'
    t0 = time.time()
    cmd = ['python3', '/app/scripts/runners/run_intrabar_backtest.py',
           '--strategy', cls, '--timeframe', tf, '--variant', jesse_variant,
           '--leverage', str(leverage), '--start', START, '--end', END,
           '--balance', BALANCE, '--fee', FEE, '--hp-json', hp_json,
           '--output', str(out_dir)]
    print(f'[START] {tag}', flush=True)
    try:
        subprocess.run(cmd, check=False, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        print(f'[TIMEOUT] {tag}  {elapsed:.0f}s', flush=True)
        return {'tag': tag, 'ok': False, 'elapsed': elapsed}
    except Exception as e:
        elapsed = time.time() - t0
        print(f'[ERROR] {tag}: {e}', flush=True)
        return {'tag': tag, 'ok': False, 'elapsed': elapsed}
    elapsed = time.time() - t0
    stats_file = out_dir / 'stats.json'
    ok = stats_file.exists() and stats_file.stat().st_mtime >= t0
    if ok:
        try:
            s = json.loads(stats_file.read_text())
            print(f'[OK] {tag}  CAGR={s.get("cagr_pct",0):.1f}%  MDD={s.get("max_drawdown_pct",0):.1f}%  {elapsed:.0f}s', flush=True)
        except Exception:
            print(f'[OK] {tag}  {elapsed:.0f}s', flush=True)
    else:
        print(f'[FAIL] {tag}  {elapsed:.0f}s', flush=True)
    return {'tag': tag, 'ok': ok, 'elapsed': elapsed}


def main():
    print(f'run_supertrend_only: {len(JOBS)} jobs, {PARALLELISM} workers', flush=True)
    t_total = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futs = {pool.submit(run_one, j): j for j in JOBS}
        for fut in as_completed(futs):
            results.append(fut.result())
    print('\n─── Summary ───', flush=True)
    ok = sum(1 for r in results if r['ok'])
    wall = time.time() - t_total
    print(f'Total: {ok} OK / {len(results)-ok} FAIL  |  wall={wall:.0f}s ({wall/60:.1f}m)', flush=True)
    for r in sorted(results, key=lambda x: x['tag']):
        print(f'  {"✓" if r["ok"] else "✗"} {r["tag"]}  {r["elapsed"]:.0f}s', flush=True)
    return 0 if ok == len(results) else 1


if __name__ == '__main__':
    sys.exit(main())
