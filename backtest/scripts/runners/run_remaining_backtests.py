#!/usr/bin/env python3
"""
run_remaining_backtests.py — Re-run all non-PLATEAU supertrend variants after same-ts bug fix.

Covers 27 variants across SupertrendStrategy and SupertrendTrendTypeStrategy
(all TFs and leverage levels). Overwrites existing results.

Usage (inside backtester container):
    python3 /app/scripts/runners/run_remaining_backtests.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

STRATEGY_DIR = 'SupertrendStrategy'
START        = '2017-08-18'
END          = '2026-04-30'
BALANCE      = '10000'
FEE          = '0.00055'
PARALLELISM  = 4
TIMEOUT_S    = 1800

BASE_RESULT  = Path('/result/7-strategies')

# Must be explicit — Jesse does NOT fall back to strategy defaults when hp dict is provided.
# Passing '{}' causes KeyError on self.hp['fast_ema_len'] etc.
HP_ST = json.dumps({
    'st_factor': 3.0, 'st_period': 7,
    'fast_ema_len': 7, 'slow_ema_len': 20,
    'direction_ema_len': 200, 'atr_mult': 3.0,
})
HP_ST_V2 = json.dumps({
    'st_factor': 2.3, 'st_period': 8,
    'fast_ema_len': 10, 'slow_ema_len': 20,
    'direction_ema_len': 250, 'atr_mult': 3.0,
})
HP_STT = json.dumps({
    'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1,
    'st_factor': 3.0, 'st_period': 7,
    'fast_ema_len': 7, 'slow_ema_len': 20,
    'direction_ema_len': 200, 'atr_mult': 3.0,
})
HP_STT_V2 = json.dumps({
    'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1,
    'st_factor': 2.3, 'st_period': 8,
    'fast_ema_len': 10, 'slow_ema_len': 20,
    'direction_ema_len': 250, 'atr_mult': 3.0,
})

# (strategy_class, result_dir_prefix, tf, variant_suffix, leverage, hp_json, jesse_variant)
# jesse_variant: 'long_only' or 'bidirectional' (passed as --variant to run_intrabar_backtest.py)
JOBS = [
    # --- SupertrendStrategy / 4h ---
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only',       1, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x2',    2, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x3',    3, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_v2',    1, HP_ST_V2,  'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'long_only_x3_v2', 3, HP_ST_V2,  'long_only'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional',    1, HP_ST,     'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional_x2', 2, HP_ST,     'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '4h', 'bidirectional_x3', 3, HP_ST,     'bidirectional'),
    # --- SupertrendStrategy / 1h ---
    ('SupertrendStrategy', 'supertrend', '1h', 'long_only',        1, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '1h', 'bidirectional',    1, HP_ST,     'bidirectional'),
    # --- SupertrendStrategy / 1D ---
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only',        1, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only_x2',     2, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'long_only_x3',     3, HP_ST,     'long_only'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional',    1, HP_ST,     'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional_x2', 2, HP_ST,     'bidirectional'),
    ('SupertrendStrategy', 'supertrend', '1D', 'bidirectional_x3', 3, HP_ST,     'bidirectional'),
    # --- SupertrendTrendTypeStrategy / 4h ---
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '4h', 'long_only',       1, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '4h', 'long_only_x2',    2, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '4h', 'long_only_x3',    3, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '4h', 'long_only_x3_v2', 3, HP_STT_V2, 'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '4h', 'bidirectional',   1, HP_STT,    'bidirectional'),
    # --- SupertrendTrendTypeStrategy / 1h ---
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1h', 'long_only',        1, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1h', 'bidirectional',    1, HP_STT,    'bidirectional'),
    # --- SupertrendTrendTypeStrategy / 1D ---
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1D', 'long_only',        1, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1D', 'long_only_x2',     2, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1D', 'long_only_x3',     3, HP_STT,    'long_only'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype', '1D', 'bidirectional',    1, HP_STT,    'bidirectional'),
]


def run_one(job: tuple) -> dict:
    cls, strat_dir, tf, variant_name, leverage, hp_json, jesse_variant = job
    out_dir = BASE_RESULT / strat_dir / tf / variant_name
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f'{strat_dir}/{tf}/{variant_name}'
    t0 = time.time()

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy',  cls,
        '--timeframe', tf,
        '--variant',   jesse_variant,
        '--leverage',  str(leverage),
        '--start',     START,
        '--end',       END,
        '--balance',   BALANCE,
        '--fee',       FEE,
        '--hp-json',   hp_json,
        '--output',    str(out_dir),
    ]

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
    # Check both existence AND recency — old file left over from a failed run counts as FAIL
    ok = stats_file.exists() and (stats_file.stat().st_mtime >= t0)
    status = 'OK' if ok else 'FAIL'
    if ok:
        try:
            s = json.loads((out_dir / 'stats.json').read_text())
            cagr = s.get('cagr_pct', 0)
            mdd  = s.get('max_drawdown_pct', 0)
            print(f'[{status}] {tag}  CAGR={cagr:.1f}%  MDD={mdd:.1f}%  {elapsed:.0f}s', flush=True)
        except Exception:
            print(f'[{status}] {tag}  {elapsed:.0f}s', flush=True)
    else:
        print(f'[{status}] {tag}  {elapsed:.0f}s', flush=True)
    return {'tag': tag, 'ok': ok, 'elapsed': elapsed}


def main() -> int:
    print(f'run_remaining_backtests: {len(JOBS)} jobs, {PARALLELISM} workers', flush=True)
    print(f'Start: {START}  End: {END}  Fee: {FEE}', flush=True)
    print(flush=True)

    t_total = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futs = {pool.submit(run_one, j): j for j in JOBS}
        for fut in as_completed(futs):
            results.append(fut.result())

    print('\n─── Summary ───', flush=True)
    ok   = sum(1 for r in results if r['ok'])
    fail = len(results) - ok
    wall = time.time() - t_total
    print(f'Total: {ok} OK / {fail} FAIL  |  wall={wall:.0f}s ({wall/60:.1f}m)', flush=True)
    for r in sorted(results, key=lambda x: x['tag']):
        mark = '✓' if r['ok'] else '✗'
        print(f'  {mark} {r["tag"]}  {r["elapsed"]:.0f}s', flush=True)

    return 0 if fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
