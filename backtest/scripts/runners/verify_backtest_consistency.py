#!/usr/bin/env python3
"""
verify_backtest_consistency.py — 기존 DB 결과 vs 현재 환경 재실행 결과 비교.

10개 combo+window 쌍을 선정해서 재실행한 뒤 DB 저장값과 비교.
trades_count는 exact match, cagr/mdd/finishing_balance는 0.1% 이내 허용.

Usage (inside backtester container):
    python3 /app/scripts/runners/verify_backtest_consistency.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, '/app/scripts/db')
from _common import connect

STRATEGY  = 'SupertrendStrategy'
TIMEFRAME = '4h'
VARIANT   = 'long_only'
LEVERAGE  = 3
BALANCE   = 10000.0
FEE       = 0.00055

WINDOWS = {
    'W1': ('2017-08-18', '2018-12-15'),
    'W2': ('2018-12-15', '2019-10-22'),
    'W3': ('2019-10-22', '2021-02-21'),
    'W4': ('2021-02-21', '2021-11-10'),
    'W5': ('2021-11-10', '2023-01-01'),
    'W6': ('2023-01-01', '2024-03-01'),
    'W7': ('2024-03-01', '2025-04-03'),
    'W8': ('2025-04-03', '2026-04-30'),
}

# 10 diverse test cases (combo_id, window)
TEST_CASES = [
    (0,    'W1'),   # basic, positive
    (0,    'W5'),   # basic, negative
    (50,   'W3'),   # different slow_ema, bull run
    (150,  'W7'),   # different fast_ema, negative
    (300,  'W4'),   # different fast_ema
    (500,  'W2'),   # different fast_ema=10
    (800,  'W6'),   # different st_period=7
    (1000, 'W3'),   # st_period=7 + slow_ema=29
    (1000, 'W8'),   # recent window
    (1500, 'W1'),   # st_period=8
]

CAGR_TOL   = 0.1   # %p absolute tolerance
MDD_TOL    = 0.1   # %p absolute tolerance
BAL_TOL    = 0.1   # % relative tolerance


def get_db_results(combo_ids: list[int]) -> dict:
    """Fetch stored results from DB: {(combo_id, window): row}"""
    conn = connect()
    results = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.combo_id,
                   c.st_factor::float, c.st_period::int,
                   c.fast_ema_len::int, c.slow_ema_len::int,
                   c.direction_ema_len::int, c.atr_mult::float,
                   wr.window,
                   wr.cagr_raw::float, wr.mdd_raw::float,
                   wr.trades_count::int, wr.finishing_balance::float
            FROM st_combos c
            JOIN st_window_results wr ON wr.combo_pk = c.pk
            WHERE c.sweep_id = 'v7_st'
              AND c.combo_id = ANY(%s)
              AND wr.complete = TRUE
        """, (combo_ids,))
        for row in cur.fetchall():
            (cid, sf, sp, fe, se, de, am,
             win, cagr, mdd, trades, bal) = row
            results[(cid, win)] = {
                'params': {
                    'st_factor': sf, 'st_period': sp,
                    'fast_ema_len': fe, 'slow_ema_len': se,
                    'direction_ema_len': de, 'atr_mult': am,
                },
                'cagr': cagr, 'mdd': mdd,
                'trades': trades, 'balance': bal,
            }
    conn.close()
    return results


def run_backtest(params: dict, start: str, end: str) -> dict | None:
    """Run single backtest, return stats or None on failure."""
    hp = json.dumps({
        'st_factor':        params['st_factor'],
        'st_period':        params['st_period'],
        'fast_ema_len':     params['fast_ema_len'],
        'slow_ema_len':     params['slow_ema_len'],
        'direction_ema_len': params['direction_ema_len'],
        'atr_mult':         params['atr_mult'],
    })
    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = [
            'python3', '/app/scripts/runners/run_intrabar_backtest.py',
            '--strategy', STRATEGY,
            '--timeframe', TIMEFRAME,
            '--variant', VARIANT,
            '--leverage', str(LEVERAGE),
            '--start', start,
            '--end', end,
            '--balance', str(BALANCE),
            '--fee', str(FEE),
            '--hp-json', hp,
            '--output', tmpdir,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        stats_path = Path(tmpdir) / 'stats.json'
        if not stats_path.exists():
            print(f'    [ERROR] stats.json not found. stderr:\n{result.stderr[-500:]}')
            return None
        return json.loads(stats_path.read_text())


def compare(key: str, db_val: float, new_val: float, tol: float, exact: bool = False) -> bool:
    if exact:
        ok = (db_val == new_val)
        marker = '✓' if ok else '✗'
        print(f'    {marker} {key}: DB={db_val}  NEW={new_val}  (exact)', flush=True)
        return ok
    diff = abs(db_val - new_val)
    ok = diff <= tol
    marker = '✓' if ok else '✗'
    print(f'    {marker} {key}: DB={db_val:.4f}  NEW={new_val:.4f}  diff={diff:.4f}  tol={tol}', flush=True)
    return ok


def main() -> int:
    combo_ids = list({c for c, _ in TEST_CASES})
    print(f'Fetching DB results for combo_ids: {sorted(combo_ids)}', flush=True)
    db = get_db_results(combo_ids)

    passed = 0
    failed = 0

    for i, (combo_id, window) in enumerate(TEST_CASES, 1):
        key = (combo_id, window)
        if key not in db:
            print(f'\n[{i}/10] combo={combo_id} {window}  SKIP (not in DB)')
            continue

        ref = db[key]
        start, end = WINDOWS[window]
        params = ref['params']

        print(f'\n[{i}/10] combo={combo_id} {window}  '
              f'sf={params["st_factor"]} sp={params["st_period"]} '
              f'fe={params["fast_ema_len"]} se={params["slow_ema_len"]} '
              f'de={params["direction_ema_len"]} am={params["atr_mult"]}',
              flush=True)
        print(f'   DB: CAGR={ref["cagr"]:.4f}%  MDD={ref["mdd"]:.4f}%  '
              f'trades={ref["trades"]}  balance={ref["balance"]:.2f}', flush=True)

        stats = run_backtest(params, start, end)
        if stats is None:
            print(f'   FAIL: backtest returned no stats', flush=True)
            failed += 1
            continue

        new_cagr   = stats.get('cagr_pct', 0.0)
        new_mdd    = stats.get('max_drawdown_pct', 0.0)
        new_trades = stats.get('total_trades', 0)
        print(f'   NEW: CAGR={new_cagr:.4f}%  MDD={new_mdd:.4f}%  trades={new_trades}', flush=True)

        ok_cagr   = compare('CAGR',   ref['cagr'],   new_cagr,   CAGR_TOL)
        ok_mdd    = compare('MDD',    ref['mdd'],    new_mdd,    MDD_TOL)
        ok_trades = compare('trades', ref['trades'], new_trades, 0, exact=True)

        case_ok = ok_cagr and ok_mdd and ok_trades
        if case_ok:
            print(f'   → PASS', flush=True)
            passed += 1
        else:
            print(f'   → FAIL', flush=True)
            failed += 1

    print(f'\n{"="*50}')
    print(f'결과: {passed}/10 PASS  {failed}/10 FAIL')
    if passed == 10:
        print('✓ 전체 합격 — 현재 환경 데이터 일치 확인')
    else:
        print('✗ 불합격 — 데이터 불일치 존재')
    return 0 if passed == 10 else 1


if __name__ == '__main__':
    sys.exit(main())
