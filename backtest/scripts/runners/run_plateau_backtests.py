#!/usr/bin/env python3
"""
run_plateau_backtests.py — Run full-period (2017-2026) backtests for top 10 PLATEAU combos.

Queries the top 10 PLATEAU combos from v5_2 by compound_ratio (sweet_spot_score),
then runs 1x and 3x backtests in parallel (4 workers), storing results in
/result/7-strategies/supertrend/4h/long_only_{combo_id}/ and
/result/7-strategies/supertrend/4h/long_only_x3_{combo_id}/.

Usage (inside backtester container):
    python3 /app/scripts/runners/run_plateau_backtests.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, '/app/scripts/db')
from _common import connect

STRATEGY    = 'SupertrendStrategy'
TIMEFRAME   = '4h'
START       = '2017-08-18'
END         = '2026-04-30'
BALANCE     = '10000'
FEE         = '0.00055'
SWEEP_ID    = 'v6_st'
TOP_N       = 10
PARALLELISM = 4   # concurrent backtests
TIMEOUT_S   = 1800  # 30 min per backtest


def get_top_plateau(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.combo_id, c.st_factor::float, c.st_period::int,
                   c.fast_ema_len::int, c.slow_ema_len::int,
                   c.direction_ema_len::int, c.atr_mult::float,
                   c.sweet_spot_score::float, c.mean_cagr::float,
                   c.worst_mdd_recent::float
            FROM st_combos c
            WHERE c.sweep_id = %s
              AND c.sweet_spot_score IS NOT NULL
            ORDER BY c.sweet_spot_score DESC
            LIMIT %s
        """, (SWEEP_ID, TOP_N))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_hp_json(combo: dict) -> str:
    hp = {
        'st_factor':        combo['st_factor'],
        'st_period':        combo['st_period'],
        'fast_ema_len':     combo['fast_ema_len'],
        'slow_ema_len':     combo['slow_ema_len'],
        'direction_ema_len': combo['direction_ema_len'],
        'atr_mult':         combo['atr_mult'],
    }
    return json.dumps(hp)


def run_one(combo: dict, leverage: int, out_dir: Path) -> tuple[int, int, bool, float]:
    cid = combo['combo_id']
    out_dir.mkdir(parents=True, exist_ok=True)
    variant = 'long_only'
    t0 = time.time()

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', STRATEGY,
        '--timeframe', TIMEFRAME,
        '--variant', variant,
        '--leverage', str(leverage),
        '--start', START,
        '--end', END,
        '--balance', BALANCE,
        '--fee', FEE,
        '--hp-json', build_hp_json(combo),
        '--output', str(out_dir),
    ]

    tag = f'combo={cid} {leverage}x'
    print(f'[START] {tag}  → {out_dir.name}', flush=True)
    try:
        subprocess.run(cmd, check=False, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        print(f'[TIMEOUT] {tag}', flush=True)
        return cid, leverage, False, time.time() - t0
    except Exception as e:
        print(f'[ERROR] {tag}: {e}', flush=True)
        return cid, leverage, False, time.time() - t0

    success = (out_dir / 'stats.json').exists()
    elapsed = time.time() - t0
    status = 'OK' if success else 'FAIL'
    if success:
        try:
            s = json.loads((out_dir / 'stats.json').read_text())
            cagr = s.get('cagr_pct', 0)
            mdd  = s.get('max_drawdown_pct', 0)
            print(f'[{status}] {tag}  CAGR={cagr:.1f}%  MDD={mdd:.1f}%  elapsed={elapsed:.0f}s', flush=True)
        except Exception:
            print(f'[{status}] {tag}  elapsed={elapsed:.0f}s', flush=True)
    else:
        print(f'[{status}] {tag}  elapsed={elapsed:.0f}s', flush=True)

    return cid, leverage, success, elapsed


def main() -> int:
    conn = connect()
    combos = get_top_plateau(conn)
    conn.close()

    if not combos:
        print('No PLATEAU combos found in v5_2', file=sys.stderr)
        return 1

    print(f'Top {len(combos)} PLATEAU combos by compound_ratio:')
    for c in combos:
        compound = 10 ** c['sweet_spot_score']
        print(f'  combo={c["combo_id"]:4d}  sf={c["st_factor"]}  sp={c["st_period"]}  '
              f'fe={c["fast_ema_len"]}  se={c["slow_ema_len"]}  de={c["direction_ema_len"]}  '
              f'at={c["atr_mult"]}  compound~{compound:.0f}x  CAGR={c["mean_cagr"]:.1f}%')
    print()

    BASE = Path('/result/7-strategies/supertrend/4h')

    # Build task list: all 1x first, then all 3x
    tasks = []
    for combo in combos:
        cid = combo['combo_id']
        tasks.append((combo, 1, BASE / f'long_only_{cid}'))
    for combo in combos:
        cid = combo['combo_id']
        tasks.append((combo, 3, BASE / f'long_only_x3_{cid}'))

    t_total = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futs = {pool.submit(run_one, combo, lev, out): (combo['combo_id'], lev)
                for combo, lev, out in tasks}
        for fut in as_completed(futs):
            results.append(fut.result())

    # Summary
    print('\n─── Summary ───')
    ok = sum(1 for r in results if r[2])
    fail = len(results) - ok
    total_elapsed = time.time() - t_total
    print(f'Total: {ok} OK / {fail} FAIL  |  wall={total_elapsed:.0f}s')

    for cid, lev, success, elapsed in sorted(results):
        print(f'  combo={cid} {lev}x  {"✓" if success else "✗"}  {elapsed:.0f}s')

    return 0 if fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
