#!/usr/bin/env python3
"""
run_top10_backtests.py — v7_st top 10 combos 전체 기간 백테스트 실행.

sweet_spot_score 상위 10개에 대해 1x, 3x 백테스트를 4 workers로 병렬 실행.
결과: /result/7-strategies/supertrend/4h/long_only_{combo_id}/
      /result/7-strategies/supertrend/4h/long_only_x3_{combo_id}/

Usage (inside backtester container):
    python3 /app/scripts/runners/run_top10_backtests.py
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
SWEEP_ID    = 'v7_st'
TOP_N       = 10
PARALLELISM = 4
TIMEOUT_S   = 1800


def get_top10(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.combo_id, c.st_factor::float, c.st_period::int,
                   c.fast_ema_len::int, c.slow_ema_len::int,
                   c.direction_ema_len::int, c.atr_mult::float,
                   c.sweet_spot_score::float, c.mean_cagr::float,
                   c.worst_mdd_recent::float
            FROM st_combos c
            WHERE c.sweep_id = %s AND c.sweet_spot_score IS NOT NULL
            ORDER BY c.sweet_spot_score DESC
            LIMIT %s
        """, (SWEEP_ID, TOP_N))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_hp_json(combo: dict) -> str:
    return json.dumps({
        'st_factor':         combo['st_factor'],
        'st_period':         combo['st_period'],
        'fast_ema_len':      combo['fast_ema_len'],
        'slow_ema_len':      combo['slow_ema_len'],
        'direction_ema_len': combo['direction_ema_len'],
        'atr_mult':          combo['atr_mult'],
    })


def run_one(combo: dict, leverage: int, out_dir: Path) -> tuple[int, int, bool, float]:
    cid = combo['combo_id']
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    tag = f'combo={cid} {leverage}x'

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', STRATEGY,
        '--timeframe', TIMEFRAME,
        '--variant', 'long_only',
        '--leverage', str(leverage),
        '--start', START,
        '--end', END,
        '--balance', BALANCE,
        '--fee', FEE,
        '--hp-json', build_hp_json(combo),
        '--output', str(out_dir),
    ]

    print(f'[START] {tag}  sf={combo["st_factor"]} sp={combo["st_period"]} '
          f'fe={combo["fast_ema_len"]} se={combo["slow_ema_len"]} '
          f'de={combo["direction_ema_len"]} am={combo["atr_mult"]}', flush=True)
    try:
        subprocess.run(cmd, check=False, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        print(f'[TIMEOUT] {tag}', flush=True)
        return cid, leverage, False, time.time() - t0
    except Exception as e:
        print(f'[ERROR] {tag}: {e}', flush=True)
        return cid, leverage, False, time.time() - t0

    elapsed = time.time() - t0
    stats_path = out_dir / 'stats.json'
    success = stats_path.exists()
    if success:
        try:
            s = json.loads(stats_path.read_text())
            print(f'[OK] {tag}  CAGR={s.get("cagr_pct",0):.1f}%  '
                  f'MDD={s.get("max_drawdown_pct",0):.1f}%  elapsed={elapsed:.0f}s', flush=True)
        except Exception:
            print(f'[OK] {tag}  elapsed={elapsed:.0f}s', flush=True)
    else:
        print(f'[FAIL] {tag}  elapsed={elapsed:.0f}s', flush=True)
    return cid, leverage, success, elapsed


def main() -> int:
    conn = connect()
    combos = get_top10(conn)
    conn.close()

    if not combos:
        print(f'No combos found in {SWEEP_ID}', file=sys.stderr)
        return 1

    print(f'Top {len(combos)} combos from {SWEEP_ID} by sweet_spot_score:')
    for c in combos:
        print(f'  combo={c["combo_id"]:5d}  score={c["sweet_spot_score"]:.4f}  '
              f'sf={c["st_factor"]} sp={c["st_period"]} fe={c["fast_ema_len"]} '
              f'se={c["slow_ema_len"]} de={c["direction_ema_len"]} am={c["atr_mult"]}  '
              f'CAGR~{c["mean_cagr"]:.1f}%')
    print()

    BASE = Path('/result/7-strategies/supertrend/4h')
    tasks = []
    for combo in combos:
        cid = combo['combo_id']
        tasks.append((combo, 1, BASE / f'long_only_{cid}'))
        tasks.append((combo, 3, BASE / f'long_only_x3_{cid}'))

    t_total = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=PARALLELISM) as pool:
        futs = {pool.submit(run_one, combo, lev, out): (combo['combo_id'], lev)
                for combo, lev, out in tasks}
        for fut in as_completed(futs):
            results.append(fut.result())

    print('\n─── Summary ───')
    ok   = sum(1 for r in results if r[2])
    fail = len(results) - ok
    wall = time.time() - t_total
    print(f'Total: {ok} OK / {fail} FAIL  |  wall={wall:.0f}s ({wall/60:.1f}m)')
    for cid, lev, success, elapsed in sorted(results):
        print(f'  combo={cid} {lev}x  {"✓" if success else "✗"}  {elapsed:.0f}s')

    return 0 if fail == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
