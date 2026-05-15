#!/usr/bin/env python3
"""
v3_baseline.py — combo_1390 정확값 baseline 실행 (H-0 게이트)

combo_1390 HP (sl=-25)로 전체기간 1회 + 10 독립 윈도우 11 백테스트 실행.
v3 새 그리드에 포함되지 않으므로 별도 baseline으로 확보.
결과를 combo_1390_baseline.json으로 저장.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v3_baseline.py \
        --output-dir /result/v3_optimization
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

# combo_1390 정확 HP (v2 검증된 robust winner)
BASELINE_HP = {
    'st_factor':         2.3,
    'st_period':         8,
    'fast_ema_len':      10,   # NOT in v3 grid [5,7,9,11]
    'slow_ema_len':      20,   # NOT in v3 grid [25,30,35]
    'direction_ema_len': 250,  # NOT in v3 grid [200,230,260]
    'atr_mult':          2.5,
    'sl_margin_pct':     -25.0,
}

FULL_PERIOD = ('2017-08-18', '2026-04-30')

WINDOWS_10 = [
    ('W01', '2017-08-18', '2018-07-01'),
    ('W02', '2018-07-01', '2019-05-13'),
    ('W03', '2019-05-13', '2020-03-25'),
    ('W04', '2020-03-25', '2021-02-04'),
    ('W05', '2021-02-04', '2021-12-17'),
    ('W06', '2021-12-17', '2022-10-29'),
    ('W07', '2022-10-29', '2023-09-10'),
    ('W08', '2023-09-10', '2024-07-22'),
    ('W09', '2024-07-22', '2025-06-03'),
    ('W10', '2025-06-03', '2026-04-15'),
]

BACKTEST_TIMEOUT = 1200


def run_single(label: str, start: str, end: str, out_dir: Path) -> dict | None:
    hp_json = json.dumps(BASELINE_HP)
    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', start,
        '--end', end,
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', hp_json,
        '--output', str(out_dir),
    ]

    run_dir = Path(tempfile.mkdtemp(prefix=f'jesse_baseline_{label}_'))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        stats_path = out_dir / 'stats.json'
        if not stats_path.exists():
            print(f"  [FAIL] {label}: stats.json not found")
            return None
        with open(stats_path) as f:
            stats = json.load(f)
        raw = stats.get('raw_metrics', {})
        balance = raw.get('finishing_balance', 10000)
        return {
            'label':    label,
            'start':    start,
            'end':      end,
            'cagr':     stats.get('cagr_pct'),
            'mdd':      stats.get('max_drawdown_pct'),
            'sharpe':   stats.get('sharpe_ratio'),
            'trades':   stats.get('total_trades'),
            'multiplier': balance / 10000.0 if balance else 0.0,
            'win_rate': stats.get('win_rate_pct'),
        }
    except Exception as e:
        print(f"  [ERROR] {label}: {e}", file=sys.stderr)
        return None
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output-dir', type=str, default='/result/v3_optimization')
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("combo_1390 Baseline — HP:", json.dumps(BASELINE_HP))
    print(f"Running 11 backtests (1 full + 10 windows)")

    all_results = {}

    # 1. Full period
    t0 = time.time()
    print(f"\n[FULL] {FULL_PERIOD[0]} ~ {FULL_PERIOD[1]}")
    out_full = output_dir / 'baseline_combo1390_FULL'
    result = run_single('FULL', FULL_PERIOD[0], FULL_PERIOD[1], out_full)
    elapsed = time.time() - t0
    if result:
        print(f"  FULL: cagr={result['cagr']:.1f}% mdd={result['mdd']:.1f}% "
              f"mult={result['multiplier']:.2f}x ({elapsed:.0f}s)")
        all_results['full'] = result
    else:
        print(f"  FULL: FAILED ({elapsed:.0f}s)")
        all_results['full'] = None

    # 2. 10 windows (independent)
    window_results = []
    for w_name, w_start, w_end in WINDOWS_10:
        t0 = time.time()
        print(f"\n[{w_name}] {w_start} ~ {w_end}")
        out_w = output_dir / f'baseline_combo1390_{w_name}'
        res = run_single(w_name, w_start, w_end, out_w)
        elapsed = time.time() - t0
        if res:
            print(f"  {w_name}: cagr={res['cagr']:.1f}% mdd={res['mdd']:.1f}% "
                  f"trades={res['trades']} ({elapsed:.0f}s)")
        else:
            print(f"  {w_name}: FAILED ({elapsed:.0f}s)")
        window_results.append(res)

    all_results['windows'] = window_results
    all_results['hp'] = BASELINE_HP

    # 3. Robustness summary
    valid = [w for w in window_results if w and w.get('cagr') is not None]
    if valid:
        cagrs = [w['cagr'] for w in valid]
        n_pos = sum(1 for c in cagrs if c > 0)
        mean_cagr = sum(cagrs) / len(cagrs)
        worst_mdd = min(w['mdd'] for w in valid if w.get('mdd') is not None)
        print(f"\nBaseline summary: {n_pos}/{len(valid)} positive | "
              f"mean_cagr={mean_cagr:.1f}% | worst_mdd={worst_mdd:.1f}%")
        all_results['summary'] = {
            'n_positive': n_pos,
            'n_windows': len(valid),
            'mean_cagr_independent': mean_cagr,
            'worst_mdd_independent': worst_mdd,
        }

    # 4. Save
    out_json = output_dir / 'combo_1390_baseline.json'
    out_json.write_text(json.dumps(all_results, indent=2, default=str))
    print(f"\nBaseline saved: {out_json}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
