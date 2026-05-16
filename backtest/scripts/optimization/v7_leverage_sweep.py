#!/usr/bin/env python3
"""
v7_leverage_sweep.py — v5_2 top carrier × leverage 1x/2x/2.5x/3x 빠른 검증

목적: v5_2 PLATEAU top-3 carrier에서 3x 미만 레버리지가 실거래 적합한 MDD를 달성하는지 확인.
입력: /result/v5_2_optimization/v7_input_combos.csv (top N by sweet_spot_score)
출력: /result/v7_leverage_test/{v7_results.csv, 22_V7_LEVERAGE_VERDICT.md}

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v7_leverage_sweep.py
    python3 /app/scripts/optimization/v7_leverage_sweep.py --top 5 --leverages 1,2,2.5,3
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

V7_INPUT_CSV = Path('/result/v5_2_optimization/v7_input_combos.csv')
OUTPUT_DIR   = Path('/result/v7_leverage_test')

PERIODS = [
    ('full',   '2017-08-18', '2026-04-30'),
    ('recent', '2024-03-01', '2026-04-30'),  # W7+W8
]

BACKTEST_TIMEOUT = 600


def load_top_carriers(csv_path: Path, top_n: int) -> list[dict]:
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    # already sorted by sweet_spot_score desc (v5_2_aggregate output)
    return rows[:top_n]


def run_backtest(carrier: dict, leverage: float, period_name: str,
                 start: str, end: str, output_dir: Path) -> dict | None:
    combo_id = carrier['combo_id']
    lev_str  = str(leverage).replace('.', 'p')
    tag      = f"c{combo_id}_lev{lev_str}_{period_name}"
    out_dir  = output_dir / tag

    hp = {
        'st_factor':         float(carrier['st_factor']),
        'st_period':         int(carrier['st_period']),
        'fast_ema_len':      int(carrier['fast_ema_len']),
        'slow_ema_len':      int(carrier['slow_ema_len']),
        'direction_ema_len': int(carrier['direction_ema_len']),
        'atr_mult':          float(carrier['atr_mult']),
        'sl_margin_pct':     0.0,
    }

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', str(leverage),
        '--start', start,
        '--end', end,
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', json.dumps(hp),
        '--output', str(out_dir),
    ]

    run_dir = Path(tempfile.mkdtemp(prefix=f'jesse_v7_{tag}_'))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    t0 = time.time()
    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT {tag}]", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[ERROR {tag}] {e}", file=sys.stderr)
        return None
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)

    stats_path = out_dir / 'stats.json'
    if not stats_path.exists():
        print(f"[MISSING {tag}] stats.json not found", file=sys.stderr)
        return None

    with open(stats_path) as f:
        stats = json.load(f)
    raw = stats.get('raw_metrics', {})
    balance = raw.get('finishing_balance', stats.get('starting_balance', 10000))
    elapsed = time.time() - t0

    result = {
        'combo_id':     combo_id,
        'leverage':     leverage,
        'period':       period_name,
        'st_factor':    carrier['st_factor'],
        'st_period':    carrier['st_period'],
        'fast_ema_len': carrier['fast_ema_len'],
        'slow_ema_len': carrier['slow_ema_len'],
        'dir_ema_len':  carrier['direction_ema_len'],
        'atr_mult':     carrier['atr_mult'],
        'cagr':         stats.get('cagr_pct'),
        'mdd':          stats.get('max_drawdown_pct'),
        'sharpe':       stats.get('sharpe_ratio'),
        'trades':       stats.get('total_trades'),
        'multiplier':   balance / 10000.0 if balance else 0.0,
        'win_rate':     stats.get('win_rate_pct'),
        'elapsed_s':    round(elapsed, 1),
    }
    print(f"[{tag}] cagr={result['cagr']:.1f}% mdd={result['mdd']:.1f}% "
          f"sharpe={result['sharpe']:.2f} elapsed={elapsed:.0f}s", flush=True)
    return result


def write_results_csv(results: list[dict], out_path: Path) -> None:
    if not results:
        return
    fieldnames = list(results[0].keys())
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def write_verdict(results: list[dict], carriers: list[dict], out_path: Path) -> None:
    lines = []
    lines.append('# 22_SUPERTREND_V7_LEVERAGE_VERDICT\n')
    lines.append(f'Generated: {datetime.now(timezone.utc).isoformat()}\n')
    lines.append(f'Carriers tested: {len(carriers)} (top by sweet_spot_score)\n')
    lines.append(f'Leverages: {sorted(set(r["leverage"] for r in results))}\n')
    lines.append(f'Periods: full (2017-08-18→2026-04-30), recent (2024-03-01→2026-04-30)\n\n')

    lines.append('## 목적\n')
    lines.append('v5_2 PLATEAU 상위 carrier의 3x 이하 레버리지 적합성 확인. '
                 '3x archive는 이미 확정 — v7는 실거래 레버리지 결정을 위한 검증.\n\n')

    lines.append('## Carrier 정보\n')
    lines.append('| combo_id | st_f | st_p | fe | se | de | atr | sweet_score | v5_2_mean_cagr |\n')
    lines.append('|---|---|---|---|---|---|---|---|---|\n')
    for c in carriers:
        lines.append(f"| {c['combo_id']} | {c['st_factor']} | {c['st_period']} | "
                     f"{c['fast_ema_len']} | {c['slow_ema_len']} | {c['direction_ema_len']} | "
                     f"{c['atr_mult']} | {float(c['sweet_spot_score']):.2f} | "
                     f"{float(c['mean_cagr']):.1f}% |\n")
    lines.append('\n')

    # Full-period table grouped by leverage
    full_results = [r for r in results if r['period'] == 'full']
    recent_results = [r for r in results if r['period'] == 'recent']
    leverages = sorted(set(r['leverage'] for r in results))
    combo_ids = [c['combo_id'] for c in carriers]

    lines.append('## 결과: Full period (2017-08-18 → 2026-04-30)\n')
    lines.append('| leverage | combo_id | CAGR% | MDD% | Sharpe | Trades | Multiplier |\n')
    lines.append('|---|---|---|---|---|---|---|\n')
    for lev in leverages:
        for cid in combo_ids:
            r = next((x for x in full_results if x['leverage'] == lev and x['combo_id'] == cid), None)
            if r:
                lines.append(f"| {lev}x | {cid} | {r['cagr']:.1f}% | {r['mdd']:.1f}% | "
                              f"{r['sharpe']:.2f} | {r['trades']} | {r['multiplier']:.2f}x |\n")
    lines.append('\n')

    lines.append('## 결과: Recent period (2024-03-01 → 2026-04-30, W7+W8)\n')
    lines.append('| leverage | combo_id | CAGR% | MDD% | Sharpe | Trades |\n')
    lines.append('|---|---|---|---|---|---|\n')
    for lev in leverages:
        for cid in combo_ids:
            r = next((x for x in recent_results if x['leverage'] == lev and x['combo_id'] == cid), None)
            if r:
                lines.append(f"| {lev}x | {cid} | {r['cagr']:.1f}% | {r['mdd']:.1f}% | "
                              f"{r['sharpe']:.2f} | {r['trades']} |\n")
    lines.append('\n')

    # MDD threshold analysis
    lines.append('## MDD 임계값 분석 (Full period)\n')
    lines.append('실거래 기준: MDD -30% 이하 = VIABLE, -50% 이하 = MARGINAL, 초과 = FAIL\n\n')
    lines.append('| leverage | avg_cagr | avg_mdd | avg_sharpe | verdict |\n')
    lines.append('|---|---|---|---|---|\n')
    for lev in leverages:
        lev_full = [r for r in full_results if r['leverage'] == lev]
        if not lev_full:
            continue
        avg_cagr   = mean(r['cagr'] for r in lev_full if r['cagr'] is not None)
        avg_mdd    = mean(r['mdd']  for r in lev_full if r['mdd']  is not None)
        avg_sharpe = mean(r['sharpe'] for r in lev_full if r['sharpe'] is not None)
        if avg_mdd >= -30:
            verdict = '✅ VIABLE'
        elif avg_mdd >= -50:
            verdict = '⚠️ MARGINAL'
        else:
            verdict = '❌ FAIL'
        lines.append(f"| {lev}x | {avg_cagr:.1f}% | {avg_mdd:.1f}% | {avg_sharpe:.2f} | {verdict} |\n")
    lines.append('\n')

    # Final recommendation
    viable_leverages = []
    for lev in leverages:
        lev_full = [r for r in full_results if r['leverage'] == lev]
        if lev_full:
            avg_mdd = mean(r['mdd'] for r in lev_full if r['mdd'] is not None)
            if avg_mdd >= -50:
                viable_leverages.append((lev, avg_mdd))

    lines.append('## 최종 판단\n\n')
    if viable_leverages:
        best_lev, best_mdd = max(viable_leverages, key=lambda x: x[0])  # highest viable leverage
        lev_full_best = [r for r in full_results if r['leverage'] == best_lev]
        avg_cagr_best = mean(r['cagr'] for r in lev_full_best if r['cagr'] is not None)
        lines.append(f'**추천 레버리지: {best_lev}x** (avg_mdd={best_mdd:.1f}%, avg_cagr={avg_cagr_best:.1f}%)\n\n')
        lines.append('viable 레버리지:\n')
        for lev, mdd in viable_leverages:
            lines.append(f'- {lev}x: avg_mdd={mdd:.1f}%\n')
    else:
        lines.append('❌ **모든 레버리지에서 avg_mdd < -50% — 실거래 부적합**\n\n')
        lines.append('분석 결론: Supertrend 전략은 3x에서와 마찬가지로 모든 레버리지에서 '
                     '구조적 MDD 한계 존재. fa80 funding-arb 전략 유지.\n')

    lines.append('\n---\n')
    lines.append('3x archive 유지. 실거래 = fa80 funding-arb 단독.\n')

    out_path.write_text(''.join(lines))


def main():
    p = argparse.ArgumentParser(description='v7 leverage sweep')
    p.add_argument('--top', type=int, default=3, help='Top N carriers from v7_input_combos.csv')
    p.add_argument('--leverages', type=str, default='1,2,2.5,3')
    args = p.parse_args()

    leverages = [float(x) for x in args.leverages.split(',')]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    carriers = load_top_carriers(V7_INPUT_CSV, args.top)
    print(f'v7 Leverage Sweep — {len(carriers)} carriers × {len(leverages)} leverages × '
          f'{len(PERIODS)} periods = {len(carriers) * len(leverages) * len(PERIODS)} backtests')
    print(f'Carriers: {[c["combo_id"] for c in carriers]}')
    print(f'Leverages: {leverages}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')

    results = []
    total = len(carriers) * len(leverages) * len(PERIODS)
    done  = 0
    for carrier in carriers:
        for leverage in leverages:
            for period_name, start, end in PERIODS:
                done += 1
                print(f'\n[{done}/{total}] carrier={carrier["combo_id"]} lev={leverage}x period={period_name}',
                      flush=True)
                result = run_backtest(carrier, leverage, period_name, start, end, OUTPUT_DIR)
                if result:
                    results.append(result)

    print(f'\nSweep done. {len(results)}/{total} succeeded.')
    print(f'End: {datetime.now(timezone.utc).isoformat()}')

    csv_out = OUTPUT_DIR / 'v7_results.csv'
    write_results_csv(results, csv_out)
    print(f'Results: {csv_out}')

    verdict_out = OUTPUT_DIR / '22_V7_LEVERAGE_VERDICT.md'
    write_verdict(results, carriers, verdict_out)
    print(f'Verdict: {verdict_out}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
