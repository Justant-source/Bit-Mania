#!/usr/bin/env python3
"""
v3_walk_forward.py — Phase B: top-N hard_pass combo 독립 10-fold 검증 + plateau detection

v3_all_combos.csv에서 hard_pass 상위 N개를 선정,
각각 WINDOWS_10에 대해 독립 subprocess 백테스트 실행 → robustness + plateau 판정.

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v3_walk_forward.py \
        --output-dir /result/v3_optimization \
        --top-n 50
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from statistics import mean, stdev

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

BACKTEST_TIMEOUT = 900  # 15 min per window backtest
GRID_LEVELS = {
    'st_factor':         [2.3, 2.4, 2.5],
    'st_period':         [5, 6, 7, 8, 9],
    'fast_ema_len':      [5, 7, 9, 11],
    'slow_ema_len':      [25, 30, 35],
    'direction_ema_len': [200, 230, 260],
    'atr_mult':          [2.5, 3.0, 3.5],
}


def load_top_n(csv_path: Path, n: int) -> list[dict]:
    """Load hard_pass combos sorted by composite, take top N."""
    rows = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if row.get('hard_pass', '').lower() in ('true', '1'):
                    rows.append({
                        'combo_id':          int(row['combo_id']),
                        'st_factor':         float(row['st_factor']),
                        'st_period':         int(row['st_period']),
                        'fast_ema_len':      int(row['fast_ema_len']),
                        'slow_ema_len':      int(row['slow_ema_len']),
                        'direction_ema_len': int(row['direction_ema_len']),
                        'atr_mult':          float(row['atr_mult']),
                        'sl_margin_pct':     float(row['sl_margin_pct']),
                        'composite':         float(row.get('composite', 0) or 0),
                        'mean_cagr_adj':     float(row.get('mean_cagr_adj', 0) or 0),
                        'worst_mdd':         float(row.get('worst_mdd', 0) or 0),
                        'n_positive':        int(row.get('n_positive', 0) or 0),
                        'total_trades':      int(row.get('total_trades', 0) or 0),
                    })
            except (ValueError, KeyError):
                continue
    rows.sort(key=lambda r: r['composite'], reverse=True)
    return rows[:n]


def run_window_backtest(combo: dict, w_name: str, w_start: str, w_end: str,
                        out_dir: Path) -> dict | None:
    hp = {k: combo[k] for k in ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
                                  'direction_ema_len', 'atr_mult', 'sl_margin_pct']}
    hp_json = json.dumps(hp)

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategyWithSL',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', w_start,
        '--end', w_end,
        '--balance', '10000',
        '--fee', '0.00055',
        '--hp-json', hp_json,
        '--output', str(out_dir),
    ]

    run_dir = Path(tempfile.mkdtemp(
        prefix=f"jesse_v3wf_{combo['combo_id']}_{w_name}_"
    ))
    (run_dir / 'strategies').symlink_to('/app/strategies')

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir), timeout=BACKTEST_TIMEOUT)
        stats_path = out_dir / 'stats.json'
        if not stats_path.exists():
            return None
        with open(stats_path) as f:
            stats = json.load(f)
        return {
            'window': w_name,
            'cagr':   stats.get('cagr_pct'),
            'mdd':    stats.get('max_drawdown_pct'),
            'sharpe': stats.get('sharpe_ratio'),
            'trades': stats.get('total_trades'),
        }
    except Exception as e:
        print(f"  [ERROR combo={combo['combo_id']} {w_name}] {e}", file=sys.stderr)
        return None
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def compute_robustness(window_results: list[dict | None]) -> str:
    """ROBUST / BORDERLINE / OVERFIT per plan §5-2."""
    valid = [w for w in window_results if w and w.get('cagr') is not None]
    if len(valid) < 5:
        return 'INCOMPLETE'

    cagrs = [w['cagr'] for w in valid]
    pos_ratio = sum(1 for c in cagrs if c > 0) / len(cagrs)

    cagr_mean = mean(cagrs)
    cagr_std  = stdev(cagrs) if len(cagrs) > 1 else 0.0
    cv = cagr_std / abs(cagr_mean) if cagr_mean != 0 else float('inf')

    # Recent = last 3 valid windows
    recent_cagrs = [w['cagr'] for w in valid[-3:]]
    recent_mean = mean(recent_cagrs)
    recent_ratio = recent_mean / cagr_mean if cagr_mean > 0 else 0

    if pos_ratio >= 0.7 and cv < 1.5 and recent_ratio > 0.5:
        return 'ROBUST'
    elif pos_ratio >= 0.6 and cv < 2.0:
        return 'BORDERLINE'
    else:
        return 'OVERFIT'


def compute_plateau(combo: dict, all_combos: list[dict]) -> str:
    """PLATEAU / ISLAND / MIXED / EDGE per plan §4-2."""
    param_order = list(GRID_LEVELS.keys())

    # Build a lookup dict: (param tuple) -> composite
    lookup: dict[tuple, float] = {
        tuple(r[p] for p in param_order): r['composite']
        for r in all_combos
    }

    target_key = tuple(combo[p] for p in param_order)
    target_score = combo['composite']
    neighbor_scores = []

    for i, (param, levels) in enumerate(GRID_LEVELS.items()):
        target_val = combo[param]
        try:
            target_idx = next(j for j, v in enumerate(levels)
                              if abs(float(v) - float(target_val)) < 0.001)
        except StopIteration:
            continue

        for offset in (-1, 1):
            new_idx = target_idx + offset
            if not (0 <= new_idx < len(levels)):
                continue
            # Change only the i-th element of the target key
            key = list(target_key)
            key[i] = levels[new_idx]
            neighbor_key = tuple(key)
            if neighbor_key in lookup:
                neighbor_scores.append(lookup[neighbor_key])

    if not neighbor_scores:
        return 'EDGE'

    mean_n = mean(neighbor_scores)
    std_n  = stdev(neighbor_scores) if len(neighbor_scores) > 1 else 0.0

    if target_score > mean_n * 1.20:
        return 'ISLAND'
    elif mean_n > 0 and std_n / mean_n < 0.10:
        return 'PLATEAU'
    else:
        return 'MIXED'


def write_report(results: list[dict], output: Path) -> None:
    lines = [
        '# v3 Walk-Forward OOS + Plateau 검증 (Phase B)',
        '',
        f'hard_pass 상위 {len(results)}개 combo × 10 독립 윈도우 백테스트',
        '',
        '> **Robustness 기준** (§5-2): pos_ratio≥0.7 & CV<1.5 & recent_ratio>0.5 → ROBUST',
        '> **Plateau 기준** (§4-2): 이웃 std/mean<0.10 → PLATEAU, target>이웃평균×1.2 → ISLAND',
        '',
    ]

    for r in results:
        combo = r['combo']
        window_results = r['windows']
        robustness = r['robustness']
        plateau = r['plateau']

        verdict_emoji = {'ROBUST': '✅', 'BORDERLINE': '⚠️', 'OVERFIT': '❌', 'INCOMPLETE': '❓'}.get(robustness, '❓')
        plateau_emoji = {'PLATEAU': '✅', 'MIXED': '⚠️', 'ISLAND': '❌', 'EDGE': '🔲'}.get(plateau, '❓')

        lines += [
            f"## combo_id={combo['combo_id']} | Robustness: {verdict_emoji}{robustness} | Plateau: {plateau_emoji}{plateau}",
            '',
            f"HP: st_factor={combo['st_factor']} | st_period={combo['st_period']} | "
            f"fast_ema={combo['fast_ema_len']} | slow_ema={combo['slow_ema_len']} | "
            f"dir_ema={combo['direction_ema_len']} | atr_mult={combo['atr_mult']} | sl=-25%",
            '',
            f"Phase A composite={combo['composite']:.2f} | n_positive={combo['n_positive']}/10 | "
            f"mean_cagr_adj={combo['mean_cagr_adj']:.1f}% | worst_mdd={combo['worst_mdd']:.1f}%",
            '',
            '| 윈도우 | 기간 | CAGR% | MDD% | Sharpe | trades |',
            '|---|---|---|---|---|---|',
        ]

        valid_cagrs = []
        for (w_name, w_start, w_end), w_res in zip(WINDOWS_10, window_results):
            if w_res is None:
                lines.append(f"| {w_name} | {w_start}~{w_end} | ERR | ERR | ERR | ERR |")
            else:
                cagr  = w_res.get('cagr')
                mdd   = w_res.get('mdd')
                sharpe = w_res.get('sharpe', 0)
                trades = w_res.get('trades', 0)
                cagr_str  = f"{cagr:.1f}"  if cagr  is not None else 'N/A'
                mdd_str   = f"{mdd:.1f}"   if mdd   is not None else 'N/A'
                sharpe_str = f"{sharpe:.3f}" if sharpe is not None else 'N/A'
                lines.append(f"| {w_name} | {w_start}~{w_end} | {cagr_str} | {mdd_str} | {sharpe_str} | {trades} |")
                if cagr is not None:
                    valid_cagrs.append(cagr)

        if valid_cagrs:
            pos = sum(1 for c in valid_cagrs if c > 0)
            lines.append('')
            lines.append(f"독립 OOS: 양수 구간 {pos}/{len(valid_cagrs)} | 평균 CAGR {mean(valid_cagrs):.1f}%")

        lines.append('')

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(lines) + '\n')
    print(f"Report: {output}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output-dir', type=str, default='/result/v3_optimization')
    p.add_argument('--top-n', type=int, default=50)
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    csv_path = output_dir / 'v3_all_combos.csv'

    if not csv_path.exists():
        print(f"[ERROR] v3_all_combos.csv not found: {csv_path}", file=sys.stderr)
        print("Run v3_aggregate.py first.", file=sys.stderr)
        return 1

    # Load all combos for plateau lookup
    all_combos: list[dict] = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                all_combos.append({
                    'combo_id':          int(row['combo_id']),
                    'st_factor':         float(row['st_factor']),
                    'st_period':         int(row['st_period']),
                    'fast_ema_len':      int(row['fast_ema_len']),
                    'slow_ema_len':      int(row['slow_ema_len']),
                    'direction_ema_len': int(row['direction_ema_len']),
                    'atr_mult':          float(row['atr_mult']),
                    'sl_margin_pct':     float(row['sl_margin_pct']),
                    'composite':         float(row.get('composite', 0) or 0),
                    'mean_cagr_adj':     float(row.get('mean_cagr_adj', 0) or 0),
                    'worst_mdd':         float(row.get('worst_mdd', 0) or 0),
                    'n_positive':        int(row.get('n_positive', 0) or 0),
                    'hard_pass':         row.get('hard_pass', '').lower() in ('true', '1'),
                })
            except (ValueError, KeyError):
                continue

    candidates = sorted(
        [r for r in all_combos if r['hard_pass']],
        key=lambda r: r['composite'], reverse=True
    )[:args.top_n]

    print(f"Walk-forward: {len(candidates)} candidates × {len(WINDOWS_10)} windows "
          f"= {len(candidates) * len(WINDOWS_10)} backtests")

    all_results = []
    for combo in candidates:
        print(f"\n  combo={combo['combo_id']} composite={combo['composite']:.2f} "
              f"n_pos={combo['n_positive']}/10 worst_mdd={combo['worst_mdd']:.1f}%")
        window_results = []
        for w_name, w_start, w_end in WINDOWS_10:
            out = output_dir / f"wf_combo_{combo['combo_id']}_{w_name}"
            t0 = time.time()
            res = run_window_backtest(combo, w_name, w_start, w_end, out)
            elapsed = time.time() - t0
            if res:
                print(f"    {w_name}: cagr={res['cagr']:.1f}% mdd={res['mdd']:.1f}% "
                      f"trades={res['trades']} ({elapsed:.0f}s)")
            else:
                print(f"    {w_name}: FAILED ({elapsed:.0f}s)")
            window_results.append(res)

        robustness = compute_robustness(window_results)
        plateau    = compute_plateau(combo, all_combos)
        print(f"  → Robustness: {robustness} | Plateau: {plateau}")

        all_results.append({
            'combo': combo,
            'windows': window_results,
            'robustness': robustness,
            'plateau': plateau,
        })

    report_path = output_dir / 'walk_forward_top50.md'
    write_report(all_results, report_path)

    # Print summary
    robust_plateau = [r for r in all_results
                      if r['robustness'] == 'ROBUST' and r['plateau'] == 'PLATEAU']
    print(f"\n=== Phase B 결과 ===")
    print(f"ROBUST + PLATEAU (winner 후보): {len(robust_plateau)}/{len(all_results)}")
    for r in robust_plateau:
        c = r['combo']
        print(f"  combo={c['combo_id']} composite={c['composite']:.2f} "
              f"n_pos={c['n_positive']}/10 worst_mdd={c['worst_mdd']:.1f}%")

    return 0


if __name__ == '__main__':
    sys.exit(main())
