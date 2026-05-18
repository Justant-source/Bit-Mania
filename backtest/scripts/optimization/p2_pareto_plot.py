#!/usr/bin/env python3
"""
p2_pareto_plot.py — CAGR vs MDD Pareto frontier scatter plot

Usage:
    python3 p2_pareto_plot.py \
        --csv /result/p1_optimization/all_results.csv \
        --output /result/p1_optimization/pareto.png
"""
from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

CHAMPION_HP = {
    'st_factor': 2.5, 'st_period': 6, 'fast_ema_len': 7,
    'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0, 'sl_margin_pct': 0.0,
}


def load_csv(path: Path) -> list[dict]:
    rows = []
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['status'] != 'done':
                continue
            try:
                rows.append({
                    'combo_id':         int(row['combo_id']),
                    'st_factor':        float(row['st_factor']),
                    'st_period':        int(row['st_period']),
                    'fast_ema_len':     int(row['fast_ema_len']),
                    'slow_ema_len':     int(row['slow_ema_len']),
                    'direction_ema_len': int(row['direction_ema_len']),
                    'atr_mult':         float(row['atr_mult']),
                    'sl_margin_pct':    float(row['sl_margin_pct']),
                    'cagr':             float(row['cagr']),
                    'mdd':              float(row['mdd']),
                    'sharpe':           float(row['sharpe']) if row['sharpe'] else 0.0,
                })
            except (ValueError, KeyError):
                continue
    return rows


def is_champion(row: dict) -> bool:
    for k, v in CHAMPION_HP.items():
        if abs(row.get(k, -999) - v) > 0.001:
            return False
    return True


def pareto_frontier(rows: list[dict]) -> list[dict]:
    """Return rows on the Pareto frontier (higher CAGR, higher MDD i.e. less negative)."""
    frontier = []
    for r in rows:
        dominated = False
        for other in rows:
            if other is r:
                continue
            if other['cagr'] >= r['cagr'] and other['mdd'] >= r['mdd']:
                if other['cagr'] > r['cagr'] or other['mdd'] > r['mdd']:
                    dominated = True
                    break
        if not dominated:
            frontier.append(r)
    return sorted(frontier, key=lambda r: r['cagr'], reverse=True)


def plot(rows: list[dict], output: Path) -> None:
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.cm as cm
        import numpy as np
    except ImportError:
        print("[WARN] matplotlib not available — skipping plot, writing text summary")
        _write_text_summary(rows, output.with_suffix('.txt'))
        return

    fig, ax = plt.subplots(figsize=(12, 8))

    # color by sl_margin_pct
    sl_values = sorted(set(r['sl_margin_pct'] for r in rows))
    color_map = {sl: cm.tab10(i / max(len(sl_values) - 1, 1)) for i, sl in enumerate(sl_values)}

    for sl in sl_values:
        subset = [r for r in rows if r['sl_margin_pct'] == sl]
        xs = [r['mdd'] for r in subset]
        ys = [r['cagr'] for r in subset]
        label = f"sl={sl:.0f}%" if sl < 0 else "sl=off"
        ax.scatter(xs, ys, c=[color_map[sl]], alpha=0.4, s=15, label=label)

    # Pareto frontier
    frontier = pareto_frontier(rows)
    if frontier:
        fx = [r['mdd'] for r in frontier]
        fy = [r['cagr'] for r in frontier]
        ax.plot(sorted(fx), [fy[fx.index(x)] for x in sorted(fx)],
                'k--', linewidth=1.5, label='Pareto frontier', zorder=5)

    # Champion marker
    champ = next((r for r in rows if is_champion(r)), None)
    if champ:
        ax.scatter([champ['mdd']], [champ['cagr']],
                   c='red', marker='*', s=300, zorder=10, label='Champion (combo_18 equiv)')

    ax.set_xlabel('Max Drawdown (%)')
    ax.set_ylabel('CAGR (%)')
    ax.set_title('Supertrend 4h Long 3x — CAGR vs MDD Pareto (4,374 combos)')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=150)
    plt.close(fig)
    print(f"Plot saved: {output}")

    # print top 10 frontier
    print(f"\nPareto frontier ({len(frontier)} points):")
    for r in frontier[:10]:
        sl_label = f"sl={r['sl_margin_pct']:.0f}%" if r['sl_margin_pct'] < 0 else "sl=off"
        print(f"  combo={r['combo_id']} cagr={r['cagr']:.1f}% mdd={r['mdd']:.1f}% "
              f"sharpe={r['sharpe']:.3f} {sl_label}")


def _write_text_summary(rows: list[dict], path: Path) -> None:
    frontier = pareto_frontier(rows)
    lines = ["# Pareto Frontier (text fallback — matplotlib not available)", ""]
    lines.append(f"Total done: {len(rows)} | Frontier points: {len(frontier)}")
    lines.append("")
    lines.append("| combo_id | CAGR% | MDD% | Sharpe | sl% |")
    lines.append("|---|---|---|---|---|")
    for r in frontier[:20]:
        sl = f"{r['sl_margin_pct']:.0f}" if r['sl_margin_pct'] < 0 else "off"
        lines.append(f"| {r['combo_id']} | {r['cagr']:.1f} | {r['mdd']:.1f} | "
                     f"{r['sharpe']:.3f} | {sl} |")
    path.write_text('\n'.join(lines) + '\n')
    print(f"Text summary: {path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--csv', type=str, default='/result/p1_optimization/all_results.csv')
    p.add_argument('--output', type=str, default='/result/p1_optimization/pareto.png')
    args = p.parse_args()

    csv_path = Path(args.csv)
    output_path = Path(args.output)

    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1

    rows = load_csv(csv_path)
    print(f"Loaded {len(rows)} done rows")
    if not rows:
        print("[WARN] No done rows — nothing to plot")
        return 0

    plot(rows, output_path)
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
