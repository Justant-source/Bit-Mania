#!/usr/bin/env python3
"""
AGENT-A1: Hold Time Analysis for combo_18 Phase 5 validation

Measures actual trade hold times from intrabar backtest results.
Compares against 32h assumption used in realistic cost calculations.

Input: trades.csv files (epoch milliseconds)
Output: JSON validation report + stdout table
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
COMBO18_INTRABAR = ROOT / 'results/intrabar/supertrend/4h/long_only/combo_18'
OUTPUT_DIR = ROOT / 'results/validation_phase5'
OUTPUT_FILE = OUTPUT_DIR / 'a1_hold_time.json'
PERIODS = ['p0', 'p1', 'p2', 'p3', 'p4']
ASSUMED_HOLD_H = 32.0  # Current assumption in apply_realistic_costs.py
FUNDING_CYCLE_H = 8.0  # 8-hour funding cycle


def load_and_analyze_period(period: str) -> Dict[str, Any]:
    """
    Load trades.csv for period and compute hold time statistics.

    Returns:
        Dict with keys: n, mean_h, median_h, p25_h, p75_h, p95_h, min_h, max_h, n_funding_avg
    """
    csv_path = COMBO18_INTRABAR / period / 'trades.csv'

    if not csv_path.exists():
        raise FileNotFoundError(f"Missing {csv_path}")

    df = pd.read_csv(csv_path)

    # Calculate hold times in hours (convert from milliseconds)
    hold_ms = df['closed_at'] - df['opened_at']
    hold_h = hold_ms / 3_600_000

    # Compute statistics
    stats = {
        'n': len(hold_h),
        'mean_h': round(hold_h.mean(), 2),
        'median_h': round(hold_h.median(), 2),
        'p25_h': round(hold_h.quantile(0.25), 2),
        'p75_h': round(hold_h.quantile(0.75), 2),
        'p95_h': round(hold_h.quantile(0.95), 2),
        'min_h': round(hold_h.min(), 2),
        'max_h': round(hold_h.max(), 2),
    }

    # Compute number of funding cycles per trade
    stats['n_funding_avg'] = round(stats['mean_h'] / FUNDING_CYCLE_H, 2)

    return stats


def compute_overall_weighted_mean(period_stats: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute weighted mean hold time across all periods.
    Weight by trade count per period.
    """
    total_trades = sum(s['n'] for s in period_stats.values())

    weighted_sum = sum(
        s['mean_h'] * s['n']
        for s in period_stats.values()
    )

    weighted_mean_h = round(weighted_sum / total_trades, 2)
    n_funding_avg = round(weighted_mean_h / FUNDING_CYCLE_H, 2)

    # Gap vs assumed (32h)
    gap_pct = round(
        (weighted_mean_h - ASSUMED_HOLD_H) / ASSUMED_HOLD_H * 100,
        2
    )

    return {
        'weighted_mean_h': weighted_mean_h,
        'n_funding_avg': n_funding_avg,
        'assumed_h': ASSUMED_HOLD_H,
        'vs_assumed_pct_gap': gap_pct,
    }


def main():
    print("=" * 80)
    print("AGENT-A1: Hold Time Analysis (combo_18)")
    print("=" * 80)
    print()

    # Analyze each period
    period_stats = {}
    for period in PERIODS:
        stats = load_and_analyze_period(period)
        period_stats[period] = stats
        print(f"{period}: n={stats['n']}, mean={stats['mean_h']}h, median={stats['median_h']}h")

    print()

    # Compute overall statistics
    overall = compute_overall_weighted_mean(period_stats)

    # Print summary table
    print("Hold Time Distribution (all periods)")
    print("-" * 80)
    print(f"{'Period':<8} {'N':>6} {'Mean':>8} {'Median':>8} {'P25':>8} {'P75':>8} {'P95':>8} {'Cycles':>8}")
    print("-" * 80)
    for period in PERIODS:
        s = period_stats[period]
        print(
            f"{period:<8} {s['n']:>6} {s['mean_h']:>8.2f}h {s['median_h']:>8.2f}h "
            f"{s['p25_h']:>8.2f}h {s['p75_h']:>8.2f}h {s['p95_h']:>8.2f}h {s['n_funding_avg']:>8.2f}"
        )
    print("-" * 80)
    print()

    # Key finding
    print("KEY FINDING:")
    print(f"  Weighted mean hold time: {overall['weighted_mean_h']}h")
    print(f"  Assumed in apply_realistic_costs.py: {overall['assumed_h']}h")
    print(f"  Gap: {overall['vs_assumed_pct_gap']:+.2f}%")
    print(f"  Funding cycles per trade: {overall['n_funding_avg']}")
    print()

    if overall['vs_assumed_pct_gap'] > 5:
        print(f"  ⚠️  ACTUAL IS {overall['vs_assumed_pct_gap']:.1f}% HIGHER than assumed")
        print(f"     → Cost model may be UNDERESTIMATING funding fees")
    elif overall['vs_assumed_pct_gap'] < -5:
        print(f"  ✓  Actual is {abs(overall['vs_assumed_pct_gap']):.1f}% lower than assumed")
        print(f"     → Cost model is conservative")
    else:
        print(f"  ✓  Within ±5% of assumption (model well-calibrated)")

    print()

    # Write JSON output
    output_data = {
        **period_stats,
        'overall': overall
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"Output written to: {OUTPUT_FILE}")
    print()
    print(json.dumps(output_data, indent=2))

    return output_data


if __name__ == '__main__':
    main()
