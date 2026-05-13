#!/usr/bin/env python3
"""
A3 Funding Recalc — Phase 5 GO sensitivity analysis

Tests whether the GO margin (adj_cagr=35.71% vs baseline=34.87%, margin=+0.84%p)
survives when:
1. Hold time assumption of 32h → empirical ~41h (29% more funding exposure)
2. P0 funding fill from 0% → 0.01%/8h (Bybit pre-launch underestimation)

Sensitivity: hold_time={32h, 41h, 50h} × p0_fund={0%, 0.01%/8h}
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import statistics

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')

# Config
BASELINE_CAGR = 34.87
FEE_DELTA_PER_SIDE = (0.055 - 0.020) / 100  # taker 0.055% vs assumed 0.020%

PERIOD_DATES = {
    'p0': ('2018-04-01', '2020-06-30'),
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

PERIOD_YEARS = {
    'p0': 2.25, 'p1': 5.08, 'p2': 3.42, 'p3': 4.50, 'p4': 2.83
}

HOLD_HOURS = [32, 41, 50]
P0_FUND_OVERRIDES = [None, 0.0001]  # None = actual data, 0.0001 = 0.01%/8h

# Load inputs
print("[1] Loading funding rates...")
fund_df = pd.read_parquet(ROOT / 'data/funding/BTCUSDT_8h.parquet')
fund_df['date'] = pd.to_datetime(fund_df['timestamp'], unit='ms')
print(f"    {len(fund_df)} rows, range: {fund_df['date'].min()} to {fund_df['date'].max()}")

print("[2] Loading summary...")
summary = json.loads((ROOT / 'results/param_sweep/v3/supertrend/4h/long_only/combo_18/summary.json').read_text())
periods_data = summary['periods']

# Helper: get average funding rate for a period
def get_avg_funding_for_period(period_id, override_fund=None):
    """
    If override_fund is None: use actual data (0.0 for p0 since all rows have rate=0)
    If override_fund is a number: use that uniformly for the period
    """
    if override_fund is not None:
        return override_fund

    start_date, end_date = PERIOD_DATES[period_id]
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    mask = (fund_df['date'] >= start_ts) & (fund_df['date'] <= end_ts)
    period_rates = fund_df.loc[mask, 'funding_rate']

    if len(period_rates) == 0:
        return 0.0

    # Return mean, filtering out exact zeros only if there are non-zero values
    non_zero = period_rates[period_rates != 0.0]
    if len(non_zero) > 0:
        return non_zero.mean()
    else:
        return 0.0

# Build results
results = {}

print("[3] Computing sensitivities...")
for hold_h in HOLD_HOURS:
    for p0_fund_idx, p0_fund in enumerate(P0_FUND_OVERRIDES):
        key = f"hold{hold_h}_p0fund{p0_fund_idx}"

        period_adj_cagrs = []
        period_details = {}

        for period_id in ['p0', 'p1', 'p2', 'p3', 'p4']:
            original_cagr = periods_data[period_id]['cagr']
            trades = periods_data[period_id]['trades']
            period_years = PERIOD_YEARS[period_id]

            # Get avg funding rate
            if period_id == 'p0' and p0_fund is not None:
                avg_fund = p0_fund
            else:
                avg_fund = get_avg_funding_for_period(period_id)

            # Annual metrics
            trades_per_year = trades / period_years
            n_funding_per_trade = hold_h / 8.0

            # Fee cost: trading both sides (open + close), delta vs baseline
            fee_cost_annual = trades_per_year * FEE_DELTA_PER_SIDE * 2 * 100  # %

            # Funding cost: funding_rate * hold_periods * trades_per_year
            # long_only means we PAY funding when positive (sign=+1)
            fund_cost_annual = trades_per_year * n_funding_per_trade * avg_fund * 1.0 * 100  # %

            # Adjusted CAGR
            adj_cagr = original_cagr - fee_cost_annual - fund_cost_annual

            period_adj_cagrs.append(adj_cagr)
            period_details[period_id] = {
                'original_cagr': round(original_cagr, 2),
                'trades': trades,
                'trades_per_year': round(trades_per_year, 2),
                'avg_fund_rate': round(avg_fund, 6),
                'n_funding_per_trade': round(n_funding_per_trade, 2),
                'fee_cost_annual': round(fee_cost_annual, 2),
                'fund_cost_annual': round(fund_cost_annual, 2),
                'adj_cagr': round(adj_cagr, 2),
            }

        overall_adj_cagr = statistics.mean(period_adj_cagrs)
        margin_vs_baseline = overall_adj_cagr - BASELINE_CAGR

        results[key] = {
            'hold_hours': hold_h,
            'p0_fund_override': p0_fund,
            'overall_adj_cagr': round(overall_adj_cagr, 2),
            'margin_vs_baseline': round(margin_vs_baseline, 2),
            'periods': period_details,
        }

# Write JSON
output_json = ROOT / 'results/validation_phase5/a3_funding_sensitivity.json'
with open(output_json, 'w') as f:
    json.dump(results, f, indent=2)
print(f"[4] Written: {output_json}")

# Write Markdown table
output_md = ROOT / 'results/validation_phase5/a3_funding_sensitivity.md'
with open(output_md, 'w') as f:
    f.write("# A3 Funding Sensitivity Analysis\n\n")
    f.write(f"**Generated**: {datetime.now().isoformat()}\n\n")
    f.write(f"**Baseline CAGR**: {BASELINE_CAGR}%\n\n")
    f.write("## Summary Table\n\n")
    f.write("| Hold Time | P0 Fund | Adj CAGR | Margin vs Baseline | Status |\n")
    f.write("|-----------|---------|----------|--------------------|---------|\n")

    for hold_h in HOLD_HOURS:
        for p0_fund_idx, p0_fund in enumerate(P0_FUND_OVERRIDES):
            key = f"hold{hold_h}_p0fund{p0_fund_idx}"
            res = results[key]
            adj_cagr = res['overall_adj_cagr']
            margin = res['margin_vs_baseline']

            p0_fund_str = "0% (actual)" if p0_fund is None else "0.01% /8h"
            status = "✓ GO" if margin >= 0.5 else "⚠ MARGINAL" if margin >= 0 else "✗ FAIL"

            f.write(f"| {hold_h}h | {p0_fund_str} | {adj_cagr}% | {margin:+.2f}%p | {status} |\n")

    f.write("\n## Detailed Results\n\n")
    for hold_h in HOLD_HOURS:
        for p0_fund_idx, p0_fund in enumerate(P0_FUND_OVERRIDES):
            key = f"hold{hold_h}_p0fund{p0_fund_idx}"
            res = results[key]

            p0_fund_str = "0% (actual)" if p0_fund is None else "0.01% /8h"
            f.write(f"\n### Hold={hold_h}h, P0 Fund={p0_fund_str}\n\n")
            f.write(f"**Overall Adj CAGR**: {res['overall_adj_cagr']}%\n")
            f.write(f"**Margin vs Baseline**: {res['margin_vs_baseline']:+.2f}%p\n\n")

            f.write("| Period | Original | Adj CAGR | Fee Cost | Fund Cost | Trades/Yr |\n")
            f.write("|--------|----------|----------|----------|-----------|----------|\n")
            for period_id in ['p0', 'p1', 'p2', 'p3', 'p4']:
                p_det = res['periods'][period_id]
                f.write(
                    f"| {period_id} | {p_det['original_cagr']}% | {p_det['adj_cagr']}% | "
                    f"{p_det['fee_cost_annual']:.2f}% | {p_det['fund_cost_annual']:.2f}% | "
                    f"{p_det['trades_per_year']:.1f} |\n"
                )

print(f"[5] Written: {output_md}")
print("\n" + "="*70)
print("SENSITIVITY ANALYSIS COMPLETE")
print("="*70)

# Print summary to stdout
print("\n📊 SUMMARY TABLE\n")
print("| Hold Time | P0 Fund | Adj CAGR | Margin | Status |")
print("|-----------|---------|----------|--------|--------|")
for hold_h in HOLD_HOURS:
    for p0_fund_idx, p0_fund in enumerate(P0_FUND_OVERRIDES):
        key = f"hold{hold_h}_p0fund{p0_fund_idx}"
        res = results[key]
        p0_fund_str = "0% (actual)" if p0_fund is None else "0.01% /8h"
        status = "GO" if res['margin_vs_baseline'] >= 0.5 else "MARGINAL" if res['margin_vs_baseline'] >= 0 else "FAIL"
        print(f"| {hold_h}h | {p0_fund_str:<13} | {res['overall_adj_cagr']:>6}% | {res['margin_vs_baseline']:>+5.2f}%p | {status} |")

print("\n🎯 CRITICAL SCENARIO: hold=41h, p0_fund=0.01%/8h")
critical_key = "hold41_p0fund1"
critical = results[critical_key]
print(f"   Adj CAGR: {critical['overall_adj_cagr']}%")
print(f"   Margin vs Baseline: {critical['margin_vs_baseline']:+.2f}%p")
print(f"   VERDICT: {'✓ GO margin survives' if critical['margin_vs_baseline'] >= 0.5 else '✗ Margin LOST'}")
