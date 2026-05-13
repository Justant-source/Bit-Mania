#!/usr/bin/env python3
"""
AGENT-A2: MDD Confirmation for combo_18 (supertrend/4h/long_only)
Compares MDD across 3 sources for Phase 5 GO/NO-GO decision.

Sources:
  - Source A: param_sweep v3 (1m fake expansion) → p0 MDD = -28.86%
  - Source B: intrabar replay (real 1m wicks) → p0 MDD = -33.08%
  - Source C: adjusted_costs (fee-adjusted from A, but should be from B) → p0 adj_mdd = -30.39%
"""

import json
from pathlib import Path

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
OUT = ROOT / 'results/validation_phase5'
OUT.mkdir(parents=True, exist_ok=True)

# Load Source A: param_sweep v3
sweep_path = ROOT / 'results/param_sweep/v3/supertrend/4h/long_only/combo_18/summary.json'
with open(sweep_path) as f:
    sweep_data = json.load(f)

# Load Source B: intrabar (real 1m)
intrabar_data = {}
for p in range(5):
    intrabar_path = ROOT / f'results/intrabar/supertrend/4h/long_only/combo_18/p{p}/stats.json'
    with open(intrabar_path) as f:
        intrabar_data[f'p{p}'] = json.load(f)

# Load Source C: adjusted_costs
adjusted_path = ROOT / 'results/adjusted_costs/supertrend/4h/long_only/combo_18/adjusted_stats.json'
with open(adjusted_path) as f:
    adjusted_data = json.load(f)

# Build comparison table
periods = {}
for p in range(5):
    period_key = f'p{p}'
    sweep_period = sweep_data['periods'][period_key]
    intrabar_period = intrabar_data[period_key]
    adjusted_period = adjusted_data['periods'][period_key]

    periods[period_key] = {
        'sweep_cagr': round(sweep_period['cagr'], 2),
        'sweep_mdd': round(sweep_period['mdd'], 2),
        'sweep_sharpe': round(sweep_period['sharpe'], 2),
        'sweep_trades': sweep_period['trades'],
        'intrabar_cagr': round(intrabar_period['cagr_pct'], 2),
        'intrabar_mdd': round(intrabar_period['max_drawdown_pct'], 2),
        'intrabar_trades': intrabar_period['total_trades'],
        'intrabar_verdict': intrabar_period['verdict'],
        'adj_cagr': round(adjusted_period['adj_cagr'], 2),
        'adj_mdd': round(adjusted_period['adj_mdd'], 2),
        'adj_sharpe': round(adjusted_period['adj_sharpe'], 2),
        'mdd_gap': round(intrabar_period['max_drawdown_pct'] - sweep_period['mdd'], 2),
    }

# Compute summary stats
intrabar_mdds = [periods[f'p{i}']['intrabar_mdd'] for i in range(5)]
mean_intrabar_mdd = round(sum(intrabar_mdds) / 5, 2)
worst_intrabar_mdd = round(min(intrabar_mdds), 2)
periods_failing_minus30 = sum(1 for mdd in intrabar_mdds if mdd < -30.0)
mdd_gaps = [periods[f'p{i}']['mdd_gap'] for i in range(5)]
mean_mdd_gap = round(sum(mdd_gaps) / 5, 2)

# Determine recommendation
if periods_failing_minus30 >= 3:
    recommendation = f"CRITICAL: {periods_failing_minus30}/5 periods fail -30% MDD gate (intrabar). Source B (real 1m) is most realistic."
elif periods_failing_minus30 == 2:
    recommendation = f"WARNING: {periods_failing_minus30}/5 periods fail -30% MDD gate. Review risk tolerance."
else:
    recommendation = f"OK: Only {periods_failing_minus30}/5 periods fail -30% MDD gate."

summary = {
    'mean_intrabar_mdd': mean_intrabar_mdd,
    'worst_intrabar_mdd': worst_intrabar_mdd,
    'periods_failing_minus30': periods_failing_minus30,
    'mean_mdd_gap': mean_mdd_gap,
    'recommendation': recommendation,
    'notes': 'Source B (intrabar with real 1m wicks) is most realistic for Phase 5 risk assessment.',
}

# Write JSON output
output_json = {
    'combo': 'combo_18',
    'strategy': 'supertrend',
    'timeframe': '4h',
    'variant': 'long_only',
    'periods': periods,
    'summary': summary,
}

json_path = OUT / 'a2_p0_mdd.json'
with open(json_path, 'w') as f:
    json.dump(output_json, f, indent=2)

# Write markdown report
md_path = OUT / 'a2_p0_mdd.md'
with open(md_path, 'w') as f:
    f.write("""# AGENT-A2: MDD Confirmation for combo_18

## Executive Summary
Phase 5 GO/NO-GO decision for combo_18 (supertrend/4h/long_only) based on drawdown risk across 3 sources.

### Key Findings
""")
    f.write(f"- **Mean Intrabar MDD (Source B)**: {mean_intrabar_mdd}%\n")
    f.write(f"- **Worst Intrabar MDD**: {worst_intrabar_mdd}%\n")
    f.write(f"- **Periods Failing -30% Gate**: {periods_failing_minus30}/5\n")
    f.write(f"- **Mean MDD Gap (intrabar vs sweep)**: {mean_mdd_gap}%\n")
    f.write(f"- **Recommendation**: {summary['recommendation']}\n\n")

    f.write("## 5-Period Comparison Table\n\n")
    f.write("| Period | Sweep CAGR | Sweep MDD | Intrabar CAGR | Intrabar MDD | Adj MDD | MDD Gap | Verdict |\n")
    f.write("|--------|-----------|-----------|--------------|--------------|---------|---------|----------|\n")
    for p in range(5):
        period_key = f'p{p}'
        row = periods[period_key]
        f.write(f"| {period_key} | {row['sweep_cagr']}% | {row['sweep_mdd']}% | {row['intrabar_cagr']}% | {row['intrabar_mdd']}% | {row['adj_mdd']}% | {row['mdd_gap']}% | {row['intrabar_verdict']} |\n")

    f.write("\n## Detailed Analysis\n\n")
    f.write("### Source Comparison\n")
    f.write("- **Source A (param_sweep v3)**: 1m fake expansion → p0 MDD = -28.86%\n")
    f.write("- **Source B (intrabar replay)**: Real 1m wicks → p0 MDD = -33.08%\n")
    f.write("- **Source C (adjusted_costs)**: Fee-adjusted from A → p0 adj_mdd = -30.39%\n\n")
    f.write("### Assessment\n")
    f.write(f"- Source B is the most realistic (real intrabar wicks)\n")
    f.write(f"- Source C adjusted from A, not B (methodological issue)\n")
    f.write(f"- **{periods_failing_minus30} periods** exceed -30% MDD threshold in Source B\n")
    f.write(f"- Mean gap between intrabar and sweep: {mean_mdd_gap}% (intrabar worse)\n\n")

    f.write("### Decision Criteria\n")
    f.write("- **Phase 5 Approval Gate**: MDD < -30% for all periods (or most periods acceptable)\n")
    f.write(f"- **Current Status**: {periods_failing_minus30}/5 periods fail gate\n")
    f.write(f"- **Recommendation**: {summary['recommendation']}\n")

print(f"✓ JSON written to: {json_path}")
print(f"✓ Markdown written to: {md_path}")
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Mean Intrabar MDD: {mean_intrabar_mdd}%")
print(f"Worst Intrabar MDD: {worst_intrabar_mdd}%")
print(f"Periods Failing -30% Gate: {periods_failing_minus30}/5")
print(f"Mean MDD Gap: {mean_mdd_gap}%")
print(f"\n{summary['recommendation']}")
print("="*60)
