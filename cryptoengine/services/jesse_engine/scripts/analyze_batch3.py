#!/usr/bin/env python3
"""Analyze Batch 3 V2 backtest results and generate reports"""

import json
import os
from pathlib import Path
from datetime import datetime

def load_stats(filepath):
    """Load stats.json safely"""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def get_decision_tier(stats, bnh_sharpe=0.9537):
    """Determine TIER based on decision logic"""
    if not stats:
        return "ERROR", []

    checks = [
        stats.get("checks", {}).get("CAGR ≥ 5%", False),
        stats.get("checks", {}).get("Sharpe ≥ 0.5", False),
        stats.get("checks", {}).get("MDD ≥ -30%", False),
        stats.get("checks", {}).get("Trades ≥ 30", False),
        stats.get("checks", {}).get("WinRate ≥ 35%", False),
        stats.get("checks", {}).get("ProfitFactor ≥ 1.2", False),
    ]

    verdict = stats.get("verdict", "FAIL")
    sharpe = stats.get("sharpe_ratio", 0)
    pf = stats.get("profit_factor", 0)
    trades = stats.get("total_trades", 0)
    check_count = sum(checks)

    if verdict == "PASS" and sharpe >= bnh_sharpe * 0.7:
        tier = "TIER A"
    elif check_count >= 4 and pf >= 1.0 and trades >= 100:
        tier = "TIER B"
    else:
        tier = "TIER C"

    return tier, checks

def create_decision_md(strategy_name, stats_bi, stats_lo, bnh_stats, output_dir):
    """Create decision.md for a strategy"""

    bnh_sharpe = bnh_stats.get("sharpe_ratio", 0)
    bnh_cagr = bnh_stats.get("cagr_pct", 0)

    # Get stats
    def get_val(s, key, default=0):
        return s.get(key, default) if s else default

    bi_cagr = get_val(stats_bi, "cagr_pct")
    bi_sharpe = get_val(stats_bi, "sharpe_ratio")
    bi_mdd = get_val(stats_bi, "max_drawdown_pct")
    bi_trades = get_val(stats_bi, "total_trades")
    bi_wr = get_val(stats_bi, "win_rate_pct")
    bi_pf = get_val(stats_bi, "profit_factor")
    bi_verdict = get_val(stats_bi, "verdict", "UNKNOWN")

    lo_cagr = get_val(stats_lo, "cagr_pct")
    lo_sharpe = get_val(stats_lo, "sharpe_ratio")
    lo_mdd = get_val(stats_lo, "max_drawdown_pct")
    lo_trades = get_val(stats_lo, "total_trades")
    lo_wr = get_val(stats_lo, "win_rate_pct")
    lo_pf = get_val(stats_lo, "profit_factor")
    lo_verdict = get_val(stats_lo, "verdict", "UNKNOWN")

    bi_tier, bi_checks = get_decision_tier(stats_bi, bnh_sharpe)
    lo_tier, lo_checks = get_decision_tier(stats_lo, bnh_sharpe)

    content = f"""# {strategy_name} V2 Decision

## Variant Comparison
| 항목 | Bidirectional | Long Only |
|------|---------------|-----------|
| CAGR | {bi_cagr:.2f}% | {lo_cagr:.2f}% |
| Sharpe | {bi_sharpe:.4f} | {lo_sharpe:.4f} |
| MDD | {bi_mdd:.2f}% | {lo_mdd:.2f}% |
| Trades | {int(bi_trades)} | {int(lo_trades)} |
| WinRate | {bi_wr:.2f}% | {lo_wr:.2f}% |
| PF | {bi_pf:.3f} | {lo_pf:.3f} |
| Verdict | {bi_verdict} | {lo_verdict} |
| Tier | {bi_tier} | {lo_tier} |

## vs Buy & Hold (2020-01-01 ~ 2025-12-31)
- BnH CAGR: {bnh_cagr:.2f}%, Sharpe: {bnh_sharpe:.4f}, MDD: -74.58%

## TIER 판정
- **Bidirectional**: {bi_tier}
- **Long Only**: {lo_tier}

## 근거
Bidirectional: Sharpe {bi_sharpe:.4f} vs BnH threshold {bnh_sharpe * 0.7:.4f}
Long Only: Sharpe {lo_sharpe:.4f} vs BnH threshold {bnh_sharpe * 0.7:.4f}
"""

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "decision.md", "w") as f:
        f.write(content)

def main():
    base_dir = Path(__file__).parent.parent.parent.parent.parent / 'backtest-results' / 'data' / 'batch_3'

    # Load BnH stats
    bnh_stats = load_stats(base_dir / "buy_and_hold" / "stats.json")
    if not bnh_stats:
        print("ERROR: BnH stats not found")
        return

    print(f"BnH stats loaded: CAGR={bnh_stats.get('cagr_pct', 0):.2f}%, Sharpe={bnh_stats.get('sharpe_ratio', 0):.4f}")

    strategies = {
        "TrendType": {
            "dir": "trendtype",
            "full_name": "TrendTypeStrategy",
        },
        "SupertrendTrendType": {
            "dir": "supertrend_trendtype",
            "full_name": "SupertrendTrendTypeStrategy",
        },
        "TradeIQ220323": {
            "dir": "tradeiq_220323",
            "full_name": "TradeIQ220323Strategy",
        },
    }

    results = {}
    tier_count = {"TIER A": 0, "TIER B": 0, "TIER C": 0, "ERROR": 0}
    pass_count = 0

    # Load and analyze each strategy
    for key, info in strategies.items():
        strat_dir = base_dir / info["dir"]
        bi_stats = load_stats(strat_dir / "bidirectional" / "stats.json")
        lo_stats = load_stats(strat_dir / "long_only" / "stats.json")

        results[key] = {
            "bidirectional": bi_stats,
            "long_only": lo_stats,
        }

        # Create decision.md if both variants have stats
        if bi_stats and lo_stats:
            create_decision_md(info["full_name"], bi_stats, lo_stats, bnh_stats, strat_dir)
            print(f"Created decision.md for {key}")

            # Count tiers and verdicts
            for variant, stats in [("Bidirectional", bi_stats), ("Long Only", lo_stats)]:
                if stats:
                    tier, _ = get_decision_tier(stats, bnh_stats.get("sharpe_ratio", 0.9537))
                    verdict = stats.get("verdict", "UNKNOWN")
                    if verdict == "PASS":
                        pass_count += 1
                    tier_count[tier] += 1
        elif bi_stats or lo_stats:
            print(f"WARNING: Only one variant completed for {key}")
            # Still try to count what we have
            for variant, stats in [("Bidirectional", bi_stats), ("Long Only", lo_stats)]:
                if stats:
                    tier, _ = get_decision_tier(stats, bnh_stats.get("sharpe_ratio", 0.9537))
                    verdict = stats.get("verdict", "UNKNOWN")
                    if verdict == "PASS":
                        pass_count += 1
                    tier_count[tier] += 1

    # Create SUMMARY.md
    summary_content = f"""# Batch 3 (Hybrid/Regime) V2 Summary
생성일: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
기간: 2020-01-01 ~ 2025-12-31 (6년)
설정: 10,000 USDT | 1x leverage

## 결과 표
| 전략 | Variant | CAGR | Sharpe | MDD | Trades | WinRate | PF | Verdict | Tier |
|------|---------|------|--------|-----|--------|---------|-----|---------|------|
"""

    for key, info in strategies.items():
        bi = results[key]["bidirectional"]
        lo = results[key]["long_only"]

        for variant, stats in [("Bidirectional", bi), ("Long Only", lo)]:
            if stats:
                tier, _ = get_decision_tier(stats, bnh_stats.get("sharpe_ratio", 0.9537))
                verdict = stats.get("verdict", "UNKNOWN")

                summary_content += f"| {key} | {variant} | {stats.get('cagr_pct', 0):.2f}% | {stats.get('sharpe_ratio', 0):.4f} | {stats.get('max_drawdown_pct', 0):.2f}% | {int(stats.get('total_trades', 0))} | {stats.get('win_rate_pct', 0):.2f}% | {stats.get('profit_factor', 0):.3f} | {verdict} | {tier} |\n"
            else:
                summary_content += f"| {key} | {variant} | ERROR | ERROR | ERROR | ERROR | ERROR | ERROR | ERROR | ERROR |\n"

    summary_content += f"""
## vs Buy & Hold
- BnH CAGR: {bnh_stats.get('cagr_pct', 0):.2f}%, Sharpe: {bnh_stats.get('sharpe_ratio', 0):.4f}, MDD: -74.58%

## 카테고리 결론 (Hybrid/Regime)
PASS 수: {pass_count}/6
Tier A: {tier_count['TIER A']}, Tier B: {tier_count['TIER B']}, Tier C: {tier_count['TIER C']}, ERROR: {tier_count['ERROR']}
"""

    with open(base_dir / "SUMMARY.md", "w") as f:
        f.write(summary_content)

    print(f"Created SUMMARY.md")
    print(f"Results: PASS={pass_count}/6, A={tier_count['TIER A']}, B={tier_count['TIER B']}, C={tier_count['TIER C']}, E={tier_count['ERROR']}")

if __name__ == "__main__":
    main()
