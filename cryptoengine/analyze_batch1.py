#!/usr/bin/env python3
"""
Analyze Batch 1 backtest results and generate decision.md files
"""
import json
import os
from pathlib import Path

# BnH benchmark
BNH_STATS_PATH = ".result/batch_1/buy_and_hold/stats.json"

def load_stats(path):
    """Load stats.json file"""
    if not os.path.exists(path):
        return None
    with open(path, 'r') as f:
        return json.load(f)

def get_bnh_sharpe():
    """Get BnH Sharpe ratio for tier comparison"""
    stats = load_stats(BNH_STATS_PATH)
    if stats:
        return stats.get('sharpe_ratio', 0)
    return 0

def create_variant_comparison_table(strat_name, bidirectional_stats, longonly_stats):
    """Create markdown table comparing variants"""
    table = "| 항목 | Bidirectional | Long Only |\n"
    table += "|------|---------------|----------|\n"

    if bidirectional_stats:
        cagr_bid = bidirectional_stats.get('cagr_pct', 0)
        sharpe_bid = bidirectional_stats.get('sharpe_ratio', 0)
        mdd_bid = bidirectional_stats.get('max_drawdown_pct', 0)
        trades_bid = bidirectional_stats.get('total_trades', 0)
        wr_bid = bidirectional_stats.get('win_rate_pct', 0)
        pf_bid = bidirectional_stats.get('profit_factor', 0)
        verdict_bid = bidirectional_stats.get('verdict', 'N/A')
    else:
        cagr_bid = sharpe_bid = mdd_bid = trades_bid = wr_bid = pf_bid = 0
        verdict_bid = "ERROR"

    if longonly_stats:
        cagr_lo = longonly_stats.get('cagr_pct', 0)
        sharpe_lo = longonly_stats.get('sharpe_ratio', 0)
        mdd_lo = longonly_stats.get('max_drawdown_pct', 0)
        trades_lo = longonly_stats.get('total_trades', 0)
        wr_lo = longonly_stats.get('win_rate_pct', 0)
        pf_lo = longonly_stats.get('profit_factor', 0)
        verdict_lo = longonly_stats.get('verdict', 'N/A')
    else:
        cagr_lo = sharpe_lo = mdd_lo = trades_lo = wr_lo = pf_lo = 0
        verdict_lo = "ERROR"

    table += f"| CAGR | {cagr_bid:.2f}% | {cagr_lo:.2f}% |\n"
    table += f"| Sharpe | {sharpe_bid:.3f} | {sharpe_lo:.3f} |\n"
    table += f"| MDD | {mdd_bid:.2f}% | {mdd_lo:.2f}% |\n"
    table += f"| Trades | {int(trades_bid)} | {int(trades_lo)} |\n"
    table += f"| WinRate | {wr_bid:.1f}% | {wr_lo:.1f}% |\n"
    table += f"| PF | {pf_bid:.2f} | {pf_lo:.2f} |\n"
    table += f"| Verdict | {verdict_bid} | {verdict_lo} |\n"

    return table, bidirectional_stats, longonly_stats

def determine_tier(stats, bnh_sharpe, strat_name):
    """Determine tier based on criteria"""
    if not stats:
        return "ERROR", "파일 없음"

    verdict = stats.get('verdict', 'N/A')
    checks = stats.get('checks', {})
    cagr = stats.get('cagr_pct', 0)
    sharpe = stats.get('sharpe_ratio', 0)
    mdd = stats.get('max_drawdown_pct', 0)
    trades = stats.get('total_trades', 0)
    wr = stats.get('win_rate_pct', 0)
    pf = stats.get('profit_factor', 0)

    # TIER A: verdict=PASS AND sharpe >= bnh_sharpe * 0.7
    if verdict == "PASS" and sharpe >= (bnh_sharpe * 0.7):
        reason = f"PASS verdict + Sharpe {sharpe:.3f} >= BnH 0.7x ({bnh_sharpe*0.7:.3f})"
        return "TIER A", reason

    # TIER B: 6개 체크 중 4개 이상 True AND profit_factor >= 1.0 AND total_trades >= 100
    true_checks = sum(1 for v in checks.values() if v is True)
    if true_checks >= 4 and pf >= 1.0 and trades >= 100:
        reason = f"6개 체크 중 {true_checks}개 PASS + PF {pf:.2f} + {int(trades)}개 거래"
        return "TIER B", reason

    # TIER C: 나머지
    reason = f"TIER A/B 조건 미충족 (checks: {true_checks}/6, PF: {pf:.2f}, trades: {int(trades)})"
    return "TIER C", reason

def create_decision_md(strat_name, strat_dir, bidirectional_path, longonly_path, bnh_stats):
    """Create decision.md file for a strategy"""
    bid_stats = load_stats(bidirectional_path)
    lo_stats = load_stats(longonly_path)

    bnh_sharpe = bnh_stats.get('sharpe_ratio', 0)
    bnh_cagr = bnh_stats.get('cagr_pct', 0)
    bnh_mdd = bnh_stats.get('max_drawdown_pct', 0)

    table, best_bid, best_lo = create_variant_comparison_table(strat_name, bid_stats, lo_stats)

    # Determine tier for each variant
    tier_bid, reason_bid = determine_tier(bid_stats, bnh_sharpe, strat_name)
    tier_lo, reason_lo = determine_tier(lo_stats, bnh_sharpe, strat_name)

    # Choose better variant
    if bid_stats and lo_stats:
        bid_sharpe = bid_stats.get('sharpe_ratio', 0)
        lo_sharpe = lo_stats.get('sharpe_ratio', 0)
        better_variant = "Bidirectional" if bid_sharpe >= lo_sharpe else "Long Only"
        better_tier = tier_bid if bid_sharpe >= lo_sharpe else tier_lo
    else:
        better_variant = "Unknown"
        better_tier = "ERROR"

    content = f"""# {strat_name} V2 Decision

생성일: 2026-05-09
기간: 2020-01-01 ~ 2025-12-31 (6년)
설정: 10,000 USDT | 1x leverage

## Variant 비교
{table}

## vs Buy & Hold (2020-01-01 ~ 2025-12-31)
- BnH CAGR: {bnh_cagr:.2f}%
- BnH Sharpe: {bnh_sharpe:.4f}
- BnH MDD: {bnh_mdd:.2f}%

## TIER 판정

### Bidirectional
- **{tier_bid}**
- 근거: {reason_bid}

### Long Only
- **{tier_lo}**
- 근거: {reason_lo}

## 권장 변형
**{better_variant}** → {better_tier}

---
*생성: 2026-05-09 | 분석 대상 기간: 2020-01-01 ~ 2025-12-31*
"""

    # Write to file
    output_path = f".result/batch_1/{strat_dir}/decision.md"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(content)

    print(f"[{strat_name}] Created {output_path}")
    return better_variant, better_tier

def create_summary_md(all_results):
    """Create SUMMARY.md file"""
    summary = """# Batch 1 (Mean Reversion) V2 Summary

생성일: 2026-05-09
기간: 2020-01-01 ~ 2025-12-31 (6년)
설정: 10,000 USDT | 1x leverage

## 결과 표
| 전략 | Variant | CAGR | Sharpe | MDD | Trades | WinRate | PF | Verdict | Tier |
|------|---------|------|--------|-----|--------|---------|-----|---------|------|
"""

    # Load BnH
    bnh_stats = load_stats(BNH_STATS_PATH)
    bnh_cagr = bnh_stats.get('cagr_pct', 0) if bnh_stats else 0
    bnh_sharpe = bnh_stats.get('sharpe_ratio', 0) if bnh_stats else 0
    bnh_mdd = bnh_stats.get('max_drawdown_pct', 0) if bnh_stats else 0

    # Add all results
    for strat, results in sorted(all_results.items()):
        for variant, stats in results.items():
            if stats:
                cagr = stats.get('cagr_pct', 0)
                sharpe = stats.get('sharpe_ratio', 0)
                mdd = stats.get('max_drawdown_pct', 0)
                trades = int(stats.get('total_trades', 0))
                wr = stats.get('win_rate_pct', 0)
                pf = stats.get('profit_factor', 0)
                verdict = stats.get('verdict', 'N/A')

                # Determine tier
                tier, _ = determine_tier(stats, bnh_sharpe, strat)

                summary += f"| {strat} | {variant} | {cagr:.2f}% | {sharpe:.3f} | {mdd:.2f}% | {trades} | {wr:.1f}% | {pf:.2f} | {verdict} | {tier} |\n"

    summary += f"\n## vs Buy & Hold\n"
    summary += f"- BnH CAGR: {bnh_cagr:.2f}%\n"
    summary += f"- BnH Sharpe: {bnh_sharpe:.4f}\n"
    summary += f"- BnH MDD: {bnh_mdd:.2f}%\n"

    # Count tiers
    tier_a = sum(1 for strat, res in all_results.items() for var, stats in res.items() if stats and determine_tier(stats, bnh_sharpe, strat)[0] == "TIER A")
    tier_b = sum(1 for strat, res in all_results.items() for var, stats in res.items() if stats and determine_tier(stats, bnh_sharpe, strat)[0] == "TIER B")
    tier_c = sum(1 for strat, res in all_results.items() for var, stats in res.items() if stats and determine_tier(stats, bnh_sharpe, strat)[0] == "TIER C")

    summary += f"\n## 카테고리 결론 (Mean Reversion)\n"
    summary += f"PASS 수: {sum(1 for s, r in all_results.items() for v, st in r.items() if st and st.get('verdict') == 'PASS')}/6\n"
    summary += f"Tier A: {tier_a}, Tier B: {tier_b}, Tier C: {tier_c}\n"

    with open(".result/batch_1/SUMMARY.md", 'w') as f:
        f.write(summary)

    print(f"Created .result/batch_1/SUMMARY.md")

def main():
    """Main analysis function"""
    bnh_stats = load_stats(BNH_STATS_PATH)
    if not bnh_stats:
        print("ERROR: BnH stats not found!")
        return

    print(f"BnH Sharpe: {bnh_stats.get('sharpe_ratio', 0):.4f}")

    all_results = {}

    # Analyze each strategy
    strategies = [
        ("BBPB", "bbpb"),
        ("BBWP", "bbwp"),
        ("Stoch", "stoch"),
    ]

    for strat_name, strat_dir in strategies:
        bid_path = f".result/batch_1/{strat_dir}/bidirectional/stats.json"
        lo_path = f".result/batch_1/{strat_dir}/long_only/stats.json"

        print(f"\nAnalyzing {strat_name}...")

        bid_stats = load_stats(bid_path)
        lo_stats = load_stats(lo_path)

        if not bid_stats or not lo_stats:
            print(f"  WARNING: Missing stats for {strat_name}")
            all_results[strat_name] = {
                "bidirectional": bid_stats,
                "long_only": lo_stats
            }
            continue

        # Create decision.md
        better_variant, better_tier = create_decision_md(strat_name, strat_dir, bid_path, lo_path, bnh_stats)

        all_results[strat_name] = {
            "bidirectional": bid_stats,
            "long_only": lo_stats
        }

    # Create SUMMARY.md
    print(f"\nCreating summary...")
    create_summary_md(all_results)

    print(f"\n✓ Analysis complete!")

if __name__ == "__main__":
    main()
