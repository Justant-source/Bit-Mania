#!/usr/bin/env python3
"""
Phase 5 Go/No-Go Decision Report Generator

Combines:
1. Adjusted costs (Phase B): adjusted_stats.json
2. Intrabar MDD (Phase C): stats.json per period
3. Original sweep results: summary.json

Applies 5 Phase 5 gates and produces final recommendation.

Output: /home/justant/Data/Bit-Mania/.result/08_PHASE5_RECOMMENDATION.md
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import statistics

# ============================================================================
# Constants
# ============================================================================

BASELINE_CAGR = 34.87
BASELINE_MDD = -4.52
BASELINE_SHARPE = 3.583

GATE_CAGR_MIN = 34.87
GATE_MDD_MIN = -35.0
GATE_TRADES_MIN = 30
GATE_SHARPE_MIN = 0.5

RESULTS_BASE = Path("/home/justant/Data/Bit-Mania/backtest/results")
OUTPUT_DIR = Path("/home/justant/Data/Bit-Mania/.result")
OUTPUT_FILE = OUTPUT_DIR / "08_PHASE5_RECOMMENDATION.md"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Data Loading
# ============================================================================

def load_adjusted_stats(strat: str, tf: str, variant: str, combo: int) -> Optional[Dict]:
    """Load adjusted_stats.json from Phase B results."""
    path = (
        RESULTS_BASE / "adjusted_costs" / strat / tf / variant / f"combo_{combo}" / "adjusted_stats.json"
    )
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def load_intrabar_stats(strat: str, tf: str, variant: str, combo: int) -> Dict[str, Dict]:
    """Load intrabar stats for all periods. Returns {period: stats_dict}."""
    results = {}
    intrabar_dir = RESULTS_BASE / "intrabar" / strat / tf / variant / f"combo_{combo}"

    if not intrabar_dir.exists():
        return results

    for period_dir in sorted(intrabar_dir.iterdir()):
        if not period_dir.is_dir():
            continue
        stats_file = period_dir / "stats.json"
        if stats_file.exists():
            try:
                with open(stats_file) as f:
                    results[period_dir.name] = json.load(f)
            except Exception as e:
                print(f"Error loading {stats_file}: {e}")

    return results


def load_original_sweep(strat: str, tf: str, variant: str, combo: int, version: str = "v3") -> Optional[Dict]:
    """Load original sweep summary.json."""
    path = RESULTS_BASE / "param_sweep" / version / strat / tf / variant / f"combo_{combo}" / "summary.json"
    if not path.exists():
        # Try v2 if v3 doesn't exist
        if version == "v3":
            return load_original_sweep(strat, tf, variant, combo, version="v2")
        return None

    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def discover_survivors() -> List[Tuple[str, str, str, int]]:
    """Discover all survivor combos (strat, tf, variant, combo)."""
    survivors = []

    adjusted_costs_dir = RESULTS_BASE / "adjusted_costs"
    if not adjusted_costs_dir.exists():
        return survivors

    for strat_dir in adjusted_costs_dir.iterdir():
        if not strat_dir.is_dir():
            continue
        strat = strat_dir.name

        for tf_dir in strat_dir.iterdir():
            if not tf_dir.is_dir():
                continue
            tf = tf_dir.name

            for variant_dir in tf_dir.iterdir():
                if not variant_dir.is_dir():
                    continue
                variant = variant_dir.name

                for combo_dir in variant_dir.iterdir():
                    if not combo_dir.is_dir() or not combo_dir.name.startswith("combo_"):
                        continue
                    try:
                        combo = int(combo_dir.name.split("_")[1])
                        survivors.append((strat, tf, variant, combo))
                    except (ValueError, IndexError):
                        pass

    return sorted(survivors)


# ============================================================================
# Scoring and Gate Logic
# ============================================================================

def compute_intrabar_mdd_mean(intrabar_stats: Dict[str, Dict]) -> Tuple[Optional[float], int]:
    """Compute mean intrabar MDD across periods.

    Returns:
        (mean_mdd, num_periods_with_data)
    """
    mdds = []
    for period, stats in intrabar_stats.items():
        if "max_drawdown_pct" in stats:
            mdds.append(stats["max_drawdown_pct"])

    if not mdds:
        return None, 0

    return statistics.mean(mdds), len(mdds)


def compute_original_metrics(original_sweep: Optional[Dict]) -> Tuple[Optional[float], Optional[float], int]:
    """Extract mean sharpe and trades from original sweep.

    Returns:
        (mean_sharpe, mean_trades, num_periods)
    """
    if not original_sweep or "periods" not in original_sweep:
        return None, None, 0

    periods = original_sweep["periods"]
    sharpes = []
    trades_list = []

    for period_key, period_data in periods.items():
        # summary.json uses 'sharpe' and 'trades'; adjusted_stats uses 'original_sharpe'/'adj_sharpe'
        sharpe_val = period_data.get("sharpe") or period_data.get("adj_sharpe") or period_data.get("original_sharpe")
        if sharpe_val is not None:
            sharpes.append(float(sharpe_val))
        trades_val = period_data.get("trades") or period_data.get("total_trades")
        if trades_val is not None:
            trades_list.append(float(trades_val))

    mean_sharpe = statistics.mean(sharpes) if sharpes else None
    mean_trades = statistics.mean(trades_list) if trades_list else None
    num_periods = len(periods)

    return mean_sharpe, mean_trades, num_periods


def check_adjusted_cagr_all_periods_positive(adjusted_stats: Dict) -> bool:
    """Check if all 5 periods have positive adjusted CAGR."""
    if "periods" not in adjusted_stats:
        return False

    periods = adjusted_stats["periods"]
    for period_key in ["p0", "p1", "p2", "p3", "p4"]:
        if period_key not in periods:
            return False
        if periods[period_key].get("adj_cagr", -999) <= 0:
            return False

    return True


def evaluate_candidate(strat: str, tf: str, variant: str, combo: int) -> Dict:
    """Evaluate one candidate against Phase 5 gates.

    Returns evaluation dict with scores, gate results, and notes.
    """
    result = {
        "strat": strat,
        "tf": tf,
        "variant": variant,
        "combo": combo,
        "label": f"{strat}/{tf}/{variant}/combo_{combo}",
    }

    # Load data
    adjusted_stats = load_adjusted_stats(strat, tf, variant, combo)
    if not adjusted_stats:
        result["passes_all_gates"] = False
        result["notes"] = "No adjusted_stats.json found"
        return result

    intrabar_stats = load_intrabar_stats(strat, tf, variant, combo)
    original_sweep = load_original_sweep(strat, tf, variant, combo)

    # Extract metrics
    result["adjusted_cagr"] = adjusted_stats.get("adjusted_score")

    intrabar_mdd, intrabar_periods = compute_intrabar_mdd_mean(intrabar_stats)
    result["intrabar_mdd"] = intrabar_mdd
    result["intrabar_periods_available"] = intrabar_periods

    mean_sharpe, mean_trades, num_periods = compute_original_metrics(original_sweep)
    result["mean_sharpe"] = mean_sharpe
    result["mean_trades"] = mean_trades
    result["num_periods"] = num_periods

    # Check each gate
    gates_passed = []
    gates_failed = []

    # Gate 1: CAGR >= 34.87%
    if result["adjusted_cagr"] is not None:
        if result["adjusted_cagr"] >= GATE_CAGR_MIN:
            gates_passed.append("CAGR")
        else:
            gates_failed.append(("CAGR", f"{result['adjusted_cagr']:.2f}% < {GATE_CAGR_MIN}%"))
    else:
        gates_failed.append(("CAGR", "no adjusted_cagr"))

    # Gate 2: Intrabar MDD >= -35%
    if intrabar_mdd is not None:
        if intrabar_mdd >= GATE_MDD_MIN:
            gates_passed.append("Intrabar MDD")
        else:
            gates_failed.append(("Intrabar MDD", f"{intrabar_mdd:.2f}% < {GATE_MDD_MIN}%"))
    else:
        gates_failed.append(("Intrabar MDD", "no intrabar data"))

    # Gate 3: All 5 periods have positive adj_cagr
    if check_adjusted_cagr_all_periods_positive(adjusted_stats):
        gates_passed.append("5-period consistency")
    else:
        gates_failed.append(("5-period consistency", "not all periods positive"))

    # Gate 4: Mean trades >= 30 per period
    if mean_trades is not None:
        if mean_trades >= GATE_TRADES_MIN:
            gates_passed.append("Trades")
        else:
            gates_failed.append(("Trades", f"{mean_trades:.1f} < {GATE_TRADES_MIN}"))
    else:
        gates_failed.append(("Trades", "no trade data"))

    # Gate 5: Mean sharpe >= 0.5
    if mean_sharpe is not None:
        if mean_sharpe >= GATE_SHARPE_MIN:
            gates_passed.append("Sharpe")
        else:
            gates_failed.append(("Sharpe", f"{mean_sharpe:.2f} < {GATE_SHARPE_MIN}"))
    else:
        gates_failed.append(("Sharpe", "no sharpe data"))

    result["gates_passed"] = gates_passed
    result["gates_failed"] = gates_failed
    result["passes_all_gates"] = len(gates_failed) == 0

    if result["passes_all_gates"]:
        result["notes"] = "All gates passed"
    else:
        result["notes"] = f"Failed: {', '.join(g[0] for g in gates_failed)}"

    return result


# ============================================================================
# Report Generation
# ============================================================================

def generate_markdown_report(evaluations: List[Dict]) -> str:
    """Generate the Phase 5 recommendation markdown report."""

    # Count passes and failures
    passed = [e for e in evaluations if e["passes_all_gates"]]
    failed = [e for e in evaluations if not e["passes_all_gates"]]

    # Count failure reasons
    failure_reasons = {}
    for e in failed:
        for gate_name, _ in e["gates_failed"]:
            failure_reasons[gate_name] = failure_reasons.get(gate_name, 0) + 1

    # Determine conclusion
    if passed:
        conclusion = "GO"
        conclusion_text = f"**[GO]**: {len(passed)}개 후보가 모든 Phase 5 게이트 통과"
    else:
        conclusion = "NO-GO"
        conclusion_text = f"**[NO-GO]**: 모든 후보가 게이트 미달 — Phase 4 (funding-arb) 유지"

    # Build report
    lines = []
    lines.append("# 08. Phase 5 Go/No-Go 최종 권고")
    lines.append("")
    lines.append(f"> 작성일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("> 방법론: 3단계 보정 (원본 → 비용보정 → Intrabar MDD)")
    lines.append(f"> 비교 baseline: fa80_lev5_r30 (+{BASELINE_CAGR}% CAGR, {BASELINE_MDD}% MDD, {BASELINE_SHARPE} Sharpe)")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 결론")
    lines.append("")
    lines.append(conclusion_text)
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Phase 5 게이트 기준")
    lines.append("")
    lines.append("| 게이트 | 기준 | 근거 |")
    lines.append("|---|---|---|")
    lines.append(f"| CAGR ≥ {GATE_CAGR_MIN}% | 조정CAGR (비용보정 후) | funding-arb baseline 초과 |")
    lines.append(f"| MDD ≥ {GATE_MDD_MIN}% | Intrabar 평균 MDD | wick 손절 포함 실측 |")
    lines.append("| 5구간 모두 양수 CAGR | 모든 구간 adj_cagr > 0 | 결과 일관성 |")
    lines.append(f"| trades ≥ {GATE_TRADES_MIN}/구간 | 원본 sweep 기준 | 통계적 신뢰도 |")
    lines.append(f"| Sharpe ≥ {GATE_SHARPE_MIN} | 원본 sweep 기준 | 위험대비 수익 최소 기준 |")
    lines.append("")

    # Passed candidates
    if passed:
        lines.append(f"## 최종 통과 후보 ({len(passed)}개)")
        lines.append("")
        lines.append("| 순위 | 전략/TF/방향/Combo | 조정CAGR | Intrabar MDD | Sharpe | trades/구간 |")
        lines.append("|---|---|---|---|---|---|")

        # Sort by adjusted CAGR descending
        passed_sorted = sorted(passed, key=lambda x: x.get("adjusted_cagr") or -999, reverse=True)

        for i, e in enumerate(passed_sorted, 1):
            cagr_str = f"{e['adjusted_cagr']:.2f}%" if e["adjusted_cagr"] is not None else "—"
            mdd_str = f"{e['intrabar_mdd']:.2f}%" if e["intrabar_mdd"] is not None else "—"
            sharpe_str = f"{e['mean_sharpe']:.2f}" if e["mean_sharpe"] is not None else "—"
            trades_str = f"{e['mean_trades']:.1f}" if e["mean_trades"] is not None else "—"

            lines.append(
                f"| {i} | {e['label']} | {cagr_str} | {mdd_str} | {sharpe_str} | {trades_str} |"
            )
        lines.append("")

    # Summary of all evaluations
    lines.append(f"## 평가 요약")
    lines.append("")
    lines.append(f"- **총 후보**: {len(evaluations)}")
    lines.append(f"- **통과**: {len(passed)}")
    lines.append(f"- **탈락**: {len(failed)}")
    lines.append("")

    if failure_reasons:
        lines.append("## 탈락 분석")
        lines.append("")
        lines.append("| 탈락 원인 | 건수 |")
        lines.append("|---|---|")
        for reason in sorted(failure_reasons.keys()):
            lines.append(f"| {reason} | {failure_reasons[reason]} |")
        lines.append("")

    # Detailed failure breakdown (top 10 failed candidates for context)
    if failed:
        lines.append("### 탈락 후보 (상위 10개, 조정CAGR 기준)")
        lines.append("")
        failed_sorted = sorted(failed, key=lambda x: x.get("adjusted_cagr") or -999, reverse=True)
        for e in failed_sorted[:10]:
            cagr_str = f"{e['adjusted_cagr']:.2f}%" if e["adjusted_cagr"] is not None else "—"
            mdd_str = f"{e['intrabar_mdd']:.2f}%" if e["intrabar_mdd"] is not None else "—"
            lines.append(f"- **{e['label']}** | CAGR={cagr_str}, MDD={mdd_str} | {e['notes']}")
        lines.append("")

    # Recommendations
    lines.append("## 권고 행동")
    lines.append("")

    if passed:
        lines.append("### GO 분석 (Phase 5 진입)")
        lines.append("")
        top_candidate = passed_sorted[0]
        lines.append(f"최상위 후보: **{top_candidate['label']}**")
        lines.append(f"- 조정CAGR: {top_candidate['adjusted_cagr']:.2f}%")
        lines.append(f"- Intrabar MDD: {top_candidate['intrabar_mdd']:.2f}%")
        lines.append(f"- Sharpe: {top_candidate['mean_sharpe']:.2f}")
        lines.append("")
        lines.append("**다음 단계**:")
        lines.append("1. `scripts/phase5_preflight.py` 실행하여 8개 사전검증 항목 확인")
        lines.append("2. `scripts/switch_to_mainnet.py` 실행 — 메인넷 Phase 5 진입")
        lines.append("3. 초기 자본: $200 USDT, 레버리지 1x")
        lines.append("4. 48시간 모니터링 후 정상 시 레버리지 단계별 상향")
    else:
        lines.append("### NO-GO 분석 (Phase 4 유지)")
        lines.append("")
        lines.append("**권고사항**:")
        lines.append("1. 현재 funding-arb (fa80_lev5_r30) 유지 — Phase 4 계속")
        lines.append(f"   - CAGR +{BASELINE_CAGR}%, MDD {BASELINE_MDD}%, Sharpe {BASELINE_SHARPE}")
        lines.append("2. 다음 검토 시점: param_sweep_v4 결과 확인 후")
        lines.append("3. 혹은 새로운 전략 개발 및 백테스트 권고")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 재현 명령")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 /home/justant/Data/Bit-Mania/backtest/scripts/analysis/generate_phase5_report.py")
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


# ============================================================================
# Main
# ============================================================================

def main():
    """Main entry point."""
    print(f"Phase 5 Go/No-Go Report Generator")
    print(f"=" * 60)
    print(f"Output: {OUTPUT_FILE}")
    print(f"Baseline: CAGR {BASELINE_CAGR}% | MDD {BASELINE_MDD}% | Sharpe {BASELINE_SHARPE}")
    print(f"")

    # Discover all survivors
    print("Discovering survivors...")
    survivors = discover_survivors()
    print(f"Found {len(survivors)} survivors in adjusted_costs/")
    print()

    if not survivors:
        print("ERROR: No survivors found in adjusted_costs/. Check directory structure.")
        return

    # Evaluate each candidate
    print("Evaluating candidates against Phase 5 gates...")
    evaluations = []
    for i, (strat, tf, variant, combo) in enumerate(survivors, 1):
        if i % 20 == 0:
            print(f"  ... {i}/{len(survivors)}")
        result = evaluate_candidate(strat, tf, variant, combo)
        evaluations.append(result)

    print(f"Evaluated {len(evaluations)} candidates")
    print()

    # Count results
    passed = [e for e in evaluations if e["passes_all_gates"]]
    failed = [e for e in evaluations if not e["passes_all_gates"]]

    print(f"Results:")
    print(f"  Passed (GO): {len(passed)}")
    print(f"  Failed (NO-GO): {len(failed)}")
    print()

    if passed:
        print("Top 5 candidates (by adjusted CAGR):")
        passed_sorted = sorted(passed, key=lambda x: x.get("adjusted_cagr") or -999, reverse=True)
        for e in passed_sorted[:5]:
            print(f"  {e['label']}: {e['adjusted_cagr']:.2f}% CAGR, {e['intrabar_mdd']:.2f}% MDD")
        print()

    # Generate report
    print("Generating markdown report...")
    markdown = generate_markdown_report(evaluations)

    # Write report
    OUTPUT_FILE.write_text(markdown)
    print(f"Report written to: {OUTPUT_FILE}")
    print()
    print("=" * 60)
    print("Complete!")


if __name__ == "__main__":
    main()
