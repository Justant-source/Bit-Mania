#!/usr/bin/env python3
"""
Generate intrabar MDD verification report.

Reads completed intrabar backtest results and original param_sweep summaries,
computes MDD inflation (wick-based vs close-only), and generates verification report.

Output: /home/justant/Data/Bit-Mania/.result/07_INTRABAR_VERIFICATION.md
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import statistics


# =============================================================================
# CONFIG
# =============================================================================

HOST_BASE = Path("/home/justant/Data/Bit-Mania")
INTRABAR_BASE = HOST_BASE / "backtest" / "results" / "intrabar"
PARAM_SWEEP_BASE = HOST_BASE / "backtest" / "results" / "param_sweep"
ADJUSTED_COSTS_BASE = HOST_BASE / "backtest" / "results" / "adjusted_costs"
OUTPUT_DIR = HOST_BASE / ".result"
OUTPUT_FILE = OUTPUT_DIR / "07_INTRABAR_VERIFICATION.md"

# The 10 baseline-exceeding combos from param_sweep (all supertrend/4h/long_only)
BASELINE_COMBOS = {
    "supertrend": {
        "4h": {
            "long_only": [
                {"idx": 18, "adj_cagr": 38.47},
                {"idx": 1, "adj_cagr": 37.42},
                {"idx": 3, "adj_cagr": 37.36},
                {"idx": 4, "adj_cagr": 36.92},
                {"idx": 17, "adj_cagr": 36.78},
                {"idx": 7, "adj_cagr": 35.92},
                {"idx": 6, "adj_cagr": 35.76},
                {"idx": 2, "adj_cagr": 35.72},
                {"idx": 5, "adj_cagr": 35.66},
                {"idx": 8, "adj_cagr": 35.34},
            ]
        }
    }
}

MDD_GATE = -35.0  # -35% gate threshold


# =============================================================================
# DATA COLLECTION
# =============================================================================

def collect_intrabar_results() -> Dict:
    """Collect all intrabar stats.json files, grouped by (strat, tf, variant, combo, period)."""
    results = defaultdict(list)

    for stats_file in INTRABAR_BASE.rglob("stats.json"):
        try:
            with open(stats_file, "r") as f:
                stats = json.load(f)

            # Extract from path: intrabar/{strat}/{tf}/{variant}/combo_{i}/{period}/stats.json
            parts = stats_file.relative_to(INTRABAR_BASE).parts
            if len(parts) < 5:
                continue

            strat = parts[0]
            tf = parts[1]
            variant = parts[2]
            combo_str = parts[3]  # "combo_N"
            period = parts[4]  # "p0", "p1", etc.

            if not combo_str.startswith("combo_"):
                continue

            combo_idx = int(combo_str.replace("combo_", ""))
            mdd = stats.get("max_drawdown_pct")
            cagr = stats.get("cagr_pct")
            trades = stats.get("total_trades")
            sharpe = stats.get("sharpe_ratio")
            pf = stats.get("profit_factor")

            key = (strat, tf, variant, combo_idx, period)
            results[key].append({
                "mdd": mdd,
                "cagr": cagr,
                "trades": trades,
                "sharpe": sharpe,
                "pf": pf,
                "path": str(stats_file),
            })

        except Exception as e:
            print(f"[WARN] Failed to read {stats_file}: {e}")

    return results


def get_original_mdd(strat: str, tf: str, variant: str, combo_idx: int, period: str) -> Optional[float]:
    """Get original MDD from param_sweep summary.json."""
    # Determine version (v2 or v3) by checking which exists
    for version in ["v3", "v2"]:
        summary_path = PARAM_SWEEP_BASE / version / strat / tf / variant / f"combo_{combo_idx}" / "summary.json"
        if summary_path.exists():
            try:
                with open(summary_path, "r") as f:
                    summary = json.load(f)
                    if "periods" in summary and period in summary["periods"]:
                        return summary["periods"][period].get("mdd")
            except Exception as e:
                print(f"[WARN] Failed to read {summary_path}: {e}")
    return None


def get_adjusted_cagr(strat: str, tf: str, variant: str, combo_idx: int) -> Optional[float]:
    """Get adjusted CAGR from adjusted_costs."""
    adj_path = ADJUSTED_COSTS_BASE / strat / tf / variant / f"combo_{combo_idx}" / "adjusted_stats.json"
    if adj_path.exists():
        try:
            with open(adj_path, "r") as f:
                adj = json.load(f)
                return adj.get("adjusted_score")
        except Exception as e:
            print(f"[WARN] Failed to read {adj_path}: {e}")
    return None


# =============================================================================
# ANALYSIS
# =============================================================================

def compute_mdd_inflation(intrabar_results: Dict) -> Dict:
    """
    Compute MDD inflation for each combo across periods.
    Returns: {(strat, tf, variant, combo, period): {"original_mdd": x, "intrabar_mdd": y, "inflation": z}, ...}
    """
    inflation_data = {}

    for (strat, tf, variant, combo_idx, period), intra_list in intrabar_results.items():
        if not intra_list:
            continue

        intra_stats = intra_list[0]  # Assume one stats.json per combo/period
        intra_mdd = intra_stats.get("mdd")

        orig_mdd = get_original_mdd(strat, tf, variant, combo_idx, period)

        if intra_mdd is None or orig_mdd is None:
            continue

        inflation = intra_mdd - orig_mdd  # Negative means intrabar is worse

        key = (strat, tf, variant, combo_idx, period)
        inflation_data[key] = {
            "original_mdd": orig_mdd,
            "intrabar_mdd": intra_mdd,
            "inflation": inflation,
            "intra_cagr": intra_stats.get("cagr"),
            "intra_trades": intra_stats.get("trades"),
            "intra_sharpe": intra_stats.get("sharpe"),
        }

    return inflation_data


def compute_combo_mean_mdd(inflation_data: Dict) -> Dict:
    """
    For each (strat, tf, variant, combo), compute mean intrabar MDD across all periods.
    Returns: {(strat, tf, variant, combo): {"mean_mdd": x, "n_periods": n, "periods": {...}}, ...}
    """
    combo_means = defaultdict(lambda: {"mdds": [], "periods": {}})

    for (strat, tf, variant, combo, period), data in inflation_data.items():
        key = (strat, tf, variant, combo)
        combo_means[key]["mdds"].append(data["intrabar_mdd"])
        combo_means[key]["periods"][period] = data["intrabar_mdd"]

    result = {}
    for key, agg in combo_means.items():
        if agg["mdds"]:
            result[key] = {
                "mean_mdd": statistics.mean(agg["mdds"]),
                "n_periods": len(agg["mdds"]),
                "periods": agg["periods"],
            }

    return result


def get_baseline_results(combo_means: Dict) -> Dict:
    """Filter baseline-exceeding combos and check if they pass -35% gate."""
    baseline_results = {}

    for (strat, tf, variant, combo), mean_data in combo_means.items():
        if strat not in BASELINE_COMBOS:
            continue
        if tf not in BASELINE_COMBOS[strat]:
            continue
        if variant not in BASELINE_COMBOS[strat][tf]:
            continue

        combo_list = BASELINE_COMBOS[strat][tf][variant]
        combo_info = next((c for c in combo_list if c["idx"] == combo), None)
        if not combo_info:
            continue

        adj_cagr = combo_info["adj_cagr"]
        mean_mdd = mean_data["mean_mdd"]
        passes_gate = mean_mdd >= MDD_GATE  # Not worse than -35%

        baseline_results[(strat, tf, variant, combo)] = {
            "adj_cagr": adj_cagr,
            "mean_mdd": mean_mdd,
            "n_periods": mean_data["n_periods"],
            "passes_gate": passes_gate,
            "periods": mean_data["periods"],
        }

    return baseline_results


# =============================================================================
# GLOBAL STATS
# =============================================================================

def compute_global_stats(inflation_data: Dict) -> Dict:
    """Compute global MDD statistics."""
    if not inflation_data:
        return {}

    original_mdds = [d["original_mdd"] for d in inflation_data.values()]
    intrabar_mdds = [d["intrabar_mdd"] for d in inflation_data.values()]
    inflations = [d["inflation"] for d in inflation_data.values()]

    return {
        "n_results": len(inflation_data),
        "original_mdd_mean": statistics.mean(original_mdds) if original_mdds else None,
        "intrabar_mdd_mean": statistics.mean(intrabar_mdds) if intrabar_mdds else None,
        "inflation_mean": statistics.mean(inflations) if inflations else None,
        "inflation_std": statistics.stdev(inflations) if len(inflations) > 1 else 0,
    }


def compute_tf_stats(inflation_data: Dict) -> Dict:
    """Compute MDD stats grouped by timeframe."""
    by_tf = defaultdict(lambda: {"original": [], "intrabar": [], "inflation": []})

    for (strat, tf, variant, combo, period), data in inflation_data.items():
        by_tf[tf]["original"].append(data["original_mdd"])
        by_tf[tf]["intrabar"].append(data["intrabar_mdd"])
        by_tf[tf]["inflation"].append(data["inflation"])

    result = {}
    for tf, lists in by_tf.items():
        result[tf] = {
            "n": len(lists["original"]),
            "original_mdd_mean": statistics.mean(lists["original"]),
            "intrabar_mdd_mean": statistics.mean(lists["intrabar"]),
            "inflation_mean": statistics.mean(lists["inflation"]),
        }

    return result


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_report(
    inflation_data: Dict,
    combo_means: Dict,
    baseline_results: Dict,
    global_stats: Dict,
    tf_stats: Dict,
) -> str:
    """Generate markdown report."""
    lines = []

    # Header
    lines.append("# 07. Intra-bar MDD 검증 — 1m OHLC Wick 손절 시뮬레이션\n")
    lines.append(f"> 작성일: {datetime.now().strftime('%Y-%m-%d')}")
    lines.append(f"> 대상: 219개 기존게이트 통과 후보 (intrabar 완료: {global_stats.get('n_results', 0)} 결과)")
    lines.append("> 검증 목표: close-only MDD vs 실제 wick 발동 MDD 차이 측정\n")
    lines.append("---\n")

    # 1. Global MDD Inflation Summary
    lines.append("## 1. 전체 MDD 인플레이션 요약\n")

    if global_stats and global_stats.get("n_results"):
        lines.append("| 구분 | 원본 평균 MDD | Intrabar 평균 MDD | 평균 인플레이션 |")
        lines.append("|---|---|---|---|")
        lines.append(
            f"| 전체 {global_stats['n_results']} 결과 | "
            f"{global_stats['original_mdd_mean']:.2f}% | "
            f"{global_stats['intrabar_mdd_mean']:.2f}% | "
            f"{global_stats['inflation_mean']:.2f}%p |"
        )
    else:
        lines.append("(No data available)")
    lines.append("")

    # 2. TF-wise MDD Inflation
    lines.append("## 2. TF별 MDD 인플레이션\n")

    if tf_stats:
        lines.append("| TF | N | 평균 원본 MDD | 평균 Intrabar MDD | 평균 인플레이션 |")
        lines.append("|---|---|---|---|---|")
        for tf in sorted(tf_stats.keys()):
            s = tf_stats[tf]
            lines.append(
                f"| {tf} | {s['n']} | {s['original_mdd_mean']:.2f}% | "
                f"{s['intrabar_mdd_mean']:.2f}% | {s['inflation_mean']:.2f}%p |"
            )
    else:
        lines.append("(No data available)")
    lines.append("")

    # 3. Baseline-exceeding 10 combos
    lines.append("## 3. Baseline 초과 10개 후보 Intrabar 검증\n")

    if baseline_results:
        lines.append(
            "| Combo | 조정CAGR | 평균 원본MDD | 평균 Intrabar MDD | 인플레이션 | -35% 게이트 |"
        )
        lines.append("|---|---|---|---|---|---|")

        # Sort by adjusted CAGR (descending)
        sorted_baseline = sorted(
            baseline_results.items(),
            key=lambda x: x[1]["adj_cagr"],
            reverse=True,
        )

        for (strat, tf, variant, combo), data in sorted_baseline:
            combo_key = f"{strat}/{tf}/{variant}/combo_{combo}"
            gate_status = "PASS" if data["passes_gate"] else "FAIL"

            # Compute original mean MDD for this combo
            original_mdds_for_combo = [
                inflation_data[(strat, tf, variant, combo, p)]["original_mdd"]
                for p in data["periods"].keys()
                if (strat, tf, variant, combo, p) in inflation_data
            ]
            original_mean = statistics.mean(original_mdds_for_combo) if original_mdds_for_combo else None

            lines.append(
                f"| {combo_key} | {data['adj_cagr']:.2f}% | "
                f"{original_mean:.2f}% | {data['mean_mdd']:.2f}% | "
                f"{data['mean_mdd'] - original_mean:.2f}%p | {gate_status} |"
            )
    else:
        lines.append("(No baseline combos with intrabar results)")
    lines.append("")

    # 4. Additional Intrabar-passing combos (below baseline CAGR but passing -35% MDD gate)
    lines.append("## 4. 추가 Intrabar 통과 후보 (조정CAGR < 34.87% but MDD < -35%)\n")

    additional_candidates = []
    for (strat, tf, variant, combo), data in combo_means.items():
        mean_mdd = data["mean_mdd"]
        adj_cagr = get_adjusted_cagr(strat, tf, variant, combo)

        # Not in baseline, passes -35% gate, and has valid adj_cagr
        key_tuple = (strat, tf, variant, combo)
        if key_tuple not in baseline_results and mean_mdd >= MDD_GATE and adj_cagr:
            additional_candidates.append({
                "strat": strat,
                "tf": tf,
                "variant": variant,
                "combo": combo,
                "adj_cagr": adj_cagr,
                "mean_mdd": mean_mdd,
                "n_periods": data["n_periods"],
            })

    if additional_candidates:
        # Sort by adjusted CAGR (descending)
        additional_candidates.sort(key=lambda x: x["adj_cagr"], reverse=True)

        lines.append(
            "| Combo | 조정CAGR | 평균 Intrabar MDD | 테스트 기간수 |"
        )
        lines.append("|---|---|---|---|")
        for c in additional_candidates[:20]:  # Top 20
            lines.append(
                f"| {c['strat']}/{c['tf']}/{c['variant']}/combo_{c['combo']} | "
                f"{c['adj_cagr']:.2f}% | {c['mean_mdd']:.2f}% | {c['n_periods']} |"
            )
    else:
        lines.append("(No additional candidates)")
    lines.append("")

    # 5. Summary metrics
    lines.append("## 5. 검증 메트릭\n")

    gate_count = sum(1 for d in baseline_results.values() if d["passes_gate"])
    baseline_count = len(baseline_results)

    lines.append(f"- **Baseline 10개 중 -35% 게이트 통과**: {gate_count}/{baseline_count}")
    lines.append(f"- **평균 MDD 인플레이션**: {global_stats.get('inflation_mean', 0):.2f}%p")
    lines.append(f"- **인플레이션 표준편차**: {global_stats.get('inflation_std', 0):.2f}%p")
    lines.append("")

    # 6. Reproduction command
    lines.append("## 재현 명령\n")
    lines.append("```bash")
    lines.append("cd /home/justant/Data/Bit-Mania")
    lines.append("python3 backtest/scripts/analysis/generate_intrabar_report.py")
    lines.append("```\n")

    return "\n".join(lines)


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("[*] Collecting intrabar results...")
    intrabar_results = collect_intrabar_results()
    print(f"[*] Found {len(intrabar_results)} intrabar entries")

    print("[*] Computing MDD inflation...")
    inflation_data = compute_mdd_inflation(intrabar_results)
    print(f"[*] Computed {len(inflation_data)} inflation records")

    print("[*] Computing combo-level statistics...")
    combo_means = compute_combo_mean_mdd(inflation_data)
    print(f"[*] Computed {len(combo_means)} combo means")

    print("[*] Filtering baseline results...")
    baseline_results = get_baseline_results(combo_means)
    print(f"[*] Found {len(baseline_results)} baseline combos with intrabar results")

    print("[*] Computing global statistics...")
    global_stats = compute_global_stats(inflation_data)
    tf_stats = compute_tf_stats(inflation_data)

    print("[*] Generating report...")
    report = generate_report(
        inflation_data,
        combo_means,
        baseline_results,
        global_stats,
        tf_stats,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        f.write(report)

    print(f"[+] Report written to {OUTPUT_FILE}")
    print(f"[+] {global_stats.get('n_results', 0)} results analyzed")
    if baseline_results:
        gate_pass = sum(1 for d in baseline_results.values() if d["passes_gate"])
        print(f"[+] Baseline: {gate_pass}/{len(baseline_results)} combos pass -35% MDD gate")


if __name__ == "__main__":
    main()
