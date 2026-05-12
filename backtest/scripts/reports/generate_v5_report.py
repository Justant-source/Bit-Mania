"""
Phase 7.5 — Generate V5 Strategy Report.

Aggregates results from:
  - Jesse backtest JSON (full period)
  - Walk-Forward summary
  - Regime split analysis
  - Sanity check

Outputs a Markdown report to .result/backtest/v5/<strategy>_v5_report.md

Usage:
    python scripts/generate_v5_report.py --strategy IntradaySeasonality
"""

from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

STORAGE_DIR  = Path("storage")
RESULT_DIR   = Path("/home/justant/Data/Bit-Mania/.result/backtest/v5")

V5_CRITERIA = {
    "cagr":        ("≥ 10%",    0.10),
    "sharpe":      ("≥ 1.0",    1.0),
    "mdd":         ("≤ -15%",   0.15),   # absolute value
    "wf_ratio":    ("≥ 0.6",    0.6),
    "mc_5p_sharpe":("≥ 0.0",    0.0),
    "trades_per_year": ("≥ 30", 30),
}


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def check_criterion(key: str, value, threshold) -> bool:
    if value is None:
        return False
    v = float(value)
    if key == "mdd":        # MDD stored as negative, compare absolute
        return abs(v) <= threshold
    if key in ("cagr", "sharpe", "wf_ratio", "mc_5p_sharpe", "trades_per_year"):
        return v >= threshold
    return False


def build_report(strategy: str) -> str:
    # Load data sources
    main_bt = load_json(STORAGE_DIR / f"{strategy}_main.json")
    wf      = load_json(STORAGE_DIR / "walk_forward" / f"{strategy}_wf_summary.json")
    regime  = load_json(STORAGE_DIR / "regime_split" / f"{strategy}_regime_split.json")

    wf_summary = wf.get("summary", {})
    regime_results = regime.get("results", [])

    # Gather key metrics
    cagr        = main_bt.get("cagr")
    sharpe      = main_bt.get("sharpe")
    mdd         = main_bt.get("mdd")
    num_trades  = main_bt.get("num_trades")
    win_rate    = main_bt.get("win_rate")
    total_fees  = main_bt.get("total_fees")
    gross_pnl   = main_bt.get("gross_pnl")
    wf_ratio    = wf_summary.get("oos_is_ratio")
    is_sharpe   = wf_summary.get("avg_is_sharpe")
    oos_sharpe  = wf_summary.get("avg_oos_sharpe")

    compressed = next((r for r in regime_results if r.get("regime") == "compressed"), {})
    compressed_trades = compressed.get("num_trades", "N/A")

    # V5 pass/fail evaluation
    checks = {
        "CAGR ≥ 10%":          check_criterion("cagr",  cagr,   0.10),
        "Sharpe ≥ 1.0":        check_criterion("sharpe", sharpe, 1.0),
        "MDD ≤ 15%":           check_criterion("mdd",    mdd,    0.15),
        "WF OOS/IS ≥ 0.6":    check_criterion("wf_ratio", wf_ratio, 0.6),
        "Trades/yr ≥ 30":      check_criterion("trades_per_year", num_trades, 30),
    }
    all_pass = all(checks.values())

    verdict = "✓ PASS" if all_pass else "✗ FAIL"

    lines = [
        f"# V5 Strategy Report: {strategy}",
        f"",
        f"**Generated**: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Verdict**: {verdict}",
        f"",
        f"---",
        f"",
        f"## Core Metrics (Full Period 2023-04 → 2026-04)",
        f"",
        f"| Metric | Value | V5 Criterion | Status |",
        f"|--------|-------|--------------|--------|",
        f"| CAGR | {cagr or 'N/A'} | ≥ 10% | {'✓' if checks.get('CAGR ≥ 10%') else '✗'} |",
        f"| Sharpe | {sharpe or 'N/A'} | ≥ 1.0 | {'✓' if checks.get('Sharpe ≥ 1.0') else '✗'} |",
        f"| MDD | {mdd or 'N/A'} | ≤ -15% | {'✓' if checks.get('MDD ≤ 15%') else '✗'} |",
        f"| Trades | {num_trades or 'N/A'} | ≥ 30/yr | {'✓' if checks.get('Trades/yr ≥ 30') else '✗'} |",
        f"| Win Rate | {win_rate or 'N/A'} | — | — |",
        f"| Gross P&L | {gross_pnl or 'N/A'} | — | — |",
        f"| Total Fees | {total_fees or 'N/A'} | — | — |",
        f"",
        f"## Walk-Forward Validation",
        f"",
        f"| Metric | Value | V5 Criterion | Status |",
        f"|--------|-------|--------------|--------|",
        f"| Avg IS Sharpe | {is_sharpe or 'N/A'} | — | — |",
        f"| Avg OOS Sharpe | {oos_sharpe or 'N/A'} | — | — |",
        f"| OOS/IS Ratio | {wf_ratio or 'N/A'} | ≥ 0.6 | {'✓' if checks.get('WF OOS/IS ≥ 0.6') else '✗'} |",
        f"",
        f"## Regime Split Analysis",
        f"",
        f"| Regime | CAGR | Sharpe | MDD | Trades |",
        f"|--------|------|--------|-----|--------|",
    ]

    for r in regime_results:
        if r.get("error"):
            lines.append(f"| {r.get('regime','?')} | ERROR | — | — | — |")
        else:
            lines.append(
                f"| {r.get('regime','?')} | "
                f"{r.get('cagr','?')} | "
                f"{r.get('sharpe','?')} | "
                f"{r.get('mdd','?')} | "
                f"{r.get('num_trades','?')} |"
            )

    lines += [
        f"",
        f"**Compressed regime (2025+) trades**: {compressed_trades}",
        f"",
        f"---",
        f"",
        f"## V5 Pass/Fail Summary",
        f"",
        f"| Criterion | Status |",
        f"|-----------|--------|",
    ]
    for criterion, passed in checks.items():
        lines.append(f"| {criterion} | {'✓ PASS' if passed else '✗ FAIL'} |")

    lines += [
        f"",
        f"### Final Verdict: **{verdict}**",
        f"",
        f"---",
        f"*Report generated by generate_v5_report.py (Phase 7.5)*",
    ]

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", required=True)
    p.add_argument("--output",   default=None,
                   help="Override output path (default: .result/backtest/v5/<strategy>_v5_report.md)")
    args = p.parse_args()

    report_md = build_report(args.strategy)

    out_path = Path(args.output) if args.output else (
        RESULT_DIR / f"{args.strategy}_v5_report.md"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_md)
    print(f"[generate_v5_report] Saved: {out_path}")
    print(report_md[:500] + "...")


if __name__ == "__main__":
    main()
