"""
Phase 7.5 — Automated Sanity Check for V5 Backtest Results.

Validates Jesse backtest JSON output against V5 rules.
Called by run_full_validation.sh after each backtest.

Rules:
  CRITICAL-1: Sharpe ~0 with N>50 trades → NaN contamination
  CRITICAL-2: CAGR > 50% + MDD < 1% → lookahead bias
  WARN-1:     Fees > 50% gross P&L → over-trading
  WARN-2:     100% winrate + N<30 → suspicious tiny sample
  WARN-3:     Zero trades in 2025+ compressed regime

Exit codes:
  0 = all PASS (or only WARNings)
  1 = at least one CRITICAL warning
"""

from __future__ import annotations
import json
import sys
import argparse
from pathlib import Path
from typing import Any


def validate(result: dict[str, Any]) -> list[str]:
    """
    Apply all V5 sanity checks to a backtest result dict.
    Returns list of warning strings (empty = fully clean).
    """
    warnings: list[str] = []

    sharpe     = float(result.get("sharpe", 0) or 0)
    num_trades = int(result.get("num_trades", 0) or 0)
    total_fees = float(result.get("total_fees", 0) or 0)
    gross_pnl  = float(result.get("gross_pnl", 0) or 0)
    win_rate   = float(result.get("win_rate", 0) or 0)
    cagr       = float(result.get("cagr", 0) or 0)
    mdd        = float(result.get("mdd", 0) or 0)
    regime_compressed_trades = int(result.get("regime_compressed_trades", -1))

    # ── CRITICAL checks ────────────────────────────────────────────────────────

    # Rule 1: Sharpe ~0 with many trades → NaN contamination (HMM bug pattern)
    if abs(sharpe) < 0.01 and num_trades > 50:
        warnings.append(
            "CRITICAL-1: Sharpe≈0 with many trades. "
            f"(sharpe={sharpe:.4f}, trades={num_trades}) "
            "Likely NaN contamination in P&L series. Do NOT pass this strategy."
        )

    # Rule 2: Unrealistic CAGR + almost-zero MDD → lookahead bias
    if cagr > 0.5 and abs(mdd) < 0.01:
        warnings.append(
            "CRITICAL-2: CAGR > 50% with MDD < 1% is not physically possible. "
            f"(cagr={cagr:.2%}, mdd={mdd:.2%}) "
            "Lookahead bias suspected. Strategy FAILED."
        )

    # ── WARNING checks ─────────────────────────────────────────────────────────

    # Rule 3: Fees eating too much gross P&L
    if gross_pnl > 0 and total_fees > gross_pnl * 0.5:
        warnings.append(
            f"WARN-1: Fees ({total_fees:.2f}) > 50% of gross P&L ({gross_pnl:.2f}). "
            "Reduce trade frequency or use maker orders."
        )

    # Rule 4: Suspicious 100% winrate on tiny sample
    if win_rate > 0.99 and num_trades < 30:
        warnings.append(
            f"WARN-2: 100% winrate with only {num_trades} trades. "
            "Not statistically meaningful. Run with more data."
        )

    # Rule 5: Zero trades in 2025+ compressed regime
    if regime_compressed_trades == 0:
        warnings.append(
            "WARN-3: Zero trades in 2025+ compressed regime. "
            "Strategy may not work in current market environment."
        )

    return warnings


def print_report(strategy_name: str, result: dict, warnings: list[str]) -> None:
    print(f"\n{'='*60}")
    print(f"SANITY CHECK: {strategy_name}")
    print(f"{'='*60}")
    print(f"  CAGR:        {result.get('cagr', 'N/A')}")
    print(f"  Sharpe:      {result.get('sharpe', 'N/A')}")
    print(f"  MDD:         {result.get('mdd', 'N/A')}")
    print(f"  Trades:      {result.get('num_trades', 'N/A')}")
    print(f"  Win Rate:    {result.get('win_rate', 'N/A')}")
    print(f"  Gross P&L:   {result.get('gross_pnl', 'N/A')}")
    print(f"  Total Fees:  {result.get('total_fees', 'N/A')}")
    print()

    if not warnings:
        print("  ✓ All sanity checks PASSED")
    else:
        criticals = [w for w in warnings if w.startswith("CRITICAL")]
        warns     = [w for w in warnings if w.startswith("WARN")]
        for w in criticals:
            print(f"  ✗ {w}")
        for w in warns:
            print(f"  ⚠ {w}")

    print(f"{'='*60}\n")


def parse_args():
    p = argparse.ArgumentParser(description="V5 Sanity Check for Jesse backtest results")
    p.add_argument("--result-json", required=True, help="Path to Jesse JSON result file")
    p.add_argument("--strategy",    required=True, help="Strategy name (for reporting)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result_path = Path(args.result_json)

    if not result_path.exists():
        print(f"ERROR: Result file not found: {result_path}", file=sys.stderr)
        sys.exit(1)

    with open(result_path) as f:
        result = json.load(f)

    warnings = validate(result)
    print_report(args.strategy, result, warnings)

    criticals = [w for w in warnings if w.startswith("CRITICAL")]
    sys.exit(1 if criticals else 0)
