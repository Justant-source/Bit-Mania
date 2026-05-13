#!/usr/bin/env python3
r"""
D4 Funding Independent Audit

Independently recomputes adj_cagr for each entry in all_adjusted_results_pre21.json
and compares to stored value. Detects:
1. funding_sign bug: long_only_x2, long_only_x3 get sign=0 instead of +1
2. leverage_mult bug: fee_cost and fund_cost not multiplied by leverage

Cost formula (correct):
  base_variant = variant.replace(_xN, '')
  leverage_mult = int(re.search(r'_x(\d+)$', variant).group(1)) or 1
  if base_variant == 'long_only': funding_sign = +1.0
  elif base_variant == 'short_only': funding_sign = -1.0
  else: funding_sign = 0.0
  fee_cost = trades_per_year × FEE_DELTA_PER_SIDE × 2 × 100 × leverage_mult
  fund_cost = trades_per_year × n_funding × avg_funding_rate × funding_sign × 100 × leverage_mult

Output: RESULTS_ROOT/audit/06_d4_funding.json
Exit code: 0 if fail=0, else 1
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict


def get_results_root():
    """Find RESULTS_ROOT by scanning up from script location."""
    script = Path(__file__).resolve()
    current = script.parent  # audit/
    while current != current.parent:
        # Check if we're in backtest/scripts/audit, then results is 2 levels up
        candidate = current.parent.parent / "results"
        if candidate.exists():
            return candidate
        current = current.parent
    raise RuntimeError("Cannot find RESULTS_ROOT")


RESULTS_ROOT = Path(get_results_root())
ADJUSTED_RESULTS_JSON = RESULTS_ROOT / "adjusted_costs_pre2021" / "all_adjusted_results_pre21.json"
AUDIT_OUTPUT = RESULTS_ROOT / "audit" / "06_d4_funding.json"
AUDIT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# Cost model constants (must match apply_realistic_costs_pre21.py)
FEE_DELTA_PER_SIDE = (0.055 - 0.020) / 100.0  # 0.00035
TF_HOLD_HOURS = {
    '1h': 10,
    '4h': 32,
    '1D': 96,
}

PRE21_PERIOD_YEARS = {
    'pre21_full': 3.37,
    'pre21_bear': 0.99,
    'pre21_range': 0.29,
    'pre21_recovery': 0.91,
    'pre21_covid': 0.17,
    'pre21_bull': 0.67,
}

# Tolerance for rounding (0.5%/year accounts for rounding differences)
CAGR_DELTA_TOLERANCE = 0.5


def extract_base_variant(variant):
    """Extract base variant name (remove _xN suffix)."""
    return re.sub(r'_x\d+$', '', variant)


def extract_leverage(variant):
    """Extract leverage multiplier from variant name (default 1)."""
    match = re.search(r'_x(\d+)$', variant)
    return int(match.group(1)) if match else 1


def compute_costs(trades, tf, variant, period, avg_funding_rate_stored):
    """
    Independently recompute fee_cost and fund_cost using the CORRECT formula.

    Args:
      trades: number of trades
      tf: timeframe ('1h', '4h', '1D')
      variant: variant name ('long_only', 'long_only_x2', etc.)
      period: period name (for looking up period_years)
      avg_funding_rate_stored: average funding rate as stored in JSON (already × 100)

    Returns:
      (fee_cost_pct_annual, fund_cost_pct_annual, funding_sign, leverage_mult)
    """
    # Convert stored funding rate back to decimal (stored as × 100)
    avg_funding_rate_decimal = avg_funding_rate_stored / 100.0

    # Extract base variant and leverage
    base_variant = extract_base_variant(variant)
    leverage_mult = extract_leverage(variant)

    # Determine funding sign based on BASE variant
    if base_variant == 'long_only':
        funding_sign = +1.0
    elif base_variant == 'short_only':
        funding_sign = -1.0
    else:
        funding_sign = 0.0

    # Get hold hours and compute n_funding periods
    hold_hours = TF_HOLD_HOURS.get(tf, 32)
    n_funding = hold_hours / 8.0

    # Get period years
    period_years = PRE21_PERIOD_YEARS.get(period, 1.0)

    # Compute trades per year
    trades_per_year = trades / period_years if period_years > 0 else 0

    # Apply CORRECT formula with leverage multiplier
    fee_cost_pct_annual = trades_per_year * FEE_DELTA_PER_SIDE * 2 * 100 * leverage_mult
    fund_cost_pct_annual = (
        trades_per_year * n_funding * avg_funding_rate_decimal * funding_sign * 100 * leverage_mult
    )

    return fee_cost_pct_annual, fund_cost_pct_annual, funding_sign, leverage_mult


def check_funding_sign_bug(variant, stored_funding_cost, funding_coverage):
    """
    Check if long_only_x2 or long_only_x3 has stored_funding_cost == 0.0 (bug indicator).
    Skips check when funding_coverage == 'fee_only' (0.0 is correct in that case).
    """
    base_variant = extract_base_variant(variant)
    leverage_mult = extract_leverage(variant)
    if leverage_mult <= 1:
        return 'SKIP', f"Not a leveraged variant (variant={variant})"
    if base_variant != 'long_only':
        return 'SKIP', f"Not long_only base variant (variant={variant})"
    # With fee_only coverage, avg_funding_rate=0 → fund_cost=0 is correct
    if funding_coverage == 'fee_only':
        return 'SKIP', f"fee_only coverage: funding_cost=0 is expected (variant={variant})"
    # Only flag if there's actual funding data and cost is still 0
    if stored_funding_cost == 0.0:
        return 'FAIL', f"funding_sign=0 bug detected (variant={variant}, stored_funding_cost=0.0, coverage={funding_coverage})"
    return 'PASS', f"No funding_sign bug (variant={variant}, stored_funding_cost={stored_funding_cost})"


def main():
    print("[d4] Starting funding-independent audit...", file=sys.stderr)

    # Load JSON
    if not ADJUSTED_RESULTS_JSON.exists():
        print(f"[d4] ERROR: {ADJUSTED_RESULTS_JSON} not found", file=sys.stderr)
        sys.exit(1)

    with open(ADJUSTED_RESULTS_JSON) as f:
        results = json.load(f)

    print(f"[d4] Loaded {len(results)} entries from {ADJUSTED_RESULTS_JSON}", file=sys.stderr)

    checks = []
    summary = {"total": 0, "pass": 0, "fail": 0, "warn": 0, "skip": 0}

    for entry_idx, entry in enumerate(results, 1):
        strat = entry.get('strat')
        tf = entry.get('tf')
        variant = entry.get('variant')
        periods_dict = entry.get('periods', {})

        if not all([strat, tf, variant]):
            print(f"[d4] Warning: Malformed entry at index {entry_idx}: missing strat/tf/variant", file=sys.stderr)
            continue

        for period_name, period_data in periods_dict.items():
            check_id = f"{strat}/{tf}/{variant}/{period_name}"

            # ─── Check 1: Recompute adj_cagr ───────────────────────────────────────
            original_cagr = period_data.get('original_cagr', 0.0)
            stored_adj_cagr = period_data.get('adj_cagr', 0.0)
            trades = period_data.get('trades', 0)
            avg_funding_rate_stored = period_data.get('avg_funding_rate', 0.0)
            funding_coverage = period_data.get('funding_coverage', 'fee_only')

            # If fee_only, set avg_funding_rate to 0.0 for recomputation
            avg_funding_rate_for_computation = 0.0 if funding_coverage == 'fee_only' else avg_funding_rate_stored

            # Recompute costs
            fee_cost, fund_cost, funding_sign, leverage_mult = compute_costs(
                trades, tf, variant, period_name, avg_funding_rate_for_computation
            )

            total_cost = fee_cost + fund_cost
            recomputed_adj_cagr = round(original_cagr - total_cost, 2)

            # Compare
            delta = abs(recomputed_adj_cagr - stored_adj_cagr)
            status_cagr = "FAIL" if delta > CAGR_DELTA_TOLERANCE else "PASS"

            checks.append({
                "id": check_id,
                "check": "adj_cagr_recompute",
                "status": status_cagr,
                "expected": stored_adj_cagr,
                "actual": recomputed_adj_cagr,
                "delta": round(delta, 2),
                "msg": (
                    f"original={original_cagr}, fee_cost={round(fee_cost, 3)}, "
                    f"fund_cost={round(fund_cost, 3)}, "
                    f"leverage_mult={leverage_mult}, delta={round(delta, 2)}"
                ),
            })
            summary[status_cagr.lower()] += 1
            summary["total"] += 1

            # ─── Check 2: Funding sign bug for long_only_x variants ────────────────
            status_bug, msg_bug = check_funding_sign_bug(
                variant, period_data.get('funding_cost_annual_pct', 0.0), funding_coverage
            )

            checks.append({
                "id": f"{check_id}/funding_sign",
                "check": "funding_sign_bug",
                "status": status_bug,
                "expected": f"funding_sign={funding_sign}" if 'long_only' in variant else None,
                "actual": period_data.get('funding_cost_annual_pct', 0.0),
                "delta": None,
                "msg": msg_bug,
            })
            summary[status_bug.lower()] += 1
            summary["total"] += 1

        if entry_idx % 10 == 0:
            print(f"[d4] Processed {entry_idx}/{len(results)} entries", file=sys.stderr)

    # Build output JSON
    audit = {
        "audit_id": "d4",
        "name": "funding_independent",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "summary": summary,
    }

    with open(AUDIT_OUTPUT, "w") as f:
        json.dump(audit, f, indent=2)

    print(
        f"SUMMARY: pass={summary['pass']} fail={summary['fail']} warn={summary['warn']} skip={summary['skip']} → {AUDIT_OUTPUT}",
        file=sys.stdout,
    )

    exit_code = 0 if summary["fail"] == 0 else 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
