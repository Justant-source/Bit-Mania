#!/usr/bin/env python3
"""
D3 Cost & Liquidation Audit

Verifies per-trade fee arithmetic and checks leverage drawdown thresholds:

Check A — fee arithmetic (per trade):
  Expected fee: (entry_price * qty + exit_price * qty) * 0.0002
  Tolerance: ±0.01 USDT OR ±1% (whichever is larger)

Check B — leverage drawdown (leveraged variants only: long_only_x2, long_only_x3):
  - Build monthly equity curve
  - x2: WARN if any month shows equity drop > 50% from start-of-month peak
  - x3: WARN if any month shows equity drop > 33% from start-of-month peak

Output: RESULTS_ROOT/audit/05_d3_cost.json
Exit code: 0 if fail=0, else 1
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd


def get_results_root():
    """Find RESULTS_ROOT by looking for backtest/results/ structure."""
    script = Path(__file__).resolve()
    current = script.parent
    while current != current.parent:
        # We're in backtest/scripts/audit, need to find backtest/results
        if current.name == "backtest":
            results = current / "results"
            if (results / "pre2021_backfill").exists():
                return results
        # Try direct results/pre2021_backfill check
        if (current / "pre2021_backfill").exists():
            return current
        current = current.parent
    raise RuntimeError("Cannot find RESULTS_ROOT (pre2021_backfill not found)")


RESULTS_ROOT = Path(get_results_root())
PRE2021_ROOT = RESULTS_ROOT / "pre2021_backfill"
AUDIT_OUTPUT = RESULTS_ROOT / "audit" / "05_d3_cost.json"
AUDIT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

FEE_RATE = 0.0002  # 0.02%
STARTING_BALANCE = 10000.0


def discover_combos():
    """Scan pre2021_backfill for all unique (strat, tf, variant) combos."""
    combos = set()
    for result_file in PRE2021_ROOT.glob("*/*/*/*/result.json"):
        parts = result_file.relative_to(PRE2021_ROOT).parts
        if len(parts) >= 3:
            strat, tf, variant = parts[0], parts[1], parts[2]
            combos.add((strat, tf, variant))
    return sorted(combos)


def calculate_expected_fee(entry_price, exit_price, qty):
    """Calculate expected fee for a trade."""
    return (entry_price * qty + exit_price * qty) * FEE_RATE


def check_fee_arithmetic(strat, tf, variant, period):
    """
    Check A: Verify per-trade fees match expected formula.
    Tolerance: max(0.01 USDT, 1% of expected fee)
    Returns (status, expected, actual, delta, msg)
    """
    period_dir = PRE2021_ROOT / strat / tf / variant / period
    trades_file = period_dir / "trades.csv"

    if not trades_file.exists():
        return "SKIP", None, None, None, "Missing trades.csv"

    try:
        df = pd.read_csv(trades_file)

        failures = []
        for idx, row in df.iterrows():
            expected_fee = calculate_expected_fee(row["entry_price"], row["exit_price"], row["qty"])
            actual_fee = row["fee"]
            delta = abs(expected_fee - actual_fee)
            tolerance = max(0.01, expected_fee * 0.01)

            if delta > tolerance:
                failures.append({
                    "trade": idx,
                    "expected": expected_fee,
                    "actual": actual_fee,
                    "delta": delta,
                    "tolerance": tolerance,
                })

        if failures:
            worst = max(failures, key=lambda x: x["delta"])
            return (
                "FAIL",
                f"{worst['expected']:.4f} (trade {worst['trade']})",
                f"{worst['actual']:.4f}",
                worst["delta"],
                f"{len(failures)}/{len(df)} trades exceed tolerance",
            )

        return (
            "PASS",
            "All fees match",
            "Verified",
            None,
            f"All {len(df)} trades within tolerance",
        )

    except Exception as e:
        return "FAIL", None, None, None, f"Error: {str(e)}"


def check_leverage_drawdown(strat, tf, variant, period):
    """
    Check B: For leveraged variants (x2, x3), verify monthly drawdown.
    x2: WARN if equity drop > 50% from start-of-month peak
    x3: WARN if equity drop > 33% from start-of-month peak
    Returns (status, expected, actual, delta, msg)
    """
    # Only check leveraged variants
    if not (variant.endswith("_x2") or variant.endswith("_x3")):
        return "SKIP", None, None, None, "Not a leveraged variant"

    period_dir = PRE2021_ROOT / strat / tf / variant / period
    trades_file = period_dir / "trades.csv"

    if not trades_file.exists():
        return "SKIP", None, None, None, "Missing trades.csv"

    try:
        df = pd.read_csv(trades_file)

        # Convert opened_at to datetime (milliseconds to nanoseconds)
        df["opened_at_dt"] = pd.to_datetime(df["opened_at"], unit="ms")
        df["year_month"] = df["opened_at_dt"].dt.to_period("M")

        # Build monthly equity curve
        monthly_groups = df.groupby("year_month")
        monthly_data = []

        equity = STARTING_BALANCE
        for period_key, group in monthly_groups:
            pnl_sum = group["pnl"].sum()
            fees_sum = group["fee"].sum()
            equity_at_month_end = equity + pnl_sum - fees_sum

            monthly_data.append({
                "period": str(period_key),
                "start_equity": equity,
                "pnl": pnl_sum,
                "fees": fees_sum,
                "end_equity": equity_at_month_end,
            })
            equity = equity_at_month_end

        # Check drawdown thresholds
        leverage = 2 if variant.endswith("_x2") else 3
        threshold = 0.5 if leverage == 2 else (1.0 / 3.0)
        threshold_pct = int(threshold * 100)

        violations = []
        for month_data in monthly_data:
            start_equity = month_data["start_equity"]
            end_equity = month_data["end_equity"]
            drawdown = (start_equity - end_equity) / start_equity if start_equity > 0 else 0

            if drawdown > threshold:
                violations.append({
                    "period": month_data["period"],
                    "start": start_equity,
                    "end": end_equity,
                    "drawdown_pct": drawdown * 100,
                })

        if violations:
            worst = max(violations, key=lambda x: x["drawdown_pct"])
            return (
                "WARN",
                f"< {threshold_pct}% monthly drawdown",
                f"{worst['drawdown_pct']:.2f}% in {worst['period']}",
                worst["drawdown_pct"],
                f"{len(violations)}/{len(monthly_data)} months exceed {threshold_pct}% threshold",
            )

        return (
            "PASS",
            f"< {threshold_pct}% monthly drawdown",
            "All months OK",
            None,
            f"Verified {len(monthly_data)} months within {threshold_pct}% threshold",
        )

    except Exception as e:
        return "FAIL", None, None, None, f"Error: {str(e)}"


def main():
    print("[d3] Starting cost & liquidation audit...", file=sys.stderr)

    combos = discover_combos()
    print(f"[d3] Discovered {len(combos)} combos", file=sys.stderr)

    periods = ["pre21_full", "pre21_bear", "pre21_range", "pre21_recovery", "pre21_covid", "pre21_bull"]
    checks = []
    summary = {"total": 0, "pass": 0, "fail": 0, "warn": 0, "skip": 0}

    for strat, tf, variant in combos:
        for period in periods:
            print(f"[d3] Checking {strat}/{tf}/{variant}/{period}...", file=sys.stderr)

            # Check A: fee arithmetic
            status, expected, actual, delta, msg = check_fee_arithmetic(strat, tf, variant, period)
            check_id = f"{strat}/{tf}/{variant}/{period}"
            checks.append(
                {
                    "id": check_id,
                    "check": "fee_arithmetic",
                    "status": status,
                    "expected": expected,
                    "actual": actual,
                    "delta": delta,
                    "msg": msg,
                }
            )
            summary[status.lower()] += 1
            summary["total"] += 1
            print(f"  fee_arithmetic: {status}", file=sys.stderr)

            # Check B: leverage drawdown (only for x2, x3)
            if variant.endswith("_x2") or variant.endswith("_x3"):
                status, expected, actual, delta, msg = check_leverage_drawdown(strat, tf, variant, period)
                checks.append(
                    {
                        "id": check_id,
                        "check": "leverage_drawdown",
                        "status": status,
                        "expected": expected,
                        "actual": actual,
                        "delta": delta,
                        "msg": msg,
                    }
                )
                summary[status.lower()] += 1
                summary["total"] += 1
                print(f"  leverage_drawdown: {status}", file=sys.stderr)

    # Build output JSON
    audit = {
        "audit_id": "d3",
        "name": "cost_liquidation",
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
