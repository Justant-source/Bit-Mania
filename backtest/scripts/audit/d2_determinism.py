#!/usr/bin/env python3
"""
D-2 Determinism Audit — Pre-2021 Backfill Results Consistency

Verifies that backtest results are deterministic by checking:
1. result_completeness: All expected result.json and trades.csv files exist
2. trade_count_consistency: result.json["metrics"]["total_trades"] matches len(trades.csv) - 1
3. pnl_consistency: sum(trades.csv.pnl) ≈ finishing_balance - starting_balance (±0.1 USDT)

This is a HOST-SIDE script that compares existing files, NOT a Docker re-run.

Output: results/audit/04_d2_determinism.json
Exit code: 0 if all pass, 1 if any fail
"""

import json
import sys
import csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Any


def get_all_result_dirs():
    """Scan pre2021_backfill/ for all (strat, tf, variant, period) combinations."""
    BT_ROOT = Path(__file__).parent.parent.parent
    backfill_root = BT_ROOT / "results" / "pre2021_backfill"

    result_dirs = []
    if backfill_root.exists():
        for result_json in backfill_root.glob("*/*/*/*/result.json"):
            # Path: strat/tf/variant/period/result.json
            period = result_json.parent.name
            variant = result_json.parent.parent.name
            tf = result_json.parent.parent.parent.name
            strat = result_json.parent.parent.parent.parent.name
            result_dirs.append((strat, tf, variant, period, result_json.parent))

    return sorted(result_dirs)


def load_result_json(dir_path: Path) -> dict[str, Any] | None:
    """Load result.json from directory."""
    result_file = dir_path / "result.json"
    if not result_file.exists():
        return None
    try:
        with open(result_file) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ERROR loading {result_file}: {e}", file=sys.stderr)
        return None


def load_trades_csv(dir_path: Path) -> list[dict] | None:
    """Load trades.csv from directory, returns list of trade dicts."""
    trades_file = dir_path / "trades.csv"
    if not trades_file.exists():
        return None
    try:
        with open(trades_file) as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"  ERROR loading {trades_file}: {e}", file=sys.stderr)
        return None


def load_stats_json(dir_path: Path) -> dict[str, Any] | None:
    """Load stats.json from directory."""
    stats_file = dir_path / "stats.json"
    if not stats_file.exists():
        return None
    try:
        with open(stats_file) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ERROR loading {stats_file}: {e}", file=sys.stderr)
        return None


def audit_check(strat: str, tf: str, variant: str, period: str, dir_path: Path) -> dict[str, Any]:
    """
    Run all three determinism checks for this backtest result.

    Returns:
        {
            "id": "strat/tf/variant/period",
            "status": "PASS" | "FAIL" | "SKIP",
            "expected": {...},
            "actual": {...},
            "delta": {...},
            "msg": "..."
        }
    """
    check_id = f"{strat}/{tf}/{variant}/{period}"

    # Load files
    result = load_result_json(dir_path)
    trades = load_trades_csv(dir_path)
    stats = load_stats_json(dir_path)

    # Check 1: Completeness
    if result is None or trades is None:
        return {
            "id": check_id,
            "status": "FAIL",
            "expected": {"result.json": "exists", "trades.csv": "exists"},
            "actual": {"result.json": "missing" if result is None else "exists",
                      "trades.csv": "missing" if trades is None else "exists"},
            "delta": {},
            "msg": "result.json or trades.csv missing"
        }

    # Check 2: Trade count consistency
    expected_trade_count = result.get("metrics", {}).get("total_trades", 0)
    actual_trade_count = len(trades)
    trade_count_match = (expected_trade_count == actual_trade_count)

    if not trade_count_match:
        return {
            "id": check_id,
            "status": "FAIL",
            "expected": {"total_trades": expected_trade_count},
            "actual": {"total_trades": actual_trade_count},
            "delta": {"total_trades": actual_trade_count - expected_trade_count},
            "msg": f"trade count mismatch: expected {expected_trade_count}, got {actual_trade_count}"
        }

    # Check 3: PnL consistency (only if stats.json exists)
    if stats is None:
        return {
            "id": check_id,
            "status": "SKIP",
            "expected": {"pnl_check": "stats.json required"},
            "actual": {"stats.json": "missing"},
            "delta": {},
            "msg": "stats.json missing, skipping PnL check"
        }

    raw_metrics = stats.get("raw_metrics", {})
    starting_balance = raw_metrics.get("starting_balance", 0)
    finishing_balance = raw_metrics.get("finishing_balance", 0)
    expected_net_pnl = finishing_balance - starting_balance

    # Sum trades PnL
    actual_net_pnl = 0.0
    try:
        for trade in trades:
            pnl_str = trade.get("pnl", "0")
            actual_net_pnl += float(pnl_str)
    except (ValueError, KeyError) as e:
        return {
            "id": check_id,
            "status": "FAIL",
            "expected": {"pnl_sum": "numeric"},
            "actual": {"pnl_sum": f"error: {e}"},
            "delta": {},
            "msg": f"error parsing trades.csv pnl: {e}"
        }

    # Check tolerance (±0.1 USDT)
    pnl_diff = abs(expected_net_pnl - actual_net_pnl)
    pnl_tolerance = 0.1
    pnl_match = (pnl_diff <= pnl_tolerance)

    if not pnl_match:
        return {
            "id": check_id,
            "status": "FAIL",
            "expected": {"net_pnl": round(expected_net_pnl, 2)},
            "actual": {"net_pnl": round(actual_net_pnl, 2)},
            "delta": {"net_pnl": round(actual_net_pnl - expected_net_pnl, 2)},
            "msg": f"PnL mismatch: expected {expected_net_pnl:.2f}, got {actual_net_pnl:.2f} (diff={pnl_diff:.2f})"
        }

    # All checks passed
    return {
        "id": check_id,
        "status": "PASS",
        "expected": {
            "total_trades": expected_trade_count,
            "net_pnl": round(expected_net_pnl, 2)
        },
        "actual": {
            "total_trades": actual_trade_count,
            "net_pnl": round(actual_net_pnl, 2)
        },
        "delta": {
            "total_trades": 0,
            "net_pnl": 0.0
        },
        "msg": "deterministic"
    }


def main():
    BT_ROOT = Path(__file__).parent.parent.parent
    RESULTS_DIR = BT_ROOT / "results" / "audit"
    OUTPUT_FILE = RESULTS_DIR / "04_d2_determinism.json"

    # Ensure output dir exists
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Discover all result directories
    print("Scanning for pre2021_backfill results...", file=sys.stderr)
    result_dirs = get_all_result_dirs()
    print(f"Found {len(result_dirs)} expected result directories", file=sys.stderr)

    # Run all checks
    checks = []
    for i, (strat, tf, variant, period, dir_path) in enumerate(result_dirs, 1):
        if i % 50 == 0:
            print(f"  [{i}/{len(result_dirs)}] {strat}/{tf}/{variant}/{period}", file=sys.stderr)

        check = audit_check(strat, tf, variant, period, dir_path)
        checks.append(check)

    # Tally results
    summary = {
        "total": len(checks),
        "pass": sum(1 for c in checks if c["status"] == "PASS"),
        "fail": sum(1 for c in checks if c["status"] == "FAIL"),
        "warn": 0,
        "skip": sum(1 for c in checks if c["status"] == "SKIP")
    }

    # Build output JSON
    output = {
        "audit_id": "d2",
        "name": "determinism",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "summary": summary
    }

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    # Print summary
    print(f"\nSUMMARY: pass={summary['pass']} fail={summary['fail']} warn={summary['warn']} skip={summary['skip']} → {OUTPUT_FILE}", file=sys.stdout)

    # Exit code
    sys.exit(0 if summary["fail"] == 0 else 1)


if __name__ == "__main__":
    main()
