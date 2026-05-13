#!/usr/bin/env python3
"""
D1 Equity Continuity Audit

Verifies equity math integrity and date bounds for pre-2021 backfill results:
1. equity_balance: sum(trades.pnl) + starting_balance ≈ finishing_balance (tolerance ±0.01)
2. trade_dates: For pre21_full, all closed_at <= 1609459199000 ms (2020-12-31 end)
3. no_overlap: No trade in pre21_full overlaps with post-2021 results

Output: RESULTS_ROOT/audit/03_d1_equity.json
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
POST2021_ROOT = RESULTS_ROOT / "7-strategies"
AUDIT_OUTPUT = RESULTS_ROOT / "audit" / "03_d1_equity.json"
AUDIT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# Constants
PRE21_FULL_END_MS = 1609459199000  # 2020-12-31 23:59:59 UTC in ms
PRE21_FULL_START_MS = 1609459200000  # 2021-01-01 00:00:00 UTC in ms
EQUITY_TOLERANCE = 0.01  # USDT


def discover_combos():
    """Scan pre2021_backfill for all unique (strat, tf, variant) combos."""
    combos = set()
    for result_file in PRE2021_ROOT.glob("*/*/*/*/result.json"):
        parts = result_file.relative_to(PRE2021_ROOT).parts
        if len(parts) >= 3:
            strat, tf, variant = parts[0], parts[1], parts[2]
            combos.add((strat, tf, variant))
    return sorted(combos)


def load_post2021_opened_ats():
    """
    Load all opened_at timestamps from post-2021 trades.csv files.
    Returns a set of opened_at values (in ms).
    """
    opened_ats = set()
    for trades_file in POST2021_ROOT.glob("*/*/*/trades.csv"):
        try:
            df = pd.read_csv(trades_file)
            if "opened_at" in df.columns:
                opened_ats.update(df["opened_at"].values)
        except Exception as e:
            print(f"[d1] Warning: Could not read {trades_file}: {e}", file=sys.stderr)
    return opened_ats


def check_equity_balance(strat, tf, variant, period):
    """
    Check: sum(trades.pnl) + starting_balance ≈ finishing_balance
    Returns (status, expected, actual, delta, msg)
    """
    period_dir = PRE2021_ROOT / strat / tf / variant / period
    trades_file = period_dir / "trades.csv"
    stats_file = period_dir / "stats.json"

    if not trades_file.exists() or not stats_file.exists():
        return "SKIP", None, None, None, "Missing trades.csv or stats.json"

    try:
        df = pd.read_csv(trades_file)
        with open(stats_file) as f:
            stats = json.load(f)

        raw = stats.get("raw_metrics", {})
        if "starting_balance" not in raw or "finishing_balance" not in raw:
            # 0-trade periods: Jesse omits these keys; nothing to verify
            total_trades = stats.get("total_trades", 0)
            if total_trades == 0:
                return "SKIP", None, None, None, "0-trade period: no balance keys"
            return "FAIL", None, None, None, "Missing starting_balance/finishing_balance in raw_metrics"
        starting_balance = raw["starting_balance"]
        finishing_balance = raw["finishing_balance"]

        # Calculate expected finishing balance from PnL
        total_pnl = df["pnl"].sum()
        calculated_balance = starting_balance + total_pnl

        delta = abs(calculated_balance - finishing_balance)
        status = "PASS" if delta <= EQUITY_TOLERANCE else "FAIL"

        return (
            status,
            finishing_balance,
            calculated_balance,
            delta,
            f"PnL sum: {total_pnl:.2f}, delta: {delta:.4f}",
        )
    except Exception as e:
        return "FAIL", None, None, None, f"Error: {str(e)}"


def check_trade_dates(strat, tf, variant, period, post2021_opened_ats):
    """
    For pre21_full period:
    - All closed_at <= 1609459199000 ms (2020-12-31 end)
    - No opened_at overlaps with post-2021 trades
    Returns (status, expected, actual, delta, msg)
    """
    if period != "pre21_full":
        return "SKIP", None, None, None, "Not pre21_full period"

    period_dir = PRE2021_ROOT / strat / tf / variant / period
    trades_file = period_dir / "trades.csv"

    if not trades_file.exists():
        return "SKIP", None, None, None, "Missing trades.csv"

    try:
        df = pd.read_csv(trades_file)

        # Check closed_at bounds
        late_closes = df[df["closed_at"] > PRE21_FULL_END_MS]
        if len(late_closes) > 0:
            return (
                "FAIL",
                f"<= {PRE21_FULL_END_MS}",
                f"max closed_at: {late_closes['closed_at'].max():.0f}",
                None,
                f"Found {len(late_closes)} trades with closed_at > 2020-12-31",
            )

        # Check opened_at overlaps with post-2021
        overlaps = df[df["opened_at"].isin(post2021_opened_ats)]
        if len(overlaps) > 0:
            return (
                "FAIL",
                "No overlap",
                f"{len(overlaps)} overlapping opened_at",
                None,
                f"Found {len(overlaps)} pre21_full trades that opened at same time as post-2021 trades",
            )

        return "PASS", "All dates valid", "Verified", None, "All closed_at <= boundary, no opened_at overlap"

    except Exception as e:
        return "FAIL", None, None, None, f"Error: {str(e)}"


def main():
    print("[d1] Starting equity continuity audit...", file=sys.stderr)

    combos = discover_combos()
    print(f"[d1] Discovered {len(combos)} combos", file=sys.stderr)

    post2021_opened_ats = load_post2021_opened_ats()
    print(f"[d1] Loaded {len(post2021_opened_ats)} post-2021 opened_at values", file=sys.stderr)

    periods = ["pre21_full", "pre21_bear", "pre21_range", "pre21_recovery", "pre21_covid", "pre21_bull"]
    checks = []
    summary = {"total": 0, "pass": 0, "fail": 0, "warn": 0, "skip": 0}

    for strat, tf, variant in combos:
        for period in periods:
            print(f"[d1] Checking {strat}/{tf}/{variant}/{period}...", file=sys.stderr)

            # Check 1: equity_balance
            status, expected, actual, delta, msg = check_equity_balance(strat, tf, variant, period)
            check_id = f"{strat}/{tf}/{variant}/{period}"
            checks.append(
                {
                    "id": check_id,
                    "check": "equity_balance",
                    "status": status,
                    "expected": expected,
                    "actual": actual,
                    "delta": delta,
                    "msg": msg,
                }
            )
            summary[status.lower()] += 1
            summary["total"] += 1
            print(f"  equity_balance: {status}", file=sys.stderr)

            # Check 2: trade_dates (only for pre21_full)
            if period == "pre21_full":
                status, expected, actual, delta, msg = check_trade_dates(
                    strat, tf, variant, period, post2021_opened_ats
                )
                check_id = f"{strat}/{tf}/{variant}/{period}"
                checks.append(
                    {
                        "id": check_id,
                        "check": "trade_dates",
                        "status": status,
                        "expected": expected,
                        "actual": actual,
                        "delta": delta,
                        "msg": msg,
                    }
                )
                summary[status.lower()] += 1
                summary["total"] += 1
                print(f"  trade_dates: {status}", file=sys.stderr)

    # Build output JSON
    audit = {
        "audit_id": "d1",
        "name": "equity_continuity",
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
