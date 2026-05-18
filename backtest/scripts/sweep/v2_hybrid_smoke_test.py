#!/usr/bin/env python3
"""
v2_hybrid_smoke_test.py — Sanity check before running full 128-mask sweep.

Runs two critical masks to validate the sweep setup:
  - mask=0 (baseline): checks that backtest runs and produces multiplier >= 1.0
  - mask=127 (max filter): checks that filtering reduces trade count

Usage:
    python3 v2_hybrid_smoke_test.py --output-dir /result/v2_hybrid

Exit codes:
    0 = all tests passed
    1 = at least one test failed
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _run_worker(mask: int, output_dir: Path) -> dict | None:
    """Call v2_hybrid_worker.py and parse JSON result. Returns parsed dict or None on error."""
    worker_script = Path(__file__).parent / "v2_hybrid_worker.py"

    try:
        result = subprocess.run(
            ["python3", str(worker_script), "--mask", str(mask), "--output-dir", str(output_dir)],
            capture_output=True,
            text=True,
            check=False,
        )

        # Parse last line as JSON
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            for line in reversed(lines):
                if line.startswith('{'):
                    return json.loads(line)

        if result.returncode != 0:
            print(f"[ERROR] Worker exited with code {result.returncode}")
            if result.stderr:
                print(f"Stderr: {result.stderr}", file=sys.stderr)

        return None
    except Exception as e:
        print(f"[ERROR] Failed to run worker: {e}", file=sys.stderr)
        return None


def _test_mask_0(output_dir: Path) -> bool:
    """Test mask=0 (baseline): check multiplier >= 1.0"""
    print("=== Smoke Test: mask=0 (baseline) ===")

    result = _run_worker(0, output_dir)
    if result is None or result.get("status") != "ok":
        error_msg = result.get("error", "unknown error") if result else "worker failed"
        print(f"FAIL: mask=0 worker failed: {error_msg}")
        return False

    data = result.get("data", {})
    finishing_balance = data.get("finishing_balance")
    if finishing_balance is None:
        print(f"FAIL: mask=0 stats missing finishing_balance")
        return False

    multiplier = finishing_balance / 10000.0
    if multiplier < 1.0:
        print(f"FAIL: mask=0 multiplier {multiplier:.2f}x < 1.0")
        return False

    print(f"PASS: mask=0 multiplier={multiplier:.2f}x, balance={finishing_balance:.2f}")
    return True


def _test_mask_127(output_dir: Path, baseline_trades: int) -> bool:
    """Test mask=127 (max filter): check trades_127 < trades_0"""
    print("=== Smoke Test: mask=127 (max filter) ===")

    result = _run_worker(127, output_dir)
    if result is None or result.get("status") != "ok":
        error_msg = result.get("error", "unknown error") if result else "worker failed"
        print(f"FAIL: mask=127 worker failed: {error_msg}")
        return False

    data = result.get("data", {})
    trades_127 = data.get("number_of_trades")
    if trades_127 is None:
        print(f"FAIL: mask=127 stats missing number_of_trades")
        return False

    if trades_127 >= baseline_trades:
        print(f"FAIL: mask=127 trades {trades_127} >= baseline {baseline_trades}")
        return False

    print(f"PASS: mask=127 trades={trades_127} < baseline {baseline_trades}")
    return True


def main():
    p = argparse.ArgumentParser(description="Smoke test for v2_hybrid sweep")
    p.add_argument("--output-dir", type=str, required=True, help="Output directory")
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Test mask=0
    if not _test_mask_0(output_dir):
        return 1

    # Extract baseline trade count from mask=0
    result_0 = _run_worker(0, output_dir)
    if result_0 is None or result_0.get("status") != "ok":
        print("FAIL: Could not retrieve mask=0 result for baseline")
        return 1

    baseline_trades = result_0.get("data", {}).get("number_of_trades")
    if baseline_trades is None:
        print("FAIL: mask=0 missing number_of_trades")
        return 1

    # Test mask=127
    if not _test_mask_127(output_dir, baseline_trades):
        return 1

    print("=== All smoke tests PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
