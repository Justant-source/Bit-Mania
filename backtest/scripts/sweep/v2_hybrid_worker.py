#!/usr/bin/env python3
"""
v2_hybrid_worker.py — Single-mask backtest wrapper for Supertrend+TrendType hybrid sweep.

Runs a single mask (0-127) of the v2_hybrid filter sweep. Called by orchestrator.
Supports idempotency: skips if output already exists and is valid JSON.

Usage (inside Jesse container):
    python3 v2_hybrid_worker.py --mask 0 --output-dir /result/v2_hybrid --dry-run
    python3 v2_hybrid_worker.py --mask 5 --output-dir /result/v2_hybrid

Output:
    Prints JSON result to stdout on success:
    {"mask": N, "status": "ok", "data": {...stats fields...}}

    Or error JSON on failure:
    {"mask": N, "status": "error", "error": "..."}
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


# ── Champion hyperparameters ──────────────────────────────────────────────────
# These are the baseline Supertrend champion values + TrendType params.
# Replace with actual champion HP from your sweep results.

SUPERTREND_CHAMPION_HP = {
    "st_factor": 2.5,
    "st_period": 6,
}

TRENDTYPE_HP = {
    "fast_ema_len": 7,
    "slow_ema_len": 20,
    "direction_ema_len": 200,
    "atr_mult": 3.0,
    "atr_len": 9,
    "atr_ma_len": 20,
    "di_len": 9,
    "smooth": 1,
}


def _build_hp_json(mask: int) -> str:
    """Build hyperparameter JSON string with filter_mask."""
    hp = {**SUPERTREND_CHAMPION_HP, **TRENDTYPE_HP, "filter_mask": mask}
    return json.dumps(hp)


def _check_idempotency(output_dir: Path, mask: int) -> bool:
    """Check if mask output already exists and is valid JSON."""
    mask_dir = output_dir / f"mask_{mask}"
    stats_path = mask_dir / "stats.json"

    if not stats_path.exists():
        return False

    try:
        with open(stats_path, 'r') as f:
            json.load(f)
        return True  # Valid JSON exists
    except (json.JSONDecodeError, IOError):
        return False


def _run_backtest(mask: int, output_dir: Path, hp_json: str, dry_run: bool = False) -> bool:
    """Run the backtest subprocess. Returns True on success.

    Each subprocess runs from an isolated temp directory so that
    is_jesse_project() never sees a pre-existing storage/ dir, which would
    trigger Jesse's redis.py to attempt a Redis connection at import time.
    """
    import tempfile
    import shutil
    mask_dir = output_dir / f"mask_{mask}"

    cmd = [
        "python3", "/app/scripts/runners/run_intrabar_backtest.py",
        "--strategy", "SupertrendTrendType1dFilterStrategy",
        "--timeframe", "4h",
        "--variant", "long_only",
        "--leverage", "3",
        "--start", "2017-08-18",
        "--end", "2026-04-30",
        "--balance", "10000",
        "--fee", "0.00055",
        "--hp-json", hp_json,
        "--output", str(mask_dir),
    ]

    if dry_run:
        print(f"[DRY-RUN mask={mask}] {' '.join(cmd)}")
        return True

    # Isolated CWD: contains only 'strategies' symlink so is_jesse_project()
    # returns False (no 'storage/' yet). Jesse creates storage/ here, not in /app.
    run_dir = Path(tempfile.mkdtemp(prefix=f"jesse_mask{mask}_"))
    (run_dir / "strategies").symlink_to("/app/strategies")

    try:
        subprocess.run(cmd, check=False, cwd=str(run_dir))
        stats_path = mask_dir / "stats.json"
        if stats_path.exists():
            return True
        print(f"[ERROR mask={mask}] stats.json not found after backtest", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[ERROR mask={mask}] Backtest failed: {e}", file=sys.stderr)
        return False
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def _parse_stats(output_dir: Path, mask: int) -> dict | None:
    """Parse stats.json and return relevant fields. Returns None on error."""
    stats_path = output_dir / f"mask_{mask}" / "stats.json"

    if not stats_path.exists():
        return None

    try:
        with open(stats_path, 'r') as f:
            stats = json.load(f)

        raw = stats.get("raw_metrics", {})
        return {
            "finishing_balance": raw.get("finishing_balance", stats.get("starting_balance", 10000)),
            "max_drawdown_pct": stats.get("max_drawdown_pct"),
            "sharpe_ratio": stats.get("sharpe_ratio"),
            "number_of_trades": stats.get("total_trades"),
            "cagr": stats.get("cagr_pct"),
            "win_rate_pct": stats.get("win_rate_pct"),
            "profit_factor": stats.get("profit_factor"),
        }
    except (json.JSONDecodeError, IOError, KeyError) as e:
        print(f"[ERROR mask={mask}] Failed to parse stats.json: {e}", file=sys.stderr)
        return None


def main():
    p = argparse.ArgumentParser(description="Single-mask v2_hybrid backtest worker")
    p.add_argument("--mask", type=int, required=True, help="Filter mask (0-127)")
    p.add_argument("--output-dir", type=str, required=True, help="Output directory (host path)")
    p.add_argument("--dry-run", action="store_true", help="Print command only, don't run")
    args = p.parse_args()

    mask = args.mask
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Check idempotency — still output JSON so caller can parse result
    if _check_idempotency(output_dir, mask):
        stats = _parse_stats(output_dir, mask)
        if stats is not None:
            result = {"mask": mask, "status": "ok", "data": stats, "skipped": True}
            print(json.dumps(result))
            return 0
        # stats.json invalid — fall through to re-run

    # 2. Build HP JSON
    hp_json = _build_hp_json(mask)

    # 3. Run backtest
    if not _run_backtest(mask, output_dir, hp_json, dry_run=args.dry_run):
        result = {"mask": mask, "status": "error", "error": "backtest execution failed"}
        print(json.dumps(result))
        return 1

    # 4. Parse result (skip in dry-run)
    if args.dry_run:
        result = {"mask": mask, "status": "ok", "data": {"note": "dry-run, no actual data"}}
        print(json.dumps(result))
        return 0

    stats = _parse_stats(output_dir, mask)
    if stats is None:
        result = {"mask": mask, "status": "error", "error": "stats.json parse failed"}
        print(json.dumps(result))
        return 1

    # 5. Print success result
    result = {"mask": mask, "status": "ok", "data": stats}
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
