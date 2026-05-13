#!/usr/bin/env python3
"""
B-1: Walk-forward OOS validation for combo_18 (SupertrendStrategy, 4h, long_only)
Fixed hyperparameters: st_factor=2.5, st_period=6, fast_ema_len=7, slow_ema_len=20, direction_ema_len=200, atr_mult=3.0
7 windows, 12-month each, 6-month step.
"""

import subprocess
import json
import sys
import time
import statistics
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
OUT_BASE = ROOT / 'results/validation_phase5/b1_walkforward'
HP = {
    "st_factor": 2.5,
    "st_period": 6,
    "fast_ema_len": 7,
    "slow_ema_len": 20,
    "direction_ema_len": 200,
    "atr_mult": 3.0,
}
RUNNER = str(ROOT / 'scripts/runners/run_intrabar_backtest.py')
FEE = 0.00055

WINDOWS = [
    ("W1", "2022-04-01", "2023-04-01"),
    ("W2", "2022-10-01", "2023-10-01"),
    ("W3", "2023-04-01", "2024-04-01"),
    ("W4", "2023-10-01", "2024-10-01"),
    ("W5", "2024-04-01", "2025-04-01"),
    ("W6", "2024-10-01", "2025-10-01"),
    ("W7", "2025-04-01", "2026-04-01"),
]


def run_window(label, start, end):
    """Run a single OOS window. Idempotent: skip if stats.json exists."""
    out_dir = OUT_BASE / label
    out_dir.mkdir(parents=True, exist_ok=True)
    stats_path = out_dir / 'stats.json'

    # Idempotent check
    if stats_path.exists():
        try:
            return {
                "label": label,
                "status": "SKIP",
                "stats": json.loads(stats_path.read_text()),
            }
        except Exception as e:
            print(f"[{label}] Warn: stats.json exists but unreadable: {e}")

    cmd = [
        sys.executable,
        RUNNER,
        '--strategy', 'SupertrendStrategy',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--start', start,
        '--end', end,
        '--hp-json', json.dumps(HP),
        '--fee', str(FEE),
        '--output', str(out_dir),
    ]

    t0 = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        return {
            "label": label,
            "status": "TIMEOUT",
            "elapsed": 600,
        }

    if result.returncode != 0:
        return {
            "label": label,
            "status": "FAIL",
            "stderr": result.stderr[-300:],
            "elapsed": elapsed,
        }

    # Check for stats.json
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text())
            return {
                "label": label,
                "status": "OK",
                "stats": stats,
                "elapsed": elapsed,
            }
        except Exception as e:
            return {
                "label": label,
                "status": "JSON_PARSE_ERROR",
                "error": str(e),
                "elapsed": elapsed,
            }

    # Try finding any stats json in output dir
    jsons = list(out_dir.glob('*.json'))
    return {
        "label": label,
        "status": "NO_OUTPUT",
        "files": [f.name for f in jsons],
        "elapsed": elapsed,
    }


def main():
    print(f"B-1 Walk-Forward OOS Validation")
    print(f"  Strategy: SupertrendStrategy, 4h, long_only")
    print(f"  HP: {HP}")
    print(f"  Output: {OUT_BASE}")
    print(f"  Windows: 7 x 12-month (6-month step)")
    print()

    # Quick sanity check: run one test window to detect polars requirement early
    print("Sanity check: testing W1 ...")

    # Run a single window to check for polars requirement
    test_out_dir = OUT_BASE / "W1"
    test_out_dir.mkdir(parents=True, exist_ok=True)
    test_cmd = [
        sys.executable,
        RUNNER,
        '--strategy', 'SupertrendStrategy',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--start', '2022-04-01',
        '--end', '2023-04-01',
        '--hp-json', json.dumps(HP),
        '--fee', str(FEE),
        '--output', str(test_out_dir),
    ]
    test_result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=10)

    # Check for polars requirement
    combined_output = test_result.stdout + test_result.stderr
    if 'polars required' in combined_output or 'ImportError' in combined_output:
        print("\nERROR: Runner requires polars library (Docker only)")
        print("\nRun B-1 inside Docker backtest container:")
        print("  docker compose -f backtest/docker/docker-compose.yml run --rm backtest \\")
        print("    python3 /app/scripts/validation/b1_walkforward_oos.py")

        # Create placeholder result
        summary_json = {
            "status": "PENDING_DOCKER",
            "message": "B-1 requires Docker backtest container (polars dependency)",
            "command": "docker compose -f backtest/docker/docker-compose.yml run --rm backtest python3 /app/scripts/validation/b1_walkforward_oos.py",
        }
        summary_json_path = OUT_BASE.parent / 'b1_summary.json'
        summary_json_path.parent.mkdir(parents=True, exist_ok=True)
        summary_json_path.write_text(json.dumps(summary_json, indent=2))
        print(f"\nPlaceholder: {summary_json_path}")
        return


    # Sanity check passed, run all windows sequentially (ProcessPoolExecutor unreliable in Docker)
    print("Sanity check: OK, continuing with all 7 windows...")
    results = []

    for label, start, end in WINDOWS:
        r = run_window(label, start, end)
        results.append(r)
        elapsed_str = f" elapsed={r.get('elapsed',0):.1f}s" if 'elapsed' in r else ""
        print(f"[{r['label']}] {r['status']}{elapsed_str}")

    # Summarize results
    print("\n--- Summary ---")
    summary_rows = []
    for r in sorted(results, key=lambda x: x['label']):
        if r['status'] in ('OK', 'SKIP') and 'stats' in r:
            s = r['stats']
            cagr = s.get('cagr_pct') or s.get('annual_return_pct')
            mdd = s.get('max_drawdown_pct')
            sharpe = s.get('sharpe_ratio')
            trades = s.get('total_trades')
            summary_rows.append({
                "window": r['label'],
                "cagr_pct": cagr,
                "mdd_pct": mdd,
                "sharpe": sharpe,
                "trades": trades,
            })
            print(
                f"[{r['label']}] CAGR={cagr:.1f}%, MDD={mdd:.1f}%, Sharpe={sharpe:.2f}, Trades={trades}"
            )
        else:
            err_msg = r.get('stderr', '')[-50:] if 'stderr' in r else ""
            summary_rows.append({
                "window": r['label'],
                "status": r['status'],
                "error": err_msg,
            })
            print(f"[{r['label']}] {r['status']}")

    # Compute statistics
    ok_rows = [row for row in summary_rows if 'cagr_pct' in row and row.get('cagr_pct') is not None]
    n_completed = len(ok_rows)
    n_negative = sum(1 for r in ok_rows if r['cagr_pct'] < 0)
    mean_cagr = round(statistics.mean(r['cagr_pct'] for r in ok_rows), 2) if ok_rows else None
    worst_mdd = round(min(r['mdd_pct'] for r in ok_rows), 2) if ok_rows else None
    mean_mdd = round(statistics.mean(r['mdd_pct'] for r in ok_rows), 2) if ok_rows else None

    summary_json = {
        "windows": summary_rows,
        "n_completed": n_completed,
        "n_negative_cagr": n_negative,
        "mean_cagr": mean_cagr,
        "worst_mdd": worst_mdd,
        "mean_mdd": mean_mdd,
    }

    # Write summary JSON
    summary_json_path = OUT_BASE.parent / 'b1_summary.json'
    summary_json_path.write_text(json.dumps(summary_json, indent=2))
    print(f"\nSummary JSON: {summary_json_path}")

    # Write markdown summary table
    md = "# B-1 Walk-Forward OOS Summary\n\n"
    md += "| Window | Start | End | CAGR (%) | MDD (%) | Sharpe | Trades |\n"
    md += "|--------|-------|-----|----------|---------|--------|--------|\n"
    for w, (label, start, end) in zip(summary_rows, WINDOWS):
        if 'cagr_pct' in w:
            md += (
                f"| {w['window']} | {start} | {end} | "
                f"{w['cagr_pct']:.1f} | {w['mdd_pct']:.1f} | "
                f"{w.get('sharpe', 0):.2f} | {w.get('trades', 0)} |\n"
            )
        else:
            md += f"| {w['window']} | {start} | {end} | {w.get('status', 'FAIL')} | — | — | — |\n"

    md += f"\n**Summary**:\n"
    md += f"- Completed: {n_completed}/7\n"
    md += f"- Negative CAGR windows: {n_negative}\n"
    md += f"- Mean CAGR: {mean_cagr}%\n"
    md += f"- Worst MDD: {worst_mdd}%\n"
    md += f"- Mean MDD: {mean_mdd}%\n"

    summary_md_path = OUT_BASE.parent / 'b1_summary.md'
    summary_md_path.write_text(md)
    print(f"Summary Markdown: {summary_md_path}")

    print("\nB-1 complete.")


if __name__ == '__main__':
    main()
