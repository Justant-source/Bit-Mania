"""
Phase 7.5 — Walk-Forward Analysis for Jesse Strategies.

Splits the full date range into IS (in-sample) + OOS (out-of-sample) windows
and runs Jesse backtest for each window.

Default: train 365 days / test 180 days / slide 90 days

Usage:
    python scripts/walk_forward.py --strategy IntradaySeasonality \
        --start 2023-04-01 --end 2026-04-01 \
        --train-days 365 --test-days 180 --slide-days 90

Output:
    storage/walk_forward/<strategy>_wf_summary.json
    storage/walk_forward/<strategy>_wf_summary.md
"""

from __future__ import annotations
import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

JESSE_EXCHANGE = "Bybit Perpetual"
JESSE_SYMBOL   = "BTCUSDT"
JESSE_TIMEFRAME = "1h"
STORAGE_DIR = Path("storage/walk_forward")


def daterange_windows(
    start: str, end: str,
    train_days: int, test_days: int, slide_days: int
) -> list[dict]:
    """Generate IS+OOS window pairs."""
    windows = []
    s = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    while True:
        train_end = s + timedelta(days=train_days)
        test_end  = train_end + timedelta(days=test_days)
        if test_end > e:
            break
        windows.append({
            "is_start":  s.strftime("%Y-%m-%d"),
            "is_end":    train_end.strftime("%Y-%m-%d"),
            "oos_start": train_end.strftime("%Y-%m-%d"),
            "oos_end":   test_end.strftime("%Y-%m-%d"),
        })
        s += timedelta(days=slide_days)

    return windows


def run_jesse_backtest(strategy: str, start: str, end: str) -> dict:
    """
    Run a Jesse backtest using the Python API (Jesse 1.x).
    Returns parsed result dict or {'error': str} on failure.
    """
    result_file = STORAGE_DIR / f"{strategy}_{start}_{end}.json"
    result_file.parent.mkdir(parents=True, exist_ok=True)

    # Call run_backtest.py script via docker compose
    cmd = [
        "docker", "compose", "run", "--rm", "jesse",
        "python", "scripts/run_backtest.py",
        "--strategy", strategy,
        "--start", start,
        "--end", end,
        "--output", str(result_file),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0:
            return {"error": proc.stderr[-500:]}
        if result_file.exists():
            with open(result_file) as f:
                return json.load(f)
        return {"error": "Result file not created"}
    except subprocess.TimeoutExpired:
        return {"error": "Backtest timed out after 600s"}
    except Exception as exc:
        return {"error": str(exc)}


def compute_wf_ratio(is_results: list[dict], oos_results: list[dict]) -> dict:
    """Compute IS vs OOS Sharpe ratios and degradation ratio."""
    def avg_sharpe(results: list[dict]) -> float:
        valid = [float(r["sharpe"]) for r in results
                 if "sharpe" in r and r.get("sharpe") is not None]
        return sum(valid) / len(valid) if valid else 0.0

    is_sharpe  = avg_sharpe(is_results)
    oos_sharpe = avg_sharpe(oos_results)
    ratio      = (oos_sharpe / is_sharpe) if is_sharpe != 0 else 0.0

    return {
        "avg_is_sharpe":  round(is_sharpe, 4),
        "avg_oos_sharpe": round(oos_sharpe, 4),
        "oos_is_ratio":   round(ratio, 4),
        "pass": ratio >= 0.6,  # V5 criterion: OOS ≥ 0.6 × IS
    }


def write_markdown_report(strategy: str, windows: list[dict],
                           is_results: list[dict], oos_results: list[dict],
                           wf_summary: dict) -> str:
    lines = [
        f"# Walk-Forward Report: {strategy}",
        f"",
        f"Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Avg IS Sharpe  | {wf_summary['avg_is_sharpe']} |",
        f"| Avg OOS Sharpe | {wf_summary['avg_oos_sharpe']} |",
        f"| OOS/IS Ratio   | {wf_summary['oos_is_ratio']} |",
        f"| V5 Criterion (≥0.6) | {'✓ PASS' if wf_summary['pass'] else '✗ FAIL'} |",
        f"",
        f"## Window Results",
        f"",
        f"| Window | IS Period | OOS Period | IS Sharpe | OOS Sharpe |",
        f"|--------|-----------|------------|-----------|------------|",
    ]
    for i, (w, is_r, oos_r) in enumerate(zip(windows, is_results, oos_results)):
        is_sh  = is_r.get("sharpe",  "ERR")
        oos_sh = oos_r.get("sharpe", "ERR")
        lines.append(
            f"| {i+1} | {w['is_start']}→{w['is_end']} | "
            f"{w['oos_start']}→{w['oos_end']} | {is_sh} | {oos_sh} |"
        )
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="Walk-Forward Analysis for Jesse strategies")
    p.add_argument("--strategy",    required=True)
    p.add_argument("--start",       default="2023-04-01")
    p.add_argument("--end",         default="2026-04-01")
    p.add_argument("--train-days",  type=int, default=365)
    p.add_argument("--test-days",   type=int, default=180)
    p.add_argument("--slide-days",  type=int, default=90)
    args = p.parse_args()

    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    windows = daterange_windows(
        args.start, args.end,
        args.train_days, args.test_days, args.slide_days
    )
    print(f"[walk_forward] {args.strategy}: {len(windows)} windows")

    is_results, oos_results = [], []
    for i, w in enumerate(windows):
        print(f"  Window {i+1}/{len(windows)}: IS {w['is_start']}→{w['is_end']}, OOS {w['oos_start']}→{w['oos_end']}")
        is_r  = run_jesse_backtest(args.strategy, w["is_start"],  w["is_end"])
        oos_r = run_jesse_backtest(args.strategy, w["oos_start"], w["oos_end"])
        is_results.append(is_r)
        oos_results.append(oos_r)

    wf_summary = compute_wf_ratio(is_results, oos_results)
    print(f"\n  WF Summary: IS Sharpe={wf_summary['avg_is_sharpe']}, "
          f"OOS Sharpe={wf_summary['avg_oos_sharpe']}, "
          f"Ratio={wf_summary['oos_is_ratio']} "
          f"→ {'PASS' if wf_summary['pass'] else 'FAIL'}")

    # Save JSON
    out_json = STORAGE_DIR / f"{args.strategy}_wf_summary.json"
    with open(out_json, "w") as f:
        json.dump({
            "strategy": args.strategy,
            "windows": windows,
            "is_results": is_results,
            "oos_results": oos_results,
            "summary": wf_summary,
        }, f, indent=2)

    # Save Markdown
    out_md = STORAGE_DIR / f"{args.strategy}_wf_summary.md"
    md_content = write_markdown_report(
        args.strategy, windows, is_results, oos_results, wf_summary
    )
    out_md.write_text(md_content)
    print(f"  Saved: {out_json}, {out_md}")


if __name__ == "__main__":
    main()
