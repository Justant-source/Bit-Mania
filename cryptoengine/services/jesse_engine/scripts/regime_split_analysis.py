"""
Phase 7.5 — Market Regime Split Analysis.

Runs the same strategy over 3 market regime periods and compares performance.

Default regimes:
  bull:       2023-04-01 → 2023-12-31  (BTC: $28k → $44k, +57%)
  transition: 2024-01-01 → 2024-12-31  (ETF launch, BTC: $44k → $94k, +113%)
  compressed: 2025-01-01 → 2026-04-01  (post-ETF compression, lower volatility)

Usage:
    python scripts/regime_split_analysis.py --strategy IntradaySeasonality \
        --regimes "2023-04-01:2023-12-31:bull,2024-01-01:2024-12-31:transition,2025-01-01:2026-04-01:compressed"
"""

from __future__ import annotations
import argparse
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

STORAGE_DIR = Path("storage/regime_split")
JESSE_EXCHANGE  = "Bybit Perpetual"
JESSE_SYMBOL    = "BTCUSDT"
JESSE_TIMEFRAME = "1h"

DEFAULT_REGIMES = "2023-04-01:2023-12-31:bull,2024-01-01:2024-12-31:transition,2025-01-01:2026-04-01:compressed"


def parse_regimes(regimes_str: str) -> list[dict]:
    regimes = []
    for segment in regimes_str.split(","):
        parts = segment.strip().split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid regime format: {segment}. Use start:end:label")
        regimes.append({"start": parts[0], "end": parts[1], "label": parts[2]})
    return regimes


def run_backtest(strategy: str, start: str, end: str, label: str) -> dict:
    result_file = STORAGE_DIR / f"{strategy}_{label}.json"
    result_file.parent.mkdir(parents=True, exist_ok=True)

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
            return {"error": proc.stderr[-300:], "regime": label}
        if result_file.exists():
            data = json.loads(result_file.read_text())
            data["regime"] = label
            return data
        return {"error": "No result file", "regime": label}
    except Exception as exc:
        return {"error": str(exc), "regime": label}


def write_report(strategy: str, results: list[dict]) -> str:
    lines = [
        f"# Regime Split Analysis: {strategy}",
        f"",
        f"Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"",
        f"## Performance by Market Regime",
        f"",
        f"| Regime | Period | CAGR | Sharpe | MDD | Trades |",
        f"|--------|--------|------|--------|-----|--------|",
    ]
    for r in results:
        if "error" in r:
            lines.append(f"| {r.get('regime','?')} | ERROR | — | — | — | — |")
        else:
            lines.append(
                f"| {r.get('regime','?')} | "
                f"{r.get('start','?')}→{r.get('end','?')} | "
                f"{r.get('cagr','?')} | "
                f"{r.get('sharpe','?')} | "
                f"{r.get('mdd','?')} | "
                f"{r.get('num_trades','?')} |"
            )
    lines += [
        f"",
        f"## V5 Compressed Regime Check",
        f"",
        "The 2025+ compressed regime is the critical test. "
        "Zero trades or negative Sharpe here = strategy cannot work in current market.",
    ]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", required=True)
    p.add_argument("--regimes",  default=DEFAULT_REGIMES)
    args = p.parse_args()

    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    regimes = parse_regimes(args.regimes)
    print(f"[regime_split] {args.strategy}: {len(regimes)} regime periods")

    results = []
    for r in regimes:
        print(f"  {r['label']}: {r['start']} → {r['end']}")
        res = run_backtest(args.strategy, r["start"], r["end"], r["label"])
        # Attach period info
        res["start"] = r["start"]
        res["end"]   = r["end"]
        results.append(res)

    # Save JSON
    out = STORAGE_DIR / f"{args.strategy}_regime_split.json"
    out.write_text(json.dumps({"strategy": args.strategy, "results": results}, indent=2))

    # Save Markdown
    md = write_report(args.strategy, results)
    (STORAGE_DIR / f"{args.strategy}_regime_split.md").write_text(md)
    print(f"  Saved: {out}")

    # Check compressed regime trades
    compressed = next((r for r in results if r.get("regime") == "compressed"), None)
    if compressed and not compressed.get("error") and compressed.get("num_trades", 0) == 0:
        print("  WARN-3: Zero trades in compressed regime!")


if __name__ == "__main__":
    main()
