#!/usr/bin/env python3
"""
B-2: Intrabar backtest with taker fee (0.00055) vs maker fee (0.0002)
Combo 18 (SupertrendStrategy, 4h, long_only)
5 periods comparison
"""

import subprocess
import json
import sys
import time
import statistics
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Check if Jesse is available (runner requirement)
try:
    import jesse
    JESSE_AVAILABLE = True
except ImportError:
    JESSE_AVAILABLE = False

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
INTRABAR_BASE = ROOT / 'results/intrabar/supertrend/4h/long_only/combo_18'
OUT_BASE = ROOT / 'results/validation_phase5/b2_intrabar_fee'
HP = {
    "st_factor": 2.5,
    "st_period": 6,
    "fast_ema_len": 7,
    "slow_ema_len": 20,
    "direction_ema_len": 200,
    "atr_mult": 3.0
}
RUNNER = str(ROOT / 'scripts/runners/run_intrabar_backtest.py')
FEE_TAKER = 0.00055
FEE_MAKER = 0.0002

PERIODS = {
    'p0': ('2018-04-01', '2020-06-30'),
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

def run_period(period, start, end):
    """Run intrabar backtest for a single period with taker fee."""
    out_dir = OUT_BASE / period
    out_dir.mkdir(parents=True, exist_ok=True)
    stats_path = out_dir / 'stats.json'

    # Idempotent: skip if already exists
    if stats_path.exists():
        return {
            "period": period,
            "status": "SKIP",
            "stats": json.loads(stats_path.read_text())
        }

    cmd = [
        sys.executable,
        RUNNER,
        '--strategy', 'SupertrendStrategy',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--start', start,
        '--end', end,
        '--hp-json', json.dumps(HP),
        '--fee', str(FEE_TAKER),
        '--output', str(out_dir),
    ]

    t0 = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        return {
            "period": period,
            "status": "TIMEOUT",
            "elapsed": elapsed
        }

    if result.returncode != 0:
        return {
            "period": period,
            "status": "FAIL",
            "stderr": (result.stderr + '\n' + result.stdout)[-500:],
            "elapsed": elapsed
        }

    if stats_path.exists():
        return {
            "period": period,
            "status": "OK",
            "stats": json.loads(stats_path.read_text()),
            "elapsed": elapsed
        }

    return {
        "period": period,
        "status": "NO_OUTPUT",
        "elapsed": elapsed
    }

# Check environment
if not JESSE_AVAILABLE:
    print("[B-2] B-2 requires Docker backtest container (Jesse not available)")
    print("[B-2] Creating placeholder results...")

# Load existing maker-fee results for comparison
print("[B-2] Loading existing maker-fee results...")
maker_results = {}
for p in PERIODS:
    existing = INTRABAR_BASE / p / 'stats.json'
    if existing.exists():
        s = json.loads(existing.read_text())
        maker_results[p] = {
            "cagr": s.get('cagr_pct') or s.get('annual_return_pct'),
            "mdd": s.get('max_drawdown_pct'),
            "trades": s.get('total_trades'),
            "verdict": s.get('verdict'),
        }
        print(f"  {p}: CAGR={maker_results[p]['cagr']:.2f}% MDD={maker_results[p]['mdd']:.2f}%")
    else:
        print(f"  {p}: NOT FOUND")

# Run taker-fee intrabar
print("\n[B-2] Running taker-fee intrabar (fee=0.00055)...")
OUT_BASE.mkdir(parents=True, exist_ok=True)

if JESSE_AVAILABLE:
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(run_period, p, start, end): p
            for p, (start, end) in PERIODS.items()
        }
        taker_results = {}
        for fut in as_completed(futures):
            r = fut.result()
            p = r['period']
            print(f"  [{p}] {r['status']} elapsed={r.get('elapsed', 0):.1f}s")
            if r['status'] in ('OK', 'SKIP') and 'stats' in r:
                s = r['stats']
                taker_results[p] = {
                    "cagr": s.get('cagr_pct') or s.get('annual_return_pct'),
                    "mdd": s.get('max_drawdown_pct'),
                    "trades": s.get('total_trades'),
                    "verdict": s.get('verdict'),
                }
            else:
                taker_results[p] = {
                    "status": r['status'],
                    "error": r.get('stderr', '')
                }
else:
    taker_results = {p: {"status": "PENDING_DOCKER"} for p in PERIODS}

# Build comparison
print("\n[B-2] Building comparison...")
comparison = {}
for p in PERIODS:
    mk = maker_results.get(p, {})
    tk = taker_results.get(p, {})
    comparison[p] = {
        "maker_fee_cagr": mk.get('cagr'),
        "maker_fee_mdd": mk.get('mdd'),
        "maker_verdict": mk.get('verdict'),
        "taker_fee_cagr": tk.get('cagr'),
        "taker_fee_mdd": tk.get('mdd'),
        "taker_verdict": tk.get('verdict'),
        "cagr_delta": round(tk.get('cagr', 0) - mk.get('cagr', 0), 2) if tk.get('cagr') else None,
        "mdd_delta": round(tk.get('mdd', 0) - mk.get('mdd', 0), 2) if tk.get('mdd') else None,
    }

# Summary stats
taker_mdds = [
    v['taker_fee_mdd'] for v in comparison.values()
    if v.get('taker_fee_mdd') is not None
]
summary_out = {
    "comparison": comparison,
    "taker_mean_mdd": round(statistics.mean(taker_mdds), 2) if taker_mdds else None,
    "taker_worst_mdd": round(min(taker_mdds), 2) if taker_mdds else None,
    "taker_periods_failing_minus30": sum(1 for m in taker_mdds if m < -30),
}

# Write JSON output
out_json = OUT_BASE.parent / 'b2_intrabar_fee.json'
out_json.write_text(json.dumps(summary_out, indent=2))
print(f"  -> {out_json}")

# Write Markdown comparison
md = "# B-2 Fee-Inclusive Intrabar Comparison\n\n"
md += "Maker fee=0.0002 vs Taker fee=0.00055 (Combo 18: SupertrendStrategy 4h long_only)\n\n"
md += "| Period | Maker CAGR | Maker MDD | Taker CAGR | Taker MDD | ΔCAGR | ΔMDD |\n"
md += "|---|---|---|---|---|---|---|\n"
for p in sorted(comparison.keys()):
    c = comparison[p]
    maker_cagr = f"{c.get('maker_fee_cagr', 0):.2f}" if c.get('maker_fee_cagr') is not None else "—"
    maker_mdd = f"{c.get('maker_fee_mdd', 0):.2f}" if c.get('maker_fee_mdd') is not None else "—"
    taker_cagr = f"{c.get('taker_fee_cagr', 0):.2f}" if c.get('taker_fee_cagr') is not None else "—"
    taker_mdd = f"{c.get('taker_fee_mdd', 0):.2f}" if c.get('taker_fee_mdd') is not None else "—"
    cagr_delta = f"{c['cagr_delta']:.2f}" if c.get('cagr_delta') is not None else "—"
    mdd_delta = f"{c['mdd_delta']:.2f}" if c.get('mdd_delta') is not None else "—"
    md += f"| {p} | {maker_cagr}% | {maker_mdd}% | {taker_cagr}% | {taker_mdd}% | {cagr_delta} | {mdd_delta} |\n"

md += f"\n**Summary Stats (Taker Fee)**\n"
md += f"- Mean MDD: {summary_out['taker_mean_mdd']}%\n"
md += f"- Worst MDD: {summary_out['taker_worst_mdd']}%\n"
md += f"- Periods < -30% MDD: {summary_out['taker_periods_failing_minus30']}\n"

out_md = OUT_BASE.parent / 'b2_intrabar_fee.md'
out_md.write_text(md)
print(f"  -> {out_md}")

print("\n[B-2] Complete")
print("\n" + "="*60)
print("COMPARISON TABLE (Maker fee=0.0002 vs Taker fee=0.00055)")
print("="*60)
for p in sorted(comparison.keys()):
    c = comparison[p]
    print(f"{p}:")
    maker_cagr = c.get('maker_fee_cagr')
    maker_mdd = c.get('maker_fee_mdd')
    taker_cagr = c.get('taker_fee_cagr')
    taker_mdd = c.get('taker_fee_mdd')

    if maker_cagr is not None and maker_mdd is not None:
        print(f"  Maker: CAGR={maker_cagr:.2f}% MDD={maker_mdd:.2f}%")
    else:
        print(f"  Maker: CAGR=N/A MDD=N/A")

    if taker_cagr is not None and taker_mdd is not None:
        print(f"  Taker: CAGR={taker_cagr:.2f}% MDD={taker_mdd:.2f}%")
    else:
        print(f"  Taker: CAGR=N/A MDD=N/A (FAILED)")

    if c.get('cagr_delta') is not None:
        print(f"  Delta: ΔCAGR={c['cagr_delta']:+.2f}% ΔMDD={c['mdd_delta']:+.2f}%")
