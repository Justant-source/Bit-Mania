#!/usr/bin/env python3
"""Live execution-quality audit (plan §1, .temp/2026-08-31_slippage_tripwire_plan.md).

Read-only. Pulls filled orders for strategy_id='supertrend-01' via
`docker compose exec -T postgres psql` (never a direct DB connection — this
mirrors how the plan's own DB investigation was done, and keeps the script
usable from the host without exposing DB credentials), joins each fill to
its triggering 4h signal-close price, and measures the realized slippage +
fee cost against the backtest's execution assumptions.

Excludes: `manual-test-01` fills and the 2026-05-20/05-27 launch-shakedown
fills (market-order fallback while the system was still being validated) —
these are listed separately, not blended into the slippage stats.

Run: python3 backtest/scripts/analysis/live_slippage.py
  (must run where `docker compose` resolves cryptoengine/docker-compose.yml,
   i.e. cwd = repo root or cryptoengine/ — the script cds into cryptoengine/)
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
CRYPTOENGINE = REPO / "cryptoengine"

sys.path.insert(0, str(REPO / "backtest/scripts/analysis/v12"))
import replay_lib as L  # noqa: E402
import _replay_supertrend as RS  # noqa: E402

EXCLUDED_STRATEGY_IDS = ("manual-test-01",)
SHAKEDOWN_CUTOFF = "2026-07-01"  # supertrend-01 fills before this are launch shakedown


def psql(sql: str) -> list[dict]:
    """Run one read-only SQL statement via docker compose exec, parse CSV output."""
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql",
         "-U", "cryptoengine", "-d", "cryptoengine", "--csv", "-c", sql],
        cwd=str(CRYPTOENGINE), capture_output=True, text=True, check=True,
    )
    return list(csv.DictReader(io.StringIO(proc.stdout)))


def fetch_fills() -> list[dict]:
    sql = """
        SELECT o.id, o.created_at, o.side, o.order_type, o.filled_qty, o.filled_price,
               o.fee, o.strategy_id,
               to_char(to_timestamp(floor(extract(epoch from o.created_at)/14400)*14400 - 14400)
                       AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS bar_ts,
               s.price AS signal_price, s.expected_action, s.exit_reason,
               s.fast_ema, s.slow_ema, s.atr_14
        FROM orders o
        LEFT JOIN supertrend_signals s
          ON s.bar_ts = to_timestamp(floor(extract(epoch from o.created_at)/14400)*14400 - 14400)
        WHERE o.status = 'filled'
        ORDER BY o.created_at
    """
    return psql(sql)


def fetch_kill_switch_events() -> list[dict]:
    return psql("SELECT * FROM kill_switch_events ORDER BY id")


def classify(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split into (main analysis set, appendix/shakedown set)."""
    main, appendix = [], []
    for r in rows:
        if r["strategy_id"] in EXCLUDED_STRATEGY_IDS:
            appendix.append(r)
        elif r["created_at"] < SHAKEDOWN_CUTOFF:
            appendix.append(r)
        else:
            main.append(r)
    return main, appendix


def compute_costs(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        side = r["side"]
        filled_qty = float(r["filled_qty"])
        filled_price = float(r["filled_price"])
        fee = float(r["fee"])
        signal_price = float(r["signal_price"]) if r["signal_price"] else None
        sign = 1.0 if side == "buy" else -1.0
        slip_bps = None
        if signal_price:
            slip_bps = sign * (filled_price - signal_price) / signal_price * 1e4
        fee_bps = fee / (filled_qty * filled_price) * 1e4
        out.append({
            **r,
            "filled_qty": filled_qty, "filled_price": filled_price, "fee": fee,
            "signal_price": signal_price, "slip_bps": slip_bps, "fee_bps": fee_bps,
            "total_bps": (slip_bps + fee_bps) if slip_bps is not None else None,
        })
    return out


def pair_roundtrips(rows: list[dict]) -> list[dict]:
    """Pair consecutive buy/sell into round trips (strategy is long-only, one position at a time)."""
    trips = []
    pending = None
    for r in rows:
        if r["side"] == "buy":
            pending = r
        elif r["side"] == "sell" and pending is not None:
            trips.append({
                "entry": pending, "exit": r,
                "roundtrip_bps": (pending["total_bps"] or 0) + (r["total_bps"] or 0),
            })
            pending = None
    return trips


def median(vals):
    v = sorted(vals)
    n = len(v)
    if n == 0:
        return None
    mid = n // 2
    return v[mid] if n % 2 else (v[mid - 1] + v[mid]) / 2


def main():
    fills = fetch_fills()
    main_rows, appendix_rows = classify(fills)
    main_costed = compute_costs(main_rows)
    appendix_costed = compute_costs(appendix_rows)
    trips = pair_roundtrips(main_costed)
    ks_events = fetch_kill_switch_events()

    all_slip = [r["slip_bps"] for r in main_costed if r["slip_bps"] is not None]
    all_fee = [r["fee_bps"] for r in main_costed]
    all_total = [r["total_bps"] for r in main_costed if r["total_bps"] is not None]
    rt_bps = [t["roundtrip_bps"] for t in trips]

    entry_slip = [r["slip_bps"] for r in main_costed if r["side"] == "buy" and r["slip_bps"] is not None]
    exit_slip = [r["slip_bps"] for r in main_costed if r["side"] == "sell" and r["slip_bps"] is not None]

    print(f"n_fills (main) = {len(main_costed)}  n_roundtrips = {len(trips)}  n_appendix = {len(appendix_costed)}")
    print(f"n_kill_switch_events = {len(ks_events)}")
    print()
    print("per-fill:")
    print(f"{'id':>4} {'created_at':<26} {'side':<5} {'signal':>10} {'filled':>10} "
          f"{'slip_bps':>9} {'fee_bps':>8} {'total_bps':>10}")
    for r in main_costed:
        print(f"{r['id']:>4} {r['created_at']:<26} {r['side']:<5} "
              f"{(r['signal_price'] or 0):>10.1f} {r['filled_price']:>10.1f} "
              f"{(r['slip_bps'] or 0):>9.2f} {r['fee_bps']:>8.2f} {(r['total_bps'] or 0):>10.2f}")
    print()
    print(f"slippage bps: median={median(all_slip):.2f}  mean={sum(all_slip)/len(all_slip):.2f}  "
          f"worst={max(all_slip, key=abs):.2f}  best={min(all_slip, key=abs):.2f}")
    print(f"  entry only: median={median(entry_slip):.2f}  mean={sum(entry_slip)/len(entry_slip):.2f}")
    print(f"  exit  only: median={median(exit_slip):.2f}  mean={sum(exit_slip)/len(exit_slip):.2f}")
    print(f"fee bps:      median={median(all_fee):.2f}  mean={sum(all_fee)/len(all_fee):.2f}")
    print(f"roundtrip total cost bps (slip+fee, entry+exit): "
          f"median={median(rt_bps):.2f}  mean={sum(rt_bps)/len(rt_bps):.2f}  worst={max(rt_bps):.2f}  best={min(rt_bps):.2f}")
    print(f"  (backtest assumption: taker 0.055%x2 = 11.0bps, slip 0; "
          f"maker 0.020%x2 = 4.0bps, slip 0)")

    # §A.3: realistic holdout rerun.
    # median case: convert median roundtrip fee_bps component to a fee rate, apply median slip.
    med_fee_bps = median(all_fee)
    med_slip_bps = median([abs(x) for x in all_slip])  # magnitude, matches replay's slip_bps convention
    worst_fee_bps = max(r["fee_bps"] for r in main_costed)
    worst_slip_bps = max(abs(x) for x in all_slip)

    med_fee_frac = med_fee_bps / 1e4
    worst_fee_frac = worst_fee_bps / 1e4

    HS, HE = L.date_ms("2025-01-01"), L.date_ms("2026-08-29")
    t_med, e_med = L.run(L.BASELINE, start_ms=HS, end_ms=HE, fee=med_fee_frac, slip_bps=med_slip_bps)
    m_med = RS._metrics(t_med, e_med)
    t_worst, e_worst = L.run(L.BASELINE, start_ms=HS, end_ms=HE, fee=worst_fee_frac, slip_bps=worst_slip_bps)
    m_worst = RS._metrics(t_worst, e_worst)

    print()
    print(f"§A.3 realistic holdout (measured fee={med_fee_bps:.2f}bps, slip={med_slip_bps:.2f}bps): "
          f"net={m_med['net_pct']:+.2f}%  MDD={m_med['mdd']:.2f}%")
    print(f"§A.3 worst-case holdout (measured fee={worst_fee_bps:.2f}bps, slip={worst_slip_bps:.2f}bps): "
          f"net={m_worst['net_pct']:+.2f}%  MDD={m_worst['mdd']:.2f}%")

    print()
    print("appendix (excluded — manual-test / launch shakedown):")
    for r in appendix_costed:
        print(f"  id={r['id']} {r['created_at']} {r['side']:<5} {r['order_type']:<6} "
              f"strategy_id={r['strategy_id']} filled_price={r['filled_price']:.1f}")

    result = {
        "n_fills": len(main_costed), "n_roundtrips": len(trips),
        "slip_median_bps": median(all_slip), "slip_worst_bps": max(all_slip, key=abs),
        "fee_median_bps": med_fee_bps, "fee_worst_bps": worst_fee_bps,
        "roundtrip_median_bps": median(rt_bps), "roundtrip_worst_bps": max(rt_bps),
        "holdout_net_median_case": m_med["net_pct"], "holdout_net_worst_case": m_worst["net_pct"],
        "holdout_mdd_median_case": m_med["mdd"], "holdout_mdd_worst_case": m_worst["mdd"],
    }
    out_path = REPO / "backtest/results/2026-08-31/live_slippage_summary.json"
    out_path.write_text(json.dumps(result, indent=2, default=float))
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
