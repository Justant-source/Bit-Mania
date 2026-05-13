#!/usr/bin/env python3
"""
apply_realistic_costs_7strategies.py

Compute post-hoc cost adjustments (fee delta + funding fee) for all
results/7-strategies/<strat>/<tf>/<variant>/ entries.

Cost model (per-trade, compounded):
  notional_t  = qty_t * entry_price_t     (leverage already baked into qty)
  fund_usd_t  = notional_t * Σ rate_i * funding_sign
                where rate_i are all 8h funding events in [opened_at, closed_at)
  fee_usd_t   = notional_t * FEE_DELTA_PER_SIDE * 2
  net_t       = raw_pnl_t - fee_usd_t - fund_usd_t

Compounded equity simulation (mirrors JS balanceSim):
  raw_eq = adj_eq = STARTING_BALANCE
  for each trade (sorted by opened_at):
      scale    = adj_eq / raw_eq          (position scale factor in adj world)
      adj_pnl  = net_t * scale            (adj world: smaller position → scaled cost+pnl)
      raw_eq  += raw_pnl_t
      adj_eq  += adj_pnl

adj_cagr is derived from final adj_eq.

Funding source:
  backtest/data/funding/BTCUSDT_8h.parquet
  Per-event (8h) lookup: Σ rates for events in trade's hold window.
  Zero-rate rows (pre-2019-09 before Binance/Bybit launch) use overall fallback mean.

Periods:
  2021 ~ 2025 (yearly)  +  post21_full (2021-01-01 ~ 2026-04-30)

Output:
  backtest/results/adjusted_costs_7strategies/all_adjusted_results_7s.json

Exit code: 0 if no errors, 1 on fatal failure.
"""

import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# ── Paths ─────────────────────────────────────────────────────────────────────
BT_ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_7S = BT_ROOT / "results" / "7-strategies"
FUNDING_8H_PARQUET = BT_ROOT / "data" / "funding" / "BTCUSDT_8h.parquet"
OUT_DIR = BT_ROOT / "results" / "adjusted_costs_7strategies"
OUT_JSON = OUT_DIR / "all_adjusted_results_7s.json"

# ── Cost model constants ───────────────────────────────────────────────────────
FEE_DELTA_PER_SIDE = (0.055 - 0.020) / 100.0  # 0.00035 (maker→taker delta)
STARTING_BALANCE = 10_000.0

# ── Periods ────────────────────────────────────────────────────────────────────
def _ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

POST21_PERIODS = {
    "2021":       (_ms(datetime(2021, 1,  1)), _ms(datetime(2021, 12, 31, 23, 59, 59))),
    "2022":       (_ms(datetime(2022, 1,  1)), _ms(datetime(2022, 12, 31, 23, 59, 59))),
    "2023":       (_ms(datetime(2023, 1,  1)), _ms(datetime(2023, 12, 31, 23, 59, 59))),
    "2024":       (_ms(datetime(2024, 1,  1)), _ms(datetime(2024, 12, 31, 23, 59, 59))),
    "2025":       (_ms(datetime(2025, 1,  1)), _ms(datetime(2026,  4, 30, 23, 59, 59))),
    "post21_full":(_ms(datetime(2021, 1,  1)), _ms(datetime(2026,  4, 30, 23, 59, 59))),
}

PERIOD_YEARS = {
    "2021":        1.0,
    "2022":        1.0,
    "2023":        1.0,
    "2024":        1.0,
    "2025":        (datetime(2026, 4, 30) - datetime(2025, 1, 1)).days / 365.25,  # ~1.33
    "post21_full": (datetime(2026, 4, 30) - datetime(2021, 1, 1)).days / 365.25,  # ~5.33
}


# ── Utilities ──────────────────────────────────────────────────────────────────

def extract_base_variant(variant: str) -> str:
    return re.sub(r'_x\d+$', '', variant)


def extract_leverage(variant: str) -> int:
    m = re.search(r'_x(\d+)$', variant)
    return int(m.group(1)) if m else 1


def funding_sign_for(variant: str) -> float:
    base = extract_base_variant(variant)
    if base == "long_only":
        return +1.0
    if base == "short_only":
        return -1.0
    return 0.0


def load_trades(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({
                "opened_at": float(r["opened_at"]),
                "closed_at": float(r["closed_at"]),
                "pnl": float(r["pnl"]),
                "qty": float(r["qty"]),
                "entry_price": float(r["entry_price"]),
            })
    return rows


def load_8h_funding(parquet_path: Path) -> tuple[np.ndarray, np.ndarray, float]:
    """Returns (timestamps_ms, rates, fallback_rate). Sorted by timestamp.
    Fallback is the mean of non-zero (observed) rates — used for zero-filled rows.
    """
    df = pd.read_parquet(parquet_path).sort_values("timestamp")
    ts = df["timestamp"].to_numpy(dtype=np.int64)
    rates = df["funding_rate"].to_numpy(dtype=np.float64)
    fallback = float(rates[rates != 0].mean()) if (rates != 0).any() else 0.0
    return ts, rates, fallback


def funding_rate_sum_in_window(
    open_ms: float, close_ms: float,
    ts_arr: np.ndarray, rate_arr: np.ndarray, fallback: float
) -> float:
    """Sum of funding rates for all events in [open_ms, close_ms).
    Zero-rate rows (pre-launch zero-fill) are substituted with fallback.
    Returns the raw rate sum — caller multiplies by notional × sign.
    """
    open_i = int(open_ms)
    close_i = int(close_ms)
    lo = int(np.searchsorted(ts_arr, open_i, side="left"))
    hi = int(np.searchsorted(ts_arr, close_i, side="left"))
    if lo >= hi:
        return 0.0
    slice_rates = rate_arr[lo:hi]
    return float(np.where(slice_rates == 0, fallback, slice_rates).sum())


# ── Per-period simulation ──────────────────────────────────────────────────────

def simulate_period(
    period_trades: list[dict],
    f_sign: float,
    ts_arr: np.ndarray,
    rate_arr: np.ndarray,
    fallback: float,
) -> dict:
    """Compounded equity simulation for a pre-filtered set of trades."""
    period_trades = sorted(period_trades, key=lambda t: t["opened_at"])
    n_trades = len(period_trades)

    raw_eq = STARTING_BALANCE
    adj_eq = STARTING_BALANCE
    total_fee_usd = 0.0
    total_fund_usd = 0.0
    total_fund_events = 0

    for t in period_trades:
        notional = t["qty"] * t["entry_price"]
        open_ms = t["opened_at"]
        close_ms = t["closed_at"]

        fee_usd = notional * FEE_DELTA_PER_SIDE * 2

        sum_rate = funding_rate_sum_in_window(open_ms, close_ms, ts_arr, rate_arr, fallback)
        fund_usd = notional * sum_rate * f_sign

        # Count funding events that fell in this trade's window
        lo = int(np.searchsorted(ts_arr, int(open_ms), side="left"))
        hi = int(np.searchsorted(ts_arr, int(close_ms), side="left"))
        total_fund_events += max(0, hi - lo)

        total_fee_usd += fee_usd
        total_fund_usd += fund_usd

        raw_pnl = t["pnl"]
        net = raw_pnl - fee_usd - fund_usd

        scale = (adj_eq / raw_eq) if raw_eq > 0 else 0.0
        adj_pnl = net * scale

        raw_eq += raw_pnl
        adj_eq += adj_pnl

    avg_fund_events = total_fund_events / n_trades if n_trades else 0.0

    return {
        "n_trades": n_trades,
        "final_raw_equity": raw_eq,
        "final_adj_equity": adj_eq,
        "total_fee_usd": total_fee_usd,
        "total_fund_usd": total_fund_usd,
        "avg_fund_events_per_trade": avg_fund_events,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not FUNDING_8H_PARQUET.exists():
        print(f"ERROR: {FUNDING_8H_PARQUET} not found", file=sys.stderr)
        sys.exit(1)

    ts_arr, rate_arr, fallback_rate = load_8h_funding(FUNDING_8H_PARQUET)
    print(
        f"[funding] Loaded {len(ts_arr)} 8h events, "
        f"range: {pd.Timestamp(ts_arr[0], unit='ms', tz='UTC').date()} → "
        f"{pd.Timestamp(ts_arr[-1], unit='ms', tz='UTC').date()}, "
        f"fallback={fallback_rate:.8f}",
        file=sys.stderr,
    )

    results = []
    combo_count = 0
    skipped = []

    for strat_dir in sorted(RESULTS_7S.iterdir()):
        strat = strat_dir.name
        if not strat_dir.is_dir() or strat in ("archive", "buy_and_hold", "rolling_window"):
            continue
        for tf_dir in sorted(strat_dir.iterdir()):
            if not tf_dir.is_dir() or tf_dir.name == "walk_forward":
                continue
            tf = tf_dir.name
            for variant_dir in sorted(tf_dir.iterdir()):
                if not variant_dir.is_dir():
                    continue
                variant = variant_dir.name
                trades_path = variant_dir / "trades.csv"
                stats_path = variant_dir / "stats.json"

                if not trades_path.exists() or not stats_path.exists():
                    skipped.append(f"{strat}/{tf}/{variant}")
                    continue

                trades = load_trades(trades_path)
                if not trades:
                    skipped.append(f"{strat}/{tf}/{variant} (no trades)")
                    continue

                combo_count += 1
                lev_mult = extract_leverage(variant)
                f_sign = funding_sign_for(variant)

                periods_out = {}

                for period_key, (p_start_ms, p_end_ms) in POST21_PERIODS.items():
                    period_years = PERIOD_YEARS[period_key]
                    period_trades = [t for t in trades
                                     if p_start_ms <= t["opened_at"] <= p_end_ms]

                    n_trades = len(period_trades)
                    if n_trades == 0:
                        periods_out[period_key] = {
                            "trades": 0,
                            "original_return_pct": 0.0,
                            "original_cagr": 0.0,
                            "adj_return_pct": 0.0,
                            "adj_cagr": 0.0,
                            "fee_cost_annual_pct": 0.0,
                            "funding_cost_annual_pct": 0.0,
                            "avg_funding_rate": 0.0,
                            "total_fee_usd": 0.0,
                            "total_funding_usd": 0.0,
                            "final_raw_equity": STARTING_BALANCE,
                            "final_adj_equity": STARTING_BALANCE,
                            "avg_fund_events_per_trade": 0.0,
                            "funding_coverage": "no_trades",
                            "leverage_mult": lev_mult,
                        }
                        continue

                    sim = simulate_period(period_trades, f_sign, ts_arr, rate_arr, fallback_rate)

                    raw_return_pct = (sim["final_raw_equity"] - STARTING_BALANCE) / STARTING_BALANCE * 100.0
                    raw_base = sim["final_raw_equity"] / STARTING_BALANCE
                    if raw_base <= 0:
                        original_cagr = -100.0
                    else:
                        try:
                            original_cagr = (raw_base ** (1.0 / period_years) - 1) * 100
                        except (ValueError, ZeroDivisionError):
                            original_cagr = raw_return_pct

                    adj_return_pct = (sim["final_adj_equity"] - STARTING_BALANCE) / STARTING_BALANCE * 100.0
                    adj_base = sim["final_adj_equity"] / STARTING_BALANCE
                    if adj_base <= 0:
                        adj_cagr = -100.0
                    else:
                        try:
                            adj_cagr = (adj_base ** (1.0 / period_years) - 1) * 100
                        except (ValueError, ZeroDivisionError):
                            adj_cagr = adj_return_pct

                    # Annualized cost breakdown
                    fee_cost_pct_annual = sim["total_fee_usd"] / STARTING_BALANCE * 100.0 / period_years
                    fund_cost_pct_annual = sim["total_fund_usd"] / STARTING_BALANCE * 100.0 / period_years

                    # Trade-weighted average 8h funding rate (for reference)
                    avg_fund_events = sim["avg_fund_events_per_trade"]
                    avg_funding_rate = (
                        (sim["total_fund_usd"] / f_sign) / (
                            sum(t["qty"] * t["entry_price"] for t in period_trades) * avg_fund_events * n_trades
                        ) if avg_fund_events > 0 and f_sign != 0 and n_trades > 0 else 0.0
                    )

                    periods_out[period_key] = {
                        "trades": n_trades,
                        "original_return_pct": round(raw_return_pct, 4),
                        "original_cagr": round(original_cagr, 4),
                        "adj_return_pct": round(adj_return_pct, 4),
                        "adj_cagr": round(adj_cagr, 2),
                        "fee_cost_annual_pct": round(fee_cost_pct_annual, 4),
                        "funding_cost_annual_pct": round(fund_cost_pct_annual, 4),
                        "avg_funding_rate": round(avg_funding_rate, 8),
                        "total_fee_usd": round(sim["total_fee_usd"], 4),
                        "total_funding_usd": round(sim["total_fund_usd"], 4),
                        "final_raw_equity": round(sim["final_raw_equity"], 4),
                        "final_adj_equity": round(sim["final_adj_equity"], 4),
                        "avg_fund_events_per_trade": round(avg_fund_events, 2),
                        "funding_coverage": "per_event_8h",
                        "leverage_mult": lev_mult,
                    }

                results.append({
                    "strat": strat,
                    "tf": tf,
                    "variant": variant,
                    "periods": periods_out,
                })

                if combo_count % 10 == 0:
                    print(f"  {combo_count} combos processed...", file=sys.stderr)

    with open(OUT_JSON, "w") as f:
        json.dump(results, f, indent=2)

    if skipped:
        print(f"[info] Skipped {len(skipped)}: {skipped[:5]}...", file=sys.stderr)

    print(
        f"SUMMARY: {combo_count} combos × {len(POST21_PERIODS)} periods → {OUT_JSON}",
        file=sys.stdout,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
