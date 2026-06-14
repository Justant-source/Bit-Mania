"""Bybit-native 4h backtest of the LIVE Supertrend strategy — closes the grid gap.

#7908 was backtested on Binance 1h→4h (Jesse's expand→re-aggregate produced a
02계열 fill grid). The live system trades Bybit *native* 4h (00계열). This re-runs
the strategy on Bybit 4h with the LIVE (fixed) indicators and the #7908 entry/exit
rules, producing the trade series + headline stats on the same exchange and grid
the live system actually uses.

Indicators: full-history via the live functions (compute_ema / _atr_jesse) + the
live supertrend algorithm (verified == Jesse in test_supertrend_parity). Fills at
bar close; 95% × 3x sizing; taker fee 0.055% per side. Equity is marked-to-market
each 4h bar (open position included) so Sharpe/Sortino/Calmar approximate Jesse's
daily-MTM metrics rather than realized-only.

Modes:
  (no args)         → print headline stats + #7908 comparison (quick check)
  --out <dir>       → also write dashboard-format stats.json / trades.csv /
                      monthly_returns.csv into <dir> (for build_strategy_dashboard.py)

Run: docker run --rm -v "$PWD/cryptoengine:/work" -w /work \
       --entrypoint python cryptoengine-supertrend:latest \
       tests/fixtures/_replay_supertrend.py [--out /path/to/results_dir]
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sys
from collections import Counter, OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, "/work")
from services.strategies.supertrend.indicators import (  # noqa: E402
    _atr_jesse,
    compute_ema,
    compute_supertrend,
)

ST_PERIOD, ST_FACTOR = 9, 2.6
FAST, SLOW, DIR = 7, 29, 240
ATR_MULT = 3.3
LEVERAGE = 3
ALLOC = 0.95
FEE = 0.00055
START_BAL = 10_000.0
_4H_MS = 4 * 3600 * 1000
FX = Path(__file__).resolve().parent

# #7908 (Bybit 네이티브 4h, 00계열 정본, 2026-06-14 재빌드) — stats.json 기준
REF = {"trades": 360, "winrate": 48.61, "cagr": 137.64, "mdd": -73.29,
       "pf": 1.184, "sharpe": 1.349}


def _u(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def _st_seq(high, low, close, period, factor):
    atr = _atr_jesse(high, low, close, period)
    n = len(close)
    mid = (high + low) / 2.0
    ub = mid + factor * atr
    lb = mid - factor * atr
    st = np.zeros(n)
    seed = period - 1
    st[seed] = ub[seed] if close[seed] <= ub[seed] else lb[seed]
    for i in range(period, n):
        p = i - 1
        pc = close[p]
        if pc <= ub[p]:
            ub[i] = min(ub[i], ub[p])
        if pc >= lb[p]:
            lb[i] = max(lb[i], lb[p])
        if st[p] == ub[p]:
            st[i] = lb[i] if close[i] > ub[i] else ub[i]
        else:
            st[i] = ub[i] if close[i] < lb[i] else lb[i]
    return np.where(close > st, 1, -1)


def run_backtest():
    """Returns (trades, equity_4h, df, ts) where trades carry full fill detail."""
    rows = list(csv.DictReader(open(FX / "btc_4h.csv")))
    ts = np.array([int(r["timestamp"]) for r in rows])
    o = np.array([float(r["open"]) for r in rows])
    h = np.array([float(r["high"]) for r in rows])
    lo = np.array([float(r["low"]) for r in rows])
    c = np.array([float(r["close"]) for r in rows])
    df = pd.DataFrame({"open": o, "high": h, "low": lo, "close": c})
    n = len(df)

    ema7 = compute_ema(df, FAST).to_numpy()
    ema29 = compute_ema(df, SLOW).to_numpy()
    ema240 = compute_ema(df, DIR).to_numpy()
    atr14 = _atr_jesse(h, lo, c, 14)
    d = _st_seq(h, lo, c, ST_PERIOD, ST_FACTOR)
    _d, _ = compute_supertrend(df, ST_PERIOD, ST_FACTOR)
    assert _d == d[-1], f"st_seq mismatch: {_d} {d[-1]}"

    balance = START_BAL
    pos = False
    entry = 0.0
    entry_ts = 0
    size = 0.0
    last_liq = -1
    atr_exit = -(10 ** 18)
    trades = []           # (open_ts, entry, close_ts, exit, qty, pnl, fee, reason)
    equity_4h = []        # (ts, mark-to-market equity)
    WARMUP = DIR
    for i in range(WARMUP, n):
        price = c[i]
        t = int(ts[i])
        # mark-to-market equity (include open position's unrealized)
        unreal = size * (price - entry) if pos else 0.0
        equity_4h.append((t, balance + unreal))
        if not pos:
            if t <= last_liq or t <= atr_exit + _4H_MS:
                continue
            if d[i] == 1 and ema7[i] > ema29[i] and price > ema240[i]:
                pos, entry, entry_ts = True, price, t
                size = (balance * ALLOC * LEVERAGE) / entry
        else:
            a = atr14[i] * ATR_MULT
            if np.isnan(a):
                continue
            reason = None
            if ema7[i] < ema29[i]:
                reason = "ema"
            elif abs(price - entry) >= a:
                reason = "atr"
            if reason:
                gross = size * (price - entry)
                fee = (size * entry + size * price) * FEE
                net = gross - fee
                balance += net
                trades.append((entry_ts, entry, t, price, size, net, fee, reason))
                last_liq = t
                if reason == "atr":
                    atr_exit = t
                pos = False
                size = 0.0
    return trades, equity_4h, df, ts


def _metrics(trades, equity_4h):
    final = equity_4h[-1][1] if equity_4h else START_BAL
    gp = sum(t[5] for t in trades if t[5] >= 0)
    gl = -sum(t[5] for t in trades if t[5] < 0)
    wins = sum(1 for t in trades if t[3] > t[1])
    # daily MTM series → returns
    daily = OrderedDict()
    for t, eq in equity_4h:
        day = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d")
        daily[day] = eq  # last bar of the day wins
    dvals = list(daily.values())
    rets = [(dvals[i] / dvals[i - 1] - 1.0)
            for i in range(1, len(dvals)) if dvals[i - 1] > 0]
    mdd = 0.0
    peak = dvals[0] if dvals else START_BAL
    for v in dvals:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v - peak) / peak)
    mean = sum(rets) / len(rets) if rets else 0.0
    std = math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets)) if rets else 0.0
    dn = [r for r in rets if r < 0]
    dstd = math.sqrt(sum(r * r for r in dn) / len(rets)) if rets and dn else 0.0
    sharpe = (mean / std * math.sqrt(365)) if std > 0 else 0.0
    sortino = (mean / dstd * math.sqrt(365)) if dstd > 0 else 0.0
    days = (equity_4h[-1][0] - equity_4h[0][0]) / 86_400_000 if equity_4h else 1
    years = max(days / 365.0, 1e-9)
    cagr = ((final / START_BAL) ** (1 / years) - 1) * 100 if final > 0 else -100.0
    calmar = (cagr / abs(mdd * 100)) if mdd < 0 else 0.0
    return {
        "final": final, "cagr": cagr, "mdd": mdd * 100, "pf": (gp / gl) if gl > 0 else float("inf"),
        "win_rate": 100 * wins / max(1, len(trades)), "sharpe": sharpe, "sortino": sortino,
        "calmar": calmar, "gp": gp, "gl": gl, "net_pct": (final / START_BAL - 1) * 100,
    }


def write_dashboard_results(out_dir: Path, trades, m, ts):
    out_dir.mkdir(parents=True, exist_ok=True)
    # trades.csv (dashboard schema)
    with open(out_dir / "trades.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["opened_at", "closed_at", "side", "entry_price",
                    "exit_price", "qty", "pnl", "fee"])
        for (ot, ep, ct, xp, qty, pnl, fee, r) in trades:
            w.writerow([float(ot), float(ct), "long", ep, xp, qty, pnl, fee])
    # monthly_returns.csv (month, pnl_usdt)
    monthly = OrderedDict()
    for (ot, ep, ct, xp, qty, pnl, fee, r) in trades:
        mo = datetime.fromtimestamp(ct / 1000, timezone.utc).strftime("%Y-%m")
        monthly[mo] = monthly.get(mo, 0.0) + pnl
    with open(out_dir / "monthly_returns.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["month", "pnl_usdt"])
        for mo in sorted(monthly):
            w.writerow([mo, round(monthly[mo], 6)])
    # stats.json (dashboard-consumed fields)
    pf = m["pf"] if math.isfinite(m["pf"]) else 0.0
    stats = {
        "cagr_pct": round(m["cagr"], 4),
        "annual_return_pct": round(m["cagr"], 4),
        "sharpe_ratio": round(m["sharpe"], 4),
        "max_drawdown_pct": round(m["mdd"], 4),
        "total_trades": len(trades),
        "win_rate_pct": round(m["win_rate"], 4),
        "profit_factor": round(pf, 4),
        "net_profit_pct": round(m["net_pct"], 4),
        "starting_balance": START_BAL,
        "leverage": LEVERAGE,
        "variant": "long_only",
        "timeframe": "4h",
        "strategy": "SupertrendStrategy",
        "data_source": "bybit_native_4h_00grid_live_logic",
        "start": _u(ts[0]),
        "end": _u(ts[-1]),
        "raw_metrics": {
            "finishing_balance": round(m["final"], 6),
            "sortino_ratio": round(m["sortino"], 4),
            "calmar_ratio": round(m["calmar"], 4),
            "gross_profit": round(m["gp"], 4),
            "gross_loss": round(-m["gl"], 4),
        },
    }
    (out_dir / "stats.json").write_text(json.dumps(stats, indent=2))
    print(f"  wrote {out_dir}/stats.json, trades.csv, monthly_returns.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="results dir for dashboard-format output")
    args = ap.parse_args()

    trades, equity_4h, df, ts = run_backtest()
    m = _metrics(trades, equity_4h)

    print("=== Bybit native 4h (00계열) backtest — LIVE strategy (fixed indicators) ===")
    print("bars=%d  (%s ~ %s)" % (len(df), _u(ts[0]), _u(ts[-1])))
    print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  Sharpe=%.3f  final=$%.0f"
          % (len(trades), m["win_rate"], m["cagr"], m["mdd"], m["pf"], m["sharpe"], m["final"]))
    print("exit reasons:", dict(Counter(t[7] for t in trades)))

    print("\n=== #7908 reference (Binance 1h→4h, 02계열) ===")
    print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  Sharpe=%.3f"
          % (REF["trades"], REF["winrate"], REF["cagr"], REF["mdd"], REF["pf"], REF["sharpe"]))

    # entry-timing overlap vs #7908
    ref = list(csv.DictReader(open(FX / "trades_7908.csv")))
    ref_open = sorted(int(float(r["opened_at"])) for r in ref)
    sim_open = sorted(t[0] for t in trades)
    tol = _4H_MS + 2 * 3600 * 1000
    matched = 0
    for so in sim_open:
        j = bisect.bisect_left(ref_open, so)
        cand = [abs(so - ref_open[k]) for k in (j - 1, j) if 0 <= k < len(ref_open)]
        if cand and min(cand) <= tol:
            matched += 1
    print("entry-timing overlap vs #7908: %d/%d (%.1f%%)"
          % (matched, len(sim_open), 100 * matched / max(1, len(sim_open))))

    if args.out:
        print("\nWriting dashboard-format results…")
        write_dashboard_results(Path(args.out), trades, m, ts)


if __name__ == "__main__":
    main()
