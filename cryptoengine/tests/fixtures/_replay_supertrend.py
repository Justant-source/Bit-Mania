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
                      monthly_returns.csv into <dir> (parity fixture 생성용)

Run: docker run --rm -v "$PWD/cryptoengine:/work" -w /work \
       --entrypoint python cryptoengine-supertrend:latest \
       tests/fixtures/_replay_supertrend.py [--out /path/to/results_dir]

Hold-out (S7): --csv / --params / --start / --end (end exclusive). No extra
args still prints the canonical #7908 fixture (198 / 219.06 / −66.70 / 1.667).
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

_CE = Path(__file__).resolve().parents[2]
if str(_CE) not in sys.path:
    sys.path.insert(0, str(_CE))
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

# #7908 (Bybit 네이티브 4h, ATR 손절만·익절 없음, 2026-08-20)
REF = {"trades": 198, "winrate": 42.42, "cagr": 219.06, "mdd": -66.70,
       "pf": 1.507, "sharpe": 1.667}


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


def date_ms(s: str) -> int:
    """UTC midnight of YYYY-MM-DD as epoch ms."""
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse_params(s: str | None) -> dict:
    """JSON object or path. Aliases match st_combos column names."""
    out = {
        "st_factor": float(ST_FACTOR),
        "st_period": int(ST_PERIOD),
        "fast_ema": int(FAST),
        "slow_ema": int(SLOW),
        "dir_ema": int(DIR),
        "atr_mult": float(ATR_MULT),
    }
    if not s:
        return out
    p = Path(s)
    raw = json.loads(p.read_text() if p.is_file() else s)
    aliases = {
        "fast_ema_len": "fast_ema",
        "slow_ema_len": "slow_ema",
        "direction_ema_len": "dir_ema",
    }
    for k, v in raw.items():
        key = aliases.get(k, k)
        if key not in out:
            raise ValueError(f"unknown param {k}")
        out[key] = float(v) if key in ("st_factor", "atr_mult") else int(v)
    return out


def load_funding_csv(path: Path) -> dict[int, float]:
    """timestamp_ms,funding_rate CSV → {ts: rate} lookup for run_backtest(funding=...)."""
    out = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            out[int(row["timestamp_ms"])] = float(row["funding_rate"])
    return out


from functools import lru_cache


@lru_cache(maxsize=8)
def _load_ohlcv(csv_path_str: str):
    """Cached CSV → (ts,o,h,lo,c) numpy arrays. Combo-independent; a v12 grid run calls
    run_backtest thousands of times against the same csv_path, so re-parsing the CSV
    every call (the pre-v12 behaviour) would dominate runtime. Pure read of immutable
    on-disk data — safe to cache across the whole process."""
    rows = list(csv.DictReader(open(csv_path_str)))
    ts = np.array([int(r["timestamp"]) for r in rows])
    o = np.array([float(r["open"]) for r in rows])
    h = np.array([float(r["high"]) for r in rows])
    lo = np.array([float(r["low"]) for r in rows])
    c = np.array([float(r["close"]) for r in rows])
    return ts, o, h, lo, c


def run_backtest(
    csv_path: Path | None = None,
    st_factor: float = ST_FACTOR,
    st_period: int = ST_PERIOD,
    fast: int = FAST,
    slow: int = SLOW,
    dir_ema: int = DIR,
    atr_mult: float = ATR_MULT,
    start_ms: int | None = None,
    end_ms: int | None = None,
    warmup_bars: int | None = None,
    safety_stop: bool = False,
    fee: float = FEE,
    slip_bps: float = 0.0,
    fill: str = "close",
    funding: dict[int, float] | None = None,
    funding_log: list | None = None,
):
    """Returns (trades, equity_4h, df, ts). Indicators on full CSV; fills in [start,end).

    v12 additions (all default to the pre-v12 behaviour so a bare call is byte-identical
    to the original #7908 canonical run):
      warmup_bars  — fixed start index for ALL combos (None = old per-combo `int(dir_ema)`
                      behaviour, which biases combos with a smaller dir_ema toward an
                      earlier, more favourable start date).
      safety_stop  — model the live exchange stop-market at entry*(1 - 0.70/LEVERAGE),
                      checked against the bar's intrabar LOW (the one structural gap vs the
                      live system, which the pre-v12 replay did not model at all).
      fee/slip_bps/fill — execution-assumption sensitivity (G6). fill="next_open" defers
                      signal-driven entries/exits to the following bar's open (with slippage);
                      the safety stop always fires intrabar on the bar's low, independent of
                      `fill`, since it is a resting exchange order, not a signal decision.

    funding/funding_log (opt-in, default None = no funding modeled, byte-identical to the
                      pre-funding behaviour):
      funding      — {settlement_ts_ms: rate} for Bybit USDT-perp 00/08/16 UTC funding.
                      Missing timestamps (uncovered dates) are treated as rate 0, not an
                      error — that is the intended behaviour for periods the funding
                      history API does not cover. Charged only on a position that was
                      already open going into this bar (opened on a prior bar), using this
                      bar's close as a mark-price proxy (no separate index/mark series).
      funding_log  — if given a list, (ts, cost_usd) is appended for every non-zero funding
                      charge (cost > 0 = long paid; cost < 0 = long received). Reporting
                      only; does not affect the simulation.
    """
    csv_path = Path(csv_path) if csv_path else FX / "btc_4h.csv"
    ts, o, h, lo, c = _load_ohlcv(str(csv_path))
    df = pd.DataFrame({"open": o, "high": h, "low": lo, "close": c})
    n = len(df)

    ema_fast = compute_ema(df, fast).to_numpy()
    ema_slow = compute_ema(df, slow).to_numpy()
    ema_dir = compute_ema(df, dir_ema).to_numpy()
    atr14 = _atr_jesse(h, lo, c, 14)
    d = _st_seq(h, lo, c, st_period, st_factor)
    _d, _ = compute_supertrend(df, st_period, st_factor)
    assert _d == d[-1], f"st_seq mismatch: {_d} {d[-1]}"

    balance = START_BAL
    pos = False
    entry = 0.0
    entry_ts = 0
    size = 0.0
    stop_price = None
    last_liq = -1
    atr_exit = -(10 ** 18)
    trades = []           # (open_ts, entry, close_ts, exit, qty, pnl, fee, reason)
    equity_4h = []        # (ts, mark-to-market equity)
    slip = slip_bps / 10_000.0
    warmup = warmup_bars if warmup_bars is not None else int(dir_ema)
    pending = None        # queued signal fill for fill="next_open": {'type','reason'?}
    funding = funding or {}

    def _fee(px_entry, px_exit, qty):
        return (qty * px_entry + qty * px_exit) * fee

    for i in range(warmup, n):
        t = int(ts[i])

        # 1) execute a queued signal fill at THIS bar's open (fill="next_open" only).
        #    Must run BEFORE the end_ms/start_ms gates below: a fill queued on the last
        #    in-window bar otherwise gets silently dropped by `break`, leaving a phantom
        #    open position and an unrecorded trade (bug found by adversarial audit,
        #    2026-08-31 — 3 trades / 11.5% final equity on the canonical run under
        #    fill="next_open"; fill="close", the only mode used by any v12 result to
        #    date, does not use `pending` and was unaffected).
        if pending is not None:
            if pending["type"] == "enter":
                fp = o[i] * (1 + slip)
                pos, entry, entry_ts = True, fp, t
                size = (balance * ALLOC * LEVERAGE) / entry
                stop_price = entry * (1 - 0.70 / LEVERAGE) if safety_stop else None
            else:
                fp = o[i] * (1 - slip)
                gross = size * (fp - entry)
                feeamt = _fee(entry, fp, size)
                net = gross - feeamt
                balance += net
                trades.append((entry_ts, entry, t, fp, size, net, feeamt, pending["reason"]))
                last_liq = t
                if pending["reason"] == "atr":
                    atr_exit = t
                pos, size, stop_price = False, 0.0, None
            pending = None

        if end_ms is not None and t >= end_ms:
            break
        if start_ms is not None and t < start_ms:
            continue

        price = c[i]
        # mark-to-market equity (include open position's unrealized)
        unreal = size * (price - entry) if pos else 0.0
        equity_4h.append((t, balance + unreal))

        # v13 addition: 8h funding settlement (Bybit USDT-perp, 00/08/16 UTC), charged on
        # a position that was already open going into this bar (pos here reflects the state
        # carried from the previous bar — this bar's own entry, if any, happens further down
        # and is not charged funding on the same bar it opens). Mark price approximated by
        # this bar's close (no separate index/mark price series available).
        if pos and (t // 3_600_000) % 24 in (0, 8, 16):
            rate = funding.get(t, 0.0)
            if rate:
                cost = size * price * rate
                balance -= cost
                if funding_log is not None:
                    funding_log.append((t, cost))

        # 2) intrabar exchange safety stop — always checked on the bar's LOW, regardless
        #    of `fill` mode (it is a resting stop order, not a signal decision).
        if pos and safety_stop and stop_price is not None and lo[i] <= stop_price:
            fp = stop_price * (1 - slip)
            gross = size * (fp - entry)
            feeamt = _fee(entry, fp, size)
            net = gross - feeamt
            balance += net
            trades.append((entry_ts, entry, t, fp, size, net, feeamt, "safety_stop"))
            last_liq = t
            pos, size, stop_price = False, 0.0, None
            continue

        # 3) signal decisions (evaluated at this bar's close)
        if not pos:
            if t <= last_liq or t <= atr_exit + _4H_MS:
                continue
            if d[i] == 1 and ema_fast[i] > ema_slow[i] and price > ema_dir[i]:
                if fill == "close":
                    fp = price * (1 + slip)
                    pos, entry, entry_ts = True, fp, t
                    size = (balance * ALLOC * LEVERAGE) / entry
                    stop_price = entry * (1 - 0.70 / LEVERAGE) if safety_stop else None
                else:
                    pending = {"type": "enter"}
        else:
            a = atr14[i] * atr_mult
            if np.isnan(a):
                continue
            reason = None
            if ema_fast[i] < ema_slow[i]:
                reason = "ema"
            elif price <= entry - a:
                reason = "atr"
            if reason:
                if fill == "close":
                    fp = price * (1 - slip)
                    gross = size * (fp - entry)
                    feeamt = _fee(entry, fp, size)
                    net = gross - feeamt
                    balance += net
                    trades.append((entry_ts, entry, t, fp, size, net, feeamt, reason))
                    last_liq = t
                    if reason == "atr":
                        atr_exit = t
                    pos, size, stop_price = False, 0.0, None
                else:
                    pending = {"type": "exit", "reason": reason}
    return trades, equity_4h, df, ts


def daily_mtm(equity_4h) -> "OrderedDict[str, float]":
    """Last-bar-of-day mark-to-market equity, keyed by UTC date string. Shared by
    `_metrics` and any external caller (e.g. the v12 grid runner) that needs the
    per-day equity series directly instead of just the summary stats dict."""
    daily: "OrderedDict[str, float]" = OrderedDict()
    for t, eq in equity_4h:
        day = datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d")
        daily[day] = eq
    return daily


def _metrics(trades, equity_4h):
    final = equity_4h[-1][1] if equity_4h else START_BAL
    gp = sum(t[5] for t in trades if t[5] >= 0)
    gl = -sum(t[5] for t in trades if t[5] < 0)
    wins = sum(1 for t in trades if t[3] > t[1])
    # daily MTM series → returns
    daily = daily_mtm(equity_4h)
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
    ap.add_argument("--csv", default=None, help="OHLCV CSV (default: fixtures/btc_4h.csv)")
    ap.add_argument("--params", default=None, help="JSON object or path (st_factor/st_period/fast_ema/…)")
    ap.add_argument("--start", default=None, help="trade window start YYYY-MM-DD inclusive UTC")
    ap.add_argument("--end", default=None, help="trade window end YYYY-MM-DD exclusive UTC")
    ap.add_argument("--label", default=None, help="printed run label")
    ap.add_argument("--json", action="store_true", help="print one JSON object of metrics")
    ap.add_argument("--warmup-bars", type=int, default=None,
                     help="v12: fixed start index for all combos (default: legacy int(dir_ema))")
    ap.add_argument("--safety-stop", action="store_true",
                     help="v12: model intrabar exchange stop at entry*(1-0.70/lev) on the bar low")
    ap.add_argument("--fee", type=float, default=None, help="v12: taker fee per side (default %.5f)" % FEE)
    ap.add_argument("--slip-bps", type=float, default=0.0, help="v12: slippage in bps applied on fills")
    ap.add_argument("--fill", choices=["close", "next_open"], default="close",
                     help="v12: signal fills at this bar's close (default) or next bar's open")
    ap.add_argument("--funding-csv", default=None,
                     help="funding: CSV with timestamp_ms,funding_rate columns (Bybit "
                          "USDT-perp 00/08/16 UTC settlements). Omit for no funding modeled.")
    args = ap.parse_args()

    p = parse_params(args.params)
    start_ms = date_ms(args.start) if args.start else None
    end_ms = date_ms(args.end) if args.end else None
    csv_path = Path(args.csv) if args.csv else FX / "btc_4h.csv"
    funding = load_funding_csv(Path(args.funding_csv)) if args.funding_csv else None
    default_run = (
        args.csv is None and args.params is None
        and args.start is None and args.end is None
        and args.warmup_bars is None and not args.safety_stop
        and args.fee is None and args.slip_bps == 0.0 and args.fill == "close"
        and args.funding_csv is None
    )

    trades, equity_4h, df, ts = run_backtest(
        csv_path=csv_path,
        st_factor=p["st_factor"],
        st_period=p["st_period"],
        fast=p["fast_ema"],
        slow=p["slow_ema"],
        dir_ema=p["dir_ema"],
        atr_mult=p["atr_mult"],
        start_ms=start_ms,
        end_ms=end_ms,
        warmup_bars=args.warmup_bars,
        safety_stop=args.safety_stop,
        fee=args.fee if args.fee is not None else FEE,
        slip_bps=args.slip_bps,
        fill=args.fill,
        funding=funding,
    )
    m = _metrics(trades, equity_4h)

    if args.json:
        payload = {
            "label": args.label,
            "bars": int(len(df)),
            "csv": str(csv_path),
            "params": p,
            "start": args.start,
            "end": args.end,
            "first": _u(ts[0]),
            "last": _u(ts[-1]),
            "trades": len(trades),
            **{k: (None if isinstance(v, float) and not math.isfinite(v) else v) for k, v in m.items()},
        }
        print(json.dumps(payload, default=float))
        if args.out:
            write_dashboard_results(Path(args.out), trades, m, ts)
        return

    title = args.label or "LIVE strategy (fixed indicators)"
    print("=== Bybit native 4h (00계열) backtest — %s ===" % title)
    print("bars=%d  (%s ~ %s)" % (len(df), _u(ts[0]), _u(ts[-1])))
    print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  Sharpe=%.3f  final=$%.0f"
          % (len(trades), m["win_rate"], m["cagr"], m["mdd"], m["pf"], m["sharpe"], m["final"]))
    print("exit reasons:", dict(Counter(t[7] for t in trades)))

    if default_run:
        print("\n=== prior ATR-TP series (Bybit native 4h, 2026-06-14) ===")
        print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  Sharpe=%.3f"
              % (360, 48.61, 137.64, -73.29, 1.184, 1.349))

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
