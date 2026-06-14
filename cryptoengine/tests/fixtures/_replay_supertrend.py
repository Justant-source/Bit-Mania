"""Bybit-native 4h backtest of the LIVE Supertrend strategy — closes the grid gap.

#7908 was backtested on Binance 1h→4h (Jesse's expand→re-aggregate produced a
02계열 fill grid). The live system trades Bybit *native* 4h (00계열). This re-runs
the strategy on Bybit 4h with the LIVE (fixed) indicators and the #7908 entry/exit
rules, producing both the trade series and headline stats (CAGR / MDD / PF /
win-rate) for an apples-to-apples comparison with the original #7908 result — now
on the same exchange and grid the live system actually uses.

Indicators: full-history via the live functions (compute_ema / _atr_jesse) + the
live supertrend algorithm (verified == Jesse in test_supertrend_parity). Fills at
bar close; 95% × 3x sizing; taker fee 0.055% per side (matches the sweep FEE).
Stats are realized-trade compounded (Jesse marks intrabar) → headline approximation.

Run: docker run --rm -v "$PWD/cryptoengine:/work" -w /work \
       --entrypoint python cryptoengine-supertrend:latest tests/fixtures/_replay_supertrend.py
"""

from __future__ import annotations

import bisect
import csv
import sys
from collections import Counter
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
FEE = 0.00055            # taker per side (matches backtest sweep pg_worker FEE)
START_BAL = 10_000.0
_4H_MS = 4 * 3600 * 1000
FX = Path(__file__).resolve().parent

# #7908 (Binance 1h→4h, 02계열) headline stats — results/.../long_only_x3_7908/stats.json
REF = {"trades": 346, "winrate": 48.27, "cagr": 152.61, "mdd": -74.32,
       "pf": 1.188, "sharpe": 1.401}


def _u(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def _st_seq(high, low, close, period, factor):
    """Full-array supertrend direction — same algorithm as live compute_supertrend."""
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


def _backtest_stats(trades):
    """Compound equity over realized trades → CAGR / MDD / PF."""
    bal = START_BAL
    peak = bal
    mdd = 0.0
    gp = gl = 0.0
    for (et, ep, xt, xp, r) in trades:
        notional = bal * ALLOC * LEVERAGE
        size = notional / ep
        gross = size * (xp - ep)
        fee = (size * ep + size * xp) * FEE
        net = gross - fee
        bal += net
        if net >= 0:
            gp += net
        else:
            gl += -net
        peak = max(peak, bal)
        if peak > 0:
            mdd = min(mdd, (bal - peak) / peak)
    days = (trades[-1][2] - trades[0][0]) / 86_400_000 if trades else 1
    years = max(days / 365.0, 1e-9)
    cagr = ((bal / START_BAL) ** (1 / years) - 1) * 100 if bal > 0 else -100.0
    pf = (gp / gl) if gl > 0 else float("inf")
    return {"final": bal, "cagr": cagr, "mdd": mdd * 100, "pf": pf}


def main() -> None:
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

    pos = False
    entry = 0.0
    entry_ts = 0
    last_liq = -1
    atr_exit = -(10 ** 18)
    trades = []
    WARMUP = DIR  # dir_ema(240) convergence — Jesse evaluates once enough candles exist
    for i in range(WARMUP, n):
        price = c[i]
        t = int(ts[i])
        if not pos:
            if t <= last_liq:
                continue
            if t <= atr_exit + _4H_MS:
                continue
            if d[i] == 1 and ema7[i] > ema29[i] and price > ema240[i]:
                pos, entry, entry_ts = True, price, t
        else:
            a = atr14[i] * ATR_MULT
            if np.isnan(a):
                continue
            if ema7[i] < ema29[i]:
                trades.append((entry_ts, entry, t, price, "ema"))
                last_liq, pos = t, False
            elif abs(price - entry) >= a:
                trades.append((entry_ts, entry, t, price, "atr"))
                last_liq, atr_exit, pos = t, t, False

    wins = sum(1 for (_, ep, _, xp, _) in trades if xp > ep)
    wr = 100 * wins / max(1, len(trades))
    st = _backtest_stats(trades)

    print("=== Bybit native 4h (00계열) backtest — LIVE strategy (fixed indicators) ===")
    print("bars=%d  (%s ~ %s)" % (n, _u(ts[0]), _u(ts[-1])))
    print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  final=$%.0f"
          % (len(trades), wr, st["cagr"], st["mdd"], st["pf"], st["final"]))
    print("exit reasons:", dict(Counter(r for *_, r in trades)))

    print("\n=== #7908 reference (Binance 1h→4h, 02계열) ===")
    print("trades=%d  win=%.1f%%  CAGR=%.2f%%  MDD=%.2f%%  PF=%.3f  Sharpe=%.3f"
          % (REF["trades"], REF["winrate"], REF["cagr"], REF["mdd"], REF["pf"], REF["sharpe"]))

    print("\n=== Δ (Bybit live − Binance #7908) ===")
    print("trades %+d | win %+.1f%%p | CAGR %+.1f%%p | MDD %+.1f%%p | PF %+.3f"
          % (len(trades) - REF["trades"], wr - REF["winrate"], st["cagr"] - REF["cagr"],
             st["mdd"] - REF["mdd"], st["pf"] - REF["pf"]))

    # entry-timing overlap (ref 02계열 vs sim 00계열, ±1bar+grid tolerance)
    ref = list(csv.DictReader(open(FX / "trades_7908.csv")))
    ref_open = sorted(int(float(r["opened_at"])) for r in ref)
    sim_open = sorted(et for et, *_ in trades)
    tol = _4H_MS + 2 * 3600 * 1000
    matched = 0
    for so in sim_open:
        j = bisect.bisect_left(ref_open, so)
        cand = [abs(so - ref_open[k]) for k in (j - 1, j) if 0 <= k < len(ref_open)]
        if cand and min(cand) <= tol:
            matched += 1
    print("\nentry-timing overlap vs #7908: %d/%d (%.1f%%)"
          % (matched, len(sim_open), 100 * matched / max(1, len(sim_open))))
    print("NOTE: CAGR/MDD는 realized-trade 복리 근사(Jesse는 intrabar MTM). "
          "거래소·격자가 라이브와 동일(Bybit native 4h 00계열)해진 결과 — 격자 갭 해소.")


if __name__ == "__main__":
    main()
