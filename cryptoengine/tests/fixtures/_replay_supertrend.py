"""Replay the LIVE (fixed) Supertrend logic on Bybit 4h — the exchange/grid the
live system actually consumes — and compare qualitatively to #7908 (Binance).

The Binance 1h source that produced #7908 is absent from the host, so an exact
trade-for-trade reproduction isn't possible. Instead this validates that the
live logic (fixed indicators + entry/exit rules) generates a coherent trade
series on real Bybit data, and that its shape (count / win-rate / exit mix /
entry timing) tracks the #7908 backtest despite the exchange difference.

Indicators are computed full-history with the LIVE functions (compute_ema /
_atr_jesse) and the live supertrend algorithm (verified == jesse in
test_supertrend_parity). Entry/exit fill at bar close (Jesse no-intrabar model).

Run (live runtime image has the indicators module):
    docker run --rm -v "$PWD/cryptoengine:/work" -w /work \
        --entrypoint python cryptoengine-supertrend:latest tests/fixtures/_replay_supertrend.py
"""

from __future__ import annotations

import bisect
import csv
import datetime
import sys
from collections import Counter
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
_4H_MS = 4 * 3600 * 1000
FX = Path(__file__).resolve().parent


def _u(ms: float) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")


def _st_seq(high, low, close, period, factor):
    """Full-array supertrend direction — same algorithm as live compute_supertrend
    (which returns only the last value). Sanity-checked against it below."""
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

    # sanity: live compute_supertrend on full history == seq last
    _d, _ = compute_supertrend(df, ST_PERIOD, ST_FACTOR)
    assert _d == d[-1], f"st_seq mismatch vs compute_supertrend: {_d} {d[-1]}"

    # ── replay backtest rules with live indicators ──────────────────────────
    pos = False
    entry = 0.0
    entry_ts = 0
    last_liq = -1
    atr_exit = -(10 ** 18)
    trades = []  # (entry_ts, entry_px, exit_ts, exit_px, reason)
    # Warmup: dir_ema(240) must converge before signals are meaningful — Jesse
    # only evaluates should_long once enough candles exist. Skip the first DIR bars.
    WARMUP = DIR
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

    # ── stats ───────────────────────────────────────────────────────────────
    wins = sum(1 for (_, ep, _, xp, _) in trades if xp > ep)
    hold_bars = [(xt - et) / _4H_MS for (et, _, xt, _, _) in trades]
    print("=== LIVE-logic replay on Bybit 4h (00계열, fixed indicators) ===")
    print("bars=%d  (%s ~ %s)" % (n, _u(ts[0]), _u(ts[-1])))
    print("sim trades=%d  win=%d (%.1f%%)  avg_hold=%.1f bars"
          % (len(trades), wins, 100 * wins / max(1, len(trades)),
             sum(hold_bars) / max(1, len(hold_bars))))
    print("exit reasons:", dict(Counter(r for *_, r in trades)))
    print("first 3:", [(_u(et), round(ep, 1), _u(xt), round(xp, 1), r)
                       for et, ep, xt, xp, r in trades[:3]])
    print("last 3 :", [(_u(et), round(ep, 1), _u(xt), round(xp, 1), r)
                       for et, ep, xt, xp, r in trades[-3:]])

    # ── #7908 (Binance) qualitative comparison ──────────────────────────────
    ref = list(csv.DictReader(open(FX / "trades_7908.csv")))
    rwins = sum(1 for r in ref if float(r["exit_price"]) > float(r["entry_price"]))
    print("\n=== #7908 reference (Binance 1h→4h) ===")
    print("ref trades=%d  win=%d (%.1f%%)" % (len(ref), rwins, 100 * rwins / len(ref)))

    # entry-timing overlap: sim entry within ±(1 bar + 2h grid offset) of a #7908 entry
    ref_open = sorted(int(float(r["opened_at"])) for r in ref)
    sim_open = sorted(et for et, *_ in trades)
    tol = _4H_MS + 2 * 3600 * 1000
    matched = 0
    for so in sim_open:
        j = bisect.bisect_left(ref_open, so)
        cand = [abs(so - ref_open[k]) for k in (j - 1, j) if 0 <= k < len(ref_open)]
        if cand and min(cand) <= tol:
            matched += 1
    print("sim entries near a #7908 entry (±1bar+grid): %d/%d (%.1f%%)"
          % (matched, len(sim_open), 100 * matched / max(1, len(sim_open))))
    print("\nNOTE: exchange differs (Bybit vs Binance) + #7908 has no intrabar; "
          "exact match not expected. Shape agreement is the signal.")


if __name__ == "__main__":
    main()
