"""v12 shared analysis library — thin wrapper around cryptoengine/tests/fixtures/_replay_supertrend.

FROZEN once Phase 2 (grid exploration) begins. See PREREGISTRATION.md. Any change after that
point invalidates every gate result computed before the change and requires a full re-run.

All v12 scripts (grid runner + the six G-gate scripts) import this module instead of talking
to _replay_supertrend directly, so the execution assumptions (fee/warmup/safety-stop) and the
block/grid/scoring definitions are defined in exactly one place.
"""
from __future__ import annotations

import csv as csvmod
import itertools
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[4]
_FX = _REPO / "cryptoengine" / "tests" / "fixtures"
if str(_FX) not in sys.path:
    sys.path.insert(0, str(_FX))
import _replay_supertrend as RS  # noqa: E402

CSV = _FX / "btc_4h_extended.csv"
WARMUP_BARS = 420
DESIGN_END = "2025-01-01"      # exclusive — holdout begins here, never touched before Phase 4
HOLDOUT_END = "2026-08-29"     # exclusive — last bar is 2026-08-28 20:00 UTC
FEE = RS.FEE                   # 0.00055, unchanged from the canonical tool

PARAMS5 = ["st_factor", "st_period", "fast_ema", "slow_ema", "dir_ema"]
ATR_FIXED = 3.3

# ── Phase 2 grid (§5.1 of the v12 plan, transcribed verbatim — do not edit after
#    PREREGISTRATION.md is written) ────────────────────────────────────────────
GRID = {
    "st_factor": [2.0, 2.2, 2.4, 2.6, 2.8, 3.0],
    "st_period": [6, 8, 10, 12],
    "fast_ema":  [4, 6, 8, 10],
    "slow_ema":  [24, 27, 30, 33],
    "dir_ema":   [180, 220, 260, 300],
}
BASELINE = dict(st_factor=2.6, st_period=9, fast_ema=7, slow_ema=29, dir_ema=240, atr_mult=3.3)


def date_ms(s: str) -> int:
    return RS.date_ms(s)


def design_end_ms() -> int:
    return date_ms(DESIGN_END)


def holdout_end_ms() -> int:
    return date_ms(HOLDOUT_END)


def all_combos() -> list[dict]:
    keys = PARAMS5
    out = []
    for vals in itertools.product(*[GRID[k] for k in keys]):
        d = dict(zip(keys, vals))
        d["atr_mult"] = ATR_FIXED
        out.append(d)
    return out


def block_boundaries() -> list[datetime]:
    """Mechanical 6-month blocks from the warm-up end date to DESIGN_END. Deterministic and
    calendar-based — computed once, before any combo is scored, so the boundaries cannot be
    chosen to flatter any particular result (D5: no hindsight cycle-boundary picking)."""
    rows = list(csvmod.DictReader(open(CSV)))
    t0 = int(rows[WARMUP_BARS]["timestamp"])
    start = datetime.fromtimestamp(t0 / 1000, timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    end = datetime.strptime(DESIGN_END, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    bounds = [start]
    cur = start
    while cur < end:
        y, m = cur.year, cur.month + 6
        y += (m - 1) // 12
        m = (m - 1) % 12 + 1
        cur = cur.replace(year=y, month=m)
        bounds.append(min(cur, end))
    return bounds


BLOCKS = block_boundaries()
N_BLOCKS = len(BLOCKS) - 1


def run(params: dict, start_ms: int | None = None, end_ms: int | None = None, **kw):
    """One backtest with the v12-locked execution assumptions (safety stop on, fixed 420-bar
    warm-up, canonical fee, no slippage, close-fill). kw can override for G6 sensitivity."""
    defaults = dict(warmup_bars=WARMUP_BARS, safety_stop=True, fee=FEE, slip_bps=0.0, fill="close")
    defaults.update(kw)
    trades, equity_4h, df, ts = RS.run_backtest(
        csv_path=CSV,
        st_factor=params["st_factor"], st_period=params["st_period"],
        fast=params["fast_ema"], slow=params["slow_ema"], dir_ema=params["dir_ema"],
        atr_mult=params.get("atr_mult", ATR_FIXED),
        start_ms=start_ms, end_ms=end_ms, **defaults,
    )
    return trades, equity_4h


def block_lg(equity_4h, bounds: list[datetime] | None = None) -> list[float]:
    """Per-block log growth ln(end/start) sliced from ONE continuous equity curve (not
    independent $10k windows — see phaseA_analysis_verdict.md §2 for why per-window
    independence + arithmetic-mean aggregation was part of what broke the Jesse selector)."""
    bounds = bounds if bounds is not None else BLOCKS
    daily = RS.daily_mtm(equity_4h)
    days = list(daily.keys())
    vals = list(daily.values())
    dts = [datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc) for d in days]
    lgs = []
    for i in range(len(bounds) - 1):
        lo, hi = bounds[i], bounds[i + 1]
        idx = [j for j, dt in enumerate(dts) if lo <= dt < hi]
        if not idx:
            lgs.append(0.0)
            continue
        v0 = vals[idx[0] - 1] if idx[0] > 0 else RS.START_BAL
        v1 = vals[idx[-1]]
        lgs.append(math.log(max(v1, 1e-6) / max(v0, 1e-6)))
    return lgs


def score_raw(lgs: list[float]) -> float:
    """S_raw = median block log-growth (§5.3). Median, not mean: a single outlier block
    (e.g. the 2020-21 window) must not dominate the score the way it dominated the
    Jesse mean-CAGR selector that produced #7908."""
    return float(np.median(lgs))


def neighbors(params: dict) -> list[dict]:
    """Grid-adjacent combos: exactly one of the 5 axes moved by one grid step. atr_mult is
    fixed and excluded from adjacency (v10 showed it carries no signal)."""
    out = []
    for k in PARAMS5:
        vals = GRID[k]
        i = vals.index(params[k])
        for di in (-1, 1):
            j = i + di
            if 0 <= j < len(vals):
                nb = dict(params)
                nb[k] = vals[j]
                out.append(nb)
    return out


def key5(params: dict) -> tuple:
    return tuple(params[k] for k in PARAMS5)
