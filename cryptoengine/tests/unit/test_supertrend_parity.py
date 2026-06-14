"""Supertrend indicator parity: live indicators vs Jesse 2.1.2 (combo #7908 ground truth).

The live strategy (services/strategies/supertrend/indicators.py) reimplements
Jesse's indicators so that mainnet signals match the backtest that selected
combo #7908. This test pins that claim to ground truth:

  * compute_supertrend  <-> Jesse numba supertrend_fast + atr_loop  (period 9, factor 2.6)
  * compute_ema         <-> Jesse jesse_rust.ema                    (7 / 29 / 240)
  * compute_atr/talib   <-> Jesse jesse_rust.atr                    (14, exit distance)

Ground truth lives in fixtures/golden_supertrend.json, produced by
fixtures/_gen_golden_supertrend.py run inside the backtester image (Jesse==2.1.2).
No Jesse dependency at test time — only the golden JSON + talib.

Beyond raw indicator parity we also assert ENTRY-FILTER DECISION parity per bar
(st_dir==1 AND fast>slow AND price>dir_ema): even if an indicator differs by a
hair, what matters operationally is whether the entry gate flips the same way.

Run (inside supertrend image which has talib):
    docker run --rm -v "$PWD/cryptoengine:/work" -w /work \
        --entrypoint python cryptoengine-supertrend:latest \
        tests/unit/test_supertrend_parity.py        # standalone, no pytest needed
or via pytest:
    pytest tests/unit/test_supertrend_parity.py -v
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import pytest
    _HAVE_PYTEST = True
except ImportError:  # standalone in-container execution
    _HAVE_PYTEST = False

# Project root on sys.path for `services...` imports (tests/unit -> cryptoengine)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.strategies.supertrend.indicators import (  # noqa: E402
    _atr_jesse,
    compute_atr,
    compute_ema,
    compute_supertrend,
)

FIXTURE = Path(__file__).resolve().parent.parent / "fixtures" / "golden_supertrend.json"

# Live deque size — keep in sync with strategy.py CANDLE_LOOKBACK. Live recomputes
# indicators on the most-recent N bars only; Jesse used full history, so the window
# must be large enough that the 240-EMA seed decays (see window_stability check).
CANDLE_LOOKBACK = 1000

# Supertrend is a faithful port -> expect (near) bit-identical on the same window.
ST_ABS_TOL = 1e-6
ST_REL_TOL = 1e-9
# EMA/ATR cross engines (talib vs jesse_rust) -> allow tiny float divergence,
# but flag any meaningful gap so a real seed/algorithm mismatch surfaces.
EMA_ABS_TOL = 1e-3
EMA_REL_TOL = 1e-6
ATR_ABS_TOL = 1e-3
ATR_REL_TOL = 1e-6


def _load_golden() -> dict:
    with open(FIXTURE) as f:
        return json.load(f)


def _load_df(g: dict) -> pd.DataFrame:
    o = g["ohlcv"]
    return pd.DataFrame({
        "open": o["open"], "high": o["high"], "low": o["low"],
        "close": o["close"], "volume": o["volume"],
    })


def _is_nan(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


# ───────────────────────── checks (shared by pytest + standalone) ──────────

def _check_supertrend_full_history(g: dict, df: pd.DataFrame) -> None:
    """compute_supertrend on full [0:k] window == Jesse st_line/st_dir, every bar."""
    period, factor = g["meta"]["period"], g["meta"]["factor"]
    exp_line, exp_dir = g["expected"]["st_line"], g["expected"]["st_dir"]
    n = len(df)
    line_mism, dir_mism = [], []
    for k in range(period + 1, n + 1):
        d, line = compute_supertrend(df.iloc[:k], period, factor)
        gl, gd = exp_line[k - 1], exp_dir[k - 1]
        if _is_nan(gl):
            continue
        if not math.isclose(line, gl, rel_tol=ST_REL_TOL, abs_tol=ST_ABS_TOL):
            line_mism.append((k - 1, line, gl, abs(line - gl)))
        if d != gd:
            dir_mism.append((k - 1, d, gd))
    assert not line_mism, (
        f"st_line diverges at {len(line_mism)}/{n} bars "
        f"(max abs {max(m[3] for m in line_mism):.3g}); first {line_mism[:3]}")
    assert not dir_mism, (
        f"st_dir diverges at {len(dir_mism)}/{n} bars; first {dir_mism[:5]}")


def _check_ema_full_history(g: dict, df: pd.DataFrame) -> None:
    """compute_ema (talib) == Jesse ema for 7/29/240 on overlapping (non-warmup) bars."""
    failures = {}
    for p, key in ((7, "ema7"), (29, "ema29"), (240, "ema240")):
        exp = g["expected"][key]
        live = compute_ema(df, p).to_numpy()
        diffs = []
        for i in range(len(live)):
            gv, lv = exp[i], live[i]
            if _is_nan(gv) or _is_nan(lv):
                continue
            if not math.isclose(lv, gv, rel_tol=EMA_REL_TOL, abs_tol=EMA_ABS_TOL):
                diffs.append((i, lv, gv, abs(lv - gv)))
        if diffs:
            failures[key] = (len(diffs), max(d[3] for d in diffs), diffs[:3])
    assert not failures, "EMA talib-vs-jesse_rust divergence: " + "; ".join(
        f"{k}: {c} bars, max abs {mx:.3g}, first {ex}" for k, (c, mx, ex) in failures.items())


def _check_atr_full_history(g: dict, df: pd.DataFrame) -> None:
    """The exit-distance ATR(14): live _atr_jesse (what compute_atr uses) == Jesse ATR."""
    exp = g["expected"]["atr14"]
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    live = _atr_jesse(high, low, close, 14)
    diffs = []
    for i in range(len(live)):
        gv, lv = exp[i], live[i]
        if _is_nan(gv) or _is_nan(lv):
            continue
        if not math.isclose(lv, gv, rel_tol=ATR_REL_TOL, abs_tol=ATR_ABS_TOL):
            diffs.append((i, lv, gv, abs(lv - gv)))
    assert not diffs, (
        f"ATR(14) talib-vs-jesse_rust diverges at {len(diffs)}/{len(live)} bars "
        f"(max abs {max(d[3] for d in diffs):.3g}); first {diffs[:3]}")
    # spot-check the wrapper compute_atr returns the same last value
    last = compute_atr(df, 14)
    assert math.isclose(last, exp[-1], rel_tol=ATR_REL_TOL, abs_tol=ATR_ABS_TOL), (
        f"compute_atr last={last} vs golden {exp[-1]}")


def _check_supertrend_window_stability(g: dict, df: pd.DataFrame) -> None:
    """Live uses deque(maxlen=CANDLE_LOOKBACK); Jesse used full history.

    The live window must reproduce, vs full-history ground truth:
      (a) supertrend direction  — ATR(9) converges fast, exact match expected
      (b) the price>dir_ema entry decision — needs LOOKBACK large enough that the
          240-EMA seed has decayed (the reason LOOKBACK was raised 300 -> 1000)
    """
    period, factor = g["meta"]["period"], g["meta"]["factor"]
    exp_line, exp_dir = g["expected"]["st_line"], g["expected"]["st_dir"]
    exp240 = g["expected"]["ema240"]
    close = g["ohlcv"]["close"]
    n = len(df)
    dir_mism, line_maxdiff = [], 0.0
    ema_dec_mism, ema_maxdiff = [], 0.0
    for k in range(CANDLE_LOOKBACK, n + 1):
        win = df.iloc[k - CANDLE_LOOKBACK:k]
        d, line = compute_supertrend(win, period, factor)
        e240 = compute_ema(win, 240).iloc[-1]
        i = k - 1
        gl, gd, g240 = exp_line[i], exp_dir[i], exp240[i]
        if not _is_nan(gl):
            if d != gd:
                dir_mism.append(i)
            line_maxdiff = max(line_maxdiff, abs(line - gl))
        if not _is_nan(g240):
            ema_maxdiff = max(ema_maxdiff, abs(e240 - g240))
            if (close[i] > e240) != (close[i] > g240):
                ema_dec_mism.append(i)
    assert not dir_mism, (
        f"deque({CANDLE_LOOKBACK}) vs full-history st_dir mismatch at "
        f"{len(dir_mism)} bars: {dir_mism[:5]} (line max diff {line_maxdiff:.4g})")
    assert not ema_dec_mism, (
        f"deque({CANDLE_LOOKBACK}) vs full-history price>dir_ema decision mismatch "
        f"at {len(ema_dec_mism)} bars: {ema_dec_mism[:5]} (ema240 max diff {ema_maxdiff:.4g})")


def _check_entry_filter_decision(g: dict, df: pd.DataFrame) -> None:
    """Operational truth: per-bar entry gate (excl. cooldowns) agrees with Jesse.

    Live gate: st_dir==1 AND fast_ema>slow_ema AND price>dir_ema, using LIVE
    indicators. Compared against the same gate on Jesse golden indicators.
    """
    period, factor = g["meta"]["period"], g["meta"]["factor"]
    close = np.asarray(g["ohlcv"]["close"], dtype=float)
    exp_dir = g["expected"]["st_dir"]
    eg7, eg29, eg240 = g["expected"]["ema7"], g["expected"]["ema29"], g["expected"]["ema240"]
    live7 = compute_ema(df, 7).to_numpy()
    live29 = compute_ema(df, 29).to_numpy()
    live240 = compute_ema(df, 240).to_numpy()
    n = len(df)
    mism = []
    for k in range(period + 1, n + 1):
        i = k - 1
        if _is_nan(eg240[i]) or _is_nan(live240[i]):
            continue
        d, _ = compute_supertrend(df.iloc[:k], period, factor)
        live_entry = (d == 1) and (live7[i] > live29[i]) and (close[i] > live240[i])
        jesse_entry = (exp_dir[i] == 1) and (eg7[i] > eg29[i]) and (close[i] > eg240[i])
        if live_entry != jesse_entry:
            mism.append((i, live_entry, jesse_entry))
    assert not mism, (
        f"entry-filter decision diverges at {len(mism)} bars; first {mism[:5]}")


# ───────────────────────────── pytest entry points ────────────────────────

if _HAVE_PYTEST:
    @pytest.fixture(scope="module")
    def golden():
        return _load_golden()

    @pytest.fixture(scope="module")
    def df_full(golden):
        return _load_df(golden)

    @pytest.mark.unit
    def test_supertrend_line_dir_full_history_parity(golden, df_full):
        _check_supertrend_full_history(golden, df_full)

    @pytest.mark.unit
    def test_ema_parity_full_history(golden, df_full):
        _check_ema_full_history(golden, df_full)

    @pytest.mark.unit
    def test_atr14_parity_full_history(golden, df_full):
        _check_atr_full_history(golden, df_full)

    @pytest.mark.unit
    def test_supertrend_window_stability(golden, df_full):
        _check_supertrend_window_stability(golden, df_full)

    @pytest.mark.unit
    def test_entry_filter_decision_parity(golden, df_full):
        _check_entry_filter_decision(golden, df_full)


# ──────────────────────── standalone runner (no pytest) ────────────────────

def _main() -> int:
    g = _load_golden()
    df = _load_df(g)
    print(f"golden: jesse {g['meta']['jesse_version']}  n={g['meta']['n']}  "
          f"period={g['meta']['period']} factor={g['meta']['factor']}")
    checks = [
        ("supertrend_line_dir_full_history", _check_supertrend_full_history),
        ("ema_parity_full_history", _check_ema_full_history),
        ("atr14_parity_full_history", _check_atr_full_history),
        ("supertrend_window_stability", _check_supertrend_window_stability),
        ("entry_filter_decision_parity", _check_entry_filter_decision),
    ]
    failed = 0
    for name, fn in checks:
        try:
            fn(g, df)
            print(f"PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {name}\n      {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"ERROR {name}\n      {type(e).__name__}: {e}")
    print(f"\n{len(checks) - failed}/{len(checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
