"""Generate Jesse 2.1.2 ground-truth golden values for the Supertrend parity test.

This script MUST be run inside the backtester image, which carries the exact
Jesse (==2.1.2) used to produce combo #7908:

    docker run --rm \
        -v "$PWD/cryptoengine/tests/fixtures:/out" \
        --entrypoint python cryptoengine-backtest-backtester:latest \
        /out/_gen_golden_supertrend.py

It emits golden_supertrend.json: a deterministic OHLCV series plus the exact
Jesse outputs (supertrend line/direction, EMA 7/29/240, ATR-14). The live
indicators (cryptoengine/services/strategies/supertrend/indicators.py) are then
asserted against these in test_supertrend_parity.py — with NO Jesse dependency
at test time.

Determinism: fixed seed + pure-numpy generation. Re-running reproduces the file
byte-for-byte (Jesse supertrend/atr are numba/Rust but deterministic for fixed
input). Regenerate only if the parameter set or the data design intentionally
changes.
"""

from __future__ import annotations

import json

import numpy as np

# combo #7908 parameters
PERIOD = 9
FACTOR = 2.6
EMA_LENS = [7, 29, 240]
ATR_PERIOD = 14

# N >= live CANDLE_LOOKBACK (1000) + headroom so the parity test can validate the
# live deque(1000) window against full-history ground truth on real overlap.
N = 1400
SEED = 7908


def build_candles() -> np.ndarray:
    """Regime-switching OHLCV: up -> down -> chop -> up.

    Drives multiple Supertrend flips, EMA cross-overs, and varied ATR so the
    parity check exercises the band ratchet/reset and trend-gate branches, not
    just a monotonic trend.
    """
    rng = np.random.RandomState(SEED)
    seg = N // 7
    drift = np.concatenate([
        np.full(seg, 0.004),            # up
        np.full(seg, -0.004),           # down
        np.full(seg, 0.003),            # up
        np.full(seg, -0.005),           # down (sharper)
        np.full(seg, 0.0005),           # chop
        np.full(seg, 0.004),            # up
        np.full(N - 6 * seg, -0.003),   # down
    ])  # ~zero mean keeps price near 50k while forcing many ST flips / EMA crosses
    noise = rng.normal(0, 0.006, N)
    close = np.empty(N, dtype=np.float64)
    close[0] = 50000.0
    for i in range(1, N):
        close[i] = close[i - 1] * (1.0 + drift[i] + noise[i])

    open_ = np.empty(N, dtype=np.float64)
    open_[0] = close[0]
    open_[1:] = close[:-1]

    high = close * (1.0 + np.abs(rng.normal(0, 0.003, N)))
    low = close * (1.0 - np.abs(rng.normal(0, 0.003, N)))
    # enforce OHLC validity: high >= max(open, close), low <= min(open, close)
    high = np.maximum.reduce([high, open_, close])
    low = np.minimum.reduce([low, open_, close])

    vol = rng.uniform(100, 1000, N)
    ts = 1_500_000_000_000 + np.arange(N, dtype=np.int64) * (4 * 3600 * 1000)

    # Jesse candle layout: [timestamp, open, close, high, low, volume]
    return np.column_stack([ts, open_, close, high, low, vol]).astype(np.float64)


def _clean(arr) -> list:
    """NaN -> None so the JSON is valid and warmup gaps are explicit."""
    out = []
    for x in np.asarray(arr, dtype=float).tolist():
        out.append(None if (x is None or np.isnan(x)) else float(x))
    return out


def main() -> None:
    import jesse.indicators as ta

    candles = build_candles()
    ts = candles[:, 0].astype(np.int64)
    open_, close, high, low, vol = (candles[:, 1], candles[:, 2],
                                    candles[:, 3], candles[:, 4], candles[:, 5])

    st = ta.supertrend(candles, period=PERIOD, factor=FACTOR, sequential=True)
    st_line = np.asarray(st.trend, dtype=float)
    st_changed = np.asarray(st.changed, dtype=float)
    # direction convention matches backtest strategy.py st_direction: price > line -> +1
    st_dir = [int(1) if close[i] > st_line[i] else int(-1) for i in range(N)]

    emas = {p: np.asarray(ta.ema(candles, period=p, sequential=True), dtype=float)
            for p in EMA_LENS}
    atr14 = np.asarray(ta.atr(candles, period=ATR_PERIOD, sequential=True), dtype=float)

    out = {
        "meta": {
            "jesse_version": "2.1.2",
            "period": PERIOD, "factor": FACTOR,
            "ema_lens": EMA_LENS, "atr_period": ATR_PERIOD,
            "n": N, "seed": SEED,
            "note": "ground-truth from combo #7908 Jesse; do not edit by hand",
        },
        "ohlcv": {
            "ts": [int(x) for x in ts],
            "open": _clean(open_), "high": _clean(high),
            "low": _clean(low), "close": _clean(close), "volume": _clean(vol),
        },
        "expected": {
            "st_line": _clean(st_line),
            "st_changed": _clean(st_changed),
            "st_dir": st_dir,
            "ema7": _clean(emas[7]),
            "ema29": _clean(emas[29]),
            "ema240": _clean(emas[240]),
            "atr14": _clean(atr14),
        },
    }

    with open("/out/golden_supertrend.json", "w") as f:
        json.dump(out, f)

    flips = int(np.nansum(st_changed))
    print("WROTE /out/golden_supertrend.json  n=%d  st_flips=%d" % (N, flips))
    print("  st_line[-1]=%.4f st_dir[-1]=%d" % (st_line[-1], st_dir[-1]))
    print("  ema7[-1]=%.4f ema29[-1]=%.4f ema240[-1]=%.4f atr14[-1]=%.4f" % (
        emas[7][-1], emas[29][-1], emas[240][-1], atr14[-1]))
    print("  price[-1]=%.4f  (entry filter price>ema240? %s)" % (
        close[-1], bool(close[-1] > emas[240][-1])))


if __name__ == "__main__":
    main()
