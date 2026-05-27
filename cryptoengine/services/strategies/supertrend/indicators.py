"""Supertrend and EMA indicators.

compute_supertrend is a direct port of Jesse framework's ta.supertrend
(atr_loop + supertrend_fast) so that live signals match the backtest exactly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import talib


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Compute ATR (Average True Range) and return the latest value."""
    if len(df) < period:
        return 0.0

    high = df["high"].values
    low = df["low"].values
    close = df["close"].values

    atr = talib.ATR(high, low, close, timeperiod=period)
    latest = atr[-1]

    return float(latest) if not np.isnan(latest) else 0.0


def compute_ema(df: pd.DataFrame, period: int) -> pd.Series:
    """Compute EMA (Exponential Moving Average) of close prices."""
    close = df["close"].values
    ema = talib.EMA(close, timeperiod=period)
    return pd.Series(ema, index=df.index)


def _atr_jesse(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    """Wilder ATR matching Jesse's atr_loop exactly.

    Differences from talib.ATR: seeds at index period-1 (not period),
    TR[0] = high[0]-low[0], SMA seed then Wilder recursion.
    """
    n = len(close)
    tr = np.empty(n, dtype=np.float64)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        d1 = high[i] - low[i]
        d2 = abs(float(high[i]) - float(close[i - 1]))
        d3 = abs(float(low[i]) - float(close[i - 1]))
        tr[i] = d1 if d1 >= d2 and d1 >= d3 else (d2 if d2 >= d3 else d3)

    atr = np.full(n, np.nan, dtype=np.float64)
    atr[period - 1] = tr[:period].mean()
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def compute_supertrend(df: pd.DataFrame, period: int, factor: float) -> tuple[int, float]:
    """Compute Supertrend direction and line value (port of Jesse ta.supertrend).

    Algorithm matches Jesse's supertrend_fast exactly:
    - Wilder ATR via _atr_jesse (seeds at period-1, not period)
    - Upper band resets when prevClose > upper_band (Pine Script rule)
    - Lower band resets when prevClose < lower_band
    - Trend is gated on the active band of the previous bar
    - Direction = +1 if close > st_line else -1

    Args:
        df: DataFrame with 'high', 'low', 'close' columns.
        period: ATR period (combo #7908: 9).
        factor: ATR multiplier (combo #7908: 2.6).

    Returns:
        (trend_dir, st_line): trend_dir is +1 (uptrend) or -1 (downtrend);
        st_line is the supertrend band price (lower band when up, upper when down).
    """
    n = len(df)
    if n < period:
        return 0, 0.0

    high = df["high"].values.astype(np.float64)
    low = df["low"].values.astype(np.float64)
    close = df["close"].values.astype(np.float64)

    atr = _atr_jesse(high, low, close, period)

    # Pre-compute basic bands (NaN where ATR is NaN)
    mid = (high + low) / 2.0
    upper_band = mid + factor * atr
    lower_band = mid - factor * atr

    st_line = np.zeros(n, dtype=np.float64)

    # Seed at period-1 (first valid ATR)
    seed = period - 1
    st_line[seed] = upper_band[seed] if close[seed] <= upper_band[seed] else lower_band[seed]

    # Bar-by-bar sticky band + trend computation (exact Jesse supertrend_fast logic)
    for i in range(period, n):
        p = i - 1
        prev_close = close[p]

        # Upper band: ratchet down unless previous close broke above it (reset)
        if prev_close <= upper_band[p]:
            upper_band[i] = min(upper_band[i], upper_band[p])
        # else: upper_band[i] stays at basic (reset by prior close piercing)

        # Lower band: ratchet up unless previous close broke below it (reset)
        if prev_close >= lower_band[p]:
            lower_band[i] = max(lower_band[i], lower_band[p])
        # else: lower_band[i] stays at basic (reset)

        # Trend: gated on which band was the active supertrend line last bar
        if st_line[p] == upper_band[p]:
            # Previous bar was downtrend (line = upper band)
            st_line[i] = lower_band[i] if close[i] > upper_band[i] else upper_band[i]
        else:
            # Previous bar was uptrend (line = lower band)
            st_line[i] = upper_band[i] if close[i] < lower_band[i] else lower_band[i]

    last_line = float(st_line[-1])
    trend_dir = 1 if float(close[-1]) > last_line else -1

    return trend_dir, last_line
