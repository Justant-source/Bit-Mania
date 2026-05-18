"""Self-contained Supertrend and EMA indicators using TA-Lib and pandas."""

from __future__ import annotations

import numpy as np
import pandas as pd
import talib


def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Compute ATR (Average True Range) and return the latest value.

    Args:
        df: DataFrame with 'high', 'low', 'close' columns.
        period: ATR period (default 14).

    Returns:
        Latest ATR value as float.
    """
    if len(df) < period:
        return 0.0

    high = df["high"].values
    low = df["low"].values
    close = df["close"].values

    atr = talib.ATR(high, low, close, timeperiod=period)
    latest = atr[-1]

    return float(latest) if not np.isnan(latest) else 0.0


def compute_ema(df: pd.DataFrame, period: int) -> pd.Series:
    """Compute EMA (Exponential Moving Average) of close prices.

    Args:
        df: DataFrame with 'close' column.
        period: EMA period.

    Returns:
        Series of EMA values (same length as df).
    """
    close = df["close"].values
    ema = talib.EMA(close, timeperiod=period)
    return pd.Series(ema, index=df.index)


def compute_supertrend(df: pd.DataFrame, period: int, factor: float) -> int:
    """Compute Supertrend and return the current trend direction.

    Algorithm:
    1. atr = ATR(high, low, close, period)
    2. hl2 = (high + low) / 2
    3. basic_ub = hl2 + factor * atr
    4. basic_lb = hl2 - factor * atr
    5. final_ub: stacked upper band (moves toward close when trend intact)
    6. final_lb: stacked lower band (moves toward close when trend intact)
    7. trend: +1 if close > final_ub, -1 if close < final_lb, else sticky

    Args:
        df: DataFrame with 'high', 'low', 'close' columns (must have period + 1 rows).
        period: Supertrend period (typical 10).
        factor: ATR multiplier (typical 3.0).

    Returns:
        Latest trend direction: +1 (uptrend) or -1 (downtrend).
    """
    if len(df) < period + 1:
        return 0

    high = df["high"].values
    low = df["low"].values
    close = df["close"].values

    # Compute ATR
    atr = talib.ATR(high, low, close, timeperiod=period)

    # Compute hl2
    hl2 = (high + low) / 2.0

    # Basic bands
    basic_ub = hl2 + factor * atr
    basic_lb = hl2 - factor * atr

    # Stacked (final) bands with sticky logic
    final_ub = np.full_like(close, np.nan)
    final_lb = np.full_like(close, np.nan)
    trend = np.ones_like(close, dtype=int)  # default uptrend

    # Seed bands at first non-NaN ATR position (index = period, TA-Lib convention)
    first_valid = np.where(~np.isnan(atr))[0]
    if len(first_valid) == 0:
        return 0
    seed = int(first_valid[0])
    final_ub[seed] = basic_ub[seed]
    final_lb[seed] = basic_lb[seed]

    # Compute bands and trends bar-by-bar starting after seed
    for i in range(seed + 1, len(close)):
        # Upper band: tighten (lower) only if new band is smaller; carry forward on NaN
        prev_ub = final_ub[i - 1]
        if np.isnan(prev_ub):
            final_ub[i] = basic_ub[i] if not np.isnan(basic_ub[i]) else np.nan
        elif np.isnan(basic_ub[i]) or basic_ub[i] >= prev_ub:
            final_ub[i] = prev_ub
        else:
            final_ub[i] = basic_ub[i]

        # Lower band: tighten (raise) only if new band is larger; carry forward on NaN
        prev_lb = final_lb[i - 1]
        if np.isnan(prev_lb):
            final_lb[i] = basic_lb[i] if not np.isnan(basic_lb[i]) else np.nan
        elif np.isnan(basic_lb[i]) or basic_lb[i] <= prev_lb:
            final_lb[i] = prev_lb
        else:
            final_lb[i] = basic_lb[i]

        # Determine trend; if bands are NaN, carry forward previous trend
        if np.isnan(final_lb[i]) or np.isnan(final_ub[i]):
            trend[i] = trend[i - 1]
        elif close[i] <= final_lb[i]:
            trend[i] = -1
        elif close[i] >= final_ub[i]:
            trend[i] = 1
        else:
            trend[i] = trend[i - 1]

    # Return the latest trend
    return int(trend[-1])
