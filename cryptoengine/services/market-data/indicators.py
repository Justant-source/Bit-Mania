"""Technical indicator wrappers built on TA-Lib.

All functions accept a pandas DataFrame with columns:
  open, high, low, close, volume
and return indicator values as pandas Series (or tuples of Series for
multi-output indicators like Bollinger Bands and MACD).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import talib


# ---------------------------------------------------------------------------
# Trend indicators
# ---------------------------------------------------------------------------

def compute_ema(df: pd.DataFrame, period: int = 20, col: str = "close") -> pd.Series:
    """Exponential Moving Average."""
    return pd.Series(talib.EMA(df[col].values, timeperiod=period), index=df.index, name=f"ema_{period}")


def compute_sma(df: pd.DataFrame, period: int = 20, col: str = "close") -> pd.Series:
    """Simple Moving Average."""
    return pd.Series(talib.SMA(df[col].values, timeperiod=period), index=df.index, name=f"sma_{period}")


def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average Directional Index — trend strength."""
    return pd.Series(
        talib.ADX(df["high"].values, df["low"].values, df["close"].values, timeperiod=period),
        index=df.index,
        name=f"adx_{period}",
    )


def compute_plus_di(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Plus Directional Indicator."""
    return pd.Series(
        talib.PLUS_DI(df["high"].values, df["low"].values, df["close"].values, timeperiod=period),
        index=df.index,
        name=f"plus_di_{period}",
    )


def compute_minus_di(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Minus Directional Indicator."""
    return pd.Series(
        talib.MINUS_DI(df["high"].values, df["low"].values, df["close"].values, timeperiod=period),
        index=df.index,
        name=f"minus_di_{period}",
    )


# ---------------------------------------------------------------------------
# Volatility indicators
# ---------------------------------------------------------------------------

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range — volatility measure."""
    return pd.Series(
        talib.ATR(df["high"].values, df["low"].values, df["close"].values, timeperiod=period),
        index=df.index,
        name=f"atr_{period}",
    )


def compute_bb(
    df: pd.DataFrame,
    period: int = 20,
    std_dev: float = 2.0,
    col: str = "close",
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands — returns (upper, middle, lower)."""
    upper, middle, lower = talib.BBANDS(
        df[col].values,
        timeperiod=period,
        nbdevup=std_dev,
        nbdevdn=std_dev,
        matype=0,  # SMA
    )
    return (
        pd.Series(upper, index=df.index, name=f"bb_upper_{period}"),
        pd.Series(middle, index=df.index, name=f"bb_mid_{period}"),
        pd.Series(lower, index=df.index, name=f"bb_lower_{period}"),
    )


def compute_bb_width(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.Series:
    """Bollinger Band width as a ratio of the middle band."""
    upper, middle, lower = compute_bb(df, period=period, std_dev=std_dev)
    width = (upper - lower) / middle.replace(0, np.nan)
    width.name = f"bb_width_{period}"
    return width


# ---------------------------------------------------------------------------
# Momentum indicators
# ---------------------------------------------------------------------------

def compute_rsi(df: pd.DataFrame, period: int = 14, col: str = "close") -> pd.Series:
    """Relative Strength Index."""
    return pd.Series(
        talib.RSI(df[col].values, timeperiod=period),
        index=df.index,
        name=f"rsi_{period}",
    )


def compute_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    col: str = "close",
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD — returns (macd_line, signal_line, histogram)."""
    macd_line, signal_line, histogram = talib.MACD(
        df[col].values,
        fastperiod=fast,
        slowperiod=slow,
        signalperiod=signal,
    )
    return (
        pd.Series(macd_line, index=df.index, name="macd"),
        pd.Series(signal_line, index=df.index, name="macd_signal"),
        pd.Series(histogram, index=df.index, name="macd_hist"),
    )


def compute_stoch_rsi(
    df: pd.DataFrame,
    period: int = 14,
    k_period: int = 3,
    d_period: int = 3,
    col: str = "close",
) -> tuple[pd.Series, pd.Series]:
    """Stochastic RSI — returns (%K, %D)."""
    fastk, fastd = talib.STOCHRSI(
        df[col].values,
        timeperiod=period,
        fastk_period=k_period,
        fastd_period=d_period,
        fastd_matype=0,
    )
    return (
        pd.Series(fastk, index=df.index, name="stochrsi_k"),
        pd.Series(fastd, index=df.index, name="stochrsi_d"),
    )


def compute_cci(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Commodity Channel Index."""
    return pd.Series(
        talib.CCI(df["high"].values, df["low"].values, df["close"].values, timeperiod=period),
        index=df.index,
        name=f"cci_{period}",
    )


def compute_mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Money Flow Index — volume-weighted RSI."""
    return pd.Series(
        talib.MFI(
            df["high"].values,
            df["low"].values,
            df["close"].values,
            df["volume"].values,
            timeperiod=period,
        ),
        index=df.index,
        name=f"mfi_{period}",
    )


# ---------------------------------------------------------------------------
# Volume helpers
# ---------------------------------------------------------------------------

def compute_obv(df: pd.DataFrame) -> pd.Series:
    """On Balance Volume."""
    return pd.Series(talib.OBV(df["close"].values, df["volume"].values), index=df.index, name="obv")


def compute_vwap(df: pd.DataFrame) -> pd.Series:
    """Volume Weighted Average Price (rolling intraday approximation)."""
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    cumulative_tp_vol = (typical_price * df["volume"]).cumsum()
    cumulative_vol = df["volume"].cumsum()
    vwap = cumulative_tp_vol / cumulative_vol.replace(0, np.nan)
    vwap.name = "vwap"
    return vwap


def compute_volume_sma(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Simple moving average of volume."""
    return pd.Series(talib.SMA(df["volume"].values, timeperiod=period), index=df.index, name=f"vol_sma_{period}")


def compute_volume_ratio(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Current volume relative to its SMA — >1 means above-average volume."""
    vol_sma = compute_volume_sma(df, period)
    ratio = df["volume"] / vol_sma.replace(0, np.nan)
    ratio.name = f"vol_ratio_{period}"
    return ratio
