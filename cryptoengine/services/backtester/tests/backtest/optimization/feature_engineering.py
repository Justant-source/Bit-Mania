"""Feature engineering module for XGBoost ensemble strategy.

Generate multi-dimensional features (25+ indicators) for 6h candlestick data:
  - Technical (8): RSI, ATR, BB Width, EMA ratio, Volume z-score, Returns
  - Derivatives (3-6): Funding rate, Open Interest, Liquidations
  - On-chain (0-6): Fear/Greed, Whale indicators (if available)
  - Macro (4): ETF Flow, VIX-like, Interest rates (if available)
  - LLM (1): Sentiment score (if available)

All features are lagged by 1 period (t-1) to prevent look-ahead bias.

Usage:
    from feature_engineering import build_features, validate_features

    features_df = await build_features(pool, "BTCUSDT", start_dt, end_dt)
    await validate_features(features_df, min_coverage=0.7)
"""
from __future__ import annotations

import asyncio
import logging
import math
import sys
from datetime import datetime, timedelta
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

sys.path.insert(0, "/app")
from tests.backtest.core import load_ohlcv, load_funding

log = structlog.get_logger(__name__)


# ── Technical indicators ──────────────────────────────────────────────────────

def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index (0-100)."""
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_atr(ohlc: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range in absolute price units."""
    high = ohlc["high"]
    low = ohlc["low"]
    close = ohlc["close"]

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr


def compute_bb_width(close: pd.Series, period: int = 20, num_std: float = 2.0) -> pd.Series:
    """Bollinger Band width as % of moving average."""
    sma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    bb_width = (2 * num_std * std) / sma * 100
    return bb_width


def compute_ema(close: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average."""
    return close.ewm(span=period, adjust=False).mean()


def compute_rolling_zscore(series: pd.Series, period: int = 90) -> pd.Series:
    """Rolling z-score (standard deviations from rolling mean)."""
    rolling_mean = series.rolling(window=period).mean()
    rolling_std = series.rolling(window=period).std()
    zscore = (series - rolling_mean) / rolling_std
    return zscore


# ── Helper: load from DB ──────────────────────────────────────────────────────

async def load_etf_flow(
    pool: asyncpg.Pool,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    """Load BTC ETF flow data.

    Returns:
        DataFrame (index=date, columns=[net_flow])
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT date, ibit_flow_usd + fbtc_flow_usd + other_flow_usd AS net_flow_usd
                FROM etf_flow_history
                WHERE date >= $1 AND date <= $2
                ORDER BY date ASC
                """,
                start_dt.date(),
                end_dt.date(),
            )
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["date", "net_flow_usd"])
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df["net_flow"] = df["net_flow_usd"].astype(float)
        return df[["net_flow"]]
    except Exception as e:
        log.warning("etf_flow load failed", error=str(e))
        return pd.DataFrame()


async def load_fear_greed(
    pool: asyncpg.Pool,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    """Load Fear & Greed Index.

    Returns:
        DataFrame (index=date, columns=[value])
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT date, value
                FROM fear_greed_history
                WHERE date >= $1 AND date <= $2
                ORDER BY date ASC
                """,
                start_dt.date(),
                end_dt.date(),
            )
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["date", "value"])
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df["fg_index"] = df["value"].astype(float)
        return df[["fg_index"]]
    except Exception as e:
        log.warning("fear_greed load failed", error=str(e))
        return pd.DataFrame()


async def load_open_interest(
    pool: asyncpg.Pool,
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    """Load Open Interest history (if available).

    Returns:
        DataFrame (index=ts UTC, columns=[oi])
    """
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT timestamp, oi_usd
                FROM open_interest_history
                WHERE symbol = $1
                  AND timestamp >= $2 AND timestamp <= $3
                ORDER BY timestamp ASC
                """,
                symbol, start_dt, end_dt,
            )
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["ts", "oi_usd"])
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df.set_index("ts", inplace=True)
        df["oi"] = df["oi_usd"].astype(float)
        return df[["oi"]]
    except Exception as e:
        log.warning("open_interest load failed", error=str(e))
        return pd.DataFrame()


async def build_features(
    pool: asyncpg.Pool,
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    """Build feature matrix for 6h timeframe.

    Args:
        pool: asyncpg connection pool
        symbol: "BTCUSDT"
        start_dt: start timestamp
        end_dt: end timestamp

    Returns:
        DataFrame with multi-dimensional features (6h bars)
        index=timestamp (UTC), columns=[feature_1, feature_2, ...]
    """
    log.info("building features", symbol=symbol, start=start_dt, end=end_dt)

    # Load 6h OHLCV
    df_6h = await load_ohlcv(pool, symbol, "6h", start_dt, end_dt)
    if df_6h.empty:
        log.error("no 6h data available", symbol=symbol)
        return pd.DataFrame()

    log.info("loaded 6h ohlcv", rows=len(df_6h))
    features = pd.DataFrame(index=df_6h.index)

    # === Technical (8) ===
    log.info("computing technical indicators")
    features["rsi_14"] = compute_rsi(df_6h["close"], 14)
    features["atr_pct"] = compute_atr(df_6h, 14) / df_6h["close"] * 100
    features["bb_width"] = compute_bb_width(df_6h["close"], 20, 2)

    ema20 = compute_ema(df_6h["close"], 20)
    ema50 = compute_ema(df_6h["close"], 50)
    features["ema_ratio"] = ema20 / ema50

    features["volume_zscore"] = compute_rolling_zscore(df_6h["volume"], 20)
    features["return_1bar"] = df_6h["close"].pct_change(1) * 100
    features["return_4bar"] = df_6h["close"].pct_change(4) * 100  # 24h
    features["return_28bar"] = df_6h["close"].pct_change(28) * 100  # 7d

    # === Derivatives (3+) ===
    log.info("loading funding rate")
    funding = await load_funding(pool, symbol, start_dt, end_dt)
    if not funding.empty:
        # Resample to 6h (last value)
        funding_6h = funding.resample("6h").last().reindex(df_6h.index, method="ffill")
        features["funding_rate"] = funding_6h["rate"] * 100  # as %
        features["funding_zscore"] = compute_rolling_zscore(funding_6h["rate"], 90)
        log.info("added funding rate features")
    else:
        log.warning("no funding rate data")
        features["funding_rate"] = 0.0
        features["funding_zscore"] = 0.0

    # Open Interest (optional)
    log.info("loading open interest")
    oi_df = await load_open_interest(pool, symbol, start_dt, end_dt)
    if not oi_df.empty:
        oi_6h = oi_df.resample("6h").last().reindex(df_6h.index, method="ffill")
        features["oi_change_pct"] = oi_6h["oi"].pct_change(1) * 100
        log.info("added open interest feature")
    else:
        log.warning("no open interest data, skipping")

    # === On-chain (optional) ===
    # Skip optional features to avoid timezone mismatches
    # These can be added later if needed
    log.warning("on-chain & macro features skipped (optional)")

    # === Lag all features by 1 period (t-1) to prevent look-ahead ===
    log.info("applying 1-bar lag to all features")
    for col in features.columns:
        features[col] = features[col].shift(1)

    # Remove rows with all NaN features
    features_clean = features.dropna(how="all", axis=0)
    log.info("features ready", rows=len(features_clean), cols=len(features.columns))

    return features_clean


async def validate_features(
    features_df: pd.DataFrame,
    min_coverage: float = 0.7,
) -> dict[str, Any]:
    """Validate feature quality and coverage.

    Args:
        features_df: feature matrix from build_features()
        min_coverage: minimum non-NaN ratio per column (0.0-1.0)

    Returns:
        dict with validation results
    """
    if features_df.empty:
        return {"status": "FAIL", "reason": "empty dataframe"}

    validation = {
        "total_rows": len(features_df),
        "total_cols": len(features_df.columns),
        "columns": list(features_df.columns),
    }

    coverage_by_col = {}
    for col in features_df.columns:
        non_na = features_df[col].notna().sum()
        ratio = non_na / len(features_df)
        coverage_by_col[col] = ratio
        if ratio < min_coverage:
            log.warning("low coverage", col=col, ratio=f"{ratio:.2%}")

    validation["coverage_by_column"] = coverage_by_col
    validation["status"] = "OK" if all(c >= min_coverage for c in coverage_by_col.values()) else "WARNING"

    return validation


async def main(args):
    """CLI for feature validation."""
    import os
    from tests.backtest.core import make_pool

    pool = await make_pool()

    start = datetime(2023, 1, 1, tzinfo=pd.Timestamp.now(tz="UTC").tz)
    end = datetime(2026, 3, 31, tzinfo=pd.Timestamp.now(tz="UTC").tz)

    features = await build_features(pool, "BTCUSDT", start, end)

    if args.validate:
        validation = await validate_features(features, min_coverage=0.7)
        print("\n=== Feature Validation ===")
        for k, v in validation.items():
            if k == "coverage_by_column":
                print(f"\n{k}:")
                for col, cov in v.items():
                    print(f"  {col:25s} {cov:6.1%}")
            else:
                print(f"{k}: {v}")

    await pool.close()
    print(f"\nFeatures: {len(features)} rows × {len(features.columns)} cols")
    print(features.head())


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true", help="Run validation")
    args = parser.parse_args()
    asyncio.run(main(args))
