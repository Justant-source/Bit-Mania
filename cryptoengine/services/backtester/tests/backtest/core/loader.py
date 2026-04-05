"""core.loader — DB에서 OHLCV·펀딩비 데이터를 로드하는 공통 함수.

사용법:
    from core.loader import load_ohlcv, load_funding

    ohlcv   = await load_ohlcv(pool, "BTCUSDT", "1h", start_dt, end_dt)
    funding = await load_funding(pool, "BTCUSDT", start_dt, end_dt)
"""
from __future__ import annotations

from datetime import datetime

import asyncpg
import pandas as pd


async def load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """ohlcv_history 테이블에서 OHLCV 데이터를 로드한다.

    Returns:
        DataFrame (index=ts UTC, columns=[open,high,low,close,volume])
        데이터 없으면 빈 DataFrame.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = $2
              AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            """,
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    return df


async def load_funding(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """funding_rate_history 테이블에서 펀딩비 데이터를 로드한다.

    Returns:
        DataFrame (index=ts UTC, columns=[rate])
        데이터 없으면 빈 DataFrame.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    df["rate"] = df["rate"].astype(float)
    return df
