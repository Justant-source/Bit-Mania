"""
Phase 7.3 — Import Binance Vision Parquet → Jesse PostgreSQL candle storage.

Converts Binance OHLCV Parquet files (from download_binance_vision.py)
into Jesse's candles table format and bulk-inserts them.

Jesse candle schema:
    id            BIGSERIAL PRIMARY KEY
    timestamp     BIGINT NOT NULL          -- milliseconds since epoch
    open          DECIMAL(32, 16) NOT NULL
    close         DECIMAL(32, 16) NOT NULL
    high          DECIMAL(32, 16) NOT NULL
    low           DECIMAL(32, 16) NOT NULL
    volume        DECIMAL(32, 16) NOT NULL
    exchange      VARCHAR(50) NOT NULL     -- e.g. 'Bybit Perpetual'
    symbol        VARCHAR(50) NOT NULL     -- e.g. 'BTCUSDT'
    timeframe     VARCHAR(10) NOT NULL     -- e.g. '1h'

Usage:
    python scripts/jesse_import.py --symbol BTCUSDT --timeframe 1h \
        --start 2020-01-01 --end 2026-04-01

    python scripts/jesse_import.py --symbol BTCUSDT --timeframe 1d \
        --start 2020-01-01 --end 2026-04-01
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timezone
import asyncio

try:
    import polars as pl
except ImportError:
    print("ERROR: polars not installed. Run: pip install polars", file=sys.stderr)
    sys.exit(1)

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg not installed. Run: pip install asyncpg", file=sys.stderr)
    sys.exit(1)

import os

# ─── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
BINANCE_VISION_DIR = DATA_DIR / "binance_vision"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB   = os.environ.get("POSTGRES_NAME", "jesse_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cryptoengine")
POSTGRES_PASS = os.environ.get("DB_PASSWORD", "")

JESSE_EXCHANGE = "Bybit Perpetual"

# Binance Vision timeframe → Jesse timeframe mapping
TF_MAP = {
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
}

# ─── Parquet loader ─────────────────────────────────────────────────────────────

def load_parquet(symbol: str, timeframe: str, start: str, end: str) -> pl.DataFrame:
    """
    Load Binance Vision Parquet files for the given symbol/timeframe/date range.

    Expected file locations (set by download_binance_vision.py):
        /data/binance_vision/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-<YYYY-MM-DD>.parquet
    OR  /data/binance_vision/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-<YYYY-MM>.parquet
    """
    search_paths = [
        BINANCE_VISION_DIR / "futures" / "um" / "daily" / "klines" / symbol / timeframe,
        BINANCE_VISION_DIR / "futures" / "um" / "monthly" / "klines" / symbol / timeframe,
        BINANCE_VISION_DIR / symbol / timeframe,  # fallback flat layout
    ]

    frames = []
    for base in search_paths:
        if not base.exists():
            continue
        for f in sorted(base.glob("*.parquet")):
            frames.append(pl.read_parquet(f))

    if not frames:
        # Try a single combined parquet
        combined = BINANCE_VISION_DIR / f"{symbol}_{timeframe}.parquet"
        if combined.exists():
            frames.append(pl.read_parquet(combined))

    if not frames:
        raise FileNotFoundError(
            f"No Parquet files found for {symbol}/{timeframe}. "
            f"Searched: {[str(p) for p in search_paths]}\n"
            "Run: python scripts/download_binance_vision.py first."
        )

    df = pl.concat(frames, rechunk=True)

    # Normalize column names (Binance Vision has fixed schema)
    # Expected columns: open_time, open, high, low, close, volume, close_time, ...
    col_lower = {c: c.lower() for c in df.columns}
    df = df.rename(col_lower)

    # Timestamp column detection
    if "open_time" in df.columns:
        ts_col = "open_time"
    elif "timestamp" in df.columns:
        ts_col = "timestamp"
    else:
        raise ValueError(f"Cannot find timestamp column. Columns: {df.columns}")

    # Ensure timestamp is in milliseconds
    ts_series = df[ts_col]
    if ts_series.max() < 1e12:  # seconds, not ms
        df = df.with_columns((pl.col(ts_col) * 1000).alias(ts_col))

    # Filter by date range
    start_ms = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() * 1000)

    df = df.filter(
        (pl.col(ts_col) >= start_ms) & (pl.col(ts_col) < end_ms)
    )

    # Select and rename to Jesse schema
    df = df.select([
        pl.col(ts_col).alias("timestamp"),
        pl.col("open").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ]).sort("timestamp")

    # Drop duplicates
    df = df.unique(subset=["timestamp"], keep="first").sort("timestamp")

    print(f"  Loaded {len(df):,} candles for {symbol}/{timeframe} "
          f"({start} → {end})")
    return df


# ─── PostgreSQL import ──────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS candles (
    id        BIGSERIAL PRIMARY KEY,
    timestamp BIGINT         NOT NULL,
    open      DECIMAL(32,16) NOT NULL,
    close     DECIMAL(32,16) NOT NULL,
    high      DECIMAL(32,16) NOT NULL,
    low       DECIMAL(32,16) NOT NULL,
    volume    DECIMAL(32,16) NOT NULL,
    exchange  VARCHAR(50)    NOT NULL,
    symbol    VARCHAR(50)    NOT NULL,
    timeframe VARCHAR(10)    NOT NULL,
    UNIQUE (exchange, symbol, timeframe, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_candles_lookup
    ON candles (exchange, symbol, timeframe, timestamp);
"""

INSERT_SQL = """
INSERT INTO candles (timestamp, open, close, high, low, volume, exchange, symbol, timeframe)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (exchange, symbol, timeframe, timestamp) DO NOTHING;
"""

CHUNK_SIZE = 5_000  # rows per INSERT batch


async def import_to_jesse(df: pl.DataFrame, symbol: str, timeframe: str) -> int:
    """Bulk-insert candle rows into Jesse's PostgreSQL candles table."""
    jesse_tf = TF_MAP.get(timeframe, timeframe)

    conn = await asyncpg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
    )

    try:
        await conn.execute(CREATE_TABLE_SQL)

        rows = df.to_dicts()
        inserted = 0

        for i in range(0, len(rows), CHUNK_SIZE):
            chunk = rows[i : i + CHUNK_SIZE]
            records = [
                (
                    r["timestamp"],
                    r["open"],
                    r["close"],
                    r["high"],
                    r["low"],
                    r["volume"],
                    JESSE_EXCHANGE,
                    symbol,
                    jesse_tf,
                )
                for r in chunk
            ]
            result = await conn.executemany(INSERT_SQL, records)
            inserted += len(chunk)
            print(f"  Inserted chunk {i//CHUNK_SIZE + 1}: {inserted:,}/{len(rows):,} rows", end="\r")

        print(f"\n  Done: {inserted:,} rows → candles table "
              f"(exchange='{JESSE_EXCHANGE}', symbol='{symbol}', timeframe='{jesse_tf}')")
        return inserted

    finally:
        await conn.close()


# ─── Verification ───────────────────────────────────────────────────────────────

async def verify_import(symbol: str, timeframe: str) -> None:
    """Query row count and date range to confirm import success."""
    jesse_tf = TF_MAP.get(timeframe, timeframe)
    conn = await asyncpg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS,
    )
    try:
        row = await conn.fetchrow(
            """
            SELECT COUNT(*) as cnt,
                   MIN(timestamp) as first_ts,
                   MAX(timestamp) as last_ts
            FROM candles
            WHERE exchange=$1 AND symbol=$2 AND timeframe=$3
            """,
            JESSE_EXCHANGE, symbol, jesse_tf,
        )
        if row["cnt"] == 0:
            print(f"  WARN: 0 rows found in candles for {symbol}/{jesse_tf}")
            return

        first = datetime.fromtimestamp(row["first_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        last  = datetime.fromtimestamp(row["last_ts"]  / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        print(f"  Verification OK: {row['cnt']:,} rows, {first} → {last}")
    finally:
        await conn.close()


# ─── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Import Binance Vision Parquet → Jesse PostgreSQL")
    p.add_argument("--symbol",    default="BTCUSDT", help="e.g. BTCUSDT")
    p.add_argument("--timeframe", default="1h",      help="1h | 4h | 1d")
    p.add_argument("--start",     default="2020-01-01", help="YYYY-MM-DD")
    p.add_argument("--end",       default="2026-04-01", help="YYYY-MM-DD")
    p.add_argument("--verify",    action="store_true", help="Verify row count after import")
    return p.parse_args()


async def main():
    args = parse_args()
    print(f"[jesse_import] {args.symbol} {args.timeframe} {args.start}→{args.end}")

    df = load_parquet(args.symbol, args.timeframe, args.start, args.end)
    if len(df) == 0:
        print("ERROR: Empty DataFrame after filtering. Check date range.", file=sys.stderr)
        sys.exit(1)

    await import_to_jesse(df, args.symbol, args.timeframe)

    if args.verify:
        await verify_import(args.symbol, args.timeframe)


if __name__ == "__main__":
    asyncio.run(main())
