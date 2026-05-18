"""
Parquet 파일 레이크 → backtest-postgres 일괄 적재.

data/ohlcv/BTCUSDT/1m/YYYY-MM.parquet (105파일) 를
backtest-postgres 의 ohlcv_1m 테이블에 UPSERT 하고,
ohlcv_4h 를 1m에서 리샘플해 동시에 채운다.

실행 (backtester 컨테이너 안에서):
    python scripts/data/import_parquet_to_pg.py
    python scripts/data/import_parquet_to_pg.py --verify-only
    python scripts/data/import_parquet_to_pg.py --symbol BTCUSDT --exchange "Bybit Perpetual"

환경 변수 (기본값은 backtest-compose 기준):
    JESSE_DB_HOST   backtest-postgres
    JESSE_DB_PORT   5432
    JESSE_DB_NAME   jesse_db
    JESSE_DB_USER   jesse
    JESSE_DB_PASSWORD  ***REMOVED***
    DATA_DIR        /data
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

try:
    import polars as pl
except ImportError:
    sys.exit("ERROR: polars not installed — pip install polars")

try:
    import asyncpg
except ImportError:
    sys.exit("ERROR: asyncpg not installed — pip install asyncpg")

# ── Config ─────────────────────────────────────────────────────────────────────

DATA_DIR      = Path(os.environ.get("DATA_DIR", "/data"))
OHLCV_1M_DIR  = DATA_DIR / "ohlcv" / "BTCUSDT" / "1m"

DB_HOST = os.environ.get("JESSE_DB_HOST", "backtest-postgres")
DB_PORT = int(os.environ.get("JESSE_DB_PORT", "5432"))
DB_NAME = os.environ.get("JESSE_DB_NAME", "jesse_db")
DB_USER = os.environ.get("JESSE_DB_USER", "jesse")
DB_PASS = os.environ.get("JESSE_DB_PASSWORD", "***REMOVED***")

BATCH_SIZE = 50_000   # rows per INSERT batch

# ── Parquet loader ──────────────────────────────────────────────────────────────

def load_1m_parquet(symbol_dir: Path) -> pl.DataFrame:
    files = sorted(symbol_dir.glob("*.parquet"))
    if not files:
        sys.exit(f"ERROR: no parquet files found in {symbol_dir}")
    print(f"  Loading {len(files)} parquet files from {symbol_dir} …")
    df = pl.concat([pl.read_parquet(f) for f in files], rechunk=True)
    df = df.sort("timestamp").unique(subset=["timestamp"], keep="first")
    print(f"  Loaded {len(df):,} rows  [{df['timestamp'].min()} … {df['timestamp'].max()}] ms")
    return df


def resample_to_4h(df_1m: pl.DataFrame) -> pl.DataFrame:
    """1m DataFrame(timestamp ms) → 4h OHLCV (bar open timestamp ms)."""
    df = (
        df_1m
        .with_columns(
            # floor timestamp to 4h boundary (4*60*60*1000 = 14_400_000 ms)
            (pl.col("timestamp") // 14_400_000 * 14_400_000).alias("ts4h")
        )
        .group_by("ts4h")
        .agg([
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
        ])
        .sort("ts4h")
        .rename({"ts4h": "timestamp"})
    )
    print(f"  Resampled → {len(df):,} 4h bars")
    return df

# ── DB helpers ──────────────────────────────────────────────────────────────────

async def connect() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )


async def upsert_batch(conn: asyncpg.Connection, table: str,
                       exchange: str, symbol: str,
                       rows: list[tuple]) -> int:
    sql = f"""
        INSERT INTO {table} (exchange, symbol, timestamp, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
    """
    records = [(exchange, symbol, ts, o, h, l, c, v) for ts, o, h, l, c, v in rows]
    await conn.executemany(sql, records)
    return len(records)


async def insert_table(conn: asyncpg.Connection, table: str,
                       df: pl.DataFrame, exchange: str, symbol: str) -> int:
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    data = df.select(cols).to_numpy().tolist()
    total = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        total += await upsert_batch(conn, table, exchange, symbol, batch)
        print(f"    {table}: {total:,}/{len(data):,} rows inserted …", end="\r")
    print(f"    {table}: {total:,} rows inserted.       ")
    return total


async def verify(conn: asyncpg.Connection, table: str, exchange: str, symbol: str) -> dict:
    row = await conn.fetchrow(
        f"SELECT count(*) AS n, min(timestamp) AS mn, max(timestamp) AS mx "
        f"FROM {table} WHERE exchange=$1 AND symbol=$2",
        exchange, symbol,
    )
    return {"n": row["n"], "min_ts": row["mn"], "max_ts": row["mx"]}

# ── Main ────────────────────────────────────────────────────────────────────────

async def run(exchange: str, symbol: str, verify_only: bool) -> None:
    print(f"\n=== Parquet → backtest-postgres ({DB_HOST}:{DB_PORT}/{DB_NAME}) ===")
    print(f"  Exchange: {exchange}  Symbol: {symbol}")

    conn = await connect()
    print("  DB connection OK.")

    if verify_only:
        print("\n[VERIFY-ONLY MODE]")
        for tbl in ("ohlcv_1m", "ohlcv_4h"):
            v = await verify(conn, tbl, exchange, symbol)
            ts_min = v["min_ts"] // 1000 if v["min_ts"] else None
            ts_max = v["max_ts"] // 1000 if v["max_ts"] else None
            print(f"  {tbl}: {v['n']:,} rows  min_ts={ts_min}s  max_ts={ts_max}s")
        await conn.close()
        return

    # ── Load Parquet ──────────────────────────────────────────────────────
    print(f"\n[1/4] Loading 1m Parquet from {OHLCV_1M_DIR}")
    df_1m = load_1m_parquet(OHLCV_1M_DIR)

    # ── Resample 4h ──────────────────────────────────────────────────────
    print("\n[2/4] Resampling 1m → 4h")
    df_4h = resample_to_4h(df_1m)

    # ── Insert ────────────────────────────────────────────────────────────
    print("\n[3/4] Inserting into backtest-postgres")
    n1m = await insert_table(conn, "ohlcv_1m", df_1m, exchange, symbol)
    n4h = await insert_table(conn, "ohlcv_4h", df_4h, exchange, symbol)

    # ── Verify ────────────────────────────────────────────────────────────
    print("\n[4/4] Verification")
    v1m = await verify(conn, "ohlcv_1m", exchange, symbol)
    v4h = await verify(conn, "ohlcv_4h", exchange, symbol)

    parquet_rows = len(df_1m)
    ok_1m = v1m["n"] == parquet_rows
    print(f"  ohlcv_1m: DB={v1m['n']:,}  Parquet={parquet_rows:,}  {'✓ MATCH' if ok_1m else '✗ MISMATCH'}")
    print(f"  ohlcv_4h: DB={v4h['n']:,}  resampled={len(df_4h):,}")

    await conn.close()

    if ok_1m:
        print("\n=== VERIFICATION PASSED ===")
        print("Parquet 파일 삭제 준비가 됐습니다. 아래 명령을 직접 실행하세요:\n")
        print(f"  rm -rf {OHLCV_1M_DIR}")
        print("\n※ 삭제 전 백업이 필요하다면 먼저 tar 아카이브 후 삭제하세요.")
    else:
        print("\n=== VERIFICATION FAILED — 파일 삭제하지 마세요 ===")
        sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Parquet OHLCV → backtest-postgres")
    ap.add_argument("--exchange", default="Bybit Perpetual")
    ap.add_argument("--symbol",   default="BTCUSDT")
    ap.add_argument("--verify-only", action="store_true",
                    help="DB 현황만 출력하고 종료")
    args = ap.parse_args()
    asyncio.run(run(args.exchange, args.symbol, args.verify_only))


if __name__ == "__main__":
    main()
