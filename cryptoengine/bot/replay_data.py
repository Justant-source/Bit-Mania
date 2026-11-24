"""Replay-backtest input data in Postgres: Binance SPOT BTCUSDT 4h candles.

Tables (also created by bot.db.ensure_schema; DDL is idempotent):
  replay_ohlcv_4h          bar open time (ms) -> OHLCV, Binance spot
  replay_reference_trades  the frozen #7908 reference trade list

CLI (env DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD):
  python -m bot.replay_data import-csv <path>     upsert a candle CSV
  python -m bot.replay_data import-trades <path>  replace the reference trades
  python -m bot.replay_data sync                  fetch missing closed bars from Binance
  python -m bot.replay_data export-csv <path>     dump the table as CSV
  python -m bot.replay_data status
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
import time

import aiohttp
import structlog

log = structlog.get_logger(__name__)

KLINES_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "4h"
BAR_MS = 4 * 3600 * 1000
PAGE = 1000
FIRST_TS = 1502942400000  # 2017-08-17 04:00 UTC, first Binance BTCUSDT 4h bar

DDL = """
CREATE TABLE IF NOT EXISTS replay_ohlcv_4h (
  ts BIGINT PRIMARY KEY,
  open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS replay_reference_trades (
  id SERIAL PRIMARY KEY, opened_at BIGINT NOT NULL, closed_at BIGINT NOT NULL, side TEXT NOT NULL,
  entry_price DOUBLE PRECISION, exit_price DOUBLE PRECISION, qty DOUBLE PRECISION,
  pnl DOUBLE PRECISION, fee DOUBLE PRECISION);
"""

UPSERT = """
INSERT INTO replay_ohlcv_4h (ts, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (ts) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high,
  low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume
"""


async def ensure_tables(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(DDL)


async def _fetch_page(session: aiohttp.ClientSession, start_ms: int) -> list:
    params = {"symbol": SYMBOL, "interval": INTERVAL, "startTime": start_ms, "limit": PAGE}
    async with session.get(KLINES_URL, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()


async def sync(db, now_ms: int | None = None, session: aiohttp.ClientSession | None = None) -> int:
    """Fetch Binance spot 4h bars after the last stored ts and upsert closed ones.

    Returns the number of bars upserted. Never raises: failures are logged.
    """
    try:
        pool = db.pool
        await ensure_tables(pool)
        async with pool.acquire() as conn:
            last = await conn.fetchval("SELECT MAX(ts) FROM replay_ohlcv_4h")
        start = FIRST_TS if last is None else int(last) + BAR_MS
        now = int(time.time() * 1000) if now_ms is None else now_ms
        own = session is None
        if own:
            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        total = 0
        try:
            while start <= now:
                rows = await _fetch_page(session, start)
                if not rows:
                    break
                batch = []
                for k in rows:
                    close_time = int(k[6])
                    if close_time >= now:  # still-open bar: never store
                        continue
                    batch.append((int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                                  float(k[4]), float(k[5])))
                if batch:
                    async with pool.acquire() as conn:
                        await conn.executemany(UPSERT, batch)
                    total += len(batch)
                last_open = int(rows[-1][0])
                if len(rows) < PAGE or last_open + BAR_MS <= start:
                    break
                start = last_open + BAR_MS
        finally:
            if own:
                await session.close()
        log.info("replay_data_synced", upserted=total)
        return total
    except Exception as exc:  # never break trading
        log.warning("replay_data_sync_failed", error=str(exc))
        return 0


async def import_csv(pool, path: str) -> int:
    await ensure_tables(pool)
    with open(path, newline="") as f:
        rows = [(int(r["timestamp"]), float(r["open"]), float(r["high"]), float(r["low"]),
                 float(r["close"]), float(r["volume"])) for r in csv.DictReader(f)]
    async with pool.acquire() as conn:
        await conn.executemany(UPSERT, rows)
    return len(rows)


async def import_trades(pool, path: str) -> int:
    """Replace the reference trade list with the CSV contents."""
    await ensure_tables(pool)
    with open(path, newline="") as f:
        rows = [(int(float(r["opened_at"])), int(float(r["closed_at"])), r["side"],
                 float(r["entry_price"]), float(r["exit_price"]), float(r["qty"]),
                 float(r["pnl"]), float(r["fee"])) for r in csv.DictReader(f)]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE replay_reference_trades RESTART IDENTITY")
            await conn.executemany(
                "INSERT INTO replay_reference_trades (opened_at, closed_at, side, entry_price,"
                " exit_price, qty, pnl, fee) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", rows)
    return len(rows)


async def export_csv(pool, path: str) -> int:
    await ensure_tables(pool)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ts, open, high, low, close, volume FROM replay_ohlcv_4h ORDER BY ts")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for r in rows:
            w.writerow([r["ts"], r["open"], r["high"], r["low"], r["close"], r["volume"]])
    return len(rows)


async def status(pool) -> str:
    await ensure_tables(pool)
    async with pool.acquire() as conn:
        n, lo, hi = (await conn.fetchrow(
            "SELECT COUNT(*), MIN(ts), MAX(ts) FROM replay_ohlcv_4h")).values()
        t = await conn.fetchval("SELECT COUNT(*) FROM replay_reference_trades")
    from datetime import datetime, timezone
    fmt = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M") if ms else "-"
    return f"replay_ohlcv_4h: {n} bars {fmt(lo)} .. {fmt(hi)} UTC; replay_reference_trades: {t} rows"


class _PoolHolder:
    def __init__(self, pool):
        self.pool = pool


async def _connect():
    import asyncpg
    return await asyncpg.create_pool(
        host=os.environ.get("DB_HOST", "postgres"), port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "cryptoengine"), user=os.environ.get("DB_USER", "cryptoengine"),
        password=os.environ.get("DB_PASSWORD", ""), min_size=1, max_size=2)


async def _main(argv: list[str]) -> int:
    usage = "usage: python -m bot.replay_data import-csv <path> | import-trades <path> | sync | export-csv <path> | status"
    if not argv or argv[0] not in {"import-csv", "import-trades", "sync", "export-csv", "status"}:
        print(usage)
        return 2
    cmd = argv[0]
    if cmd in {"import-csv", "import-trades", "export-csv"} and len(argv) < 2:
        print(usage)
        return 2
    pool = await _connect()
    try:
        if cmd == "import-csv":
            print(f"imported {await import_csv(pool, argv[1])} bars")
        elif cmd == "import-trades":
            print(f"imported {await import_trades(pool, argv[1])} reference trades")
        elif cmd == "export-csv":
            print(f"exported {await export_csv(pool, argv[1])} bars")
        elif cmd == "sync":
            print(f"synced {await sync(_PoolHolder(pool))} bars")
        print(await status(pool))
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main(sys.argv[1:])))
