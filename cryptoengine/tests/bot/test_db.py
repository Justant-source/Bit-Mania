"""Database: pool, ensure_schema is idempotent and a no-op against a live-schema database."""

from __future__ import annotations

import re
from pathlib import Path

import asyncpg
import pytest

from core_helpers import PG, ensure_database, db  # noqa: F401
from bot.db import SCHEMA_SQL, Database

pytestmark = pytest.mark.asyncio
ROOT = Path(__file__).resolve().parents[2]
LEGACY_TABLES = ("orders", "positions", "supertrend_signals", "portfolio_snapshots", "ohlcv_history")
NEW_TABLES = ("bot_state", "replay_ohlcv_4h", "replay_reference_trades")

# DDL the old services created on their own (copied from services/execution/main.py and
# services/strategies/supertrend/strategy.py); the rest comes from tests/fixtures/legacy_schema/ (the pre-merge production schema).
OLD_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, order_id TEXT,
    exchange TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL, price DOUBLE PRECISION, status TEXT NOT NULL DEFAULT 'pending',
    filled_qty DOUBLE PRECISION DEFAULT 0, filled_price DOUBLE PRECISION, fee DOUBLE PRECISION DEFAULT 0,
    fee_currency TEXT DEFAULT 'USDT', strategy_id TEXT, post_only BOOLEAN DEFAULT TRUE,
    reduce_only BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delay_reason TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS original_signal_ts TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS original_request_id TEXT;
"""
OLD_SIGNALS_ALTER = "ALTER TABLE supertrend_signals ADD COLUMN IF NOT EXISTS st_line DOUBLE PRECISION;"


async def columns(conn, table):
    rows = await conn.fetch(
        "SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, "
        "numeric_precision, numeric_scale FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position", table)
    return [tuple(r) for r in rows]


async def scratch(name):
    await ensure_database(name)
    conn = await asyncpg.connect(database=name, **PG)
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    return conn


async def test_ensure_schema_creates_everything_and_is_idempotent(db):
    await db.ensure_schema()
    await db.ensure_schema()
    async with db.pool.acquire() as c:
        names = {r["tablename"] for r in await c.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")}
    assert set(LEGACY_TABLES + NEW_TABLES) <= names


async def test_ddl_has_no_destructive_statements():
    body = re.sub(r"--.*", "", SCHEMA_SQL).upper()
    for word in ("ALTER ", "DROP ", "TRUNCATE", "DELETE ", "UPDATE "):
        assert word not in body
    for stmt in re.findall(r"CREATE (?:UNIQUE )?(TABLE|INDEX)[^;]*", body):
        pass
    creates = re.findall(r"CREATE (?:UNIQUE )?(?:TABLE|INDEX) (?!IF NOT EXISTS)", body)
    assert creates == []  # every CREATE is IF NOT EXISTS


async def test_matches_live_schema_and_is_a_noop_on_it():
    """Build the live schema the way the old services did, then ensure_schema must change nothing."""
    legacy = await scratch("test_core_legacy")
    try:
        await legacy.execute((ROOT / "tests/fixtures/legacy_schema/init_schema.sql").read_text())
        await legacy.execute(OLD_ORDERS)
        # supertrend_signals: migration 016 + the st_line ALTER the strategy ran at start
        await legacy.execute((ROOT / "tests/fixtures/legacy_schema/016_supertrend_signals.sql").read_text())
        await legacy.execute(OLD_SIGNALS_ALTER)
        before = {t: await columns(legacy, t) for t in LEGACY_TABLES}
        idx_before = {r["indexname"] for r in await legacy.fetch("SELECT indexname FROM pg_indexes WHERE schemaname='public'")}
        await legacy.execute("INSERT INTO supertrend_signals (bar_ts, st_dir, fast_ema, slow_ema, dir_ema, price, "
                             "atr_14, allocated_capital, had_position, entry_ok, exit_signal, expected_action) "
                             "VALUES (now(),1,1,1,1,1,1,1,false,false,false,'hold')")
        await legacy.close()

        d = Database(host=PG["host"], port=PG["port"], name="test_core_legacy", user=PG["user"], password=PG["password"])
        await d.connect()
        await d.ensure_schema()
        await d.close()

        legacy = await asyncpg.connect(database="test_core_legacy", **PG)
        after = {t: await columns(legacy, t) for t in LEGACY_TABLES}
        assert after == before  # existing tables untouched
        assert await legacy.fetchval("SELECT COUNT(*) FROM supertrend_signals") == 1  # history kept
        idx_after = {r["indexname"] for r in await legacy.fetch("SELECT indexname FROM pg_indexes WHERE schemaname='public'")}
        assert idx_before <= idx_after
        assert set(NEW_TABLES) <= {r["tablename"] for r in await legacy.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")}
    finally:
        await legacy.close()


async def test_fresh_schema_columns_equal_live_schema():
    """Columns created from scratch equal the live ones (orders: live + the three extension columns)."""
    legacy = await scratch("test_core_legacy2")
    try:
        await legacy.execute((ROOT / "tests/fixtures/legacy_schema/init_schema.sql").read_text())
        await legacy.execute(OLD_ORDERS)
        await legacy.execute((ROOT / "tests/fixtures/legacy_schema/016_supertrend_signals.sql").read_text())
        await legacy.execute(OLD_SIGNALS_ALTER)
        live = {t: await columns(legacy, t) for t in LEGACY_TABLES}
    finally:
        await legacy.close()
    fresh = await scratch("test_core_fresh")
    try:
        await fresh.close()
        d = Database(host=PG["host"], port=PG["port"], name="test_core_fresh", user=PG["user"], password=PG["password"])
        await d.connect()
        await d.ensure_schema()
        async with d.pool.acquire() as c:
            mine = {t: await columns(c, t) for t in LEGACY_TABLES}
        await d.close()
    finally:
        pass
    for t in LEGACY_TABLES:
        # ordinal order can differ for st_line (live added it by ALTER at the end)
        assert sorted(mine[t]) == sorted(live[t]), t


async def test_unique_constraints_needed_by_upserts(db):
    async with db.pool.acquire() as c:
        for _ in range(2):
            await c.execute("INSERT INTO supertrend_signals (bar_ts, st_dir, fast_ema, slow_ema, dir_ema, price, atr_14, "
                            "allocated_capital, had_position, entry_ok, exit_signal, expected_action) "
                            "VALUES ('2026-01-01',1,1,1,1,1,1,1,false,false,false,'hold') ON CONFLICT (bar_ts) DO NOTHING")
            await c.execute("INSERT INTO ohlcv_history (exchange,symbol,timeframe,timestamp,open,high,low,close,volume) "
                            "VALUES ('bybit','BTCUSDT','4h','2026-01-01',1,1,1,1,1) "
                            "ON CONFLICT (exchange,symbol,timeframe,timestamp) DO NOTHING")
        assert await c.fetchval("SELECT COUNT(*) FROM supertrend_signals") == 1
        assert await c.fetchval("SELECT COUNT(*) FROM ohlcv_history") == 1


async def test_pool_property_requires_connect(monkeypatch):
    monkeypatch.setenv("DB_HOST", "h")
    monkeypatch.setenv("DB_PORT", "1234")
    monkeypatch.setenv("DB_NAME", "n")
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "p")
    d = Database()
    assert (d.host, d.port, d.name, d.user, d.password) == ("h", 1234, "n", "u", "p")
    with pytest.raises(RuntimeError):
        d.pool
    monkeypatch.delenv("DB_HOST")
    assert Database().host == "postgres"
