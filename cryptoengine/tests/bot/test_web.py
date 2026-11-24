"""Tests for bot/web.py against a throwaway Postgres (127.0.0.1:55432)."""

from __future__ import annotations

import json
import socket
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import asyncpg
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from bot import web

PG = dict(host="127.0.0.1", port=55432, user="postgres", password="test")
TOKEN = "s3cret-token"
UTC = timezone.utc

DDL = """
CREATE TABLE bot_state (key TEXT PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE portfolio_snapshots (id BIGSERIAL PRIMARY KEY, snapshot_at TIMESTAMPTZ NOT NULL, total_equity NUMERIC,
  daily_drawdown DOUBLE PRECISION, weekly_drawdown DOUBLE PRECISION, monthly_drawdown DOUBLE PRECISION);
CREATE TABLE positions (id BIGSERIAL PRIMARY KEY, side TEXT, size NUMERIC, entry_price NUMERIC,
  opened_at TIMESTAMPTZ, closed_at TIMESTAMPTZ, close_reason TEXT);
CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, side TEXT, order_type TEXT, quantity DOUBLE PRECISION,
  price DOUBLE PRECISION, status TEXT, filled_qty DOUBLE PRECISION, filled_price DOUBLE PRECISION,
  fee DOUBLE PRECISION, reduce_only BOOLEAN, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
CREATE TABLE supertrend_signals (id BIGSERIAL PRIMARY KEY, bar_ts TIMESTAMPTZ, st_dir SMALLINT, price DOUBLE PRECISION,
  atr_14 DOUBLE PRECISION, entry_ok BOOLEAN, exit_signal BOOLEAN, exit_reason TEXT, expected_action TEXT, had_position BOOLEAN);
CREATE TABLE ohlcv_history (id BIGSERIAL PRIMARY KEY, exchange TEXT, symbol TEXT, timeframe TEXT,
  open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC, timestamp TIMESTAMPTZ);
"""
TABLES = ["bot_state", "portfolio_snapshots", "positions", "orders", "supertrend_signals", "ohlcv_history"]


@pytest_asyncio.fixture
async def db():
    admin = await asyncpg.connect(database="postgres", **PG)
    if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname='test_web'"):
        await admin.execute("CREATE DATABASE test_web")
    await admin.close()
    pool = await asyncpg.create_pool(database="test_web", min_size=1, max_size=3, **PG)
    async with pool.acquire() as c:
        await c.execute("DROP TABLE IF EXISTS " + ",".join(TABLES))
        await c.execute(DDL)
    yield SimpleNamespace(pool=pool)
    await pool.close()


async def _client(db, token=TOKEN):
    c = TestClient(TestServer(web._make_app(db, token)))
    await c.start_server()
    return c


def _order(side, qty, price, t, reduce_only=False, fee=0.1, status="filled"):
    return (side, "limit", qty, price, status, qty, price, fee, reduce_only, t, t + timedelta(seconds=5))


async def _seed(pool):
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    base = now - timedelta(days=20)
    orders = [
        # trade 1: single buy, exit in two fill rows
        _order("buy", 0.006, 80000.0, base),
        _order("sell", 0.004, 82000.0, base + timedelta(hours=16), True, 0.05),
        _order("sell", 0.002, 83000.0, base + timedelta(hours=16, minutes=1), True, 0.05),
        # rejected order must be ignored
        ("buy", "limit", 0.006, 70000.0, "rejected", 0.0, None, 0.0, False, base + timedelta(days=2), base + timedelta(days=2)),
        # trade 2: two buy fills, one exit, a loss
        _order("buy", 0.003, 90000.0, base + timedelta(days=3)),
        _order("buy", 0.003, 90200.0, base + timedelta(days=3, minutes=1)),
        _order("sell", 0.006, 88000.0, base + timedelta(days=4), True, 0.1),
        # open trade (no exit yet): must not appear as closed trade
        _order("buy", 0.006, 85000.0, base + timedelta(days=6)),
    ]
    await pool.executemany(
        """INSERT INTO orders (side,order_type,quantity,price,status,filled_qty,filled_price,fee,reduce_only,created_at,updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)""", orders)
    await pool.execute(
        "INSERT INTO supertrend_signals (bar_ts,st_dir,price,atr_14,entry_ok,exit_signal,exit_reason,expected_action,had_position) "
        "VALUES ($1,-1,82000,1500,false,true,'ema_cross','exit',true)",
        base + timedelta(hours=12))
    for i in range(70):
        await pool.execute(
            "INSERT INTO supertrend_signals (bar_ts,st_dir,price,atr_14,entry_ok,exit_signal,expected_action,had_position) "
            "VALUES ($1,1,$2,1500,$3,false,'hold',false)", now - timedelta(hours=4 * i), 85000.0 + i, i == 5)
    for i in range(250):
        p = 80000.0 + i * 10
        await pool.execute(
            "INSERT INTO ohlcv_history (exchange,symbol,timeframe,open,high,low,close,volume,timestamp) "
            "VALUES ('bybit','BTCUSDT','4h',$1,$2,$3,$4,10,$5)", p, p + 50, p - 50, p + 20,
            now - timedelta(hours=4 * (250 - i)))
    # a different timeframe/symbol that must not leak in
    await pool.execute(
        "INSERT INTO ohlcv_history (exchange,symbol,timeframe,open,high,low,close,volume,timestamp) "
        "VALUES ('bybit','BTCUSDT','1h',1,1,1,1,1,$1)", now)
    # several snapshots within the same hour + one per day
    for d in range(30):
        t = now - timedelta(days=d, minutes=1)
        await pool.execute("INSERT INTO portfolio_snapshots (snapshot_at,total_equity) VALUES ($1,$2)", t, 150 + d)
        await pool.execute("INSERT INTO portfolio_snapshots (snapshot_at,total_equity) VALUES ($1,$2)",
                           t - timedelta(minutes=10), 1)  # same hour as the above only if minute allows
    await pool.execute("INSERT INTO portfolio_snapshots (snapshot_at,total_equity) VALUES ($1,170.5)", datetime.now(UTC))
    await pool.execute("INSERT INTO positions (side,size,entry_price,opened_at) VALUES ('long',0.006,85000,$1)",
                       base + timedelta(days=6))
    # duplicated historical closed rows: must not matter
    await pool.executemany(
        "INSERT INTO positions (side,size,entry_price,opened_at,closed_at,close_reason) VALUES ('long',0.006,80000,$1,$2,'exit')",
        [(base, base + timedelta(hours=16))] * 2)
    now_iso = datetime.now(UTC).isoformat()
    await pool.execute("INSERT INTO bot_state (key,value) VALUES ('heartbeat',$1::jsonb)", json.dumps({"at": now_iso}))
    await pool.execute("INSERT INTO bot_state (key,value) VALUES ('manual_halt',$1::jsonb)",
                       json.dumps({"active": True, "reason": "점검", "at": now_iso}))
    await pool.execute("INSERT INTO bot_state (key,value) VALUES ('kill_switch',$1::jsonb)",
                       json.dumps({"level": "PORTFOLIO", "reason": "dd", "triggered_at": now_iso, "alert_only": True}))


# ------------------------------------------------------------------ auth

@pytest.mark.asyncio
async def test_auth_flow(db):
    c = await _client(db)
    try:
        assert (await c.get("/healthz")).status == 200
        assert (await (await c.get("/healthz")).text()) == "ok"
        assert (await c.get("/")).status == 403
        assert (await c.get("/api/summary")).status == 403
        assert (await c.get("/api/summary?t=wrong")).status == 403
        r = await c.get("/?t=" + TOKEN)
        assert r.status == 200
        assert "BTC" in await r.text()
        sc = r.headers["Set-Cookie"]
        assert "HttpOnly" in sc and "SameSite=Lax" in sc and "Max-Age=2592000" in sc
        # cookie jar now carries the token: no query needed
        assert (await c.get("/api/summary")).status == 200
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_forged_cookie_rejected(db):
    c = await _client(db)
    try:
        r = await c.get("/api/summary", cookies={web.COOKIE: "nope"})
        assert r.status == 403
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_empty_token_serves_nothing(db):
    c = await _client(db, token="")
    try:
        assert (await c.get("/healthz")).status == 200
        for path in ("/", "/api/summary", "/?t=", "/api/summary?t="):
            assert (await c.get(path)).status == 403
        assert (await c.get("/", cookies={web.COOKIE: ""})).status == 403
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_start_web_and_stop(db):
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    srv = await web.start_web(db, port=port, token=TOKEN)
    try:
        import aiohttp
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"http://127.0.0.1:{port}/healthz") as r:
                assert r.status == 200
            async with sess.get(f"http://127.0.0.1:{port}/api/summary?t={TOKEN}") as r:
                assert r.status == 200
    finally:
        await srv.stop()


# --------------------------------------------------------------- summary

@pytest.mark.asyncio
async def test_summary_seeded(db):
    await _seed(db.pool)
    c = await _client(db)
    try:
        r = await c.get("/api/summary?t=" + TOKEN)
        assert r.status == 200
        d = await r.json()
    finally:
        await c.close()

    assert set(d) == {"generated_at", "status", "equity_curve", "candles", "trades", "trade_totals", "signals"}
    st = d["status"]
    assert st["equity"] == pytest.approx(170.5)
    assert st["position"]["size"] == pytest.approx(0.006)
    assert st["position"]["entry_price"] == pytest.approx(85000)
    assert st["trading"]["entries_blocked"] is True and "점검" in st["trading"]["reason"]
    assert st["kill_switch"]["alert_only"] is True
    assert 0 <= st["heartbeat_age_sec"] < 30
    assert st["last_bar"]["signal"] in ("진입", "대기", "청산")
    assert len(d["signals"]) == 60
    assert len(d["candles"]) == 200
    ts = [x["t"] for x in d["candles"]]
    assert ts == sorted(ts)
    # hourly downsample: no two points in the same hour
    hours = [x["t"][:13] for x in d["equity_curve"]]
    assert len(hours) == len(set(hours))
    assert len(d["equity_curve"]) >= 20

    assert len(d["trades"]) == 2  # open trade excluded, rejected ignored
    newest, oldest = d["trades"]
    assert newest["opened_at"] > oldest["opened_at"]
    # trade 2: entry avg 90100, exit 88000, qty 0.006, fee 0.1+0.1+0.1
    assert newest["entry_price"] == pytest.approx(90100.0)
    assert newest["pnl"] == pytest.approx((88000 - 90100) * 0.006 - 0.3)
    assert newest["pnl_pct"] == pytest.approx(newest["pnl"] / (90100 * 0.006) * 100)
    # trade 1: exit avg = (0.004*82000+0.002*83000)/0.006, fees 0.1+0.05+0.05
    exit_avg = (0.004 * 82000 + 0.002 * 83000) / 0.006
    assert oldest["exit_price"] == pytest.approx(exit_avg)
    assert oldest["pnl"] == pytest.approx((exit_avg - 80000) * 0.006 - 0.2)
    assert oldest["exit_reason"] == "ema_cross"
    assert newest["exit_reason"] == "기타(스탑/수동)"
    tot = d["trade_totals"]
    assert tot["count"] == 2 and tot["wins"] == 1 and tot["losses"] == 1
    assert tot["pnl"] == pytest.approx(newest["pnl"] + oldest["pnl"])
    assert tot["fees"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_summary_empty_database(db):
    c = await _client(db)
    try:
        r = await c.get("/api/summary?t=" + TOKEN)
        assert r.status == 200
        d = await r.json()
    finally:
        await c.close()
    assert d["status"]["equity"] is None
    assert d["status"]["position"] is None
    assert d["status"]["heartbeat_age_sec"] is None
    assert d["status"]["last_bar"] is None
    assert d["status"]["trading"]["entries_blocked"] is False
    assert d["trades"] == [] and d["signals"] == [] and d["candles"] == [] and d["equity_curve"] == []
    assert d["trade_totals"]["count"] == 0


@pytest.mark.asyncio
async def test_summary_survives_missing_tables(db):
    # bot_state does not exist in production before the bot's first start
    await db.pool.execute("DROP TABLE bot_state")
    c = await _client(db)
    try:
        r = await c.get("/api/summary?t=" + TOKEN)
        assert r.status == 200
        assert (await r.json())["status"]["trading"]["entries_blocked"] is False
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_block_until_in_past_is_not_blocked(db):
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    fut = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    ins = "INSERT INTO bot_state (key,value) VALUES ('portfolio_block_until',$1::jsonb) ON CONFLICT (key) DO UPDATE SET value=$1::jsonb"
    await db.pool.execute(ins, json.dumps({"until": past, "reason": "x"}))
    assert (await web.build_summary(db.pool))["status"]["trading"]["entries_blocked"] is False
    await db.pool.execute(ins, json.dumps({"until": fut, "reason": "손실"}))
    t = (await web.build_summary(db.pool))["status"]["trading"]
    assert t["entries_blocked"] is True and "손실" in t["reason"]


# --------------------------------------------------------------- pairing

def _row(side, qty, price, t, ro=False, fee=0.0):
    return {"side": side, "filled_qty": qty, "filled_price": price, "fee": fee, "reduce_only": ro,
            "created_at": t, "updated_at": t}


def test_pairing_pure():
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("buy", 0.01, 100.0, t0, fee=0.1),
        _row("sell", 0.004, 110.0, t0 + timedelta(hours=1), True, 0.1),
        _row("sell", 0.006, 120.0, t0 + timedelta(hours=2), True, 0.1),
        _row("sell", 0.5, 120.0, t0 + timedelta(hours=3), True),  # stray reduce-only while flat
        _row("buy", 0.01, 100.0, t0 + timedelta(days=1)),
    ]
    trades = web.pair_trades(rows)
    assert len(trades) == 1
    t = trades[0]
    assert t["entry_price"] == pytest.approx(100.0)
    assert t["exit_price"] == pytest.approx((0.004 * 110 + 0.006 * 120) / 0.01)
    assert t["pnl"] == pytest.approx((t["exit_price"] - 100.0) * 0.01 - 0.3)


def test_pairing_dust_remainder_closes_and_next_trade_starts():
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("buy", 0.0061, 100.0, t0),
        _row("sell", 0.006, 101.0, t0 + timedelta(hours=1), True),  # 0.0001 dust left
        _row("buy", 0.006, 102.0, t0 + timedelta(days=1)),
        _row("sell", 0.006, 100.0, t0 + timedelta(days=2), True),
    ]
    trades = web.pair_trades(rows)
    assert len(trades) == 2
    assert trades[1]["pnl"] == pytest.approx(-0.012)


def test_pairing_exchange_side_close_is_a_trade_with_unknown_pnl():
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row("buy", 0.01, 100.0, t0, fee=0.1),
        _row("buy", 0.01, 120.0, t0 + timedelta(days=2), fee=0.1),
        _row("sell", 0.01, 200.0, t0 + timedelta(days=3), True, 0.1),
    ]
    positions = [
        {"opened_at": t0 + timedelta(minutes=1), "closed_at": t0 + timedelta(days=1), "close_reason": "exchange_side"},
        {"opened_at": t0 + timedelta(days=2, minutes=1), "closed_at": t0 + timedelta(days=3, minutes=1),
         "close_reason": "exit"},
    ]
    trades = web.pair_trades(rows, None, positions)
    assert len(trades) == 2
    a, b = trades
    assert a["entry_price"] == pytest.approx(100.0) and a["exit_price"] is None
    assert a["pnl"] is None and a["pnl_pct"] is None and "거래소" in a["exit_reason"]
    assert b["entry_price"] == pytest.approx(120.0) and b["pnl"] == pytest.approx(80 * 0.01 - 0.2)
    tot = web.trade_totals(trades)
    assert (tot["count"], tot["wins"], tot["losses"], tot["unknown"]) == (2, 1, 0, 1)
    assert tot["pnl"] == pytest.approx(b["pnl"])
    assert web.pair_trades(rows) != trades            # without the boundary the two buys merge


def test_pairing_ignores_exit_rows_in_positions():
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row("buy", 0.01, 100.0, t0), _row("sell", 0.01, 110.0, t0 + timedelta(hours=1), True)]
    pos = [{"opened_at": t0, "closed_at": t0 + timedelta(hours=1), "close_reason": "exit"}]
    assert web.pair_trades(rows, None, pos) == web.pair_trades(rows)
