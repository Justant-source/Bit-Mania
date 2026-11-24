"""Tests for bot/web_supertrend.py: the restored /supertrend dashboard and its JSON API.

The JSON shapes were verified byte-for-byte against the old Node server (see docs/operations.md);
here we pin the node-postgres conventions (NUMERIC as string, ms ISO timestamps, JS numbers).
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import asyncpg
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from bot import web
from bot import web_supertrend as ws

PG = dict(host="127.0.0.1", port=55432, user="postgres", password="test")
UTC = timezone.utc
H4 = timedelta(hours=4)
API = "/api/internal/supertrend/"

DDL = """
DROP TABLE IF EXISTS supertrend_signals, orders, portfolio_snapshots, ohlcv_history, bot_state, positions;
CREATE TABLE supertrend_signals (id BIGSERIAL PRIMARY KEY, bar_ts TIMESTAMPTZ NOT NULL, computed_at TIMESTAMPTZ,
  st_dir SMALLINT, fast_ema DOUBLE PRECISION, slow_ema DOUBLE PRECISION, dir_ema DOUBLE PRECISION,
  price DOUBLE PRECISION, atr_14 DOUBLE PRECISION, allocated_capital DOUBLE PRECISION, had_position BOOLEAN,
  entry_ok BOOLEAN, exit_signal BOOLEAN, exit_reason VARCHAR(20), expected_action VARCHAR(10),
  expected_qty DOUBLE PRECISION, expected_stop_loss DOUBLE PRECISION, st_line DOUBLE PRECISION,
  actual_exit_price DOUBLE PRECISION, actual_exit_at TIMESTAMPTZ, delay_note TEXT);
CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, request_id TEXT, order_id TEXT, side TEXT,
  filled_qty DOUBLE PRECISION, filled_price DOUBLE PRECISION, fee DOUBLE PRECISION, strategy_id TEXT,
  status TEXT, reduce_only BOOLEAN, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, original_signal_ts TIMESTAMPTZ);
CREATE TABLE portfolio_snapshots (id BIGSERIAL PRIMARY KEY, total_equity NUMERIC(20,2), snapshot_at TIMESTAMPTZ);
CREATE TABLE ohlcv_history (id BIGSERIAL PRIMARY KEY, exchange VARCHAR(20), symbol VARCHAR(20), timeframe VARCHAR(5),
  open NUMERIC(20,2), high NUMERIC(20,2), low NUMERIC(20,2), close NUMERIC(20,2), volume NUMERIC(30,8),
  timestamp TIMESTAMPTZ);
"""

BAR0 = datetime(2026, 5, 20, 0, 0, tzinfo=UTC)
MICRO = timedelta(microseconds=123987)


@pytest_asyncio.fixture
async def db():
    admin = await asyncpg.connect(database="postgres", **PG)
    if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname='test_web_st'"):
        await admin.execute("CREATE DATABASE test_web_st")
    await admin.close()
    pool = await asyncpg.create_pool(database="test_web_st", min_size=1, max_size=3, **PG)
    async with pool.acquire() as c:
        await c.execute(DDL)
    yield SimpleNamespace(pool=pool)
    await pool.close()


async def _client(db, providers=None):
    c = TestClient(TestServer(web._make_app(db, "s3cret", providers)))
    await c.start_server()
    return c


async def _sig(pool, i, action, price, qty=None, **extra):
    t = BAR0 + H4 * i
    await pool.execute(
        """INSERT INTO supertrend_signals (bar_ts,computed_at,st_dir,fast_ema,slow_ema,dir_ema,price,atr_14,
             allocated_capital,had_position,entry_ok,exit_signal,exit_reason,expected_action,expected_qty,
             expected_stop_loss,st_line,actual_exit_price,actual_exit_at,delay_note)
           VALUES ($1,$2,1,$3,$3,$3,$4,1234.5678,148.20679382,false,$5,$6,$7,$8,$9,$10,$4,$11,$12,$13)""",
        t, t + H4 + MICRO, price * 0.99 + 1 / 3, price, action == "enter", action == "exit",
        "atr_dist" if action == "exit" else None, action, qty,
        price * 0.95 if action == "enter" else None,
        extra.get("ax"), extra.get("axt"), extra.get("note"))


async def _order(pool, i, side, reduce, qty, price, fee, *, secs=5, status="filled", strategy="supertrend-01",
                 orig=None):
    t = BAR0 + H4 * i + MICRO
    await pool.execute(
        """INSERT INTO orders (request_id,order_id,side,filled_qty,filled_price,fee,strategy_id,status,reduce_only,
             created_at,updated_at,original_signal_ts) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
        f"r{i}{side}{qty}", f"o{i}{side}{qty}", side, qty, price, fee, strategy, status, reduce,
        t, t + timedelta(seconds=secs), orig)


async def _seed(pool):
    # bar 0: enter (fill 4h after bar close + 5s), bar 2: exit (fill with null fee), bar 4: enter missed,
    # bar 6: exit with a delayed execution recorded on the signal row, bar 8: enter at price 0
    await _sig(pool, 0, "enter", 60000.5, 0.0123)
    await _sig(pool, 1, "hold", 60100.0)
    await _sig(pool, 2, "exit", 60400.25)
    await _sig(pool, 4, "enter", 60500.0, 0.01)
    await _sig(pool, 6, "exit", 61000.0, ax=61100.0, axt=BAR0 + H4 * 7 + timedelta(minutes=7) + MICRO,
               note="delayed exit")
    await _sig(pool, 8, "enter", 0.0, 0.5)
    await _order(pool, 1, "buy", False, 0.0123, 60100.5, 0.15)
    await _order(pool, 3, "sell", True, 0.0123, 60800.25, None)
    await _order(pool, 7, "sell", True, 0.0111, 61020.0, 0.0, status="filled_delayed",
                 orig=BAR0 + H4 * 6 + timedelta(minutes=10))
    await _order(pool, 9, "buy", False, 0.0125, 60900.0, 0.1)
    await _order(pool, 9, "buy", False, 0.9, 1.0, 0.0, strategy="other")  # other strategy ignored
    for i in range(6):
        await pool.execute(
            "INSERT INTO ohlcv_history (exchange,symbol,timeframe,open,high,low,close,volume,timestamp) "
            "VALUES ('bybit','BTCUSDT','4h',$1::numeric,$1::numeric+100,$1::numeric-100.555,$1::numeric+50.005,1234.56789012,$2)",
            Decimal("60000") + i, BAR0 + H4 * i)
    await pool.execute(
        "INSERT INTO ohlcv_history (exchange,symbol,timeframe,open,high,low,close,volume,timestamp) "
        "VALUES ('bybit','BTCUSDT','1h',1,1,1,1,1,$1)", BAR0)
    for i in range(4):
        await pool.execute("INSERT INTO portfolio_snapshots (total_equity,snapshot_at) VALUES ($1,$2)",
                           Decimal("148.20") + i, BAR0 + timedelta(minutes=15 * i) + MICRO)


# ------------------------------------------------------------ static files

GOLDEN = {  # sha256 of the files restored from the old dashboard (git HEAD at the time of removal)
    "supertrend.html": "be5d41be4cf60f7e1da388483e3cecbdc0624007d960e9e12ac397b6ae0341df",
    "css/tokens.css": "fcd820f33095db19996f87c90cb6fd898c0f71bdf365eb4a5b9faa95145a44b2",
    "css/components.css": "07afee74a99d8f00ad771ada41b0cfc064504b9c197569fa45b2e5f19bc755c5",
    "css/dashboard.css": "118756a371fa6d2fe980853bad9cc8b4b3012c2fdc933158608499a8a7d734e5",
    "js/supertrend-dashboard.js": "c7d118028e5ebfbfa214c6767ff2f5c5aff9161a9261b1a5682aee4a6ff44ae0",
    "js/theme.js": "4e1dfe75dd2fa4e57f89ada04bc0b619b421139e6a05ac772f9b3cd864ec1376",
}


@pytest.mark.asyncio
async def test_static_files_unchanged_and_served_without_token(db):
    c = await _client(db)
    try:
        for rel, sha in GOLDEN.items():
            assert hashlib.sha256((ws.SUPERTREND_DIR / rel).read_bytes()).hexdigest() == sha, rel
        urls = {"/supertrend": "supertrend.html", "/supertrend/": "supertrend.html",
                "/supertrend.html": "supertrend.html"}
        urls |= {"/" + k: k for k in GOLDEN if k != "supertrend.html"}
        for url, rel in urls.items():
            r = await c.get(url)
            assert r.status == 200, url
            assert hashlib.sha256(await r.read()).hexdigest() == GOLDEN[rel], url
        assert (await c.get("/supertrend")).headers["Content-Type"] == "text/html; charset=UTF-8"
        assert (await c.get("/js/theme.js")).headers["Content-Type"] == "application/javascript; charset=UTF-8"
        assert (await c.get("/css/tokens.css")).headers["Content-Type"] == "text/css; charset=UTF-8"
        assert (await c.get("/js/nope.js")).status == 404
        assert (await c.get("/js/.hidden")).status == 404
        assert (await c.get("/monitor")).status == 403  # not ported: unknown paths still need the token
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_summary_page_stays_token_protected(db):
    c = await _client(db)
    try:
        assert (await c.get("/")).status == 403
        assert (await c.get("/api/summary")).status == 403
    finally:
        await c.close()


# ------------------------------------------------------------ JSON helpers

def test_js_number_matches_javascript():
    cases = {5.0: "5", -0.0: "0", 0.1: "0.1", 1e21: "1e+21", 1e-7: "1e-7", 1.5e-7: "1.5e-7", 123456789.125: "123456789.125",
             0.000001: "0.000001", 1e20: "100000000000000000000", 2 ** 60: "1152921504606847000",
             1.2345e25: "1.2345e+25", -1234.5: "-1234.5", 9007199254740993.0: "9007199254740992",
             -0.000000123: "-1.23e-7", 148.20679382: "148.20679382"}
    for v, s in cases.items():
        assert ws._js_number(float(v)) == s, v
    assert ws._js_number(math.nan) == "null" and ws._js_number(math.inf) == "null"


def test_encoding_conventions():
    dt = datetime(2026, 5, 20, 4, 0, 0, 999999, tzinfo=UTC)
    out = ws._enc({"a": Decimal("1.50"), "z": Decimal("0E-8"), "t": dt, "f": 3.0, "n": None, "b": True,
                   "l": [1, 2.5, "xé"]})
    assert out == '{"a":"1.50","z":"0.00000000","t":"2026-05-20T04:00:00.999Z","f":3,"n":null,"b":true,"l":[1,2.5,"xé"]}'


def test_parse_time_param():
    p = ws.parse_time_param
    assert p(None, 30) == ("days", 30) and p("", 90) == ("days", 90)
    assert p("30", 5) == ("days", 30) and p("7 days", 5) == ("days", 7) and p("30d", 5) == ("days", 30)
    assert p("0", 5) == ("days", 1) and p("99999", 5) == ("days", 365) and p("abc", 7) == ("days", 7)
    assert p("2026-05-18", 30) == ("ts", datetime(2026, 5, 18, tzinfo=UTC))
    assert p("2026-11-01T12:30:00.123456+09:00", 30) == ("ts", datetime(2026, 11, 1, 3, 30, 0, 123000, tzinfo=UTC))
    assert p("2024-02-30", 30) == ("ts", datetime(2024, 3, 1, tzinfo=UTC))  # V8 rolls over
    assert p("2026-13-45", 30) == ("days", 365)  # invalid date -> leading digits "2026" -> clamped
    assert p("2026-11-01 GMT", 30) == ("ts", datetime(2026, 11, 1, tzinfo=UTC))
    with pytest.raises(ws._BadParam):
        p(["1", "2"], 30)


# ---------------------------------------------------------------- the API

@pytest.mark.asyncio
async def test_expected_and_candles_shapes(db):
    await _seed(db.pool)
    c = await _client(db)
    try:
        r = await c.get(API + "expected?from=2026-05-20")
        assert r.status == 200
        assert r.headers["Content-Type"] == "application/json; charset=utf-8"
        assert r.headers["Access-Control-Allow-Origin"] == "*"
        body = await r.json()
        assert body["count"] == 6 == len(body["signals"])
        s = body["signals"][0]
        assert list(s) == ["bar_ts", "computed_at", "st_dir", "st_line", "fast_ema", "slow_ema", "dir_ema", "price",
                           "atr_14", "allocated_capital", "had_position", "entry_ok", "exit_signal", "exit_reason",
                           "expected_action", "expected_qty", "expected_stop_loss"]
        assert s["bar_ts"] == "2026-05-20T00:00:00.000Z"
        assert s["computed_at"] == "2026-05-20T04:00:00.123Z"      # microseconds cut to ms
        assert s["price"] == "60000.50" and s["atr_14"] == "1234.57"  # round(numeric) -> string
        assert s["expected_qty"] == "0.012300" and s["allocated_capital"] == 148.20679382  # float8 -> number
        assert s["st_dir"] == 1 and s["exit_reason"] is None
        raw = await (await c.get(API + "expected?from=2026-05-20")).text()
        assert '"st_dir":1,' in raw and "148.20679382" in raw and " " not in raw.split('"signals"')[0]

        r = await c.get(API + "candles?from=2026-05-20")
        body = await r.json()
        assert body["count"] == 6  # only bybit BTCUSDT 4h
        assert body["candles"][0] == {"ts": "2026-05-20T00:00:00.000Z", "open": "60000.00", "high": "60100.00",
                                     "low": "59899.45", "close": "60050.01", "volume": "1234.5679"}
        # default window is relative to now: nothing from 2026-05 in the last 90 days
        assert (await (await c.get(API + "candles")).json()) == {"candles": [], "count": 0}
        assert (await (await c.get(API + "expected?from=2026-05-20&to=2026-05-20T05:00:00Z")).json())["count"] == 2
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_actual_pairs_round_trips(db):
    await _seed(db.pool)
    c = await _client(db)
    try:
        body = await (await c.get(API + "actual?from=2026-05-01")).json()
        assert body["count"] == 2
        t0, t1 = body["trades"]
        assert t0["entry_order_id"] == "o1buy0.0123" and t0["exit_order_id"] == "o3sell0.0123"
        assert t0["entry_time"] == "2026-05-20T04:00:00.123Z"
        assert t0["entry_price"] == 60100.5 and t0["exit_price"] == 60800.25 and t0["qty"] == 0.0123
        assert t0["total_fees"] == 0.15 and t0["exit_reason"] is None   # null exit fee counts as 0
        assert t0["realized_pnl"] == pytest.approx((60800.25 - 60100.5) * 0.0123 - 0.15)
        assert "is_open" not in t0
        assert t1["is_open"] is True and t1["exit_order_id"] is None and t1["realized_pnl"] is None
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_compare_and_equity(db):
    await _seed(db.pool)
    c = await _client(db)
    try:
        body = await (await c.get(API + "compare?from=2026-05-01")).json()
        rows = body["comparison"]
        assert [(r["expected_action"], r["status"]) for r in rows] == [
            ("enter", "matched"), ("exit", "matched"), ("enter", "missed"), ("exit", "matched"), ("enter", "matched")]
        e = rows[0]
        assert e["timing_lag_ms"] == 5123 + 0 and e["actual_fill_time"] == "2026-05-20T04:00:05.123Z"
        assert e["expected_price"] == 60000.5 and e["actual_price"] == 60100.5
        assert e["slippage_pct"] == pytest.approx((60100.5 - 60000.5) / 60000.5 * 100)
        assert e["qty_diff"] == pytest.approx(0.0123 - 0.0123)
        assert list(rows[2]) == ["bar_ts", "expected_action", "expected_price", "expected_qty", "actual_price",
                                 "actual_qty", "actual_fill_time", "timing_lag_ms", "slippage_pct", "qty_diff", "status"]
        d = rows[3]  # delayed path: values straight from the signal row + the order found via original_signal_ts
        assert d["delay_note"] == "delayed exit" and d["actual_price"] == 61100.0 and d["actual_qty"] == 0.0111
        assert d["timing_lag_ms"] == 7 * 60_000 + 123 + 0
        z = rows[4]  # expected price 0 -> Infinity -> null, like JSON.stringify
        assert z["expected_price"] == 0 and z["slippage_pct"] is None
        st = body["stats"]
        assert st["total"] == 5 and st["matched"] == 4 and st["missed"] == 1
        assert st["avg_slippage_pct"] is None  # Infinity average -> null

        eq = await (await c.get(API + "equity?from=2026-05-20")).json()
        assert eq["days"] == 90
        # one round trip: (60400.25-60000.5)*0.0123 on top of 200, then the delayed exit has no open trade
        assert eq["expected"][0] == {"ts": "2026-05-20T00:00:00.000Z", "equity": 200}
        assert eq["expected"][2]["equity"] == round(200 + (60400.25 - 60000.5) * 0.0123, 2)
        assert [a["equity"] for a in eq["actual"]] == [148.2, 149.2, 150.2, 151.2]
        assert eq["actual"][0]["ts"] == "2026-05-20T00:00:00.123Z"
        assert (await (await c.get(API + "equity?days=7")).json())["days"] == 7
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_status_with_and_without_providers(db):
    await _seed(db.pool)
    c = await _client(db)
    try:
        body = await (await c.get(API + "status")).json()
        assert list(body) == ["latest_signal", "strategy_status", "live_position", "total_equity",
                              "next_bar_ts", "next_bar_eta_ms"]
        assert body["strategy_status"] is None and body["live_position"] is None
        assert body["latest_signal"]["bar_ts"] == "2026-05-20T08:00:00.000Z" or body["latest_signal"]["bar_ts"].startswith("2026-05-2")
        assert body["total_equity"] == 151.2
        assert body["next_bar_ts"].endswith(".000Z") and 0 <= body["next_bar_eta_ms"] <= 14_400_000
    finally:
        await c.close()

    async def st():
        return {"strategy_id": "supertrend-01", "is_running": True, "allocated_capital": 140.8,
                "current_pnl": 1.5, "position_count": 1, "last_tick": "2026-11-24T03:41:12.123456"}

    async def pos():
        return [{"symbol": "ETH/USDT:USDT", "side": "long", "size": 1.0},
                {"symbol": "BTC/USDT:USDT", "side": "long", "size": 0.007, "entry_price": 61000.5,
                 "unrealized_pnl": 1.2345, "leverage": 3.0}]

    async def boom():
        raise RuntimeError("exchange down")

    c = await _client(db, ws.SupertrendProviders(strategy_status=st, positions=pos))
    try:
        body = await (await c.get(API + "status")).json()
        assert body["strategy_status"]["position_count"] == 1
        assert body["live_position"]["symbol"] == "BTC/USDT:USDT" and body["live_position"]["size"] == 0.007
        assert '"leverage":3}' in await (await c.get(API + "status")).text()
    finally:
        await c.close()
    c = await _client(db, ws.SupertrendProviders(strategy_status=boom, positions=boom))
    try:  # provider failures degrade to null ("Redis optional"), never a 500
        body = await (await c.get(API + "status")).json()
        assert body["strategy_status"] is None and body["live_position"] is None
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_in_progress_candle(db):
    import time
    bucket = int(time.time() * 1000) // ws.BAR_MS * ws.BAR_MS

    async def cur():
        return {"ts": bucket, "open": 61000.5, "high": 61500.0, "low": 60800.25, "close": 61241.0,
                "volume": 1234.567, "confirmed": False}

    async def stale():
        return {**(await cur()), "ts": bucket - ws.BAR_MS}

    async def none():
        return None

    async def boom():
        raise RuntimeError("x")

    for prov, expect in ((cur, True), (stale, False), (none, False), (boom, False), (None, False)):
        c = await _client(db, ws.SupertrendProviders(in_progress_candle=prov))
        try:
            r = await c.get(API + "candles/in-progress")
            assert r.status == 200
            body = await r.json()
            if expect:
                assert body == {"candle": {"ts": bucket, "open": 61000.5, "high": 61500, "low": 60800.25,
                                           "close": 61241, "volume": 1234.567, "confirmed": False},
                                "source": "exchange"}
                assert '"high":61500,' in await (await c.get(API + "candles/in-progress")).text()
            else:
                assert body == {"candle": None, "source": "empty"}
        finally:
            await c.close()


@pytest.mark.asyncio
async def test_errors_are_500_with_old_bodies(db):
    c = await _client(db)  # no tables seeded beyond DDL; break one
    try:
        async with db.pool.acquire() as con:
            await con.execute("DROP TABLE ohlcv_history")
        r = await c.get(API + "candles")
        assert r.status == 500 and await r.json() == {"error": "Failed to fetch candles"}
        r = await c.get(API + "expected?from=1&from=2")  # repeated param: the old handler threw -> 500
        assert r.status == 500 and await r.json() == {"error": "Failed to fetch expected signals"}
        assert (await c.get(API + "nope")).status == 404
    finally:
        await c.close()


# ------------------------------------------------------- bot-side providers

@pytest.mark.asyncio
async def test_make_providers_from_bot_objects():
    from bot.interfaces import SYMBOL
    from shared.models.position import Position

    bucket = int(datetime.now(UTC).timestamp() * 1000) // ws.BAR_MS * ws.BAR_MS
    calls = {"status": 0, "pos": 0, "ohlcv": 0}

    class App:
        async def status(self):
            calls["status"] += 1
            return {"equity": 100.0, "position": {"unrealized_pnl": 2.5}, "halted": False,
                    "heartbeat_at": datetime(2026, 11, 24, 3, 0, 1, 5, tzinfo=UTC)}

    class Executor:
        async def get_position(self):
            calls["pos"] += 1
            return Position(exchange="bybit", symbol=SYMBOL, side="long", size=0.007, entry_price=61000.5,
                            unrealized_pnl=2.5, leverage=3.0, liquidation_price=41000.0, margin_used=140.0)

    class Exchange:
        async def get_ohlcv(self, symbol, timeframe="4h", since=None, limit=1000):
            calls["ohlcv"] += 1
            return [[bucket - ws.BAR_MS, 1, 2, 0.5, 1.5, 10], [bucket, 61000.5, 61500, 60800.25, 61241, 1234.567]]

    p = ws.make_providers(app=App(), executor=Executor(), exchange=Exchange())
    st = await p.strategy_status()
    assert st == {"strategy_id": "supertrend-01", "is_running": True, "allocated_capital": 95.0,
                  "current_pnl": 2.5, "position_count": 1, "last_tick": "2026-11-24T03:00:01.000005"}
    positions = await p.positions()
    assert positions == [Position(exchange="bybit", symbol=SYMBOL, side="long", size=0.007, entry_price=61000.5,
                                  unrealized_pnl=2.5, leverage=3.0, liquidation_price=41000.0,
                                  margin_used=140.0).model_dump()]
    c1 = await p.in_progress_candle()
    assert c1 == {"ts": bucket, "open": 61000.5, "high": 61500.0, "low": 60800.25, "close": 61241.0,
                  "volume": 1234.567, "confirmed": False}
    for _ in range(3):  # cached: no extra exchange traffic from a polling page
        await p.strategy_status(); await p.positions(); await p.in_progress_candle()
    assert calls == {"status": 1, "pos": 1, "ohlcv": 1}
