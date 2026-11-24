from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from bot.report import build_daily_report, pair_trades, report_keyboard

ADMIN = dict(host="127.0.0.1", port=55432, user="postgres", password="test")

DDL = """
DROP TABLE IF EXISTS orders, positions, portfolio_snapshots, supertrend_signals;
CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, side TEXT, order_type TEXT, quantity DOUBLE PRECISION,
  price DOUBLE PRECISION, status TEXT, filled_qty DOUBLE PRECISION, filled_price DOUBLE PRECISION,
  fee DOUBLE PRECISION, reduce_only BOOLEAN, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, strategy_id TEXT);
CREATE TABLE positions (id BIGSERIAL PRIMARY KEY, side TEXT, size NUMERIC, entry_price NUMERIC,
  opened_at TIMESTAMPTZ, closed_at TIMESTAMPTZ, close_reason TEXT);
CREATE TABLE portfolio_snapshots (id BIGSERIAL PRIMARY KEY, snapshot_at TIMESTAMPTZ, total_equity NUMERIC,
  daily_drawdown DOUBLE PRECISION, weekly_drawdown DOUBLE PRECISION, monthly_drawdown DOUBLE PRECISION);
CREATE TABLE supertrend_signals (id BIGSERIAL PRIMARY KEY, bar_ts TIMESTAMPTZ, st_dir SMALLINT,
  price DOUBLE PRECISION, exit_signal BOOLEAN, exit_reason TEXT, entry_ok BOOLEAN, expected_action TEXT);
"""


class Db:
    pool = None


@pytest_asyncio.fixture
async def db():
    admin = await asyncpg.connect(database="postgres", **ADMIN)
    if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname='test_tg'"):
        await admin.execute("CREATE DATABASE test_tg")
    await admin.close()
    d = Db()
    d.pool = await asyncpg.create_pool(database="test_tg", min_size=1, max_size=3, **ADMIN)
    async with d.pool.acquire() as c:
        await c.execute(DDL)
    yield d
    await d.pool.close()


class FakeControl:
    def __init__(self, **kw):
        self._s = {"equity": 150.0, "position": None, "halted": False, "halt_reason": None,
                   "kill_level": "NORMAL", "daily_dd_pct": 0.5, "weekly_dd_pct": 1.0,
                   "monthly_dd_pct": 2.0, **kw}

    async def status(self):
        return dict(self._s)


def T(day, hour=0, minute=0):
    return datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)


def order(side, qty, price, t, *, reduce_only=False, fee=0.1, otype="limit", status="filled", id=0):
    return dict(id=id, side=side, order_type=otype, quantity=qty, price=price, status=status,
                filled_qty=qty, filled_price=price, fee=fee, reduce_only=reduce_only,
                created_at=t, updated_at=t, strategy_id="supertrend-01")


# ── pure pairing ────────────────────────────────────────────────────────────


def test_simple_trade():
    tr = pair_trades([order("buy", 0.007, 80000, T(1, 4)), order("sell", 0.007, 82000, T(2, 4), reduce_only=True)])
    assert len(tr) == 1
    assert tr[0].pnl == pytest.approx(0.007 * 2000 - 0.2)
    assert tr[0].fees == pytest.approx(0.2)


def test_partial_fill_entry_and_market_fallback_exit():
    rows = [
        # entry: limit partial 0.005 + market top-up 0.002 (same bar, two rows)
        order("buy", 0.005, 77399.9, T(1, 4, 5), fee=0.0774, id=1),
        order("buy", 0.002, 77208.9, T(1, 4, 1), otype="market", fee=0.0849, id=2),
        # exit: limit fills 0.004 then cancelled partial, rest by market fallback
        order("sell", 0.004, 75000.0, T(3, 8, 0), reduce_only=True, status="cancelled", fee=0.06, id=3),
        order("sell", 0.003, 74800.0, T(3, 8, 3), reduce_only=True, otype="market", fee=0.1, id=4),
    ]
    tr = pair_trades(rows)
    assert len(tr) == 1
    t = tr[0]
    entry_avg = (0.005 * 77399.9 + 0.002 * 77208.9) / 0.007
    exit_avg = (0.004 * 75000.0 + 0.003 * 74800.0) / 0.007
    assert t.qty == pytest.approx(0.007)
    assert t.entry_avg == pytest.approx(entry_avg)
    assert t.exit_avg == pytest.approx(exit_avg)
    assert t.fees == pytest.approx(0.0774 + 0.0849 + 0.06 + 0.1)
    assert t.pnl == pytest.approx((exit_avg - entry_avg) * 0.007 - t.fees)
    assert t.closed_at == T(3, 8, 3)


def test_open_trade_not_counted_and_orphan_sell_ignored():
    rows = [
        order("sell", 0.007, 77413.9, T(1), reduce_only=True, id=1),   # orphan exit while flat
        order("buy", 0.007, 80000, T(2), id=2),                          # still open
    ]
    assert pair_trades(rows) == []


def test_two_trades_and_dust_remainder_closes():
    rows = [
        order("buy", 0.007, 80000, T(1), id=1),
        order("sell", 0.0065, 81000, T(2), reduce_only=True, id=2),  # 0.0005 dust < min lot left
        order("buy", 0.006, 70000, T(3), id=3),
        order("sell", 0.006, 69000, T(4), reduce_only=True, id=4),
    ]
    tr = pair_trades(rows)
    assert len(tr) == 2
    assert tr[0].qty == pytest.approx(0.0065)
    assert tr[1].pnl == pytest.approx(-6.0 - 0.2)


# ── report against Postgres ─────────────────────────────────────────────────


async def _insert_order(c, o):
    await c.execute(
        "INSERT INTO orders (side,order_type,quantity,price,status,filled_qty,filled_price,fee,reduce_only,"
        "created_at,updated_at,strategy_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        o["side"], o["order_type"], o["quantity"], o["price"], o["status"], o["filled_qty"],
        o["filled_price"], o["fee"], o["reduce_only"], o["created_at"], o["updated_at"], o["strategy_id"])


@pytest.mark.asyncio
async def test_build_report(db):
    now = datetime.now(timezone.utc)
    async with db.pool.acquire() as c:
        # winner closed 10h ago, loser closed 10 days ago
        for o in [
            order("buy", 0.007, 80000, now - timedelta(hours=30)),
            order("sell", 0.007, 82000, now - timedelta(hours=10), reduce_only=True),
            order("buy", 0.007, 80000, now - timedelta(days=12)),
            order("sell", 0.007, 79000, now - timedelta(days=10), reduce_only=True),
            order("buy", 0.007, 70000, now - timedelta(days=40)),
            order("sell", 0.007, 71000, now - timedelta(days=39), reduce_only=True),
        ]:
            await _insert_order(c, o)
        for ago, eq in [(timedelta(hours=25), 100), (timedelta(days=8), 90), (timedelta(days=31), 120)]:
            await c.execute("INSERT INTO portfolio_snapshots (snapshot_at,total_equity) VALUES ($1,$2)",
                            now - ago, eq)
        await c.execute(
            "INSERT INTO supertrend_signals (bar_ts,st_dir,price,exit_signal,exit_reason,entry_ok,expected_action)"
            " VALUES ($1,-1,60185.9,true,'ema_cross',false,'exit')", T(21, 20))
    ctl = FakeControl(equity=150.0, position={"side": "long", "size": 0.007, "entry_price": 80000.0,
                                              "unrealized_pnl": 3.5},
                      halted=True, halt_reason="manual_halt")
    text = await build_daily_report(db, ctl)
    assert "$150.00" in text
    assert "+50.00 USD (+50.00%)" in text       # vs 25h-old snapshot (100)
    assert "+60.00 USD (+66.67%)" in text       # 7d baseline 90
    assert "+30.00 USD (+25.00%)" in text       # 30d baseline 120
    assert "0.0070" in text and "$80,000.00" in text
    # 24h: 1 trade, 2000*0.007=14 - 0.2 fees = +13.80 ; 30d: add the -7.2 loser
    assert "1건 1승 0패  PnL +13.80" in text
    assert "2건 1승 1패  PnL +6.60" in text
    assert "manual_halt" in text
    assert "청산신호 ema_cross" in text
    assert text.count("<pre>") == text.count("</pre>")


@pytest.mark.asyncio
async def test_report_with_empty_tables_and_no_status(db):
    class Broken:
        async def status(self):
            raise RuntimeError("down")
    text = await build_daily_report(db, Broken())
    assert "N/A" in text and "마지막 신호: 없음" in text


def test_report_keyboard():
    assert report_keyboard("") is None
    kb = report_keyboard("https://x.example/?t=abc")
    assert kb.to_dict()["inline_keyboard"][0][0]["url"] == "https://x.example/?t=abc"


# ── exchange-side closes as trade boundaries ────────────────────────────────


def prow(opened, closed, reason):
    return dict(opened_at=opened, closed_at=closed, close_reason=reason)


def test_exchange_side_close_ends_the_open_trade_with_unknown_exit():
    rows = [order("buy", 0.01, 100.0, T(1, 0), id=1),
            order("buy", 0.01, 120.0, T(3, 0), id=2),
            order("sell", 0.01, 200.0, T(4, 0), reduce_only=True, id=3)]
    pos = [prow(T(1, 0, 1), T(2, 0), "exchange_side"), prow(T(3, 0, 1), T(4, 0, 1), "exit")]
    tr = pair_trades(rows, pos)
    assert len(tr) == 2
    assert tr[0].exit_avg is None and tr[0].pnl is None and tr[0].exit_reason == "exchange_side"
    assert tr[0].entry_avg == pytest.approx(100.0) and tr[0].qty == pytest.approx(0.01)
    assert tr[0].closed_at == T(2, 0)
    assert tr[1].entry_avg == pytest.approx(120.0) and tr[1].pnl == pytest.approx(80 * 0.01 - 0.2)
    from bot.report import summarize
    s = summarize(tr)
    assert (s["count"], s["wins"], s["losses"], s["unknown"]) == (2, 1, 0, 1)
    assert s["pnl"] == pytest.approx(tr[1].pnl)


def test_positions_closed_by_exit_or_duplicated_rows_do_not_change_pairing():
    rows = [order("buy", 0.01, 100.0, T(1, 0)), order("sell", 0.01, 110.0, T(2, 0), reduce_only=True)]
    base = pair_trades(rows)
    pos = [prow(T(1, 0, 1), T(2, 0, 1), "exit"), prow(T(1, 1), T(2, 0, 1), "exit"),
           prow(T(1, 0, 1), None, None)]
    assert pair_trades(rows, pos) == base and len(base) == 1


def test_boundary_with_matching_bot_sell_is_not_applied_twice():
    rows = [order("buy", 0.01, 100.0, T(1, 0)), order("sell", 0.01, 110.0, T(2, 0), reduce_only=True)]
    pos = [prow(T(1, 0, 1), T(2, 0, 5), "kill_switch"), prow(T(1, 0, 1), T(2, 0, 5), "kill_switch")]
    tr = pair_trades(rows, pos)
    assert len(tr) == 1 and tr[0].pnl is not None


def test_boundary_without_open_trade_is_ignored():
    rows = [order("buy", 0.01, 100.0, T(1, 0)), order("sell", 0.01, 110.0, T(1, 5), reduce_only=True)]
    tr = pair_trades(rows, [prow(T(1, 0, 1), T(3, 0), "exchange_flat")])
    assert len(tr) == 1 and tr[0].pnl is not None


@pytest.mark.asyncio
async def test_daily_report_counts_unknown_exit_trades(db):
    now = datetime.now(timezone.utc)
    async with db.pool.acquire() as c:
        await c.execute("TRUNCATE orders, positions")
        for i, (side, ro, mins) in enumerate([("buy", False, 120)]):
            await c.execute(
                "INSERT INTO orders (side, order_type, quantity, price, status, filled_qty, filled_price, fee,"
                " reduce_only, created_at, updated_at) VALUES ($1,'limit',0.01,100,'filled',0.01,100,0.1,$2,$3,$3)",
                side, ro, now - timedelta(minutes=mins))
        await c.execute("INSERT INTO positions (side,size,entry_price,opened_at,closed_at,close_reason)"
                        " VALUES ('long',0.01,100,$1,$2,'exchange_side')",
                        now - timedelta(minutes=119), now - timedelta(minutes=60))

    class Ctl:
        async def status(self):
            return {}

    text = await build_daily_report(db, Ctl())
    line = [l for l in text.splitlines() if "24h" in l and "건" in l and "PnL" in l][0]
    assert "1건 0승 0패" in line and "손익 불명 1건" in line
