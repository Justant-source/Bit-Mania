"""OrderExecutor tests against FakeExchange and a throwaway Postgres (127.0.0.1:55432, db test_exec)."""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for p in (str(_ROOT / "tests"), str(_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

import asyncpg  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

import bot.execution as ex  # noqa: E402
from bot.execution import OrderExecutor, compute_realized_pnl, floor_to_step, format_hold_duration  # noqa: E402
from bot.safety import SafetyGuard  # noqa: E402
from fakes.exchange import FakeExchange  # noqa: E402
from shared.models.order import OrderRequest  # noqa: E402
from shared.models.position import Position  # noqa: E402

pytestmark = pytest.mark.asyncio
SYM = "BTC/USDT:USDT"
PG = dict(host="127.0.0.1", port=55432, user="postgres", password="test")

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, order_id TEXT,
    exchange TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL, price DOUBLE PRECISION, status TEXT NOT NULL DEFAULT 'pending',
    filled_qty DOUBLE PRECISION DEFAULT 0, filled_price DOUBLE PRECISION, fee DOUBLE PRECISION DEFAULT 0,
    fee_currency TEXT DEFAULT 'USDT', strategy_id TEXT, post_only BOOLEAN DEFAULT TRUE,
    reduce_only BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), delay_reason TEXT,
    original_signal_ts TIMESTAMPTZ, original_request_id TEXT
);
"""


class DbHolder:
    def __init__(self, pool):
        self.pool = pool


@pytest_asyncio.fixture
async def db():
    admin = await asyncpg.connect(database="postgres", **PG)
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname='test_exec'"):
            await admin.execute("CREATE DATABASE test_exec")
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(database="test_exec", min_size=1, max_size=4, **PG)
    async with pool.acquire() as c:
        await c.execute(DDL)
        await c.execute("TRUNCATE orders")
    yield DbHolder(pool)
    await pool.close()


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 0.05)
    monkeypatch.setattr(ex, "REPEG_POLL_S", 0.01)
    monkeypatch.setattr(ex, "REPEG_ERROR_SLEEP_S", 0.01)
    monkeypatch.setattr(ex, "RETRY_BACKOFF_S", 0.01)
    monkeypatch.setattr(ex, "SETTLE_POLL_S", 0.01)
    monkeypatch.setattr(ex, "CLOSE_RETRY_PAUSE_S", 0.01)
    monkeypatch.setattr(ex, "PERSIST_BACKOFF_S", 0.001)


class Gate:
    blocked = False

    def entries_blocked(self):
        return (True, "manual_halt") if self.blocked else (False, "")


@pytest.fixture
def fx():
    f = FakeExchange(wallet=1000.0)
    f.set_book(99_995, 100_005)
    return f


@pytest.fixture
def gate():
    return Gate()


@pytest.fixture
def make(fx, db, gate):
    def _make(**kw):
        safety = SafetyGuard(fx, gate)
        return OrderExecutor(fx, db, safety, **kw)
    return _make


def entry(qty=0.01, **kw):
    d = dict(strategy_id="supertrend-01", exchange="bybit", symbol=SYM, side="buy", order_type="limit",
             quantity=qty, price=99_995.0, post_only=True, stop_loss=70_000.0)
    d.update(kw)
    return OrderRequest(**d)


def exit_order(qty=0.01, **kw):
    d = dict(strategy_id="supertrend-01", exchange="bybit", symbol=SYM, side="sell", order_type="market",
             quantity=qty, post_only=False, reduce_only=True)
    d.update(kw)
    return OrderRequest(**d)


async def row(db, rid):
    async with db.pool.acquire() as c:
        return await c.fetchrow("SELECT * FROM orders WHERE request_id=$1", rid)


async def test_implements_protocol(make):
    e = make()
    for name in ("start", "execute", "get_position", "get_balance", "close_position"):
        assert callable(getattr(e, name))


# ── pure helpers ─────────────────────────────────────────────────────────

async def test_floor_to_step():
    assert floor_to_step(0.0057, 0.001) == pytest.approx(0.005)
    assert floor_to_step(0.006, 0.001) == pytest.approx(0.006)
    assert floor_to_step(-0.001, 0.001) == 0.0
    assert floor_to_step(0.0009, 0.001) == 0.0


async def test_realized_pnl_and_hold():
    pos = Position(exchange="bybit", symbol=SYM, side="long", size=0.01, entry_price=100_000, leverage=3)
    pnl, pct = compute_realized_pnl(pos, 101_000, 0.01, 0.5)
    assert pnl == pytest.approx(9.5) and pct == pytest.approx(1.0)
    now = datetime(2026, 1, 3, 12, tzinfo=timezone.utc)
    assert format_hold_duration(datetime(2026, 1, 1, 8, tzinfo=timezone.utc), now) == "2d 4h"
    assert format_hold_duration(datetime(2026, 1, 3, 9, 45, tzinfo=timezone.utc), now) == "2h 15m"
    assert format_hold_duration(None, now) is None


# ── limit entry / re-peg ─────────────────────────────────────────────────

async def test_limit_entry_fills_and_persists(make, fx, db):
    ex_ = make()
    o = entry()
    res = await ex_.execute(o, signal_ts=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert res.status == "filled" and res.is_filled
    assert res.filled_qty == pytest.approx(0.01) and res.filled_price == pytest.approx(99_995.0)
    assert res.reason == "" and len(res.order_ids) == 1
    sent = fx.placed[0]
    assert sent.post_only and sent.stop_loss == 70_000.0 and sent.price == 99_995.0
    r = await row(db, o.request_id)
    assert r["status"] == "filled" and r["filled_qty"] == pytest.approx(0.01)
    assert r["order_id"] == res.order_ids[-1] and r["strategy_id"] == "supertrend-01"
    assert r["post_only"] is True and r["reduce_only"] is False
    assert r["original_signal_ts"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert fx.position.size == pytest.approx(0.01) and fx.position_stop_loss == 70_000.0


async def test_peg_follows_best_bid_for_buy_and_best_ask_for_sell(make, fx):
    ex_ = make()
    fx.set_position("long", 0.01, 100_000)
    await ex_.execute(entry(price=1.0))
    assert fx.placed[0].price == 99_995.0
    await ex_.execute(entry(side="sell", reduce_only=True, stop_loss=None, price=1.0))
    assert fx.placed[1].price == 100_005.0


async def test_partial_fill_repegs_remaining_only(make, fx):
    fx.plan_fills(("partial", 0.4), "full")
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert [round(p.quantity, 6) for p in fx.placed] == [0.01, 0.006]
    assert len(res.order_ids) == 2
    assert fx.position.size == pytest.approx(0.01)
    assert not [p for p in fx.placed if p.order_type == "market"]


async def test_weighted_average_price_across_repegs(make, fx):
    fx.plan_fills(("partial", 0.4), "full")
    ex_ = make()

    original = fx.get_orderbook

    async def moving_book(symbol, limit=25):
        book = await original(symbol, limit)
        return book

    res = await ex_.execute(entry(0.01))
    # second attempt re-pegs at the same bid here; price must equal the average of fills
    assert res.filled_price == pytest.approx(99_995.0)


async def test_post_only_rejection_repegs_immediately(make, fx):
    fx.post_only_reject_next(2)
    res = await make().execute(entry())
    assert res.status == "filled"
    assert len(fx.placed) == 3  # two rejected, third rests and fills


async def test_market_fallback_after_exhaustion_combines_partial(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_MAX_ATTEMPTS", 2)
    fx.plan_fills(("partial", 0.3), "never")
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    markets = [p for p in fx.placed if p.order_type == "market"]
    assert len(markets) == 1
    assert markets[0].quantity == pytest.approx(0.007)
    assert markets[0].post_only is False and markets[0].stop_loss == 70_000.0
    # partial at 99_995 (maker) + market at ask 100_005
    expected = (0.003 * 99_995 + 0.007 * 100_005) / 0.01
    assert res.filled_price == pytest.approx(expected)
    assert res.fee == pytest.approx(0.003 * 99_995 * 0.0002 + 0.007 * 100_005 * 0.00055)
    assert fx.cancelled  # live orders cleaned up
    assert not [o for o in fx.orders.values() if o.status == "open"]


async def test_fallback_rejected_after_partial_reports_partial(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_MAX_ATTEMPTS", 1)
    fx.plan_fills(("partial", 0.3))
    ex_ = make()
    orig = fx.place_order

    async def place(order):
        if order.order_type == "market":
            return fx._rejected(order, "no funds")
        return await orig(order)

    fx.place_order = place
    res = await ex_.execute(entry(0.01))
    assert res.status == "partial" and res.filled_qty == pytest.approx(0.003)
    assert "no funds" in res.reason and not res.is_filled and res.any_fill


async def test_nothing_filled_and_fallback_rejected(make, fx, monkeypatch, db):
    monkeypatch.setattr(ex, "REPEG_MAX_ATTEMPTS", 1)
    fx.fill_limits("never")
    ex_ = make()
    fx.reject_next(0)
    orig = fx.place_order

    async def place(order):
        if order.order_type == "market":
            return fx._rejected(order, "market refused")
        return await orig(order)

    fx.place_order = place
    o = entry()
    res = await ex_.execute(o)
    assert res.status == "rejected" and res.filled_qty == 0 and "market refused" in res.reason
    assert (await row(db, o.request_id))["status"] == "rejected"


async def test_remainder_below_min_step_is_dropped(make, fx):
    fx.fill_limits(("partial", 1.0))  # fills 0.010 of 0.0102, stays open
    res = await make().execute(entry(0.0102))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.010)
    assert not [p for p in fx.placed if p.order_type == "market"]
    assert [round(p.quantity, 6) for p in fx.placed] == [0.01]


async def test_order_below_min_qty_rejected_without_exchange_call(make, fx):
    res = await make().execute(entry(0.0005))
    assert res.status == "rejected" and res.reason == "repeg_no_fill_below_min_qty"
    assert fx.placed == []


async def test_fill_after_n_polls_waits_without_repeg(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 1.0)
    fx.fill_limits(("after", 3))
    res = await make().execute(entry())
    assert res.status == "filled" and len(fx.placed) == 1


async def test_dead_order_repegs_without_waiting_full_interval(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 30.0)
    fx.plan_fills("never", "full")
    orig = fx.fetch_order_result
    state = {"n": 0}

    async def fetch(oid, symbol, rid):
        state["n"] += 1
        if state["n"] == 1:
            fx.orders[oid].status = "canceled"  # exchange killed it
        return await orig(oid, symbol, rid)

    fx.fetch_order_result = fetch
    res = await asyncio.wait_for(make().execute(entry()), 5)
    assert res.status == "filled" and len(fx.placed) == 2


async def test_orderbook_failure_retries(make, fx):
    fx.fail_next("get_orderbook", 2)
    res = await make().execute(entry())
    assert res.status == "filled"


async def test_cancellation_cleans_up_live_order(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 5.0)
    fx.fill_limits("never")
    task = asyncio.create_task(make().execute(entry()))
    await asyncio.sleep(0.15)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fx.cancelled and not [o for o in fx.orders.values() if o.status == "open"]


async def test_timeout_cancels_order_and_keeps_partial(make, fx, monkeypatch, db):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 5.0)
    monkeypatch.setattr(ex, "ORDER_TIMEOUT", 0.2)
    fx.plan_fills(("partial", 0.5))
    o = entry()
    res = await make().execute(o)
    assert res.status == "partial" and res.reason == "order_timeout"
    assert res.filled_qty == pytest.approx(0.005)
    assert not [x for x in fx.orders.values() if x.status == "open"]
    assert (await row(db, o.request_id))["status"] == "partially_filled"


# ── market path ──────────────────────────────────────────────────────────

async def test_market_order_filled(make, fx):
    fx.set_position("long", 0.01, 100_000)
    res = await make().execute(exit_order())
    assert res.status == "filled" and res.filled_price == pytest.approx(99_995)
    assert fx.position is None


async def test_market_rejected_carries_exchange_reason(make, fx):
    fx.reject_next(1, "Insufficient balance (110007)")
    res = await make().execute(exit_order())
    assert res.status == "rejected" and "110007" in res.reason


async def test_market_exception_retried_then_ok(make, fx):
    fx.set_position("long", 0.01, 100_000)
    fx.fail_next("place_order", 2)
    res = await make().execute(exit_order())
    assert res.status == "filled" and len(fx.placed) == 1
    assert [c for c in fx.calls if c[0] == "place_order"].__len__() == 3


async def test_market_exception_exhausted(make, fx):
    fx.fail_next("place_order", 5)
    res = await make().execute(exit_order())
    assert res.status == "rejected" and res.reason.startswith("max_retries_exhausted")
    assert len([c for c in fx.calls if c[0] == "place_order"]) == 3


# ── safety integration / gate ────────────────────────────────────────────

async def test_entry_rejected_while_blocked_and_persisted(make, fx, gate, db):
    gate.blocked = True
    o = entry()
    res = await make().execute(o)
    assert res.status == "rejected" and res.reason.startswith("entries_blocked")
    assert fx.placed == []
    assert (await row(db, o.request_id))["status"] == "rejected"


async def test_reduce_only_passes_while_entries_blocked(make, fx, gate):
    gate.blocked = True
    fx.set_position("long", 0.01, 100_000)
    res = await make().execute(exit_order())
    assert res.status == "filled" and fx.position is None


async def test_safety_internal_error_rejects(make, fx):
    ex_ = make()

    async def boom(order):
        raise RuntimeError("x")

    ex_._safety.check_order = boom
    res = await ex_.execute(entry())
    assert res.status == "rejected" and res.reason == "safety_check_internal_error"
    assert fx.placed == []


# ── dry run, idempotency ─────────────────────────────────────────────────

async def test_dry_run_sends_nothing(make, fx, db):
    ex_ = make(dry_run=True)
    o = entry()
    res = await ex_.execute(o)
    assert res.status == "rejected" and res.reason == "dry_run"
    assert fx.placed == []
    assert await row(db, o.request_id) is None
    fx.set_position("long", 0.01, 100_000)
    close = await ex_.close_position("test")
    assert close.reason == "dry_run" and fx.placed == [] and fx.position is not None


async def test_duplicate_request_id_is_idempotent(make, fx):
    ex_ = make()
    o = entry()
    a = await ex_.execute(o)
    b = await ex_.execute(o)
    assert a is b and len(fx.placed) == 1


async def test_duplicate_after_restart_uses_db_row(make, fx, db):
    o = entry()
    a = await make().execute(o)
    b = await make().execute(o)  # fresh executor, same DB
    assert len(fx.placed) == 1
    assert b.status == a.status == "filled" and b.filled_qty == pytest.approx(a.filled_qty)
    assert b.order_ids == a.order_ids


async def test_concurrent_duplicate_shares_one_execution(make, fx):
    fx.fill_limits(("after", 3))
    ex_ = make()
    o = entry()
    a, b = await asyncio.gather(ex_.execute(o), ex_.execute(o))
    assert a.status == b.status == "filled" and len(fx.placed) == 1


async def test_unfinished_row_from_crashed_run_is_not_resent(make, fx, db):
    o = entry()
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO orders (request_id,exchange,symbol,side,order_type,quantity,status) "
                        "VALUES ($1,'bybit',$2,'buy','limit',0.01,'submitted')", o.request_id, SYM)
    res = await make().execute(o)
    assert res.status == "rejected" and res.reason == "duplicate_request_id_unfinished"
    assert fx.placed == []


# ── reads ────────────────────────────────────────────────────────────────

async def test_get_balance_and_position(make, fx):
    ex_ = make()
    bal = await ex_.get_balance()
    assert bal["total"] == bal["wallet"] == 1000.0 and "free" in bal
    assert await ex_.get_position() is None
    fx.set_position("long", 0.01, 100_000)
    pos = await ex_.get_position()
    assert pos.size == 0.01 and pos.side == "long"


async def test_get_balance_adds_wallet_when_missing(make, fx):
    ex_ = make()

    async def gb():
        return {"total": 5.0, "free": 4.0}

    fx.get_balance = gb
    assert (await ex_.get_balance())["wallet"] == 5.0


# ── close_position ───────────────────────────────────────────────────────

async def test_close_position_when_flat_returns_none(make, fx):
    assert await make().close_position("x") is None
    assert fx.placed == []


async def test_close_position_closes_full_size_reduce_only(make, fx):
    fx.set_position("long", 0.023, 100_000)
    res = await make().close_position("emergency")
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.023)
    o = fx.placed[0]
    assert o.order_type == "market" and o.reduce_only and o.side == "sell" and o.quantity == pytest.approx(0.023)
    assert fx.position is None
    assert fx.cancel_conditional_calls == [SYM]


async def test_close_position_short_buys(make, fx):
    fx.set_position("short", 0.01, 100_000)
    res = await make().close_position("x")
    assert res.status == "filled" and fx.placed[0].side == "buy"


async def test_close_position_retries_until_flat(make, fx):
    fx.set_position("long", 0.01, 100_000)
    fx.reject_next(2, "temporary")
    res = await make().close_position("x")
    assert res.status == "filled" and len(fx.placed) == 3 and fx.position is None


async def test_close_position_gives_up_after_three_orders(make, fx):
    fx.set_position("long", 0.01, 100_000)
    fx.reject_next(10, "exchange down")
    res = await make().close_position("x")
    assert res.status == "rejected" and "exchange down" in res.reason
    assert len(fx.placed) == 3 and fx.position is not None
    assert fx.cancel_conditional_calls == []


async def test_close_position_partial_fills_combine(make, fx):
    fx.set_position("long", 0.01, 100_000)
    ex_ = make()
    orig = fx.place_order
    n = {"i": 0}

    async def place(order):
        n["i"] += 1
        if n["i"] == 1:  # first close fills only 0.004
            return await orig(order.model_copy(update={"quantity": 0.004}))
        return await orig(order)

    fx.place_order = place
    res = await ex_.close_position("x")
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert len(res.order_ids) == 2


async def test_close_position_read_failures(make, fx):
    fx.set_position("long", 0.01, 100_000)
    fx.fail_next("get_position", 1)
    res = await make().close_position("x")
    assert res.status == "filled"
    fx.set_position("long", 0.01, 100_000)
    fx.fail_next("get_position", 3)
    res = await make().close_position("x")
    assert res.status == "rejected" and res.reason == "position_read_failed"


async def test_close_position_flat_before_fill(make, fx):
    ex_ = make()
    fx.set_position("long", 0.01, 100_000)
    orig = fx.place_order

    async def place(order):
        fx.position = None  # someone else closed it first
        return await orig(order)

    fx.place_order = place
    res = await ex_.close_position("x")
    assert res.status == "cancelled" and res.reason == "flat_before_fill"


async def test_close_position_aborts_inflight_entry(make, fx, monkeypatch):
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 5.0)
    fx.plan_fills(("partial", 0.5))
    ex_ = make()
    task = asyncio.create_task(ex_.execute(entry(0.01)))
    await asyncio.sleep(0.15)
    close = await ex_.close_position("emergency")
    res = await task
    assert res.status == "partial" and res.reason == "aborted" and res.filled_qty == pytest.approx(0.005)
    assert close.status == "filled" and close.filled_qty == pytest.approx(0.005)
    assert fx.position is None
    assert not [o for o in fx.orders.values() if o.status == "open"]


# ── stale stop cleanup ───────────────────────────────────────────────────

async def test_exit_to_flat_cancels_conditional_orders(make, fx):
    fx.set_position("long", 0.01, 100_000)
    await make().execute(exit_order())
    assert fx.cancel_conditional_calls == [SYM]


async def test_partial_exit_keeps_stops(make, fx):
    fx.set_position("long", 0.02, 100_000)
    await make().execute(exit_order(0.01))
    assert fx.cancel_conditional_calls == []


async def test_stop_cleanup_failure_is_swallowed(make, fx):
    fx.set_position("long", 0.01, 100_000)
    fx.fail_next("cancel_conditional_orders")
    res = await make().execute(exit_order())
    assert res.status == "filled"


# ── orphan cleanup on start ──────────────────────────────────────────────

async def _insert(db, rid, oid, status):
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO orders (request_id,order_id,exchange,symbol,side,order_type,quantity,status) "
                        "VALUES ($1,$2,'bybit',$3,'buy','limit',0.01,$4)", rid, oid, SYM, status)


async def test_start_cancels_orphans_and_resolves_rows(make, fx, db):
    fx.fill_limits("never")
    open_o = await fx.place_order(entry(price=99_000))
    part = await fx.place_order(entry(price=99_000))
    fx.fill_order(part.order_id, 0.004)
    done = await fx.place_order(entry(price=99_000))
    fx.fill_order(done.order_id)
    await _insert(db, "r-open", open_o.order_id, "submitted")
    await _insert(db, "r-part", part.order_id, "submitted")
    await _insert(db, "r-done", done.order_id, "submitted")
    await _insert(db, "r-none", None, "pending")
    await _insert(db, "r-gone", "fx-999", "submitted")
    await _insert(db, "r-final", None, "filled")

    await make().start()

    assert (await row(db, "r-open"))["status"] == "cancelled"
    r = await row(db, "r-part")
    assert r["status"] == "partially_filled" and r["filled_qty"] == pytest.approx(0.004)
    r = await row(db, "r-done")
    assert r["status"] == "filled" and r["filled_qty"] == pytest.approx(0.01)
    assert (await row(db, "r-none"))["status"] == "rejected"
    assert (await row(db, "r-gone"))["status"] == "submitted"  # unknown: left alone, logged
    assert (await row(db, "r-final"))["status"] == "filled"
    assert fx.orders[open_o.order_id].status == "canceled"
    assert fx.orders[part.order_id].status == "canceled"


async def test_start_survives_exchange_errors(make, fx, db):
    await _insert(db, "r1", "fx-1", "submitted")
    fx.fail_next("fetch_order_status", 1)
    await make().start()  # must not raise
    assert (await row(db, "r1"))["status"] == "submitted"


# ── ambiguous placement failures (timeouts) ─────────────────────────────────


def open_orders(fx):
    return [o for o in fx.orders.values() if o.status == "open"]


@pytest.mark.parametrize("raises", [False, True])
async def test_ambiguous_failure_order_rests_is_cancelled_no_orphan(make, fx, raises):
    fx.ambiguous_next(1, "rests", raises=raises)
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert len(fx.placed) == 2                       # the retry went out after the cleanup
    assert fx.cancel_open_calls == [SYM]
    assert open_orders(fx) == []
    assert fx.position.size == pytest.approx(0.01)   # not doubled
    assert res.order_ids == ["fx-2"]                 # the first order was cancelled and never tracked


async def test_ambiguous_failure_order_filled_is_adopted_not_doubled(make, fx):
    fx.ambiguous_next(1, "filled")
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert len(fx.placed) == 1                       # no second order
    assert fx.position.size == pytest.approx(0.01)   # requested quantity, not 2x
    assert res.filled_price == pytest.approx(99_995.0)   # last peg price
    assert res.fee == 0.0 and open_orders(fx) == []


async def test_ambiguous_failure_adopted_fill_is_logged_at_error(make, fx):
    from structlog.testing import capture_logs

    fx.ambiguous_next(1, "filled")
    with capture_logs() as logs:
        await make().execute(entry(0.01))
    adopted = [e for e in logs if e["event"] == "untracked_fill_adopted"]
    assert len(adopted) == 1 and adopted[0]["log_level"] == "error"
    assert adopted[0]["qty"] == pytest.approx(0.01)


async def test_ambiguous_failure_partially_adopted_then_remainder_placed(make, fx):
    fx.set_position("long", 0.004, 99_000.0)         # existing size: only the delta counts
    fx.ambiguous_next(1, "filled")
    res = await make().execute(entry(0.01))
    assert res.filled_qty == pytest.approx(0.01) and len(fx.placed) == 1
    assert fx.position.size == pytest.approx(0.014)


async def test_ambiguous_failure_nothing_reached_exchange_retries_normally(make, fx):
    fx.ambiguous_next(1, "lost")
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert len(fx.placed) == 2 and len(fx.orders) == 1
    assert fx.cancel_open_calls == [SYM]
    assert fx.position.size == pytest.approx(0.01)


async def test_ambiguous_failure_position_unreadable_stops_with_ambiguous_order_state(make, fx):
    fx.ambiguous_next(1, "rests")
    real = fx.get_position
    n = {"calls": 0}

    async def flaky(symbol):
        n["calls"] += 1
        if n["calls"] >= 2:                          # baseline read worked, the re-read fails
            raise ConnectionError("down")
        return await real(symbol)

    fx.get_position = flaky                          # type: ignore[assignment]
    res = await make().execute(entry(0.01))
    assert res.status == "rejected" and res.reason == "ambiguous_order_state"
    assert len(fx.placed) == 1                       # stopped: no blind second order
    assert open_orders(fx) == []                     # the possibly resting order was cancelled first


async def test_ambiguous_failure_baseline_unknown_stops(make, fx):
    real = fx.get_position
    n = {"calls": 0}

    async def flaky(symbol):
        n["calls"] += 1
        if n["calls"] == 2:                          # 1st: safety check, 2nd: the baseline read
            raise ConnectionError("down")
        return await real(symbol)

    fx.get_position = flaky                          # type: ignore[assignment]
    fx.ambiguous_next(1, "rests")
    res = await make().execute(entry(0.01))
    assert res.status == "rejected" and res.reason == "ambiguous_order_state"
    assert len(fx.placed) == 1 and open_orders(fx) == []


async def test_ambiguous_failure_cancel_failure_stops(make, fx):
    fx.ambiguous_next(1, "rests")
    fx.fail_next("cancel_open_orders", 1)
    res = await make().execute(entry(0.01))
    assert res.status == "rejected" and res.reason == "ambiguous_order_state"
    assert len(fx.placed) == 1


async def test_ambiguous_failure_after_partial_fill_reports_partial(make, fx):
    fx.plan_fills(("partial", 0.5))                  # first order fills half, then a timeout on the re-peg
    fx.ambiguous_next(0)
    real = fx.place_order
    n = {"calls": 0}

    async def second_times_out(order):
        n["calls"] += 1
        if n["calls"] == 2:
            fx.ambiguous_next(1, "rests")
        return await real(order)

    fx.place_order = second_times_out                # type: ignore[assignment]
    real_get = fx.get_position
    reads = {"n": 0}

    async def get_position(symbol):
        reads["n"] += 1
        if fx.cancel_open_calls:                     # unreadable after the ambiguous failure
            raise ConnectionError("down")
        return await real_get(symbol)

    fx.get_position = get_position                   # type: ignore[assignment]
    res = await make().execute(entry(0.01))
    assert res.status == "partial" and res.reason == "ambiguous_order_state"
    assert res.filled_qty == pytest.approx(0.005)


async def test_definite_rejection_behaviour_unchanged(make, fx):
    fx.reject_next(2, "invalid order parameters (10001)")   # definite: exchange answered with an error
    res = await make().execute(entry(0.01))
    assert res.status == "filled" and len(fx.placed) == 3
    assert fx.cancel_open_calls == []                # no cleanup for a definite rejection


async def test_definite_insufficient_funds_unchanged_goes_to_market_fallback(make, fx):
    fx.reject_next(1, "insufficient balance 110007")
    res = await make().execute(entry(0.01))
    assert res.status == "filled"                    # pre-existing behaviour: ends the re-pegs, market fallback
    assert [o.order_type for o in fx.placed] == ["limit", "market"] and fx.cancel_open_calls == []


async def test_market_leg_ambiguous_filled_is_adopted(make, fx):
    fx.set_position("long", 0.01, 100_000.0)
    fx.ambiguous_next(1, "filled")
    res = await make().execute(exit_order(0.01))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert len(fx.placed) == 1 and fx.position is None   # no second sell
    assert fx.cancel_open_calls == [SYM]


async def test_market_leg_ambiguous_lost_retries(make, fx):
    fx.set_position("long", 0.01, 100_000.0)
    fx.ambiguous_next(1, "lost", raises=True)
    res = await make().execute(exit_order(0.01))
    assert res.status == "filled" and len(fx.placed) == 2 and fx.position is None


async def test_market_leg_position_unreadable_stops(make, fx):
    fx.set_position("long", 0.01, 100_000.0)
    fx.ambiguous_next(1, "filled")
    real = fx.get_position
    n = {"calls": 0}

    async def flaky(symbol):
        n["calls"] += 1
        if n["calls"] >= 2:
            raise ConnectionError("down")
        return await real(symbol)

    fx.get_position = flaky                          # type: ignore[assignment]
    res = await make().execute(exit_order(0.01))
    assert res.status == "rejected" and res.reason == "ambiguous_order_state"
    assert len(fx.placed) == 1


# ── order row persistence (upsert) ──────────────────────────────────────────


async def test_final_write_recreates_a_missing_row(make, fx, db):
    ex_ = make()

    async def lost_insert(order, signal_ts):
        return None

    ex_._persist_new = lost_insert                   # type: ignore[assignment]
    sig = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = entry(0.01)
    res = await ex_.execute(req, signal_ts=sig)
    r = await row(db, req.request_id)
    assert r is not None and r["status"] == "filled" and r["filled_qty"] == pytest.approx(0.01)
    assert (r["exchange"], r["symbol"], r["side"], r["order_type"]) == ("bybit", SYM, "buy", "limit")
    assert r["quantity"] == pytest.approx(0.01) and r["price"] == pytest.approx(99_995.0)
    assert r["strategy_id"] == "supertrend-01" and r["post_only"] is True and r["reduce_only"] is False
    assert r["order_id"] == res.order_ids[-1] and r["filled_price"] == pytest.approx(res.filled_price)
    assert r["fee"] == pytest.approx(res.fee) and r["original_signal_ts"] == sig


async def test_final_write_is_retried_then_succeeds(make, fx, db):
    ex_ = make()
    real = ex_._upsert_result
    n = {"calls": 0}

    async def flaky(*a):
        n["calls"] += 1
        if n["calls"] <= 2:
            raise ConnectionError("blip")
        return await real(*a)

    ex_._upsert_result = flaky                       # type: ignore[assignment]
    req = entry(0.01)
    await ex_.execute(req)
    assert n["calls"] == 3
    assert (await row(db, req.request_id))["status"] == "filled" and ex_._unwritten == {}


async def test_final_write_gives_up_logs_error_and_recovers_later(make, fx, db):
    from structlog.testing import capture_logs

    ex_ = make()
    real = ex_._upsert_result
    state = {"down": True}

    async def maybe(*a):
        if state["down"]:
            raise ConnectionError("db down")
        return await real(*a)

    ex_._upsert_result = maybe                       # type: ignore[assignment]
    req = entry(0.01)
    with capture_logs() as logs:
        await ex_.execute(req)
    assert any(e["log_level"] == "error" and e["event"] == "persist_result_gave_up_will_retry_later" for e in logs)
    assert req.request_id in ex_._unwritten
    state["down"] = False
    await ex_.get_position()                         # next executor activity flushes it
    r = await row(db, req.request_id)
    assert r["status"] == "filled" and ex_._unwritten == {}


async def test_start_logs_error_for_pending_row_without_order_id(make, fx, db):
    from structlog.testing import capture_logs

    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO orders (request_id, exchange, symbol, side, order_type, quantity, status)"
                        " VALUES ('stuck-1','bybit',$1,'buy','limit',0.01,'pending')", SYM)
    with capture_logs() as logs:
        await make().start()
    assert (await row(db, "stuck-1"))["status"] == "rejected"          # marking unchanged
    errs = [e for e in logs if e["log_level"] == "error" and e.get("request_id") == "stuck-1"]
    assert errs and errs[0]["exchange_position_size"] == 0.0
