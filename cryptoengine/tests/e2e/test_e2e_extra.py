"""Extra adversarial scenarios: partial entries, exit failures, in-flight orders vs emergency close,
crash recovery of orphan orders, state refresh vs DB blips, a full day of scheduler housekeeping."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import bot.execution as ex
from bot.state import MANUAL_HALT
from e2e_harness import (
    FlakyPool, approx_qty, atr_crash, ema_exit_tail, entry_plan, extend, feed_for,
)
from shared.models.order import OrderRequest

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def _ready(make_stack, fx, clock, bars, idx, extra=0, **kw):
    feed = feed_for(fx, clock, extend(bars, 0.012, extra) if extra else bars, idx - 1)
    st = await make_stack(feed=feed, **kw)
    await st.app.start()
    await st.bar(idx - 1)
    return st, feed


async def _drain_retry(st, n=1):
    for _ in range(n):
        await asyncio.sleep(0)
        t = st.trader._retry_task
        if t is None or t.done():
            return
        st.clock.release_retry()
        await t


async def test_x1_partial_entry_with_failed_fallback_then_retry_fills_only_the_remainder(
        make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    want = approx_qty(1000, feed.candles[idx]["close"])
    fx.plan_fills(("partial", 0.4))
    fx.fill_limits("never")
    orig = fx.place_order

    async def reject_market(order):
        if order.order_type == "market":
            fx.last_error = "market rejected (injected)"
            fx.placed.append(order)
            from shared.models.order import OrderResult
            return OrderResult(request_id=order.request_id, order_id="", status="rejected")
        return await orig(order)

    fx.place_order = reject_market                          # type: ignore[assignment]
    await st.bar(idx)
    part = fx.position.size
    assert 0 < part < want                                  # only the limit partial is in
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(part)
    (row,) = await st.orders()
    assert row["status"] == "partially_filled"
    assert float((await st.positions())[0]["size"]) == pytest.approx(part)
    fx.place_order = orig                                   # type: ignore[assignment]
    fx.fill_limits("full")
    await _drain_retry(st)                                  # same-bar retry for the remainder only
    assert fx.position.size == pytest.approx(want), "must end at the target, never overfill"
    assert fx.position.size * fx.position.entry_price <= 1000 * 0.95 * 3 + 1.0
    rows = await st.positions()
    assert len(rows) == 1 and float(rows[0]["size"]) == pytest.approx(want)
    assert all(o.status != "open" for o in fx.orders.values())
    assert len(await st.orders()) == 2


async def test_x2_exit_rejected_then_retried_cooldown_only_on_confirmed_exit(make_stack, fx, clock, plan):
    bars, idx = plan
    crash, cidx, pct = atr_crash(bars, bars[-1][3])
    feed = feed_for(fx, clock, crash, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    fx.reject_next(21)                                       # exit: 20 limit attempts + market all rejected
    await st.bar(cidx)
    assert fx.position is not None and st.trader.has_position
    assert st.trader.last_liquidation_ts == 0 and st.trader.atr_cooldown_until == 0   # not set yet
    assert st.notifier.of("exit") == []
    assert st.notifier.of("order_rejected")[-1]["side"] == "sell"
    await _drain_retry(st)
    assert fx.position is None
    (xn,) = st.notifier.of("exit")
    assert xn["reason"] == "atr_distance"
    assert st.trader.atr_cooldown_until == feed.candles[cidx]["ts"] + 14_400_000
    pos = (await st.positions())[0]
    assert pos["closed_at"] is not None and pos["close_reason"] == "atr_distance"


async def test_x3_exit_with_no_fills_falls_back_to_market(make_stack, fx, clock, plan):
    bars, idx = plan
    crash, cidx, pct = atr_crash(bars, bars[-1][3])
    feed = feed_for(fx, clock, crash, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    size = fx.position.size
    fx.fill_limits("never")
    await st.bar(cidx)
    exits = [o for o in fx.placed if o.side == "sell"]
    assert len([o for o in exits if o.order_type == "limit"]) == 20
    (m,) = [o for o in exits if o.order_type == "market"]
    assert m.reduce_only and m.quantity == pytest.approx(size)
    assert fx.position is None and fx.position_stop_loss is None
    assert all(o.status != "open" for o in fx.orders.values())


async def test_x4_emergency_close_aborts_an_in_flight_entry(make_stack, fx, clock, plan, monkeypatch):
    bars, idx = plan
    monkeypatch.setattr(ex, "REPEG_INTERVAL_S", 0.4)         # keep the entry in flight long enough
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.plan_fills(("partial", 0.5))
    fx.fill_limits("never")
    task = asyncio.create_task(st.bar(idx))
    for _ in range(200):
        await asyncio.sleep(0.01)
        if fx.position is not None or fx.placed:
            break
    await asyncio.sleep(0.1)
    assert not task.done()
    res = await st.app.emergency_close("test")
    await asyncio.wait_for(task, 20)
    await st.flush()
    assert res["flat"] is True
    assert fx.position is None, "entry kept working after the emergency close"
    assert all(o.status != "open" for o in fx.orders.values())
    buys_after = [o for o in fx.placed if o.side == "buy" and o.order_type == "market"]
    assert buys_after == []                                  # no market fallback after the abort
    assert st.state.entries_blocked()[0]
    t = st.trader._retry_task
    assert t is None or t.done()
    rows = await st.positions()
    assert all(r["closed_at"] is not None for r in rows)
    assert not st.trader.has_position


async def test_x5_crash_recovery_cancels_orphan_resting_order_and_adopts_partial_fill(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st0, feed = await _ready(make_stack, fx, clock, bars, idx)
    await st0.app.shutdown()
    # "crashed" process left: a resting limit buy that partially filled, its orders row non-terminal
    fx.plan_fills(("partial", 0.5))
    req = OrderRequest(strategy_id="supertrend-01", exchange="bybit", symbol="BTC/USDT:USDT", side="buy",
                       order_type="limit", quantity=0.04, price=fx._bid, post_only=True, reduce_only=False)
    res = await fx.place_order(req)
    await fx.fetch_order_status(res.order_id, "BTC/USDT:USDT")     # one poll: partial fill happens
    async with db.pool.acquire() as c:
        await c.execute(
            "INSERT INTO orders (request_id, order_id, exchange, symbol, side, order_type, quantity, price, "
            "status, strategy_id, post_only, reduce_only) VALUES ($1,$2,'bybit','BTC/USDT:USDT','buy','limit',"
            "0.04,$3,'submitted','supertrend-01',true,false)", req.request_id, res.order_id, fx._bid)
        await c.execute("INSERT INTO orders (request_id, exchange, symbol, side, order_type, quantity, status) "
                        "VALUES ('never-sent','bybit','BTC/USDT:USDT','buy','limit',0.04,'pending')")
    partial = fx.orders[res.order_id].filled
    assert 0 < partial < 0.04 and fx.orders[res.order_id].status == "open"
    st = await make_stack(feed=feed)
    placed_before = len(fx.placed)
    await st.app.start()
    assert fx.orders[res.order_id].status == "canceled"
    assert len(fx.placed) == placed_before                     # nothing re-sent
    by_req = {o["request_id"]: o for o in await st.orders()}
    assert by_req[req.request_id]["status"] == "partially_filled"
    assert by_req[req.request_id]["filled_qty"] == pytest.approx(partial)
    assert by_req["never-sent"]["status"] == "rejected"
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(partial)
    rows = await st.positions()
    assert len(rows) == 1 and float(rows[0]["size"]) == pytest.approx(partial)


async def test_x6_halt_whose_db_write_failed_survives_the_next_state_refresh(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    pool = FlakyPool(db.pool)
    db._pool = pool
    pool.down = True
    with pytest.raises(Exception):
        await st.state.set(MANUAL_HALT, {"active": True, "by": "t", "reason": "r", "at": clock.now().isoformat()})
    assert st.state.entries_blocked()[0]                       # in force in memory (as documented)
    pool.down = False
    await st.app.tick()                                        # state.load() from Postgres
    assert st.state.entries_blocked()[0], "manual halt vanished after the next scheduler tick"


async def test_x7_a_full_day_of_scheduler_housekeeping_report_and_bars(make_stack, db, fx, clock, plan):
    bars, idx = plan
    full = extend(bars, 0.004, 8)
    feed = feed_for(fx, clock, full, idx - 2)
    st = await make_stack(feed=feed)
    # start a bit before a UTC midnight so that one daily report falls inside the window
    from datetime import datetime, timezone
    day = (st.clock.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start = day - timedelta(hours=3, minutes=1)
    clock.set(start)
    feed.publish(clock.now())
    # candles are positioned relative to the clock fixture, publish handles visibility
    hk_calls = []
    reports = []
    real_hk = st.app.housekeeping

    async def spy_hk():
        hk_calls.append(clock.now())
        await real_hk()

    st.app.housekeeping = spy_hk
    real_daily = st.app.daily_report

    async def spy_report():
        reports.append(clock.now())
        await real_daily()

    st.app.daily_report = spy_report
    # candle feed must cover the window: extend visible time by shifting nothing, bars past the feed just stop
    stop = start + timedelta(hours=24)
    n_wakes = {"n": 0}

    def hook(c):
        feed.publish(c.now())
        n_wakes["n"] += 1
        if c.now() >= stop:
            st.app.request_stop()

    clock.on_sleep = hook
    await asyncio.wait_for(st.app.run(), 300)
    gaps = [b - a for a, b in zip(hk_calls, hk_calls[1:])]
    assert all(timedelta(minutes=14, seconds=59) <= g <= timedelta(minutes=16) for g in gaps), gaps[:5]
    assert 90 <= len(hk_calls) <= 98                          # every 15 min for 24 h
    assert len(reports) == 1 and reports[0] == day
    sends = [c for c in st.notifier.of("send") if "일일 리포트" in c["html"]]
    assert len(sends) == 1 and sends[0]["dedup_key"] == f"daily_report:{day:%Y-%m-%d}"
    assert max(clock.sleeps) <= 60.0 + 1e-6                   # heartbeat gap bound
    bs = await st.bot_state()
    assert bs["heartbeat"]["at"]
    snaps = await st.snapshots()
    assert 20 <= len(snaps) <= 26                             # hourly snapshots (+ forced ones after fills)
    assert st.notifier.anomalies() == []
