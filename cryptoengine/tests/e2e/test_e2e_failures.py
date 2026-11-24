"""Scenarios 13-14: exchange / database failures, dry-run mode."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from bot.interfaces import ExecutionResult
from e2e_harness import FlakyPool, approx_qty, entry_plan, extend, feed_for
from shared.models.order import OrderResult

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


# ── exchange failures ───────────────────────────────────────────────────────


async def test_s13a_get_position_down_at_bar_never_enters_blind_then_recovers(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.fail_next("get_position", 3)                       # the 3 pre-signal reads all fail
    await st.bar(idx)
    assert fx.position is None and fx.placed == []         # never open a second position blind
    assert not st.trader.has_position
    assert len(st.notifier.anomalies("position_sync_error")) == 1
    assert len(st.notifier.anomalies("entry_skipped_unverified_position")) == 1
    assert st.trader._retry_task is not None
    await _drain_retry(st)                                  # exchange is back: same-bar retry enters
    assert fx.position is not None
    assert fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx]["close"]))
    assert len(await st.positions()) == 1 and (await st.positions())[0]["closed_at"] is None
    sigs = await st.signals()
    assert sum(1 for s in sigs if s["ts_ms"] == feed.candles[idx]["ts"]) == 1
    assert len(st.notifier.of("entry")) == 1


async def test_s13b_get_balance_down_at_first_bar_defers_entry_without_corruption(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx)
    st = await make_stack(feed=feed)
    await st.app.start()
    fx.fail_next("get_balance", 1)                         # no balance was ever read yet -> nothing to size with
    await st.bar(idx)
    assert fx.position is None and fx.placed == []
    assert len(st.notifier.anomalies("entry_skipped_no_balance")) == 1
    await _drain_retry(st)
    assert fx.position is not None
    assert fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx]["close"]))
    assert len(st.notifier.of("entry")) == 1
    assert len(await st.orders()) == 1


async def test_s13b2_get_balance_blip_after_a_good_read_sizes_with_last_known_wallet(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)      # bar idx-1 read the balance
    fx.fail_next("get_balance", 1)
    await st.bar(idx)
    assert fx.position is not None
    assert fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx]["close"]))
    assert len(await st.orders()) == 1 and len(st.notifier.of("entry")) == 1


async def test_s13c_place_order_raising_for_a_while_no_phantom_position(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.fail_next("place_order", 23, ConnectionError("network down"))   # 20 limit attempts + 3 market tries
    await st.bar(idx)
    assert fx.position is None and not st.trader.has_position
    assert [o.status for o in fx.orders.values()] == []                 # nothing ever reached the book
    (o,) = await st.orders()
    assert o["status"] == "rejected"
    assert (await st.positions()) == []
    assert st.notifier.of("entry") == []
    await _drain_retry(st)
    assert fx.position is not None and len(st.notifier.of("entry")) == 1
    assert len(await st.positions()) == 1


async def test_s13d_ambiguous_place_order_timeout_leaves_no_orphan_order(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    real = fx.place_order
    state = {"n": 0}

    async def accept_then_report_rejected(order):
        state["n"] += 1
        res = await real(order)
        if state["n"] == 1:                                  # ccxt timeout after the exchange accepted it
            fx.last_error = "RequestTimeout: timed out"
            fx.last_error_ambiguous = True
            return OrderResult(request_id=order.request_id, order_id="", status="rejected")
        return res

    fx.place_order = accept_then_report_rejected             # type: ignore[assignment]
    await st.bar(idx)
    await st.flush()
    want = approx_qty(1000, feed.candles[idx]["close"])
    open_orders = [o for o in fx.orders.values() if o.status == "open"]
    # every resting order must be one the bot is tracking; here the first one is orphaned
    assert open_orders == [], f"orphan open orders on the exchange: {[(o.id, o.qty) for o in open_orders]}"
    assert fx.position.size == pytest.approx(want)


async def test_s13e_housekeeping_exchange_down_alerts_once_and_recovers(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    await st.bar(idx)
    pos_before = fx.position.size
    fx.fail_next("get_balance", 5)
    for _ in range(5):
        clock.advance(900)
        await st.app.housekeeping()
        await st.flush()
    assert len(st.notifier.anomalies("housekeeping_failing")) == 1       # on the 3rd failure only
    assert st.trader.has_position and fx.position.size == pos_before
    snaps_before = len(await st.snapshots())
    clock.advance(900)
    await st.app.housekeeping()                                          # recovered
    assert st.app._hk_failures == 0 and len(await st.snapshots()) >= snaps_before
    fx.fail_next("get_position", 1)
    clock.advance(900)
    await st.app.housekeeping()
    assert st.trader.has_position and (await st.positions())[0]["closed_at"] is None


# ── database failures ───────────────────────────────────────────────────────


async def test_s13f_db_down_during_on_bar_does_not_lose_the_order_row(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    pool = FlakyPool(db.pool)
    db._pool = pool

    real_place = fx.place_order

    async def place_and_go_down(order):
        pool.down = True                                    # DB disappears while the order is working...
        res = await real_place(order)
        return res

    fx.place_order = place_and_go_down                       # type: ignore[assignment]
    await st.bar(idx)
    pool.down = False
    # exchange truth is fine
    assert fx.position is not None
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(fx.position.size)
    # eventual consistency of the positions row (next sync / housekeeping repairs it)
    clock.advance(900)
    await st.app.housekeeping()
    rows = await st.positions()
    assert len(rows) == 1 and rows[0]["closed_at"] is None
    # the order result must not be lost
    orders = await st.orders()
    assert len(orders) == 1 and orders[0]["status"] == "filled"
    assert orders[0]["filled_qty"] == pytest.approx(fx.position.size)


async def test_s13g_db_down_for_signal_and_position_rows_is_repaired(make_stack, db, fx, clock, plan):
    """DB down across the whole on_bar (before any order row is written): exchange + in-memory state
    stay right and the positions row is repaired by the next housekeeping."""
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    pool = FlakyPool(db.pool)
    db._pool = pool
    pool.down = True
    try:
        await st.bar(idx)                       # tick(): state.load() fails, on_bar runs with a dead DB
    except Exception as exc:                    # the scheduler guard must absorb everything
        pytest.fail(f"scheduler tick raised with the DB down: {exc!r}")
    pool.down = False
    assert pool.failures > 0
    assert fx.position is not None and len(fx.placed) == 1               # entered exactly once
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(fx.position.size)
    assert len(st.notifier.of("entry")) == 1
    clock.advance(900)
    await st.app.housekeeping()
    rows = await st.positions()
    assert len(rows) == 1 and rows[0]["closed_at"] is None
    assert float(rows[0]["size"]) == pytest.approx(fx.position.size)
    # next bar: no duplicate entry
    await st.bar(idx + 1)
    assert len(fx.placed) == 1 and len(await st.positions()) == 1


async def test_s13g2_db_down_for_whole_bar_order_row_exists_after_recovery(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    pool = FlakyPool(db.pool)
    db._pool = pool
    pool.down = True
    await st.bar(idx)
    pool.down = False
    assert fx.position is not None
    # the unwritten order row is recreated by the next executor activity (housekeeping reads the position)
    clock.advance(900)
    await st.app.housekeeping()
    orders = await st.orders()
    assert len(orders) == 1 and orders[0]["status"] == "filled"
    assert orders[0]["filled_qty"] == pytest.approx(fx.position.size)
    assert orders[0]["side"] == "buy" and orders[0]["reduce_only"] is False


async def test_s13h_emergency_close_works_with_database_down(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    await st.bar(idx)
    assert fx.position is not None
    pool = FlakyPool(db.pool)
    db._pool = pool
    pool.down = True
    upd, q = await st.telegram_emergency_close()
    pool.down = False
    assert fx.position is None, q.edits


async def test_s13i_kill_switch_closes_even_if_persisting_the_block_fails(make_stack, db, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    await st.bar(idx)
    assert fx.position is not None
    real_set = st.state.set

    async def flaky_set(key, value):
        if key == "portfolio_block_until":
            raise ConnectionError("db blip")
        await real_set(key, value)

    st.state.set = flaky_set                                  # type: ignore[assignment]
    clock.advance(900)
    fx.set_balance(wallet=fx.wallet * 0.85)
    await st.app.housekeeping()
    await st.flush()
    assert fx.position is None, "kill switch triggered but the position is still open"


# ── dry run ─────────────────────────────────────────────────────────────────


async def test_s14_dry_run_sends_nothing_and_does_not_spam(make_stack, db, fx, clock, plan):
    bars, idx = plan
    full = extend(bars, 0.012, 6)
    feed = feed_for(fx, clock, full, idx - 1)
    st = await make_stack(feed=feed, dry_run=True)
    await st.app.start()
    await st.bar(idx - 1)
    marker = st.notifier.mark()
    for j in range(idx, idx + 6):                             # six consecutive entry signals
        await st.bar(j)
        await st.flush()
    assert fx.placed == [] and fx.orders == {} and fx.position is None
    assert await st.orders() == [] and await st.positions() == []
    assert st.trader._retry_task is None or st.trader._retry_task.done()
    new = [c for c in st.notifier.calls[marker:]
           if not (c[0] == "send" and "일일 리포트" in c[1]["html"])]   # the scheduled midnight report is fine
    assert new == [], new                                      # no alert, no rejection message
    assert len(await st.signals()) == 7                        # signals are still computed and stored
    assert any(s["entry_ok"] for s in await st.signals())
    assert 60.0 not in clock.sleeps                            # no same-bar retry was ever scheduled


async def test_s14b_dry_run_exit_and_emergency_close_do_not_hit_exchange(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed, dry_run=True)
    fx.set_position("long", 0.03, 60_000.0)
    await st.app.start()
    assert st.trader.has_position
    n_calls = len([c for c in fx.calls if c[0] in ("place_order", "cancel_order")])
    res = await st.app.emergency_close("test")
    assert res["flat"] is False                                # honest: nothing was closed
    assert len([c for c in fx.calls if c[0] in ("place_order", "cancel_order")]) == n_calls
    assert fx.position is not None


async def test_s13j_status_command_survives_exchange_outage(make_stack, fx, clock, plan):
    from e2e_harness import cmd_update

    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx, extra=2)
    await st.bar(idx)
    fx.fail_next("get_position", 1)
    fx.fail_next("get_balance", 1)
    upd = cmd_update()
    await st.commands.cmd_status(upd)
    t = upd.effective_message.sent[-1][0]
    assert "현재 상태" in t and "LONG" in t and "잔고" in t        # falls back to the bot's own belief
