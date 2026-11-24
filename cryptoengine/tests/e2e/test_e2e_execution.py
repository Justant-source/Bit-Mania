"""Scenarios 3-5: re-peg / market fallback / partial fills, same-bar retry, capital rules."""

from __future__ import annotations

import asyncio

import pytest

from bot.exchange import PostOnlyRejected
from e2e_harness import approx_qty, entry_plan, evaluate_at, extend, feed_for
from shared.models.market import OrderBook, OrderBookLevel

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def _ready(make_stack, fx, clock, bars, idx, **kw):
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed, **kw)
    await st.app.start()
    await st.bar(idx - 1)
    return st, feed


async def test_s3a_post_only_rejected_repegs_until_accepted(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.post_only_reject_next(2)
    await st.bar(idx)
    limits = [o for o in fx.placed if o.order_type == "limit"]
    assert len(limits) == 3 and len(fx.placed) == 3          # 2 rejected + 1 accepted, no market
    assert fx.position is not None and fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx]["close"]))
    (o,) = await st.orders()
    assert o["status"] == "filled" and o["filled_qty"] == pytest.approx(fx.position.size)
    assert len(st.notifier.of("entry")) == 1
    assert st.notifier.of("order_rejected") == []             # re-pegs are internal, no operator noise
    assert st.notifier.anomalies() == []


async def test_s3b_crossing_stale_peg_is_repegged(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    real = fx.get_orderbook
    calls = {"n": 0}

    async def stale_then_fresh(symbol, limit=25):
        calls["n"] += 1
        book = await real(symbol, limit)
        if calls["n"] == 1:  # first look at the book is stale: bid above the real ask -> crosses
            ask = book.asks[0].price
            return OrderBook(exchange="bybit", symbol=symbol,
                             bids=[OrderBookLevel(price=ask + 5, quantity=1.0)], asks=book.asks)
        return book

    fx.get_orderbook = stale_then_fresh  # type: ignore[assignment]
    await st.bar(idx)
    limits = [o for o in fx.placed if o.order_type == "limit"]
    assert len(limits) == 2
    assert limits[0].price > limits[1].price                  # crossing peg, then the correct best bid
    assert limits[1].price == pytest.approx(round(feed.candles[idx]["close"], 1))
    assert fx.position is not None and len(await st.orders()) == 1


async def test_s3c_no_fill_for_20_repegs_then_market_fallback(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.fill_limits("never")
    await st.bar(idx)
    limits = [o for o in fx.placed if o.order_type == "limit"]
    markets = [o for o in fx.placed if o.order_type == "market"]
    assert len(limits) == 20 and len(markets) == 1
    assert len(fx.cancelled) == 20                              # every resting limit was cancelled
    assert all(o.status != "open" for o in fx.orders.values())
    want = approx_qty(1000, feed.candles[idx]["close"])
    assert markets[0].quantity == pytest.approx(want) and markets[0].reduce_only is False
    assert markets[0].stop_loss == pytest.approx(feed.candles[idx]["close"] * (1 - 0.7 / 3))
    assert fx.position.size == pytest.approx(want)
    assert fx.position_stop_loss == pytest.approx(feed.candles[idx]["close"] * (1 - 0.7 / 3))
    (o,) = await st.orders()
    assert o["status"] == "filled" and o["filled_qty"] == pytest.approx(want)
    assert o["filled_price"] == pytest.approx(fx._ask)           # market buy at the ask
    (e,) = st.notifier.of("entry")
    assert e["qty"] == pytest.approx(want)
    assert float((await st.positions())[0]["size"]) == pytest.approx(want)


async def test_s3d_partial_fill_then_fallback_is_one_combined_result(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.plan_fills(("partial", 0.5))
    fx.fill_limits("never")
    await st.bar(idx)
    want = approx_qty(1000, feed.candles[idx]["close"])
    limits = [o for o in fx.placed if o.order_type == "limit"]
    assert len(limits) == 20
    # re-pegs after the partial only asked for the REMAINDER (never the full size again)
    assert limits[0].quantity == pytest.approx(want)
    assert all(l.quantity < want for l in limits[1:])
    (m,) = [o for o in fx.placed if o.order_type == "market"]
    filled_limit = sum(o.filled for o in fx.orders.values() if o.type == "limit")
    assert filled_limit > 0
    assert fx.position.size == pytest.approx(filled_limit + m.quantity)
    assert fx.position.size == pytest.approx(want)
    (o,) = await st.orders()
    assert o["status"] == "filled" and o["filled_qty"] == pytest.approx(fx.position.size)
    (e,) = st.notifier.of("entry")
    assert e["qty"] == pytest.approx(fx.position.size)           # one notification, total size
    (p,) = await st.positions()
    assert float(p["size"]) == pytest.approx(fx.position.size) and p["closed_at"] is None
    assert st.trader.position_qty == pytest.approx(fx.position.size)


async def test_s4_rejected_entry_retries_every_60s_until_filled(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    fx.reject_next(21)                                       # 20 limit attempts + the market fallback
    await st.bar(idx)
    assert fx.position is None and not st.trader.has_position
    (rej,) = st.notifier.of("order_rejected")
    assert rej["side"] == "buy" and rej["will_retry"] is True
    (o1,) = await st.orders()
    assert o1["status"] == "rejected"
    task = st.trader._retry_task
    assert task is not None and not task.done()

    async def retry():
        await asyncio.sleep(0)
        t = st.trader._retry_task
        st.clock.release_retry()
        await t

    t_before = clock.now()
    fx.reject_next(21)                                       # second attempt also rejected
    await retry()
    assert fx.position is None
    assert 60.0 in clock.sleeps
    assert (clock.now() - t_before).total_seconds() >= 60
    assert len(await st.orders()) == 2
    await retry()                                            # third attempt: exchange healthy
    assert fx.position is not None
    assert fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx]["close"]))
    orders = await st.orders()
    assert [o["status"] for o in orders] == ["rejected", "rejected", "filled"]
    assert len({o["request_id"] for o in orders}) == 3
    assert len(st.notifier.of("entry")) == 1
    assert len(await st.positions()) == 1
    # no further retry is scheduled once filled (dust remainder ignored)
    await asyncio.sleep(0)
    t = st.trader._retry_task
    assert t is None or t.done()
    placed_before = len(fx.placed)
    st.clock.release_retry(3)
    await asyncio.sleep(0.05)
    assert len(fx.placed) == placed_before


async def test_s4_retries_stop_at_next_bar(make_stack, fx, clock, plan):
    bars, idx = plan
    nxt = extend(bars, 0.012)
    assert evaluate_at(nxt, has_position=False).entry_ok         # signal still stands next bar
    st, feed = await _ready(make_stack, fx, clock, nxt, idx)
    fx.reject_next(21)
    await st.bar(idx)
    assert fx.position is None and st.trader._retry_task is not None
    old_task = st.trader._retry_task
    # healthy exchange; the NEXT bar arrives before the 60 s retry was allowed to fire
    await st.bar(idx + 1)
    assert fx.position is not None                                 # the new bar's own entry filled
    assert len(await st.positions()) == 1
    placed_before = len(fx.placed)
    st.clock.release_retry(5)                                      # the old retry must be dead
    await asyncio.sleep(0.05)
    assert old_task.done()
    assert len(fx.placed) == placed_before
    assert fx.position.size == pytest.approx(approx_qty(1000, feed.candles[idx + 1]["close"]))
    filled = [o for o in await st.orders() if o["status"] == "filled"]
    assert len(filled) == 1


async def test_s5a_wallet_above_max_capital_is_capped_at_1000(make_stack, fx, clock, plan):
    bars, idx = plan
    fx.set_balance(wallet=5000.0)
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    await st.bar(idx)
    close = feed.candles[idx]["close"]
    assert fx.position.size == pytest.approx(approx_qty(1000.0, close))
    assert fx.position.size * fx.position.entry_price <= 1000 * 0.95 * 3 + 1e-6
    sig = (await st.signals())[-1]
    assert sig["allocated_capital"] == pytest.approx(1000.0)
    assert sig["expected_qty"] == pytest.approx(1000 * 0.95 * 3 / close)


async def test_s5b_tiny_balance_skips_entry_without_error(make_stack, fx, clock, plan):
    bars, idx = plan
    fx.set_balance(wallet=20.0)                                    # 20*0.95*3 = 57 USD notional < 65
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    await st.bar(idx)
    await st.flush()
    assert fx.placed == [] and fx.position is None
    assert await st.orders() == []
    assert st.notifier.anomalies() == [] and st.notifier.of("order_rejected") == []
    assert st.trader._retry_task is None
    sig = (await st.signals())[-1]                                 # bar still recorded
    assert sig["entry_ok"] is True and sig["allocated_capital"] == pytest.approx(20.0)


async def test_s5c_balance_between_notional_min_and_margin_min_characterisation(
        make_stack, fx, clock, plan):
    """CHARACTERISATION (not a spec violation, see report): a 30 USD wallet passes the trader's
    65 USD notional rule (85 USD) but SafetyGuard's 50 USD free-margin minimum rejects it, and
    that non-transient rejection is retried every 60 s for the rest of the bar (kill_switch and
    dry_run are the only no-retry reasons), one rejected `orders` row per retry."""
    bars, idx = plan
    fx.set_balance(wallet=30.0)
    st, feed = await _ready(make_stack, fx, clock, bars, idx)
    await st.bar(idx)
    (o,) = await st.orders()
    assert o["status"] == "rejected"
    for _ in range(3):
        t = st.trader._retry_task
        st.clock.release_retry()
        await t
    assert len(await st.orders()) == 4
    assert all(r["will_retry"] for r in st.notifier.of("order_rejected"))
