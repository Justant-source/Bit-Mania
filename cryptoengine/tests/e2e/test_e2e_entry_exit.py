"""Scenarios 1-2: cold start -> entry; exits by ema_cross and atr_distance."""

from __future__ import annotations

import pytest

from e2e_harness import (
    approx_qty, atr_crash, ema_exit_tail, entry_plan, evaluate_at, extend, feed_for,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def _enter(make_stack, fx, clock, bars, idx, **kw):
    feed = feed_for(fx, clock, bars, idx - 2)
    st = await make_stack(feed=feed, **kw)
    await st.app.start()
    await st.bar(idx - 2)
    await st.bar(idx - 1)
    await st.bar(idx)
    return st, feed


async def test_s1_cold_start_flat_then_entry(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 2)
    st = await make_stack(feed=feed)
    await st.app.start()

    # --- cold start: flat, exchange configured, nothing evaluated yet
    assert fx.margin_calls == ["isolated"]
    assert fx.leverage_calls == [3]
    assert fx.connected
    assert not st.trader.has_position and fx.position is None
    assert await st.signals() == []
    assert any("봇 시작" in t for t in st.notifier.texts())
    assert st.notifier.of("entry") == []

    # --- two bars pass with no signal: signal rows are written, no orders
    await st.bar(idx - 2)
    await st.bar(idx - 1)
    sigs = await st.signals()
    assert [s["ts_ms"] for s in sigs] == [feed.candles[idx - 2]["ts"], feed.candles[idx - 1]["ts"]]
    assert all(not s["entry_ok"] and s["expected_action"] == "hold" for s in sigs)
    assert fx.placed == [] and await st.orders() == []
    assert await st.positions() == []

    # --- entry bar
    await st.bar(idx)
    close = feed.candles[idx]["close"]
    sigs = await st.signals()
    assert len(sigs) == 3 and sigs[-1]["entry_ok"] and sigs[-1]["expected_action"] == "enter"
    assert sigs[-1]["allocated_capital"] == pytest.approx(1000.0)
    assert sigs[-1]["expected_qty"] == pytest.approx(1000 * 0.95 * 3 / close)
    assert sigs[-1]["expected_stop_loss"] == pytest.approx(close * (1 - 0.70 / 3))

    # post-only limit pegged to the best bid, reduce_only False, inline stop loss
    first = fx.placed[0]
    assert (first.side, first.order_type, first.post_only, first.reduce_only) == ("buy", "limit", True, False)
    assert first.price == pytest.approx(round(close, 1))          # best bid == close in the feed
    assert first.stop_loss == pytest.approx(close * (1 - 0.70 / 3))
    assert len(fx.placed) == 1

    # exchange state
    want_qty = approx_qty(1000.0, close)
    assert fx.position is not None and fx.position.side == "long"
    assert fx.position.size == pytest.approx(want_qty)
    assert fx.position.entry_price == pytest.approx(round(close, 1))
    assert fx.position_stop_loss == pytest.approx(close * (1 - 0.70 / 3))
    assert fx.leverage == 3 and fx.margin_calls == ["isolated"]
    # notional never above 95% x 3x of capital
    assert fx.position.size * fx.position.entry_price <= 1000 * 0.95 * 3 + 1e-6

    # orders row
    (o,) = await st.orders()
    assert (o["side"], o["status"], o["reduce_only"], o["post_only"]) == ("buy", "filled", False, True)
    assert o["filled_qty"] == pytest.approx(want_qty) and o["filled_price"] == pytest.approx(round(close, 1))
    assert o["quantity"] == pytest.approx(1000 * 0.95 * 3 / close)
    assert o["order_id"]
    assert int(o["original_signal_ts"].timestamp() * 1000) == feed.candles[idx]["ts"]

    # positions row
    (p,) = await st.positions()
    assert p["closed_at"] is None and p["side"] == "long"
    assert float(p["size"]) == pytest.approx(want_qty)
    assert float(p["entry_price"]) == pytest.approx(round(close, 1), abs=0.01)
    assert float(p["leverage"]) == 3.0

    # notification + snapshot
    (e,) = st.notifier.of("entry")
    assert e["qty"] == pytest.approx(want_qty)
    assert e["price"] == pytest.approx(round(close, 1))
    assert e["stop_loss"] == pytest.approx(close * (1 - 0.70 / 3))
    assert e["equity"] == pytest.approx(1000.0)
    snaps = await st.snapshots()
    assert snaps and float(snaps[-1]["total_equity"]) == pytest.approx(fx.wallet)
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(want_qty)
    assert st.notifier.anomalies() == []


async def test_s2a_exit_by_ema_cross(make_stack, fx, clock, plan):
    bars, idx = plan
    entry_px = bars[-1][3]
    # hold long enough that the EMA cross (not the ATR stop) fires first
    held = extend(bars, 0.006, 30)
    full, exit_idx, reason = ema_exit_tail(held, entry_px)
    assert reason == "ema_cross"
    feed = feed_for(fx, clock, full, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    assert st.trader.has_position
    entry_fill = fx.position.entry_price
    for j in range(idx + 1, exit_idx):
        await st.bar(j)
        assert fx.position is not None, f"closed early at {j}"
        assert evaluate_at(full[: j + 1], has_position=True, entry_price=entry_fill).exit_signal is False
    assert fx.position is not None and fx.position_stop_loss is not None

    await st.bar(exit_idx)
    assert fx.position is None
    assert fx.position_stop_loss is None  # no stale stop on the exchange
    exits = [o for o in await st.orders() if o["reduce_only"]]
    assert len(exits) == 1 and exits[0]["side"] == "sell" and exits[0]["status"] == "filled"
    assert exits[0]["post_only"] is True and exits[0]["order_type"] == "limit"
    pos_rows = await st.positions()
    assert len(pos_rows) == 1 and pos_rows[0]["closed_at"] is not None
    assert pos_rows[0]["close_reason"] == "ema_cross"
    (x,) = st.notifier.of("exit")
    assert x["reason"] == "ema_cross"
    qty = exits[0]["filled_qty"]
    exp_pnl = (exits[0]["filled_price"] - entry_fill) * qty - exits[0]["fee"]
    assert x["pnl"] == pytest.approx(exp_pnl)
    assert x["entry_price"] == pytest.approx(entry_fill)
    # no open orders left, cancel_conditional was consulted after the exit
    assert all(o.status != "open" for o in fx.orders.values())
    assert fx.cancel_conditional_calls
    sig = (await st.signals())[-1]
    assert sig["exit_signal"] and sig["exit_reason"] == "ema_cross"
    assert st.trader.last_liquidation_ts == feed.candles[exit_idx]["ts"]
    assert st.trader.atr_cooldown_until == 0  # ema exit sets no ATR cooldown
    bs = await st.bot_state()
    assert bs["last_liquidation_ts"] == feed.candles[exit_idx]["ts"]
    assert "atr_cooldown_until" not in bs


async def test_s2b_exit_by_atr_distance_downside_only(make_stack, fx, clock, plan):
    bars, idx = plan
    entry_px = bars[-1][3]
    crash, cidx, pct = atr_crash(bars, entry_px)
    assert cidx == idx + 1
    # the equally large UP move must not exit
    up = extend(bars, +pct)
    s_up = evaluate_at(up, has_position=True, entry_price=entry_px)
    assert not s_up.exit_signal

    # --- upside run: position stays open through a +pct bar
    feed_up = feed_for(fx, clock, up, idx - 1)
    st = await make_stack(feed=feed_up)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    assert fx.position is not None
    await st.bar(idx + 1)
    assert fx.position is not None and st.trader.has_position
    assert [o for o in await st.orders() if o["reduce_only"]] == []
    assert (await st.signals())[-1]["exit_signal"] is False
    await st.app.shutdown()


async def test_s2b_atr_exit_path(make_stack, fx, clock, plan):
    bars, idx = plan
    entry_px = bars[-1][3]
    crash, cidx, pct = atr_crash(bars, entry_px)
    feed = feed_for(fx, clock, crash, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    entry_fill = fx.position.entry_price
    await st.bar(cidx)
    assert fx.position is None and fx.position_stop_loss is None
    sig = (await st.signals())[-1]
    assert sig["exit_signal"] and sig["exit_reason"] == "atr_distance"
    (x,) = st.notifier.of("exit")
    assert x["reason"] == "atr_distance" and x["pnl"] < 0
    rows = await st.positions()
    assert rows[0]["closed_at"] is not None and rows[0]["close_reason"] == "atr_distance"
    # ATR exit => cooldown for the next bar
    assert st.trader.atr_cooldown_until == feed.candles[cidx]["ts"] + 14_400_000
    assert (await st.bot_state())["atr_cooldown_until"] == st.trader.atr_cooldown_until
    # signal at the next bar must be blocked even though entry conditions may hold
    from bot.strategy import evaluate
    nxt = extend(crash, 0.03)
    s = evaluate(__import__("e2e_harness").to_candles(nxt, feed.candles[0]["ts"]), has_position=False,
                 last_liquidation_ts=st.trader.last_liquidation_ts,
                 atr_cooldown_until=st.trader.atr_cooldown_until)
    assert not s.entry_ok
