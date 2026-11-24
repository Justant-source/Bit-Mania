"""Spec items not covered elsewhere (bar storage, replay sync isolation, heartbeat file, commands)
plus report/dashboard behaviour after an exchange-side close."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from bot.interfaces import BAR_MS
from bot.report import build_daily_report, pair_trades as report_pairs
from bot.web import pair_trades as web_pairs
from e2e_harness import (
    buttons, cb_update, cmd_update, entry_plan, extend, feed_for,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def test_c1_confirmed_bars_are_stored_and_in_progress_bar_is_not(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    rows = await st.fetch("SELECT exchange, symbol, timeframe, (EXTRACT(EPOCH FROM timestamp)*1000)::BIGINT AS ms, "
                          "open, high, low, close, volume FROM ohlcv_history ORDER BY timestamp")
    assert {(r["exchange"], r["symbol"], r["timeframe"]) for r in rows} == {("bybit", "BTCUSDT", "4h")}
    confirmed = [c for c in feed.candles if c["ts"] + BAR_MS <= clock.now().timestamp() * 1000]
    assert [r["ms"] for r in rows] == [c["ts"] for c in confirmed][-1000:]      # no in-progress bar, no gap
    last = rows[-1]
    assert float(last["close"]) == pytest.approx(feed.candles[idx]["close"])
    assert float(last["high"]) == pytest.approx(feed.candles[idx]["high"])


async def test_c2_replay_sync_runs_after_each_bar_and_never_breaks_trading(make_stack, fx, clock, plan):
    bars, idx = plan
    calls = []

    async def failing_sync(db):
        calls.append(clock.now())
        raise RuntimeError("binance down")

    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed, replay_sync=failing_sync)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    assert len(calls) == 2                                   # once per processed bar
    assert fx.position is not None and len(await st.signals()) == 2
    assert any(a["kind"] == "replay_sync_error" for a in st.notifier.anomalies())


async def test_c3_heartbeat_file_and_state(make_stack, fx, clock, plan, tmp_path):
    bars, idx = plan
    hb = tmp_path / "heartbeat_ok"
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed, heartbeat_path=str(hb))
    await st.app.start()
    await st.app.tick()
    assert hb.exists()
    m1 = hb.stat().st_mtime_ns
    await asyncio.sleep(0.02)
    clock.advance(30)
    await st.app.tick()
    assert hb.stat().st_mtime_ns > m1
    bs = await st.bot_state()
    assert bs["heartbeat"]["at"] == clock.now().isoformat()


async def test_c4_kill_state_persists_all_documented_fields(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    fx.set_balance(wallet=fx.wallet * 0.85)
    clock.advance(900)
    await st.app.housekeeping()
    bs = await st.bot_state()
    ks = bs["kill_switch"]
    assert set(ks) >= {"level", "reason", "triggered_at", "alert_only"}
    assert ks["level"] == 2 and "Daily drawdown" in ks["reason"] and ks["triggered_at"]
    blk = bs["portfolio_block_until"]
    from bot.state import parse_ts
    assert parse_ts(blk["until"]) == parse_ts(ks["triggered_at"]) + timedelta(minutes=60)


async def test_c5_help_dashboard_commands_and_authorisation(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    for chat in ("999",):
        for fn in (st.commands.cmd_help, st.commands.cmd_status, st.commands.cmd_report,
                   st.commands.cmd_dashboard, st.commands.cmd_resume, st.commands.cmd_emergency_close):
            u = cmd_update(chat=chat)
            await fn(u)
            assert u.effective_message.sent == [], fn.__name__
    u = cmd_update()
    await st.commands.cmd_help(u)
    assert "/emergency_close" in u.effective_message.sent[-1][0]
    u = cmd_update()
    await st.commands.cmd_dashboard(u)
    kb = u.effective_message.sent[-1][1]["reply_markup"].to_dict()["inline_keyboard"][0][0]
    assert kb["url"].startswith("https://dash.example/") and "t=" in kb["url"]
    u = cmd_update()
    await st.commands.cmd_resume(u)                         # nothing is halted
    assert "중단 상태가 아닙니다" in u.effective_message.sent[-1][0]
    # a forged / unknown callback token does nothing
    cb, q = cb_update("ec:yes:deadbeef")
    await st.commands.on_callback(cb)
    assert "만료" in q.edits[-1] and fx.placed == []


# ── report / dashboard after an exchange-side close ────────────────────────


async def test_c6_report_and_dashboard_pairing_after_exchange_side_stop(make_stack, fx, clock, plan):
    bars, idx = plan
    st_feed = feed_for(fx, clock, extend(bars, 0.012, 3), idx - 1)
    st = await make_stack(feed=st_feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    first_entry = fx.position.entry_price
    fx.trigger_stop_loss()                                   # exchange-side stop between bars
    clock.advance(900)
    await st.app.housekeeping()
    assert fx.position is None
    st.app._next_house_at = clock.now() + timedelta(days=1)
    await st.bar(idx + 1)                                    # bot re-enters (no cooldown for external closes)
    assert fx.position is not None
    second_entry = fx.position.entry_price
    assert second_entry != pytest.approx(first_entry)
    await st.telegram_emergency_close()                      # bot-side exit of the 2nd position
    assert fx.position is None
    orders = await st.orders("status='filled'")
    positions = await st.positions()
    trades = report_pairs(orders, positions)
    assert len(trades) == 2, "completed cycle missing from the report pairing"
    first, second = trades                                   # first cycle closed by the exchange-side stop
    assert first.entry_avg == pytest.approx(first_entry) and first.qty > 0
    assert first.exit_avg is None and first.pnl is None and first.exit_reason == "exchange_side"
    assert second.entry_avg == pytest.approx(second_entry) and second.pnl is not None
    wtrades = web_pairs([dict(o) for o in orders], None, [dict(p) for p in positions])
    assert len(wtrades) == 2
    wsecond, wfirst = wtrades[1], wtrades[0]
    assert wfirst["entry_price"] == pytest.approx(first_entry)
    assert wfirst["exit_price"] is None and wfirst["pnl"] is None and wfirst["pnl_pct"] is None
    assert wsecond["entry_price"] == pytest.approx(second_entry) and wsecond["pnl"] is not None
    assert wfirst["qty"] == pytest.approx(first.qty) and wsecond["qty"] == pytest.approx(second.qty)


async def test_c7x_restart_just_after_boundary_with_bar_not_yet_served(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 1)
    await st1.app.shutdown()
    close = st1.bar_close_time(idx)
    feed.delay[feed.candles[idx]["ts"]] = 40.0               # REST serves the closed bar 40 s late
    clock.set(close + timedelta(seconds=1))
    feed.publish(clock.now())
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    from e2e_harness import floor_boundary  # noqa: F401
    st2.clock.set(close + timedelta(minutes=3))
    st2.publish()
    await st2.app.tick()
    for _ in range(3):
        clock.advance(60)
        st2.publish()
        await st2.app.tick()
    ts = [s["ts_ms"] for s in await st2.signals()]
    assert feed.candles[idx]["ts"] in ts, "bar that closed while the bot was restarting was never evaluated"
