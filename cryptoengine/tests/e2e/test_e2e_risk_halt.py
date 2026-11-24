"""Scenarios 9-12: portfolio kill switch (flat / in position), manual emergency close via the
Telegram handlers, housekeeping reconciliation."""

from __future__ import annotations

from datetime import timedelta

import pytest

from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL
from e2e_harness import approx_qty, entry_plan, evaluate_at, extend, feed_for, to_candles

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


async def hk(st, minutes: float = 15.0):
    st.clock.advance(minutes * 60)
    st.publish()
    await st.app.housekeeping()
    await st.flush()


async def in_position(make_stack, fx, clock, bars, idx, *, extra=3, **kw):
    full = extend(bars, 0.012, extra)
    feed = feed_for(fx, clock, full, idx - 1)
    st = await make_stack(feed=feed, **kw)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    assert fx.position is not None
    return st, feed


def kills(st):
    return st.notifier.of("kill_switch")


def sells(fx):
    return [o for o in fx.placed if o.side == "sell"]


# ── 9: PORTFOLIO kill switch while flat ─────────────────────────────────────


async def _stopped_out(make_stack, fx, clock, plan):
    """Enter, let the exchange stop fire 3h30 later (realised loss), run housekeeping at 3h45."""
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx)
    await hk(st, 5)                                           # first housekeeping: peaks = ~1000
    start_equity = fx.wallet
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=30))
    fx.trigger_stop_loss()
    assert fx.position is None and fx.wallet < start_equity * 0.5
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=45))
    st.publish()
    await st.app.housekeeping()
    await st.flush()
    return st, feed, start_equity


async def test_s9a_flat_alert_only_once_then_rebase_then_retrigger(make_stack, fx, clock, plan):
    st, feed, start_equity = await _stopped_out(make_stack, fx, clock, plan)
    trig_at = clock.now()
    # one alert, entries still allowed
    (k,) = kills(st)
    assert k["level"] == "PORTFOLIO" and "알림만" in k["action"]
    assert st.state.entries_blocked() == (False, "")
    assert st.risk.alert_only is True
    bs = await st.bot_state()
    assert bs["kill_switch"]["alert_only"] is True and bs["kill_switch"]["level"] == 2
    assert PORTFOLIO_BLOCK_UNTIL not in bs
    assert sells(fx) == []                                    # flat: nothing to close
    assert len(st.notifier.anomalies("position_vanished")) == 1

    # housekeeping every 15 min for the rest of the cooldown + 3 days: no second alert
    # (no new bar is published meanwhile: this is about the housekeeping loop alone)
    frozen = [c for c in feed.candles if c["ts"] + 14_400_000 <= clock.now().timestamp() * 1000]
    feed.visible_until = len(frozen)
    equity_flat = fx.wallet
    seen_rebase = None
    for i in range(1, 4 * 24 * 3 + 1):
        await hk(st, 15)
        assert st.state.entries_blocked()[0] is False
        if seen_rebase is None and st.risk.level.name == "NONE":
            seen_rebase = clock.now()
    assert len(kills(st)) == 1, [k["action"] for k in kills(st)]
    assert seen_rebase is not None
    assert timedelta(minutes=60) <= seen_rebase - trig_at <= timedelta(minutes=75)
    assert st.risk.alert_only is False
    peaks = st.risk.peaks
    assert peaks["daily"] == pytest.approx(equity_flat) and peaks["weekly"] == pytest.approx(equity_flat)
    assert peaks["monthly"] == pytest.approx(equity_flat)
    bs = await st.bot_state()
    assert "kill_switch" not in bs and "peak_rebase_at" in bs
    assert (st.risk.daily_dd, st.risk.weekly_dd, st.risk.monthly_dd) == (0.0, 0.0, 0.0)

    # a restart after the rebase must not resurrect the old loss
    await st.app.shutdown()
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    await hk(st2, 15)
    assert st2.risk.peaks["monthly"] == pytest.approx(equity_flat)
    assert kills(st2) == []

    # a NEW loss after the rebase triggers again (once)
    fx.set_balance(wallet=equity_flat - 0.12 * equity_flat)
    await hk(st2, 15)
    assert len(kills(st2)) == 1
    for _ in range(3):                                        # still inside the 60 min cooldown
        await hk(st2, 15)
    assert len(kills(st2)) == 1
    assert st2.risk.level.name == "PORTFOLIO"
    for _ in range(4):                                        # cooldown over: rebased again, no re-trigger
        await hk(st2, 15)
    assert len(kills(st2)) == 1 and st2.risk.level.name == "NONE"


async def test_s9b_position_opened_while_alert_only_is_promoted_to_a_halt(make_stack, fx, clock, plan):
    """INTENDED (old system's behaviour): PORTFOLIO while flat = alert only and entries stay allowed,
    but a position that appears while the alert-only switch is still active is closed by the next
    housekeeping and entries are blocked until the cooldown ends."""
    bars, idx = plan
    full = extend(bars, 0.012, 3)
    assert evaluate_at(full[: idx + 2], has_position=False).entry_ok
    st, feed = await in_position(make_stack, fx, clock, bars, idx)
    await hk(st, 5)
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=40))
    fx.trigger_stop_loss()
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=50))
    st.publish()
    await st.app.housekeeping()                               # alert-only trigger, cooldown until +4h50
    assert len(kills(st)) == 1 and st.risk.alert_only
    assert st.state.entries_blocked()[0] is False             # flat: entries stay allowed
    st.app._next_house_at = clock.now() + timedelta(days=1)    # keep housekeeping out of this tick
    await st.bar(idx + 1)                                     # boundary +3 s: inside the cooldown
    assert fx.position is not None, "entry must be allowed while alert-only"
    n_orders = len(fx.placed)
    assert sells(fx) == []
    await hk(st, 15)                                          # next housekeeping, still in cooldown
    # promotion: the position is closed (reduce-only market sell) and entries are blocked
    assert fx.position is None
    (s_order,) = sells(fx)
    assert (s_order.order_type, s_order.reduce_only) == ("market", True)
    assert len(fx.placed) == n_orders + 1
    blocked, why = st.state.entries_blocked()
    assert blocked and "portfolio_block" in why
    assert not st.risk.alert_only
    assert len(kills(st)) == 2 and "포지션 감지" in kills(st)[-1]["action"]
    # entries stay blocked until the cooldown (60 min from the trigger) has elapsed
    await hk(st, 30)
    assert st.state.entries_blocked()[0] is True
    await hk(st, 60)
    assert st.state.entries_blocked()[0] is False


# ── 10: PORTFOLIO kill switch while in position ─────────────────────────────


async def test_s10_kill_in_position_closes_reduce_only_blocks_then_resumes(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx, extra=4)
    await hk(st, 5)
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=50))
    fx.set_balance(wallet=fx.wallet * 0.85)                   # -15 %: both pct and abs limits breached
    st.publish()
    await st.app.housekeeping()
    await st.flush()
    trig_at = clock.now()

    # closed with a reduce-only MARKET sell although entries are now blocked
    (s,) = sells(fx)
    assert (s.order_type, s.reduce_only, s.side) == ("market", True, "sell")
    assert fx.position is None and fx.position_stop_loss is None
    (k,) = kills(st)
    assert k["level"] == "PORTFOLIO" and "청산" in k["action"]
    blocked, why = st.state.entries_blocked()
    assert blocked and "portfolio_block" in why
    bs = await st.bot_state()
    assert bs[PORTFOLIO_BLOCK_UNTIL]["until"] and bs["kill_switch"]["alert_only"] is False
    rows = await st.positions()
    assert rows[0]["closed_at"] is not None and rows[0]["close_reason"] == "kill_switch"
    assert not st.trader.has_position
    reduce_rows = [o for o in await st.orders() if o["reduce_only"]]
    assert len(reduce_rows) == 1 and reduce_rows[0]["status"] == "filled"

    # new entry signal during the block is refused (gate), no order leaves the process
    assert evaluate_at(extend(bars, 0.012, 1), has_position=False).entry_ok
    n_placed = len(fx.placed)
    await st.bar(idx + 1)                                     # +3 s after the boundary, inside the 60 min block
    assert clock.now() < trig_at + timedelta(minutes=60)
    assert fx.position is None and len(fx.placed) == n_placed
    last = st.notifier.of("order_rejected")[-1]
    assert last["side"] == "buy" and last["will_retry"] is False and "kill_switch" in last["reason"]
    assert st.trader._retry_task is None or st.trader._retry_task.done()
    assert len(kills(st)) == 1

    # after the cooldown: peaks rebased, entries resume at the next signal
    for _ in range(6):
        await hk(st, 15)
    assert st.state.entries_blocked() == (False, "")
    assert st.risk.peaks["daily"] == pytest.approx(fx.wallet)
    await st.bar(idx + 2)
    assert fx.position is not None                            # entry resumed
    assert len(kills(st)) == 1


async def test_s10b_kill_in_position_blocked_state_survives_restart(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx)
    await hk(st, 5)
    clock.set(st.bar_close_time(idx) + timedelta(hours=3, minutes=50))
    fx.set_balance(wallet=fx.wallet * 0.85)
    st.publish()
    await st.app.housekeeping()
    assert st.state.entries_blocked()[0] and fx.position is None
    await st.app.shutdown()
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    assert st2.state.entries_blocked()[0] and st2.risk.level.name == "PORTFOLIO"
    n = len(fx.placed)
    st2.app._next_house_at = clock.now() + timedelta(days=1)
    await st2.bar(idx + 1)                                    # +4h03: still inside the persisted 60 min block
    assert len(fx.placed) == n and fx.position is None
    assert st2.notifier.of("order_rejected")[-1]["will_retry"] is False


# ── 11: manual emergency close via the Telegram handlers ───────────────────


async def test_s11a_emergency_close_two_step_then_halt_restart_resume(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx, extra=5)

    # --- /emergency_close alone, or cancel, or an unauthorised chat: nothing happens
    from e2e_harness import buttons, cb_update, cmd_update
    upd = cmd_update()
    await st.commands.cmd_emergency_close(upd)
    text = upd.effective_message.sent[-1][0]
    assert "긴급 청산 확인" in text and "BTCUSDT" in text and "LONG" in text
    yes, no = buttons(upd)
    cb, q = cb_update(no)
    await st.commands.on_callback(cb)
    assert q.edits == ["취소되었습니다."]
    cb, q = cb_update(yes)                                    # the cancelled token is dead
    await st.commands.on_callback(cb)
    assert "만료" in q.edits[-1]
    bad = cmd_update(chat="999")
    await st.commands.cmd_emergency_close(bad)
    assert bad.effective_message.sent == []
    assert fx.position is not None and sells(fx) == [] and st.state.entries_blocked()[0] is False

    # --- expired confirmation (> 60 s) does nothing
    upd = cmd_update()
    await st.commands.cmd_emergency_close(upd)
    yes, _ = buttons(upd)
    st.mono.t += 61
    cb, q = cb_update(yes)
    await st.commands.on_callback(cb)
    assert "만료" in q.edits[-1]
    assert fx.position is not None and sells(fx) == []

    # --- real confirmation
    upd, q = await st.telegram_emergency_close()
    assert "긴급 청산 완료" in q.edits[-1] and "flat" in q.edits[-1]
    (s,) = sells(fx)
    assert (s.order_type, s.reduce_only) == ("market", True)
    assert fx.position is None and fx.position_stop_loss is None
    rows = await st.positions()
    assert rows[0]["closed_at"] is not None and rows[0]["close_reason"] == "emergency_close"
    bs = await st.bot_state()
    assert bs[MANUAL_HALT]["active"] is True and bs[MANUAL_HALT]["by"] == "telegram:42"
    assert st.state.entries_blocked()[0] and st.risk.level.name == "MANUAL"
    assert [k["level"] for k in kills(st)] == ["MANUAL"]
    # the button works exactly once
    cb, q2 = cb_update(yes)
    await st.commands.on_callback(cb)
    assert len(sells(fx)) == 1

    # --- entry signal during the halt is refused
    await st.bar(idx + 1)
    assert fx.position is None and len(fx.placed) == 2        # entry + the emergency sell only
    assert st.notifier.of("order_rejected")[-1]["will_retry"] is False

    # --- restart: halt persisted, entries still refused
    await st.app.shutdown()
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    assert st2.state.entries_blocked()[0] and st2.risk.level.name == "MANUAL"
    await st2.bar(idx + 2)
    assert fx.position is None and len(fx.placed) == 2
    # housekeeping with MANUAL level never auto-resumes
    for _ in range(8):
        await hk(st2, 15)
    assert st2.state.entries_blocked()[0]

    # --- /resume is two-step too
    upd = cmd_update()
    await st2.commands.cmd_resume(upd)
    assert "재개 확인" in upd.effective_message.sent[-1][0]
    assert st2.state.entries_blocked()[0]                       # only the prompt so far
    upd, q = await st2.telegram_resume()
    assert "재개되었습니다" in q.edits[-1]
    assert st2.state.entries_blocked() == (False, "")
    assert MANUAL_HALT not in await st2.bot_state() and st2.risk.level.name == "NONE"
    await st2.bar(idx + 3)
    assert fx.position is not None                              # entries are back


@pytest.mark.parametrize("how", ["manual_halt_flag", "portfolio_block", "manual_kill_level"])
async def test_s11b_emergency_close_works_while_already_halted(make_stack, fx, clock, plan, how):
    """The old system could block its own liquidation; reduce-only must pass any halt."""
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx)
    if how == "manual_halt_flag":                              # e.g. CLI halted but its close failed
        await st.state.set(MANUAL_HALT, {"active": True, "by": "cli", "reason": "cli", "at": clock.now().isoformat()})
    elif how == "portfolio_block":
        await st.state.set(PORTFOLIO_BLOCK_UNTIL, {"until": (clock.now() + timedelta(hours=1)).isoformat(),
                                                   "reason": "PORTFOLIO: x"})
    else:
        await st.risk.manual_halt("someone")
    assert st.state.entries_blocked()[0] and fx.position is not None
    upd, q = await st.telegram_emergency_close()
    assert fx.position is None, q.edits
    assert "긴급 청산 완료" in q.edits[-1]
    (s,) = sells(fx)
    assert s.reduce_only and s.order_type == "market"
    assert (await st.positions())[0]["closed_at"] is not None
    assert st.state.entries_blocked()[0]


async def test_s11c_emergency_close_when_flat_only_halts(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    upd, q = await st.telegram_emergency_close()
    assert "청산할 포지션이 없었습니다" in q.edits[-1]
    assert fx.placed == [] and st.state.entries_blocked()[0]
    await st.bar(idx)
    assert fx.position is None                                 # entry signal refused


async def test_s11d_status_command_reflects_halt(make_stack, fx, clock, plan):
    from e2e_harness import cmd_update
    bars, idx = plan
    st, _ = await in_position(make_stack, fx, clock, bars, idx)
    upd = cmd_update()
    await st.commands.cmd_status(upd)
    t = upd.effective_message.sent[-1][0]
    assert "진입 가능" in t and "LONG" in t
    await st.telegram_emergency_close()
    upd = cmd_update()
    await st.commands.cmd_status(upd)
    t = upd.effective_message.sent[-1][0]
    assert "진입 차단" in t and "MANUAL" in t and "포지션 없음" in t


# ── 12: housekeeping reconciliation ─────────────────────────────────────────


async def test_s12a_exchange_stop_fires_between_bars(make_stack, fx, clock, plan):
    bars, idx = plan
    st, feed = await in_position(make_stack, fx, clock, bars, idx, extra=4)
    entry_orders = len(fx.placed)
    fx.trigger_stop_loss()
    assert fx.position is None
    await hk(st, 15)
    assert not st.trader.has_position
    rows = await st.positions()
    assert len(rows) == 1 and rows[0]["closed_at"] is not None and rows[0]["close_reason"] == "exchange_side"
    (a,) = st.notifier.anomalies("position_vanished")
    assert "사라졌" in a["message"]
    assert st.notifier.of("exit") == []                        # not a bot exit: no PnL message
    assert len(fx.placed) == entry_orders                      # nothing sent
    assert st.trader.last_liquidation_ts == 0 and st.trader.atr_cooldown_until == 0  # old code: no cooldown
    assert len(await st.snapshots()) >= 2                      # forced snapshot at the flip
    # a second housekeeping stays quiet
    await hk(st, 15)
    assert len(st.notifier.anomalies("position_vanished")) == 1
    # next bar: entry conditions still hold and no cooldown exists -> bot re-enters (as the old code would)
    await st.bar(idx + 1)
    assert fx.position is not None
    rows = await st.positions()
    assert len(rows) == 2 and rows[0]["closed_at"] is not None and rows[1]["closed_at"] is None


async def test_s12b_exchange_stop_noticed_by_bar_before_housekeeping(make_stack, fx, clock, plan):
    bars, idx = plan
    cr = extend(bars, 0.004, 3)
    st, feed = await in_position(make_stack, fx, clock, bars, idx, extra=3)
    fx.trigger_stop_loss()
    await st.bar(idx + 1)
    assert not st.trader.has_position
    assert len(st.notifier.anomalies("position_state_divergence")) == 1
    rows = await st.positions()
    assert rows[0]["closed_at"] is not None and rows[0]["close_reason"] == "exchange_flat"


async def test_s12c_manual_long_position_is_detected_and_alerted(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars[: idx - 2], -0.001, 5), idx - 1)  # no entry signals
    st = await make_stack(feed=feed)
    await st.app.start()
    fx.set_position("long", 0.02, 60_000.0)
    await hk(st, 15)
    (a,) = st.notifier.anomalies("position_appeared")
    assert "0.0200" in a["message"]
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(0.02)
    rows = await st.positions()
    assert len(rows) == 1 and rows[0]["closed_at"] is None and float(rows[0]["size"]) == pytest.approx(0.02)
    await hk(st, 15)
    assert len(st.notifier.anomalies("position_appeared")) == 1


async def test_s12d_short_position_alerts_and_is_not_managed(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    fx.set_position("short", 0.02, 60_000.0)
    await hk(st, 15)
    assert len(st.notifier.anomalies("unexpected_short_position")) >= 1
    assert not st.trader.has_position
    assert sells(fx) == [] and fx.position.side == "short"
