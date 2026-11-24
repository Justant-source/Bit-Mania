"""Scenarios 6-8: restart in position, restart after a missed bar, the scheduler loop."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import bot.strategy as strategy_mod
from bot.interfaces import BAR_MS
from e2e_harness import atr_crash, entry_plan, extend, feed_for

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
def plan():
    p = entry_plan()
    return p["bars"], p["idx"]


def sells(fx):
    return [o for o in fx.placed if o.side == "sell"]


async def test_s6a_restart_mid_position_keeps_position(make_stack, fx, clock, plan):
    bars, idx = plan
    long_run = extend(bars, 0.004, 6)
    feed = feed_for(fx, clock, long_run, idx - 1)
    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 1)
    await st1.bar(idx)
    size = fx.position.size
    n_placed = len(fx.placed)
    assert fx.position is not None

    # SIGTERM-style shutdown: never closes
    await st1.app.shutdown()
    assert fx.position is not None and fx.position.size == size
    assert sells(fx) == [] and len(fx.placed) == n_placed
    assert not fx.connected
    assert any("봇 종료" in t and "포지션은 유지" in t for t in st1.notifier.texts())
    rows = await st1.positions()
    assert len(rows) == 1 and rows[0]["closed_at"] is None

    # new process: same DB + same exchange
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    assert st2.trader.has_position and st2.trader.position_qty == pytest.approx(size)
    assert st2.trader.entry_price == pytest.approx(fx.position.entry_price)
    assert len(fx.placed) == n_placed                       # nothing re-entered, nothing closed
    assert len(await st2.positions()) == 1 and (await st2.positions())[0]["closed_at"] is None
    assert len(await st2.signals()) == 2                    # no missed bar -> nothing re-evaluated
    assert st2.notifier.of("entry") == [] and st2.notifier.of("exit") == []
    assert st2.notifier.anomalies() == []
    assert any("보유" in t for t in st2.notifier.texts())   # startup message reports the position
    # next bar: still holding, no second entry
    await st2.bar(idx + 1)
    await st2.bar(idx + 2)
    assert len(fx.placed) == n_placed and fx.position.size == size
    assert len(await st2.positions()) == 1
    assert [s["expected_action"] for s in (await st2.signals())[-2:]] == ["hold", "hold"]


async def test_s6b_restart_restores_cooldown_and_blocks(make_stack, fx, clock, plan):
    bars, idx = plan
    entry_px = bars[-1][3]
    crash, cidx, pct = atr_crash(bars, entry_px)
    feed = feed_for(fx, clock, crash, idx - 1)
    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 1)
    await st1.bar(idx)
    await st1.bar(cidx)                                     # atr_distance exit, cooldown persisted
    cool = st1.trader.atr_cooldown_until
    liq = st1.trader.last_liquidation_ts
    assert cool > 0 and liq > 0 and fx.position is None
    await st1.risk.manual_halt("test")                      # a manual halt must survive too
    await st1.app.shutdown()

    st2 = await make_stack(feed=feed)
    await st2.app.start()
    assert st2.trader.atr_cooldown_until == cool and st2.trader.last_liquidation_ts == liq
    blocked, why = st2.state.entries_blocked()
    assert blocked and "manual" in why
    assert st2.risk.level.name == "MANUAL"
    assert any("진입 차단" in t for t in st2.notifier.texts())


async def test_s6c_sigterm_through_run_loop_never_closes(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.004, 4), idx - 1)
    st = await make_stack(feed=feed)
    await st.app.start()
    await st.bar(idx - 1)
    await st.bar(idx)
    n = len(fx.placed)
    stop_at = clock.now() + timedelta(hours=9)

    def hook(c):
        feed.publish(c.now())
        if c.now() >= stop_at:
            st.app.request_stop()                           # what the SIGTERM handler does

    clock.on_sleep = hook
    await asyncio.wait_for(st.app.run(), 120)               # run() = start + loop + shutdown
    assert fx.position is not None and sells(fx) == []
    assert (await st.positions())[0]["closed_at"] is None
    assert not fx.connected


async def test_s7a_restart_after_missed_bar_evaluates_it_exactly_once(make_stack, fx, clock, plan, monkeypatch):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 3), idx - 2)
    evals: list[int] = []
    real_eval = strategy_mod.evaluate

    def spy(candles, **kw):
        s = real_eval(candles, **kw)
        if s is not None:
            evals.append(s.bar_ts)
        return s

    monkeypatch.setattr(strategy_mod, "evaluate", spy)
    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 2)
    await st1.bar(idx - 1)
    await st1.app.shutdown()
    assert fx.position is None
    n_evals = len(evals)

    # bot is down while bar idx (the entry bar) closes; restart 40 s after the boundary
    clock.set(st1.bar_close_time(idx) + timedelta(seconds=40))
    feed.publish(clock.now())
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    assert evals[n_evals:].count(feed.candles[idx]["ts"]) >= 1
    sigs = await st2.signals()
    assert [s["ts_ms"] for s in sigs].count(feed.candles[idx]["ts"]) == 1
    assert sigs[-1]["entry_ok"] and fx.position is not None          # the missed entry was taken
    assert len(await st2.orders()) == 1
    # scheduler continues: the missed bar is not evaluated a second time at the next ticks
    await st2.app.tick()
    await st2.app.tick()
    await st2.bar(idx + 1)
    ts_all = [s["ts_ms"] for s in await st2.signals()]
    assert len(ts_all) == len(set(ts_all)) == 4
    assert evals.count(feed.candles[idx]["ts"]) == 1                 # evaluate() ran once for it
    assert len(await st2.orders()) == 1 and len(fx.placed) == 1


async def test_s7b_restart_without_missed_bar_evaluates_nothing(make_stack, fx, clock, plan, monkeypatch):
    bars, idx = plan
    feed = feed_for(fx, clock, bars, idx - 1)
    evals: list[int] = []
    real_eval = strategy_mod.evaluate

    def spy(candles, **kw):
        s = real_eval(candles, **kw)
        evals.append(0)
        return s

    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 1)
    await st1.bar(idx)
    await st1.app.shutdown()
    monkeypatch.setattr(strategy_mod, "evaluate", spy)
    sigs_before = await st1.signals()
    placed = len(fx.placed)
    for _ in range(3):                                       # three restarts in a row, same bar
        clock.advance(120)
        st = await make_stack(feed=feed)
        await st.app.start()
        await st.app.tick()
        await st.app.shutdown()
    assert evals == []
    assert [s["ts_ms"] for s in await st1.signals()] == [s["ts_ms"] for s in sigs_before]
    assert len(fx.placed) == placed and len(await st1.orders()) == 1


async def test_s7c_two_missed_bars_only_latest_is_evaluated(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st1 = await make_stack(feed=feed)
    await st1.app.start()
    await st1.bar(idx - 1)
    await st1.app.shutdown()
    clock.set(st1.bar_close_time(idx + 1) + timedelta(seconds=10))
    feed.publish(clock.now())
    st2 = await make_stack(feed=feed)
    await st2.app.start()
    ts = [s["ts_ms"] for s in await st2.signals()]
    assert ts == [feed.candles[idx - 1]["ts"], feed.candles[idx + 1]["ts"]]


# ── scheduler ──────────────────────────────────────────────────────────────


async def _run_loop(st, until):
    """Run the real BotApp.run() loop on virtual time until ``until``."""

    def hook(c):
        st.publish()
        if c.now() >= until:
            st.app.request_stop()

    st.clock.on_sleep = hook
    await asyncio.wait_for(st.app.run(), 300)


async def test_s8a_scheduler_wakes_at_boundary_plus_3s_and_skips_in_progress_bar(
        make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st = await make_stack(feed=feed)
    # bot is running mid-bar: bar idx-2 closed 10 min ago, bar idx-1 closes at the next boundary
    clock.set(st.bar_close_time(idx - 2) + timedelta(minutes=10))
    feed.publish(clock.now())
    first_close = st.bar_close_time(idx - 1)
    wake_log: list[tuple[datetime, int]] = []
    real_on_bar = st.trader.on_bar

    async def spy_on_bar(candles):
        wake_log.append((clock.now(), candles[-1]["ts"]))
        await real_on_bar(candles)

    st.trader.on_bar = spy_on_bar
    # a baseline signal row so that the start-up missed-bar rule does not fire
    await st.app.start()
    assert st.app._next_bar_at == first_close + timedelta(seconds=3)
    until = first_close + timedelta(hours=8, minutes=5)

    def hook(c):
        feed.publish(c.now())
        if c.now() >= until:
            st.app.request_stop()

    clock.on_sleep = hook
    await asyncio.wait_for(st.app.run(), 300)

    # each bar is handed to the trader exactly at boundary + 3 s, and it is the CLOSED bar
    assert len(wake_log) == 3
    for (when, last_ts), k in zip(wake_log, range(3)):
        close = first_close + timedelta(hours=4 * k)
        assert when == close + timedelta(seconds=3), (when, close)
        assert last_ts + BAR_MS == int(close.timestamp() * 1000)   # last candle handed over just closed
    # the in-progress bar (always the last row Bybit returns) was never evaluated
    sig_ts = {s["ts_ms"] for s in await st.signals()}
    assert sig_ts == {feed.candles[idx - 1]["ts"], feed.candles[idx]["ts"], feed.candles[idx + 1]["ts"]}
    # heartbeat kept beating on every wake (<= 60 s gap)
    assert max(clock.sleeps) <= 60.0 + 1e-6


async def test_s8b_bar_returned_late_is_evaluated_once(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st = await make_stack(feed=feed)
    clock.set(st.bar_close_time(idx - 2) + timedelta(seconds=30))
    feed.publish(clock.now())
    await st.app.start()
    late_ts = feed.candles[idx - 1]["ts"]
    feed.delay[late_ts] = 47.0                                # exchange serves the bar 47 s late
    attempts: list[datetime] = []
    real = fx.get_ohlcv

    async def counting(symbol, tf="4h", since=None, limit=1000):
        attempts.append(clock.now())
        return await real(symbol, tf, since, limit)

    fx.get_ohlcv = counting  # type: ignore[assignment]
    until = st.bar_close_time(idx - 1) + timedelta(minutes=3)
    await _run_loop(st, until)
    ts = [s["ts_ms"] for s in await st.signals()]
    assert ts.count(late_ts) == 1
    close = st.bar_close_time(idx - 1)
    boundary_attempts = [a for a in attempts if a >= close]
    # retried every 5 s starting at +3 s, evaluated at the first attempt after +47 s
    gaps = {round((b - a).total_seconds()) for a, b in zip(boundary_attempts, boundary_attempts[1:])
            if (b - close).total_seconds() < 60}
    assert gaps <= {5}
    assert len(boundary_attempts) >= 9
    assert st.notifier.anomalies() == []


async def test_s8c_exchange_unreachable_over_10min_one_anomaly_then_recovers(make_stack, fx, clock, plan):
    bars, idx = plan
    feed = feed_for(fx, clock, extend(bars, 0.012, 2), idx - 1)
    st = await make_stack(feed=feed)
    clock.set(st.bar_close_time(idx - 2) + timedelta(seconds=30))
    feed.publish(clock.now())
    await st.app.start()
    close = st.bar_close_time(idx - 1)
    recover_at = close + timedelta(minutes=17)
    real = fx.get_ohlcv
    attempts: list[datetime] = []

    async def flaky(symbol, tf="4h", since=None, limit=1000):
        attempts.append(clock.now())
        if close <= clock.now() < recover_at:
            raise ConnectionError("exchange unreachable")
        return await real(symbol, tf, since, limit)

    fx.get_ohlcv = flaky  # type: ignore[assignment]
    await _run_loop(st, close + timedelta(minutes=25))
    anomalies = st.notifier.anomalies("bar_missing")
    assert len(anomalies) == 1
    assert st.notifier.anomalies() == anomalies                # nothing else screamed
    ts = [s["ts_ms"] for s in await st.signals()]
    assert ts.count(feed.candles[idx - 1]["ts"]) == 1          # recovered and evaluated once
    during = [a for a in attempts if close <= a < recover_at]
    fast = [a for a in during if (a - close).total_seconds() < 600]
    slow = [a for a in during if (a - close).total_seconds() >= 600]
    assert len(fast) >= 100 and 3 <= len(slow) <= 8             # 5 s cadence, then ~60 s
