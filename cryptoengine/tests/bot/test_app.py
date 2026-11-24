"""BotApp: startup, scheduler (4h boundary, confirmed-bar rule), housekeeping, BotControl,
restart survival, shutdown; plus main settings and the CLI helpers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core_helpers import (BAR_MS, FakeClock, ScriptedExecutor, build_stack, db,  # noqa: F401
                          fast_executor_timings, make_rows, ms, pos)
from bot import cli
from bot.app import BotApp, bar_boundary_ms
from bot.main import SettingsError, load_settings
from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL, BotState
from bot.strategy import ATR_MULT, evaluate
from core_helpers import rows_to_candles
from fakes.exchange import FakeExchange
from shared.kill_switch import KillLevel

pytestmark = pytest.mark.asyncio


def dt(h, m=0, s=0, day=20):
    return datetime(2026, 11, day, h, m, s, tzinfo=timezone.utc)


def set_candles(fx, last_closed_open: datetime, *, in_progress=True, n=400):
    """Closed bars up to ``last_closed_open`` plus (like Bybit) the in-progress bar last."""
    rows = make_rows(n, ms(last_closed_open))
    if in_progress:
        c = rows[-1][4]
        rows.append([rows[-1][0] + BAR_MS, c, c * 1.001, c * 0.999, c, 1.0])
    fx.set_candles(rows)
    return rows


@pytest_asyncio.fixture
async def env(db, monkeypatch, tmp_path):
    fast_executor_timings(monkeypatch)
    clock = FakeClock(dt(11, 58))
    fx = FakeExchange(wallet=1000.0)
    set_candles(fx, dt(4))  # bar 04:00-08:00 closed, 08:00-12:00 in progress
    fx.set_position("long", 0.01, 99_000.0)  # hold on every bar: no orders in these tests
    s = build_stack(db, clock=clock, fx=fx, heartbeat_path=str(tmp_path / "hb"))
    s.hb = tmp_path / "hb"
    s.bars_seen = []
    orig = s.trader.on_bar

    async def spy(c):
        s.bars_seen.append(c)
        await orig(c)

    s.trader.on_bar = spy
    yield s
    await s.trader.shutdown()


async def sig_count(db):
    async with db.pool.acquire() as c:
        return await c.fetchval("SELECT COUNT(*) FROM supertrend_signals")


# ── helpers ─────────────────────────────────────────────────────────────

async def test_bar_boundary_math():
    t = ms(dt(11, 58))
    assert bar_boundary_ms(t) == ms(dt(12))
    assert bar_boundary_ms(ms(dt(12))) == ms(dt(16))  # strictly after
    nxt = BotApp._boundary_after(dt(11, 58))
    assert nxt == dt(12, 0, 3)
    assert BotApp._next_midnight(dt(23, 59)) == dt(0, day=21)


# ── startup ─────────────────────────────────────────────────────────────

async def test_start_sequence(env, db):
    await env.app.start()
    fx = env.fx
    assert fx.connected and fx.margin_calls == ["isolated"] and fx.leverage_calls == [3]
    assert env.trader.has_position and env.trader.position_qty == pytest.approx(0.01)
    assert any(n == "send" for n, _ in env.notifier.calls)  # startup message
    async with db.pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM ohlcv_history WHERE exchange='bybit' AND symbol='BTCUSDT' AND timeframe='4h'")
        last = await c.fetchval("SELECT MAX(timestamp) FROM ohlcv_history")
    assert n == 400 and last == dt(4)  # confirmed only: the in-progress 08:00 bar is not stored
    assert env.app._next_bar_at == dt(12, 0, 3)


async def test_start_restores_persisted_state_before_trading(env):
    await env.state.set(MANUAL_HALT, {"active": True, "reason": "x"})
    await env.state.set("atr_cooldown_until", 123)
    env.state._data.clear()  # fresh process: nothing in memory
    await env.app.start()
    assert env.state.entries_blocked()[0] is True and env.trader.atr_cooldown_until == 123


async def test_missed_bar_on_start_processed_once(env, db):
    async with db.pool.acquire() as c:
        await c.execute(
            "INSERT INTO supertrend_signals (bar_ts, st_dir, fast_ema, slow_ema, dir_ema, price, atr_14, "
            "allocated_capital, had_position, entry_ok, exit_signal, expected_action) "
            "VALUES ($1,1,1,1,1,1,1,1,false,false,false,'hold')", dt(0))
    env.clock.set(dt(8, 0, 30))  # 04:00 bar has closed; its signal was never computed
    await env.app.start()
    assert len(env.bars_seen) == 1 and env.bars_seen[0][-1]["ts"] == ms(dt(4))
    assert await sig_count(db) == 2
    # restart: the signal row exists now, nothing is reprocessed
    s2 = build_stack(db, clock=FakeClock(dt(8, 1)), fx=env.fx)
    seen = []
    o = s2.trader.on_bar
    s2.trader.on_bar = lambda c: (seen.append(c), o(c))[1]
    await s2.app.start()
    assert seen == [] and await sig_count(db) == 2


# ── scheduler: confirmed-bar rule ───────────────────────────────────────

async def test_fetch_confirmed_never_returns_in_progress_bar(env):
    env.clock.set(dt(11, 59))
    bars = await env.app._fetch_confirmed()
    assert bars[-1]["ts"] == ms(dt(4))  # the 08:00 bar is still running at 11:59
    env.clock.set(dt(12, 0, 3))
    env.fx.set_candles(env.fx.candles[:-1] + [[ms(dt(8)), 1, 2, 1, 1.5, 1.0],
                                              [ms(dt(12)), 1.5, 2, 1, 1.6, 0.1]])
    bars = await env.app._fetch_confirmed()
    assert bars[-1]["ts"] == ms(dt(8)) and ms(dt(12)) not in {b["ts"] for b in bars}


async def test_boundary_processes_the_just_closed_bar_and_not_the_running_one(env, db):
    await env.app.start()
    env.bars_seen.clear()
    rows = make_rows(401, ms(dt(8)))  # bar 08:00-12:00 now closed
    rows.append([ms(dt(12)), rows[-1][4], rows[-1][4], rows[-1][4], rows[-1][4], 0.1])  # in progress
    env.fx.set_candles(rows)
    env.clock.set(dt(12, 0, 3))
    await env.app.tick()
    assert len(env.bars_seen) == 1
    ts = [b["ts"] for b in env.bars_seen[0]]
    assert ts[-1] == ms(dt(8)) and ms(dt(12)) not in ts
    assert env.trader.last_bar_ts == ms(dt(8))
    async with db.pool.acquire() as c:
        stored = await c.fetchval("SELECT COUNT(*) FROM ohlcv_history WHERE timestamp=$1", dt(8))
        sigs = await c.fetchval("SELECT COUNT(*) FROM supertrend_signals WHERE bar_ts=$1", dt(8))
    assert stored == 1 and sigs == 1
    assert env.app._next_bar_at == dt(16, 0, 3) and env.app._pending_bar is None
    await env.app.tick()  # nothing is processed twice
    assert len(env.bars_seen) == 1


async def test_in_progress_only_means_not_processed_then_retries_then_anomaly(env):
    await env.app.start()
    env.bars_seen.clear()
    # exchange lags: closed bars end at 04:00, only the 12:00 in-progress row is new
    rows = make_rows(400, ms(dt(4)))
    rows.append([ms(dt(12)), 1.0, 1.0, 1.0, 1.0, 0.1])
    env.fx.set_candles(rows)
    env.clock.set(dt(12, 0, 3))
    await env.app.tick()
    assert env.bars_seen == [] and env.app._pending_bar == ms(dt(8))
    assert env.app._bar_retry_at == dt(12, 0, 8)  # retry in 5 s
    env.clock.set(dt(12, 0, 8))
    await env.app.tick()
    assert env.bars_seen == [] and env.app._bar_retry_at == dt(12, 0, 13)
    assert env.notifier.of("anomaly") == []
    env.clock.set(dt(12, 10, 4))  # > 10 minutes still missing
    await env.app.tick()
    assert [a["kind"] for a in env.notifier.of("anomaly")] == ["bar_missing"]
    assert env.app._bar_retry_at == dt(12, 11, 4)  # then once a minute
    env.clock.set(dt(12, 11, 4))
    await env.app.tick()
    assert [a["kind"] for a in env.notifier.of("anomaly")] == ["bar_missing"]  # only once
    assert env.bars_seen == []
    # the bar finally shows up
    rows = make_rows(401, ms(dt(8)))
    rows.append([ms(dt(12)), 1.0, 1.0, 1.0, 1.0, 0.1])
    env.fx.set_candles(rows)
    env.clock.set(dt(12, 12, 4))
    await env.app.tick()
    assert len(env.bars_seen) == 1 and env.bars_seen[0][-1]["ts"] == ms(dt(8))
    assert env.app._pending_bar is None


async def test_fetch_error_is_retried_not_fatal(env):
    await env.app.start()
    env.bars_seen.clear()
    env.fx.set_candles(make_rows(401, ms(dt(8))))
    env.fx.fail_next("get_ohlcv", 1)
    env.clock.set(dt(12, 0, 3))
    await env.app.tick()
    assert env.bars_seen == [] and env.app._pending_bar is not None
    env.clock.set(dt(12, 0, 9))
    await env.app.tick()
    assert len(env.bars_seen) == 1


async def test_replay_sync_runs_after_bar_and_never_breaks_trading(env, db):
    calls = []

    async def bad_sync(d):
        calls.append(d)
        raise RuntimeError("binance down")

    env.app._replay_sync = bad_sync
    await env.app.start()
    env.fx.set_candles(make_rows(401, ms(dt(8))) + [[ms(dt(12)), 1, 1, 1, 1, 1]])
    env.clock.set(dt(12, 0, 3))
    await env.app.tick()
    assert calls == [db] and env.trader.last_bar_ts == ms(dt(8))


async def test_on_bar_exception_does_not_stop_scheduler_and_bar_is_stored(env, db):
    await env.app.start()

    async def boom(c):
        raise RuntimeError("bug")

    env.trader.on_bar = boom
    env.fx.set_candles(make_rows(401, ms(dt(8))) + [[ms(dt(12)), 1, 1, 1, 1, 1]])
    env.clock.set(dt(12, 0, 3))
    await env.app.tick()  # must not raise
    async with db.pool.acquire() as c:
        assert await c.fetchval("SELECT COUNT(*) FROM ohlcv_history WHERE timestamp=$1", dt(8)) == 1
    assert env.app._pending_bar is None


async def test_full_run_loop_with_virtual_clock(env, db):
    started, stopped = [], []

    async def st():
        started.append(1)

    async def sp():
        stopped.append(1)

    env.app.add_service(st, sp)

    def stop_when_late(clock):
        if clock.now() > dt(12, 2):
            env.app.request_stop()

    env.clock.on_sleep = stop_when_late
    fx = env.fx

    # at 12:00:03 the exchange has the new bar
    def publish(clock):
        if clock.now() >= dt(12, 0, 3):
            fx.set_candles(make_rows(401, ms(dt(8))) + [[ms(dt(12)), 1, 1, 1, 1, 1]])

    orig = env.clock.on_sleep
    env.clock.on_sleep = lambda c: (publish(c), orig(c))
    await asyncio.wait_for(env.app.run(), 30)
    assert started == [1] and stopped == [1]
    assert env.trader.last_bar_ts == ms(dt(8))
    hb = env.state.get("heartbeat")
    assert hb and env.hb.exists()
    assert fx.position is not None and fx.placed == []  # shutdown/run never closed anything


# ── housekeeping ────────────────────────────────────────────────────────

async def test_housekeeping_runs_every_15_minutes_not_before(env, db):
    env.clock.set(dt(9))
    await env.app.start()
    await env.app.tick()  # first pass runs housekeeping at once
    n1 = len(env.fx.calls)
    env.clock.advance(600)
    await env.app.tick()
    calls = [c for c, _ in env.fx.calls[n1:]]
    assert "get_balance" not in calls and "get_position" not in calls
    env.clock.advance(400)  # now > 15 min since the last pass
    await env.app.tick()
    calls = [c for c, _ in env.fx.calls[n1:]]
    assert "get_balance" in calls and "get_position" in calls


async def test_housekeeping_writes_snapshot_heartbeat_and_evaluates_risk(env, db):
    await env.app.start()
    await env.app.housekeeping()
    async with db.pool.acquire() as c:
        row = await c.fetchrow("SELECT * FROM portfolio_snapshots")
    assert row["total_equity"] == 1000.0
    assert env.risk.peaks["daily"] == 1000.0
    await env.app.tick()
    assert env.state.get("heartbeat")["at"] == env.clock.now().isoformat() and env.hb.exists()


async def test_housekeeping_detects_position_vanished_after_exchange_stop(env, db):
    await env.app.start()
    assert env.trader.has_position
    env.fx.position = None  # exchange-side stop fired / manual close
    await env.app.housekeeping()
    assert not env.trader.has_position
    a = env.notifier.of("anomaly")
    assert [x["kind"] for x in a] == ["position_vanished"] and "0.0100" in a[0]["message"]
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM positions ORDER BY id DESC LIMIT 1")
        snaps = await c.fetchval("SELECT COUNT(*) FROM portfolio_snapshots")
    assert r["closed_at"] is not None and r["close_reason"] == "exchange_side"
    assert snaps == 1  # a reconcile forces a snapshot
    await env.app.housekeeping()  # reconciled: no second alert
    assert len(env.notifier.of("anomaly")) == 1


async def test_housekeeping_stop_loss_fired_by_exchange_after_a_real_entry(db, monkeypatch):
    fast_executor_timings(monkeypatch)
    clock = FakeClock(dt(8, 0, 10))
    fx = FakeExchange(wallet=1000.0)
    rows = make_rows(400, ms(dt(4)))
    px = rows[-1][4]
    fx.set_candles(rows)
    fx.set_book(px - 5, px + 5)
    s = build_stack(db, clock=clock, fx=fx)
    await s.trader.on_bar(rows_to_candles(rows))
    assert fx.position is not None and fx.position_stop_loss is not None
    fx.trigger_stop_loss()
    assert fx.position is None
    await s.app.housekeeping()
    assert [a["kind"] for a in s.notifier.of("anomaly")] == ["position_vanished"]
    assert not s.trader.has_position
    await s.trader.shutdown()


async def test_housekeeping_detects_position_appeared(env, db):
    env.fx.position = None
    await env.app.start()
    assert not env.trader.has_position
    env.fx.set_position("long", 0.02, 98_000.0)  # manual trade
    await env.app.housekeeping()
    assert env.trader.has_position and env.trader.position_qty == pytest.approx(0.02)
    assert [a["kind"] for a in env.notifier.of("anomaly")] == ["position_appeared"]
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM positions WHERE closed_at IS NULL")
    assert float(r["size"]) == pytest.approx(0.02)


async def test_housekeeping_failures_alert_once_after_three(env):
    await env.app.start()
    env.fx.fail_next("get_balance", 4)
    for _ in range(4):
        await env.app.housekeeping()
    assert [a["kind"] for a in env.notifier.of("anomaly")] == ["housekeeping_failing"]
    await env.app.housekeeping()  # recovered
    assert env.app._hk_failures == 0


async def test_housekeeping_skips_risk_on_zero_equity(env, db):
    await env.app.start()
    env.fx.set_balance(wallet=0.0)
    await env.app.housekeeping()
    assert env.notifier.of("kill_switch") == []
    assert env.risk.level == KillLevel.NONE


async def test_housekeeping_kill_switch_closes_position_and_blocks(env, db):
    await env.app.start()
    await env.app.housekeeping()  # peaks 1000
    env.fx.set_balance(wallet=880.0)  # -12 %, 120 USD
    await env.app.housekeeping()
    assert env.risk.level == KillLevel.PORTFOLIO and env.fx.position is None
    assert not env.trader.has_position
    assert env.state.entries_blocked()[0] is True
    assert env.notifier.of("kill_switch")


# ── BotControl ──────────────────────────────────────────────────────────

async def test_status_keys(env):
    await env.app.start()
    st = await env.app.status()
    for k in ("equity", "position", "halted", "halt_reason", "kill_level", "last_bar_ts", "last_signal",
              "next_bar_at", "daily_dd_pct", "weekly_dd_pct", "monthly_dd_pct", "heartbeat_at"):
        assert k in st
    assert st["equity"] == 1000.0 and st["position"]["size"] == pytest.approx(0.01)
    assert st["halted"] is False and st["kill_level"] == "NONE"
    assert st["next_bar_at"] == dt(12, 0, 3)


async def test_emergency_close_with_position_while_entries_blocked(env, db):
    await env.app.start()
    await env.state.set(PORTFOLIO_BLOCK_UNTIL, {"until": (env.clock.now() + timedelta(hours=1)).isoformat()})
    res = await env.app.emergency_close("tester")
    assert res["had_position"] and res["closed"] and res["flat"]
    assert env.fx.position is None and not env.trader.has_position
    assert env.state.get(MANUAL_HALT)["active"] and env.risk.level == KillLevel.MANUAL
    assert env.state.entries_blocked()[0] is True
    sells = [o for o in env.fx.placed if o.reduce_only]
    assert len(sells) == 1 and sells[0].order_type == "market" and sells[0].side == "sell"
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM positions ORDER BY id DESC LIMIT 1")
    assert r["closed_at"] is not None and r["close_reason"] == "emergency_close"
    assert env.notifier.of("kill_switch")[0]["level"] == "MANUAL"


async def test_emergency_close_flat(env):
    env.fx.position = None
    await env.app.start()
    res = await env.app.emergency_close("tester")
    assert res["had_position"] is False and res["flat"] is True and res["closed"] is False
    assert env.state.entries_blocked()[0] is True


async def test_halt_blocks_new_entries_but_not_exits_then_resume(env, db):
    env.fx.position = None
    await env.app.start()
    await env.app.emergency_close("tester")
    rows = make_rows(400, ms(dt(4)))
    await env.trader.on_bar(rows_to_candles(rows))
    assert [o for o in env.fx.placed if not o.reduce_only] == []  # entry blocked
    r = await env.app.resume("tester")
    assert r == {"was_halted": True, "halted": False}
    assert env.risk.level == KillLevel.NONE and env.state.entries_blocked() == (False, "")
    assert (await env.app.resume("tester")) == {"was_halted": False, "halted": False}


async def test_emergency_close_failure_reports_not_flat(db):
    sc = ScriptedExecutor()
    sc.position = pos(0.01)

    async def boom(reason):
        raise RuntimeError("exchange down")

    sc.close_position = boom
    s = build_stack(db, clock=FakeClock(dt(9)), scripted=sc)
    res = await s.app.emergency_close("tester")
    assert res["had_position"] and not res["flat"] and "exchange down" in res["detail"]
    assert s.state.entries_blocked()[0] is True  # halted regardless
    assert s.notifier.of("anomaly")[-1]["kind"] == "emergency_close_not_flat"


# ── restart survival ────────────────────────────────────────────────────

async def test_kill_state_and_cooldowns_survive_simulated_restart(db, monkeypatch):
    fast_executor_timings(monkeypatch)
    fx = FakeExchange(wallet=1000.0)
    set_candles(fx, dt(4), in_progress=True)
    fx.set_position("long", 0.01, 99_000.0)
    clock = FakeClock(dt(9))
    s = build_stack(db, clock=clock, fx=fx)
    await s.app.start()
    await s.app.housekeeping()
    fx.set_balance(wallet=880.0)
    await s.app.housekeeping()  # PORTFOLIO kill with a position: closed + blocked
    assert s.risk.level == KillLevel.PORTFOLIO
    # ATR exit recorded earlier
    await s.state.set("atr_cooldown_until", ms(dt(8)) + BAR_MS)
    await s.state.set("last_liquidation_ts", ms(dt(8)))
    # --- restart: all new objects on the same database and exchange
    s2 = build_stack(db, clock=clock, fx=fx)
    await s2.app.start()
    assert s2.state.entries_blocked()[0] is True
    assert s2.risk.level == KillLevel.PORTFOLIO and s2.risk.kill_switch.triggered_at == s.risk.kill_switch.triggered_at
    assert s2.trader.atr_cooldown_until == ms(dt(8)) + BAR_MS and s2.trader.last_liquidation_ts == ms(dt(8))
    st = await s2.app.status()
    assert st["halted"] is True and st["kill_level"] == "PORTFOLIO"
    # no second alert after restart, peaks stayed
    await s2.app.housekeeping()
    assert s2.notifier.of("kill_switch") == []


# ── shutdown ────────────────────────────────────────────────────────────

async def test_shutdown_never_closes_position(env):
    started, stopped = [], []

    async def sp():
        stopped.append(1)

    env.app.add_service(None, sp)
    await env.app.start()
    await env.app.shutdown()
    assert env.fx.position is not None and env.fx.placed == []
    assert stopped == [1] and env.fx.connected is False


async def test_daily_report_at_midnight(env):
    sent = []

    async def report_fn():
        return "<b>report</b>", None

    env.app._report_fn = report_fn
    await env.app.start()
    assert env.app._next_report_at == dt(0, day=21)
    env.clock.set(dt(0, 0, 5, day=21))
    await env.app.tick()
    sends = [kw for n, kw in env.notifier.calls if n == "send" and kw["html"] == "<b>report</b>"]
    assert len(sends) == 1 and sends[0]["dedup_key"] == "daily_report:2026-11-21"
    assert env.app._next_report_at == dt(0, day=22)
    await env.app.tick()
    assert len([1 for n, kw in env.notifier.calls if n == "send" and kw["html"] == "<b>report</b>"]) == 1


async def test_daily_report_passes_keyboard_when_present(env):
    got = {}

    async def send(html, *, dedup_key=None, reply_markup=None):
        got.update(html=html, markup=reply_markup)

    env.notifier.send = send

    async def report_fn():
        return "r", "KEYBOARD"

    env.app._report_fn = report_fn
    await env.app.daily_report()
    assert got == {"html": "r", "markup": "KEYBOARD"}


# ── main settings ───────────────────────────────────────────────────────

GOOD = {"BYBIT_TESTNET": "false", "BYBIT_API_KEY": "k", "BYBIT_API_SECRET": "s", "DB_PASSWORD": "p",
        "TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c"}


@pytest.mark.parametrize("val", ["true", "", "False ", "0", "no"])
async def test_main_refuses_unless_testnet_is_exactly_false(val):
    env = {**GOOD, "BYBIT_TESTNET": val}
    if val.strip().lower() == "false":
        assert load_settings(env)  # case/space tolerant
    else:
        with pytest.raises(SettingsError):
            load_settings(env)


async def test_main_refuses_when_testnet_unset():
    e = dict(GOOD)
    del e["BYBIT_TESTNET"]
    with pytest.raises(SettingsError):
        load_settings(e)


@pytest.mark.parametrize("missing", ["BYBIT_API_KEY", "BYBIT_API_SECRET", "TELEGRAM_BOT_TOKEN",
                                     "TELEGRAM_CHAT_ID", "DB_PASSWORD"])
async def test_main_refuses_on_missing_credentials(missing):
    e = dict(GOOD)
    e[missing] = ""
    with pytest.raises(SettingsError, match=missing):
        load_settings(e)


async def test_main_settings_defaults_and_overrides():
    s = load_settings(GOOD)
    assert s.leverage_limit == 3.0 and s.dashboard_port == 3000 and s.dry_run is False
    s = load_settings({**GOOD, "BOT_DRY_RUN": "1", "SAFETY_LEVERAGE_LIMIT": "2.5", "DASHBOARD_PORT": "8080"})
    assert s.dry_run is True and s.leverage_limit == 2.5 and s.dashboard_port == 8080
    e = dict(GOOD)
    del e["TELEGRAM_BOT_TOKEN"]
    assert load_settings(e, need_telegram=False)  # the CLI does not need Telegram


# ── CLI ─────────────────────────────────────────────────────────────────

async def test_cli_emergency_close_confirms_unless_yes(db, monkeypatch):
    fast_executor_timings(monkeypatch)
    fx = FakeExchange(wallet=1000.0)
    fx.set_position("long", 0.01, 99_000.0)
    s = build_stack(db, clock=FakeClock(dt(9)), fx=fx)
    out = []
    res = await cli.cmd_emergency_close(s.executor, s.state, assume_yes=False, ask=lambda _: "n", out=out.append)
    assert res == {"aborted": True} and fx.position is not None and s.state.get(MANUAL_HALT) is None
    res = await cli.cmd_emergency_close(s.executor, s.state, assume_yes=False, ask=lambda _: "yes", out=out.append)
    assert res["flat"] and fx.position is None
    assert s.state.get(MANUAL_HALT)["active"] and s.state.entries_blocked()[0] is True
    # a running bot reloads state from Postgres and honours the CLI halt
    other = BotState(db)
    await other.load()
    assert other.entries_blocked()[0] is True


async def test_cli_yes_flag_and_status_and_resume(db, monkeypatch):
    fast_executor_timings(monkeypatch)
    fx = FakeExchange(wallet=1000.0)
    fx.set_position("long", 0.01, 99_000.0)
    s = build_stack(db, clock=FakeClock(dt(9)), fx=fx)
    out = []
    await cli.cmd_emergency_close(s.executor, s.state, assume_yes=True,
                                  ask=lambda _: pytest.fail("must not ask"), out=out.append)
    assert fx.position is None
    info = await cli.cmd_status(s.executor, s.state, out=out.append)
    assert info["entries_blocked"] is True and info["position"] is None and info["wallet"] > 0
    r = await cli.cmd_resume(s.state, out=out.append)
    assert r == {"was_halted": True} and s.state.entries_blocked() == (False, "")
    assert (await cli.cmd_resume(s.state, out=out.append)) == {"was_halted": False}
