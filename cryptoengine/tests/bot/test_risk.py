"""Risk: peaks, drawdowns, KillSwitch integration, halt logic, restart survival.

Carries over tests/unit/test_orchestrator_ks_loop_guard.py and
test_orchestrator_kill_switch_alert.py onto the new RiskManager.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core_helpers import FakeClock, ScriptedExecutor, build_stack, db, pos  # noqa: F401
from bot.risk import KEY_KILL_STATE, KEY_PEAK_REBASE_AT, KEY_PEAKS, RiskConfig, RiskManager, _period_keys, drawdown
from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL, BotState, SystemClock
from shared.kill_switch import KillLevel

pytestmark = pytest.mark.asyncio
ROOT = Path(__file__).resolve().parents[2]


def stack(db, **kw):
    """Risk tests use the real clock: the shared KillSwitch stamps triggers with wall time."""
    sc = kw.pop("scripted", None) or ScriptedExecutor()
    s = build_stack(db, clock=SystemClock(), scripted=sc, **kw)
    s.sc = sc
    return s


def set_peaks(s, value):
    r = s.risk
    r._peaks = {"daily": value, "weekly": value, "monthly": value}
    r._keys = _period_keys(r._clock.now())


# ── config / construction ───────────────────────────────────────────────

async def test_config_from_yaml_matches_phase5_production_values():
    cfg = RiskConfig.from_yaml(ROOT / "config" / "bot.yaml")
    assert (cfg.daily_pct, cfg.weekly_pct, cfg.monthly_pct) == (5.0, 10.0, 15.0)
    assert (cfg.daily_abs_usd, cfg.weekly_abs_usd, cfg.monthly_abs_usd) == (10.0, 20.0, 30.0)
    assert cfg.cooldown_minutes == 60.0 and cfg.max_capital_usd == 1000.0


async def test_killswitch_built_like_orchestrator_phase5(db):
    ks = stack(db).risk.kill_switch
    assert (ks.daily_limit, ks.weekly_limit, ks.monthly_limit) == (-0.05, -0.10, -0.15)
    assert ks.cooldown == timedelta(hours=1)
    assert (ks.daily_loss_abs_usd, ks.weekly_loss_abs_usd, ks.monthly_loss_abs_usd) == (10.0, 20.0, 30.0)


async def test_drawdown_helper():
    assert drawdown(95, 100) == pytest.approx(0.05)
    assert drawdown(105, 100) == 0.0 and drawdown(1, 0) == 0.0


# ── flat -> alert only; position -> close + block (callback) ────────────

async def test_flat_portfolio_trigger_is_alert_only(db):
    s = stack(db)
    await s.risk._on_trigger(KillLevel.PORTFOLIO, "weekly dd")
    assert s.sc.closed == []
    assert s.risk.alert_only is True
    k = s.notifier.of("kill_switch")
    assert len(k) == 1 and "알림만" in k[0]["action"] and k[0]["level"] == "PORTFOLIO"
    assert s.state.entries_blocked() == (False, "")  # entries stay allowed
    assert s.state.get(PORTFOLIO_BLOCK_UNTIL) is None


async def test_position_portfolio_trigger_closes_and_blocks(db):
    s = stack(db)
    s.sc.position = pos(0.01)
    closed_cb = []

    async def cb(reason):
        closed_cb.append(reason)

    s.risk._on_position_closed = cb
    await s.risk.evaluate(100.0, s.sc.position)  # sets position context
    s.risk.kill_switch._active_level = KillLevel.NONE
    await s.risk.kill_switch.trigger(KillLevel.PORTFOLIO, "daily dd")
    assert s.sc.closed == ["kill_switch"] and closed_cb == ["kill_switch"]
    assert s.risk.alert_only is False
    blocked, why = s.state.entries_blocked()
    assert blocked and why.startswith("kill_switch")
    blk = s.state.get(PORTFOLIO_BLOCK_UNTIL)
    at = s.risk.kill_switch.triggered_at
    assert datetime.fromisoformat(blk["until"]) == at + timedelta(minutes=60)
    k = s.notifier.of("kill_switch")[0]
    assert "청산" in k["action"]
    assert s.state.get(KEY_KILL_STATE)["level"] == 2


@pytest.mark.parametrize("raw", ["error", "present"])
async def test_unknown_or_present_position_is_not_flat(db, raw):
    s = stack(db)
    if raw == "error":
        s.sc.pos_failures = 5
    else:
        s.sc.position = pos(0.01)
    await s.risk._on_trigger(KillLevel.PORTFOLIO, "weekly dd")
    assert s.sc.closed == ["kill_switch"] and s.risk.alert_only is False


async def test_system_level_closes_even_when_flat_and_manual_does_not_close_in_callback(db):
    s = stack(db)
    await s.risk._on_trigger(KillLevel.SYSTEM, "x")
    assert s.sc.closed == ["kill_switch"] and s.risk.alert_only is False
    s2 = stack(db)
    await s2.risk._on_trigger(KillLevel.MANUAL, "x")
    assert s2.sc.closed == []  # emergency_close does the closing itself


@pytest.mark.parametrize("level,action", [
    (KillLevel.STRATEGY, "전략 일시정지"), (KillLevel.SYSTEM, "시스템 차단"),
    (KillLevel.MANUAL, "수동 긴급 청산")])
async def test_kill_level_action_text(db, level, action):
    s = stack(db)
    await s.risk._on_trigger(level, "test_reason")
    k = s.notifier.of("kill_switch")[0]
    assert k["action"] == action and k["level"] == level.name and k["reason"] == "test_reason"


async def test_notifier_failure_free_callback_even_when_close_raises(db):
    s = stack(db)
    s.sc.position = pos(0.01)

    async def boom(reason):
        raise RuntimeError("close failed")

    s.sc.close_position = boom
    await s.risk._on_trigger(KillLevel.PORTFOLIO, "dd")  # must not raise
    assert s.notifier.of("anomaly")[0]["kind"] == "kill_switch_close_failed"
    assert s.state.entries_blocked()[0] is True  # still blocked
    assert s.notifier.of("kill_switch")


# ── evaluate: full cycle ────────────────────────────────────────────────

async def test_drawdown_sent_as_percent_not_fraction(db):
    s = stack(db)
    set_peaks(s, 1000.0)
    st = await s.risk.evaluate(900.0, None)  # -10 %, 100 USD
    assert st.daily_dd == pytest.approx(0.10) and st.level == KillLevel.PORTFOLIO
    k = s.notifier.of("kill_switch")[0]
    assert k["daily_dd_pct"] == pytest.approx(10.0) and k["weekly_dd_pct"] == pytest.approx(10.0)


async def test_pct_and_abs_must_both_breach(db):
    s = stack(db)
    set_peaks(s, 100.0)
    st = await s.risk.evaluate(94.0, None)  # 6 % but only 6 USD (< 10 USD abs)
    assert st.level == KillLevel.NONE and s.notifier.of("kill_switch") == []
    s2 = stack(db)
    set_peaks(s2, 1000.0)
    st = await s2.risk.evaluate(960.0, None)  # 40 USD abs but only 4 % (< 5 %)
    assert st.level == KillLevel.NONE


async def test_flat_trigger_cycle_is_alert_only_and_entries_allowed(db):
    s = stack(db)
    set_peaks(s, 198.9)
    st = await s.risk.evaluate(175.93, None)
    assert st.level == KillLevel.PORTFOLIO and st.alert_only and st.newly_triggered
    assert s.state.entries_blocked()[0] is False and s.sc.closed == []


async def test_no_retrigger_after_cooldown_for_same_loss(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    ks = s.risk.kill_switch
    assert ks.is_triggered
    ks._triggered_at -= timedelta(minutes=61)  # cooldown elapsed
    st = await s.risk.evaluate(175.93, None)
    assert not ks.is_triggered and st.level == KillLevel.NONE  # same drawdown does not re-trigger
    assert s.risk.peaks["weekly"] == pytest.approx(175.93)
    assert s.state.get(KEY_PEAK_REBASE_AT) is not None
    assert len(s.notifier.of("kill_switch")) == 1
    assert s.risk.alert_only is False


async def test_new_loss_after_rebase_still_triggers(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    s.risk.kill_switch._triggered_at -= timedelta(minutes=61)
    await s.risk.evaluate(175.93, None)
    assert not s.risk.kill_switch.is_triggered
    await s.risk.evaluate(150.0, None)  # fresh -14.7 % / 25.9 USD
    assert s.risk.kill_switch.is_triggered and len(s.notifier.of("kill_switch")) == 2


async def test_in_cooldown_no_second_alert(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    await s.risk.evaluate(175.93, None)
    await s.risk.evaluate(170.0, None)
    assert len(s.notifier.of("kill_switch")) == 1


async def test_in_position_cooldown_elapsed_unblocks_entries(db):
    s = stack(db)
    s.sc.position = pos(0.01)
    set_peaks(s, 198.9)
    st = await s.risk.evaluate(175.93, s.sc.position)
    assert st.level == KillLevel.PORTFOLIO and not st.alert_only
    assert s.sc.closed == ["kill_switch"] and s.state.entries_blocked()[0] is True
    s.sc.position = None
    await s.risk.evaluate(175.93, None)
    assert s.state.entries_blocked()[0] is True  # still cooling down
    s.risk.kill_switch._triggered_at -= timedelta(minutes=61)
    # the persisted deadline lapses with the cooldown; risk clears it and rebases
    s.state._data[PORTFOLIO_BLOCK_UNTIL]["until"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    await s.risk.evaluate(175.93, None)
    assert s.state.entries_blocked() == (False, "") and s.state.get(PORTFOLIO_BLOCK_UNTIL) is None
    assert s.state.get(KEY_KILL_STATE) is None


async def test_alert_only_promoted_when_position_appears(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    assert s.risk.alert_only
    s.sc.position = pos(0.01)  # strategy entered while entries were allowed
    st = await s.risk.evaluate(175.93, s.sc.position)
    assert s.sc.closed == ["kill_switch"] and not s.risk.alert_only and not st.alert_only
    assert s.state.entries_blocked()[0] is True
    assert "포지션 감지" in s.notifier.of("kill_switch")[-1]["action"]


async def test_position_unknown_fetches_from_executor(db):
    s = stack(db)
    s.sc.position = pos(0.01)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None, position_known=False)
    assert s.sc.closed == ["kill_switch"]  # executor said "in position"


# ── peaks ───────────────────────────────────────────────────────────────

async def test_period_peaks_roll_at_day_week_month_boundaries(db):
    clock = FakeClock(datetime(2026, 11, 18, 12, tzinfo=timezone.utc))  # Wednesday
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    r = s.risk
    await r.evaluate(100.0, None)
    assert r.peaks == {"daily": 100.0, "weekly": 100.0, "monthly": 100.0}
    await r.evaluate(110.0, None)
    clock.set(datetime(2026, 11, 19, 0, 0, 5, tzinfo=timezone.utc))  # Thursday: new day only
    await r.evaluate(105.0, None)
    assert r.peaks == {"daily": 105.0, "weekly": 110.0, "monthly": 110.0}
    clock.set(datetime(2026, 11, 23, 0, 0, 5, tzinfo=timezone.utc))  # Monday: new week
    await r.evaluate(104.0, None)
    assert r.peaks == {"daily": 104.0, "weekly": 104.0, "monthly": 110.0}
    clock.set(datetime(2026, 12, 1, 0, 0, 5, tzinfo=timezone.utc))  # the 1st: new month
    await r.evaluate(103.0, None)
    assert r.peaks["monthly"] == 103.0
    assert r.daily_dd == 0.0


async def test_peaks_persist_and_are_restored(db):
    clock = FakeClock(datetime(2026, 11, 18, 12, tzinfo=timezone.utc))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s.risk.evaluate(120.0, None)
    await s.risk.evaluate(110.0, None)
    s2 = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s2.state.load()
    await s2.risk.load()
    assert s2.risk.peaks == {"daily": 120.0, "weekly": 120.0, "monthly": 120.0}


async def test_peaks_rebuilt_from_snapshots_when_nothing_persisted(db):
    clock = FakeClock(datetime(2026, 11, 18, 15, tzinfo=timezone.utc))
    async with db.pool.acquire() as c:
        for hours, eq in ((5, 198.9), (1, 175.93)):
            await c.execute("INSERT INTO portfolio_snapshots (total_equity, snapshot_at) VALUES ($1,$2)",
                            eq, clock.now() - timedelta(hours=hours))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s.state.load()
    await s.risk.load()
    assert s.risk.peaks["weekly"] == pytest.approx(198.9)


async def test_rebase_floor_survives_restart(db):
    clock = FakeClock(datetime(2026, 11, 18, 15, tzinfo=timezone.utc))
    async with db.pool.acquire() as c:
        for hours, eq in ((5, 198.9), (1, 175.93)):
            await c.execute("INSERT INTO portfolio_snapshots (total_equity, snapshot_at) VALUES ($1,$2)",
                            eq, clock.now() - timedelta(hours=hours))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s.state.set(KEY_PEAK_REBASE_AT, (clock.now() - timedelta(hours=2)).isoformat())
    await s.state.load()
    await s.risk.load()
    assert s.risk.peaks["weekly"] == pytest.approx(175.93)  # pre-rebase history ignored


# ── restart survival of kill state ──────────────────────────────────────

async def test_kill_state_and_block_survive_restart(db):
    s = stack(db)
    s.sc.position = pos(0.01)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, s.sc.position)
    ks = s.risk.kill_switch
    assert ks.level == KillLevel.PORTFOLIO
    s2 = stack(db)  # simulated restart: everything rebuilt from Postgres
    await s2.state.load()
    await s2.risk.load()
    ks2 = s2.risk.kill_switch
    assert ks2.level == KillLevel.PORTFOLIO and ks2.triggered_at == ks.triggered_at
    assert ks2.reason == ks.reason and s2.risk.alert_only is False
    assert s2.state.entries_blocked()[0] is True  # persisted deadline still in the future
    # and the restored switch behaves: no second trigger, cooldown handled as before
    s2.sc.position = None
    st = await s2.risk.evaluate(175.93, None)
    assert not st.newly_triggered and s2.notifier.of("kill_switch") == []


async def test_alert_only_flag_survives_restart(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    s2 = stack(db)
    await s2.state.load()
    await s2.risk.load()
    assert s2.risk.alert_only is True and s2.risk.level == KillLevel.PORTFOLIO
    assert s2.state.entries_blocked()[0] is False


async def test_manual_halt_survives_restart(db):
    s = stack(db)
    await s.risk.manual_halt("tester")
    s2 = stack(db)
    await s2.state.load()
    await s2.risk.load()
    assert s2.state.entries_blocked()[0] is True and s2.risk.level == KillLevel.MANUAL
    assert await s2.risk.resume("tester") is True
    assert s2.state.entries_blocked()[0] is False and s2.risk.level == KillLevel.NONE


async def test_rebase_after_restart_when_cooldown_elapsed_while_down(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    st = s.state.get(KEY_KILL_STATE)
    st["triggered_at"] = (datetime.now(timezone.utc) - timedelta(minutes=90)).isoformat()
    await s.state.set(KEY_KILL_STATE, st)
    s2 = stack(db)
    await s2.state.load()
    await s2.risk.load()
    out = await s2.risk.evaluate(175.93, None)
    assert out.level == KillLevel.NONE and s2.notifier.of("kill_switch") == []


# ── manual halt / resume ────────────────────────────────────────────────

async def test_manual_halt_and_resume(db):
    s = stack(db)
    await s.risk.manual_halt("tester", "because")
    h = s.state.get(MANUAL_HALT)
    assert h["active"] and h["by"] == "tester" and h["reason"] == "because"
    assert s.risk.level == KillLevel.MANUAL
    assert s.state.entries_blocked()[0] is True
    assert s.notifier.of("kill_switch")[0]["level"] == "MANUAL"
    st = await s.risk.evaluate(100.0, None)  # MANUAL never auto-resumes
    assert st.level == KillLevel.MANUAL
    assert await s.risk.resume("tester") is True
    assert s.state.get(MANUAL_HALT) is None and s.risk.level == KillLevel.NONE
    assert s.state.get(KEY_KILL_STATE) is None
    assert await s.risk.resume("tester") is False


async def test_manual_halt_overrides_active_portfolio_trigger(db):
    s = stack(db)
    set_peaks(s, 198.9)
    await s.risk.evaluate(175.93, None)
    await s.risk.manual_halt("op")
    assert s.risk.level == KillLevel.MANUAL
    s.risk.kill_switch._triggered_at -= timedelta(hours=5)
    assert (await s.risk.evaluate(175.93, None)).level == KillLevel.MANUAL  # no auto-resume


# ── snapshots ───────────────────────────────────────────────────────────

async def test_snapshot_hourly_limit_and_force(db):
    clock = FakeClock(datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s.risk.evaluate(100.0, None)
    assert await s.risk.record_snapshot(100.0) is True
    clock.advance(1800)
    assert await s.risk.record_snapshot(100.0) is False  # < 1 h
    assert await s.risk.record_snapshot(100.0, force=True) is True  # after a fill
    clock.advance(3600)
    assert await s.risk.record_snapshot(101.0, position_count=1, unrealized=2.5) is True
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM portfolio_snapshots ORDER BY id")
    assert len(rows) == 3
    r = rows[-1]
    assert r["total_equity"] == 101.0 and r["unrealized_pnl"] == 2.5 and r["realized_pnl_today"] == 0.0
    strat = json.loads(r["strategies"])
    assert strat[0]["position_count"] == 1 and strat[0]["strategy_id"] == "supertrend-01"
    assert r["daily_drawdown"] == 0.0


async def test_snapshot_not_rewritten_right_after_restart(db):
    clock = FakeClock(datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s.risk.record_snapshot(100.0)
    s2 = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s2.risk.load()
    assert await s2.risk.record_snapshot(100.0) is False


async def test_sharpe_needs_history(db):
    clock = FakeClock(datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    s = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    assert s.risk._sharpe_30d(clock.now()) is None
    for i in range(20):
        clock.advance(900)
        await s.risk.evaluate(100.0 + (i % 3), None)
    assert isinstance(s.risk._sharpe_30d(clock.now()), float)
