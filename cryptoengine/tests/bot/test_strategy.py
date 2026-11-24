"""Strategy: pure evaluate(), Trader position sync, sizing, entry/exit, same-bar retry, cooldowns."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core_helpers import (BAR_MS, SYM, FakeClock, ScriptedExecutor, build_stack, db,  # noqa: F401
                          fast_executor_timings, filled, make_rows, ms, partial, pos, rejected,
                          rows_to_candles, settle)
from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL, BotState
from bot.strategy import (ATR_MULT, KEY_ATR_COOLDOWN_UNTIL, KEY_LAST_LIQUIDATION_TS, MIN_BARS, Trader,
                          candle_from_ohlcv, entry_sizing, evaluate, remainder_is_dust)
from fakes.exchange import FakeExchange
from fakes.notifier import FakeNotifier

pytestmark = pytest.mark.asyncio

END_OPEN = ms(datetime(2026, 11, 20, 4, tzinfo=timezone.utc))  # last closed bar opens 04:00
NOW = datetime(2026, 11, 20, 8, 0, 3, tzinfo=timezone.utc)  # right after it closed (bar 04:00 closed 08:00)


def up_rows(n=400):
    return make_rows(n, END_OPEN)


def up_candles(n=400):
    return rows_to_candles(up_rows(n))


def exit_candles():
    """Uptrend ending in a sharp fall so fast EMA < slow EMA."""
    for tail in range(1, 30):
        c = rows_to_candles(make_rows(400, END_OPEN, tail_drift=-0.02, tail=tail))
        s = evaluate(c, has_position=True, entry_price=1.0)
        if s and s.exit_reason == "ema_cross":
            return c
    raise AssertionError("no exit series found")


# ── pure evaluate ───────────────────────────────────────────────────────

async def test_evaluate_entry_on_uptrend():
    s = evaluate(up_candles(), has_position=False)
    assert s.entry_ok and s.expected_action == "enter" and s.st_dir == 1
    assert s.fast_ema > s.slow_ema and s.price > s.dir_ema
    assert s.bar_ts == END_OPEN and not s.exit_signal


async def test_evaluate_insufficient_bars():
    assert evaluate(up_candles(MIN_BARS - 1), has_position=False) is None


async def test_evaluate_cooldowns_block_entry():
    c = up_candles()
    assert not evaluate(c, has_position=False, last_liquidation_ts=END_OPEN).entry_ok
    assert not evaluate(c, has_position=False, atr_cooldown_until=END_OPEN).entry_ok
    # the bar after the cooldown bar is allowed again
    assert evaluate(c, has_position=False, atr_cooldown_until=END_OPEN - BAR_MS).entry_ok


async def test_evaluate_ema_cross_exit():
    s = evaluate(exit_candles(), has_position=True, entry_price=1.0)
    assert s.exit_signal and s.exit_reason == "ema_cross" and s.expected_action == "exit"
    assert not s.entry_ok


async def test_evaluate_atr_exit_is_downside_only():
    c = up_candles()
    base = evaluate(c, has_position=True, entry_price=1.0)
    assert not base.exit_signal  # price far above entry: upside never exits
    entry = base.price + base.atr_14 * ATR_MULT + 1.0
    s = evaluate(c, has_position=True, entry_price=entry)
    assert s.exit_signal and s.exit_reason == "atr_distance"
    # exactly at the boundary exits (<=)
    s2 = evaluate(c, has_position=True, entry_price=base.price + base.atr_14 * ATR_MULT)
    assert s2.exit_reason == "atr_distance"


async def test_sizing_and_dust():
    qty, sl = entry_sizing(1000.0, 100_000.0)
    assert qty == pytest.approx(0.0285) and sl == pytest.approx(100_000 * (1 - 0.70 / 3))
    assert remainder_is_dust(0.0, 100_000) and remainder_is_dust(0.0005, 100_000)
    assert remainder_is_dust(0.001, 50_000)  # 50 USD notional < 65
    assert not remainder_is_dust(0.002, 100_000)


async def test_candle_from_ohlcv_variants():
    row = [1_000, 1, 2, 0.5, 1.5, 9]
    assert candle_from_ohlcv(row) == {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                                      "volume": 9.0, "ts": 1000}
    assert candle_from_ohlcv({"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 9, "ts": 1000})["ts"] == 1000


# ── trader fixtures ─────────────────────────────────────────────────────

@pytest.fixture
def clock():
    return FakeClock(NOW)


@pytest.fixture
def scripted():
    return ScriptedExecutor()


@pytest_asyncio.fixture
async def st(db, scripted, clock):
    s = build_stack(db, clock=clock, scripted=scripted, retry_delay=0.01)
    yield s
    await s.trader.shutdown()


# ── position truth sync (ported from test_strategy_state_sync) ──────────

async def test_sync_divergence_corrected_to_exchange(st, scripted):
    scripted.position = pos(0.007)
    assert await st.trader.sync_position("test") is True
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(0.007)
    assert st.trader.entry_price == pytest.approx(100_000.0)


async def test_sync_phantom_position_cleared(st, scripted):
    scripted.position = pos(0.007)
    await st.trader.sync_position("t")
    scripted.position = None
    await st.trader.sync_position("t")
    assert not st.trader.has_position and st.trader.position_qty == 0.0 and st.trader.entry_price == 0.0


async def test_sync_api_failure_keeps_belief_after_three_tries(st, scripted):
    scripted.position = pos(0.007)
    await st.trader.sync_position("t")
    scripted.pos_failures = 3
    assert await st.trader.sync_position("t") is False
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(0.007)
    assert st.clock.sleeps == [1.0, 2.0]


async def test_sync_transient_failure_recovers(st, scripted):
    scripted.position = pos(0.007)
    scripted.pos_failures = 1
    assert await st.trader.sync_position("t") is True
    assert st.trader.has_position


async def test_sync_unexpected_short_is_flat(st, scripted):
    scripted.position = pos(0.01, side="short")
    await st.trader.sync_position("t")
    assert not st.trader.has_position


async def test_sync_writes_positions_row_and_closes_it(st, scripted, db):
    scripted.position = pos(0.007, entry=99_000.0)
    await st.trader.sync_position("t")
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM positions WHERE closed_at IS NULL")
    assert r["symbol"] == SYM and float(r["size"]) == pytest.approx(0.007) and r["side"] == "long"
    scripted.position = None
    await st.trader.sync_position("t")
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM positions")
    assert r["closed_at"] is not None and r["close_reason"] == "exchange_flat"


# ── capital ─────────────────────────────────────────────────────────────

async def test_capital_min_of_wallet_cap_and_total(st, scripted):
    scripted.balance.update(wallet=5000.0, total=5000.0)
    assert await st.trader._capital() == (1000.0, 1000.0)
    scripted.balance.update(wallet=200.0, total=150.0)
    assert await st.trader._capital() == (200.0, 150.0)  # live equity below allocation
    scripted.balance.update(wallet=200.0, total=250.0)
    assert await st.trader._capital() == (200.0, 200.0)


async def test_capital_balance_error_uses_last_known_then_zero(st, scripted):
    scripted.balance_fail = True
    assert await st.trader._capital() == (0.0, 0.0)
    scripted.balance_fail = False
    scripted.balance.update(wallet=300.0, total=300.0)
    await st.trader._capital()
    scripted.balance_fail = True
    assert await st.trader._capital() == (300.0, 300.0)


# ── on_bar: entry / exit with scripted executor ─────────────────────────

async def test_entry_confirmed_only_on_fill_and_persisted(st, scripted, db):
    scripted.results = [filled(0.0285, 99_950.0)]
    await st.trader.on_bar(up_candles())
    o = scripted.orders[0]
    price = up_candles()[-1]["close"]
    assert o.side == "buy" and o.order_type == "limit" and o.post_only and not o.reduce_only
    assert o.quantity == pytest.approx(1000 * 0.95 * 3 / price)
    assert o.price == pytest.approx(price)
    assert o.stop_loss == pytest.approx(price * (1 - 0.70 / 3))
    assert st.trader.has_position and st.trader.entry_price == pytest.approx(99_950.0)
    assert st.notifier.of("entry") and st.notifier.of("entry")[0]["stop_loss"] == pytest.approx(o.stop_loss)
    async with db.pool.acquire() as c:
        sig = await c.fetchrow("SELECT * FROM supertrend_signals")
        p = await c.fetchrow("SELECT * FROM positions")
    assert sig["entry_ok"] and sig["expected_action"] == "enter" and not sig["had_position"]
    assert sig["expected_qty"] == pytest.approx(o.quantity)
    assert sig["expected_stop_loss"] == pytest.approx(o.stop_loss)
    assert sig["allocated_capital"] == pytest.approx(1000.0)
    assert sig["st_line"] is not None
    assert int(sig["bar_ts"].timestamp() * 1000) == END_OPEN
    assert p["closed_at"] is None and float(p["entry_price"]) == pytest.approx(99_950.0)


async def test_entry_not_assumed_on_rejection_then_retry_remainder_only(st, scripted):
    """Rejected -> no position believed, re-sync from exchange, retry full target once."""
    scripted.results = [rejected("post_only_failed"), filled(0.0285)]
    await st.trader.on_bar(up_candles())
    assert not st.trader.has_position
    assert st.notifier.of("order_rejected")[0]["will_retry"] is True
    await settle(st.trader)
    assert len(scripted.orders) == 2
    assert scripted.orders[1].quantity == pytest.approx(scripted.orders[0].quantity)
    assert scripted.orders[1].request_id != scripted.orders[0].request_id
    assert st.trader.has_position


async def test_partial_entry_retries_remainder_only(st, scripted):
    q = 1000 * 0.95 * 3 / up_candles()[-1]["close"]
    scripted.results = [partial(0.01), filled(q - 0.01)]
    scripted.on_execute = lambda o, r: setattr(scripted, "position", pos(0.01)) if r.status == "partial" else None
    await st.trader.on_bar(up_candles())
    assert st.trader.has_position and st.trader.position_qty == pytest.approx(0.01)  # from exchange sync
    await settle(st.trader)
    assert scripted.orders[1].quantity == pytest.approx(q - 0.01)
    assert scripted.orders[1].quantity != pytest.approx(q)


async def test_dust_remainder_after_fill_is_not_retried(st, scripted):
    q = 1000 * 0.95 * 3 / up_candles()[-1]["close"]
    scripted.results = [filled(q - 0.0004)]  # 0.0004 BTC short: below min qty
    await st.trader.on_bar(up_candles())
    assert st.trader._retry_task is None
    assert len(scripted.orders) == 1


async def test_non_dust_remainder_after_filled_is_retried(st, scripted):
    q = 1000 * 0.95 * 3 / up_candles()[-1]["close"]
    scripted.results = [filled(0.01), filled(q - 0.01)]
    scripted.on_execute = lambda o, r: setattr(scripted, "position", pos(0.01 if len(scripted.orders) == 1 else q))
    await st.trader.on_bar(up_candles())
    await settle(st.trader)
    assert scripted.orders[1].quantity == pytest.approx(q - 0.01)


async def test_kill_switch_reason_does_not_retry(st, scripted):
    scripted.results = [rejected("kill_switch_active: blocked")]
    await st.trader.on_bar(up_candles())
    assert st.trader._retry_task is None
    assert st.notifier.of("order_rejected")[0]["will_retry"] is False
    await asyncio.sleep(0.05)
    assert len(scripted.orders) == 1


async def test_dry_run_rejection_neither_retries_nor_alerts(st, scripted):
    scripted.results = [rejected("dry_run")]
    await st.trader.on_bar(up_candles())
    assert st.trader._retry_task is None
    assert st.notifier.of("order_rejected") == [] and st.notifier.of("anomaly") == []


async def test_new_bar_cancels_pending_retry(st, scripted):
    st.trader._retry_delay = 3600  # keep it in the sleep phase
    st.clock.on_sleep = None

    async def slow_sleep(seconds):  # a sleep that really waits
        await asyncio.sleep(3600)

    st.clock.sleep = slow_sleep
    scripted.results = [rejected(), filled(0.0285)]
    await st.trader.on_bar(up_candles())
    t = st.trader._retry_task
    assert t is not None and not t.done()
    scripted.results = [rejected("kill_switch")]  # next bar: its own order
    await st.trader.on_bar(up_candles())
    await asyncio.sleep(0)
    assert t.cancelled() or t.done()
    assert len(scripted.orders) == 2  # the retry never fired, only the new bar's order


async def test_retry_skips_when_signal_gone(st, scripted):
    scripted.results = [rejected(), filled(0.0285)]
    await st.trader.on_bar(up_candles())
    st.trader._last_liquidation_ts = END_OPEN  # cooldown appears before the retry runs
    await settle(st.trader)
    assert len(scripted.orders) == 1


async def test_entry_skipped_when_position_unverifiable(st, scripted):
    scripted.pos_failures = 99
    await st.trader.on_bar(up_candles())
    assert scripted.orders == []  # never open a second position blind
    assert st.trader._retry_task is not None
    st.trader._generation += 1
    await st.trader.shutdown()


async def test_entry_skipped_when_too_small(db, clock):
    sc = ScriptedExecutor()
    sc.balance.update(wallet=20.0, total=20.0)  # 20*0.95*3 = 57 USD < 65
    s = build_stack(db, clock=clock, scripted=sc)
    await s.trader.on_bar(up_candles())
    assert sc.orders == []


async def test_entry_skipped_without_balance_then_retries(st, scripted):
    scripted.balance_fail = True
    scripted.results = [filled(0.0285)]
    await st.trader.on_bar(up_candles())
    assert scripted.orders == [] and st.trader._retry_task is not None
    scripted.balance_fail = False
    await settle(st.trader)
    assert len(scripted.orders) == 1 and st.trader.has_position


async def test_exit_cooldowns_only_on_confirmed_fill(st, scripted, db):
    scripted.position = pos(0.0057, entry=100_000.0)
    scripted.results = [rejected("net")]
    c = exit_candles()
    await st.trader.on_bar(c)
    assert scripted.orders[0].reduce_only and scripted.orders[0].side == "sell"
    assert st.trader.last_liquidation_ts == 0  # rejected exit leaves no cooldown
    assert st.state.get(KEY_LAST_LIQUIDATION_TS) is None
    assert st.trader.has_position  # exchange still holds it
    scripted.results = [filled(0.0057, 90_000.0)]
    await settle(st.trader)
    assert scripted.orders[1].quantity == pytest.approx(0.0057) and scripted.orders[1].request_id != scripted.orders[0].request_id
    assert st.trader.last_liquidation_ts == c[-1]["ts"]
    assert st.state.get(KEY_LAST_LIQUIDATION_TS) == c[-1]["ts"]
    assert st.state.get(KEY_ATR_COOLDOWN_UNTIL) is None  # ema_cross: no ATR cooldown
    ex = st.notifier.of("exit")[0]
    assert ex["reason"] == "ema_cross" and ex["entry_price"] == pytest.approx(100_000.0)
    assert ex["pnl"] == pytest.approx((90_000.0 - 100_000.0) * 0.0057 - 0.1)
    async with db.pool.acquire() as cn:
        p = await cn.fetchrow("SELECT * FROM positions")
    assert p["closed_at"] is not None and p["close_reason"] == "ema_cross"


async def test_atr_exit_sets_atr_cooldown_and_blocks_next_bar_entry(st, scripted):
    c = up_candles()
    base = evaluate(c, has_position=True, entry_price=1.0)
    scripted.position = pos(0.01, entry=base.price + base.atr_14 * ATR_MULT + 5)
    scripted.results = [filled(0.01, base.price)]
    await st.trader.on_bar(c)
    assert st.trader.atr_cooldown_until == END_OPEN + BAR_MS
    assert st.state.get(KEY_ATR_COOLDOWN_UNTIL) == END_OPEN + BAR_MS
    # next bar (one step later) is still inside the cooldown: entry_ok False
    nxt = rows_to_candles(make_rows(401, END_OPEN + BAR_MS))
    sig = evaluate(nxt, has_position=False, last_liquidation_ts=st.trader.last_liquidation_ts,
                   atr_cooldown_until=st.trader.atr_cooldown_until)
    assert not sig.entry_ok


async def test_exit_retry_skipped_when_already_flat(st, scripted):
    scripted.position = pos(0.0057)
    scripted.results = [rejected("net")]
    await st.trader.on_bar(exit_candles())
    scripted.position = None  # exchange-side stop closed it meanwhile
    await settle(st.trader)
    assert len(scripted.orders) == 1 and not st.trader.has_position


async def test_signal_persisted_once_per_bar_and_in_progress_bar_dropped(st, scripted, db):
    scripted.position = pos(0.01)  # hold: no order
    inprog = up_candles()
    inprog.append({**inprog[-1], "ts": END_OPEN + BAR_MS})  # closes 12:00, still running at 08:00:03
    await st.trader.on_bar(inprog)
    await st.trader.on_bar(inprog)
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM supertrend_signals")
    assert len(rows) == 1 and int(rows[0]["bar_ts"].timestamp() * 1000) == END_OPEN
    assert rows[0]["expected_action"] == "hold" and rows[0]["had_position"]


# ── entries blocked, reduce-only still works ────────────────────────────

async def test_entry_blocked_by_manual_halt(st, scripted):
    await st.state.set(MANUAL_HALT, {"active": True, "reason": "test"})
    await st.trader.on_bar(up_candles())
    assert scripted.orders == []
    assert st.notifier.of("order_rejected")[0]["will_retry"] is False


async def test_entry_blocked_by_portfolio_block(st, scripted):
    until = (st.clock.now() + timedelta(minutes=30)).isoformat()
    await st.state.set(PORTFOLIO_BLOCK_UNTIL, {"until": until, "reason": "x"})
    await st.trader.on_bar(up_candles())
    assert scripted.orders == []
    st.clock.advance(3600)  # deadline passed -> entries allowed again
    scripted.results = [filled(0.0285)]
    await st.trader.on_bar(up_candles())
    assert len(scripted.orders) == 1


async def test_exit_still_works_while_blocked(st, scripted):
    await st.state.set(MANUAL_HALT, {"active": True, "reason": "test"})
    scripted.position = pos(0.01)
    scripted.results = [filled(0.01, 90_000.0)]
    await st.trader.on_bar(exit_candles())
    assert len(scripted.orders) == 1 and scripted.orders[0].reduce_only
    assert not st.trader.has_position


# ── missed bar on start ─────────────────────────────────────────────────

async def test_missed_bar_processed_once(st, scripted, db):
    scripted.position = pos(0.01)
    older = END_OPEN - BAR_MS
    async with db.pool.acquire() as c:
        await c.execute(
            "INSERT INTO supertrend_signals (bar_ts, st_dir, fast_ema, slow_ema, dir_ema, price, atr_14, "
            "allocated_capital, had_position, entry_ok, exit_signal, expected_action) "
            "VALUES (to_timestamp($1::bigint/1000.0),1,1,1,1,1,1,1,false,false,false,'hold')", older)
    cands = up_candles()
    assert await st.trader.process_missed_bar(cands) is True
    assert await st.trader.process_missed_bar(cands) is False  # signal row now exists
    async with db.pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM supertrend_signals")
    assert n == 2


async def test_missed_bar_not_triggered_on_empty_table(st):
    assert await st.trader.process_missed_bar(up_candles()) is False


# ── cooldown persistence across restart ─────────────────────────────────

async def test_cooldowns_survive_restart(st, scripted, db, clock):
    c = up_candles()
    base = evaluate(c, has_position=True, entry_price=1.0)
    scripted.position = pos(0.01, entry=base.price + base.atr_14 * ATR_MULT + 5)
    scripted.results = [filled(0.01, base.price)]
    await st.trader.on_bar(c)
    s2 = build_stack(db, clock=clock, scripted=ScriptedExecutor())
    await s2.state.load()
    s2.trader.load_state()
    assert s2.trader.atr_cooldown_until == END_OPEN + BAR_MS
    assert s2.trader.last_liquidation_ts == END_OPEN


# ── real executor on FakeExchange ───────────────────────────────────────

@pytest_asyncio.fixture
async def real(db, clock, monkeypatch):
    fast_executor_timings(monkeypatch)
    fx = FakeExchange(wallet=1000.0)
    c = up_candles()
    px = c[-1]["close"]
    fx.set_book(px - 5, px + 5)
    s = build_stack(db, clock=clock, fx=fx, retry_delay=0.01)
    yield s
    await s.trader.shutdown()


async def test_real_executor_entry_places_limit_with_inline_stop(real, db):
    await real.trader.on_bar(up_candles())
    placed = real.fx.placed[0]
    px = up_candles()[-1]["close"]
    assert placed.order_type == "limit" and placed.post_only and placed.stop_loss == pytest.approx(px * (1 - 0.7 / 3))
    assert real.fx.position is not None and real.trader.has_position
    async with db.pool.acquire() as c:
        o = await c.fetchrow("SELECT * FROM orders ORDER BY id LIMIT 1")
    assert o["status"] == "filled" and o["side"] == "buy"
    assert real.trader.position_qty == pytest.approx(real.fx.position.size, rel=0.02)


async def test_real_executor_rejection_then_retry_enters(real):
    real.fx.set_balance(free=10.0)  # SafetyGuard rejects: not enough free margin
    await real.trader.on_bar(up_candles())
    assert real.fx.placed == [] and real.fx.position is None and not real.trader.has_position
    assert real.notifier.of("order_rejected")
    real.fx.set_balance(free=None)
    await settle(real.trader)
    assert real.fx.position is not None and real.trader.has_position


async def test_real_executor_exit_while_blocked_and_close_position_works(real):
    real.fx.set_position("long", 0.01, 99_000.0)
    await real.state.set(MANUAL_HALT, {"active": True, "reason": "halt"})
    await real.trader.on_bar(exit_candles())
    assert real.fx.position is None  # reduce-only exit passed the gate
    real.fx.set_position("long", 0.01, 99_000.0)
    res = await real.executor.close_position("emergency")
    assert res is not None and real.fx.position is None  # close works while entries are blocked


async def test_real_executor_blocked_entry_places_nothing(real):
    await real.state.set(MANUAL_HALT, {"active": True, "reason": "halt"})
    await real.trader.on_bar(up_candles())
    assert real.fx.placed == []


async def test_real_dry_run_no_retry_no_alert(db, clock, monkeypatch):
    fast_executor_timings(monkeypatch)
    fx = FakeExchange(wallet=1000.0)
    s = build_stack(db, clock=clock, fx=fx, dry_run=True)
    await s.trader.on_bar(up_candles())
    assert fx.placed == [] and s.trader._retry_task is None
    assert s.notifier.of("order_rejected") == [] and s.notifier.of("anomaly") == []


# ── verification script reproduces what the Trader recorded ─────────────

async def test_verify_script_matches_trader_output_and_flags_tampering(db, clock):
    import verify_live_signal_parity as v
    from datetime import datetime as _dt

    end = ms(_dt(2026, 11, 20, 4, tzinfo=timezone.utc))
    rows = make_rows(1100, end)
    cands = rows_to_candles(rows)
    async with db.pool.acquire() as c:
        await c.executemany(
            "INSERT INTO ohlcv_history (exchange,symbol,timeframe,timestamp,open,high,low,close,volume) "
            "VALUES ('bybit','BTCUSDT','4h',to_timestamp($1::bigint/1000.0),$2,$3,$4,$5,$6)",
            [(x["ts"], x["open"], x["high"], x["low"], x["close"], x["volume"]) for x in cands])
    sc = ScriptedExecutor()
    sc.position = pos(0.01)
    s = build_stack(db, clock=clock, scripted=sc)
    for k in range(5):  # five consecutive bars through the real Trader (holding a position)
        await s.trader.on_bar(cands[: len(cands) - 4 + k])
    async with db.pool.acquire() as c:
        rep = await v.verify(c)
        assert rep.compared == 5 and rep.mismatches == [] and rep.skipped_short == 0
        # too little history -> skipped, never reported as mismatches
        rep2 = await v.verify(c, min_bars=2000)
        assert rep2.compared == 0 and rep2.skipped_short == 5
        # a tampered stored value is reported
        await c.execute("UPDATE supertrend_signals SET fast_ema = fast_ema * 1.01, st_dir = -1 "
                        "WHERE bar_ts = (SELECT MAX(bar_ts) FROM supertrend_signals)")
        rep3 = await v.verify(c)
    assert len(rep3.mismatches) == 1 and "fast_ema" in rep3.mismatches[0] and "st_dir" in rep3.mismatches[0]
    await s.trader.shutdown()
