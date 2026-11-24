"""BotState: persistence, in-memory reads, entry gate."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core_helpers import FakeClock, db  # noqa: F401
from bot.interfaces import EntryGate
from bot.state import MANUAL_HALT, PORTFOLIO_BLOCK_UNTIL, BotState, SystemClock, parse_ts

pytestmark = pytest.mark.asyncio


async def test_set_get_delete_roundtrip_and_persistence(db):
    s = BotState(db)
    await s.set("a", {"x": [1, 2], "y": "한글"})
    await s.set("n", 5)
    assert s.get("a") == {"x": [1, 2], "y": "한글"} and s.get("missing", 7) == 7
    s2 = BotState(db)
    assert s2.get("a") is None  # nothing until load()
    await s2.load()
    assert s2.get("a") == {"x": [1, 2], "y": "한글"} and s2.get("n") == 5
    await s2.delete("a")
    s3 = BotState(db)
    await s3.load()
    assert s3.get("a") is None and s3.get("n") == 5


async def test_set_overwrites_and_updates_timestamp(db):
    s = BotState(db)
    await s.set("k", 1)
    await s.set("k", 2)
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT key, value::text AS v FROM bot_state")
    assert [(r["key"], r["v"]) for r in rows] == [("k", "2")]


async def test_memory_updated_even_if_db_write_fails(db):
    s = BotState(db)
    await db.close()
    with pytest.raises(RuntimeError):
        await s.set(MANUAL_HALT, {"active": True, "reason": "x"})
    assert s.entries_blocked()[0] is True  # a halt is never lost to a DB hiccup


async def test_implements_entry_gate(db):
    assert isinstance(BotState(db).entries_blocked, object)
    gate: EntryGate = BotState(db)
    assert gate.entries_blocked() == (False, "")


async def test_manual_halt_blocks_until_cleared(db):
    s = BotState(db)
    await s.set(MANUAL_HALT, {"active": True, "reason": "ops", "by": "t"})
    blocked, why = s.entries_blocked()
    assert blocked and why.startswith("kill_switch") and "ops" in why
    await s.set(MANUAL_HALT, {"active": False})
    assert s.entries_blocked() == (False, "")
    await s.set(MANUAL_HALT, {"active": True})
    await s.delete(MANUAL_HALT)
    assert s.entries_blocked() == (False, "")


async def test_portfolio_block_uses_deadline_and_clock(db):
    clock = FakeClock(datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    s = BotState(db, clock=clock)
    until = clock.now() + timedelta(minutes=60)
    await s.set(PORTFOLIO_BLOCK_UNTIL, {"until": until.isoformat(), "reason": "PORTFOLIO: dd"})
    blocked, why = s.entries_blocked()
    assert blocked and why.startswith("kill_switch") and "dd" in why
    clock.advance(59 * 60)
    assert s.entries_blocked()[0] is True
    clock.advance(2 * 60)
    assert s.entries_blocked() == (False, "")


async def test_block_deadline_survives_restart(db):
    clock = FakeClock(datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    s = BotState(db, clock=clock)
    await s.set(PORTFOLIO_BLOCK_UNTIL, {"until": (clock.now() + timedelta(minutes=30)).isoformat()})
    await s.set(MANUAL_HALT, {"active": True, "reason": "r"})
    s2 = BotState(db, clock=clock)
    await s2.load()
    assert s2.entries_blocked()[0] is True
    await s2.delete(MANUAL_HALT)
    assert s2.entries_blocked()[0] is True  # block deadline still in the future
    clock.advance(31 * 60)
    assert s2.entries_blocked()[0] is False


async def test_garbage_values_do_not_block(db):
    s = BotState(db)
    await s.set(PORTFOLIO_BLOCK_UNTIL, "not-a-dict")
    await s.set(MANUAL_HALT, "x")
    assert s.entries_blocked() == (False, "")
    await s.set(PORTFOLIO_BLOCK_UNTIL, {"until": "garbage"})
    assert s.entries_blocked() == (False, "")


async def test_load_picks_up_external_changes(db):
    s = BotState(db)
    other = BotState(db)  # e.g. the CLI process
    await other.set(MANUAL_HALT, {"active": True, "reason": "cli"})
    assert s.entries_blocked()[0] is False
    await s.load()
    assert s.entries_blocked()[0] is True


async def test_parse_ts_and_system_clock():
    assert parse_ts("2026-11-20T12:00:00").tzinfo is not None
    assert parse_ts(None) is None and parse_ts("x") is None
    assert SystemClock().now().tzinfo is not None
