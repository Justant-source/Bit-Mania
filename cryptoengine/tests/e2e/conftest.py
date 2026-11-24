"""Fixtures for the e2e suite (throwaway Postgres on 127.0.0.1:55432, database ``test_e2e``)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).resolve().parent))

from e2e_harness import (  # noqa: E402
    DB_NAME, PG, TABLES, E2EClock, E2ENotifier, FakeExchange, build_stack, ensure_database,
    fast_executor_timings, floor_boundary, patch_kill_switch_clock,
)

from bot.db import Database  # noqa: E402


@pytest_asyncio.fixture
async def db():
    await ensure_database()
    d = Database(host=PG["host"], port=PG["port"], name=DB_NAME, user=PG["user"],
                 password=PG["password"], max_size=8)
    await d.connect()
    await d.ensure_schema()
    async with d.pool.acquire() as c:
        await c.execute("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY")
    yield d
    await d.close()


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    fast_executor_timings(monkeypatch)
    yield
    from bot.logging_setup import attach_notifier

    attach_notifier(None)


@pytest.fixture
def clock(monkeypatch):
    """Virtual clock starting 3 s after the real 4h boundary 12 h ago (so a 3-bar scenario ends at the latest real boundary)."""
    from datetime import timedelta

    start = floor_boundary(datetime.now(timezone.utc)) - timedelta(hours=12) + timedelta(seconds=3)
    c = E2EClock(start)
    patch_kill_switch_clock(monkeypatch, c)
    return c


@pytest.fixture
def fx():
    return FakeExchange(wallet=1000.0)


@pytest.fixture
def make_stack(db, fx, clock):
    """Factory: ``await make_stack(feed=..., **kw)`` -> Stack. Reuses db/fx/clock (restarts)."""
    async def _make(**kw):
        kw.setdefault("notifier", E2ENotifier())
        return await build_stack(db, kw.pop("fx", fx), kw.pop("clock", clock), **kw)
    return _make


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: long-running e2e scenario")
