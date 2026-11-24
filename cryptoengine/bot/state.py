"""BotState: key/value store in Postgres (``bot_state``) plus the entry gate.

Everything that must survive a restart (manual halt, portfolio block deadline,
kill state, cooldown timestamps, period peaks, heartbeat) lives here. Reads are
served from an in-memory copy so ``entries_blocked`` can stay synchronous.
Writes update memory first and then Postgres, so a database hiccup can never
leave a halt un-applied in the running process.

Also home of the tiny ``Clock`` abstraction used by the scheduler, trader and
risk code so tests can drive time without sleeping.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Protocol

import structlog

from bot.db import Database

log = structlog.get_logger("bot.state")

_DELETED = object()

MANUAL_HALT = "manual_halt"
PORTFOLIO_BLOCK_UNTIL = "portfolio_block_until"


class Clock(Protocol):
    def now(self) -> datetime:
        """Timezone-aware UTC now."""

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


def parse_ts(value: Any) -> datetime | None:
    """ISO string -> aware UTC datetime. None on anything unparsable."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class BotState:
    """Key/value state in Postgres; implements ``EntryGate``."""

    def __init__(self, db: Database, *, clock: Clock | None = None) -> None:
        self._db = db
        self._clock: Clock = clock or SystemClock()
        self._data: dict[str, Any] = {}
        # Keys whose last write did not reach Postgres: value, or _DELETED.
        self._dirty: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    # ── persistence ─────────────────────────────────────────────────────

    async def load(self) -> None:
        """(Re)load every key from Postgres. Replaces the in-memory copy."""
        async with self._lock:
            async with self._db.pool.acquire() as conn:
                rows = await conn.fetch("SELECT key, value FROM bot_state")
            data: dict[str, Any] = {}
            for r in rows:
                v = r["value"]
                data[r["key"]] = json.loads(v) if isinstance(v, str) else v
            # Writes that failed earlier win over what the database holds, and
            # are retried now: a halt must never be dropped by a refresh.
            for key, value in list(self._dirty.items()):
                if value is _DELETED:
                    data.pop(key, None)
                else:
                    data[key] = value
                try:
                    await self._write(key, value)
                    self._dirty.pop(key, None)
                except Exception:
                    log.warning("state_write_retry_failed", key=key)
            self._data = data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    async def set(self, key: str, value: Any) -> None:
        """Memory first, then Postgres. If Postgres fails the value stays applied
        in memory, is retried on the next load(), and the error is re-raised."""
        async with self._lock:
            self._data[key] = value
            try:
                await self._write(key, value)
                self._dirty.pop(key, None)
            except Exception:
                self._dirty[key] = value
                raise

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._data.pop(key, None)
            try:
                await self._write(key, _DELETED)
                self._dirty.pop(key, None)
            except Exception:
                self._dirty[key] = _DELETED
                raise

    async def _write(self, key: str, value: Any) -> None:
        async with self._db.pool.acquire() as conn:
            if value is _DELETED:
                await conn.execute("DELETE FROM bot_state WHERE key = $1", key)
                return
            await conn.execute(
                """
                INSERT INTO bot_state (key, value, updated_at)
                VALUES ($1, $2::jsonb, now())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now()
                """,
                key,
                json.dumps(value),
            )

    # ── EntryGate ───────────────────────────────────────────────────────

    def entries_blocked(self) -> tuple[bool, str]:
        """Blocked = manual halt, or a portfolio block deadline in the future.

        Value formats (shared with web.py): ``manual_halt`` = {"active": true, "reason",
        "by", "at"}; ``portfolio_block_until`` = {"until": ISO, "reason"}.

        Reasons start with ``kill_switch`` so the trader's "never retry a
        kill-switch rejection" rule applies unchanged.
        """
        halt = self._data.get(MANUAL_HALT)
        if isinstance(halt, dict) and halt.get("active"):
            return True, f"kill_switch_manual_halt: {halt.get('reason') or '수동 정지 (/resume 필요)'}"
        blk = self._data.get(PORTFOLIO_BLOCK_UNTIL)
        until = parse_ts(blk.get("until")) if isinstance(blk, dict) else None
        if until is not None and self._clock.now() < until:
            return True, f"kill_switch_portfolio_block: until {until.isoformat()} ({blk.get('reason') or ''})"
        return False, ""

