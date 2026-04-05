"""Position Tracker — real-time position synchronisation for the Execution Engine.

Responsibilities:
  - Full position sync from exchange on startup
  - Cache positions in Redis  (cache:position:{exchange}:{symbol})
  - Track unrealized PnL per position
  - Detect position changes from WebSocket fill updates
  - Recovery after disconnect: resync all positions
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.exchange import ExchangeConnector, exchange_factory
from shared.models.position import Position
from shared.log_events import *

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSITION_CACHE_TTL = 120  # seconds
SYNC_INTERVAL = 60.0  # periodic full-sync interval
DISCONNECT_THRESHOLD = 30.0  # seconds without update before resync
WATCHED_SYMBOLS_KEY = "config:watched_symbols:{exchange}"


class PositionTracker:
    """Keeps an authoritative, cached view of all open positions."""

    def __init__(
        self,
        *,
        exchange: str,
        api_key: str,
        api_secret: str,
        testnet: bool,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
    ) -> None:
        self._exchange_id = exchange
        self._redis = redis
        self._db_pool = db_pool

        self._connector: ExchangeConnector = exchange_factory(
            exchange,
            api_key=api_key,
            api_secret=api_secret,
            testnet=testnet,
        )

        # In-memory position cache: symbol -> Position
        self._positions: dict[str, Position] = {}
        self._last_update: dict[str, float] = {}
        self._last_sync: float = 0.0
        self._connected = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def sync_from_exchange(self) -> None:
        """Full position sync from the exchange REST API.

        Called on startup and after detected disconnects.
        """
        if not self._connected:
            await self._connector.connect()
            self._connected = True

        symbols = await self._get_watched_symbols()
        synced = 0

        for symbol in symbols:
            try:
                position = await self._connector.get_position(symbol)
                if position is not None and position.size > 0:
                    self._positions[symbol] = position
                    await self._cache_position(position)
                    await self._persist_position(position)
                    synced += 1
                else:
                    # No open position -- clear stale cache
                    self._positions.pop(symbol, None)
                    await self._clear_position_cache(symbol)
                    await self._clear_position_db(symbol)
            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="position sync error", symbol=symbol)

            self._last_update[symbol] = time.monotonic()

        self._last_sync = time.monotonic()
        log.info(
            SERVICE_HEALTH_OK,
            message="positions synced",
            exchange=self._exchange_id,
            symbols_checked=len(symbols),
            open_positions=synced,
        )

    async def run(self, shutdown: asyncio.Event) -> None:
        """Background loop: periodic sync and stale-position detection."""
        log.info(SERVICE_STARTED, message="position tracker starting", exchange=self._exchange_id)

        try:
            while not shutdown.is_set():
                elapsed = time.monotonic() - self._last_sync
                if elapsed >= SYNC_INTERVAL:
                    await self.sync_from_exchange()

                # Check for stale positions (possible disconnect)
                now = time.monotonic()
                for symbol, last in list(self._last_update.items()):
                    if now - last > DISCONNECT_THRESHOLD and symbol in self._positions:
                        log.warning(
                            SERVICE_HEALTH_FAIL,
                            message="position stale detected",
                            symbol=symbol,
                            seconds_since_update=round(now - last, 1),
                        )
                        # Resync this symbol specifically
                        try:
                            position = await self._connector.get_position(symbol)
                            if position is not None and position.size > 0:
                                await self._apply_position_update(symbol, position)
                            else:
                                await self._remove_position(symbol)
                        except Exception:
                            log.exception(SERVICE_HEALTH_FAIL, message="stale resync error", symbol=symbol)
                        self._last_update[symbol] = now

                await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            pass
        finally:
            if self._connected:
                await self._connector.disconnect()
                self._connected = False
            log.info(SERVICE_STOPPED, message="position tracker stopped")

    async def on_order_fill(self, result: dict[str, Any]) -> None:
        """Called by ``ExecutionEngine`` when an order reaches a fill state.

        Triggers an incremental position refresh for the affected symbol.
        """
        symbol = result.get("symbol", "")
        if not symbol:
            return

        try:
            position = await self._connector.get_position(symbol)
            if position is not None and position.size > 0:
                await self._apply_position_update(symbol, position)
            else:
                await self._remove_position(symbol)
        except Exception:
            log.exception(ORDER_FILLED, message="on order fill sync error", symbol=symbol)

        self._last_update[symbol] = time.monotonic()
        log.info(
            ORDER_FILLED,
            message="position updated from fill",
            symbol=symbol,
            status=result.get("status"),
            filled_qty=result.get("filled_qty"),
        )

    async def on_ws_position_update(self, data: dict[str, Any]) -> None:
        """Process a position change pushed via WebSocket.

        Expected keys: symbol, side, size, entry_price, unrealized_pnl,
        leverage, liquidation_price, margin_used.
        """
        symbol = data.get("symbol", "")
        if not symbol:
            return

        size = float(data.get("size", 0))
        if size <= 0:
            await self._remove_position(symbol)
            return

        position = Position(
            exchange=self._exchange_id,
            symbol=symbol,
            side=data.get("side", "long"),
            size=size,
            entry_price=float(data.get("entry_price", 0)),
            unrealized_pnl=float(data.get("unrealized_pnl", 0)),
            leverage=float(data.get("leverage", 1)),
            liquidation_price=float(data["liquidation_price"])
            if data.get("liquidation_price")
            else None,
            margin_used=float(data.get("margin_used", 0)),
        )

        prev = self._positions.get(symbol)
        if prev and prev.model_dump() != position.model_dump():
            log.info(
                SERVICE_HEALTH_OK,
                message="position change detected",
                symbol=symbol,
                prev_size=prev.size,
                new_size=position.size,
                prev_pnl=prev.unrealized_pnl,
                new_pnl=position.unrealized_pnl,
            )

        await self._apply_position_update(symbol, position)
        self._last_update[symbol] = time.monotonic()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_position(self, symbol: str) -> Position | None:
        """Return the locally cached position for *symbol*, or ``None``."""
        return self._positions.get(symbol)

    def get_all_positions(self) -> dict[str, Position]:
        """Return a snapshot of all tracked positions."""
        return dict(self._positions)

    def get_unrealized_pnl(self, symbol: str) -> float:
        """Return unrealized PnL for a symbol, or 0.0 if no position."""
        pos = self._positions.get(symbol)
        return pos.unrealized_pnl if pos else 0.0

    def get_total_unrealized_pnl(self) -> float:
        """Sum of unrealized PnL across all open positions."""
        return sum(p.unrealized_pnl for p in self._positions.values())

    # ------------------------------------------------------------------
    # Disconnect recovery
    # ------------------------------------------------------------------

    async def recovery_resync(self) -> None:
        """Full resync triggered after a detected disconnect event."""
        log.warning(SERVICE_RECONNECTED, message="position recovery resync triggered", exchange=self._exchange_id)
        if self._connected:
            try:
                await self._connector.disconnect()
            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="disconnect during recovery")
            self._connected = False

        await self._connector.connect()
        self._connected = True
        await self.sync_from_exchange()
        log.info(SERVICE_RECONNECTED, message="position recovery complete", open_positions=len(self._positions))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _apply_position_update(self, symbol: str, position: Position) -> None:
        """Store position in memory, Redis cache, and database."""
        self._positions[symbol] = position
        await self._cache_position(position)
        await self._persist_position(position)

    async def _remove_position(self, symbol: str) -> None:
        """Remove a closed position from all stores."""
        self._positions.pop(symbol, None)
        await self._clear_position_cache(symbol)
        await self._clear_position_db(symbol)
        log.info(SERVICE_HEALTH_OK, message="position removed", symbol=symbol, exchange=self._exchange_id)

    # -- Redis cache --

    def _cache_key(self, symbol: str) -> str:
        return f"cache:position:{self._exchange_id}:{symbol}"

    async def _cache_position(self, position: Position) -> None:
        """Write position to Redis with TTL."""
        key = self._cache_key(position.symbol)
        payload = position.model_dump_json()
        try:
            await self._redis.setex(key, POSITION_CACHE_TTL, payload)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="cache position error", symbol=position.symbol)

    async def _clear_position_cache(self, symbol: str) -> None:
        key = self._cache_key(symbol)
        try:
            await self._redis.delete(key)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="clear position cache error", symbol=symbol)

    async def get_cached_position(self, symbol: str) -> Position | None:
        """Read a position from Redis cache (fallback when memory is stale)."""
        key = self._cache_key(symbol)
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            return Position.model_validate_json(raw)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="read cached position error", symbol=symbol)
            return None

    # -- Database persistence --

    async def _persist_position(self, position: Position) -> None:
        """Upsert a position row."""
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO positions
                        (exchange, symbol, side, size, entry_price,
                         unrealized_pnl, leverage, liquidation_price,
                         margin_used, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                    ON CONFLICT (exchange, symbol, side)
                    DO UPDATE SET
                        size = EXCLUDED.size,
                        entry_price = EXCLUDED.entry_price,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        leverage = EXCLUDED.leverage,
                        liquidation_price = EXCLUDED.liquidation_price,
                        margin_used = EXCLUDED.margin_used,
                        updated_at = NOW()
                    """,
                    position.exchange,
                    position.symbol,
                    position.side,
                    position.size,
                    position.entry_price,
                    position.unrealized_pnl,
                    position.leverage,
                    position.liquidation_price,
                    position.margin_used,
                )
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="persist position error", symbol=position.symbol)

    async def _clear_position_db(self, symbol: str) -> None:
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM positions WHERE exchange = $1 AND symbol = $2",
                    self._exchange_id,
                    symbol,
                )
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="clear position db error", symbol=symbol)

    async def _get_watched_symbols(self) -> list[str]:
        """Return the list of symbols this tracker should monitor.

        Reads from Redis config key; falls back to a sensible default.
        """
        key = WATCHED_SYMBOLS_KEY.format(exchange=self._exchange_id)
        try:
            raw = await self._redis.get(key)
            if raw:
                symbols = json.loads(raw)
                if isinstance(symbols, list) and symbols:
                    return symbols
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="get watched symbols error")

        # Fallback: load positions from DB to discover symbols
        try:
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT DISTINCT symbol FROM positions WHERE exchange = $1",
                    self._exchange_id,
                )
                if rows:
                    return [row["symbol"] for row in rows]
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="get watched symbols db fallback error")

        return []
