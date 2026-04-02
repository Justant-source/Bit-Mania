"""Async repositories for core entities (asyncpg)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import asyncpg

from shared.db.connection import get_pool

logger = logging.getLogger(__name__)


class _BaseRepo:
    """Thin convenience wrapper around the shared pool."""

    @staticmethod
    def _pool() -> asyncpg.Pool:
        return get_pool()

    @classmethod
    async def _fetchrow(cls, query: str, *args: Any) -> asyncpg.Record | None:
        async with cls._pool().acquire() as conn:
            return await conn.fetchrow(query, *args)

    @classmethod
    async def _fetch(cls, query: str, *args: Any) -> list[asyncpg.Record]:
        async with cls._pool().acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def _execute(cls, query: str, *args: Any) -> str:
        async with cls._pool().acquire() as conn:
            return await conn.execute(query, *args)


# ── Trades ───────────────────────────────────────────────────────────────


class TradeRepo(_BaseRepo):
    """CRUD for the ``trades`` table."""

    @classmethod
    async def insert(
        cls,
        *,
        strategy_id: str,
        exchange: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
        fee: float = 0.0,
        fee_currency: str = "USDT",
        order_id: str = "",
        request_id: str | None = None,
    ) -> str:
        trade_id = uuid4().hex
        await cls._execute(
            """
            INSERT INTO trades
                (id, strategy_id, exchange, symbol, side, order_type,
                 quantity, price, fee, fee_currency, order_id, request_id, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            """,
            trade_id,
            strategy_id,
            exchange,
            symbol,
            side,
            order_type,
            quantity,
            price,
            fee,
            fee_currency,
            order_id,
            request_id or "",
            datetime.now(tz=timezone.utc),
        )
        return trade_id

    @classmethod
    async def get_by_id(cls, trade_id: str) -> asyncpg.Record | None:
        return await cls._fetchrow("SELECT * FROM trades WHERE id = $1", trade_id)

    @classmethod
    async def list_by_strategy(
        cls,
        strategy_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[asyncpg.Record]:
        return await cls._fetch(
            "SELECT * FROM trades WHERE strategy_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            strategy_id,
            limit,
            offset,
        )

    @classmethod
    async def list_recent(cls, limit: int = 50) -> list[asyncpg.Record]:
        return await cls._fetch(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT $1", limit
        )

    @classmethod
    async def delete(cls, trade_id: str) -> None:
        await cls._execute("DELETE FROM trades WHERE id = $1", trade_id)


# ── Positions ────────────────────────────────────────────────────────────


class PositionRepo(_BaseRepo):
    """CRUD for the ``positions`` table."""

    @classmethod
    async def upsert(
        cls,
        *,
        exchange: str,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        unrealized_pnl: float = 0.0,
        leverage: float = 1.0,
        liquidation_price: float | None = None,
        margin_used: float = 0.0,
    ) -> None:
        await cls._execute(
            """
            INSERT INTO positions
                (exchange, symbol, side, size, entry_price, unrealized_pnl,
                 leverage, liquidation_price, margin_used, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (exchange, symbol) DO UPDATE SET
                side = EXCLUDED.side,
                size = EXCLUDED.size,
                entry_price = EXCLUDED.entry_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                leverage = EXCLUDED.leverage,
                liquidation_price = EXCLUDED.liquidation_price,
                margin_used = EXCLUDED.margin_used,
                updated_at = EXCLUDED.updated_at
            """,
            exchange,
            symbol,
            side,
            size,
            entry_price,
            unrealized_pnl,
            leverage,
            liquidation_price,
            margin_used,
            datetime.now(tz=timezone.utc),
        )

    @classmethod
    async def get(cls, exchange: str, symbol: str) -> asyncpg.Record | None:
        return await cls._fetchrow(
            "SELECT * FROM positions WHERE exchange = $1 AND symbol = $2",
            exchange,
            symbol,
        )

    @classmethod
    async def list_all(cls) -> list[asyncpg.Record]:
        return await cls._fetch("SELECT * FROM positions WHERE size > 0")

    @classmethod
    async def delete(cls, exchange: str, symbol: str) -> None:
        await cls._execute(
            "DELETE FROM positions WHERE exchange = $1 AND symbol = $2",
            exchange,
            symbol,
        )


# ── Funding rates ────────────────────────────────────────────────────────


class FundingRepo(_BaseRepo):
    """CRUD for the ``funding_rates`` table."""

    @classmethod
    async def insert(
        cls,
        *,
        exchange: str,
        symbol: str,
        rate: float,
        predicted_rate: float | None = None,
        next_funding_time: datetime | None = None,
        collected_at: datetime | None = None,
    ) -> None:
        await cls._execute(
            """
            INSERT INTO funding_rates
                (id, exchange, symbol, rate, predicted_rate, next_funding_time, collected_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
            uuid4().hex,
            exchange,
            symbol,
            rate,
            predicted_rate,
            next_funding_time,
            collected_at or datetime.now(tz=timezone.utc),
        )

    @classmethod
    async def list_recent(
        cls,
        exchange: str,
        symbol: str,
        limit: int = 50,
    ) -> list[asyncpg.Record]:
        return await cls._fetch(
            """
            SELECT * FROM funding_rates
            WHERE exchange = $1 AND symbol = $2
            ORDER BY collected_at DESC
            LIMIT $3
            """,
            exchange,
            symbol,
            limit,
        )


# ── Portfolio snapshots ──────────────────────────────────────────────────


class SnapshotRepo(_BaseRepo):
    """CRUD for the ``portfolio_snapshots`` table."""

    @classmethod
    async def insert(
        cls,
        *,
        total_equity: float,
        unrealized_pnl: float = 0.0,
        realized_pnl_today: float = 0.0,
        daily_drawdown: float = 0.0,
        weekly_drawdown: float = 0.0,
        strategies: list[dict[str, Any]] | None = None,
        kill_switch_triggered: bool = False,
    ) -> str:
        snap_id = uuid4().hex
        await cls._execute(
            """
            INSERT INTO portfolio_snapshots
                (id, total_equity, unrealized_pnl, realized_pnl_today,
                 daily_drawdown, weekly_drawdown, strategies,
                 kill_switch_triggered, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            snap_id,
            total_equity,
            unrealized_pnl,
            realized_pnl_today,
            daily_drawdown,
            weekly_drawdown,
            json.dumps(strategies or []),
            kill_switch_triggered,
            datetime.now(tz=timezone.utc),
        )
        return snap_id

    @classmethod
    async def get_latest(cls) -> asyncpg.Record | None:
        return await cls._fetchrow(
            "SELECT * FROM portfolio_snapshots ORDER BY created_at DESC LIMIT 1"
        )

    @classmethod
    async def list_range(
        cls,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[asyncpg.Record]:
        return await cls._fetch(
            """
            SELECT * FROM portfolio_snapshots
            WHERE created_at BETWEEN $1 AND $2
            ORDER BY created_at
            LIMIT $3
            """,
            start,
            end,
            limit,
        )
