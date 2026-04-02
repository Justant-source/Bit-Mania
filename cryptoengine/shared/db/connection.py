"""AsyncPG connection-pool management."""

from __future__ import annotations

import logging
import os

import asyncpg

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def create_pool(
    dsn: str | None = None,
    min_size: int = 2,
    max_size: int = 10,
) -> asyncpg.Pool:
    """Create (or return existing) connection pool.

    If *dsn* is ``None`` the ``DATABASE_URL`` env-var is used.
    """
    global _pool
    if _pool is not None:
        return _pool

    dsn = dsn or os.environ.get(
        "DATABASE_URL",
        "postgresql://cryptoengine:cryptoengine@localhost:5432/cryptoengine",
    )

    _pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
    )
    logger.info("asyncpg pool created (min=%d, max=%d)", min_size, max_size)
    return _pool


def get_pool() -> asyncpg.Pool:
    """Return the current pool — raises if ``create_pool`` was never awaited."""
    if _pool is None:
        raise RuntimeError(
            "Database pool not initialised. Call `await create_pool()` first."
        )
    return _pool


async def close_pool() -> None:
    """Gracefully close the pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("asyncpg pool closed")
