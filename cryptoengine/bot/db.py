"""Postgres access: one asyncpg pool and the single source of all DDL.

``ensure_schema`` is a no-op against the live database: every statement is
``CREATE ... IF NOT EXISTS`` and nothing ever ALTERs or DROPs. The five legacy
tables mirror the live schema exactly (shared/db/init_schema.sql and the DDL
that used to be embedded in the old services); three tables are new.
"""

from __future__ import annotations

import os

import asyncpg
import structlog

log = structlog.get_logger("bot.db")

SCHEMA_SQL = """
-- ── existing tables (live schema; keep history) ─────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id             BIGSERIAL PRIMARY KEY,
    request_id     TEXT UNIQUE NOT NULL,
    order_id       TEXT,
    exchange       TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    side           TEXT        NOT NULL,
    order_type     TEXT        NOT NULL,
    quantity       DOUBLE PRECISION NOT NULL,
    price          DOUBLE PRECISION,
    status         TEXT        NOT NULL DEFAULT 'pending',
    filled_qty     DOUBLE PRECISION DEFAULT 0,
    filled_price   DOUBLE PRECISION,
    fee            DOUBLE PRECISION DEFAULT 0,
    fee_currency   TEXT DEFAULT 'USDT',
    strategy_id    TEXT,
    post_only      BOOLEAN DEFAULT TRUE,
    reduce_only    BOOLEAN DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delay_reason        TEXT,
    original_signal_ts  TIMESTAMPTZ,
    original_request_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_orders_request_id ON orders (request_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders (strategy_id);

CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    strategy_id     VARCHAR(50) NOT NULL,
    exchange        VARCHAR(20) NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    side            VARCHAR(10) NOT NULL,
    size            DECIMAL(20, 8) NOT NULL,
    entry_price     DECIMAL(20, 2) NOT NULL,
    current_price   DECIMAL(20, 2),
    unrealized_pnl  DECIMAL(20, 8),
    leverage        DECIMAL(5, 2) DEFAULT 1.0,
    opened_at       TIMESTAMPTZ DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    close_reason    VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy_id, opened_at);
CREATE INDEX IF NOT EXISTS idx_positions_open ON positions(strategy_id) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_positions_closed_at_filter ON positions (closed_at) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_positions_symbol_open ON positions (symbol, closed_at) WHERE closed_at IS NULL;

CREATE TABLE IF NOT EXISTS supertrend_signals (
    id                 BIGSERIAL PRIMARY KEY,
    bar_ts             TIMESTAMPTZ NOT NULL,
    computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    st_dir             SMALLINT    NOT NULL,
    st_line            DOUBLE PRECISION,
    fast_ema           DOUBLE PRECISION NOT NULL,
    slow_ema           DOUBLE PRECISION NOT NULL,
    dir_ema            DOUBLE PRECISION NOT NULL,
    price              DOUBLE PRECISION NOT NULL,
    atr_14             DOUBLE PRECISION NOT NULL,
    allocated_capital  DOUBLE PRECISION NOT NULL,
    had_position       BOOLEAN NOT NULL,
    entry_ok           BOOLEAN NOT NULL,
    exit_signal        BOOLEAN NOT NULL,
    exit_reason        VARCHAR(20),
    expected_action    VARCHAR(10) NOT NULL,
    expected_qty       DOUBLE PRECISION,
    expected_stop_loss DOUBLE PRECISION
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_supertrend_signals_bar_ts ON supertrend_signals (bar_ts);
CREATE INDEX IF NOT EXISTS idx_supertrend_signals_computed_at ON supertrend_signals (computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_supertrend_signals_action ON supertrend_signals (expected_action, bar_ts DESC);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    total_equity        DOUBLE PRECISION NOT NULL,
    unrealized_pnl      DOUBLE PRECISION DEFAULT 0,
    realized_pnl_today  DOUBLE PRECISION DEFAULT 0,
    daily_drawdown      DOUBLE PRECISION DEFAULT 0,
    weekly_drawdown     DOUBLE PRECISION DEFAULT 0,
    monthly_drawdown    DOUBLE PRECISION DEFAULT 0,
    sharpe_ratio_30d    DOUBLE PRECISION,
    strategies          JSONB DEFAULT '[]',
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_time ON portfolio_snapshots(snapshot_at);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_at ON portfolio_snapshots (snapshot_at DESC);

CREATE TABLE IF NOT EXISTS ohlcv_history (
    id          BIGSERIAL PRIMARY KEY,
    exchange    VARCHAR(20) NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    timeframe   VARCHAR(5) NOT NULL,
    open        DECIMAL(20, 2) NOT NULL,
    high        DECIMAL(20, 2) NOT NULL,
    low         DECIMAL(20, 2) NOT NULL,
    close       DECIMAL(20, 2) NOT NULL,
    volume      DECIMAL(20, 8) NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_lookup
    ON ohlcv_history(exchange, symbol, timeframe, timestamp);

-- ── new tables ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS replay_ohlcv_4h (
    ts BIGINT PRIMARY KEY,
    open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_reference_trades (
    id SERIAL PRIMARY KEY, opened_at BIGINT NOT NULL, closed_at BIGINT NOT NULL,
    side TEXT NOT NULL,
    entry_price DOUBLE PRECISION, exit_price DOUBLE PRECISION, qty DOUBLE PRECISION,
    pnl DOUBLE PRECISION, fee DOUBLE PRECISION
);
"""


class Database:
    """asyncpg pool wrapper. Settings come from ``DB_*`` env unless overridden."""

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        name: str | None = None,
        user: str | None = None,
        password: str | None = None,
        min_size: int = 1,
        max_size: int = 5,
    ) -> None:
        self.host = host or os.environ.get("DB_HOST", "postgres")
        self.port = int(port or os.environ.get("DB_PORT", "5432"))
        self.name = name or os.environ.get("DB_NAME", "cryptoengine")
        self.user = user or os.environ.get("DB_USER", "cryptoengine")
        self.password = password if password is not None else os.environ.get("DB_PASSWORD", "")
        self._min_size = min_size
        self._max_size = max_size
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database is not connected")
        return self._pool

    async def connect(self) -> None:
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.name,
            user=self.user,
            password=self.password,
            min_size=self._min_size,
            max_size=self._max_size,
        )
        log.info("db_connected", host=self.host, port=self.port, database=self.name)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)
        log.info("db_schema_ensured")
