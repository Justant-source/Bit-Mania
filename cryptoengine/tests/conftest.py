"""Shared pytest fixtures for the CryptoEngine test suite.

Provides:
  * mock_redis         — fakeredis async instance
  * mock_db            — asyncpg pool mock
  * mock_exchange      — ccxt sandbox connector mock
  * sample_ohlcv_data  — 200-bar OHLCV DataFrame
  * sample_funding_data — funding rate DataFrame
  * sample_order_request — OrderRequest fixture
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pandas as pd
import numpy as np

# Ensure project root is on sys.path for shared imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.models.market import FundingRate, MarketRegime, OHLCV, OrderBook
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import PortfolioState, Position, StrategySnapshot
from shared.models.strategy import StrategyCommand, StrategyStatus


# ===================================================================
# Event loop
# ===================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Session-scoped event loop for all async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ===================================================================
# Redis mock (fakeredis)
# ===================================================================

@pytest.fixture
async def mock_redis():
    """Provide a fakeredis async client.

    Falls back to a MagicMock if fakeredis is not installed.
    """
    try:
        import fakeredis.aioredis as fakeredis_aio
        r = fakeredis_aio.FakeRedis(decode_responses=True)
        yield r
        await r.aclose()
    except ImportError:
        # Fallback: fully-mocked Redis
        redis_mock = AsyncMock()
        redis_mock.ping = AsyncMock(return_value=True)
        redis_mock.get = AsyncMock(return_value=None)
        redis_mock.set = AsyncMock(return_value=True)
        redis_mock.setex = AsyncMock(return_value=True)
        redis_mock.publish = AsyncMock(return_value=1)
        redis_mock.delete = AsyncMock(return_value=1)
        redis_mock.exists = AsyncMock(return_value=0)
        redis_mock.hset = AsyncMock(return_value=1)
        redis_mock.hget = AsyncMock(return_value=None)
        redis_mock.hgetall = AsyncMock(return_value={})
        redis_mock.aclose = AsyncMock()
        redis_mock.info = AsyncMock(return_value={"used_memory_human": "1M"})

        pubsub_mock = AsyncMock()
        pubsub_mock.subscribe = AsyncMock()
        pubsub_mock.unsubscribe = AsyncMock()
        pubsub_mock.aclose = AsyncMock()
        pubsub_mock.get_message = AsyncMock(return_value=None)
        redis_mock.pubsub = MagicMock(return_value=pubsub_mock)

        yield redis_mock


# ===================================================================
# Database mock (asyncpg)
# ===================================================================

@pytest.fixture
def mock_db():
    """Provide a mock asyncpg connection pool."""
    pool = AsyncMock()

    # Connection mock
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value="INSERT 0 1")
    conn.executemany = AsyncMock()

    # Context manager for pool.acquire()
    pool.acquire = MagicMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx

    pool.close = AsyncMock()
    pool._conn = conn  # expose for direct assertions

    return pool


# ===================================================================
# Exchange mock
# ===================================================================

@pytest.fixture
def mock_exchange():
    """Provide a mock ExchangeConnector for testing without network."""
    exchange = AsyncMock()
    exchange.exchange_id = "bybit"
    exchange.connect = AsyncMock()
    exchange.disconnect = AsyncMock()

    # Market data
    exchange.get_ticker = AsyncMock(return_value={
        "symbol": "BTC/USDT:USDT",
        "last": 65000.0,
        "bid": 64990.0,
        "ask": 65010.0,
        "high": 66000.0,
        "low": 64000.0,
        "volume": 12345.67,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    })

    exchange.get_orderbook = AsyncMock(return_value=OrderBook(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        bids=[{"price": 64990, "quantity": 1.5}, {"price": 64980, "quantity": 2.0}],
        asks=[{"price": 65010, "quantity": 1.0}, {"price": 65020, "quantity": 3.0}],
    ))

    exchange.get_funding_rate = AsyncMock(return_value=FundingRate(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        rate=0.0001,
        predicted_rate=0.00008,
        next_funding_time=datetime.now(timezone.utc) + timedelta(hours=4),
    ))

    exchange.get_ohlcv = AsyncMock(return_value=[])

    exchange.place_order = AsyncMock(return_value=OrderResult(
        request_id="test-req-001",
        order_id="ord-001",
        status="filled",
        filled_qty=0.1,
        filled_price=65000.0,
        fee=0.039,
        fee_currency="USDT",
    ))

    exchange.cancel_order = AsyncMock(return_value=True)

    exchange.get_position = AsyncMock(return_value=Position(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="short",
        size=0.1,
        entry_price=65000.0,
        unrealized_pnl=-5.0,
        leverage=3.0,
        margin_used=2166.67,
    ))

    exchange.get_balance = AsyncMock(return_value={
        "total": 10000.0,
        "free": 7800.0,
        "used": 2200.0,
    })

    return exchange


# ===================================================================
# Sample data fixtures
# ===================================================================

@pytest.fixture
def sample_ohlcv_data() -> pd.DataFrame:
    """Generate 200 bars of synthetic OHLCV data."""
    np.random.seed(42)
    n = 200
    base_price = 65000.0
    dates = pd.date_range(
        start="2025-10-01", periods=n, freq="1h", tz=timezone.utc
    )

    # Random walk for close prices
    returns = np.random.normal(0, 0.002, n)
    close = base_price * np.cumprod(1 + returns)

    # OHLC derived from close
    high = close * (1 + np.abs(np.random.normal(0, 0.001, n)))
    low = close * (1 - np.abs(np.random.normal(0, 0.001, n)))
    open_price = np.roll(close, 1)
    open_price[0] = base_price
    volume = np.random.uniform(100, 5000, n)

    df = pd.DataFrame({
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=dates)
    df.index.name = "ts"
    return df


@pytest.fixture
def sample_funding_data() -> pd.DataFrame:
    """Generate synthetic funding rate data aligned with OHLCV."""
    np.random.seed(42)
    n = 25  # ~200h / 8h = 25 funding intervals
    dates = pd.date_range(
        start="2025-10-01", periods=n, freq="8h", tz=timezone.utc
    )

    rates = np.random.normal(0.0001, 0.00005, n)
    predicted = rates + np.random.normal(0, 0.00002, n)

    df = pd.DataFrame({
        "rate": rates,
        "predicted_rate": predicted,
    }, index=dates)
    df.index.name = "ts"
    return df


@pytest.fixture
def sample_order_request() -> OrderRequest:
    """Provide a standard OrderRequest for testing."""
    return OrderRequest(
        strategy_id="funding_arb_01",
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="buy",
        order_type="limit",
        quantity=0.1,
        price=65000.0,
        post_only=True,
        reduce_only=False,
    )


@pytest.fixture
def sample_order_result() -> OrderResult:
    """Provide a standard OrderResult for testing."""
    return OrderResult(
        request_id="test-req-001",
        order_id="ord-001",
        status="filled",
        filled_qty=0.1,
        filled_price=65000.0,
        fee=0.039,
        fee_currency="USDT",
    )


@pytest.fixture
def sample_portfolio_state() -> PortfolioState:
    """Provide a sample PortfolioState."""
    return PortfolioState(
        total_equity=10000.0,
        unrealized_pnl=-50.0,
        realized_pnl_today=120.0,
        daily_drawdown=-0.005,
        weekly_drawdown=-0.012,
        strategies=[
            StrategySnapshot(
                strategy_id="funding_arb_01",
                allocated_capital=2500.0,
                current_pnl=80.0,
                position_count=1,
            ),
            StrategySnapshot(
                strategy_id="adaptive_dca_01",
                allocated_capital=4000.0,
                current_pnl=40.0,
                position_count=5,
            ),
        ],
    )


@pytest.fixture
def sample_position() -> Position:
    """Provide a sample Position."""
    return Position(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="short",
        size=0.1,
        entry_price=65000.0,
        unrealized_pnl=-15.0,
        leverage=3.0,
        margin_used=2166.67,
    )
