"""Integration tests for BybitConnector against the Bybit testnet.

These tests require:
  - BYBIT_TESTNET_API_KEY and BYBIT_TESTNET_API_SECRET env vars
  - Network connectivity to Bybit testnet

Skip automatically if credentials are not set.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.exchange.bybit import BybitConnector
from shared.models.order import OrderRequest

# Skip all tests if no testnet credentials
TESTNET_KEY = os.getenv("BYBIT_TESTNET_API_KEY", "")
TESTNET_SECRET = os.getenv("BYBIT_TESTNET_API_SECRET", "")
SKIP_REASON = "Bybit testnet credentials not set (BYBIT_TESTNET_API_KEY / BYBIT_TESTNET_API_SECRET)"

pytestmark = pytest.mark.skipif(
    not TESTNET_KEY or not TESTNET_SECRET,
    reason=SKIP_REASON,
)

SYMBOL = "BTC/USDT:USDT"


@pytest.fixture
async def connector():
    """Create and connect a testnet BybitConnector."""
    conn = BybitConnector(
        api_key=TESTNET_KEY,
        api_secret=TESTNET_SECRET,
        testnet=True,
    )
    await conn.connect()
    yield conn
    await conn.disconnect()


class TestBybitTestnetConnect:
    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self):
        conn = BybitConnector(
            api_key=TESTNET_KEY,
            api_secret=TESTNET_SECRET,
            testnet=True,
        )
        await conn.connect()
        assert conn._connected is True
        await conn.disconnect()
        assert conn._connected is False

    @pytest.mark.asyncio
    async def test_connect_loads_markets(self, connector):
        # After connect, markets should be loaded
        assert connector._exchange.markets is not None
        assert len(connector._exchange.markets) > 0


class TestBybitTestnetTicker:
    @pytest.mark.asyncio
    async def test_get_ticker(self, connector):
        ticker = await connector.get_ticker(SYMBOL)
        assert "last" in ticker
        assert ticker["last"] is not None
        assert float(ticker["last"]) > 0

    @pytest.mark.asyncio
    async def test_get_ticker_has_bid_ask(self, connector):
        ticker = await connector.get_ticker(SYMBOL)
        assert ticker.get("bid") is not None or ticker.get("ask") is not None


class TestBybitTestnetOrderbook:
    @pytest.mark.asyncio
    async def test_get_orderbook(self, connector):
        ob = await connector.get_orderbook(SYMBOL, limit=5)
        assert ob.exchange == "bybit"
        assert ob.symbol == SYMBOL
        assert len(ob.bids) > 0
        assert len(ob.asks) > 0
        assert ob.best_bid is not None
        assert ob.best_ask is not None
        assert ob.best_bid < ob.best_ask


class TestBybitTestnetFunding:
    @pytest.mark.asyncio
    async def test_get_funding_rate(self, connector):
        funding = await connector.get_funding_rate(SYMBOL)
        assert funding.exchange == "bybit"
        assert funding.symbol == SYMBOL
        assert isinstance(funding.rate, float)


class TestBybitTestnetBalance:
    @pytest.mark.asyncio
    async def test_get_balance(self, connector):
        balance = await connector.get_balance()
        assert "total" in balance
        assert "free" in balance
        assert "used" in balance


class TestBybitTestnetOrderFlow:
    @pytest.mark.asyncio
    async def test_place_and_cancel_limit_order(self, connector):
        """Place a limit order far from market, then cancel it."""
        ticker = await connector.get_ticker(SYMBOL)
        price = float(ticker["last"])
        # Place buy order 10% below market — unlikely to fill
        order_price = round(price * 0.90, 1)

        order = OrderRequest(
            strategy_id="test_integration",
            exchange="bybit",
            symbol=SYMBOL,
            side="buy",
            order_type="limit",
            quantity=0.001,
            price=order_price,
            post_only=True,
        )

        result = await connector.place_order(order)
        assert result.request_id == order.request_id
        assert result.order_id != ""
        assert result.status in ("new", "filled", "rejected")

        # Cancel if it was accepted
        if result.status == "new":
            cancelled = await connector.cancel_order(result.order_id, SYMBOL)
            assert cancelled is True

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, connector):
        """Cancelling a non-existent order should return False."""
        cancelled = await connector.cancel_order("nonexistent-order-id", SYMBOL)
        assert cancelled is False


class TestBybitTestnetPosition:
    @pytest.mark.asyncio
    async def test_get_position(self, connector):
        pos = await connector.get_position(SYMBOL)
        # May be None if no position open
        if pos is not None:
            assert pos.exchange == "bybit"
            assert pos.symbol == SYMBOL
            assert pos.size >= 0
