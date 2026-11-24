"""BybitConnector error classification and cancel_open_orders (ccxt mocked, no network)."""

from __future__ import annotations

import ccxt.async_support as ccxt_async
import pytest

from bot.exchange import BybitConnector, PostOnlyRejected, is_ambiguous_error
from shared.models.order import OrderRequest

pytestmark = pytest.mark.asyncio


def order():
    return OrderRequest(strategy_id="s", exchange="bybit", symbol="BTC/USDT:USDT", side="buy",
                        order_type="limit", quantity=0.01, price=100_000.0, post_only=True)


class Stub:
    def __init__(self, exc=None, cancel_result=None):
        self.exc, self.cancel_result, self.cancel_args = exc, cancel_result, None

    async def create_order(self, **kw):
        if self.exc:
            raise self.exc
        return {"id": "1", "status": "closed", "filled": 0.01, "average": 100_000.0}

    async def cancel_all_orders(self, symbol, params=None):
        self.cancel_args = (symbol, params)
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result


def connector(stub):
    c = BybitConnector()
    c._exchange = stub
    c._connected = True
    return c


@pytest.mark.parametrize("exc,ambiguous", [
    (ccxt_async.RequestTimeout("t"), True),
    (ccxt_async.NetworkError("n"), True),
    (ccxt_async.ExchangeNotAvailable("5xx"), True),
    (ccxt_async.OnMaintenance("m"), True),
    (ccxt_async.DDoSProtection("d"), True),
    (RuntimeError("unknown"), True),
    (ValueError("parse"), True),
    (ccxt_async.InvalidOrder("x"), False),
    (ccxt_async.InsufficientFunds("x"), False),
    (ccxt_async.BadRequest("x"), False),
    (ccxt_async.AuthenticationError("x"), False),
    (ccxt_async.BadSymbol("x"), False),
    (ccxt_async.ExchangeError("x"), False),
])
async def test_classification_and_flag(exc, ambiguous):
    assert is_ambiguous_error(exc) is ambiguous
    c = connector(Stub(exc))
    res = await c.place_order(order())
    assert res.status == "rejected" and res.order_id == ""
    assert c.last_error_ambiguous is ambiguous and type(exc).__name__ in c.last_error


async def test_flag_cleared_on_success():
    c = connector(Stub(ccxt_async.RequestTimeout("t")))
    await c.place_order(order())
    assert c.last_error_ambiguous is True
    c._exchange = Stub()
    res = await c.place_order(order())
    assert res.status == "filled" and c.last_error_ambiguous is False and c.last_error == ""


async def test_post_only_rejection_still_raises():
    c = connector(Stub(ccxt_async.InvalidOrder("Order would immediately match (110030)")))
    with pytest.raises(PostOnlyRejected):
        await c.place_order(order())


async def test_cancel_open_orders_counts_and_does_not_target_conditional():
    stub = Stub(cancel_result=[{"id": "a"}, {"id": "b"}])
    assert await connector(stub).cancel_open_orders("BTC/USDT:USDT") == 2
    assert stub.cancel_args == ("BTC/USDT:USDT", None)       # no trigger/stop params: regular orders only
    assert await connector(Stub(cancel_result=None)).cancel_open_orders("X") == 0


async def test_cancel_open_orders_raises_on_api_failure():
    with pytest.raises(ccxt_async.NetworkError):
        await connector(Stub(cancel_result=ccxt_async.NetworkError("n"))).cancel_open_orders("X")
