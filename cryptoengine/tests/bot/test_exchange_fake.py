"""FakeExchange behaviour tests (the fake is the base for end-to-end tests)."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for p in (str(_ROOT / "tests"), str(_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest  # noqa: E402

from bot.exchange import PostOnlyRejected  # noqa: E402
from bot.interfaces import Exchange  # noqa: E402
from fakes.exchange import FakeExchange  # noqa: E402
from shared.models.order import OrderRequest  # noqa: E402

pytestmark = pytest.mark.asyncio
SYM = "BTC/USDT:USDT"


def req(side="buy", qty=0.01, price=None, order_type="market", post_only=False, reduce_only=False, sl=None):
    return OrderRequest(strategy_id="s", exchange="bybit", symbol=SYM, side=side, order_type=order_type,
                        quantity=qty, price=price, post_only=post_only, reduce_only=reduce_only, stop_loss=sl)


async def test_implements_protocol():
    assert isinstance(FakeExchange(), Exchange)


async def test_market_buy_fills_at_ask_with_taker_fee_and_updates_position():
    fx = FakeExchange()
    fx.set_book(100_000, 100_010)
    res = await fx.place_order(req(qty=0.01, sl=70_000))
    assert res.status == "filled" and res.filled_qty == pytest.approx(0.01)
    assert res.filled_price == pytest.approx(100_010)
    assert res.fee == pytest.approx(0.01 * 100_010 * 0.00055)
    pos = await fx.get_position(SYM)
    assert pos.side == "long" and pos.size == pytest.approx(0.01)
    assert fx.position_stop_loss == 70_000
    bal = await fx.get_balance()
    assert bal["wallet"] == bal["total"] == pytest.approx(1000 - res.fee)
    assert bal["free"] == pytest.approx(bal["total"] - 0.01 * 100_010 / 3)


async def test_post_only_crossing_raises():
    fx = FakeExchange()
    fx.set_book(100_000, 100_010)
    with pytest.raises(PostOnlyRejected):
        await fx.place_order(req(price=100_010, order_type="limit", post_only=True))
    with pytest.raises(PostOnlyRejected):
        await fx.place_order(req(side="sell", price=100_000, order_type="limit", post_only=True))
    fx.post_only_reject_next()
    with pytest.raises(PostOnlyRejected):
        await fx.place_order(req(price=99_000, order_type="limit", post_only=True))


async def test_limit_full_after_n_polls_maker_fee():
    fx = FakeExchange()
    fx.fill_limits(("after", 3))
    r = await fx.place_order(req(price=100_000, order_type="limit", post_only=True))
    assert r.status == "new"
    for _ in range(2):
        assert (await fx.fetch_order_result(r.order_id, SYM, "x")).status == "new"
    done = await fx.fetch_order_result(r.order_id, SYM, "x")
    assert done.status == "filled" and done.fee == pytest.approx(0.01 * 100_000 * 0.0002)
    assert await fx.fetch_order_status(r.order_id, SYM) == "closed"


async def test_limit_never_and_partial_then_cancel_keeps_fill():
    fx = FakeExchange()
    fx.plan_fills("never", ("partial", 0.4))
    a = await fx.place_order(req(price=100_000, order_type="limit", post_only=True))
    assert (await fx.fetch_order_result(a.order_id, SYM, "x")).filled_qty == 0
    b = await fx.place_order(req(price=100_000, order_type="limit", post_only=True))
    assert (await fx.fetch_order_result(b.order_id, SYM, "x")).filled_qty == pytest.approx(0.004)
    assert await fx.cancel_order(b.order_id, SYM)
    after = await fx.fetch_order_result(b.order_id, SYM, "x")
    assert after.status == "cancelled" and after.filled_qty == pytest.approx(0.004)
    assert not await fx.cancel_order(b.order_id, SYM)
    assert fx.cancelled == [b.order_id]


async def test_instant_plan_and_manual_fill():
    fx = FakeExchange()
    fx.plan_fills("instant", "never")
    r = await fx.place_order(req(price=100_000, order_type="limit", post_only=True))
    assert r.status == "filled"
    r2 = await fx.place_order(req(price=100_000, order_type="limit", post_only=True))
    fx.fill_order(r2.order_id, 0.003)
    assert fx.orders[r2.order_id].filled == pytest.approx(0.003)
    fx.fill_order(r2.order_id)
    assert fx.orders[r2.order_id].status == "closed"
    assert fx.position.size == pytest.approx(0.02)


async def test_reduce_only_semantics_and_pnl():
    fx = FakeExchange()
    rej = await fx.place_order(req(side="sell", reduce_only=True))
    assert rej.status == "rejected" and "110017" in fx.last_error
    fx.set_position("long", 0.01, 100_000)
    fx.set_book(101_000, 101_010)
    wrong = await fx.place_order(req(side="buy", reduce_only=True))
    assert wrong.status == "rejected"
    ok = await fx.place_order(req(side="sell", qty=0.5, reduce_only=True))  # clipped to position
    assert ok.status == "filled" and ok.filled_qty == pytest.approx(0.01)
    assert await fx.get_position(SYM) is None
    assert fx.wallet == pytest.approx(1000 + 0.01 * 1000 - ok.fee)


async def test_min_size_and_step():
    fx = FakeExchange()
    assert (await fx.place_order(req(qty=0.0004))).status == "rejected"
    r = await fx.place_order(req(qty=0.0057))
    assert r.filled_qty == pytest.approx(0.005)


async def test_stop_loss_cleared_on_flat_and_trigger():
    fx = FakeExchange()
    await fx.place_order(req(qty=0.01, sl=70_000))
    fx.trigger_stop_loss()
    assert fx.position is None and fx.position_stop_loss is None
    assert fx.wallet < 1000  # loss realised


async def test_failure_injection_and_reject_next():
    fx = FakeExchange()
    fx.fail_next("get_balance", 2)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await fx.get_balance()
    assert (await fx.get_balance())["total"] == 1000
    fx.reject_next(1, "boom")
    r = await fx.place_order(req())
    assert r.status == "rejected" and fx.last_error == "boom"
    assert (await fx.place_order(req())).status == "filled"


async def test_leverage_capped_margin_recorded_candles():
    fx = FakeExchange()
    await fx.set_leverage(SYM, 10)
    await fx.set_margin_mode(SYM, "isolated")
    assert fx.leverage_calls == [3] and fx.margin_calls == ["isolated"]
    fx.set_candles([[i * 14_400_000, 1, 2, 0.5, 1.5, 10] for i in range(5)])
    assert len(await fx.get_ohlcv(SYM, limit=3)) == 3
    rows = await fx.get_ohlcv(SYM, since=2 * 14_400_000, limit=2)
    assert [r[0] for r in rows] == [2 * 14_400_000, 3 * 14_400_000]
    meta = await fx.get_min_order_sizes([SYM])
    assert meta[SYM]["min_qty"] == 0.001
