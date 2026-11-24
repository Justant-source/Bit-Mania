"""SafetyGuard tests (kill gate, price/balance from exchange, fail-closed rules)."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for p in (str(_ROOT / "tests"), str(_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pytest  # noqa: E402

from bot.safety import SafetyGuard  # noqa: E402
from fakes.exchange import FakeExchange  # noqa: E402
from shared.models.order import OrderRequest  # noqa: E402

pytestmark = pytest.mark.asyncio
SYM = "BTC/USDT:USDT"


class Gate:
    def __init__(self, blocked=False, reason="manual_halt"):
        self.blocked, self.reason = blocked, reason

    def entries_blocked(self):
        return self.blocked, self.reason if self.blocked else ""


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def order(side="buy", qty=0.01, price=None, order_type="market", post_only=False, reduce_only=False):
    return OrderRequest(strategy_id="s", exchange="bybit", symbol=SYM, side=side, order_type=order_type,
                        quantity=qty, price=price, post_only=post_only, reduce_only=reduce_only)


def make(**kw):
    fx = FakeExchange(wallet=kw.pop("wallet", 1000.0))
    fx.set_book(99_995, 100_005)
    gate = kw.pop("gate", Gate())
    clock = kw.pop("clock", None) or Clock()
    return fx, gate, SafetyGuard(fx, gate, clock=clock, **kw), clock


async def test_normal_entry_passes():
    fx, _, g, _ = make()
    assert await g.check_order(order(qty=0.02, price=100_000, order_type="limit", post_only=True)) == (True, "")


async def test_blocked_gate_rejects_entry_but_not_reduce_only():
    fx, gate, g, _ = make(gate=Gate(True, "manual_halt"))
    ok, reason = await g.check_order(order())
    assert not ok and reason.startswith("entries_blocked") and "manual_halt" in reason
    fx.set_position("long", 0.01, 100_000)
    assert await g.check_order(order(side="sell", reduce_only=True)) == (True, "")


async def test_reduce_only_passes_even_if_gate_raises_and_price_down():
    fx, _, g, _ = make()

    class Boom:
        def entries_blocked(self):
            raise RuntimeError("x")

    g2 = SafetyGuard(fx, Boom())
    ok, reason = await g2.check_order(order())
    assert not ok and "entry_gate_error" in reason
    fx.fail_next("get_ticker", 5)
    fx.fail_next("get_balance", 5)
    assert (await g2.check_order(order(side="sell", reduce_only=True)))[0] is True


async def test_entry_rejected_when_price_unavailable():
    fx, _, g, _ = make()
    fx.fail_next("get_ticker")
    ok, reason = await g.check_order(order())
    assert not ok and "market_price_unavailable" in reason


async def test_entry_rejected_when_balance_or_position_unavailable():
    fx, _, g, _ = make()
    fx.fail_next("get_balance")
    ok, reason = await g.check_order(order())
    assert not ok and "balance_unavailable" in reason
    fx2, _, g2, _ = make()
    fx2.fail_next("get_position")
    ok, reason = await g2.check_order(order())
    assert not ok and "position_unavailable" in reason


async def test_max_order_size():
    fx, _, g, _ = make(max_order_size=1000.0, wallet=100_000)
    ok, reason = await g.check_order(order(qty=0.02))  # 2000 notional at market
    assert not ok and reason.startswith("order_size_exceeded")
    assert (await g.check_order(order(qty=0.009)))[0] is True


async def test_implied_leverage_exceeded_and_at_limit():
    # equity 1000: 0.03 BTC @100k = 3000 = 3.0x exactly -> pass; 0.031 -> fail
    fx, _, g, _ = make(wallet=1000)
    assert (await g.check_order(order(qty=0.03, price=100_000, order_type="limit", post_only=True)))[0] is True
    ok, reason = await g.check_order(order(qty=0.031, price=100_000, order_type="limit", post_only=True))
    assert not ok and reason.startswith("implied_leverage_exceeded")


async def test_implied_leverage_counts_existing_position():
    fx, _, g, _ = make(wallet=1000)
    fx.set_position("long", 0.02, 100_000)
    ok, reason = await g.check_order(order(qty=0.015, price=100_000, order_type="limit", post_only=True))
    assert not ok and "implied_leverage" in reason


async def test_reduce_only_skips_leverage_and_margin():
    fx, _, g, _ = make(wallet=10)
    fx.set_position("long", 0.5, 100_000)
    assert (await g.check_order(order(side="sell", qty=0.5, reduce_only=True)))[0] is True


async def test_leverage_limit_clamped_to_3_and_env(monkeypatch):
    fx = FakeExchange()
    assert SafetyGuard(fx, Gate(), leverage_limit=5.0).leverage_limit == 3.0
    assert SafetyGuard(fx, Gate(), leverage_limit=2.0).leverage_limit == 2.0
    monkeypatch.setenv("SAFETY_LEVERAGE_LIMIT", "2.5")
    assert SafetyGuard(fx, Gate()).leverage_limit == 2.5


async def test_insufficient_free_margin():
    fx, _, g, _ = make(wallet=1000)
    fx.set_balance(free=10.0)
    ok, reason = await g.check_order(order(qty=0.01))
    assert not ok and reason.startswith("insufficient_margin")


async def test_slippage_limit_non_post_only():
    fx, _, g, _ = make(wallet=10_000)
    ok, reason = await g.check_order(order(price=101_000, order_type="limit", post_only=False))  # +1%
    assert not ok and reason.startswith("slippage_exceeded")
    ok, reason = await g.check_order(order(side="sell", price=99_000, order_type="limit", post_only=False))
    assert not ok and reason.startswith("slippage_exceeded")
    assert (await g.check_order(order(price=100_200, order_type="limit", post_only=False)))[0] is True
    # post-only skips the slippage check
    assert (await g.check_order(order(price=101_000, order_type="limit", post_only=True)))[0] is True


async def test_rate_limit_blocks_entries_only():
    fx, _, g, clock = make(rate_limit_per_minute=10)
    for _ in range(9):
        g.record_api_call()
    ok, reason = await g.check_order(order(qty=0.005))
    assert not ok and reason.startswith("rate_limit_near")
    fx.set_position("long", 0.01, 100_000)
    assert (await g.check_order(order(side="sell", reduce_only=True)))[0] is True
    clock.t += 61
    assert g.api_calls_last_minute == 0
    assert (await g.check_order(order(qty=0.005)))[0] is True


async def test_ticker_and_balance_cached_5s():
    fx, _, g, clock = make()
    await g.check_order(order(qty=0.005))
    await g.check_order(order(qty=0.005))
    names = [c[0] for c in fx.calls]
    assert names.count("get_ticker") == 1 and names.count("get_balance") == 1
    clock.t += 6
    await g.check_order(order(qty=0.005))
    names = [c[0] for c in fx.calls]
    assert names.count("get_ticker") == 2 and names.count("get_balance") == 2
