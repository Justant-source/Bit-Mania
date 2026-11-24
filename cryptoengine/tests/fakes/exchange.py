"""In-memory exchange for tests: implements ``bot.interfaces.Exchange``.

Quick start::

    fx = FakeExchange()                       # BTC/USDT:USDT, bid 99_990 / ask 100_010, 1000 USDT
    fx.set_book(bid=100_000.0, ask=100_010.0)
    fx.fill_limits("full")                    # how resting limit orders behave (below)
    res = await fx.place_order(order)         # same contract as BybitConnector.place_order

State you can read
------------------
``fx.position``          internal ``_Pos`` (``None`` when flat); ``await fx.get_position(sym)`` gives the model
``fx.wallet``            wallet balance in USDT (fees and realized PnL applied; excludes unrealized PnL)
``fx.orders``            ``{order_id: FakeOrder}`` every order ever placed
``fx.placed``            list of every ``OrderRequest`` passed to ``place_order`` (in order, incl. rejected)
``fx.cancelled``         order ids that were cancelled
``fx.leverage_calls``    ints passed to ``set_leverage`` (after the 3x cap, like the real connector)
``fx.margin_calls``      modes passed to ``set_margin_mode``
``fx.calls``             ``[(method_name, args_tuple)]`` for every public call
``fx.position_stop_loss`` inline stop-loss price recorded from the entry (cleared when the position
                         goes flat, which is how Bybit treats a position-level SL; unverified here)
``fx.cancel_conditional_calls``  symbols passed to ``cancel_conditional_orders``
``fx.last_error``        text of the last order rejected by the fake (like ``BybitConnector.last_error``)
``fx.last_error_ambiguous``  True when that rejection was ambiguous (timeout-like), like the connector
``fx.cancel_open_calls``  symbols passed to ``cancel_open_orders`` (cancels every open order)

Market data controls
--------------------
``set_book(bid, ask, qty=10.0)``  order book and ticker (``last`` = mid unless ``set_last``)
``set_last(price)``               ticker last price
``set_candles(rows)`` / ``add_candle(row)``  raw ccxt rows ``[ts_ms, o, h, l, c, v]``. The caller
    decides whether the last row is the in-progress bar (as Bybit does). ``get_ohlcv`` returns the
    last ``limit`` rows, or the first ``limit`` rows with ``ts >= since`` when ``since`` is given.
``set_balance(wallet=None, free=None)``  override wallet / free margin (``free`` otherwise derived:
    wallet minus position notional / leverage)
``set_position(side, size, entry_price)``  create a position directly (no balance effect)
``set_min_sizes(min_qty=0.001, qty_step=0.001, min_notional=5.0)``

Limit-order fill behaviour
--------------------------
Post-only limits are rejected with ``PostOnlyRejected`` when they cross the book (buy >= ask,
sell <= bid). Otherwise they rest as ``open`` and their fate follows a *plan*, evaluated each
time the order is polled (``fetch_order_status`` / ``fetch_order_result``; the poll counter only
advances while the order is open). Plans are consumed one per placed limit order from a queue
(``plan_fills(...)``); when the queue is empty ``default`` applies (``fill_limits(plan)``, initially
``"full"``).

    "full"              fills completely on the first poll
    "never"             never fills
    "instant"           fills completely inside ``place_order`` (returned status ``filled``)
    ("after", n)        fills completely once the order has been polled n times
    ("partial", f)      fills fraction f (floored to step) on the first poll, then stays open
    ("partial_after", n, f)  same, starting at poll n
    ("partial_then_full", n, f)  fraction f on the first poll, rest on poll n

``fx.fill_order(order_id, qty=None)`` fills an open order by hand (all remaining or ``qty``).
A cancelled order keeps what it filled; ``fetch_order_result`` then reports ``cancelled`` with
``filled_qty`` (as ccxt does for Bybit ``PartiallyFilledCanceled``).

Market orders fill at once at the book (buy at ask, sell at bid) with the taker fee. Reduce-only
orders are rejected when there is no position or the side would increase it; a quantity above the
position is clipped to the position size (Bybit behaviour assumed, not verified). Orders below
``min_qty`` or notional below ``min_notional`` are rejected; quantities are floored to the step.
Fees: 0.02% maker (limit fills), 0.055% taker (market fills). Fills update position and wallet.

Failure injection
-----------------
``fail_next(method, n=1, exc=None)``  the next n calls of ``method`` raise ``exc``
(default ``RuntimeError("injected failure")``). ``place_order`` raising is *not* what the real
connector does for exchange errors (it returns a rejected result): use ``reject_next(n, error)``
for that, and ``post_only_reject_next(n)`` to force ``PostOnlyRejected``.
``ambiguous_next(n=1, outcome="rests"|"filled"|"lost", raises=False)``: the next n ``place_order``
calls fail like a timeout: ``outcome`` says what really happened on the exchange (``"rests"`` the
order was accepted and rests, ``"filled"`` it was accepted and filled completely, ``"lost"`` it never
arrived); the caller sees either a rejected result with empty order_id and
``last_error_ambiguous=True`` or, with ``raises=True``, a ``TimeoutError``.
``fail_next`` works for every public method name (``get_balance``, ``get_position``,
``fetch_order_result``, ``cancel_order``, ``get_ticker``, ``get_orderbook`` ...).

Stop loss
---------
``trigger_stop_loss()`` closes the position at the recorded SL price as a taker fill (simulates the
exchange-side stop firing) and clears the SL.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from bot.exchange import PostOnlyRejected
from shared.models.market import OrderBook, OrderBookLevel
from shared.models.order import OrderRequest, OrderResult
from shared.models.position import Position

MAKER_FEE = 0.0002
TAKER_FEE = 0.00055
MAX_LEVERAGE = 3


@dataclass
class _Pos:
    side: str  # "long" | "short"
    size: float
    entry_price: float


@dataclass
class FakeOrder:
    id: str
    request_id: str
    symbol: str
    side: str
    type: str
    qty: float
    price: float | None
    post_only: bool
    reduce_only: bool
    stop_loss: float | None
    status: str = "open"  # open | closed | canceled | rejected
    filled: float = 0.0
    avg_price: float | None = None
    fee: float = 0.0
    polls: int = 0
    plan: Any = "full"


class FakeExchange:
    def __init__(self, symbol: str = "BTC/USDT:USDT", wallet: float = 1000.0) -> None:
        self.symbol = symbol
        self.exchange_id = "bybit"
        self.connected = False
        self.wallet = wallet
        self._free_override: float | None = None
        self._bid, self._ask, self._book_qty = 99_990.0, 100_010.0, 10.0
        self._last: float | None = None
        self.candles: list[list[float]] = []
        self.position: _Pos | None = None
        self.position_stop_loss: float | None = None
        self.leverage = MAX_LEVERAGE
        self.min_qty, self.qty_step, self.min_notional = 0.001, 0.001, 5.0

        self.orders: dict[str, FakeOrder] = {}
        self.placed: list[OrderRequest] = []
        self.cancelled: list[str] = []
        self.leverage_calls: list[int] = []
        self.margin_calls: list[str] = []
        self.cancel_conditional_calls: list[str] = []
        self.calls: list[tuple[str, tuple]] = []
        self.last_error = ""
        self.last_error_ambiguous = False
        self.cancel_open_calls: list[str] = []
        self._ambiguous: list[tuple[str, bool]] = []

        self._default_plan: Any = "full"
        self._plans: list[Any] = []
        self._fail: dict[str, list[Exception]] = {}
        self._reject_next: list[str] = []
        self._post_only_reject = 0
        self._ids = itertools.count(1)

    # ── test controls ────────────────────────────────────────────────────

    def set_book(self, bid: float, ask: float, qty: float = 10.0) -> None:
        self._bid, self._ask, self._book_qty = bid, ask, qty

    def set_last(self, price: float) -> None:
        self._last = price

    def set_candles(self, rows: list[list[float]]) -> None:
        self.candles = [list(r) for r in rows]

    def add_candle(self, row: list[float]) -> None:
        self.candles.append(list(row))

    def set_balance(self, wallet: float | None = None, free: float | None = None) -> None:
        if wallet is not None:
            self.wallet = wallet
        self._free_override = free

    def set_position(self, side: str, size: float, entry_price: float) -> None:
        self.position = _Pos(side, size, entry_price)

    def set_min_sizes(self, min_qty: float = 0.001, qty_step: float = 0.001, min_notional: float = 5.0) -> None:
        self.min_qty, self.qty_step, self.min_notional = min_qty, qty_step, min_notional

    def fill_limits(self, plan: Any) -> None:
        """Default plan for resting limit orders (see module docstring)."""
        self._default_plan = plan

    def plan_fills(self, *plans: Any) -> None:
        """Queue plans, one consumed per placed limit order, before the default applies."""
        self._plans.extend(plans)

    def fail_next(self, method: str, n: int = 1, exc: Exception | None = None) -> None:
        for _ in range(n):
            self._fail.setdefault(method, []).append(exc or RuntimeError("injected failure"))

    def reject_next(self, n: int = 1, error: str = "injected rejection") -> None:
        """The next n ``place_order`` calls return a rejected result (like a swallowed exchange error)."""
        self._reject_next.extend([error] * n)

    def ambiguous_next(self, n: int = 1, outcome: str = "rests", raises: bool = False) -> None:
        """The next n ``place_order`` calls time out; ``outcome`` is what happened on the exchange."""
        assert outcome in ("rests", "filled", "lost")
        self._ambiguous.extend([(outcome, raises)] * n)

    def post_only_reject_next(self, n: int = 1) -> None:
        self._post_only_reject += n

    def fill_order(self, order_id: str, qty: float | None = None) -> None:
        o = self.orders[order_id]
        if o.status != "open":
            return
        remaining = o.qty - o.filled
        self._apply_fill(o, remaining if qty is None else min(qty, remaining), o.price or self._ask, maker=True)

    def trigger_stop_loss(self) -> None:
        """Simulate the exchange-side stop firing: close the position at the SL price."""
        if self.position is None or self.position_stop_loss is None:
            return
        pos, px = self.position, self.position_stop_loss
        self._reduce_position(pos.size, px, TAKER_FEE)

    # ── helpers ──────────────────────────────────────────────────────────

    def _call(self, name: str, *args: Any) -> None:
        self.calls.append((name, args))
        queue = self._fail.get(name)
        if queue:
            raise queue.pop(0)

    def _floor_step(self, qty: float) -> float:
        if self.qty_step <= 0:
            return qty
        return max(math.floor((qty + 1e-12) / self.qty_step) * self.qty_step, 0.0)

    @property
    def _last_price(self) -> float:
        return self._last if self._last is not None else (self._bid + self._ask) / 2

    def _used_margin(self) -> float:
        if self.position is None:
            return 0.0
        return self.position.size * self.position.entry_price / self.leverage

    def _unrealized(self) -> float:
        if self.position is None:
            return 0.0
        sign = 1.0 if self.position.side == "long" else -1.0
        return sign * (self._last_price - self.position.entry_price) * self.position.size

    def _reduce_position(self, qty: float, price: float, fee_rate: float) -> None:
        pos = self.position
        assert pos is not None
        qty = min(qty, pos.size)
        sign = 1.0 if pos.side == "long" else -1.0
        self.wallet += sign * (price - pos.entry_price) * qty - qty * price * fee_rate
        pos.size = round(pos.size - qty, 12)
        if pos.size <= 1e-12:
            self.position = None
            self.position_stop_loss = None

    def _apply_fill(self, o: FakeOrder, qty: float, price: float, *, maker: bool) -> None:
        qty = self._floor_step(qty) if qty < o.qty - o.filled - 1e-12 else qty
        if qty <= 0:
            return
        fee_rate = MAKER_FEE if maker else TAKER_FEE
        fee = qty * price * fee_rate
        prev_notional = (o.avg_price or 0.0) * o.filled
        o.filled = round(o.filled + qty, 12)
        o.avg_price = (prev_notional + qty * price) / o.filled
        o.fee += fee
        side_sign = 1.0 if o.side == "buy" else -1.0
        pos = self.position
        if o.reduce_only:
            if pos is not None:
                self._reduce_position(qty, price, fee_rate)
        elif pos is None:
            self.wallet -= fee
            self.position = _Pos("long" if side_sign > 0 else "short", qty, price)
            if o.stop_loss is not None:
                self.position_stop_loss = o.stop_loss
        elif (pos.side == "long") == (side_sign > 0):
            self.wallet -= fee
            new_size = pos.size + qty
            pos.entry_price = (pos.entry_price * pos.size + price * qty) / new_size
            pos.size = round(new_size, 12)
            if o.stop_loss is not None:
                self.position_stop_loss = o.stop_loss
        else:  # opposite side, not reduce-only: reduces (flip not modelled)
            self._reduce_position(qty, price, fee_rate)
        if o.filled >= o.qty - 1e-12:
            o.status = "closed"

    def _advance_plan(self, o: FakeOrder) -> None:
        """Called on each poll of an open order."""
        if o.status != "open":
            return
        o.polls += 1
        plan = o.plan
        kind = plan if isinstance(plan, str) else plan[0]
        price = o.price or self._ask
        remaining = o.qty - o.filled
        if kind == "full":
            self._apply_fill(o, remaining, price, maker=True)
        elif kind == "after":
            if o.polls >= plan[1]:
                self._apply_fill(o, remaining, price, maker=True)
        elif kind == "partial":
            if o.filled == 0:
                self._apply_fill(o, self._floor_step(o.qty * plan[1]), price, maker=True)
        elif kind == "partial_after":
            if o.polls >= plan[1] and o.filled == 0:
                self._apply_fill(o, self._floor_step(o.qty * plan[2]), price, maker=True)
        elif kind == "partial_then_full":
            if o.polls == 1:
                self._apply_fill(o, self._floor_step(o.qty * plan[2]), price, maker=True)
            elif o.polls >= plan[1]:
                self._apply_fill(o, o.qty - o.filled, price, maker=True)
        # "never" / "instant" (already filled): nothing

    @staticmethod
    def _to_result(o: FakeOrder, request_id: str | None = None, *, query: bool = False) -> OrderResult:
        status = {"open": "new", "closed": "filled", "canceled": "cancelled", "rejected": "rejected"}[o.status]
        return OrderResult(
            request_id=request_id or o.request_id,
            order_id=o.id,
            status=status,
            filled_qty=o.filled,
            filled_price=o.avg_price,
            fee=o.fee,
            timestamp=datetime.now(tz=timezone.utc),
        )

    def _rejected(self, order: OrderRequest, error: str, ambiguous: bool = False) -> OrderResult:
        self.last_error = error
        self.last_error_ambiguous = ambiguous
        return OrderResult(
            request_id=order.request_id, order_id="", status="rejected",
            timestamp=datetime.now(tz=timezone.utc),
        )

    # ── Exchange protocol ────────────────────────────────────────────────

    async def connect(self) -> None:
        self._call("connect")
        self.connected = True

    async def disconnect(self) -> None:
        self._call("disconnect")
        self.connected = False

    async def get_ticker(self, symbol: str) -> dict[str, Any]:
        self._call("get_ticker", symbol)
        return {"symbol": symbol, "last": self._last_price, "bid": self._bid, "ask": self._ask}

    async def get_orderbook(self, symbol: str, limit: int = 25) -> OrderBook:
        self._call("get_orderbook", symbol, limit)
        return OrderBook(
            exchange="bybit",
            symbol=symbol,
            bids=[OrderBookLevel(price=self._bid, quantity=self._book_qty)],
            asks=[OrderBookLevel(price=self._ask, quantity=self._book_qty)],
        )

    async def get_ohlcv(self, symbol: str, timeframe: str = "4h", since: int | None = None,
                        limit: int = 1000) -> list[Any]:
        self._call("get_ohlcv", symbol, timeframe, since, limit)
        if since is not None:
            return [list(r) for r in self.candles if r[0] >= since][:limit]
        return [list(r) for r in self.candles[-limit:]]

    async def place_order(self, order: OrderRequest) -> OrderResult:
        self._call("place_order", order)
        self.placed.append(order)
        if self._ambiguous:
            outcome, raises = self._ambiguous.pop(0)
            if outcome != "lost":
                if outcome == "filled":
                    self._plans.insert(0, "instant")  # limit orders fill inside the call; market always do
                await self._place(order)
            if raises:
                raise TimeoutError("injected timeout")
            return self._rejected(order, "RequestTimeout: timed out", ambiguous=True)
        return await self._place(order)

    async def _place(self, order: OrderRequest) -> OrderResult:

        if self._post_only_reject > 0 and order.post_only:
            self._post_only_reject -= 1
            raise PostOnlyRejected("injected post-only rejection (110030)")
        if self._reject_next:
            return self._rejected(order, self._reject_next.pop(0))

        if order.order_type not in ("limit", "market"):
            return self._rejected(order, f"unsupported order type {order.order_type}")

        qty = self._floor_step(order.quantity)
        if qty < self.min_qty - 1e-12:
            return self._rejected(order, f"qty {order.quantity} below min {self.min_qty}")
        ref_price = order.price if order.order_type == "limit" and order.price else (
            self._ask if order.side == "buy" else self._bid)
        if qty * ref_price < self.min_notional:
            return self._rejected(order, "notional below minimum")

        if order.reduce_only:
            pos = self.position
            if pos is None:
                return self._rejected(order, "reduce-only rule not satisfied: no position (110017)")
            if (pos.side == "long") == (order.side == "buy"):
                return self._rejected(order, "reduce-only would increase position (110017)")
            qty = min(qty, pos.size)

        if order.order_type == "limit":
            if order.price is None:
                return self._rejected(order, "limit order requires a price")
            if order.post_only:
                crosses = order.price >= self._ask if order.side == "buy" else order.price <= self._bid
                if crosses:
                    raise PostOnlyRejected("post-only order would immediately match (110030)")

        oid = f"fx-{next(self._ids)}"
        o = FakeOrder(
            id=oid, request_id=order.request_id, symbol=order.symbol, side=order.side,
            type=order.order_type, qty=qty, price=order.price, post_only=order.post_only,
            reduce_only=order.reduce_only, stop_loss=order.stop_loss,
        )
        self.orders[oid] = o

        if order.order_type == "market":
            price = self._ask if order.side == "buy" else self._bid
            self._apply_fill(o, qty, price, maker=False)
            return self._to_result(o, order.request_id)

        o.plan = self._plans.pop(0) if self._plans else self._default_plan
        if o.plan == "instant":
            self._apply_fill(o, qty, order.price, maker=True)
        self.last_error = ""
        self.last_error_ambiguous = False
        return self._to_result(o, order.request_id)

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        self._call("cancel_order", order_id, symbol)
        o = self.orders.get(order_id)
        if o is None or o.status != "open":
            return False
        o.status = "canceled"
        self.cancelled.append(order_id)
        return True

    async def fetch_order_status(self, order_id: str, symbol: str) -> str:
        self._call("fetch_order_status", order_id, symbol)
        o = self.orders.get(order_id)
        if o is None:
            return "unknown"
        self._advance_plan(o)
        return o.status

    async def fetch_order_result(self, order_id: str, symbol: str, request_id: str) -> OrderResult:
        self._call("fetch_order_result", order_id, symbol, request_id)
        o = self.orders.get(order_id)
        if o is None:
            return OrderResult(request_id=request_id, order_id=order_id, status="new")
        self._advance_plan(o)
        return self._to_result(o, request_id)

    async def cancel_open_orders(self, symbol: str) -> int:
        """Cancel every open regular order (the position stop loss is not an order and stays)."""
        self._call("cancel_open_orders", symbol)
        self.cancel_open_calls.append(symbol)
        n = 0
        for o in self.orders.values():
            if o.status == "open":
                o.status = "canceled"
                self.cancelled.append(o.id)
                n += 1
        return n

    async def cancel_conditional_orders(self, symbol: str) -> int:
        self._call("cancel_conditional_orders", symbol)
        self.cancel_conditional_calls.append(symbol)
        return 0

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        self._call("set_leverage", symbol, leverage)
        capped = min(leverage, MAX_LEVERAGE)
        self.leverage = capped
        self.leverage_calls.append(capped)

    async def set_margin_mode(self, symbol: str, mode: str = "isolated") -> None:
        self._call("set_margin_mode", symbol, mode)
        self.margin_calls.append(mode)

    async def get_position(self, symbol: str) -> Position | None:
        self._call("get_position", symbol)
        pos = self.position
        if pos is None or pos.size <= 0:
            return None
        return Position(
            exchange="bybit",
            symbol=symbol,
            side=pos.side,  # type: ignore[arg-type]
            size=pos.size,
            entry_price=pos.entry_price,
            unrealized_pnl=self._unrealized(),
            leverage=float(self.leverage),
            liquidation_price=None,
            margin_used=self._used_margin(),
        )

    async def get_balance(self) -> dict[str, float]:
        self._call("get_balance")
        used = self._used_margin()
        free = self._free_override if self._free_override is not None else max(self.wallet - used, 0.0)
        return {"total": self.wallet, "free": free, "used": used, "wallet": self.wallet}

    async def get_min_order_sizes(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        self._call("get_min_order_sizes", tuple(symbols))
        return {
            s: {"min_qty": self.min_qty, "qty_step": self.qty_step,
                "min_notional": self.min_notional, "contract_size": 1.0}
            for s in symbols
        }

    def price_to_precision(self, symbol: str, price: float) -> float:
        return round(price, 1)
