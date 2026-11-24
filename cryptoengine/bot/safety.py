"""Pre-trade safety checks.

Ported from ``services/execution/safety.py``. Changes:

* The kill gate is ``EntryGate.entries_blocked()`` and applies only to orders
  that are not reduce-only. A reduce-only order is never blocked by a halt.
* Market price comes from ``exchange.get_ticker`` and equity / free margin from
  ``exchange.get_balance`` (both cached for ``cache_ttl`` seconds). The old code
  read Redis keys that nothing wrote, so its slippage and notional checks were
  effectively inert; they now run on real data.
* Fail-closed for entries: if the price, balance or position cannot be read,
  an order that opens risk is rejected. Reduce-only orders pass (the size check
  is skipped when there is no price).
* Dropped: Redis health tracking, ``BYPASS_SAFETY``, the network-health check,
  the explicit ``leverage`` payload field (``OrderRequest`` has none).
* Reduce-only orders are also exempt from the rate-limit check so a close can
  never be refused (the limit is ~108 orders/minute, unreachable here).
"""

from __future__ import annotations

import os
import time
from typing import Any

import structlog

from bot.interfaces import EntryGate, Exchange
from shared.models.order import OrderRequest

log = structlog.get_logger(__name__)

DEFAULT_MAX_ORDER_SIZE: float = 100_000.0  # USD notional
HARD_LEVERAGE_CAP: float = 3.0  # project rule; the limit can never exceed it
DEFAULT_MIN_MARGIN_AVAILABLE: float = 50.0  # USD

SLIPPAGE_MAX_ACCEPTABLE: float = 0.005  # 0.5%

DEFAULT_RATE_LIMIT_PER_MINUTE: int = 120
RATE_LIMIT_BLOCK_THRESHOLD: float = 0.90  # block at 90% of the limit

DEFAULT_CACHE_TTL: float = 5.0  # seconds


class SafetyGuard:
    """Runs all pre-trade checks; ``check_order`` returns ``(passed, reason)``."""

    def __init__(
        self,
        exchange: Exchange,
        gate: EntryGate,
        *,
        leverage_limit: float | None = None,
        max_order_size: float = DEFAULT_MAX_ORDER_SIZE,
        min_margin_available: float = DEFAULT_MIN_MARGIN_AVAILABLE,
        rate_limit_per_minute: int = DEFAULT_RATE_LIMIT_PER_MINUTE,
        cache_ttl: float = DEFAULT_CACHE_TTL,
        clock=time.monotonic,
    ) -> None:
        self._exchange = exchange
        self._gate = gate
        if leverage_limit is None:
            env = os.getenv("SAFETY_LEVERAGE_LIMIT")
            leverage_limit = float(env) if env else HARD_LEVERAGE_CAP
        if leverage_limit > HARD_LEVERAGE_CAP:
            log.warning("leverage_limit_clamped", requested=leverage_limit, cap=HARD_LEVERAGE_CAP)
            leverage_limit = HARD_LEVERAGE_CAP
        self.leverage_limit = leverage_limit
        self.max_order_size = max_order_size
        self.min_margin_available = min_margin_available
        self.rate_limit_per_minute = rate_limit_per_minute
        self._cache_ttl = cache_ttl
        self._clock = clock
        self._cache: dict[str, tuple[Any, float]] = {}
        self._api_call_timestamps: list[float] = []

    # ── rate-limit bookkeeping ───────────────────────────────────────────

    def record_api_call(self) -> None:
        """Record one outgoing order for the rate-limit window."""
        now = self._clock()
        self._api_call_timestamps.append(now)
        cutoff = now - 60.0
        self._api_call_timestamps = [t for t in self._api_call_timestamps if t >= cutoff]

    @property
    def api_calls_last_minute(self) -> int:
        cutoff = self._clock() - 60.0
        return sum(1 for t in self._api_call_timestamps if t >= cutoff)

    # ── public entry point ───────────────────────────────────────────────

    async def check_order(self, order: OrderRequest) -> tuple[bool, str]:
        reduce_only = bool(order.reduce_only)

        # 0. Entry gate (halt / cooldown). Reduce-only orders never consult it.
        if not reduce_only:
            try:
                blocked, why = self._gate.entries_blocked()
            except Exception as exc:  # fail closed
                return self._fail(order, f"entry_gate_error: {exc}")
            if blocked:
                return self._fail(order, f"entries_blocked: {why}")

        # Market price: required for entries, best effort for reduce-only.
        price = await self._market_price(order.symbol)
        if price is None and not reduce_only:
            return self._fail(order, "market_price_unavailable: entry rejected")

        for check in (
            self._check_max_order_size,
            self._check_leverage_limit,
            self._check_margin_availability,
            self._check_slippage,
            self._check_rate_limit,
        ):
            passed, reason = await check(order, price)
            if not passed:
                return self._fail(order, reason)
        return True, ""

    # ── checks ───────────────────────────────────────────────────────────

    async def _check_max_order_size(self, order: OrderRequest, price: float | None) -> tuple[bool, str]:
        if order.reduce_only:  # a close must always be possible
            return True, ""
        ref = order.price if order.price is not None else price
        if ref is None:
            return True, ""
        notional = order.quantity * float(ref)
        if notional > self.max_order_size:
            return False, f"order_size_exceeded: notional={notional:.2f} > max={self.max_order_size:.2f}"
        return True, ""

    async def _check_leverage_limit(self, order: OrderRequest, price: float | None) -> tuple[bool, str]:
        # Reduce-only orders can never increase leverage.
        if order.reduce_only:
            return True, ""
        ref = order.price if order.price is not None else price
        order_notional = order.quantity * float(ref)  # price is not None for entries

        balance = await self._balance()
        if balance is None:
            return False, "balance_unavailable: entry rejected"
        equity = float(balance.get("total", 0) or 0)
        if equity <= 0:
            return True, ""  # nothing to compare against (old behaviour)

        try:
            position = await self._exchange.get_position(order.symbol)  # never cached
        except Exception as exc:
            log.warning("safety_position_read_failed", error=str(exc)[:200])
            return False, "position_unavailable: entry rejected"
        existing = position.size * position.entry_price if position is not None else 0.0

        implied = (existing + order_notional) / equity
        if implied > self.leverage_limit:
            return False, f"implied_leverage_exceeded: implied={implied:.2f} > limit={self.leverage_limit}"
        return True, ""

    async def _check_margin_availability(self, order: OrderRequest, price: float | None) -> tuple[bool, str]:
        if order.reduce_only:
            return True, ""
        balance = await self._balance()
        if balance is None:
            return False, "balance_unavailable: entry rejected"
        free = float(balance.get("free", 0) or 0)
        if free < self.min_margin_available:
            return False, f"insufficient_margin: available={free:.2f} < minimum={self.min_margin_available:.2f}"
        return True, ""

    async def _check_slippage(self, order: OrderRequest, price: float | None) -> tuple[bool, str]:
        # Post-only limits cannot slip; market orders carry no price to compare.
        if order.order_type == "limit" and order.post_only:
            return True, ""
        if order.price is None or price is None or price <= 0:
            return True, ""
        if order.side == "buy":
            deviation = (order.price - price) / price
        else:
            deviation = (price - order.price) / price
        if deviation > SLIPPAGE_MAX_ACCEPTABLE:
            return False, f"slippage_exceeded: deviation={deviation:.4f} (max={SLIPPAGE_MAX_ACCEPTABLE:.4f})"
        return True, ""

    async def _check_rate_limit(self, order: OrderRequest, price: float | None) -> tuple[bool, str]:
        if order.reduce_only:
            return True, ""
        calls = self.api_calls_last_minute
        threshold = int(self.rate_limit_per_minute * RATE_LIMIT_BLOCK_THRESHOLD)
        if calls >= threshold:
            return False, (
                f"rate_limit_near: {calls}/{self.rate_limit_per_minute} calls in last 60s "
                f"(block at {threshold})"
            )
        return True, ""

    # ── cached exchange reads ────────────────────────────────────────────

    def _cached(self, key: str) -> Any | None:
        hit = self._cache.get(key)
        if hit is None:
            return None
        value, ts = hit
        if self._clock() - ts > self._cache_ttl:
            del self._cache[key]
            return None
        return value

    async def _market_price(self, symbol: str) -> float | None:
        key = f"price:{symbol}"
        cached = self._cached(key)
        if cached is not None:
            return cached
        try:
            ticker = await self._exchange.get_ticker(symbol)
            last = ticker.get("last")
            if not last:
                bid, ask = ticker.get("bid"), ticker.get("ask")
                last = (bid + ask) / 2 if bid and ask else None
            price = float(last) if last else 0.0
        except Exception as exc:
            log.warning("safety_ticker_failed", symbol=symbol, error=str(exc)[:200])
            return None
        if price <= 0:
            return None
        self._cache[key] = (price, self._clock())
        return price

    async def _balance(self) -> dict[str, float] | None:
        cached = self._cached("balance")
        if cached is not None:
            return cached
        try:
            balance = await self._exchange.get_balance()
        except Exception as exc:
            log.warning("safety_balance_failed", error=str(exc)[:200])
            return None
        self._cache["balance"] = (balance, self._clock())
        return balance

    @staticmethod
    def _fail(order: OrderRequest, reason: str) -> tuple[bool, str]:
        log.warning("safety_check_failed", request_id=order.request_id, reason=reason)
        return False, reason
