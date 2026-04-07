"""Safety Checker — pre-trade validation for the Execution Engine.

Responsibilities:
  - Validate orders against configurable limits (size, leverage, margin)
  - Slippage buffer enforcement (spot 0.1%, perp 0.1%, max 0.5%)
  - Network health check: block orders if last API response > 30s
  - Rate limit tracking: block if approaching exchange rate limits
  - Redis health tracking: fail-closed when Redis is unavailable
  - Local memory cache fallback (TTL=60s) when Redis is temporarily unreachable
  - All methods async; structured logging via structlog
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.kill_switch import KILL_SWITCH_ACTIVE_KEY
from shared.log_events import *

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_MAX_ORDER_SIZE: float = 100_000.0  # USD notional
DEFAULT_LEVERAGE_LIMIT: float = 2.0
DEFAULT_MIN_MARGIN_AVAILABLE: float = 50.0  # USD

# Slippage
SLIPPAGE_SPOT: float = 0.001  # 0.1%
SLIPPAGE_PERP: float = 0.001  # 0.1%
SLIPPAGE_MAX_ACCEPTABLE: float = 0.005  # 0.5%

# Network health
NETWORK_TIMEOUT_THRESHOLD: float = 30.0  # seconds

# Rate limiting
DEFAULT_RATE_LIMIT_PER_MINUTE: int = 120  # exchange-specific; Bybit default ~120
RATE_LIMIT_BLOCK_THRESHOLD: float = 0.90  # block at 90% of limit

# Local cache TTL
LOCAL_CACHE_TTL: float = 60.0  # seconds

# Redis health thresholds
REDIS_FAILURE_THRESHOLD: int = 3  # consecutive failures before marking unhealthy


# ---------------------------------------------------------------------------
# Local memory cache
# ---------------------------------------------------------------------------

@dataclass
class _LocalCache:
    """TTL-based local memory cache as Redis fallback."""

    _store: dict[str, tuple[Any, float]] = field(default_factory=dict)

    def get(self, key: str, ttl: float = LOCAL_CACHE_TTL) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.monotonic() - ts > ttl:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (value, time.monotonic())


class SafetyGuard:
    """Pre-trade safety validation layer.

    Instantiated by ``ExecutionEngine``; each call to ``check_order``
    runs all safety checks and returns ``(passed, reason)``.

    Redis Fail-Closed Policy
    ------------------------
    - ConnectionError / TimeoutError from Redis: ``_redis_failure_count``
      increments. After ``_redis_failure_threshold`` consecutive failures
      ``_redis_healthy`` is set to False and **all new orders are blocked**.
    - Cache-miss (key not in Redis): treated as "data not yet available" —
      existing permissive behaviour is retained (warn and allow).
    - Local cache fallback: the last successfully fetched price / equity /
      margin values are stored in ``_local_cache`` with TTL=60 s.  When
      Redis has a connection error but the local cache is still fresh, the
      cached value is used transparently.
    """

    def __init__(
        self,
        *,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
        exchange: str,
        max_order_size: float = DEFAULT_MAX_ORDER_SIZE,
        leverage_limit: float = DEFAULT_LEVERAGE_LIMIT,
        min_margin_available: float = DEFAULT_MIN_MARGIN_AVAILABLE,
        rate_limit_per_minute: int = DEFAULT_RATE_LIMIT_PER_MINUTE,
    ) -> None:
        self._redis = redis
        self._db_pool = db_pool
        self._exchange_id = exchange

        # Configurable limits
        self.max_order_size = max_order_size
        self.leverage_limit = leverage_limit
        self.min_margin_available = min_margin_available
        self.rate_limit_per_minute = rate_limit_per_minute

        # Network health tracking
        self._last_api_response_time: float = time.monotonic()

        # Rate limit tracking: rolling window of call timestamps
        self._api_call_timestamps: list[float] = []

        # Redis health tracking
        self._local_cache = _LocalCache()
        self._redis_healthy: bool = True
        self._redis_failure_count: int = 0
        self._redis_failure_threshold: int = REDIS_FAILURE_THRESHOLD

    # ------------------------------------------------------------------
    # Redis health
    # ------------------------------------------------------------------

    async def _check_redis_health(self) -> bool:
        """Ping Redis to confirm connectivity.

        On success: resets failure counter and marks healthy.
        On failure: increments counter and marks unhealthy once threshold
        is reached.
        """
        try:
            await self._redis.ping()
            self._redis_healthy = True
            self._redis_failure_count = 0
            log.info(REDIS_CONNECTED, message="redis health restored")
            return True
        except Exception as exc:
            self._redis_failure_count += 1
            if self._redis_failure_count >= self._redis_failure_threshold:
                if self._redis_healthy:
                    log.error(
                        REDIS_DISCONNECTED,
                        message="redis marked unhealthy",
                        failure_count=self._redis_failure_count,
                        error=str(exc),
                    )
                self._redis_healthy = False
            return False

    def _record_redis_success(self) -> None:
        """Decrement failure counter on a successful Redis operation."""
        if self._redis_failure_count > 0:
            self._redis_failure_count = max(0, self._redis_failure_count - 1)
        if not self._redis_healthy and self._redis_failure_count == 0:
            self._redis_healthy = True

    def _record_redis_connection_error(self, context: str, error: Exception) -> None:
        """Increment failure counter and potentially mark Redis unhealthy."""
        self._redis_failure_count += 1
        if self._redis_failure_count >= self._redis_failure_threshold:
            if self._redis_healthy:
                log.error(
                    REDIS_DISCONNECTED,
                    message="redis marked unhealthy",
                    context=context,
                    failure_count=self._redis_failure_count,
                    error=str(error),
                )
            self._redis_healthy = False
        else:
            log.warning(
                REDIS_DISCONNECTED,
                message="redis connection error",
                context=context,
                failure_count=self._redis_failure_count,
                error=str(error),
            )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def check_order(self, payload: dict[str, Any]) -> tuple[bool, str]:
        """Run all safety checks against an order payload.

        Returns
        -------
        (passed, reason) -- ``passed`` is True if the order is safe to
        execute; ``reason`` explains why it was blocked when False.
        """
        # 0. Redis connectivity check (fail-closed)
        if not self._redis_healthy:
            # Attempt a ping to see if Redis has recovered
            redis_ok = await self._check_redis_health()
            if not redis_ok:
                reason = (
                    "redis_unavailable: orders blocked until Redis connectivity "
                    "restored (fail-closed)"
                )
                log.error(
                    ORDER_SAFETY_FAILED,
                    message="safety redis fail-closed",
                    request_id=payload.get("request_id"),
                    reason=reason,
                )
                return False, reason

        # 0b. Kill Switch check — block all orders when kill switch is active
        passed, reason = await self._check_kill_switch(payload)
        if not passed:
            return False, reason

        # 1. Max order size
        passed, reason = await self._check_max_order_size(payload)
        if not passed:
            return False, reason

        # 2. Leverage limit
        passed, reason = await self._check_leverage_limit(payload)
        if not passed:
            return False, reason

        # 3. Margin availability
        passed, reason = await self._check_margin_availability(payload)
        if not passed:
            return False, reason

        # 4. Slippage buffer
        passed, reason = await self._check_slippage(payload)
        if not passed:
            return False, reason

        # 5. Network health
        passed, reason = await self._check_network_health()
        if not passed:
            return False, reason

        # 6. Rate limits
        passed, reason = await self._check_rate_limit()
        if not passed:
            return False, reason

        log.debug(ORDER_SAFETY_PASSED, message="safety checks passed", request_id=payload.get("request_id"))
        return True, ""

    # ------------------------------------------------------------------
    # Network health
    # ------------------------------------------------------------------

    def record_api_response(self) -> None:
        """Called after every successful API interaction to track liveness."""
        self._last_api_response_time = time.monotonic()

    def record_api_call(self) -> None:
        """Record an outgoing API call for rate-limit tracking."""
        now = time.monotonic()
        self._api_call_timestamps.append(now)
        # Prune entries older than 60 seconds
        cutoff = now - 60.0
        self._api_call_timestamps = [
            ts for ts in self._api_call_timestamps if ts >= cutoff
        ]

    @property
    def seconds_since_last_response(self) -> float:
        return time.monotonic() - self._last_api_response_time

    @property
    def api_calls_last_minute(self) -> int:
        now = time.monotonic()
        cutoff = now - 60.0
        return sum(1 for ts in self._api_call_timestamps if ts >= cutoff)

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    async def _check_kill_switch(
        self, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        """Block all new orders while the system-wide Kill Switch is active.

        Reads the ``ce:kill_switch:active`` key from Redis.  Treats any Redis
        error as *fail-closed* (order blocked) to ensure safety even when
        connectivity is degraded.
        """
        try:
            raw = await self._redis.get(KILL_SWITCH_ACTIVE_KEY)
            if raw is not None:
                active = str(raw).strip().lower()
                if active in ("1", "true", "yes"):
                    reason = "kill_switch_active: all orders blocked by Kill Switch"
                    log.error(
                        ORDER_SAFETY_FAILED,
                        message="safety kill switch active",
                        request_id=payload.get("request_id"),
                        reason=reason,
                    )
                    return False, reason
            return True, ""
        except (ConnectionError, TimeoutError, OSError) as exc:
            self._record_redis_connection_error("kill_switch", exc)
            # Fail-closed: if we cannot check kill switch status, block the order
            reason = f"kill_switch_check_failed: redis error={exc}"
            log.error(
                ORDER_SAFETY_FAILED,
                message="safety kill switch check failed — fail-closed",
                request_id=payload.get("request_id"),
                error=str(exc),
            )
            return False, reason
        except Exception as exc:
            log.warning(
                ORDER_SAFETY_FAILED,
                message="kill switch check unexpected error — fail-closed",
                request_id=payload.get("request_id"),
                error=str(exc),
            )
            return False, f"kill_switch_check_error: {exc}"

    async def _check_max_order_size(
        self, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        """Reject orders exceeding the configured maximum notional size."""
        quantity = float(payload.get("quantity", 0))
        price = payload.get("price")

        if price is not None:
            notional = quantity * float(price)
        else:
            # For market orders without a price, estimate from cache
            notional = await self._estimate_notional(
                payload.get("symbol", ""), quantity
            )

        if notional > self.max_order_size:
            reason = (
                f"order_size_exceeded: notional={notional:.2f} "
                f"> max={self.max_order_size:.2f}"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety order size exceeded",
                request_id=payload.get("request_id"),
                notional=notional,
                max_order_size=self.max_order_size,
            )
            return False, reason

        return True, ""

    async def _check_leverage_limit(
        self, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        """Ensure the order does not exceed the configured leverage limit.

        Checks both explicit leverage in the payload and the implied
        leverage based on existing positions + this new order.
        """
        # Direct leverage field (some strategies send it)
        requested_leverage = float(payload.get("leverage", 0))
        if requested_leverage > self.leverage_limit:
            reason = (
                f"leverage_exceeded: requested={requested_leverage} "
                f"> limit={self.leverage_limit}"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety leverage exceeded",
                request_id=payload.get("request_id"),
                requested=requested_leverage,
                limit=self.leverage_limit,
            )
            return False, reason

        # Implied leverage check from cached balance + positions
        symbol = payload.get("symbol", "")
        quantity = float(payload.get("quantity", 0))
        price = payload.get("price")

        if price is not None:
            order_notional = quantity * float(price)
        else:
            order_notional = await self._estimate_notional(symbol, quantity)

        equity = await self._get_cached_equity()
        if equity > 0:
            # Sum existing position notionals
            existing_notional = await self._get_total_position_notional()
            implied_leverage = (existing_notional + order_notional) / equity
            if implied_leverage > self.leverage_limit:
                reason = (
                    f"implied_leverage_exceeded: implied={implied_leverage:.2f} "
                    f"> limit={self.leverage_limit}"
                )
                log.warning(
                    ORDER_SAFETY_FAILED,
                    message="safety implied leverage exceeded",
                    request_id=payload.get("request_id"),
                    implied=implied_leverage,
                    limit=self.leverage_limit,
                )
                return False, reason

        return True, ""

    async def _check_margin_availability(
        self, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        """Check that there is sufficient free margin to place this order."""
        free_margin = await self._get_free_margin()

        if free_margin is not None and free_margin < self.min_margin_available:
            reason = (
                f"insufficient_margin: available={free_margin:.2f} "
                f"< minimum={self.min_margin_available:.2f}"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety insufficient margin",
                request_id=payload.get("request_id"),
                free_margin=free_margin,
                min_required=self.min_margin_available,
            )
            return False, reason

        return True, ""

    async def _check_slippage(
        self, payload: dict[str, Any]
    ) -> tuple[bool, str]:
        """Validate that the order price is within acceptable slippage.

        Compares the order price against the last known market price.
        Limit orders with post_only=True skip the slippage check since
        they will not cross the spread.
        """
        order_type = payload.get("order_type", "")
        post_only = payload.get("post_only", False)

        # Post-only limit orders cannot slip by definition
        if order_type == "limit" and post_only:
            return True, ""

        price = payload.get("price")
        if price is None:
            # Market orders: we rely on the exchange for fill price;
            # just ensure max acceptable slippage flag is set
            return True, ""

        price = float(price)
        symbol = payload.get("symbol", "")
        market_price = await self._get_last_market_price(symbol)

        if market_price is None or market_price <= 0:
            # Cannot verify slippage without market data -- allow but warn
            log.warning(
                ORDER_SAFETY_PASSED,
                message="safety no market price, allowing order",
                request_id=payload.get("request_id"),
                symbol=symbol,
            )
            return True, ""

        # Determine buffer based on instrument type
        is_perp = "PERP" in symbol.upper() or ":" in symbol or "/" in symbol
        buffer = SLIPPAGE_PERP if is_perp else SLIPPAGE_SPOT

        side = payload.get("side", "")
        if side == "buy":
            deviation = (price - market_price) / market_price
        else:
            deviation = (market_price - price) / market_price

        if deviation > SLIPPAGE_MAX_ACCEPTABLE:
            reason = (
                f"slippage_exceeded: deviation={deviation:.4f} "
                f"(max={SLIPPAGE_MAX_ACCEPTABLE:.4f})"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety slippage exceeded",
                request_id=payload.get("request_id"),
                symbol=symbol,
                deviation=deviation,
                max_acceptable=SLIPPAGE_MAX_ACCEPTABLE,
                buffer=buffer,
            )
            return False, reason

        if deviation > buffer:
            log.info(
                ORDER_SAFETY_PASSED,
                message="safety slippage warning",
                request_id=payload.get("request_id"),
                symbol=symbol,
                deviation=deviation,
                buffer=buffer,
            )

        return True, ""

    async def _check_network_health(self) -> tuple[bool, str]:
        """Block new orders if the exchange API has not responded recently."""
        elapsed = self.seconds_since_last_response

        if elapsed > NETWORK_TIMEOUT_THRESHOLD:
            reason = (
                f"network_unhealthy: last_response={elapsed:.1f}s ago "
                f"(threshold={NETWORK_TIMEOUT_THRESHOLD:.0f}s)"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety network unhealthy",
                elapsed=elapsed,
                threshold=NETWORK_TIMEOUT_THRESHOLD,
            )
            return False, reason

        return True, ""

    async def _check_rate_limit(self) -> tuple[bool, str]:
        """Block orders if we are approaching the exchange rate limit."""
        calls = self.api_calls_last_minute
        threshold = int(self.rate_limit_per_minute * RATE_LIMIT_BLOCK_THRESHOLD)

        if calls >= threshold:
            reason = (
                f"rate_limit_near: {calls}/{self.rate_limit_per_minute} "
                f"calls in last 60s (block at {threshold})"
            )
            log.warning(
                ORDER_SAFETY_FAILED,
                message="safety rate limit near",
                calls=calls,
                limit=self.rate_limit_per_minute,
                threshold=threshold,
            )
            return False, reason

        return True, ""

    # ------------------------------------------------------------------
    # Data helpers (Redis / DB lookups)
    # ------------------------------------------------------------------

    async def _estimate_notional(self, symbol: str, quantity: float) -> float:
        """Estimate order notional using cached market price."""
        price = await self._get_last_market_price(symbol)
        if price and price > 0:
            return quantity * price
        # Conservative fallback: return quantity as notional (for USDT pairs ~1:1)
        return quantity

    async def _get_last_market_price(self, symbol: str) -> float | None:
        """Read last ticker price from Redis cache.

        On success: stores result in local cache and returns the price.
        On ConnectionError/TimeoutError: increments failure counter,
          attempts local cache fallback, returns cached value or None.
        On other exceptions (e.g. JSON decode, key missing): returns None
          and retains existing permissive behaviour.
        """
        key = f"cache:ticker:{self._exchange_id}:{symbol}"
        cache_key = f"local:ticker:{symbol}"
        try:
            raw = await self._redis.get(key)
            if raw is not None:
                data = json.loads(raw)
                if isinstance(data, dict):
                    price = float(data.get("last", 0) or 0)
                else:
                    price = float(data)
                if price > 0:
                    self._local_cache.set(cache_key, price)
                    self._record_redis_success()
                return price if price > 0 else None
        except (ConnectionError, TimeoutError, OSError) as exc:
            self._record_redis_connection_error("market_price", exc)
            cached = self._local_cache.get(cache_key)
            if cached is not None:
                log.info(
                    SERVICE_HEALTH_OK,
                    message="using local cache market price",
                    symbol=symbol,
                    price=cached,
                    failure_count=self._redis_failure_count,
                )
            return cached
        except Exception:
            log.debug(SERVICE_HEALTH_FAIL, message="market price lookup failed", symbol=symbol)
        return None

    async def _get_cached_equity(self) -> float:
        """Read total equity from Redis cache.

        On success: stores result in local cache and returns equity.
        On ConnectionError/TimeoutError: attempts local cache fallback.
        Returns 0.0 only when neither Redis nor local cache has data.
        """
        key = f"cache:balance:{self._exchange_id}"
        cache_key = "local:equity"
        try:
            raw = await self._redis.get(key)
            if raw is not None:
                data = json.loads(raw)
                if isinstance(data, dict):
                    equity = float(data.get("total", 0) or 0)
                    self._local_cache.set(cache_key, equity)
                    self._record_redis_success()
                    return equity
        except (ConnectionError, TimeoutError, OSError) as exc:
            self._record_redis_connection_error("equity", exc)
            cached = self._local_cache.get(cache_key)
            if cached is not None:
                log.info(
                    SERVICE_HEALTH_OK,
                    message="using local cache equity",
                    equity=cached,
                    failure_count=self._redis_failure_count,
                )
                return cached
        except Exception:
            log.debug(SERVICE_HEALTH_FAIL, message="equity lookup failed")
        return 0.0

    async def _get_free_margin(self) -> float | None:
        """Read free (available) margin from Redis cache.

        On success: stores result in local cache and returns margin.
        On ConnectionError/TimeoutError: attempts local cache fallback.
        Returns None when neither Redis nor local cache has data.
        """
        key = f"cache:balance:{self._exchange_id}"
        cache_key = "local:free_margin"
        try:
            raw = await self._redis.get(key)
            if raw is not None:
                data = json.loads(raw)
                if isinstance(data, dict):
                    margin = float(data.get("free", 0) or 0)
                    self._local_cache.set(cache_key, margin)
                    self._record_redis_success()
                    return margin
        except (ConnectionError, TimeoutError, OSError) as exc:
            self._record_redis_connection_error("free_margin", exc)
            cached = self._local_cache.get(cache_key)
            if cached is not None:
                log.info(
                    SERVICE_HEALTH_OK,
                    message="using local cache free margin",
                    free_margin=cached,
                    failure_count=self._redis_failure_count,
                )
                return cached
        except Exception:
            log.debug(SERVICE_HEALTH_FAIL, message="free margin lookup failed")
        return None

    async def _get_total_position_notional(self) -> float:
        """Sum notional of all cached open positions.

        On ConnectionError/TimeoutError: returns last locally cached total
        (if available within TTL) or 0.0 to avoid blocking the leverage
        check when Redis is temporarily unreachable.
        """
        pattern = f"cache:position:{self._exchange_id}:*"
        cache_key = "local:position_notional"
        total = 0.0
        try:
            cursor = b"0"
            while True:
                cursor, keys = await self._redis.scan(
                    cursor=cursor, match=pattern, count=100
                )
                for key in keys:
                    raw = await self._redis.get(key)
                    if raw:
                        data = json.loads(raw)
                        size = float(data.get("size", 0) or 0)
                        entry = float(data.get("entry_price", 0) or 0)
                        total += size * entry
                if cursor == 0 or cursor == b"0":
                    break
            self._local_cache.set(cache_key, total)
            self._record_redis_success()
        except (ConnectionError, TimeoutError, OSError) as exc:
            self._record_redis_connection_error("position_notional", exc)
            cached = self._local_cache.get(cache_key)
            if cached is not None:
                log.info(
                    SERVICE_HEALTH_OK,
                    message="using local cache position notional",
                    notional=cached,
                    failure_count=self._redis_failure_count,
                )
                return cached
        except Exception:
            log.debug(SERVICE_HEALTH_FAIL, message="position notional scan failed")
        return total
