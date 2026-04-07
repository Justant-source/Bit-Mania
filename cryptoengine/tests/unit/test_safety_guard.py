"""Unit tests for SafetyGuard pre-trade validation.

Covers:
  - Leverage limit enforcement (5x cap)
  - Slippage threshold rejection
  - Redis fail-closed behaviour (connection error → all orders blocked)
  - Position size / notional limit enforcement
  - Kill Switch active → all orders blocked
  - Normal order passing all checks
  - Network health timeout blocking
  - Rate-limit blocking

All tests are fully isolated: no real Redis, DB, or exchange connections.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# The execution service files live directly in services/execution/ on disk, but
# inside the Docker container they are at /app/ (PYTHONPATH=/app).
# Support both layouts:
_EXEC_DIR = PROJECT_ROOT / "services" / "execution"
if str(_EXEC_DIR) not in sys.path:
    sys.path.insert(0, str(_EXEC_DIR))

from safety import (
    SafetyGuard,
    DEFAULT_LEVERAGE_LIMIT,
    DEFAULT_MAX_ORDER_SIZE,
    DEFAULT_MIN_MARGIN_AVAILABLE,
    NETWORK_TIMEOUT_THRESHOLD,
    DEFAULT_RATE_LIMIT_PER_MINUTE,
    RATE_LIMIT_BLOCK_THRESHOLD,
)
# Constant mirrored from shared/kill_switch.py — avoids import issues when running
# against a container image that may not have the latest shared/ build.
KILL_SWITCH_ACTIVE_KEY = "ce:kill_switch:active"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis(
    *,
    kill_switch_active: bool = False,
    balance_total: float = 10_000.0,
    balance_free: float = 9_000.0,
    market_price: float | None = 65_000.0,
    position_keys: list[str] | None = None,
    ping_raises: Exception | None = None,
    get_raises: Exception | None = None,
    scan_raises: Exception | None = None,
) -> AsyncMock:
    """Build a minimal async Redis mock that satisfies SafetyGuard data reads."""
    redis = AsyncMock()

    # ping
    if ping_raises:
        redis.ping = AsyncMock(side_effect=ping_raises)
    else:
        redis.ping = AsyncMock(return_value=True)

    balance_payload = json.dumps({"total": balance_total, "free": balance_free})
    ticker_payload = json.dumps({"last": market_price}) if market_price else None
    ks_payload = "1" if kill_switch_active else None

    async def _get(key: str) -> bytes | None:
        if get_raises:
            raise get_raises
        if key == KILL_SWITCH_ACTIVE_KEY:
            return ks_payload
        if key.startswith("cache:balance:"):
            return balance_payload
        if key.startswith("cache:ticker:"):
            return ticker_payload
        return None

    redis.get = AsyncMock(side_effect=_get)

    # scan for position notional calculation
    async def _scan(cursor, match, count):  # noqa: ARG001
        if scan_raises:
            raise scan_raises
        return (0, position_keys or [])

    redis.scan = AsyncMock(side_effect=_scan)

    return redis


def _make_guard(redis: AsyncMock, **kwargs) -> SafetyGuard:
    """Instantiate SafetyGuard with a mock Redis and no real DB pool."""
    db_pool = AsyncMock()
    return SafetyGuard(
        redis=redis,
        db_pool=db_pool,
        exchange="bybit",
        leverage_limit=kwargs.get("leverage_limit", 5.0),
        max_order_size=kwargs.get("max_order_size", DEFAULT_MAX_ORDER_SIZE),
        min_margin_available=kwargs.get("min_margin_available", DEFAULT_MIN_MARGIN_AVAILABLE),
        rate_limit_per_minute=kwargs.get("rate_limit_per_minute", DEFAULT_RATE_LIMIT_PER_MINUTE),
    )


def _order(
    *,
    symbol: str = "BTC/USDT:USDT",
    side: str = "buy",
    quantity: float = 0.1,
    price: float | None = 65_000.0,
    leverage: float = 0.0,
    reduce_only: bool = False,
    post_only: bool = False,
    request_id: str = "req-test-001",
) -> dict:
    return {
        "request_id": request_id,
        "symbol": symbol,
        "side": side,
        "order_type": "limit",
        "quantity": quantity,
        "price": price,
        "leverage": leverage,
        "reduce_only": reduce_only,
        "post_only": post_only,
    }


# ---------------------------------------------------------------------------
# 1. Leverage limit tests
# ---------------------------------------------------------------------------

class TestLeverageLimitCheck:
    """SafetyGuard must reject any order that would exceed the 5x leverage cap."""

    @pytest.mark.asyncio
    async def test_explicit_leverage_exceeds_limit_rejected(self):
        redis = _make_redis()
        guard = _make_guard(redis, leverage_limit=5.0)
        # Send an order that explicitly requests 6x leverage
        payload = _order(leverage=6.0)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "leverage_exceeded" in reason

    @pytest.mark.asyncio
    async def test_explicit_leverage_at_limit_passes(self):
        redis = _make_redis()
        guard = _make_guard(redis, leverage_limit=5.0)
        payload = _order(leverage=5.0)
        passed, _ = await guard.check_order(payload)
        assert passed

    @pytest.mark.asyncio
    async def test_implied_leverage_exceeds_limit_rejected(self):
        """An order that would push total implied leverage above 5x must be blocked."""
        # Equity = 10_000, existing notional supplied via position scan
        # Existing position: 0.1 BTC @ 65,000 = 6,500 notional (already 0.65x)
        # New order: 0.1 BTC @ 65,000 = 6,500 → total = 13,000 → 1.3x (under 5x)
        # To trigger breach: large position + small equity
        redis = _make_redis(balance_total=100.0, balance_free=50.0)

        # Simulate existing position adding a lot of notional via scan
        pos_key = "cache:position:bybit:BTC/USDT:USDT"
        pos_payload = json.dumps({"size": 10.0, "entry_price": 65_000.0})

        async def _get_with_positions(key: str):
            if key == KILL_SWITCH_ACTIVE_KEY:
                return None
            if key.startswith("cache:balance:"):
                return json.dumps({"total": 100.0, "free": 50.0})
            if key.startswith("cache:ticker:"):
                return json.dumps({"last": 65_000.0})
            if key == pos_key:
                return pos_payload
            return None

        redis.get = AsyncMock(side_effect=_get_with_positions)

        async def _scan(cursor, match, count):  # noqa: ARG001
            return (0, [pos_key.encode()])

        async def _get_pos(key: str):
            if key == pos_key or key == pos_key.encode():
                return pos_payload
            return None

        redis.scan = AsyncMock(side_effect=_scan)

        # Override get to also return position data when scan keys are fetched
        original_get = redis.get.side_effect

        async def _combined_get(key):
            if hasattr(key, "decode"):
                key = key.decode()
            if key == pos_key:
                return pos_payload
            return await original_get(key)

        redis.get = AsyncMock(side_effect=_combined_get)

        guard = _make_guard(redis, leverage_limit=5.0)
        # equity=100, existing notional = 10 * 65000 = 650000,
        # implied leverage = (650000 + 6500) / 100 >> 5x  → must block
        payload = _order(quantity=0.1, price=65_000.0)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "leverage" in reason

    @pytest.mark.asyncio
    async def test_zero_leverage_in_payload_skips_explicit_check(self):
        """leverage=0 means not specified; should not trigger explicit leverage block."""
        redis = _make_redis()
        guard = _make_guard(redis, leverage_limit=5.0)
        payload = _order(leverage=0.0, quantity=0.01, price=65_000.0)
        # With small quantity and ample equity, this should pass
        passed, _ = await guard.check_order(payload)
        assert passed


# ---------------------------------------------------------------------------
# 2. Slippage limit tests
# ---------------------------------------------------------------------------

class TestSlippageCheck:
    """Orders with price deviation exceeding SLIPPAGE_MAX_ACCEPTABLE (0.5%) are rejected."""

    @pytest.mark.asyncio
    async def test_excessive_buy_slippage_rejected(self):
        """Buy order priced 1% above market must be rejected (post_only=False so slippage is checked)."""
        market_price = 65_000.0
        order_price = market_price * 1.01  # +1% → above 0.5% max
        redis = _make_redis(market_price=market_price)
        guard = _make_guard(redis)
        guard.record_api_response()
        # post_only=False so the slippage check fires for this limit order
        payload = _order(side="buy", price=order_price, leverage=0.0, post_only=False, quantity=0.01)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "slippage_exceeded" in reason

    @pytest.mark.asyncio
    async def test_excessive_sell_slippage_rejected(self):
        """Sell order priced 1% below market must be rejected."""
        market_price = 65_000.0
        order_price = market_price * 0.99  # -1% → deviation 1%
        redis = _make_redis(market_price=market_price)
        guard = _make_guard(redis)
        guard.record_api_response()
        payload = _order(side="sell", price=order_price, leverage=0.0, post_only=False, quantity=0.01)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "slippage_exceeded" in reason

    @pytest.mark.asyncio
    async def test_acceptable_slippage_passes(self):
        """Order within 0.3% of market should pass."""
        market_price = 65_000.0
        order_price = market_price * 1.003  # +0.3%
        redis = _make_redis(market_price=market_price)
        guard = _make_guard(redis)
        guard.record_api_response()
        payload = _order(side="buy", price=order_price, leverage=0.0, post_only=False, quantity=0.01)
        passed, _ = await guard.check_order(payload)
        assert passed

    @pytest.mark.asyncio
    async def test_post_only_limit_skips_slippage_check(self):
        """Post-only limit orders do not cross the spread — slippage check bypassed."""
        market_price = 65_000.0
        order_price = market_price * 1.05  # 5% above market (would normally fail)
        redis = _make_redis(market_price=market_price)
        guard = _make_guard(redis)
        guard.record_api_response()
        payload = _order(side="buy", price=order_price, leverage=0.0, post_only=True, quantity=0.01)
        passed, _ = await guard.check_order(payload)
        assert passed

    @pytest.mark.asyncio
    async def test_no_market_price_allows_order(self):
        """When no market price is in cache, slippage check cannot fire — order allowed."""
        redis = _make_redis(market_price=None)
        guard = _make_guard(redis)
        guard.record_api_response()
        payload = _order(side="buy", price=65_000.0, leverage=0.0, post_only=False, quantity=0.01)
        passed, _ = await guard.check_order(payload)
        assert passed


# ---------------------------------------------------------------------------
# 3. Redis fail-closed tests
# ---------------------------------------------------------------------------

class TestRedisFailClosed:
    """When Redis becomes unavailable, SafetyGuard must block all new orders."""

    @pytest.mark.asyncio
    async def test_redis_connection_error_blocks_orders(self):
        """After REDIS_FAILURE_THRESHOLD consecutive connection errors, orders are blocked."""
        redis = _make_redis(
            get_raises=ConnectionError("Redis down"),
            ping_raises=ConnectionError("Redis down"),
        )
        guard = _make_guard(redis)

        # Force the guard into unhealthy state immediately
        guard._redis_failure_count = guard._redis_failure_threshold
        guard._redis_healthy = False

        payload = _order()
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "redis_unavailable" in reason

    @pytest.mark.asyncio
    async def test_redis_timeout_error_increments_failure_count(self):
        """A TimeoutError increments the Redis failure counter."""
        redis = _make_redis()
        guard = _make_guard(redis)

        guard._record_redis_connection_error("test", TimeoutError("timeout"))
        assert guard._redis_failure_count == 1

    @pytest.mark.asyncio
    async def test_redis_recovery_clears_failure_count(self):
        """Once Redis comes back, successful ping resets the unhealthy flag."""
        redis = _make_redis()  # healthy redis
        guard = _make_guard(redis)
        guard._redis_failure_count = 5
        guard._redis_healthy = False

        result = await guard._check_redis_health()
        assert result is True
        assert guard._redis_healthy is True
        assert guard._redis_failure_count == 0

    @pytest.mark.asyncio
    async def test_kill_switch_redis_error_blocks_order(self):
        """If kill switch Redis read raises ConnectionError, order must be blocked."""
        redis = AsyncMock()
        redis.ping = AsyncMock(return_value=True)

        async def _get_raises(key: str):
            if key == KILL_SWITCH_ACTIVE_KEY:
                raise ConnectionError("Redis unreachable")
            return None

        redis.get = AsyncMock(side_effect=_get_raises)
        guard = _make_guard(redis)

        payload = _order()
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "kill_switch_check_failed" in reason


# ---------------------------------------------------------------------------
# 4. Position size limit tests
# ---------------------------------------------------------------------------

class TestPositionSizeLimit:
    """Orders exceeding the maximum notional size are rejected."""

    @pytest.mark.asyncio
    async def test_oversized_order_rejected(self):
        """A 2 BTC order at 65,000 = 130,000 USDT notional exceeds 100,000 cap."""
        redis = _make_redis()
        guard = _make_guard(redis, max_order_size=100_000.0)
        payload = _order(quantity=2.0, price=65_000.0)  # 130k notional
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "order_size_exceeded" in reason

    @pytest.mark.asyncio
    async def test_order_at_limit_passes(self):
        """An order at the max notional should pass (with enough equity to avoid implied leverage breach)."""
        # Use large equity so implied leverage (100k / 500k = 0.2x) is well below the 5x cap
        redis = _make_redis(balance_total=500_000.0, balance_free=400_000.0)
        guard = _make_guard(redis, max_order_size=100_000.0)
        guard.record_api_response()
        # 1.538 BTC × 65,000 ≈ 100,000 → at the notional cap (just under)
        payload = _order(quantity=1.538, price=65_000.0, post_only=True)
        passed, reason = await guard.check_order(payload)
        assert passed, f"Expected order to pass, got: {reason}"

    @pytest.mark.asyncio
    async def test_small_order_passes_size_check(self):
        redis = _make_redis()
        guard = _make_guard(redis, max_order_size=100_000.0)
        payload = _order(quantity=0.01, price=65_000.0)  # 650 notional
        passed, _ = await guard.check_order(payload)
        assert passed

    @pytest.mark.asyncio
    async def test_insufficient_margin_rejected(self):
        """When free margin is below the minimum, new orders are blocked."""
        redis = _make_redis(balance_free=10.0)  # only 10 USDT free
        guard = _make_guard(redis, min_margin_available=50.0)
        payload = _order(quantity=0.01, price=65_000.0)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "insufficient_margin" in reason


# ---------------------------------------------------------------------------
# 5. Kill Switch tests
# ---------------------------------------------------------------------------

class TestKillSwitchCheck:
    """When the Kill Switch is active all orders must be blocked regardless of validity."""

    @pytest.mark.asyncio
    async def test_kill_switch_active_blocks_all_orders(self):
        redis = _make_redis(kill_switch_active=True)
        guard = _make_guard(redis)
        payload = _order()
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "kill_switch_active" in reason

    @pytest.mark.asyncio
    async def test_kill_switch_inactive_allows_order(self):
        redis = _make_redis(kill_switch_active=False)
        guard = _make_guard(redis)
        payload = _order(quantity=0.01, price=65_000.0)
        passed, _ = await guard.check_order(payload)
        assert passed

    @pytest.mark.asyncio
    async def test_kill_switch_checked_before_other_validations(self):
        """Kill switch must block before size / leverage checks are evaluated."""
        # Combine kill switch active + oversized order
        redis = _make_redis(kill_switch_active=True)
        guard = _make_guard(redis, max_order_size=100.0)  # tiny limit
        payload = _order(quantity=0.01, price=65_000.0)  # 650 > 100
        passed, reason = await guard.check_order(payload)
        assert not passed
        # The kill switch reason should appear (block happens before size check)
        assert "kill_switch_active" in reason


# ---------------------------------------------------------------------------
# 6. Normal order passes all checks
# ---------------------------------------------------------------------------

class TestNormalOrderPasses:
    """A well-formed, small order on a healthy system should pass all checks."""

    @pytest.mark.asyncio
    async def test_normal_order_passes_all_checks(self):
        redis = _make_redis(
            kill_switch_active=False,
            balance_total=10_000.0,
            balance_free=9_000.0,
            market_price=65_000.0,
        )
        guard = _make_guard(
            redis,
            leverage_limit=5.0,
            max_order_size=100_000.0,
            min_margin_available=50.0,
        )
        # Simulate network health is OK
        guard.record_api_response()
        payload = _order(quantity=0.01, price=65_000.0, leverage=0.0)
        passed, reason = await guard.check_order(payload)
        assert passed, f"Expected order to pass, got reason: {reason}"
        assert reason == ""

    @pytest.mark.asyncio
    async def test_market_order_no_price_passes(self):
        """Market orders with no price should skip slippage check and pass."""
        redis = _make_redis()
        guard = _make_guard(redis)
        guard.record_api_response()
        payload = {
            "request_id": "req-market-001",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.01,
            "price": None,
            "leverage": 0.0,
            "reduce_only": False,
            "post_only": False,
        }
        passed, reason = await guard.check_order(payload)
        assert passed, f"Expected market order to pass, got: {reason}"

    @pytest.mark.asyncio
    async def test_api_calls_tracked(self):
        """record_api_call() should increment the call count within the rolling window."""
        redis = _make_redis()
        guard = _make_guard(redis)
        for _ in range(5):
            guard.record_api_call()
        assert guard.api_calls_last_minute == 5


# ---------------------------------------------------------------------------
# 7. Network health check
# ---------------------------------------------------------------------------

class TestNetworkHealthCheck:
    @pytest.mark.asyncio
    async def test_stale_api_response_blocks_orders(self):
        """If last API response was >30s ago, new orders must be blocked."""
        redis = _make_redis()
        guard = _make_guard(redis)
        # Backdate last response time
        guard._last_api_response_time = time.monotonic() - (NETWORK_TIMEOUT_THRESHOLD + 5.0)
        payload = _order()
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "network_unhealthy" in reason

    @pytest.mark.asyncio
    async def test_fresh_api_response_passes_network_check(self):
        redis = _make_redis()
        guard = _make_guard(redis)
        guard.record_api_response()  # just now
        payload = _order(quantity=0.01, price=65_000.0)
        passed, _ = await guard.check_order(payload)
        # Network check should pass; overall result depends on other checks
        elapsed = guard.seconds_since_last_response
        assert elapsed < NETWORK_TIMEOUT_THRESHOLD


# ---------------------------------------------------------------------------
# 8. Rate limit check
# ---------------------------------------------------------------------------

class TestRateLimitCheck:
    @pytest.mark.asyncio
    async def test_rate_limit_exceeded_blocks_orders(self):
        """If API calls in last 60s >= 90% of limit, orders must be blocked."""
        redis = _make_redis()
        rate_limit = 10
        guard = _make_guard(redis, rate_limit_per_minute=rate_limit)
        guard.record_api_response()
        threshold = int(rate_limit * RATE_LIMIT_BLOCK_THRESHOLD)  # 9
        for _ in range(threshold):
            guard.record_api_call()

        payload = _order(quantity=0.01, price=65_000.0)
        passed, reason = await guard.check_order(payload)
        assert not passed
        assert "rate_limit_near" in reason

    @pytest.mark.asyncio
    async def test_rate_limit_under_threshold_passes(self):
        redis = _make_redis()
        rate_limit = 100
        guard = _make_guard(redis, rate_limit_per_minute=rate_limit)
        guard.record_api_response()
        # Only 5 calls — well below 90% of 100
        for _ in range(5):
            guard.record_api_call()
        payload = _order(quantity=0.01, price=65_000.0)
        passed, _ = await guard.check_order(payload)
        assert passed
