"""Funding payment tracker with timing awareness (Hummingbot style).

Bitcoin perpetual swaps pay/receive funding every 8 hours (00:00, 08:00,
16:00 UTC on most exchanges).  This module:

* Records every funding payment.
* Tracks cumulative income.
* Provides timing guards so the strategy never liquidates right before a
  payment window.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any

import structlog

from shared.redis_client import get_redis

logger = structlog.get_logger()

# Standard funding payment times (UTC hours)
FUNDING_HOURS = (0, 8, 16)
# How far in advance to block liquidation commands
BLOCK_LIQUIDATION_MINUTES = 30
# How far in advance to verify position size
VERIFY_POSITION_MINUTES = 5


class FundingPayment:
    """Single funding payment record."""

    __slots__ = ("timestamp", "rate", "payment", "position_size", "symbol")

    def __init__(
        self,
        timestamp: datetime,
        rate: float,
        payment: float,
        position_size: float,
        symbol: str,
    ) -> None:
        self.timestamp = timestamp
        self.rate = rate
        self.payment = payment
        self.position_size = position_size
        self.symbol = symbol

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "rate": self.rate,
            "payment": self.payment,
            "position_size": self.position_size,
            "symbol": self.symbol,
        }


class FundingTracker:
    """Track funding rate payments and provide timing-based guards."""

    def __init__(self, strategy_id: str, symbol: str = "BTC/USDT:USDT") -> None:
        self.strategy_id = strategy_id
        self.symbol = symbol
        self._payments: list[FundingPayment] = []
        self._cumulative_income: float = 0.0
        self._log = logger.bind(component="funding_tracker", strategy_id=strategy_id)

    # ── timing helpers ──────────────────────────────────────────────────

    @staticmethod
    def next_funding_time(now: datetime | None = None) -> datetime:
        """Return the next upcoming funding payment time."""
        now = now or datetime.now(timezone.utc)
        today = now.replace(minute=0, second=0, microsecond=0)
        for hour in FUNDING_HOURS:
            candidate = today.replace(hour=hour)
            if candidate > now:
                return candidate
        # Next day 00:00 UTC
        return (today + timedelta(days=1)).replace(hour=0)

    @staticmethod
    def minutes_until_funding(now: datetime | None = None) -> float:
        """Minutes remaining until the next funding payment."""
        now = now or datetime.now(timezone.utc)
        delta = FundingTracker.next_funding_time(now) - now
        return delta.total_seconds() / 60.0

    def is_liquidation_blocked(self, now: datetime | None = None) -> bool:
        """Return True if we are within the pre-funding block window (30 min)."""
        return self.minutes_until_funding(now) <= BLOCK_LIQUIDATION_MINUTES

    def should_verify_position(self, now: datetime | None = None) -> bool:
        """Return True if we should verify position size (5 min before)."""
        mins = self.minutes_until_funding(now)
        return mins <= VERIFY_POSITION_MINUTES

    def is_post_funding(self, window_minutes: float = 5.0, now: datetime | None = None) -> bool:
        """Return True if a funding payment just happened within *window_minutes*."""
        now = now or datetime.now(timezone.utc)
        for hour in FUNDING_HOURS:
            funding_time = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            elapsed = (now - funding_time).total_seconds() / 60.0
            if 0 <= elapsed <= window_minutes:
                return True
        return False

    # ── payment recording ───────────────────────────────────────────────

    async def record_payment(
        self, rate: float, position_size: float, payment: float
    ) -> None:
        """Record a funding payment and persist to Redis."""
        now = datetime.now(timezone.utc)
        record = FundingPayment(
            timestamp=now,
            rate=rate,
            payment=payment,
            position_size=position_size,
            symbol=self.symbol,
        )
        self._payments.append(record)
        self._cumulative_income += payment

        # Persist to Redis list
        redis = await get_redis()
        key = f"funding:payments:{self.strategy_id}"
        await redis.rpush(key, json.dumps(record.to_dict()))

        # Update cumulative counter
        await redis.set(
            f"funding:cumulative:{self.strategy_id}",
            str(self._cumulative_income),
        )

        self._log.info(
            "funding_payment_recorded",
            rate=rate,
            payment=payment,
            cumulative=self._cumulative_income,
            position_size=position_size,
        )

    # ── queries ─────────────────────────────────────────────────────────

    @property
    def cumulative_income(self) -> float:
        return self._cumulative_income

    @property
    def payment_count(self) -> int:
        return len(self._payments)

    def recent_payments(self, n: int = 10) -> list[FundingPayment]:
        """Return the *n* most recent payments."""
        return self._payments[-n:]

    def average_rate(self, n: int = 30) -> float:
        """Average funding rate over the last *n* payments."""
        recent = self._payments[-n:]
        if not recent:
            return 0.0
        return sum(p.rate for p in recent) / len(recent)

    async def load_from_redis(self) -> None:
        """Hydrate state from Redis on startup."""
        redis = await get_redis()
        key = f"funding:payments:{self.strategy_id}"
        raw_entries = await redis.lrange(key, 0, -1)
        for raw in raw_entries:
            data = json.loads(raw)
            fp = FundingPayment(
                timestamp=datetime.fromisoformat(data["timestamp"]),
                rate=data["rate"],
                payment=data["payment"],
                position_size=data["position_size"],
                symbol=data["symbol"],
            )
            self._payments.append(fp)
            self._cumulative_income += fp.payment

        self._log.info(
            "funding_history_loaded",
            count=len(self._payments),
            cumulative=self._cumulative_income,
        )
