"""DCA Scheduler — handles weekly buy timing and purchase history.

Determines when it is time to execute a DCA buy (Monday UTC 00:00),
tracks purchase history to avoid duplicates, and maintains cost basis
calculations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import structlog

from shared.redis_client import RedisClient

logger = structlog.get_logger()

PURCHASE_HISTORY_KEY = "dca:purchase_history"
LAST_BUY_KEY = "dca:last_buy_timestamp"


@dataclass
class PurchaseRecord:
    """A single DCA purchase entry."""

    timestamp: str
    price: float
    quantity: float
    fng_index: int
    multiplier: float


@dataclass
class DCAScheduler:
    """Weekly DCA scheduling and purchase tracking."""

    redis: RedisClient
    strategy_id: str
    _last_buy_ts: float = 0.0
    _purchases: list[PurchaseRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._log = logger.bind(component="dca_scheduler", strategy_id=self.strategy_id)

    # ── timing ─────────────────────────────────────────────────────────

    def is_buy_time(self) -> bool:
        """Return True if it is Monday UTC 00:00 (within a 1-hour window).

        Also checks that we have not already purchased this week.
        """
        now = datetime.now(timezone.utc)

        # Monday = weekday 0, check 00:00-01:00 UTC window
        if now.weekday() != 0:
            return False

        if now.hour >= 1:
            return False

        # Check duplicate: ensure last buy was not in this same window
        if self._last_buy_ts > 0:
            last_buy = datetime.fromtimestamp(self._last_buy_ts, tz=timezone.utc)
            # Same day check
            if last_buy.date() == now.date():
                self._log.debug("already_purchased_today")
                return False

        return True

    def calculate_next_buy_time(self) -> datetime:
        """Calculate the next Monday 00:00 UTC."""
        now = datetime.now(timezone.utc)
        days_ahead = (7 - now.weekday()) % 7
        if days_ahead == 0 and now.hour >= 1:
            days_ahead = 7

        next_monday = now.replace(hour=0, minute=0, second=0, microsecond=0)
        from datetime import timedelta
        next_monday += timedelta(days=days_ahead)
        return next_monday

    # ── purchase recording ─────────────────────────────────────────────

    async def record_purchase(
        self,
        price: float,
        quantity: float,
        fng_index: int,
        multiplier: float,
    ) -> None:
        """Record a DCA purchase and persist to Redis."""
        now = datetime.now(timezone.utc)
        self._last_buy_ts = now.timestamp()

        record = PurchaseRecord(
            timestamp=now.isoformat(),
            price=price,
            quantity=quantity,
            fng_index=fng_index,
            multiplier=multiplier,
        )
        self._purchases.append(record)

        # Persist to Redis
        await self.redis.set(
            f"{LAST_BUY_KEY}:{self.strategy_id}",
            str(self._last_buy_ts),
        )

        history_key = f"{PURCHASE_HISTORY_KEY}:{self.strategy_id}"
        serialised = json.dumps(
            [
                {
                    "timestamp": p.timestamp,
                    "price": p.price,
                    "quantity": p.quantity,
                    "fng_index": p.fng_index,
                    "multiplier": p.multiplier,
                }
                for p in self._purchases
            ]
        )
        await self.redis.set(history_key, serialised)

        self._log.info(
            "purchase_recorded",
            price=price,
            quantity=quantity,
            fng_index=fng_index,
            multiplier=multiplier,
        )

    async def load_from_redis(self) -> None:
        """Load purchase history and last buy timestamp from Redis."""
        # Last buy timestamp
        ts_raw = await self.redis.get(f"{LAST_BUY_KEY}:{self.strategy_id}")
        if ts_raw is not None:
            try:
                self._last_buy_ts = float(ts_raw)
            except (ValueError, TypeError):
                pass

        # Purchase history
        history_raw = await self.redis.get(
            f"{PURCHASE_HISTORY_KEY}:{self.strategy_id}"
        )
        if history_raw is not None:
            try:
                data = json.loads(history_raw)
                self._purchases = [
                    PurchaseRecord(
                        timestamp=d["timestamp"],
                        price=d["price"],
                        quantity=d["quantity"],
                        fng_index=d["fng_index"],
                        multiplier=d["multiplier"],
                    )
                    for d in data
                ]
                self._log.info("history_loaded", count=len(self._purchases))
            except (ValueError, TypeError, KeyError):
                self._log.exception("history_load_error")

    # ── cost basis ─────────────────────────────────────────────────────

    @property
    def total_btc_held(self) -> float:
        """Total BTC accumulated across all purchases."""
        return sum(p.quantity for p in self._purchases)

    @property
    def total_cost(self) -> float:
        """Total USD spent across all purchases."""
        return sum(p.price * p.quantity for p in self._purchases)

    @property
    def average_cost_basis(self) -> float:
        """Volume-weighted average purchase price."""
        total_qty = self.total_btc_held
        if total_qty <= 0:
            return 0.0
        return self.total_cost / total_qty

    @property
    def purchase_count(self) -> int:
        """Number of DCA purchases made."""
        return len(self._purchases)
