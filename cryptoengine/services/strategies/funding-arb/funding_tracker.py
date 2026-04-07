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

from shared.log_events import *
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

    def __init__(self, strategy_id: str, symbol: str = "BTC/USDT:USDT", redis=None) -> None:
        self.strategy_id = strategy_id
        self.symbol = symbol
        self._payments: list[FundingPayment] = []
        self._cumulative_income: float = 0.0
        self._redis = redis
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
            FA_FUNDING_COLLECTED,
            message="펀딩비 수취 기록",
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
        redis_client = self._redis or get_redis()
        if not redis_client.is_healthy:
            await redis_client.ensure_connected()
        key = f"funding:payments:{self.strategy_id}"
        raw_entries = await redis_client.client.lrange(key, 0, -1)
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
            SERVICE_STARTED,
            message="펀딩 히스토리 로드 완료",
            count=len(self._payments),
            cumulative=self._cumulative_income,
        )

    # ── net profitability (Phase 5) ─────────────────────────────────────

    def estimate_net_profit_per_cycle(
        self,
        funding_rate_8h: float,
        position_usd: float,
        leverage: float = 5.0,
        spot_fee_rate: float = 0.0001,
        perp_fee_rate: float = 0.00055,
        slippage_pct: float = 0.0003,
    ) -> dict[str, float]:
        """진입 → 펀딩 1회 수취 → 청산의 예상 순수익 계산.

        Args:
            funding_rate_8h: 현재 8시간 펀딩비율 (소수점, e.g. 0.0001 = 0.01%)
            position_usd: 포지션 명목가 (USD)
            leverage: 레버리지 (perp 레그에 적용)
            spot_fee_rate: 현물 테이커 수수료율 (기본 0.01%)
            perp_fee_rate: 선물 테이커 수수료율 (기본 0.055%)
            slippage_pct: 편도 슬리피지 (기본 0.03%)

        Returns:
            dict with:
                funding_income_usd: 펀딩 수취 (양수 = 수입)
                entry_fee_usd: 진입 수수료 (양수 = 비용)
                exit_fee_usd: 청산 수수료 (양수 = 비용)
                slippage_usd: 슬리피지 비용 (양수 = 비용)
                net_profit_usd: 순수익 (양수 = 이익)
                breakeven_cycles: BEP 달성에 필요한 최소 펀딩 수취 횟수
                is_profitable: 1회 수취 시 순수익 > 0 여부
        """
        # 펀딩 수취: perp 명목가 × 펀딩비율 × 레버리지
        # (선물 포지션 명목가 = position_usd × leverage / (1 + 1/leverage) × leverage ... 단순화)
        # 실제: perp_notional = position_usd × leverage / (1 + 1/leverage)
        # 더 단순하게: funding = position_usd × funding_rate_8h
        # (position_usd는 이미 양쪽 레그의 총 자본을 의미)
        funding_income_usd = position_usd * funding_rate_8h

        # 수수료: 양쪽 레그 × 진입 + 청산 (왕복)
        entry_fee_usd = position_usd * (spot_fee_rate + perp_fee_rate)
        exit_fee_usd = position_usd * (spot_fee_rate + perp_fee_rate)
        total_fee_usd = entry_fee_usd + exit_fee_usd

        # 슬리피지: 4회 (진입 spot, 진입 perp, 청산 spot, 청산 perp) 각각 편도
        slippage_usd = position_usd * slippage_pct * 4

        # 순수익 (1회 수취 기준)
        net_profit_usd = funding_income_usd - total_fee_usd - slippage_usd

        # BEP: 수수료+슬리피지를 펀딩 수취로 커버하는 데 필요한 횟수
        total_cost = total_fee_usd + slippage_usd
        breakeven_cycles = (total_cost / funding_income_usd) if funding_income_usd > 0 else float("inf")

        return {
            "funding_income_usd": round(funding_income_usd, 6),
            "entry_fee_usd": round(entry_fee_usd, 6),
            "exit_fee_usd": round(exit_fee_usd, 6),
            "slippage_usd": round(slippage_usd, 6),
            "net_profit_usd": round(net_profit_usd, 6),
            "breakeven_cycles": round(breakeven_cycles, 2),
            "is_profitable": net_profit_usd > 0,
        }

    def is_entry_net_profitable(
        self,
        funding_rate_8h: float,
        position_usd: float,
        min_cycles_to_profit: float = 2.0,
        **kwargs: float,
    ) -> bool:
        """진입 조건: 예상 순수익이 min_cycles_to_profit 회 수취 후 양수인지 확인.

        Phase 5에서 보수적 진입을 위해 1회가 아닌 N회 수취 후 BEP를 기준으로 사용.

        Args:
            funding_rate_8h: 현재 펀딩비 (8h 기준)
            position_usd: 포지션 명목가 (USD)
            min_cycles_to_profit: 최소 수취 횟수 내 BEP 달성 필요 (기본 2회)
            **kwargs: estimate_net_profit_per_cycle에 전달되는 추가 파라미터

        Returns:
            True if BEP within min_cycles_to_profit, False otherwise
        """
        result = self.estimate_net_profit_per_cycle(
            funding_rate_8h=funding_rate_8h,
            position_usd=position_usd,
            **kwargs,
        )
        breakeven = result["breakeven_cycles"]
        is_ok = breakeven <= min_cycles_to_profit and result["funding_income_usd"] > 0

        self._log.debug(
            "net_profitability_check",
            funding_rate_8h=funding_rate_8h,
            position_usd=position_usd,
            breakeven_cycles=breakeven,
            min_cycles=min_cycles_to_profit,
            net_profit_usd=result["net_profit_usd"],
            passes=is_ok,
        )
        return is_ok
