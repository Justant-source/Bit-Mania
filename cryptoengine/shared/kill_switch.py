"""Multi-level kill-switch for CryptoEngine risk management.

Levels
------
1. Strategy-level stop loss  — halt a single strategy.
2. Portfolio-level drawdown  — daily -1%, weekly -3%, monthly -5%.
3. System-level healthcheck  — exchange / infra failure -> market-close all.
4. Manual emergency          — operator sends /kill via Telegram.

Cooldown: after any trigger the switch stays active for a configurable
period (default 4 h) before ``auto_resume`` can re-enable trading.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import Any

import structlog

from shared.log_events import (
    KILL_SWITCH_COOLDOWN,
    KILL_SWITCH_MANUAL_RESET,
    KILL_SWITCH_RESUMED,
    KILL_SWITCH_TRIGGERED,
)
from shared.models.position import PortfolioState

# Redis key/channel constants for Kill Switch ACK protocol
KILL_SWITCH_CHANNEL = "ce:kill_switch"
KILL_SWITCH_ACTIVE_KEY = "ce:kill_switch:active"
KILL_SWITCH_ACK_CHANNEL = "ce:kill_switch:ack"
KILL_SWITCH_ACK_TIME_KEY = "ce:kill_switch:ack_time"
KILL_SWITCH_ACK_TIMEOUT_SECONDS = 5
KILL_SWITCH_ACK_MAX_RETRIES = 3

log = structlog.get_logger(__name__)


class KillLevel(IntEnum):
    NONE = 0
    STRATEGY = 1
    PORTFOLIO = 2
    SYSTEM = 3
    MANUAL = 4


class KillSwitch:
    """Stateful kill-switch with cooldown and auto-resume."""

    def __init__(
        self,
        daily_limit: float = -0.01,
        weekly_limit: float = -0.03,
        monthly_limit: float = -0.05,
        cooldown_hours: float = 4.0,
        on_trigger: Any | None = None,
        # Phase 5: 절대값 임계값 (None이면 퍼센트만 사용)
        # 퍼센트 AND 절대값 둘 다 초과해야 발동 (노이즈 오발동 방지)
        daily_loss_abs_usd: float | None = None,
        weekly_loss_abs_usd: float | None = None,
        monthly_loss_abs_usd: float | None = None,
    ) -> None:
        self.daily_limit = daily_limit
        self.weekly_limit = weekly_limit
        self.monthly_limit = monthly_limit
        self.cooldown = timedelta(hours=cooldown_hours)

        # callback: async def on_trigger(level, reason) or None
        self._on_trigger = on_trigger

        # Absolute loss limits (USD) — Phase 5 소액 운영 시 노이즈 발동 방지
        # None이면 퍼센트 체크만 사용 (기존 동작)
        self.daily_loss_abs_usd = daily_loss_abs_usd
        self.weekly_loss_abs_usd = weekly_loss_abs_usd
        self.monthly_loss_abs_usd = monthly_loss_abs_usd

        self._active_level: KillLevel = KillLevel.NONE
        self._triggered_at: datetime | None = None
        self._reason: str = ""
        self._affected_strategies: set[str] = set()
        self._lock = asyncio.Lock()

    # ── public properties ────────────────────────────────────────────────

    @property
    def is_triggered(self) -> bool:
        return self._active_level > KillLevel.NONE

    @property
    def level(self) -> KillLevel:
        return self._active_level

    @property
    def reason(self) -> str:
        return self._reason

    @property
    def triggered_at(self) -> datetime | None:
        return self._triggered_at

    # ── check ────────────────────────────────────────────────────────────

    async def check(
        self,
        portfolio: PortfolioState,
        *,
        monthly_drawdown: float = 0.0,
        system_healthy: bool = True,
        equity_at_open: float = 0.0,     # Phase 5: 절대값 계산 기준 (당일 시작 자본)
    ) -> KillLevel:
        """Evaluate all kill-switch conditions and trigger if needed.

        Absolute-value mode (Phase 5): when ``daily_loss_abs_usd`` etc. are set,
        BOTH the percentage threshold AND the absolute threshold must be breached
        before triggering.  This prevents noise-triggered stops with tiny capital.

        Returns the highest active kill level.
        """
        async with self._lock:
            # If already triggered, check cooldown for possible auto-resume.
            if self.is_triggered:
                if await self._try_auto_resume():
                    log.info(KILL_SWITCH_RESUMED, message="Kill Switch 해제 (쿨다운 만료)")
                else:
                    return self._active_level

            # Level 3 — system healthcheck
            if not system_healthy:
                await self._trigger(
                    KillLevel.SYSTEM,
                    "System healthcheck failure — closing all positions",
                )
                return self._active_level

            # Level 2 — portfolio drawdown (절대값 + 퍼센트 혼합 체크)
            ref_equity = equity_at_open if equity_at_open > 0 else 1.0

            # Daily check
            pct_breach_daily = portfolio.daily_drawdown <= self.daily_limit
            if self.daily_loss_abs_usd is not None:
                daily_loss_usd = abs(portfolio.daily_drawdown) * ref_equity
                abs_breach_daily = daily_loss_usd >= self.daily_loss_abs_usd
                should_trigger_daily = pct_breach_daily and abs_breach_daily
                reason_daily = (
                    f"Daily drawdown {portfolio.daily_drawdown:.2%} ({daily_loss_usd:.2f} USD) "
                    f"breached pct={self.daily_limit:.2%} AND abs={self.daily_loss_abs_usd:.2f} USD"
                )
            else:
                should_trigger_daily = pct_breach_daily
                reason_daily = f"Daily drawdown {portfolio.daily_drawdown:.2%} breached limit {self.daily_limit:.2%}"

            if should_trigger_daily:
                await self._trigger(KillLevel.PORTFOLIO, reason_daily)
                return self._active_level

            # Weekly check
            pct_breach_weekly = portfolio.weekly_drawdown <= self.weekly_limit
            if self.weekly_loss_abs_usd is not None:
                weekly_loss_usd = abs(portfolio.weekly_drawdown) * ref_equity
                abs_breach_weekly = weekly_loss_usd >= self.weekly_loss_abs_usd
                should_trigger_weekly = pct_breach_weekly and abs_breach_weekly
                reason_weekly = (
                    f"Weekly drawdown {portfolio.weekly_drawdown:.2%} ({weekly_loss_usd:.2f} USD) "
                    f"breached pct={self.weekly_limit:.2%} AND abs={self.weekly_loss_abs_usd:.2f} USD"
                )
            else:
                should_trigger_weekly = pct_breach_weekly
                reason_weekly = f"Weekly drawdown {portfolio.weekly_drawdown:.2%} breached limit {self.weekly_limit:.2%}"

            if should_trigger_weekly:
                await self._trigger(KillLevel.PORTFOLIO, reason_weekly)
                return self._active_level

            # Monthly check
            pct_breach_monthly = monthly_drawdown <= self.monthly_limit
            if self.monthly_loss_abs_usd is not None:
                monthly_loss_usd = abs(monthly_drawdown) * ref_equity
                abs_breach_monthly = monthly_loss_usd >= self.monthly_loss_abs_usd
                should_trigger_monthly = pct_breach_monthly and abs_breach_monthly
                reason_monthly = (
                    f"Monthly drawdown {monthly_drawdown:.2%} ({monthly_loss_usd:.2f} USD) "
                    f"breached pct={self.monthly_limit:.2%} AND abs={self.monthly_loss_abs_usd:.2f} USD"
                )
            else:
                should_trigger_monthly = pct_breach_monthly
                reason_monthly = f"Monthly drawdown {monthly_drawdown:.2%} breached limit {self.monthly_limit:.2%}"

            if should_trigger_monthly:
                await self._trigger(KillLevel.PORTFOLIO, reason_monthly)
                return self._active_level

            return KillLevel.NONE

    # ── strategy-level (L1) ──────────────────────────────────────────────

    async def check_strategy(
        self,
        strategy_id: str,
        current_pnl: float,
        max_drawdown: float,
    ) -> bool:
        """Return True if the strategy should be stopped."""
        if current_pnl <= max_drawdown:
            async with self._lock:
                self._affected_strategies.add(strategy_id)
                if self._active_level < KillLevel.STRATEGY:
                    await self._trigger(
                        KillLevel.STRATEGY,
                        f"Strategy {strategy_id} drawdown {current_pnl:.4f} <= {max_drawdown:.4f}",
                    )
            return True
        return False

    # ── manual trigger (L4) ──────────────────────────────────────────────

    async def trigger_manual(self, reason: str = "Manual emergency via Telegram") -> None:
        async with self._lock:
            await self._trigger(KillLevel.MANUAL, reason)

    # ── trigger internals ────────────────────────────────────────────────

    async def trigger(self, level: KillLevel, reason: str) -> None:
        """Public trigger entry-point (used by external callers)."""
        async with self._lock:
            await self._trigger(level, reason)

    async def _trigger(self, level: KillLevel, reason: str) -> None:
        self._active_level = level
        self._reason = reason
        self._triggered_at = datetime.now(tz=timezone.utc)
        log.critical(KILL_SWITCH_TRIGGERED, message="Kill Switch 발동", level=int(level), reason=reason)

        if self._on_trigger is not None:
            try:
                await self._on_trigger(level, reason)
            except Exception:
                log.exception("on_trigger callback failed")

    # ── cooldown / auto-resume ───────────────────────────────────────────

    async def _try_auto_resume(self) -> bool:
        """Return True and reset if cooldown has elapsed (L4 never auto-resumes)."""
        if self._active_level == KillLevel.MANUAL:
            return False
        if self._triggered_at is None:
            return False
        now = datetime.now(tz=timezone.utc)
        if now - self._triggered_at >= self.cooldown:
            log.info(KILL_SWITCH_COOLDOWN, message="Kill Switch 쿨다운 시작")
            self._reset()
            return True
        return False

    async def auto_resume(self) -> bool:
        """Explicitly attempt auto-resume (e.g. called from a periodic task)."""
        async with self._lock:
            return await self._try_auto_resume()

    def _reset(self) -> None:
        self._active_level = KillLevel.NONE
        self._reason = ""
        self._triggered_at = None
        self._affected_strategies.clear()

    async def reset_manual(self) -> None:
        """Operator-initiated reset (clears even L4)."""
        async with self._lock:
            self._reset()
            log.info(KILL_SWITCH_MANUAL_RESET, message="Kill Switch 수동 리셋")
