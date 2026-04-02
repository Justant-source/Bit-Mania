"""Unit tests for the multi-level Kill Switch.

Tests all 4 levels:
  L1 — Strategy-level stop loss
  L2 — Portfolio-level drawdown (daily/weekly/monthly)
  L3 — System-level healthcheck failure
  L4 — Manual emergency via Telegram
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.kill_switch import KillLevel, KillSwitch
from shared.models.position import PortfolioState


@pytest.fixture
def kill_switch():
    return KillSwitch(
        daily_limit=-0.01,
        weekly_limit=-0.03,
        monthly_limit=-0.05,
        cooldown_hours=0.001,  # very short for tests
    )


@pytest.fixture
def healthy_portfolio():
    return PortfolioState(
        total_equity=10000.0,
        daily_drawdown=-0.005,
        weekly_drawdown=-0.01,
    )


@pytest.fixture
def breached_daily_portfolio():
    return PortfolioState(
        total_equity=9500.0,
        daily_drawdown=-0.02,  # below -1% limit
        weekly_drawdown=-0.01,
    )


# ------------------------------------------------------------------
# Level 1 — Strategy stop
# ------------------------------------------------------------------

class TestL1StrategyStop:
    @pytest.mark.asyncio
    async def test_strategy_stop_triggered(self, kill_switch):
        stopped = await kill_switch.check_strategy(
            strategy_id="funding_arb_01",
            current_pnl=-0.05,
            max_drawdown=-0.03,
        )
        assert stopped is True
        assert kill_switch.is_triggered
        assert kill_switch.level == KillLevel.STRATEGY

    @pytest.mark.asyncio
    async def test_strategy_not_stopped(self, kill_switch):
        stopped = await kill_switch.check_strategy(
            strategy_id="funding_arb_01",
            current_pnl=-0.01,
            max_drawdown=-0.03,
        )
        assert stopped is False
        assert not kill_switch.is_triggered

    @pytest.mark.asyncio
    async def test_strategy_stop_at_exact_threshold(self, kill_switch):
        stopped = await kill_switch.check_strategy(
            strategy_id="funding_arb_01",
            current_pnl=-0.03,
            max_drawdown=-0.03,
        )
        assert stopped is True


# ------------------------------------------------------------------
# Level 2 — Portfolio drawdown
# ------------------------------------------------------------------

class TestL2PortfolioLimit:
    @pytest.mark.asyncio
    async def test_daily_drawdown_breach(self, kill_switch, breached_daily_portfolio):
        level = await kill_switch.check(breached_daily_portfolio)
        assert level == KillLevel.PORTFOLIO
        assert kill_switch.is_triggered
        assert "Daily drawdown" in kill_switch.reason

    @pytest.mark.asyncio
    async def test_weekly_drawdown_breach(self, kill_switch):
        portfolio = PortfolioState(
            total_equity=9000.0,
            daily_drawdown=-0.005,
            weekly_drawdown=-0.04,  # below -3% limit
        )
        level = await kill_switch.check(portfolio)
        assert level == KillLevel.PORTFOLIO
        assert "Weekly drawdown" in kill_switch.reason

    @pytest.mark.asyncio
    async def test_monthly_drawdown_breach(self, kill_switch, healthy_portfolio):
        level = await kill_switch.check(
            healthy_portfolio, monthly_drawdown=-0.06
        )
        assert level == KillLevel.PORTFOLIO
        assert "Monthly drawdown" in kill_switch.reason

    @pytest.mark.asyncio
    async def test_no_drawdown_breach(self, kill_switch, healthy_portfolio):
        level = await kill_switch.check(healthy_portfolio)
        assert level == KillLevel.NONE
        assert not kill_switch.is_triggered


# ------------------------------------------------------------------
# Level 3 — System healthcheck
# ------------------------------------------------------------------

class TestL3SystemHealthcheck:
    @pytest.mark.asyncio
    async def test_system_unhealthy(self, kill_switch, healthy_portfolio):
        level = await kill_switch.check(
            healthy_portfolio, system_healthy=False
        )
        assert level == KillLevel.SYSTEM
        assert "healthcheck" in kill_switch.reason.lower()

    @pytest.mark.asyncio
    async def test_system_healthy(self, kill_switch, healthy_portfolio):
        level = await kill_switch.check(
            healthy_portfolio, system_healthy=True
        )
        assert level == KillLevel.NONE

    @pytest.mark.asyncio
    async def test_system_check_takes_priority(self, kill_switch, breached_daily_portfolio):
        """System failure should trigger even if portfolio is also breached."""
        level = await kill_switch.check(
            breached_daily_portfolio, system_healthy=False
        )
        assert level == KillLevel.SYSTEM


# ------------------------------------------------------------------
# Level 4 — Manual emergency
# ------------------------------------------------------------------

class TestL4ManualEmergency:
    @pytest.mark.asyncio
    async def test_manual_trigger(self, kill_switch):
        await kill_switch.trigger_manual("Operator pressed /kill")
        assert kill_switch.is_triggered
        assert kill_switch.level == KillLevel.MANUAL
        assert "Manual" in kill_switch.reason

    @pytest.mark.asyncio
    async def test_manual_does_not_auto_resume(self, kill_switch):
        await kill_switch.trigger_manual()
        # Even after cooldown elapses, manual should not auto-resume
        kill_switch._triggered_at = datetime.now(timezone.utc) - timedelta(hours=24)
        resumed = await kill_switch.auto_resume()
        assert resumed is False
        assert kill_switch.is_triggered

    @pytest.mark.asyncio
    async def test_manual_reset_clears(self, kill_switch):
        await kill_switch.trigger_manual()
        await kill_switch.reset_manual()
        assert not kill_switch.is_triggered
        assert kill_switch.level == KillLevel.NONE


# ------------------------------------------------------------------
# Cooldown and auto-resume
# ------------------------------------------------------------------

class TestCooldownAutoResume:
    @pytest.mark.asyncio
    async def test_auto_resume_after_cooldown(self, kill_switch, breached_daily_portfolio):
        await kill_switch.check(breached_daily_portfolio)
        assert kill_switch.is_triggered

        # Simulate cooldown elapsed
        kill_switch._triggered_at = datetime.now(timezone.utc) - timedelta(hours=1)

        # Re-check with healthy portfolio should auto-resume
        healthy = PortfolioState(
            total_equity=10000.0,
            daily_drawdown=-0.005,
            weekly_drawdown=-0.01,
        )
        level = await kill_switch.check(healthy)
        assert level == KillLevel.NONE

    @pytest.mark.asyncio
    async def test_no_resume_during_cooldown(self):
        ks = KillSwitch(cooldown_hours=24.0)
        portfolio = PortfolioState(
            total_equity=9000.0,
            daily_drawdown=-0.02,
            weekly_drawdown=-0.01,
        )
        await ks.check(portfolio)
        assert ks.is_triggered

        # Check again with healthy portfolio but still in cooldown
        healthy = PortfolioState(
            total_equity=10000.0,
            daily_drawdown=-0.005,
            weekly_drawdown=-0.01,
        )
        level = await ks.check(healthy)
        assert level == KillLevel.PORTFOLIO  # still active

    @pytest.mark.asyncio
    async def test_callback_on_trigger(self):
        callback = AsyncMock()
        ks = KillSwitch(on_trigger=callback)
        await ks.trigger(KillLevel.SYSTEM, "test reason")
        callback.assert_awaited_once_with(KillLevel.SYSTEM, "test reason")

    @pytest.mark.asyncio
    async def test_callback_error_does_not_crash(self):
        async def bad_callback(level, reason):
            raise RuntimeError("callback boom")

        ks = KillSwitch(on_trigger=bad_callback)
        # Should not raise
        await ks.trigger(KillLevel.SYSTEM, "test")
        assert ks.is_triggered
