"""Orchestrator Kill Switch → ce:alerts:kill_switch 리치 알림 발행 단위 테스트.

2026-09 텔레그램 알림 개편(Wave B): 지금까지 Kill Switch 발동은 log.critical()을
통해서만 간접적으로 ce:alerts:anomaly에 도달해 밋밋한 일반 포맷터로 표시됐다.
_on_kill_switch_trigger가 ce:alerts:kill_switch에 직접 발행해 전용 포맷터
(_format_kill_switch_alert — Level/Action/Trigger/DD)를 쓰도록 하는 변경을 검증한다.

shared/kill_switch.py는 이 테스트에서도 수정하지 않는다 — KillLevel enum만
import해서 쓴다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.orchestrator.core import StrategyOrchestrator  # noqa: E402
from shared.kill_switch import KillLevel  # noqa: E402


def _published_to(redis_mock: AsyncMock, channel: str) -> list[dict]:
    """redis.publish(channel, payload) 호출들 중 channel이 일치하는 payload(dict) 목록."""
    out = []
    for call in redis_mock.publish.await_args_list:
        args = call.args
        if len(args) >= 2 and args[0] == channel:
            try:
                out.append(json.loads(args[1]))
            except (json.JSONDecodeError, TypeError):
                pass
    return out


@pytest.fixture
def orchestrator():
    orch = StrategyOrchestrator({})
    orch._redis = AsyncMock()
    orch._redis.publish = AsyncMock(return_value=1)
    orch._redis.set = AsyncMock(return_value=True)
    return orch


class TestKillSwitchAlertPublish:
    @pytest.mark.asyncio
    async def test_publishes_to_ce_alerts_kill_switch(self, orchestrator):
        orchestrator._last_daily_dd_pct = 5.0
        orchestrator._last_weekly_dd_pct = 8.0

        await orchestrator._on_kill_switch_trigger(KillLevel.PORTFOLIO, "max_daily_drawdown")

        payloads = _published_to(orchestrator._redis, "ce:alerts:kill_switch")
        assert len(payloads) == 1
        p = payloads[0]
        assert p["level"] == 2
        assert p["level_name"] == "PORTFOLIO"
        assert p["trigger_reason"] == "max_daily_drawdown"

    @pytest.mark.asyncio
    async def test_drawdown_sent_as_percent_not_fraction(self):
        """가장 중요한 회귀 테스트: portfolio_monitor는 0.05(=5%)를 반환하는데
        _format_kill_switch_alert는 그대로 {:.2f}%로 찍으므로, 여기서 *100 변환이
        빠지면 "Daily DD: 0.05%"처럼 100배 축소 표시된다."""
        orch = StrategyOrchestrator({})
        orch._redis = AsyncMock()

        class _FakePortfolioMonitor:
            async def evaluate(self):
                return SimpleNamespace(
                    total_equity=1000.0,
                    unrealized_pnl=0.0,
                    realized_pnl_today=0.0,
                    daily_drawdown=0.05,
                    weekly_drawdown=0.08,
                    monthly_drawdown=0.10,
                    strategies=[],
                    kill_switch_triggered=False,
                )

        orch._portfolio_monitor = _FakePortfolioMonitor()

        class _FakeKillSwitch:
            triggered_at = None

            async def check(self, *args, **kwargs):
                return KillLevel.PORTFOLIO

            @property
            def is_triggered(self):
                return True

            reason = "max_daily_drawdown"

        orch._kill_switch = _FakeKillSwitch()

        await orch._orchestration_cycle()

        assert orch._last_daily_dd_pct == pytest.approx(5.0)
        assert orch._last_weekly_dd_pct == pytest.approx(8.0)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "level,expected_action",
        [
            (KillLevel.STRATEGY, "전략 일시정지"),
            (KillLevel.PORTFOLIO, "포트폴리오 축소"),
            (KillLevel.SYSTEM, "시스템 차단"),
            (KillLevel.MANUAL, "수동 긴급 청산"),
        ],
    )
    async def test_kill_level_action_mapping(self, orchestrator, level, expected_action):
        await orchestrator._on_kill_switch_trigger(level, "test_reason")
        payloads = _published_to(orchestrator._redis, "ce:alerts:kill_switch")
        assert payloads[0]["action"] == expected_action
        assert payloads[0]["level_name"] == level.name

    @pytest.mark.asyncio
    async def test_fingerprint_distinguishes_level_and_reason(self, orchestrator):
        await orchestrator._on_kill_switch_trigger(KillLevel.PORTFOLIO, "daily_drawdown")
        fp1 = _published_to(orchestrator._redis, "ce:alerts:kill_switch")[0]["fingerprint"]

        orchestrator._redis.publish.reset_mock()
        await orchestrator._on_kill_switch_trigger(KillLevel.SYSTEM, "healthcheck_fail")
        fp2 = _published_to(orchestrator._redis, "ce:alerts:kill_switch")[0]["fingerprint"]

        assert fp1 != fp2
        assert "PORTFOLIO" in fp1 and "daily_drawdown" in fp1
        assert "SYSTEM" in fp2 and "healthcheck_fail" in fp2

    @pytest.mark.asyncio
    async def test_publish_failure_does_not_break_callback(self, orchestrator):
        """ce:alerts:kill_switch 발행이 실패해도 나머지 콜백(전략 stop, system:kill_switch,
        ACK 등)은 계속 실행돼야 한다."""

        async def _publish_side_effect(channel, payload):
            if channel == "ce:alerts:kill_switch":
                raise ConnectionError("redis down")
            return 1

        orchestrator._redis.publish = AsyncMock(side_effect=_publish_side_effect)

        # 예외가 밖으로 전파되지 않아야 한다
        await orchestrator._on_kill_switch_trigger(KillLevel.PORTFOLIO, "max_daily_drawdown")

        # 나머지 발행(전략 stop 채널, system:kill_switch)은 여전히 시도됐는지 확인
        channels_called = {c.args[0] for c in orchestrator._redis.publish.await_args_list}
        assert "system:kill_switch" in channels_called
        assert "strategy:command:supertrend-01" in channels_called
