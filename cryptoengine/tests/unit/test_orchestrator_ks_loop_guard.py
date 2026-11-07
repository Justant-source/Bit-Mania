"""Kill Switch 재발동 루프 방지 — 쿨다운 해제 시 고점 재설정, flat이면 알림만.

2026-10-30~11-02: 확정 손실(-11.5%)이 기준 고점에 남아 쿨다운 해제 직후 매시간 재발동(216회).
shared/kill_switch.py는 수정하지 않는다 (입력 낙폭과 stop 전송 여부만 오케스트레이터에서 제어).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.orchestrator.core import StrategyOrchestrator  # noqa: E402
from services.orchestrator.portfolio_monitor import PortfolioMonitor  # noqa: E402
from shared.kill_switch import KillLevel  # noqa: E402

PHASE5 = {
    "kill_switch": {
        "cooldown_minutes": 60,
        "phase5": {
            "max_daily_drawdown_pct": 5.0,
            "max_weekly_drawdown_pct": 10.0,
            "max_monthly_drawdown_pct": 15.0,
            "max_daily_loss_abs_usd": 10,
            "max_weekly_loss_abs_usd": 20,
            "max_monthly_loss_abs_usd": 30,
            "trigger_mode": "and",
        },
    }
}


def _redis(positions="[]"):
    r = AsyncMock()
    r.publish = AsyncMock(return_value=1)
    r.set = AsyncMock(return_value=True)
    store = {"ce:positions:all": positions}
    r.get = AsyncMock(side_effect=lambda k: store.get(k))
    r.lrange = AsyncMock(return_value=[])
    return r


def _stops(r):
    return [c for c in r.publish.await_args_list if c.args[0].startswith("strategy:command:")]


@pytest.fixture(autouse=True)
def _phase5(monkeypatch):
    monkeypatch.setenv("PHASE5_MODE", "true")


class TestFlatAlertOnly:
    @pytest.mark.asyncio
    async def test_flat_portfolio_trigger_sends_alert_without_stop(self):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis("[]")
        await orch._on_kill_switch_trigger(KillLevel.PORTFOLIO, "weekly dd")
        assert _stops(orch._redis) == []
        alerts = [c for c in orch._redis.publish.await_args_list if c.args[0] == "ce:alerts:kill_switch"]
        assert len(alerts) == 1
        assert "알림만" in json.loads(alerts[0].args[1])["action"]
        assert orch._ks_alert_only is True

    @pytest.mark.asyncio
    async def test_open_position_still_stops(self):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis('[{"symbol": "BTC/USDT:USDT"}]')
        await orch._on_kill_switch_trigger(KillLevel.PORTFOLIO, "weekly dd")
        assert len(_stops(orch._redis)) == 1
        assert orch._ks_alert_only is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raw", [None, "not-json", '{"a": 1}'])
    async def test_unknown_position_state_is_not_flat(self, raw):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis(raw)
        await orch._on_kill_switch_trigger(KillLevel.PORTFOLIO, "weekly dd")
        assert len(_stops(orch._redis)) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("level", [KillLevel.SYSTEM, KillLevel.MANUAL])
    async def test_non_drawdown_levels_always_stop(self, level):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis("[]")
        await orch._on_kill_switch_trigger(level, "x")
        assert len(_stops(orch._redis)) == 1


class TestCooldownRebase:
    @pytest.mark.asyncio
    async def test_no_retrigger_after_cooldown_for_same_loss(self):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis("[]")
        mon = PortfolioMonitor(orch._redis)
        orch._portfolio_monitor = mon
        mon._daily_peak = mon._weekly_peak = mon._monthly_peak = 198.9
        orch._redis.get = AsyncMock(side_effect=lambda k: {"ce:positions:all": "[]", "cache:wallet_balance": json.dumps({"total": 175.93})}.get(k))

        await orch._orchestration_cycle()  # 최초 발동
        t0 = orch._kill_switch.triggered_at
        assert orch._kill_switch.is_triggered

        orch._kill_switch._triggered_at = t0 - timedelta(minutes=61)  # 쿨다운 경과
        await orch._orchestration_cycle()

        assert not orch._kill_switch.is_triggered  # 같은 낙폭으로 재발동하지 않음
        assert mon._weekly_peak == pytest.approx(175.93)

    @pytest.mark.asyncio
    async def test_new_loss_after_rebase_still_triggers(self):
        orch = StrategyOrchestrator(PHASE5)
        orch._redis = _redis("[]")
        mon = PortfolioMonitor(orch._redis)
        orch._portfolio_monitor = mon
        mon._daily_peak = mon._weekly_peak = mon._monthly_peak = 198.9
        bal = {"total": 175.93}
        orch._redis.get = AsyncMock(side_effect=lambda k: {"ce:positions:all": "[]", "cache:wallet_balance": json.dumps(bal)}.get(k))
        await orch._orchestration_cycle()
        orch._kill_switch._triggered_at -= timedelta(minutes=61)
        await orch._orchestration_cycle()  # 재설정·해제
        assert not orch._kill_switch.is_triggered

        bal["total"] = 150.0  # 재설정 이후 새 손실 -14.7%, 25.9 USD
        await orch._orchestration_cycle()
        assert orch._kill_switch.is_triggered

    @pytest.mark.asyncio
    async def test_rebase_floor_survives_restart(self):
        r = _redis("[]")
        now = datetime.now(timezone.utc)
        hist = [
            json.dumps({"timestamp": (now - timedelta(hours=5)).isoformat(), "equity": 198.9}),
            json.dumps({"timestamp": (now - timedelta(hours=1)).isoformat(), "equity": 175.93}),
        ]
        r.lrange = AsyncMock(return_value=hist)
        floor = (now - timedelta(hours=2)).isoformat()
        r.get = AsyncMock(side_effect=lambda k: floor if k == "orchestrator:peak_rebase_at" else None)
        mon = PortfolioMonitor(r)
        await mon._load_equity_history()
        assert mon._weekly_peak == pytest.approx(175.93)
