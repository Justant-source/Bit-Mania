"""ExecutionEngine 거부 알림·strategy_id 전파·타임아웃 정리 단위 테스트.

2026-05-27 사고에서 (1) 거부가 WARNING이라 Telegram 알림이 없었고
(2) 거부 결과에 strategy_id가 없어 전략별 채널로 발행되지 않았다.
실제 engine.py를 import해 두 수정을 검증한다.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
EXECUTION_DIR = ROOT / "services" / "execution"
if str(EXECUTION_DIR) not in sys.path:
    sys.path.insert(0, str(EXECUTION_DIR))

import order_manager as om_module  # noqa: E402
import engine as engine_module  # noqa: E402
from engine import ExecutionEngine  # noqa: E402


PAYLOAD = {
    "request_id": "req-incident",
    "strategy_id": "supertrend-01",
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "side": "sell",
    "order_type": "limit",
    "quantity": 0.007,
    "price": 100_000.0,
    "post_only": True,
    "reduce_only": True,
}


@pytest.fixture
def engine(mock_db):
    redis = AsyncMock()
    redis.publish = AsyncMock(return_value=1)
    with patch.object(om_module, "exchange_factory", return_value=AsyncMock()):
        eng = ExecutionEngine(
            exchange="bybit",
            api_key="",
            api_secret="",
            testnet=True,
            redis=redis,
            db_pool=mock_db,
            position_tracker=AsyncMock(),
        )
    return eng


def _published(redis: AsyncMock) -> dict[str, list[dict]]:
    """채널별 발행 페이로드 수집."""
    out: dict[str, list[dict]] = {}
    for call in redis.publish.call_args_list:
        channel, raw = call.args
        out.setdefault(channel, []).append(json.loads(raw))
    return out


class TestRejectionPropagation:
    @pytest.mark.asyncio
    async def test_safety_rejection_reaches_strategy_channel(self, engine):
        """safety 차단 → 전략별 채널에도 거부 발행 (사고 재현 시나리오)."""
        engine._safety.check_order = AsyncMock(
            return_value=(False, "implied_leverage_exceeded: implied=5.51 > limit=3.0")
        )

        await engine._process_order(dict(PAYLOAD))

        published = _published(engine.redis)
        assert "order:result" in published
        assert "order:result:supertrend-01" in published
        msg = published["order:result:supertrend-01"][0]
        assert msg["status"] == "rejected"
        assert msg["strategy_id"] == "supertrend-01"
        assert msg["symbol"] == "BTC/USDT:USDT"
        assert "implied_leverage_exceeded" in msg["reason"]

    @pytest.mark.asyncio
    async def test_rejection_logged_at_error_level(self, engine, monkeypatch):
        """거부는 ERROR 레벨 → ce:alerts:anomaly 경유 Telegram 알림."""
        log_mock = MagicMock()
        monkeypatch.setattr(engine_module, "log", log_mock)
        engine._safety.check_order = AsyncMock(return_value=(False, "kill_switch_active"))

        await engine._process_order(dict(PAYLOAD))

        assert log_mock.error.called
        assert not log_mock.warning.called  # 더 이상 WARNING으로 묻히지 않는다

    @pytest.mark.asyncio
    async def test_manager_rejection_also_alerts(self, engine, monkeypatch):
        """OrderManager가 반환한 rejected 결과도 ERROR 알림."""
        log_mock = MagicMock()
        monkeypatch.setattr(engine_module, "log", log_mock)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(return_value={
            "request_id": PAYLOAD["request_id"],
            "order_id": "",
            "status": "rejected",
            "filled_qty": 0.0,
            "filled_price": None,
            "fee": 0.0,
            "strategy_id": "supertrend-01",
            "reason": "max_retries_exhausted: network",
        })

        await engine._process_order(dict(PAYLOAD))

        error_events = [str(c) for c in log_mock.error.call_args_list]
        assert any("미체결" in s for s in error_events)


class TestTimeoutCleanup:
    @pytest.mark.asyncio
    async def test_timeout_cancels_and_rejects_without_blind_retry(self, engine, monkeypatch):
        """타임아웃 → 미체결 주문 취소 + 거부 발행. 블라인드 재시도 없음
        (재시도는 idempotency에 막혀 duplicate 거부만 반환하던 무의미한 동작)."""
        monkeypatch.setattr(engine_module, "ORDER_TIMEOUT", 0.05)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))

        call_count = {"n": 0}

        async def slow_place(payload):
            call_count["n"] += 1
            await asyncio.sleep(10)

        engine._order_manager.place_order = slow_place
        engine._order_manager.cancel_order = AsyncMock(
            return_value={"request_id": PAYLOAD["request_id"], "status": "cancelled"}
        )

        await engine._process_order(dict(PAYLOAD))

        assert call_count["n"] == 1  # 단 1회 실행 (재시도 제거)
        engine._order_manager.cancel_order.assert_awaited_once_with(
            PAYLOAD["request_id"], "BTC/USDT:USDT"
        )
        published = _published(engine.redis)
        msg = published["order:result:supertrend-01"][0]
        assert msg["status"] == "rejected"
        assert "order_timeout" in msg["reason"]
