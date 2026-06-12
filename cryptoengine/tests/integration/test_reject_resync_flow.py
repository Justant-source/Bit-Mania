"""2026-05-27 미체결 사고 시나리오 통합 재현 테스트.

당시 흐름: 청산 신호 → safety 차단 → (구버전) 전략은 거부를 모른 채
flat로 발산 → 실포지션 방치 → 수동 개입.

수정 후 기대 흐름 (실제 engine.py + strategy.py 코드로 검증):
  1. safety 차단 → 거부가 전략별 채널 페이로드로 발행됨 (strategy_id 포함)
  2. 전략이 거부를 수신 → 거래소 진실로 재동기화 (포지션 유지 인지)
  3. 60초(테스트에선 단축) 후 청산 자동 재시도 1회
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
EXECUTION_DIR = ROOT / "services" / "execution"
if str(EXECUTION_DIR) not in sys.path:
    sys.path.insert(0, str(EXECUTION_DIR))
STRATEGIES_DIR = ROOT / "services" / "strategies"
if str(STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(STRATEGIES_DIR))

if "strategy" not in sys.modules:
    _pkg = types.ModuleType("strategy")
    _pkg.__path__ = [str(STRATEGIES_DIR / "supertrend")]
    sys.modules["strategy"] = _pkg
sys.modules.setdefault("talib", MagicMock())

import order_manager as om_module  # noqa: E402
from engine import ExecutionEngine  # noqa: E402

strategy_module = importlib.import_module("strategy.strategy")
SupertrendLiveStrategy = strategy_module.SupertrendLiveStrategy

from shared.models.position import Position  # noqa: E402


@pytest.mark.asyncio
async def test_incident_replay_reject_resync_retry(mock_db, monkeypatch):
    monkeypatch.setattr(strategy_module, "PENDING_POLL_S", 0.01)
    monkeypatch.setattr(strategy_module, "EXIT_RETRY_DELAY_S", 0.01)

    # ── 전략: 포지션 보유 중, 청산 신호 발생 직전 상태 ──────────────────
    strat = SupertrendLiveStrategy("supertrend-01", {"tick_interval": 0.01})
    strat.is_running = True
    strat.allocated_capital = 200.0
    strat._has_position = True
    strat._position_qty = 0.007
    strat._entry_price = 100_000.0
    strat._candles.append(
        {"open": 1, "high": 1, "low": 1, "close": 99_000.0, "volume": 1, "ts": 0}
    )

    strat_redis = AsyncMock()
    strat_redis.publish = AsyncMock(return_value=1)
    strat_redis.get = AsyncMock(return_value=None)
    strat._redis = strat_redis

    # 거래소: 포지션이 계속 존재 (청산이 차단된 상황)
    live_position = Position(
        exchange="bybit", symbol="BTC/USDT:USDT", side="long",
        size=0.007, entry_price=100_000.0, leverage=3.0,
    )
    strat._exchange = AsyncMock()
    strat._exchange.get_position = AsyncMock(return_value=live_position)

    # ── 1) 전략이 청산 주문 제출 (구버전과 달리 낙관적 flat 전환 없음) ──
    await strat._exit_long("ema_cross", price=99_000.0)
    assert strat._has_position is True, "확정 전 낙관적 상태 변경이 없어야 한다"
    request_payload = json.loads(strat_redis.publish.call_args.args[1])
    assert strat_redis.publish.call_args.args[0] == "order:request"

    # ── 2) 엔진이 safety 차단 → 거부를 전략별 채널 페이로드로 발행 ──────
    engine_redis = AsyncMock()
    engine_redis.publish = AsyncMock(return_value=1)
    with patch.object(om_module, "exchange_factory", return_value=AsyncMock()):
        engine = ExecutionEngine(
            exchange="bybit", api_key="", api_secret="", testnet=True,
            redis=engine_redis, db_pool=mock_db, position_tracker=AsyncMock(),
        )
    engine._safety.check_order = AsyncMock(
        return_value=(False, "implied_leverage_exceeded: implied=5.51 > limit=3.0")
    )

    await engine._process_order(request_payload)

    strategy_channel_msgs = [
        json.loads(c.args[1])
        for c in engine_redis.publish.call_args_list
        if c.args[0] == "order:result:supertrend-01"
    ]
    assert strategy_channel_msgs, "거부가 전략별 채널로 발행되어야 한다"
    rejection = strategy_channel_msgs[0]
    assert rejection["request_id"] == request_payload["request_id"]

    # ── 3) 전략이 거부 수신 → 재동기화(포지션 유지 인지) + 자동 재시도 ──
    await strat._handle_order_result(rejection)

    assert strat._has_position is True, "재동기화로 실포지션 보유를 인지해야 한다"
    assert strat._last_liquidation_ts == 0, "거부된 청산이 쿨다운을 남기면 안 된다"

    await asyncio.sleep(0.1)  # 재시도 지연(단축됨) 경과

    retry_requests = [
        json.loads(c.args[1])
        for c in strat_redis.publish.call_args_list
        if c.args[0] == "order:request"
    ]
    assert len(retry_requests) == 2, "청산이 1회 자동 재시도되어야 한다"
    assert retry_requests[1]["reduce_only"] is True
    assert retry_requests[1]["side"] == "sell"
    assert retry_requests[1]["request_id"] != retry_requests[0]["request_id"]
    assert strat._pending_order is not None and strat._pending_order["retried"] is True

    # 정리
    tasks = list(strat._bg_tasks)
    if strat._pending_watch_task:
        tasks.append(strat._pending_watch_task)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_filled_result_confirms_strategy_state(mock_db, monkeypatch):
    """정상 체결 흐름: 엔진 결과 발행 → 전략 확정 (실체결가 채택)."""
    monkeypatch.setattr(strategy_module, "PENDING_POLL_S", 0.01)

    strat = SupertrendLiveStrategy("supertrend-01", {"tick_interval": 0.01})
    strat.is_running = True
    strat.allocated_capital = 200.0
    strat._redis = AsyncMock()
    strat._redis.publish = AsyncMock(return_value=1)
    strat._redis.get = AsyncMock(return_value=None)
    strat._exchange = AsyncMock()
    strat._exchange.get_position = AsyncMock(return_value=None)

    await strat._enter_long(100_000.0, 200.0)
    rid = strat._pending_order["request_id"]

    # 엔진의 _publish_result와 동일한 형태의 filled 결과
    await strat._handle_order_result({
        "request_id": rid,
        "order_id": "ord-1",
        "status": "filled",
        "filled_qty": 0.0057,
        "filled_price": 99_980.0,
        "strategy_id": "supertrend-01",
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
    })

    assert strat._has_position is True
    assert strat._position_qty == pytest.approx(0.0057)
    assert strat._entry_price == pytest.approx(99_980.0)
    assert strat._pending_order is None

    if strat._pending_watch_task:
        strat._pending_watch_task.cancel()
        await asyncio.gather(strat._pending_watch_task, return_exceptions=True)
