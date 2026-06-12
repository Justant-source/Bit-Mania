"""Supertrend 전략 상태 동기화·pending 추적·봉 워치독 단위 테스트.

실제 services/strategies/supertrend/strategy.py 모듈을 import해 검증한다
(2026-05-27 미체결 사고 재발 방지 수정의 핵심 동작).
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STRATEGIES_DIR = ROOT / "services" / "strategies"
if str(STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(STRATEGIES_DIR))

# Docker 이미지의 "/app/strategy" 패키지 레이아웃 재현 (namespace package)
if "strategy" not in sys.modules:
    _pkg = types.ModuleType("strategy")
    _pkg.__path__ = [str(STRATEGIES_DIR / "supertrend")]
    sys.modules["strategy"] = _pkg

# talib은 도커 이미지 전용 C 확장 — 본 테스트는 지표 계산 경로를 타지 않으므로 스텁
sys.modules.setdefault("talib", MagicMock())

strategy_module = importlib.import_module("strategy.strategy")
SupertrendLiveStrategy = strategy_module.SupertrendLiveStrategy

from shared.models.position import Position  # noqa: E402


def make_position(size: float = 0.007, side: str = "long", entry: float = 100_000.0) -> Position:
    return Position(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side=side,
        size=size,
        entry_price=entry,
        leverage=3.0,
    )


@pytest.fixture
def strategy(monkeypatch):
    """연결 없이 동작하는 전략 인스턴스 (redis/exchange 모킹)."""
    s = SupertrendLiveStrategy("supertrend-01", {"tick_interval": 0.01})
    s.is_running = True
    s.allocated_capital = 200.0

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.publish = AsyncMock(return_value=1)
    s._redis = redis_mock

    s._exchange = AsyncMock()
    s._exchange.get_position = AsyncMock(return_value=None)
    s._exchange.get_ohlcv = AsyncMock(return_value=[])

    # 테스트가 빨리 끝나도록 폴링/재시도 간격 축소
    monkeypatch.setattr(strategy_module, "PENDING_POLL_S", 0.01)
    monkeypatch.setattr(strategy_module, "EXIT_RETRY_DELAY_S", 0.01)
    return s


async def _drain_tasks(s) -> None:
    """전략이 만든 백그라운드 태스크 정리."""
    tasks = list(s._bg_tasks)
    if s._pending_watch_task:
        tasks.append(s._pending_watch_task)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# ------------------------------------------------------------------
# 거래소 진실 동기화
# ------------------------------------------------------------------

class TestPositionSync:
    @pytest.mark.asyncio
    async def test_divergence_corrected_to_exchange_truth(self, strategy):
        """전략은 flat이라 믿지만 거래소에 포지션 존재 → 강제 교정 (사고 패턴)."""
        strategy._has_position = False
        strategy._position_qty = 0.0
        strategy._exchange.get_position = AsyncMock(return_value=make_position(0.007))

        ok = await strategy._sync_position_from_exchange("test")

        assert ok is True
        assert strategy._has_position is True
        assert strategy._position_qty == pytest.approx(0.007)
        assert strategy._entry_price == pytest.approx(100_000.0)

    @pytest.mark.asyncio
    async def test_phantom_position_cleared(self, strategy):
        """전략은 보유라 믿지만 거래소엔 없음 → flat으로 교정."""
        strategy._has_position = True
        strategy._position_qty = 0.007
        strategy._entry_price = 100_000.0
        strategy._exchange.get_position = AsyncMock(return_value=None)

        await strategy._sync_position_from_exchange("test")

        assert strategy._has_position is False
        assert strategy._position_qty == 0.0
        assert strategy._entry_price == 0.0

    @pytest.mark.asyncio
    async def test_api_failure_keeps_belief(self, strategy):
        """조회 실패 시 기존 믿음 유지 (안전 검사는 엔진에서 별도 수행)."""
        strategy._has_position = True
        strategy._position_qty = 0.007
        strategy._exchange.get_position = AsyncMock(side_effect=ConnectionError("down"))

        ok = await strategy._sync_position_from_exchange("test")

        assert ok is False
        assert strategy._has_position is True
        assert strategy._position_qty == pytest.approx(0.007)

    @pytest.mark.asyncio
    async def test_unexpected_short_treated_as_flat(self, strategy):
        """Long-only 전략에 숏 포지션 → 자동 관리 대상 아님 (flat 처리)."""
        strategy._exchange.get_position = AsyncMock(
            return_value=make_position(0.01, side="short")
        )

        await strategy._sync_position_from_exchange("test")

        assert strategy._has_position is False


# ------------------------------------------------------------------
# 진입 사이징 — min(할당자본, 실시간 equity)
# ------------------------------------------------------------------

class TestEffectiveCapital:
    @pytest.mark.asyncio
    async def test_equity_below_allocation_reduces_capital(self, strategy):
        strategy._redis.get = AsyncMock(return_value=json.dumps({"total": 150.0}))
        assert await strategy._get_effective_capital() == pytest.approx(150.0)

    @pytest.mark.asyncio
    async def test_equity_above_allocation_uses_allocation(self, strategy):
        strategy._redis.get = AsyncMock(return_value=json.dumps({"total": 250.0}))
        assert await strategy._get_effective_capital() == pytest.approx(200.0)

    @pytest.mark.asyncio
    async def test_missing_cache_falls_back_to_allocation(self, strategy):
        strategy._redis.get = AsyncMock(return_value=None)
        assert await strategy._get_effective_capital() == pytest.approx(200.0)

    @pytest.mark.asyncio
    async def test_redis_error_falls_back_to_allocation(self, strategy):
        strategy._redis.get = AsyncMock(side_effect=ConnectionError("down"))
        assert await strategy._get_effective_capital() == pytest.approx(200.0)


# ------------------------------------------------------------------
# 주문 결과 처리 — 확정은 결과 수신 시에만
# ------------------------------------------------------------------

class TestOrderResultHandling:
    @pytest.mark.asyncio
    async def test_entry_not_confirmed_until_result(self, strategy):
        """제출 직후엔 낙관적 갱신 없음 — filled 결과 수신 후에만 확정."""
        await strategy._enter_long(100_000.0, 200.0)

        assert strategy._has_position is False  # 아직 미확정
        assert strategy._pending_order is not None
        assert strategy._pending_order["action"] == "enter"
        rid = strategy._pending_order["request_id"]

        await strategy._handle_order_result({
            "request_id": rid,
            "status": "filled",
            "filled_qty": 0.0057,
            "filled_price": 99_950.0,
        })

        assert strategy._has_position is True
        assert strategy._position_qty == pytest.approx(0.0057)
        assert strategy._entry_price == pytest.approx(99_950.0)  # 실체결가 채택
        assert strategy._pending_order is None
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_exit_cooldowns_set_only_on_confirm(self, strategy):
        """쿨다운 타임스탬프는 청산 '확정' 시에만 — 거부된 청산은 쿨다운 없음."""
        strategy._has_position = True
        strategy._position_qty = 0.0057
        strategy._last_bar_ts = 1_000 * strategy_module._4H_MS

        await strategy._exit_long("atr_distance", atr_triggered=True, price=101_000.0)

        assert strategy._has_position is True  # 아직 보유로 간주
        assert strategy._last_liquidation_ts == 0  # 쿨다운 미설정
        rid = strategy._pending_order["request_id"]

        await strategy._handle_order_result({"request_id": rid, "status": "filled"})

        assert strategy._has_position is False
        assert strategy._last_liquidation_ts == 1_000 * strategy_module._4H_MS
        assert strategy._atr_cooldown_until == 1_001 * strategy_module._4H_MS
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_rejected_exit_resyncs_and_retries_once(self, strategy):
        """exit 거부 → 재동기화 + 60초 후 1회 재시도 (2026-05-27 사고 자동 회복)."""
        strategy._has_position = True
        strategy._position_qty = 0.0057
        strategy._candles.append(
            {"open": 1, "high": 1, "low": 1, "close": 100_500.0, "volume": 1, "ts": 0}
        )
        # 거래소엔 여전히 포지션 존재 (청산 차단 상황)
        strategy._exchange.get_position = AsyncMock(return_value=make_position(0.0057))

        await strategy._exit_long("ema_cross", price=100_500.0)
        rid = strategy._pending_order["request_id"]

        await strategy._handle_order_result({
            "request_id": rid,
            "status": "rejected",
            "reason": "implied_leverage_exceeded: implied=5.51 > limit=3.0",
        })

        # 재동기화로 포지션 보유 상태 유지 + pending 해제
        assert strategy._has_position is True
        assert strategy._pending_order is None

        # 재시도 태스크가 EXIT_RETRY_DELAY_S(패치됨) 후 재제출
        await asyncio.sleep(0.1)
        assert strategy._pending_order is not None
        assert strategy._pending_order["action"] == "exit"
        assert strategy._pending_order["retried"] is True
        # 재시도 주문이 order:request로 발행됨
        publish_channels = [c.args[0] for c in strategy._redis.publish.call_args_list]
        assert publish_channels.count("order:request") == 2
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_retried_exit_rejection_stops_retrying(self, strategy):
        """재시도분도 거부되면 추가 재시도 없음 (알림 후 수동 대응)."""
        strategy._has_position = True
        strategy._position_qty = 0.0057
        strategy._exchange.get_position = AsyncMock(return_value=make_position(0.0057))

        await strategy._exit_long("ema_cross", price=100_500.0, is_retry=True)
        rid = strategy._pending_order["request_id"]

        await strategy._handle_order_result({"request_id": rid, "status": "rejected", "reason": "kill_switch_active"})

        await asyncio.sleep(0.1)
        assert strategy._pending_order is None  # 재시도 없음
        publish_channels = [c.args[0] for c in strategy._redis.publish.call_args_list]
        assert publish_channels.count("order:request") == 1
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_rejected_entry_never_retries(self, strategy):
        """entry 거부는 재시도하지 않는다 — 스킵이 안전한 방향."""
        strategy._exchange.get_position = AsyncMock(return_value=None)

        await strategy._enter_long(100_000.0, 200.0)
        rid = strategy._pending_order["request_id"]

        await strategy._handle_order_result({"request_id": rid, "status": "rejected", "reason": "x"})

        await asyncio.sleep(0.1)
        assert strategy._pending_order is None
        assert strategy._has_position is False
        publish_channels = [c.args[0] for c in strategy._redis.publish.call_args_list]
        assert publish_channels.count("order:request") == 1
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_unrelated_result_ignored(self, strategy):
        """pending과 무관한 request_id 결과는 무시."""
        await strategy._enter_long(100_000.0, 200.0)
        before = dict(strategy._pending_order)

        await strategy._handle_order_result({"request_id": "someone-else", "status": "filled"})

        assert strategy._pending_order == before
        assert strategy._has_position is False
        await _drain_tasks(strategy)


# ------------------------------------------------------------------
# pending 백스톱 폴링 — 결과 메시지 유실 대비
# ------------------------------------------------------------------

class TestPendingBackstop:
    @pytest.mark.asyncio
    async def test_lost_result_confirmed_by_position_poll(self, strategy):
        """order:result 유실돼도 포지션 폴링이 진입을 확정한다."""
        strategy._exchange.get_position = AsyncMock(return_value=make_position(0.0057, entry=99_900.0))

        await strategy._enter_long(100_000.0, 200.0)
        assert strategy._pending_order is not None

        await asyncio.sleep(0.2)  # PENDING_POLL_S=0.01 — 폴링이 확정할 시간

        assert strategy._pending_order is None
        assert strategy._has_position is True
        assert strategy._entry_price == pytest.approx(99_900.0)
        await _drain_tasks(strategy)

    @pytest.mark.asyncio
    async def test_tick_blocked_while_pending(self, strategy):
        """pending 중에는 새 봉 신호를 처리하지 않는다 (래치 유지)."""
        strategy._pending_order = {"request_id": "x", "action": "enter", "deadline": time.monotonic() + 100}
        strategy._new_bar = True

        await strategy.tick()

        assert strategy._new_bar is True  # 소비되지 않고 유지


# ------------------------------------------------------------------
# 봉 수신 — 중복 제거·갭 백필·워치독
# ------------------------------------------------------------------

def _candle_msg(ts: int, close: float = 100.0) -> dict:
    return {"open": close, "high": close, "low": close, "close": close, "volume": 1.0, "ts": ts, "confirmed": True}


class TestCandleIngestion:
    @pytest.mark.asyncio
    async def test_duplicate_ts_replaces_not_appends(self, strategy):
        await strategy._ingest_candle(_candle_msg(strategy_module._4H_MS, 100.0))
        await strategy._ingest_candle(_candle_msg(strategy_module._4H_MS, 101.0))

        assert len(strategy._candles) == 1
        assert strategy._candles[-1]["close"] == pytest.approx(101.0)

    @pytest.mark.asyncio
    async def test_stale_candle_ignored(self, strategy):
        await strategy._ingest_candle(_candle_msg(2 * strategy_module._4H_MS))
        await strategy._ingest_candle(_candle_msg(strategy_module._4H_MS))

        assert len(strategy._candles) == 1
        assert strategy._candles[-1]["ts"] == 2 * strategy_module._4H_MS

    @pytest.mark.asyncio
    async def test_gap_triggers_backfill(self, strategy):
        strategy._backfill_gap = AsyncMock()
        await strategy._ingest_candle(_candle_msg(strategy_module._4H_MS))
        await strategy._ingest_candle(_candle_msg(4 * strategy_module._4H_MS))  # 2봉 누락

        strategy._backfill_gap.assert_awaited_once_with(
            strategy_module._4H_MS, 4 * strategy_module._4H_MS
        )

    @pytest.mark.asyncio
    async def test_watchdog_recovers_missed_bar(self, strategy):
        """마감 봉 미수신 → 워치독이 REST 백필 후 래치 set."""
        now_ms = int(time.time() * 1000)
        # 마지막 봉이 3개 봉 전 (워치독 발동 조건 충족)
        last_ts = ((now_ms // strategy_module._4H_MS) - 3) * strategy_module._4H_MS
        missed_ts = last_ts + strategy_module._4H_MS
        strategy._candles.append({"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "ts": last_ts})

        missed = MagicMock()
        missed.open, missed.high, missed.low, missed.close, missed.volume = 1.0, 1.0, 1.0, 1.0, 1.0
        missed.timestamp = MagicMock()
        missed.timestamp.timestamp = MagicMock(return_value=missed_ts / 1000)
        strategy._exchange.get_ohlcv = AsyncMock(return_value=[missed])

        await strategy._bar_watchdog()

        assert strategy._new_bar is True
        assert strategy._candles[-1]["ts"] == missed_ts

    @pytest.mark.asyncio
    async def test_watchdog_quiet_when_feed_healthy(self, strategy):
        """최신 봉이 있으면 워치독은 조용히 통과."""
        now_ms = int(time.time() * 1000)
        last_ts = ((now_ms // strategy_module._4H_MS) - 1) * strategy_module._4H_MS
        strategy._candles.append({"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "ts": last_ts})

        await strategy._bar_watchdog()

        assert strategy._new_bar is False
        strategy._exchange.get_ohlcv.assert_not_awaited()
