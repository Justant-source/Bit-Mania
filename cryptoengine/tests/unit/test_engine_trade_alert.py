"""ExecutionEngine 매수/매도 체결 알림(ce:alerts:entry / ce:alerts:exit) 단위 테스트.

실제 매수 체결이 있었는데 텔레그램 알림이 안 온 사건의 원인은 이 두 채널에
발행하는 코드가 아예 없었기 때문이다(소비자 — 구독·포맷터·디스패치 — 는
이미 준비돼 있었다). engine.py::_publish_trade_alert가 그 발행부다.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
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
from shared.models.position import Position  # noqa: E402


ENTRY_PAYLOAD = {
    "request_id": "req-entry",
    "strategy_id": "supertrend-01",
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "side": "buy",
    "order_type": "limit",
    "quantity": 0.007,
    "price": 80_000.0,
    "post_only": True,
    "reduce_only": False,
}

EXIT_PAYLOAD = {
    "request_id": "req-exit",
    "strategy_id": "supertrend-01",
    "exchange": "bybit",
    "symbol": "BTC/USDT:USDT",
    "side": "sell",
    "order_type": "limit",
    "quantity": 0.007,
    "price": 81_000.0,
    "post_only": True,
    "reduce_only": True,
}


def _fill_result(payload: dict, filled_price: float, status: str = "filled") -> dict:
    return {
        "request_id": payload["request_id"],
        "order_id": "ord-1",
        "status": status,
        "filled_qty": payload["quantity"],
        "filled_price": filled_price,
        "fee": 0.05,
        "fee_currency": "USDT",
        "strategy_id": payload["strategy_id"],
        "symbol": payload["symbol"],
        "side": payload["side"],
        "order_type": payload["order_type"],
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
    out: dict[str, list[dict]] = {}
    for call in redis.publish.call_args_list:
        channel, raw = call.args
        out.setdefault(channel, []).append(json.loads(raw))
    return out


def _long_position(entry_price: float = 80_000.0) -> Position:
    return Position(
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="long",
        size=0.007,
        entry_price=entry_price,
        leverage=3.0,
    )


class TestEntryAlert:
    @pytest.mark.asyncio
    async def test_entry_fill_publishes_ce_alerts_entry(self, engine):
        engine.position_tracker.get_position = MagicMock(return_value=None)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(ENTRY_PAYLOAD, 80_100.0)
        )

        await engine._process_order(dict(ENTRY_PAYLOAD))

        published = _published(engine.redis)
        assert "ce:alerts:entry" in published
        msg = published["ce:alerts:entry"][0]
        assert msg["alert_type"] == "entry"
        assert msg["side"] == "buy"
        assert msg["strategy_id"] == "supertrend-01"
        assert msg["quantity"] == pytest.approx(0.007)
        assert msg["notional_usd"] == pytest.approx(80_100.0 * 0.007)
        assert "realized_pnl" not in msg

    @pytest.mark.asyncio
    async def test_small_fill_still_alerts(self, engine):
        """소액 필터는 dispatcher 쪽에서 삭제됐다 — 발행부 자체에는 애초에
        필터가 없어야 한다(회귀 잠금)."""
        engine.position_tracker.get_position = MagicMock(return_value=None)
        small_payload = dict(ENTRY_PAYLOAD, quantity=0.0001, price=100.0)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(small_payload, 100.0)
        )

        await engine._process_order(dict(small_payload))

        published = _published(engine.redis)
        assert "ce:alerts:entry" in published
        assert published["ce:alerts:entry"][0]["notional_usd"] == pytest.approx(0.01)

    @pytest.mark.asyncio
    async def test_rejected_order_publishes_no_trade_alert(self, engine):
        engine._safety.check_order = AsyncMock(return_value=(False, "kill_switch_active"))

        await engine._process_order(dict(ENTRY_PAYLOAD))

        published = _published(engine.redis)
        assert "ce:alerts:entry" not in published
        assert "ce:alerts:exit" not in published


class TestExitAlertPnl:
    @pytest.mark.asyncio
    async def test_exit_fill_publishes_ce_alerts_exit_with_pnl(self, engine):
        engine.position_tracker.get_position = MagicMock(return_value=_long_position(80_000.0))
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(EXIT_PAYLOAD, 81_000.0)
        )

        await engine._process_order(dict(EXIT_PAYLOAD))

        published = _published(engine.redis)
        assert "ce:alerts:exit" in published
        msg = published["ce:alerts:exit"][0]
        assert msg["alert_type"] == "exit"
        # gross = (81000-80000)*0.007 = 7.0, minus fee 0.05
        assert msg["realized_pnl"] == pytest.approx(7.0 - 0.05)
        assert msg["realized_pnl_pct"] > 0
        assert msg["position_side"] == "long"

    @pytest.mark.asyncio
    async def test_exit_short_pnl_sign(self, engine):
        short_pos = Position(
            exchange="bybit", symbol="BTC/USDT:USDT", side="short",
            size=0.007, entry_price=81_000.0, leverage=3.0,
        )
        engine.position_tracker.get_position = MagicMock(return_value=short_pos)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        # buy-to-close below entry -> profit for a short
        buy_close = dict(EXIT_PAYLOAD, side="buy")
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(buy_close, 80_000.0)
        )

        await engine._process_order(dict(buy_close))

        msg = _published(engine.redis)["ce:alerts:exit"][0]
        # short: gross = -(80000-81000)*0.007 = +7.0
        assert msg["realized_pnl"] == pytest.approx(7.0 - 0.05)

    @pytest.mark.asyncio
    async def test_exit_without_cached_position_omits_pnl(self, engine):
        engine.position_tracker.get_position = MagicMock(return_value=None)
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(EXIT_PAYLOAD, 81_000.0)
        )

        await engine._process_order(dict(EXIT_PAYLOAD))

        msg = _published(engine.redis)["ce:alerts:exit"][0]
        assert "realized_pnl" not in msg
        assert "entry_price" not in msg

    @pytest.mark.asyncio
    async def test_hold_duration_populated_from_db(self, engine):
        opened = datetime.now(timezone.utc) - timedelta(hours=2, minutes=15)
        engine.position_tracker.get_position = MagicMock(return_value=_long_position())
        engine.db_pool._conn.fetchrow = AsyncMock(return_value={"opened_at": opened})
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(EXIT_PAYLOAD, 81_000.0)
        )

        await engine._process_order(dict(EXIT_PAYLOAD))

        msg = _published(engine.redis)["ce:alerts:exit"][0]
        assert msg["hold_duration"] == "2h 15m"


class TestOrderingInvariant:
    @pytest.mark.asyncio
    async def test_alert_published_before_position_tracker_update(self, engine):
        """설계의 핵심: on_order_fill()이 진입가를 지우기 전에 알림이 나가야 한다."""
        call_order: list[str] = []

        async def record_publish(channel, raw):
            call_order.append(f"publish:{channel}")
            return 1

        engine.redis.publish = AsyncMock(side_effect=record_publish)
        engine.position_tracker.get_position = MagicMock(return_value=_long_position())

        async def fake_on_fill(result):
            call_order.append("on_order_fill")

        engine.position_tracker.on_order_fill = fake_on_fill
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(EXIT_PAYLOAD, 81_000.0)
        )

        await engine._process_order(dict(EXIT_PAYLOAD))

        alert_idx = call_order.index("publish:ce:alerts:exit")
        fill_idx = call_order.index("on_order_fill")
        assert alert_idx < fill_idx, f"order was {call_order}"


class TestAlertFailureIsolation:
    @pytest.mark.asyncio
    async def test_alert_publish_failure_does_not_break_order_flow(self, engine, monkeypatch):
        engine.position_tracker.get_position = MagicMock(return_value=None)
        log_mock = MagicMock()
        monkeypatch.setattr(engine_module, "log", log_mock)

        async def flaky_publish(channel, raw):
            if channel.startswith("ce:alerts:"):
                raise ConnectionError("redis down")
            return 1

        engine.redis.publish = AsyncMock(side_effect=flaky_publish)
        fill_done = {"called": False}

        async def fake_on_fill(result):
            fill_done["called"] = True

        engine.position_tracker.on_order_fill = fake_on_fill
        engine._safety.check_order = AsyncMock(return_value=(True, ""))
        engine._order_manager.place_order = AsyncMock(
            return_value=_fill_result(ENTRY_PAYLOAD, 80_100.0)
        )

        await engine._process_order(dict(ENTRY_PAYLOAD))

        assert fill_done["called"] is True
        assert log_mock.exception.called


class TestPureHelpers:
    def test_compute_realized_pnl_long_profit(self):
        pos = _long_position(80_000.0)
        pnl, pct = ExecutionEngine._compute_realized_pnl(pos, 81_000.0, 0.007, 0.05)
        assert pnl == pytest.approx(7.0 - 0.05)
        assert pct == pytest.approx((7.0 / (80_000.0 * 0.007)) * 100.0)

    def test_format_hold_duration_minutes(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        opened = now - timedelta(minutes=45)
        assert ExecutionEngine._format_hold_duration(opened, now) == "45m"

    def test_format_hold_duration_days(self):
        now = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
        opened = now - timedelta(days=3, hours=4)
        assert ExecutionEngine._format_hold_duration(opened, now) == "3d 4h"

    def test_format_hold_duration_none_when_missing(self):
        now = datetime.now(timezone.utc)
        assert ExecutionEngine._format_hold_duration(None, now) is None

    def test_build_trade_alert_entry_has_no_pnl_fields(self):
        result = _fill_result(ENTRY_PAYLOAD, 80_100.0)
        now = datetime.now(timezone.utc)
        alert = ExecutionEngine._build_trade_alert(ENTRY_PAYLOAD, result, None, None, now)
        assert alert["alert_type"] == "entry"
        assert "realized_pnl" not in alert
        assert "hold_duration" not in alert
