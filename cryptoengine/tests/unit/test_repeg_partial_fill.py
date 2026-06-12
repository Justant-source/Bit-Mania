"""재페그 루프 부분체결 누적 추적 단위 테스트 (실제 OrderManager 검증).

핵심: 재페그로 취소된 주문의 부분체결량을 누적하고 잔량만 재발주한다.
이전 구현은 전량을 재발주해 과체결(의도 초과 레버리지) 위험이 있었다.
"""

from __future__ import annotations

import asyncio
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
from order_manager import OrderManager, OrderState  # noqa: E402
from shared.models.order import OrderRequest, OrderResult  # noqa: E402


def make_order(qty: float = 0.01) -> OrderRequest:
    return OrderRequest(
        strategy_id="supertrend-01",
        exchange="bybit",
        symbol="BTC/USDT:USDT",
        side="buy",
        order_type="limit",
        quantity=qty,
        price=100_000.0,
        post_only=True,
    )


def make_orderbook():
    ob = MagicMock()
    bid = MagicMock()
    bid.price = 100_000.0
    ask = MagicMock()
    ask.price = 100_010.0
    ob.bids = [bid]
    ob.asks = [ask]
    return ob


@pytest.fixture
def manager(mock_db):
    """exchange_factory를 패치해 실제 OrderManager를 커넥터 모킹으로 생성."""
    connector = AsyncMock()
    connector.get_orderbook = AsyncMock(return_value=make_orderbook())
    connector.price_to_precision = MagicMock(side_effect=lambda s, p: p)
    connector.get_min_order_sizes = AsyncMock(
        return_value={"BTC/USDT:USDT": {"min_qty": 0.001, "qty_step": 0.001}}
    )
    connector.cancel_order = AsyncMock(return_value=True)

    with patch.object(om_module, "exchange_factory", return_value=connector):
        mgr = OrderManager(
            exchange="bybit",
            api_key="",
            api_secret="",
            testnet=True,
            redis=AsyncMock(),
            db_pool=mock_db,
        )
    return mgr, connector


def _result(rid: str, oid: str, status: str, qty: float = 0.0, price: float | None = None) -> OrderResult:
    return OrderResult(
        request_id=rid, order_id=oid, status=status,
        filled_qty=qty, filled_price=price,
    )


class TestFloorToStep:
    def test_floors_to_exchange_step(self):
        assert OrderManager._floor_to_step(0.0057, 0.001) == pytest.approx(0.005)
        assert OrderManager._floor_to_step(0.006, 0.001) == pytest.approx(0.006)
        assert OrderManager._floor_to_step(-0.001, 0.001) == 0.0
        assert OrderManager._floor_to_step(0.0009, 0.001) == 0.0


class TestRepegPartialFill:
    @pytest.mark.asyncio
    async def test_remaining_qty_after_partial_fill(self, manager, monkeypatch):
        """1차 시도 부분체결(0.004) → 2차 재발주는 잔량 0.006만."""
        mgr, connector = manager
        monkeypatch.setattr(om_module, "REPEG_INTERVAL_S", 0.02)
        monkeypatch.setattr(om_module, "REPEG_POLL_S", 0.01)
        order = make_order(0.01)
        rid = order.request_id

        placed: list[OrderRequest] = []

        async def place(req):
            placed.append(req)
            return _result(rid, f"ord-{len(placed)}", "new")

        connector.place_order = AsyncMock(side_effect=place)

        polls = {"n": 0}

        async def fetch(oid, symbol, request_id):
            # 1차 주문(ord-1): 폴링 중엔 부분체결, 취소 후 수확 시 cancelled+0.004
            if oid == "ord-1":
                return _result(rid, oid, "cancelled", 0.004, 100_000.0)
            # 2차 주문(ord-2): 즉시 전량 체결
            polls["n"] += 1
            return _result(rid, oid, "filled", 0.006, 100_005.0)

        connector.fetch_order_result = AsyncMock(side_effect=fetch)

        result = await mgr.place_limit_order(order)

        assert result["status"] == "filled"
        assert result["filled_qty"] == pytest.approx(0.01)
        # 가중평균: (0.004×100000 + 0.006×100005) / 0.01
        assert result["filled_price"] == pytest.approx(100_003.0)
        # 2차 재발주 수량 = 잔량만
        assert len(placed) == 2
        assert placed[1].quantity == pytest.approx(0.006)
        assert mgr._order_states[rid] == OrderState.FILLED

    @pytest.mark.asyncio
    async def test_market_fallback_uses_remaining_only(self, manager, monkeypatch):
        """재페그 소진 후 시장가 폴백은 잔량만 발주하고 누적과 합산 보고."""
        mgr, connector = manager
        monkeypatch.setattr(om_module, "REPEG_MAX_ATTEMPTS", 1)
        monkeypatch.setattr(om_module, "REPEG_INTERVAL_S", 0.02)
        monkeypatch.setattr(om_module, "REPEG_POLL_S", 0.01)
        order = make_order(0.01)
        rid = order.request_id

        placed: list[OrderRequest] = []

        async def place(req):
            placed.append(req)
            if req.order_type == "limit":
                return _result(rid, "ord-limit", "new")
            return _result(rid, "ord-mkt", "filled", req.quantity, 100_020.0)

        connector.place_order = AsyncMock(side_effect=place)
        connector.fetch_order_result = AsyncMock(
            return_value=_result(rid, "ord-limit", "cancelled", 0.003, 100_000.0)
        )

        result = await mgr.place_limit_order(order)

        assert result["status"] == "filled"
        assert result["filled_qty"] == pytest.approx(0.01)
        market_orders = [p for p in placed if p.order_type == "market"]
        assert len(market_orders) == 1
        assert market_orders[0].quantity == pytest.approx(0.007)

    @pytest.mark.asyncio
    async def test_cumulative_fill_completes_without_fallback(self, manager, monkeypatch):
        """누적 체결로 잔량이 최소수량 미만이 되면 시장가 폴백 없이 종결."""
        mgr, connector = manager
        monkeypatch.setattr(om_module, "REPEG_INTERVAL_S", 0.02)
        monkeypatch.setattr(om_module, "REPEG_POLL_S", 0.01)
        order = make_order(0.01)
        rid = order.request_id

        placed: list[OrderRequest] = []

        async def place(req):
            placed.append(req)
            return _result(rid, f"ord-{len(placed)}", "new")

        connector.place_order = AsyncMock(side_effect=place)
        # 1차 취소 수확에서 거의 전량(0.0095) 체결 확인 → 잔량 0.0005 < min 0.001
        connector.fetch_order_result = AsyncMock(
            return_value=_result(rid, "ord-1", "cancelled", 0.0095, 100_000.0)
        )

        result = await mgr.place_limit_order(order)

        assert result["status"] == "filled"
        assert result["filled_qty"] == pytest.approx(0.0095)
        market_orders = [p for p in placed if p.order_type == "market"]
        assert not market_orders

    @pytest.mark.asyncio
    async def test_market_fallback_rejected_reports_partial(self, manager, monkeypatch):
        """부분체결 보유 상태에서 폴백 거부 → partially_filled로 보고 (전략 재동기화 유도)."""
        mgr, connector = manager
        monkeypatch.setattr(om_module, "REPEG_MAX_ATTEMPTS", 1)
        monkeypatch.setattr(om_module, "REPEG_INTERVAL_S", 0.02)
        monkeypatch.setattr(om_module, "REPEG_POLL_S", 0.01)
        monkeypatch.setattr(mgr, "_max_retries", 1)
        order = make_order(0.01)
        rid = order.request_id

        async def place(req):
            if req.order_type == "limit":
                return _result(rid, "ord-limit", "new")
            raise type("InsufficientFunds", (Exception,), {})("no funds")

        connector.place_order = AsyncMock(side_effect=place)
        connector.fetch_order_result = AsyncMock(
            return_value=_result(rid, "ord-limit", "cancelled", 0.003, 100_000.0)
        )

        result = await mgr.place_limit_order(order)

        assert result["status"] == "partially_filled"
        assert result["filled_qty"] == pytest.approx(0.003)

    @pytest.mark.asyncio
    async def test_cancellation_cleans_up_outstanding_order(self, manager, monkeypatch):
        """엔진 타임아웃(태스크 취소) 시 거래소에 떠 있는 주문을 취소한다."""
        mgr, connector = manager
        monkeypatch.setattr(om_module, "REPEG_INTERVAL_S", 5.0)
        monkeypatch.setattr(om_module, "REPEG_POLL_S", 0.05)
        order = make_order(0.01)
        rid = order.request_id

        connector.place_order = AsyncMock(return_value=_result(rid, "ord-live", "new"))
        connector.fetch_order_result = AsyncMock(return_value=_result(rid, "ord-live", "new"))

        task = asyncio.create_task(mgr.place_limit_order(order))
        await asyncio.sleep(0.1)  # 주문이 떠 있는 상태로 진입
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        connector.cancel_order.assert_awaited_with("ord-live", "BTC/USDT:USDT")


class TestDuplicateRejection:
    @pytest.mark.asyncio
    async def test_duplicate_includes_strategy_id(self, manager):
        """중복 거부 결과에도 strategy_id 포함 — 전략별 채널 발행이 가능해야 한다."""
        mgr, connector = manager
        payload = {
            "request_id": "dup-001",
            "strategy_id": "supertrend-01",
            "exchange": "bybit",
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.001,
        }
        connector.place_order = AsyncMock(
            return_value=_result("dup-001", "ord-1", "filled", 0.001, 100_000.0)
        )

        await mgr.place_order(dict(payload))
        dup = await mgr.place_order(dict(payload))

        assert dup["status"] == "rejected"
        assert dup["reason"] == "duplicate_request_id"
        assert dup["strategy_id"] == "supertrend-01"
        assert dup["symbol"] == "BTC/USDT:USDT"
