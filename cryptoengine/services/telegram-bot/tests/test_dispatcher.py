"""Unit tests for telegram-bot dispatcher.py (2026-09-19 개편).

Covers:
- 소액 거래 필터 삭제 회귀 잠금 (금액 무관 항상 알림)
- HTML parse_mode 사용
- 등급별 disable_notification (무음/소리)
- entry/exit의 레이트리밋 예외 처리
- 4096자 절단 후에도 전송됨
- HTML 파싱 실패 시 평문 폴백 재전송
"""

from __future__ import annotations

import asyncio
import os
import sys

# Allow running tests directly from the service directory without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import AsyncMock

import pytest

from dispatcher import AlertDispatcher


class _FakeBadRequest(Exception):
    """telegram.error.BadRequest를 흉내내는 더미 예외 (실제 라이브러리 의존 없이 테스트)."""


def _make_dispatcher(bot=None, **kwargs):
    bot = bot or AsyncMock()
    return AlertDispatcher(bot=bot, chat_id=12345, batch_window_seconds=0.01, **kwargs), bot


@pytest.mark.asyncio
class TestTradeAlertsAlwaysSend:
    async def test_small_notional_entry_still_dispatches(self):
        """2026-09-19: min_trade_size_usd 필터가 완전히 삭제됐다 — 회귀 잠금."""
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("entry", {
            "strategy_id": "supertrend-01", "symbol": "BTCUSDT",
            "side": "buy", "quantity": 0.0001, "filled_price": 70000.0,
            "notional_usd": 7.0, "fee": 0.01,
        })
        await dispatcher.flush_all()
        bot.send_message.assert_awaited_once()

    async def test_tiny_quantity_fallback_still_dispatches(self):
        """notional_usd가 없고 quantity만 있어도(과거 트랩 케이스) 여전히 알림된다."""
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("exit", {"strategy_id": "s", "quantity": 0.001})
        await dispatcher.flush_all()
        bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
class TestParseModeAndSilence:
    async def test_default_parse_mode_is_html(self):
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("entry", {"symbol": "BTCUSDT", "side": "buy"})
        await dispatcher.flush_all()
        _, kwargs = bot.send_message.call_args
        assert kwargs["parse_mode"] == "HTML"

    async def test_trade_tier_is_silent(self):
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("entry", {"symbol": "BTCUSDT", "side": "buy"})
        await dispatcher.flush_all()
        _, kwargs = bot.send_message.call_args
        assert kwargs["disable_notification"] is True

    async def test_kill_switch_tier_is_not_silent(self):
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("kill_switch", {"level": 2, "trigger_reason": "x"})
        _, kwargs = bot.send_message.call_args
        assert kwargs["disable_notification"] is False

    async def test_anomaly_tier_is_not_silent(self):
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("anomaly", {"level": "ERROR", "message": "x"})
        _, kwargs = bot.send_message.call_args
        assert kwargs["disable_notification"] is False


@pytest.mark.asyncio
class TestRateLimitExemption:
    async def test_entry_survives_full_rate_limit(self):
        dispatcher, bot = _make_dispatcher(max_messages_per_minute=1)
        # 레이트리밋 한도(1건)를 kill_switch로 채운다 (고빈도 우회 타입)
        await dispatcher.dispatch("kill_switch", {"level": 1, "trigger_reason": "a"})
        bot.send_message.reset_mock()
        # 일반 타입은 이제 드롭돼야 한다
        await dispatcher.dispatch("funding", {"symbol": "BTCUSDT", "payment": -1.0})
        bot.send_message.assert_not_awaited()
        # 하지만 entry는 레이트리밋 예외 대상이라 여전히 전송된다
        await dispatcher.dispatch("entry", {"symbol": "BTCUSDT", "side": "buy"})
        await dispatcher.flush_all()
        bot.send_message.assert_awaited_once()

    async def test_non_exempt_type_dropped_over_limit(self):
        dispatcher, bot = _make_dispatcher(max_messages_per_minute=1)
        await dispatcher.dispatch("kill_switch", {"level": 1, "trigger_reason": "a"})
        bot.send_message.reset_mock()
        await dispatcher.dispatch("funding", {"symbol": "BTCUSDT", "payment": -5.0})
        bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
class TestTruncation:
    async def test_long_anomaly_still_sends_within_limit(self):
        dispatcher, bot = _make_dispatcher()
        await dispatcher.dispatch("anomaly", {
            "level": "ERROR", "component": "x", "message": "y" * 5000,
        })
        bot.send_message.assert_awaited_once()
        _, kwargs = bot.send_message.call_args
        assert len(kwargs["text"]) <= 4096


@pytest.mark.asyncio
class TestHtmlParseFallback:
    async def test_bad_request_falls_back_to_plaintext(self):
        bot = AsyncMock()
        bot.send_message.side_effect = [
            Exception("Bad Request: can't parse entities: some detail"),
            None,
        ]
        dispatcher, _ = _make_dispatcher(bot=bot)
        await dispatcher.dispatch("kill_switch", {"level": 2, "trigger_reason": "x"})
        assert bot.send_message.await_count == 2
        second_call = bot.send_message.call_args_list[1]
        assert second_call.kwargs["parse_mode"] is None
        assert "<" not in second_call.kwargs["text"]

    async def test_non_parse_error_does_not_retry(self):
        bot = AsyncMock()
        bot.send_message.side_effect = Exception("network unreachable")
        dispatcher, _ = _make_dispatcher(bot=bot)
        await dispatcher.dispatch("kill_switch", {"level": 2, "trigger_reason": "x"})
        assert bot.send_message.await_count == 1

    async def test_failed_send_does_not_count_toward_rate_limit(self):
        bot = AsyncMock()
        bot.send_message.side_effect = Exception("network unreachable")
        dispatcher, _ = _make_dispatcher(bot=bot, max_messages_per_minute=30)
        await dispatcher.dispatch("kill_switch", {"level": 2, "trigger_reason": "x"})
        assert len(dispatcher._sent_ts) == 0
