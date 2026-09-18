"""Unit tests for telegram-bot handlers.py HTML 전환 (2026-09-19 개편).

Markdown v1(`*bold*`, `` `code` ``, `_italic_`) → Telegram HTML로 전환한
handlers.py의 ~32개 parse_mode="Markdown" 사이트가 전부 바뀌었는지, 그리고
몇 개 핵심 핸들러가 실제로 태그 균형이 맞는 HTML을 만드는지 검증한다.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

import handlers as handlers_module
from handlers import BotHandlers


HANDLERS_SRC = Path(handlers_module.__file__).read_text(encoding="utf-8")


def _tags_balanced(msg: str) -> bool:
    import re

    stack: list[str] = []
    for m in re.finditer(r"</?([a-zA-Z]+)[^>]*>", msg):
        tag = m.group(1).lower()
        if m.group(0).startswith("</"):
            if not stack or stack[-1] != tag:
                return False
            stack.pop()
        elif not m.group(0).endswith("/>"):
            stack.append(tag)
    return not stack


class TestNoMarkdownLeft:
    def test_no_markdown_parse_mode_literal(self):
        assert 'parse_mode="Markdown"' not in HANDLERS_SRC

    def test_uses_parse_mode_constant(self):
        assert handlers_module.PARSE_MODE == "HTML"
        # 32개 이상 자리가 이 상수를 재사용해야 한다(하드코딩 "HTML" 문자열 남발 방지)
        assert HANDLERS_SRC.count("parse_mode=PARSE_MODE") >= 25


@pytest.fixture
def handlers(monkeypatch):
    monkeypatch.setattr(handlers_module, "AUTHORIZED_CHAT_ID", 42)
    redis_mock = AsyncMock()
    db_pool = MagicMock()
    return BotHandlers(redis_client=redis_mock, db_pool=db_pool, dispatcher=None)


def _make_update(chat_id: int = 42):
    update = MagicMock()
    update.effective_chat.id = chat_id
    update.effective_message.reply_text = AsyncMock()
    update.effective_user.id = 999
    return update


class TestUnknownCommandHtml:
    @pytest.mark.asyncio
    async def test_reply_is_html_and_balanced(self, handlers):
        update = _make_update()
        context = MagicMock()
        await handlers.unknown_command(update, context)

        update.effective_message.reply_text.assert_awaited_once()
        args, kwargs = update.effective_message.reply_text.call_args
        msg = args[0]
        assert kwargs.get("parse_mode") == "HTML"
        assert _tags_balanced(msg)
        assert "<code>/help</code>" in msg


class TestHelpCommandHtml:
    @pytest.mark.asyncio
    async def test_reply_is_html_and_balanced(self, handlers):
        update = _make_update()
        context = MagicMock()
        await handlers.help_command(update, context)

        update.effective_message.reply_text.assert_awaited_once()
        args, kwargs = update.effective_message.reply_text.call_args
        msg = args[0]
        assert kwargs.get("parse_mode") == "HTML"
        assert _tags_balanced(msg)
        assert "<b>CryptoEngine 봇</b>" in msg


class TestStopCommandEscaping:
    @pytest.mark.asyncio
    async def test_missing_args_shows_usage_html(self, handlers):
        update = _make_update()
        context = MagicMock()
        context.args = []
        await handlers.stop_command(update, context)

        args, kwargs = update.effective_message.reply_text.call_args
        msg = args[0]
        assert kwargs.get("parse_mode") == "HTML"
        assert _tags_balanced(msg)

    @pytest.mark.asyncio
    async def test_strategy_id_is_escaped(self, handlers):
        update = _make_update()
        context = MagicMock()
        context.args = ["<script>evil</script>"]
        handlers.redis.publish = AsyncMock()

        await handlers.stop_command(update, context)

        args, kwargs = update.effective_message.reply_text.call_args
        msg = args[0]
        assert "<script>evil</script>" not in msg  # 이스케이프 없이 그대로 살아남으면 안 됨
        assert "&lt;script&gt;" in msg
        assert _tags_balanced(msg)


class TestUnauthorizedIgnored:
    @pytest.mark.asyncio
    async def test_unauthorized_chat_gets_no_reply(self, handlers):
        update = _make_update(chat_id=999)  # AUTHORIZED_CHAT_ID=42 로 몽키패치됨
        context = MagicMock()
        await handlers.unknown_command(update, context)
        update.effective_message.reply_text.assert_not_awaited()
