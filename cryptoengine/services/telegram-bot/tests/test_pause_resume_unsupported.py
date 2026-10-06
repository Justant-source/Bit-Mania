""" /pause_all and /resume_all reply unsupported and must not publish."""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

import handlers as handlers_module
from handlers import BotHandlers

UNSUPPORTED = "미지원 — 긴급 중단은 /emergency_close"


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
    update.callback_query = None
    return update


@pytest.mark.asyncio
@pytest.mark.parametrize("method_name", ["pause_all_command", "resume_all_command"])
async def test_commands_do_not_publish_and_reply_unsupported(handlers, method_name):
    update = _make_update()
    context = MagicMock()

    await getattr(handlers, method_name)(update, context)

    handlers.redis.publish.assert_not_awaited()
    args, _kwargs = update.effective_message.reply_text.call_args
    assert UNSUPPORTED in args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("cmd", ["pause_all", "resume_all"])
async def test_legacy_buttons_hit_unsupported_reply(handlers, cmd):
    update = _make_update()
    update.callback_query = MagicMock()
    update.callback_query.data = f"cmd:{cmd}"
    update.callback_query.answer = AsyncMock()
    context = MagicMock()

    await handlers.handle_callback(update, context)

    handlers.redis.publish.assert_not_awaited()
    args, _kwargs = update.effective_message.reply_text.call_args
    assert UNSUPPORTED in args[0]


@pytest.mark.asyncio
async def test_help_keyboard_omits_pause_resume_and_retired_commands(handlers):
    update = _make_update()
    await handlers.help_command(update, MagicMock())

    args, kwargs = update.effective_message.reply_text.call_args
    callbacks = [
        button.callback_data
        for row in kwargs["reply_markup"].inline_keyboard
        for button in row
    ]
    assert "cmd:pause_all" not in callbacks
    assert "cmd:resume_all" not in callbacks
    assert "/start" not in args[0]
    assert "/stop" not in args[0]
    assert "/weight" not in args[0]
