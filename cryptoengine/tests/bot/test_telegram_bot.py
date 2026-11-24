from types import SimpleNamespace

import pytest

from bot.telegram_bot import TelegramCommands
from test_report import FakeControl, db  # noqa: F401  (db is a fixture)

CHAT = "555"


class Msg:
    def __init__(self):
        self.sent = []

    async def reply_text(self, text, **kw):
        self.sent.append((text, kw))


class Query:
    def __init__(self, data):
        self.data = data
        self.edits = []
        self.answered = False

    async def answer(self, *a, **k):
        self.answered = True

    async def edit_message_text(self, text, **kw):
        self.edits.append(text)


def cmd_update(chat=CHAT):
    return SimpleNamespace(effective_chat=SimpleNamespace(id=int(chat)), effective_message=Msg(),
                           effective_user=SimpleNamespace(id=42), callback_query=None)


def cb_update(data, chat=CHAT):
    q = Query(data)
    return SimpleNamespace(effective_chat=SimpleNamespace(id=int(chat)), effective_message=None,
                           effective_user=SimpleNamespace(id=42), callback_query=q), q


class Control(FakeControl):
    def __init__(self, flat=True, **kw):
        super().__init__(**kw)
        self.flat = flat
        self.closed = []
        self.resumed = []
        self.resume_halted = False

    async def emergency_close(self, requested_by):
        self.closed.append(requested_by)
        return {"had_position": True, "closed": True, "flat": self.flat, "detail": "ok"}

    async def resume(self, requested_by):
        self.resumed.append(requested_by)
        return {"was_halted": True, "halted": self.resume_halted}


class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


def make(ctl=None, url="https://d.example/?t=k"):
    clock = Clock()
    return TelegramCommands("tok", CHAT, ctl or Control(), None, dashboard_url=url, clock=clock), clock


def token_of(update) -> str:
    kb = update.effective_message.sent[-1][1]["reply_markup"].to_dict()["inline_keyboard"][0]
    return kb[0]["callback_data"], kb[1]["callback_data"]


@pytest.mark.asyncio
async def test_unauthorized_chat_ignored():
    ctl = Control()
    tc, _ = make(ctl)
    for fn in (tc.cmd_status, tc.cmd_report, tc.cmd_dashboard, tc.cmd_emergency_close, tc.cmd_resume,
               tc.cmd_help, tc.cmd_unknown):
        u = cmd_update("999")
        await fn(u)
        assert u.effective_message.sent == []
    # a confirm button from a stranger does nothing even with a valid token
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    yes, _ = token_of(u)
    cu, q = cb_update(yes, chat="999")
    await tc.on_callback(cu)
    assert ctl.closed == [] and q.edits == []


@pytest.mark.asyncio
async def test_emergency_close_needs_confirm_and_runs_once():
    ctl = Control(position={"side": "long", "size": 0.007, "entry_price": 80000.0})
    tc, _ = make(ctl)
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    text, kw = u.effective_message.sent[0]
    assert "0.0070" in text and "$150.00" in text
    assert ctl.closed == []                       # not executed by the command alone
    yes, no = token_of(u)
    assert yes.startswith("ec:yes:") and no.startswith("ec:no:")
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert ctl.closed == ["telegram:42"] and q.answered
    assert "긴급 청산 완료" in q.edits[-1]
    cu2, q2 = cb_update(yes)                       # second press: already consumed
    await tc.on_callback(cu2)
    assert ctl.closed == ["telegram:42"] and "만료" in q2.edits[-1]


@pytest.mark.asyncio
async def test_emergency_close_cancel_button():
    ctl = Control()
    tc, _ = make(ctl)
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    _, no = token_of(u)
    cu, q = cb_update(no)
    await tc.on_callback(cu)
    assert ctl.closed == [] and "취소" in q.edits[-1]


@pytest.mark.asyncio
async def test_emergency_close_expires_after_60s():
    ctl = Control()
    tc, clock = make(ctl)
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    yes, _ = token_of(u)
    clock.t += 61
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert ctl.closed == [] and "만료" in q.edits[-1]


@pytest.mark.asyncio
async def test_emergency_close_just_inside_window():
    ctl = Control()
    tc, clock = make(ctl)
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    yes, _ = token_of(u)
    clock.t += 59
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert ctl.closed == ["telegram:42"]


@pytest.mark.asyncio
@pytest.mark.parametrize("flat,expect", [(True, "긴급 청산 완료"), (False, "flat 아님")])
async def test_reply_reflects_flat(flat, expect):
    ctl = Control(flat=flat)
    tc, _ = make(ctl)
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    yes, _ = token_of(u)
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert expect in q.edits[-1]
    if flat:
        assert "flat 아님" not in q.edits[-1]


@pytest.mark.asyncio
async def test_emergency_close_exception_reported():
    class Boom(Control):
        async def emergency_close(self, requested_by):
            raise RuntimeError("exchange down")
    tc, _ = make(Boom())
    u = cmd_update()
    await tc.cmd_emergency_close(u)
    yes, _ = token_of(u)
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert "실패" in q.edits[-1]


@pytest.mark.asyncio
async def test_token_for_other_action_rejected():
    ctl = Control(halted=True, halt_reason="manual_halt")
    tc, _ = make(ctl)
    u = cmd_update()
    await tc.cmd_resume(u)
    yes, _ = token_of(u)
    tok = yes.split(":")[2]
    cu, q = cb_update(f"ec:yes:{tok}")             # resume token must not trigger emergency close
    await tc.on_callback(cu)
    assert ctl.closed == [] and ctl.resumed == []


@pytest.mark.asyncio
async def test_resume_two_step():
    ctl = Control(halted=True, halt_reason="manual_halt")
    tc, _ = make(ctl)
    u = cmd_update()
    await tc.cmd_resume(u)
    assert ctl.resumed == []
    yes, _ = token_of(u)
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert ctl.resumed == ["telegram:42"] and "재개되었습니다" in q.edits[-1]


@pytest.mark.asyncio
async def test_resume_still_blocked_message_and_expiry():
    ctl = Control(halted=True)
    ctl.resume_halted = True
    tc, clock = make(ctl)
    u = cmd_update()
    await tc.cmd_resume(u)
    yes, _ = token_of(u)
    cu, q = cb_update(yes)
    await tc.on_callback(cu)
    assert "아직 진입이 차단" in q.edits[-1]
    u2 = cmd_update()
    await tc.cmd_resume(u2)
    yes2, _ = token_of(u2)
    clock.t += 61
    cu2, q2 = cb_update(yes2)
    await tc.on_callback(cu2)
    assert len(ctl.resumed) == 1


@pytest.mark.asyncio
async def test_resume_when_not_halted():
    tc, _ = make(Control(halted=False))
    u = cmd_update()
    await tc.cmd_resume(u)
    assert "중단 상태가 아닙니다" in u.effective_message.sent[0][0]
    assert u.effective_message.sent[0][1]["reply_markup"] is None


@pytest.mark.asyncio
async def test_status_help_unknown_dashboard():
    tc, _ = make(Control(position={"side": "long", "size": 0.007, "entry_price": 80000.0}))
    u = cmd_update()
    await tc.cmd_status(u)
    assert "$150.00" in u.effective_message.sent[0][0] and "0.0070" in u.effective_message.sent[0][0]
    u = cmd_update()
    await tc.cmd_help(u)
    assert "/emergency_close" in u.effective_message.sent[0][0]
    u = cmd_update()
    await tc.cmd_unknown(u)
    assert "알 수 없는" in u.effective_message.sent[0][0]
    u = cmd_update()
    await tc.cmd_dashboard(u)
    btn = u.effective_message.sent[0][1]["reply_markup"].to_dict()["inline_keyboard"][0][0]
    assert btn["url"] == "https://d.example/?t=k"


@pytest.mark.asyncio
async def test_dashboard_not_configured():
    tc, _ = make(url="")
    u = cmd_update()
    await tc.cmd_dashboard(u)
    assert "설정되지 않았습니다" in u.effective_message.sent[0][0]


@pytest.mark.asyncio
async def test_report_command(db):  # noqa: F811
    tc = TelegramCommands("tok", CHAT, Control(), db, dashboard_url="https://d.example/?t=k")
    u = cmd_update()
    await tc.cmd_report(u)
    text, kw = u.effective_message.sent[0]
    assert "일일 리포트" in text and kw["reply_markup"] is not None


@pytest.mark.asyncio
async def test_start_without_credentials_is_noop():
    tc = TelegramCommands("", "", Control(), None, dashboard_url="")
    await tc.start()
    await tc.stop()
