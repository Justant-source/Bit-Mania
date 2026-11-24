"""Telegram command handling (long polling inside the caller's event loop).

Commands: /status /report /dashboard /emergency_close /resume /help.
Only the configured chat may use them. Dangerous actions are two-step:
an inline confirm button that is valid for 60 seconds and works once.
"""

from __future__ import annotations

import secrets
import time
from typing import Any, Callable

import structlog

from bot.notify import esc
from bot.report import build_daily_report, fmt_kst, format_position, report_keyboard, to_dt

log = structlog.get_logger("telegram_bot")

CONFIRM_TTL_S = 60.0

HELP_TEXT = (
    "<b>명령어</b>\n"
    "/status — 현재 상태 (잔고, 포지션, 차단 여부)\n"
    "/report — 일일 리포트\n"
    "/dashboard — 대시보드 링크\n"
    "/emergency_close — 신규 진입 중단 + 포지션 청산 (확인 버튼 필요)\n"
    "/resume — 수동 중단 해제 (확인 버튼 필요)\n"
    "/help — 이 도움말"
)


def _keyboard(rows: list[list[tuple[str, str]]]):
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup([[InlineKeyboardButton(t, callback_data=d) for t, d in row] for row in rows])


class TelegramCommands:
    def __init__(self, token: str, chat_id: str, control, db, *, dashboard_url: str = "",
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._token = (token or "").strip()
        self._chat_id = str(chat_id or "").strip()
        self._control = control
        self._db = db
        self._dashboard_url = dashboard_url or ""
        self._clock = clock
        self._pending: dict[str, tuple[str, float]] = {}  # token -> (action, expires_at)
        self._app = None

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        if not self._token or not self._chat_id:
            log.warning("telegram_commands_disabled", reason="token or chat_id empty")
            return
        from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

        app = Application.builder().token(self._token).build()
        for name, fn in (("status", self.cmd_status), ("report", self.cmd_report),
                         ("dashboard", self.cmd_dashboard), ("emergency_close", self.cmd_emergency_close),
                         ("resume", self.cmd_resume), ("help", self.cmd_help), ("start", self.cmd_help)):
            app.add_handler(CommandHandler(name, fn))
        app.add_handler(CallbackQueryHandler(self.on_callback))
        app.add_handler(MessageHandler(filters.COMMAND, self.cmd_unknown))
        app.add_error_handler(self._on_error)
        await app.initialize()
        await app.start()
        # stale updates (e.g. an old /emergency_close) must not run after a restart
        await app.updater.start_polling(allowed_updates=["message", "callback_query"],
                                        drop_pending_updates=True)
        self._app = app
        try:
            from telegram import BotCommand
            await app.bot.set_my_commands([
                BotCommand("status", "현재 상태"), BotCommand("report", "일일 리포트"),
                BotCommand("dashboard", "대시보드 링크"), BotCommand("emergency_close", "긴급 청산"),
                BotCommand("resume", "수동 중단 해제"), BotCommand("help", "도움말")])
        except Exception as exc:
            log.warning("telegram_set_commands_failed", error=repr(exc))
        log.info("telegram_commands_started")

    async def stop(self) -> None:
        app, self._app = self._app, None
        if app is None:
            return
        for step in (app.updater.stop, app.stop, app.shutdown):
            try:
                await step()
            except Exception as exc:
                log.warning("telegram_stop_step_failed", error=repr(exc))

    async def _on_error(self, update, context) -> None:
        # warning level on purpose: ERROR would be echoed back to Telegram
        log.warning("telegram_handler_error", error=repr(context.error))

    # ── helpers ─────────────────────────────────────────────────────────────

    def _authorized(self, update) -> bool:
        chat = getattr(update, "effective_chat", None)
        ok = chat is not None and str(chat.id) == self._chat_id
        if not ok:
            log.warning("telegram_unauthorized", chat_id=getattr(chat, "id", None))
        return ok

    @staticmethod
    async def _reply(update, text: str, reply_markup=None) -> None:
        await update.effective_message.reply_text(text, parse_mode="HTML", reply_markup=reply_markup,
                                                  disable_web_page_preview=True)

    def _new_pending(self, action: str) -> str:
        now = self._clock()
        self._pending = {k: v for k, v in self._pending.items() if v[1] > now}
        tok = secrets.token_hex(4)
        self._pending[tok] = (action, now + CONFIRM_TTL_S)
        return tok

    @staticmethod
    def _who(update) -> str:
        user = getattr(update, "effective_user", None)
        return f"telegram:{getattr(user, 'id', 'unknown')}"

    async def _status(self) -> dict[str, Any]:
        try:
            return await self._control.status() or {}
        except Exception as exc:
            log.warning("telegram_status_failed", error=repr(exc))
            return {}

    # ── commands ────────────────────────────────────────────────────────────

    async def cmd_help(self, update, context=None) -> None:
        if self._authorized(update):
            await self._reply(update, HELP_TEXT)

    async def cmd_unknown(self, update, context=None) -> None:
        if self._authorized(update):
            await self._reply(update, "알 수 없는 명령입니다. /help 를 확인하세요.")

    async def cmd_status(self, update, context=None) -> None:
        if not self._authorized(update):
            return
        s = await self._status()
        if not s:
            await self._reply(update, "⚠️ 상태를 가져오지 못했습니다.")
            return
        eq = s.get("equity")
        lines = [
            "\U0001f4ca <b>현재 상태</b>",
            "",
            f"잔고: <code>{'$%s' % format(float(eq), ',.2f') if eq is not None else 'N/A'}</code>",
            format_position(s.get("position")),
            "",
        ]
        if s.get("halted"):
            lines.append(f"\U0001f6d1 진입 차단: <code>{esc(s.get('halt_reason'))}</code>")
        else:
            lines.append("✅ 진입 가능")
        lines.append(f"Kill level: <code>{esc(s.get('kill_level'))}</code>")
        lines.append(
            f"낙폭 일/주/월: <code>{float(s.get('daily_dd_pct') or 0):.2f}% / "
            f"{float(s.get('weekly_dd_pct') or 0):.2f}% / {float(s.get('monthly_dd_pct') or 0):.2f}%</code>")
        sig = s.get("last_signal")
        if sig:
            lines.append(f"마지막 신호: <code>{esc(sig.get('expected_action') or sig.get('action'))}</code>")
        lines.append(f"마지막 확정봉: {fmt_kst(s.get('last_bar_ts'))}")
        lines.append(f"다음 봉 처리: {fmt_kst(s.get('next_bar_at'))}")
        hb = to_dt(s.get("heartbeat_at"))
        if hb is not None:
            from datetime import datetime, timezone
            age = int((datetime.now(timezone.utc) - hb).total_seconds())
            lines.append(f"Heartbeat: {age}초 전")
        await self._reply(update, "\n".join(lines))

    async def cmd_report(self, update, context=None) -> None:
        if not self._authorized(update):
            return
        try:
            text = await build_daily_report(self._db, self._control)
        except Exception as exc:
            log.warning("telegram_report_failed", error=repr(exc))
            await self._reply(update, "⚠️ 리포트를 만들지 못했습니다.")
            return
        await self._reply(update, text, report_keyboard(self._dashboard_url))

    async def cmd_dashboard(self, update, context=None) -> None:
        if not self._authorized(update):
            return
        if not self._dashboard_url:
            await self._reply(update, "대시보드가 설정되지 않았습니다 (DASHBOARD_URL).")
            return
        try:
            await self._reply(update, "\U0001f4ca 대시보드", report_keyboard(self._dashboard_url))
        except Exception as exc:
            # Telegram refuses some URL buttons (e.g. plain http on localhost): fall back to text
            log.warning("telegram_dashboard_button_failed", error=repr(exc))
            await self._reply(update, f"\U0001f4ca 대시보드: {esc(self._dashboard_url)}")

    async def cmd_emergency_close(self, update, context=None) -> None:
        if not self._authorized(update):
            return
        s = await self._status()
        eq = s.get("equity")
        tok = self._new_pending("emergency_close")
        text = (
            "\U0001f6a8 <b>긴급 청산 확인</b>\n\n"
            f"잔고: <code>{'$%s' % format(float(eq), ',.2f') if eq is not None else 'N/A'}</code>\n"
            f"{format_position(s.get('position'))}\n\n"
            "실행하면 신규 진입이 중단되고 포지션이 시장가로 청산됩니다. "
            "(/resume 으로 해제)\n<i>60초 안에 선택하세요.</i>"
        )
        await self._reply(update, text, _keyboard([[("청산 실행", f"ec:yes:{tok}"), ("취소", f"ec:no:{tok}")]]))

    async def cmd_resume(self, update, context=None) -> None:
        if not self._authorized(update):
            return
        s = await self._status()
        if s and not s.get("halted"):
            await self._reply(update, "✅ 현재 중단 상태가 아닙니다.")
            return
        tok = self._new_pending("resume")
        text = (
            "▶️ <b>재개 확인</b>\n\n"
            f"중단 사유: <code>{esc(s.get('halt_reason'))}</code>\n"
            "수동 중단을 해제하고 신규 진입을 다시 허용합니다.\n<i>60초 안에 선택하세요.</i>"
        )
        await self._reply(update, text, _keyboard([[("재개 실행", f"rs:yes:{tok}"), ("취소", f"rs:no:{tok}")]]))

    # ── confirmation buttons ────────────────────────────────────────────────

    async def on_callback(self, update, context=None) -> None:
        q = getattr(update, "callback_query", None)
        if q is None or not self._authorized(update):
            return
        try:
            await q.answer()
        except Exception:
            pass
        parts = (q.data or "").split(":")
        if len(parts) != 3 or parts[0] not in ("ec", "rs") or parts[1] not in ("yes", "no"):
            return
        prefix, choice, tok = parts
        action = "emergency_close" if prefix == "ec" else "resume"
        entry = self._pending.pop(tok, None)  # pop first: a button works exactly once
        if entry is None or entry[0] != action or entry[1] <= self._clock():
            await q.edit_message_text("⌛ 만료되었거나 이미 처리된 요청입니다. 명령을 다시 입력하세요.")
            return
        if choice == "no":
            await q.edit_message_text("취소되었습니다.")
            return
        who = self._who(update)
        if action == "emergency_close":
            await q.edit_message_text("⏳ 긴급 청산 실행 중…")
            try:
                res = await self._control.emergency_close(who)
            except Exception as exc:
                log.warning("telegram_emergency_close_failed", error=repr(exc))
                await q.edit_message_text(f"❌ 긴급 청산 실패: {esc(repr(exc))}\n거래소에서 직접 확인하세요.",
                                          parse_mode="HTML")
                return
            await q.edit_message_text(self._emergency_text(res), parse_mode="HTML")
        else:
            try:
                res = await self._control.resume(who)
            except Exception as exc:
                log.warning("telegram_resume_failed", error=repr(exc))
                await q.edit_message_text(f"❌ 재개 실패: {esc(repr(exc))}", parse_mode="HTML")
                return
            if res.get("halted"):
                text = "⚠️ 수동 중단은 해제했지만 아직 진입이 차단되어 있습니다 (Kill Switch 쿨다운 등). /status 확인."
            else:
                text = "✅ 재개되었습니다. 신규 진입이 허용됩니다."
            await q.edit_message_text(text, parse_mode="HTML")

    @staticmethod
    def _emergency_text(res: dict[str, Any]) -> str:
        detail = f"\n<code>{esc(res.get('detail'))}</code>" if res.get("detail") else ""
        if res.get("flat"):
            head = "✅ <b>긴급 청산 완료</b> — 거래소 포지션 없음 (flat)." if res.get("had_position") else \
                   "✅ 청산할 포지션이 없었습니다. 신규 진입은 중단되었습니다 (flat)."
            return head + "\n/resume 으로 재개할 수 있습니다." + detail
        return ("\U0001f6a8 <b>청산 후에도 포지션이 남아 있습니다 (flat 아님)</b>\n"
                "즉시 거래소에서 수동 확인이 필요합니다." + detail)
