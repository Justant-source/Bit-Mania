"""Telegram bot command handlers for CryptoEngine."""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from formatters import format_alert, format_daily_report, format_pnl, format_position
from shared.log_events import *

log = structlog.get_logger(__name__)

# Authorized chat ID — only respond to messages from this chat
AUTHORIZED_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# Work request/result directories (mounted via docker volume)
REQUEST_DIR = Path(os.getenv("REQUEST_DIR", "/app/request_dir"))
RESULT_DIR = Path(os.getenv("RESULT_DIR", "/app/result_dir"))


def _authorized(update: Update) -> bool:
    """Check if the message comes from the authorized chat."""
    if not update.effective_chat:
        return False
    return update.effective_chat.id == AUTHORIZED_CHAT_ID


class BotHandlers:
    """Command and alert handlers for the CryptoEngine Telegram bot."""

    def __init__(self, redis_client: aioredis.Redis, db_pool: asyncpg.Pool) -> None:
        self.redis = redis_client
        self.db_pool = db_pool

    # ── Command: /status ─────────────────────────────────────────

    async def status_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Full position + PnL summary."""
        if not _authorized(update):
            return

        try:
            portfolio_raw = await self.redis.get("ce:portfolio:state")
            if not portfolio_raw:
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\u26a0\ufe0f No portfolio state available. System may be starting up."
                )
                return

            portfolio = json.loads(portfolio_raw)
            positions_raw = await self.redis.get("ce:positions:all")
            positions = json.loads(positions_raw) if positions_raw else []

            msg_parts = [format_pnl(portfolio)]

            if positions:
                msg_parts.append("")
                msg_parts.append("*Open Positions:*")
                for pos in positions:
                    msg_parts.append("")
                    msg_parts.append(format_position(pos))

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\n".join(msg_parts),
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/status 명령 전송", chat_id=update.effective_chat.id)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/status 명령 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to fetch status. Check system logs."
            )

    # ── Command: /emergency_close ─────────────────────────────────

    async def emergency_close_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Close all positions immediately (Level 4 Kill Switch)."""
        if not _authorized(update):
            return

        try:
            kill_switch_msg = json.dumps(
                {
                    "level": 4,
                    "trigger_reason": "manual_telegram_command",
                    "triggered_by": str(update.effective_user.id if update.effective_user else "unknown"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self.redis.publish("ce:kill_switch", kill_switch_msg)
            await self.redis.set("ce:kill_switch:active", "true")

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\U0001f6a8 *EMERGENCY CLOSE INITIATED*\n\n"
                "Level 4 Kill Switch activated.\n"
                "All positions will be market-closed immediately.\n"
                "All strategies will be halted.\n\n"
                "_Use /status to monitor closure progress._",
                parse_mode="Markdown",
            )
            log.warning(
                KILL_SWITCH_TRIGGERED,
                message="긴급 청산 발동",
                triggered_by=update.effective_user.id if update.effective_user else "unknown",
            )

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="긴급 청산 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to trigger emergency close! Check system immediately."
            )

    # ── Command: /stop [strategy] ─────────────────────────────────

    async def stop_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Stop a specific strategy."""
        if not _authorized(update):
            return

        if not context.args:
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "Usage: `/stop <strategy_id>`\n"
                "Example: `/stop funding_arb`",
                parse_mode="Markdown",
            )
            return

        strategy_id = context.args[0]

        try:
            cmd = json.dumps(
                {
                    "strategy_id": strategy_id,
                    "action": "stop",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self.redis.publish("ce:strategy:command", cmd)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u23f9 Stop command sent to strategy `{strategy_id}`.\n"
                f"_Use /status to confirm._",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/stop 명령 전송", strategy_id=strategy_id)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/stop 명령 실패", strategy_id=strategy_id)
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u274c Failed to stop strategy `{strategy_id}`.",
                parse_mode="Markdown",
            )

    # ── Command: /start [strategy] ────────────────────────────────

    async def start_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Start a specific strategy."""
        if not _authorized(update):
            return

        if not context.args:
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "Usage: `/start <strategy_id>`\n"
                "Example: `/start funding_arb`",
                parse_mode="Markdown",
            )
            return

        strategy_id = context.args[0]

        try:
            # Check if kill switch is active
            kill_active = await self.redis.get("ce:kill_switch:active")
            if kill_active == "true":
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\U0001f6a8 Kill switch is active. Clear it before starting strategies.\n"
                    "Run `/emergency_close` again or manually clear via Redis.",
                    parse_mode="Markdown",
                )
                return

            cmd = json.dumps(
                {
                    "strategy_id": strategy_id,
                    "action": "start",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self.redis.publish("ce:strategy:command", cmd)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u25b6\ufe0f Start command sent to strategy `{strategy_id}`.\n"
                f"_Use /status to confirm._",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/start 명령 전송", strategy_id=strategy_id)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/start 명령 실패", strategy_id=strategy_id)
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u274c Failed to start strategy `{strategy_id}`.",
                parse_mode="Markdown",
            )

    # ── Command: /weight [strategy] [%] ───────────────────────────

    async def weight_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Manually adjust strategy weight allocation."""
        if not _authorized(update):
            return

        if not context.args or len(context.args) < 2:
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "Usage: `/weight <strategy_id> <percentage>`\n"
                "Example: `/weight funding_arb 30`",
                parse_mode="Markdown",
            )
            return

        strategy_id = context.args[0]
        try:
            weight_pct = float(context.args[1])
        except ValueError:
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Invalid percentage. Must be a number (e.g., 30)."
            )
            return

        if not (0.0 <= weight_pct <= 100.0):
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Weight must be between 0 and 100."
            )
            return

        try:
            cmd = json.dumps(
                {
                    "strategy_id": strategy_id,
                    "action": "reconfigure",
                    "params": {"weight_pct": weight_pct},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            await self.redis.publish("ce:strategy:command", cmd)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u2696\ufe0f Weight for `{strategy_id}` set to `{weight_pct}%`.\n"
                f"_Orchestrator will apply on next rebalance cycle._",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/weight 명령 전송", strategy_id=strategy_id, weight_pct=weight_pct)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/weight 명령 실패", strategy_id=strategy_id)
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u274c Failed to set weight for `{strategy_id}`.",
                parse_mode="Markdown",
            )

    # ── Command: /report ──────────────────────────────────────────

    async def report_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Generate today's PnL report."""
        if not _authorized(update):
            return

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            report = await self._build_daily_report(today)
            msg = format_daily_report(report)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                msg, parse_mode="Markdown"
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/report 명령 전송", date=today)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/report 명령 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to generate report. Check system logs."
            )

    async def _build_daily_report(self, date_str: str) -> dict[str, Any]:
        """Build daily report from database."""
        async with self.db_pool.acquire() as conn:
            # Get trade summary for the day
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_trades,
                    COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
                    COALESCE(SUM(realized_pnl), 0) AS total_pnl,
                    COALESCE(SUM(fee), 0) AS total_fees
                FROM trade_history
                WHERE DATE(closed_at) = $1
                """,
                date_str,
            )

            total_trades = row["total_trades"] if row else 0
            wins = row["wins"] if row else 0
            total_pnl = float(row["total_pnl"]) if row else 0.0
            total_fees = float(row["total_fees"]) if row else 0.0
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

            # Get funding earned
            funding_row = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(payment), 0) AS funding_earned
                FROM funding_payments
                WHERE DATE(paid_at) = $1
                """,
                date_str,
            )
            funding_earned = float(funding_row["funding_earned"]) if funding_row else 0.0

            # Get ending equity
            portfolio_raw = await self.redis.get("ce:portfolio:state")
            portfolio = json.loads(portfolio_raw) if portfolio_raw else {}
            ending_equity = portfolio.get("total_equity", 0.0)

            # Per-strategy breakdown
            strategy_rows = await conn.fetch(
                """
                SELECT
                    strategy_id,
                    COUNT(*) AS trades,
                    COALESCE(SUM(realized_pnl), 0) AS pnl
                FROM trade_history
                WHERE DATE(closed_at) = $1
                GROUP BY strategy_id
                """,
                date_str,
            )

        return {
            "date": date_str,
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "win_rate": win_rate,
            "sharpe_ratio": 0.0,  # Computed by separate analytics job
            "max_drawdown": 0.0,
            "ending_equity": ending_equity,
            "total_fees": total_fees,
            "funding_earned": funding_earned,
            "strategy_breakdown": [
                {
                    "strategy_id": r["strategy_id"],
                    "trades": r["trades"],
                    "pnl": float(r["pnl"]),
                }
                for r in strategy_rows
            ],
        }

    # ── Command: /pause_all ───────────────────────────────────────

    _ALL_STRATEGY_CHANNELS = [
        "strategy:command:funding-arb",
        "strategy:command:adaptive-dca",
    ]

    async def pause_all_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Stop all strategies (soft pause, positions retained)."""
        if not _authorized(update):
            return

        try:
            cmd = json.dumps({"action": "stop", "reason": "telegram_pause"})
            for channel in self._ALL_STRATEGY_CHANNELS:
                await self.redis.publish(channel, cmd)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u23f8 *All strategies paused.*\n"
                "Stop command sent to: `funding-arb`, `adaptive-dca`.\n"
                "_Positions are retained. Use /resume_all to restart._",
                parse_mode="Markdown",
            )
            log.warning(TELEGRAM_COMMAND_RECEIVED, message="/pause_all 명령 전송")

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/pause_all 명령 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to pause strategies. Check system logs."
            )

    # ── Command: /resume_all ──────────────────────────────────────

    async def resume_all_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Resume all strategies (orchestrator will re-issue capital allocation)."""
        if not _authorized(update):
            return

        try:
            # Check if kill switch is active before resuming
            kill_active = await self.redis.get("ce:kill_switch:active")
            if kill_active == "true":
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\U0001f6a8 Kill switch is active. Clear it before resuming strategies.",
                    parse_mode="Markdown",
                )
                return

            cmd = json.dumps({"action": "start", "capital": 0})
            for channel in self._ALL_STRATEGY_CHANNELS:
                await self.redis.publish(channel, cmd)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u25b6\ufe0f *All strategies resumed.*\n"
                "Start command sent to: `funding-arb`, `adaptive-dca`.\n"
                "_Orchestrator will re-issue capital allocation on next cycle._",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/resume_all 명령 전송")

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/resume_all 명령 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to resume strategies. Check system logs."
            )

    # ── Command: /help ───────────────────────────────────────────

    async def help_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Show all available commands as inline buttons."""
        if not _authorized(update):
            return

        keyboard = [
            [
                InlineKeyboardButton("📊 Status", callback_data="cmd:status"),
                InlineKeyboardButton("📈 Report", callback_data="cmd:report"),
            ],
            [
                InlineKeyboardButton("📋 Requests", callback_data="cmd:requests"),
                InlineKeyboardButton("📂 Results", callback_data="cmd:results"),
            ],
            [
                InlineKeyboardButton("⏸ Pause All", callback_data="cmd:pause_all"),
                InlineKeyboardButton("▶️ Resume All", callback_data="cmd:resume_all"),
            ],
            [
                InlineKeyboardButton("🚨 Emergency Close", callback_data="cmd:emergency_close"),
            ],
        ]
        msg = (
            "*CryptoEngine 봇*\n\n"
            "버튼을 탭하거나 직접 입력하세요.\n\n"
            "• `/start <id>` `/stop <id>` `/weight <id> <%>` — 전략 개별 제어\n"
            "• `.md` 파일 전송 → `.request/` 저장\n"
            "• `/results` 목록에서 탭 → 리포트 다운로드"
        )
        await update.effective_message.reply_text(  # type: ignore[union-attr]
            msg,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # ── File Management: Upload to .request/ ──────────────────────

    async def handle_document(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Receive a .md file and save it to the REQUEST_DIR."""
        if not _authorized(update):
            return

        doc = update.effective_message.document  # type: ignore[union-attr]
        if not doc or not doc.file_name:
            return

        if not doc.file_name.lower().endswith(".md"):
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c `.md` 파일만 업로드할 수 있습니다.",
                parse_mode="Markdown",
            )
            return

        try:
            REQUEST_DIR.mkdir(parents=True, exist_ok=True)
            dest = REQUEST_DIR / doc.file_name

            tg_file = await context.bot.get_file(doc.file_id)
            content = await tg_file.download_as_bytearray()
            dest.write_bytes(bytes(content))

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u2705 작업지시서 저장 완료\n"
                f"`{doc.file_name}` → `.request/`\n\n"
                f"_Claude Code에서 다음 명령으로 실행하세요:_\n"
                f"`.request/{doc.file_name} 작업지시서로 개발해줘 멀티에이전트로 병렬로 작업하고 결과 보고서는 .result/ 에 리포트해줘`",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="작업지시서 업로드", filename=doc.file_name)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="작업지시서 저장 실패", filename=doc.file_name)
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\u274c 파일 저장 실패. 로그를 확인하세요."
            )

    # ── Command: /requests ────────────────────────────────────────

    async def requests_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """List all files in REQUEST_DIR."""
        if not _authorized(update):
            return

        try:
            if not REQUEST_DIR.exists():
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\u26a0\ufe0f `.request/` 디렉토리가 없습니다."
                )
                return

            files = sorted(REQUEST_DIR.rglob("*.md"))
            if not files:
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\U0001f4c2 `.request/` 에 파일이 없습니다."
                )
                return

            lines = ["\U0001f4cb *작업지시서 목록* (`.request/`)\n"]
            for i, f in enumerate(files, 1):
                rel = f.relative_to(REQUEST_DIR)
                lines.append(f"`{i}.` `{rel}`")

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\n".join(lines), parse_mode="Markdown"
            )

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/requests 명령 실패")
            await update.effective_message.reply_text("\u274c목록 조회 실패.")  # type: ignore[union-attr]

    # ── Command: /results ─────────────────────────────────────────

    async def results_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """List result reports in RESULT_DIR and store index in Redis."""
        if not _authorized(update):
            return

        try:
            if not RESULT_DIR.exists():
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\u26a0\ufe0f `.result/` 디렉토리가 없습니다."
                )
                return

            files = sorted(RESULT_DIR.rglob("*.md"))
            if not files:
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\U0001f4c2 `.result/` 에 파일이 없습니다."
                )
                return

            # Store index in Redis for /get command (TTL 1 hour)
            index = {str(i): str(f) for i, f in enumerate(files, 1)}
            await self.redis.setex(
                "ce:tg:result_index",
                3600,
                json.dumps(index),
            )

            keyboard = []
            for i, f in enumerate(files, 1):
                rel = f.relative_to(RESULT_DIR)
                size_kb = f.stat().st_size / 1024
                keyboard.append([
                    InlineKeyboardButton(
                        f"📄 {rel}  ({size_kb:.1f} KB)",
                        callback_data=f"get_result:{i}",
                    )
                ])

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                f"\U0001f4ca *결과 리포트 목록* (`.result/`)  — {len(files)}개",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/results 명령 전송", count=len(files))

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/results 명령 실패")
            await update.effective_message.reply_text("\u274c 목록 조회 실패.")  # type: ignore[union-attr]

    # ── Command: /get <number|path> ───────────────────────────────

    async def get_result_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Send a result file to the user. Accepts index number or relative path."""
        if not _authorized(update):
            return

        if not context.args:
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "사용법: `/get <번호>` 또는 `/get <경로>`\n"
                "먼저 `/results` 로 목록을 확인하세요.",
                parse_mode="Markdown",
            )
            return

        arg = context.args[0]

        try:
            target: Path | None = None

            if arg.isdigit():
                # Look up by index stored in Redis
                index_raw = await self.redis.get("ce:tg:result_index")
                if not index_raw:
                    await update.effective_message.reply_text(  # type: ignore[union-attr]
                        "\u26a0\ufe0f 목록이 만료됐습니다. `/results` 를 다시 실행하세요.",
                        parse_mode="Markdown",
                    )
                    return
                index = json.loads(index_raw)
                path_str = index.get(arg)
                if not path_str:
                    await update.effective_message.reply_text(  # type: ignore[union-attr]
                        f"\u274c 번호 `{arg}` 를 찾을 수 없습니다.", parse_mode="Markdown"
                    )
                    return
                target = Path(path_str)
            else:
                # Treat as relative path under RESULT_DIR (prevent traversal)
                candidate = (RESULT_DIR / arg).resolve()
                if not str(candidate).startswith(str(RESULT_DIR.resolve())):
                    await update.effective_message.reply_text("\u274c잘못된 경로입니다.")  # type: ignore[union-attr]
                    return
                target = candidate

            if not target or not target.exists():
                await update.effective_message.reply_text("\u274c파일을 찾을 수 없습니다.")  # type: ignore[union-attr]
                return

            await update.effective_message.reply_document(  # type: ignore[union-attr]
                document=io.BytesIO(target.read_bytes()),
                filename=target.name,
                caption=f"\U0001f4c4 `{target.relative_to(RESULT_DIR)}`",
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_COMMAND_RECEIVED, message="/get 파일 전송", filename=target.name)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/get 명령 실패", arg=arg)
            await update.effective_message.reply_text("\u274c파일 전송 실패.")  # type: ignore[union-attr]

    # ── Callback Query Handler ────────────────────────────────────

    async def handle_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Route inline keyboard button presses."""
        query = update.callback_query
        if not query or not _authorized(update):
            return

        await query.answer()  # dismiss loading spinner

        data = query.data or ""

        if data.startswith("cmd:"):
            cmd = data[4:]
            handler_map = {
                "status": self.status_command,
                "report": self.report_command,
                "requests": self.requests_command,
                "results": self.results_command,
                "pause_all": self.pause_all_command,
                "resume_all": self.resume_all_command,
                "emergency_close": self.emergency_close_command,
            }
            handler = handler_map.get(cmd)
            if handler:
                await handler(update, context)

        elif data.startswith("get_result:"):
            idx = data.split(":", 1)[1]
            context.args = [idx]  # type: ignore[assignment]
            await self.get_result_command(update, context)

    # ── Fallback: unknown command ─────────────────────────────────

    async def unknown_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Reply to unrecognized commands."""
        if not _authorized(update):
            return
        await update.effective_message.reply_text(  # type: ignore[union-attr]
            "❓ 알 수 없는 명령어입니다. `/help` 로 전체 목록을 확인하세요.",
            parse_mode="Markdown",
        )

    # ── Alert Dispatcher ──────────────────────────────────────────

    async def dispatch_alert(
        self,
        bot: Any,
        alert_type: str,
        data: dict[str, Any],
    ) -> None:
        """Send an alert message to the authorized chat."""
        try:
            msg = format_alert(alert_type, data)
            await bot.send_message(
                chat_id=AUTHORIZED_CHAT_ID,
                text=msg,
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_NOTIFICATION_SENT, message="알림 전송 완료", alert_type=alert_type)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="알림 전송 실패", alert_type=alert_type)
