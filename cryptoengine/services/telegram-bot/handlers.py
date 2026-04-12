"""Telegram bot command handlers for CryptoEngine."""

from __future__ import annotations

import asyncio
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

from dispatcher import AlertDispatcher
from formatters import (
    compute_max_drawdown,
    compute_sharpe_annualized,
    format_alert,
    format_daily_report,
    format_pnl,
    format_position,
)
from shared.kill_switch import (
    KILL_SWITCH_ACK_CHANNEL,
    KILL_SWITCH_ACK_TIME_KEY,
    KILL_SWITCH_ACK_TIMEOUT_SECONDS,
    KILL_SWITCH_ACK_MAX_RETRIES,
)
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

    def __init__(
        self,
        redis_client: aioredis.Redis,
        db_pool: asyncpg.Pool,
        dispatcher: AlertDispatcher | None = None,
    ) -> None:
        self.redis = redis_client
        self.db_pool = db_pool
        # AlertDispatcher is injected from main.py after bot is initialized.
        # Until set, dispatch_alert falls back to the legacy inline send.
        self._dispatcher: AlertDispatcher | None = dispatcher

    def set_dispatcher(self, dispatcher: AlertDispatcher) -> None:
        """Attach an AlertDispatcher after the bot object is available."""
        self._dispatcher = dispatcher

    # ── T-4: Sharpe + monthly DD query ───────────────────────────

    async def _fetch_30d_metrics(self) -> dict[str, float]:
        """Query last-30-day daily returns from portfolio_snapshots.

        Returns dict with ``sharpe_30d`` and ``monthly_max_dd`` keys,
        or empty dict when data is unavailable.
        """
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT snapshot_at, total_equity
                    FROM portfolio_snapshots
                    WHERE snapshot_at >= NOW() - INTERVAL '30 days'
                    ORDER BY snapshot_at ASC
                    """,
                )
            if len(rows) < 2:
                return {}

            equities = [float(r["total_equity"]) for r in rows]
            # Compute daily returns from hourly/periodic snapshots
            daily_returns: list[float] = []
            for i in range(1, len(equities)):
                if equities[i - 1] > 0:
                    daily_returns.append((equities[i] - equities[i - 1]) / equities[i - 1])

            sharpe = compute_sharpe_annualized(daily_returns) if daily_returns else 0.0
            max_dd = compute_max_drawdown(equities)
            return {"sharpe_30d": sharpe, "monthly_max_dd": max_dd}

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="30일 지표 조회 실패")
            return {}

    # ── Command: /status ─────────────────────────────────────────

    async def status_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Full position + PnL summary with Sharpe and monthly drawdown (T-4)."""
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

            # T-4: enrich portfolio dict with 30-day Sharpe and monthly max DD
            metrics = await self._fetch_30d_metrics()
            portfolio.update(metrics)

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
        """Close all positions immediately (Level 4 Kill Switch).

        프로토콜:
        1. ce:kill_switch PUBLISH + ce:kill_switch:active SET (기존 로직, 즉시 실행)
        2. 5초 대기 후 ce:kill_switch:ack_time 키 확인
        3. ACK 수신 시 확인 메시지 전송
        4. ACK 미수신 시 경고 메시지 + 최대 3회 재전송 시도
        """
        if not _authorized(update):
            return

        triggered_by = str(update.effective_user.id if update.effective_user else "unknown")

        try:
            kill_switch_msg = json.dumps(
                {
                    "level": 4,
                    "trigger_reason": "manual_telegram_command",
                    "triggered_by": triggered_by,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

            # ── Step 1: Kill Switch 즉시 발동 (fire-and-forget 유지) ──
            await self.redis.publish("ce:kill_switch", kill_switch_msg)
            await self.redis.set("ce:kill_switch:active", "true")

            # ACK 대기 전 이전 ACK 타임스탬프를 스냅샷으로 기록
            prev_ack_time = await self.redis.get(KILL_SWITCH_ACK_TIME_KEY)

            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\U0001f6a8 *EMERGENCY CLOSE INITIATED*\n\n"
                "Level 4 Kill Switch activated.\n"
                "All positions will be market-closed immediately.\n"
                "All strategies will be halted.\n\n"
                f"_ACK 확인 중... ({KILL_SWITCH_ACK_TIMEOUT_SECONDS}초 대기)_",
                parse_mode="Markdown",
            )
            log.warning(
                KILL_SWITCH_TRIGGERED,
                message="긴급 청산 발동",
                triggered_by=triggered_by,
            )

            # ── Step 2: ACK 대기 및 확인 ──
            ack_received = await self._wait_for_kill_switch_ack(prev_ack_time)

            if ack_received:
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    "\u2705 *Kill Switch 수신 확인*\n\n"
                    "오케스트레이터가 Kill Switch를 수신하고 청산을 시작했습니다.\n"
                    "_Use /status to monitor closure progress._",
                    parse_mode="Markdown",
                )
                log.info(
                    KILL_SWITCH_ACK_SENT,
                    message="kill switch ACK 수신 확인",
                    triggered_by=triggered_by,
                )
            else:
                # ACK 미수신 — 최대 KILL_SWITCH_ACK_MAX_RETRIES회 재전송 시도
                retry_success = False
                for attempt in range(1, KILL_SWITCH_ACK_MAX_RETRIES + 1):
                    log.error(
                        KILL_SWITCH_ACK_MISSING,
                        message="kill switch ACK 미수신, 재전송 시도",
                        attempt=attempt,
                        max_retries=KILL_SWITCH_ACK_MAX_RETRIES,
                        triggered_by=triggered_by,
                    )
                    await update.effective_message.reply_text(  # type: ignore[union-attr]
                        f"\u26a0\ufe0f *ACK 미수신 — 재전송 중 ({attempt}/{KILL_SWITCH_ACK_MAX_RETRIES})*\n\n"
                        "오케스트레이터 응답 없음. Kill Switch는 이미 SET 상태입니다.\n"
                        "재전송 중...",
                        parse_mode="Markdown",
                    )

                    prev_ack_time = await self.redis.get(KILL_SWITCH_ACK_TIME_KEY)
                    await self.redis.publish("ce:kill_switch", kill_switch_msg)
                    ack_received = await self._wait_for_kill_switch_ack(prev_ack_time)

                    if ack_received:
                        retry_success = True
                        await update.effective_message.reply_text(  # type: ignore[union-attr]
                            f"\u2705 *Kill Switch 수신 확인 (재전송 {attempt}회)*\n\n"
                            "오케스트레이터가 Kill Switch를 수신하고 청산을 시작했습니다.\n"
                            "_Use /status to monitor closure progress._",
                            parse_mode="Markdown",
                        )
                        log.info(
                            KILL_SWITCH_ACK_SENT,
                            message="kill switch ACK 수신 확인 (재시도 성공)",
                            attempt=attempt,
                            triggered_by=triggered_by,
                        )
                        break

                if not retry_success:
                    log.error(
                        KILL_SWITCH_ACK_MISSING,
                        message="kill switch ACK 최종 미수신 — 수동 확인 필요",
                        max_retries=KILL_SWITCH_ACK_MAX_RETRIES,
                        triggered_by=triggered_by,
                    )
                    await update.effective_message.reply_text(  # type: ignore[union-attr]
                        "\u26a0\ufe0f *ACK 미수신 — 수동 확인 필요*\n\n"
                        "Kill Switch는 SET 상태이지만, 오케스트레이터 응답이 없습니다.\n\n"
                        "즉시 수행하세요:\n"
                        "\u2022 `/status` 로 포지션 확인\n"
                        "\u2022 오케스트레이터 서비스 상태 확인\n"
                        "\u2022 필요 시 수동 청산",
                        parse_mode="Markdown",
                    )

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="긴급 청산 실패")
            await update.effective_message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to trigger emergency close! Check system immediately."
            )

    async def _wait_for_kill_switch_ack(self, prev_ack_time: str | None) -> bool:
        """ce:kill_switch:ack_time 키가 prev_ack_time 이후 갱신됐는지 폴링 확인.

        KILL_SWITCH_ACK_TIMEOUT_SECONDS 동안 0.5초 간격으로 폴링한다.
        ACK는 오케스트레이터가 kill switch 수신 즉시 SET하므로, 키가
        갱신되면 True를 반환한다. 타임아웃 시 False 반환.
        """
        poll_interval = 0.5
        elapsed = 0.0

        while elapsed < KILL_SWITCH_ACK_TIMEOUT_SECONDS:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            current_ack_time = await self.redis.get(KILL_SWITCH_ACK_TIME_KEY)
            if current_ack_time and current_ack_time != prev_ack_time:
                return True

        return False

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
        """Build daily report from database.

        T-3: funding_earned includes both DB funding_payments AND any amount
        accumulated in the in-memory FundingAccumulator that has not yet been
        persisted (e.g. received within the current batch window).
        """
        # asyncpg requires datetime.date objects for date parameters, not strings
        from datetime import date as _date
        date_obj = _date.fromisoformat(date_str)

        async with self.db_pool.acquire() as conn:
            # Get trade summary for the day
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total_trades,
                    COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
                    COALESCE(SUM(pnl), 0) AS total_pnl,
                    COALESCE(SUM(fee), 0) AS total_fees
                FROM trades
                WHERE DATE(COALESCE(filled_at, created_at)) = $1
                  AND status = 'filled'
                """,
                date_obj,
            )

            total_trades = row["total_trades"] if row else 0
            wins = row["wins"] if row else 0
            total_pnl = float(row["total_pnl"]) if row else 0.0
            total_fees = float(row["total_fees"]) if row else 0.0
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

            # Get funding earned from DB
            funding_row = await conn.fetchrow(
                """
                SELECT COALESCE(SUM(payment), 0) AS funding_earned
                FROM funding_payments
                WHERE DATE(collected_at) = $1
                """,
                date_obj,
            )
            funding_db = float(funding_row["funding_earned"]) if funding_row else 0.0

            # T-3: Add in-memory accumulated funding (not yet in DB)
            funding_accumulated = 0.0
            if self._dispatcher is not None:
                funding_accumulated = self._dispatcher.funding.get_daily_total(date_str)
            funding_earned = funding_db + funding_accumulated

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
                    COALESCE(SUM(pnl), 0) AS pnl
                FROM trades
                WHERE DATE(COALESCE(filled_at, created_at)) = $1
                  AND status = 'filled'
                GROUP BY strategy_id
                """,
                date_obj,
            )

        # T-4: compute Sharpe and max DD for the report period
        metrics = await self._fetch_30d_metrics()

        # After including in daily report, reset the daily accumulator
        if self._dispatcher is not None:
            self._dispatcher.funding.reset_day(date_str)

        return {
            "date": date_str,
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "win_rate": win_rate,
            "sharpe_ratio": metrics.get("sharpe_30d", 0.0),
            "max_drawdown": metrics.get("monthly_max_dd", 0.0),
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
                f"`.request/{doc.file_name} 작업지시서로 개발해줘 멀티에이전트로 병렬로 작업하고 결과 보고서는 .result/ 에 리포트해줘 생각이 많이 필요한 작업은 opus가 하고 단순 작업은 반드시 sonnet 모델로 해줘`",
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

    # ── Command: /results  (폴더 탐색기) ──────────────────────────

    async def results_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Show root of RESULT_DIR as a folder browser."""
        if not _authorized(update):
            return
        await self._show_result_dir(update, context, rel_path="")

    async def _show_result_dir(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        rel_path: str,
    ) -> None:
        """Render folder contents as an inline keyboard (folders + .md files).

        Edits the existing message when called from a callback query,
        otherwise replies as a new message.
        """
        try:
            result_root = RESULT_DIR.resolve()
            current_dir = (RESULT_DIR / rel_path).resolve() if rel_path else result_root

            # Path traversal guard
            if not str(current_dir).startswith(str(result_root)):
                await update.effective_message.reply_text("\u274c 잘못된 경로입니다.")  # type: ignore[union-attr]
                return

            if not current_dir.exists():
                await update.effective_message.reply_text("\u274c 디렉토리를 찾을 수 없습니다.")  # type: ignore[union-attr]
                return

            # Refresh global index (all .md files) in Redis
            all_files = sorted(RESULT_DIR.rglob("*.md"))
            global_index = {str(i): str(f) for i, f in enumerate(all_files, 1)}
            path_to_idx: dict[str, str] = {str(f): str(i) for i, f in enumerate(all_files, 1)}
            await self.redis.setex("ce:tg:result_index", 3600, json.dumps(global_index))

            # Items in current directory only (not recursive)
            subdirs = sorted(
                [d for d in current_dir.iterdir() if d.is_dir()],
                key=lambda d: d.name,
            )
            local_files = sorted(
                [f for f in current_dir.iterdir() if f.is_file() and f.suffix == ".md"],
                key=lambda f: f.name,
            )

            keyboard: list[list[InlineKeyboardButton]] = []

            # ⬆️ Back button (shown for every non-root directory)
            if rel_path:
                parent_rel = str(Path(rel_path).parent)
                parent_cb = "" if parent_rel == "." else parent_rel
                keyboard.append([
                    InlineKeyboardButton("⬆️ 위로", callback_data=f"browse_dir:{parent_cb}")
                ])

            # 📁 Subdirectory buttons
            for d in subdirs:
                d_rel = str(d.relative_to(result_root))
                cb = f"browse_dir:{d_rel}"
                if len(cb.encode()) <= 64:
                    keyboard.append([
                        InlineKeyboardButton(f"📁 {d.name}/", callback_data=cb)
                    ])

            # 📄 File buttons (reference global index for /get compatibility)
            for f in local_files:
                idx = path_to_idx.get(str(f), "0")
                size_kb = f.stat().st_size / 1024
                keyboard.append([
                    InlineKeyboardButton(
                        f"📄 {f.name}  ({size_kb:.1f} KB)",
                        callback_data=f"get_result:{idx}",
                    )
                ])

            display_path = f"`.result/{rel_path}`" if rel_path else "`.result/`"
            header = (
                f"\U0001f4ca *결과 리포트* {display_path}\n"
                f"📁 폴더 {len(subdirs)}개  |  📄 파일 {len(local_files)}개"
                + (f"  |  전체 {len(all_files)}개" if not rel_path else "")
            )

            query = update.callback_query
            if query:
                await query.edit_message_text(
                    header,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            else:
                await update.effective_message.reply_text(  # type: ignore[union-attr]
                    header,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )

            log.info(
                TELEGRAM_COMMAND_RECEIVED,
                message="/results 폴더 탐색",
                path=rel_path or "/",
                subdirs=len(subdirs),
                files=len(local_files),
            )

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="/results 폴더 탐색 실패", rel_path=rel_path)
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

        elif data.startswith("browse_dir:"):
            rel = data[len("browse_dir:"):]
            await self._show_result_dir(update, context, rel_path=rel)

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
        """Route an alert through AlertDispatcher (T-1, T-3).

        Falls back to direct inline send when no dispatcher is configured.
        """
        if self._dispatcher is not None:
            await self._dispatcher.dispatch(alert_type, data)
            return

        # Legacy fallback (no dispatcher configured)
        try:
            msg = format_alert(alert_type, data)
            await bot.send_message(
                chat_id=AUTHORIZED_CHAT_ID,
                text=msg,
                parse_mode="Markdown",
            )
            log.info(TELEGRAM_NOTIFICATION_SENT, message="알림 전송 완료 (fallback)", alert_type=alert_type)
        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="알림 전송 실패", alert_type=alert_type)
