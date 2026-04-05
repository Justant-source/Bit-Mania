"""Telegram bot command handlers for CryptoEngine."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog
from telegram import Update
from telegram.ext import ContextTypes

from formatters import format_alert, format_daily_report, format_pnl, format_position

log = structlog.get_logger(__name__)

# Authorized chat ID — only respond to messages from this chat
AUTHORIZED_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))


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
                await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                "\n".join(msg_parts),
                parse_mode="Markdown",
            )
            log.info("status_command_sent", chat_id=update.effective_chat.id)

        except Exception:
            log.exception("status_command_failed")
            await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                "\U0001f6a8 *EMERGENCY CLOSE INITIATED*\n\n"
                "Level 4 Kill Switch activated.\n"
                "All positions will be market-closed immediately.\n"
                "All strategies will be halted.\n\n"
                "_Use /status to monitor closure progress._",
                parse_mode="Markdown",
            )
            log.warning(
                "emergency_close_triggered",
                triggered_by=update.effective_user.id if update.effective_user else "unknown",
            )

        except Exception:
            log.exception("emergency_close_failed")
            await update.message.reply_text(  # type: ignore[union-attr]
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
            await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                f"\u23f9 Stop command sent to strategy `{strategy_id}`.\n"
                f"_Use /status to confirm._",
                parse_mode="Markdown",
            )
            log.info("stop_command_sent", strategy_id=strategy_id)

        except Exception:
            log.exception("stop_command_failed", strategy_id=strategy_id)
            await update.message.reply_text(  # type: ignore[union-attr]
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
            await update.message.reply_text(  # type: ignore[union-attr]
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
                await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                f"\u25b6\ufe0f Start command sent to strategy `{strategy_id}`.\n"
                f"_Use /status to confirm._",
                parse_mode="Markdown",
            )
            log.info("start_command_sent", strategy_id=strategy_id)

        except Exception:
            log.exception("start_command_failed", strategy_id=strategy_id)
            await update.message.reply_text(  # type: ignore[union-attr]
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
            await update.message.reply_text(  # type: ignore[union-attr]
                "Usage: `/weight <strategy_id> <percentage>`\n"
                "Example: `/weight funding_arb 30`",
                parse_mode="Markdown",
            )
            return

        strategy_id = context.args[0]
        try:
            weight_pct = float(context.args[1])
        except ValueError:
            await update.message.reply_text(  # type: ignore[union-attr]
                "\u274c Invalid percentage. Must be a number (e.g., 30)."
            )
            return

        if not (0.0 <= weight_pct <= 100.0):
            await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                f"\u2696\ufe0f Weight for `{strategy_id}` set to `{weight_pct}%`.\n"
                f"_Orchestrator will apply on next rebalance cycle._",
                parse_mode="Markdown",
            )
            log.info("weight_command_sent", strategy_id=strategy_id, weight_pct=weight_pct)

        except Exception:
            log.exception("weight_command_failed", strategy_id=strategy_id)
            await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                msg, parse_mode="Markdown"
            )
            log.info("report_command_sent", date=today)

        except Exception:
            log.exception("report_command_failed")
            await update.message.reply_text(  # type: ignore[union-attr]
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

            await update.message.reply_text(  # type: ignore[union-attr]
                "\u23f8 *All strategies paused.*\n"
                "Stop command sent to: `funding-arb`, `adaptive-dca`.\n"
                "_Positions are retained. Use /resume_all to restart._",
                parse_mode="Markdown",
            )
            log.warning("pause_all_command_sent")

        except Exception:
            log.exception("pause_all_command_failed")
            await update.message.reply_text(  # type: ignore[union-attr]
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
                await update.message.reply_text(  # type: ignore[union-attr]
                    "\U0001f6a8 Kill switch is active. Clear it before resuming strategies.",
                    parse_mode="Markdown",
                )
                return

            cmd = json.dumps({"action": "start", "capital": 0})
            for channel in self._ALL_STRATEGY_CHANNELS:
                await self.redis.publish(channel, cmd)

            await update.message.reply_text(  # type: ignore[union-attr]
                "\u25b6\ufe0f *All strategies resumed.*\n"
                "Start command sent to: `funding-arb`, `adaptive-dca`.\n"
                "_Orchestrator will re-issue capital allocation on next cycle._",
                parse_mode="Markdown",
            )
            log.info("resume_all_command_sent")

        except Exception:
            log.exception("resume_all_command_failed")
            await update.message.reply_text(  # type: ignore[union-attr]
                "\u274c Failed to resume strategies. Check system logs."
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
            log.info("alert_dispatched", alert_type=alert_type)
        except Exception:
            log.exception("alert_dispatch_failed", alert_type=alert_type)
