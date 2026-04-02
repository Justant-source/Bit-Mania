"""Telegram message formatters for CryptoEngine alerts and reports."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def format_position(position: dict[str, Any]) -> str:
    """Format a single position for Telegram display."""
    side_emoji = "\U0001f7e2" if position.get("side") == "long" else "\U0001f534"
    pnl = position.get("unrealized_pnl", 0.0)
    pnl_emoji = "\u2705" if pnl >= 0 else "\u274c"

    return (
        f"{side_emoji} *{position.get('symbol', 'N/A')}* "
        f"({position.get('side', 'N/A').upper()})\n"
        f"   Size: `{position.get('size', 0):.4f}`  |  "
        f"Entry: `${position.get('entry_price', 0):,.2f}`\n"
        f"   Leverage: `{position.get('leverage', 0)}x`  |  "
        f"PnL: {pnl_emoji} `${pnl:+,.2f}`\n"
        f"   Liq: `${position.get('liquidation_price', 0):,.2f}`"
    )


def format_pnl(portfolio: dict[str, Any]) -> str:
    """Format portfolio PnL summary."""
    equity = portfolio.get("total_equity", 0.0)
    unrealized = portfolio.get("unrealized_pnl", 0.0)
    realized = portfolio.get("realized_pnl_today", 0.0)
    daily_dd = portfolio.get("daily_drawdown", 0.0)
    weekly_dd = portfolio.get("weekly_drawdown", 0.0)
    total = unrealized + realized

    total_emoji = "\U0001f4c8" if total >= 0 else "\U0001f4c9"

    lines = [
        f"{total_emoji} *Portfolio Summary*",
        f"",
        f"\U0001f4b0 Equity: `${equity:,.2f}`",
        f"\U0001f4ca Unrealized PnL: `${unrealized:+,.2f}`",
        f"\u2705 Realized Today: `${realized:+,.2f}`",
        f"\U0001f3af Total PnL: `${total:+,.2f}`",
        f"",
        f"\u26a0\ufe0f Daily DD: `{daily_dd:.2f}%`  |  Weekly DD: `{weekly_dd:.2f}%`",
    ]

    strategies = portfolio.get("strategies", [])
    if strategies:
        lines.append("")
        lines.append("*Strategy Breakdown:*")
        for s in strategies:
            s_pnl = s.get("current_pnl", 0.0)
            s_emoji = "\U0001f7e2" if s_pnl >= 0 else "\U0001f534"
            lines.append(
                f"  {s_emoji} {s.get('strategy_id', 'N/A')}: "
                f"`${s_pnl:+,.2f}` ({s.get('position_count', 0)} pos)"
            )

    kill = portfolio.get("kill_switch_triggered", False)
    if kill:
        lines.append("")
        lines.append("\U0001f6a8 *KILL SWITCH ACTIVE*")

    return "\n".join(lines)


def format_daily_report(report: dict[str, Any]) -> str:
    """Format end-of-day report for Telegram."""
    date_str = report.get("date", datetime.utcnow().strftime("%Y-%m-%d"))
    pnl = report.get("total_pnl", 0.0)
    trades = report.get("total_trades", 0)
    win_rate = report.get("win_rate", 0.0)
    sharpe = report.get("sharpe_ratio", 0.0)
    max_dd = report.get("max_drawdown", 0.0)
    equity = report.get("ending_equity", 0.0)
    fees = report.get("total_fees", 0.0)
    funding_earned = report.get("funding_earned", 0.0)

    pnl_emoji = "\U0001f4c8" if pnl >= 0 else "\U0001f4c9"
    wr_emoji = "\U0001f3af" if win_rate >= 50 else "\u26a0\ufe0f"

    lines = [
        f"\U0001f4c5 *Daily Report — {date_str}*",
        f"{'=' * 30}",
        f"",
        f"{pnl_emoji} Net PnL: `${pnl:+,.2f}`",
        f"\U0001f4b0 Ending Equity: `${equity:,.2f}`",
        f"",
        f"\U0001f504 Trades: `{trades}`  |  {wr_emoji} Win Rate: `{win_rate:.1f}%`",
        f"\U0001f4ca Sharpe: `{sharpe:.2f}`  |  Max DD: `{max_dd:.2f}%`",
        f"",
        f"\U0001f4b8 Fees Paid: `${fees:,.2f}`",
        f"\U0001f4b5 Funding Earned: `${funding_earned:+,.2f}`",
    ]

    strategy_breakdown = report.get("strategy_breakdown", [])
    if strategy_breakdown:
        lines.append("")
        lines.append("*Per-Strategy:*")
        for s in strategy_breakdown:
            s_pnl = s.get("pnl", 0.0)
            s_emoji = "\U0001f7e2" if s_pnl >= 0 else "\U0001f534"
            lines.append(
                f"  {s_emoji} {s.get('strategy_id', '?')}: "
                f"`${s_pnl:+,.2f}` ({s.get('trades', 0)} trades)"
            )

    return "\n".join(lines)


def format_alert(alert_type: str, data: dict[str, Any]) -> str:
    """Format an alert message based on type."""
    formatters = {
        "entry": _format_entry_alert,
        "exit": _format_exit_alert,
        "funding": _format_funding_alert,
        "kill_switch": _format_kill_switch_alert,
        "anomaly": _format_anomaly_alert,
    }
    formatter = formatters.get(alert_type, _format_generic_alert)
    return formatter(data)


def _format_entry_alert(data: dict[str, Any]) -> str:
    side_emoji = "\U0001f7e2" if data.get("side") == "buy" else "\U0001f534"
    return (
        f"{side_emoji} *New Position Opened*\n"
        f"\n"
        f"Strategy: `{data.get('strategy_id', 'N/A')}`\n"
        f"Symbol: `{data.get('symbol', 'N/A')}`\n"
        f"Side: `{data.get('side', 'N/A').upper()}`\n"
        f"Size: `{data.get('quantity', 0):.4f}`\n"
        f"Price: `${data.get('filled_price', 0):,.2f}`\n"
        f"Fee: `${data.get('fee', 0):.4f}`"
    )


def _format_exit_alert(data: dict[str, Any]) -> str:
    pnl = data.get("realized_pnl", 0.0)
    pnl_emoji = "\u2705" if pnl >= 0 else "\u274c"
    return (
        f"{pnl_emoji} *Position Closed*\n"
        f"\n"
        f"Strategy: `{data.get('strategy_id', 'N/A')}`\n"
        f"Symbol: `{data.get('symbol', 'N/A')}`\n"
        f"Exit Price: `${data.get('filled_price', 0):,.2f}`\n"
        f"Realized PnL: `${pnl:+,.2f}`\n"
        f"Hold Duration: `{data.get('hold_duration', 'N/A')}`"
    )


def _format_funding_alert(data: dict[str, Any]) -> str:
    rate = data.get("rate", 0.0)
    rate_annual = rate * 3 * 365 * 100  # 8h intervals
    payment = data.get("payment", 0.0)
    payment_emoji = "\U0001f4b5" if payment >= 0 else "\U0001f4b8"
    return (
        f"{payment_emoji} *Funding Payment*\n"
        f"\n"
        f"Symbol: `{data.get('symbol', 'N/A')}`\n"
        f"Rate: `{rate:.6f}` ({rate_annual:+.1f}% annualized)\n"
        f"Payment: `${payment:+,.4f}`\n"
        f"Next Funding: `{data.get('next_funding_time', 'N/A')}`"
    )


def _format_kill_switch_alert(data: dict[str, Any]) -> str:
    level = data.get("level", 0)
    level_names = {
        1: "Strategy Pause",
        2: "Reduce Exposure",
        3: "Close Losing Positions",
        4: "EMERGENCY CLOSE ALL",
    }
    return (
        f"\U0001f6a8\U0001f6a8\U0001f6a8 *KILL SWITCH — Level {level}*\n"
        f"\n"
        f"Action: `{level_names.get(level, 'Unknown')}`\n"
        f"Trigger: `{data.get('trigger_reason', 'N/A')}`\n"
        f"Daily DD: `{data.get('daily_drawdown', 0):.2f}%`\n"
        f"Weekly DD: `{data.get('weekly_drawdown', 0):.2f}%`\n"
        f"\n"
        f"_Automated response initiated._"
    )


def _format_anomaly_alert(data: dict[str, Any]) -> str:
    severity = data.get("severity", "warning")
    sev_emoji = "\u26a0\ufe0f" if severity == "warning" else "\U0001f6a8"
    return (
        f"{sev_emoji} *System Anomaly Detected*\n"
        f"\n"
        f"Component: `{data.get('component', 'N/A')}`\n"
        f"Type: `{data.get('anomaly_type', 'N/A')}`\n"
        f"Details: `{data.get('details', 'N/A')}`\n"
        f"Severity: `{severity.upper()}`"
    )


def _format_generic_alert(data: dict[str, Any]) -> str:
    return (
        f"\U0001f514 *Alert*\n"
        f"\n"
        f"Type: `{data.get('type', 'unknown')}`\n"
        f"Message: `{data.get('message', 'No details')}`"
    )
