"""Telegram message formatters for CryptoEngine alerts and reports.

2026-09-19 개편: Markdown v1 → Telegram HTML(parse_mode=HTML)로 전환.
텔레그램 HTML은 <b> <i> <u> <s> <code> <pre> <a> <span class="tg-spoiler">
<blockquote>(+expandable)만 지원하는 제한된 서브셋이다 — 색상·이미지·레이아웃은
지원하지 않는다. "시각적 중요도 구분"은 헤더 굵기·이모지 밀도·인용블록·
무음/소리 알림 전환으로 구현한다 (dispatcher.py의 disable_notification 참고).

모든 동적 문자열(symbol, strategy_id, reason, details 등 외부/사용자 통제
데이터)은 반드시 _esc()를 거친다 — <code>/<pre> 안에서도 이스케이프가
필요하다. 그렇지 않으면 에러 메시지에 `<`/`&`가 섞였을 때 파싱 실패로
알림 자체가 통째로 유실된다(구 Markdown에서 `*`/`` ` ``가 같은 문제를 냈다).
"""

from __future__ import annotations

import html
import math
import re
from datetime import datetime
from typing import Any

# 단일 출처: main.py(설정 오버라이드 가능)와 handlers.py(고정)가 둘 다 여기서 가져온다 —
# 리터럴 "HTML"을 두 파일에 따로 박아두면 나중에 하나만 바뀌는 드리프트가 생긴다.
DEFAULT_PARSE_MODE = "HTML"


# ── HTML escaping ────────────────────────────────────────────────────────────

def _esc(v: Any, default: str = "N/A") -> str:
    """Telegram HTML parse_mode용 이스케이프. &, <, > 만 치환한다(quote=False)."""
    if v is None or v == "":
        return default
    return html.escape(str(v), quote=False)


_TAG_RE = re.compile(r"</?([a-zA-Z]+)[^>]*>")


def _truncate_html(msg: str, limit: int = 4096) -> str:
    """텔레그램 4096자 제한에 맞춰 안전하게 절단한다.

    단순히 msg[:N]으로 자르면 태그 중간이 잘려 parse_mode=HTML에서
    "can't parse entities" 오류로 알림 자체가 유실될 수 있다. 여기서는
    잘라낸 지점까지 열린 태그를 스택으로 추적해 닫아준다.
    """
    if len(msg) <= limit:
        return msg
    suffix = "\n…"
    cut = msg[: limit - len(suffix) - 20]  # 닫는 태그용 여유
    stack: list[str] = []
    for m in _TAG_RE.finditer(cut):
        tag = m.group(1).lower()
        if m.group(0).startswith("</"):
            if stack and stack[-1] == tag:
                stack.pop()
        elif not m.group(0).endswith("/>"):
            stack.append(tag)
    closing = "".join(f"</{t}>" for t in reversed(stack))
    return cut + suffix + closing


# ── Severity / tier model ────────────────────────────────────────────────────

_LEVEL_TO_SEVERITY = {
    "CRITICAL": "critical",
    "FATAL": "critical",
    "ERROR": "error",
    "WARNING": "warning",
    "INFO": "info",
}

TIER_CRITICAL = "critical"
TIER_ERROR = "error"
TIER_TRADE = "trade"
TIER_INFO = "info"

_STATIC_TIER = {
    "kill_switch": TIER_CRITICAL,
    "entry": TIER_TRADE,
    "exit": TIER_TRADE,
    "funding": TIER_INFO,
    "daily_report": TIER_INFO,
}

# 무음(진동/소리 없음) 처리할 등급 — dispatcher가 disable_notification 결정에 사용
_SILENT_TIERS = frozenset({TIER_TRADE, TIER_INFO})


def _derive_severity(data: dict[str, Any]) -> str:
    """payload의 severity를 우선 사용하고, 없으면 level(로그 레벨)로부터 파생한다.

    2026-09-16 사고: shared/logging_config.py의 error_log payload는 severity를
    보내지 않아 _format_anomaly_alert가 항상 "warning"으로 기본값 처리했다 —
    CRITICAL Kill Switch 알림도 "Severity: WARNING"으로 표시된 원인.
    """
    sev = data.get("severity")
    if sev:
        return str(sev).lower()
    return _LEVEL_TO_SEVERITY.get(str(data.get("level", "")).upper(), "warning")


def alert_tier(alert_type: str, data: dict[str, Any]) -> str:
    """alert_type(+data)으로부터 시각/무음 등급을 정한다.

    anomaly는 severity에 따라 critical/error로 갈리고, 그 외 타입은
    정적 매핑을 따른다.
    """
    if alert_type == "anomaly" or alert_type == "error_log":
        return TIER_CRITICAL if _derive_severity(data) == "critical" else TIER_ERROR
    return _STATIC_TIER.get(alert_type, TIER_ERROR)


def alert_is_silent(tier: str) -> bool:
    """해당 등급을 무음(소리/진동 없음)으로 보낼지."""
    return tier in _SILENT_TIERS


# ── Metric helpers ────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    """Convert value to float, returning default on None/error."""
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def compute_sharpe_annualized(daily_returns: list[float]) -> float:
    """Compute annualized Sharpe ratio from a list of daily return fractions.

    Returns 0.0 when there are fewer than 2 data points or stddev is zero.
    """
    if len(daily_returns) < 2:
        return 0.0
    n = len(daily_returns)
    mean = sum(daily_returns) / n
    variance = sum((r - mean) ** 2 for r in daily_returns) / (n - 1)
    if variance <= 0:
        return 0.0
    stddev = math.sqrt(variance)
    sharpe_daily = mean / stddev
    return sharpe_daily * math.sqrt(252)


def compute_max_drawdown(equities: list[float]) -> float:
    """Compute maximum drawdown (%) from a sequence of equity values.

    Returns 0.0 when fewer than 2 values are provided.
    """
    if len(equities) < 2:
        return 0.0
    peak = equities[0]
    max_dd = 0.0
    for eq in equities:
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return max_dd


def format_position(position: dict[str, Any]) -> str:
    """Format a single position for Telegram display."""
    side_emoji = "\U0001f7e2" if position.get("side") == "long" else "\U0001f534"
    pnl = position.get("unrealized_pnl", 0.0)
    pnl_emoji = "✅" if pnl >= 0 else "❌"

    return (
        f"{side_emoji} <b>{_esc(position.get('symbol'))}</b> "
        f"({_esc(str(position.get('side', 'N/A')).upper())})\n"
        f"   Size: <code>{position.get('size', 0):.4f}</code>  |  "
        f"Entry: <code>${position.get('entry_price', 0):,.2f}</code>\n"
        f"   Leverage: <code>{position.get('leverage', 0)}x</code>  |  "
        f"PnL: {pnl_emoji} <code>${pnl:+,.2f}</code>\n"
        f"   Liq: <code>${position.get('liquidation_price', 0):,.2f}</code>"
    )


def format_pnl(portfolio: dict[str, Any]) -> str:
    """Format portfolio PnL summary.

    Optional keys in *portfolio* (added by T-4 enrichment in handlers):
    - ``sharpe_30d``   — annualized Sharpe from last 30 days (float)
    - ``monthly_max_dd`` — max drawdown over last 30 days (float, %)
    """
    equity = _safe_float(portfolio.get("total_equity"))
    unrealized = _safe_float(portfolio.get("unrealized_pnl"))
    realized = _safe_float(portfolio.get("realized_pnl_today"))
    daily_dd = _safe_float(portfolio.get("daily_drawdown"))
    weekly_dd = _safe_float(portfolio.get("weekly_drawdown"))
    total = unrealized + realized

    total_emoji = "\U0001f4c8" if total >= 0 else "\U0001f4c9"

    lines = [
        f"{total_emoji} <b>Portfolio Summary</b>",
        f"",
        f"\U0001f4b0 Equity: <code>${equity:,.2f}</code>",
        f"\U0001f4ca Unrealized PnL: <code>${unrealized:+,.2f}</code>",
        f"✅ Realized Today: <code>${realized:+,.2f}</code>",
        f"\U0001f3af Total PnL: <code>${total:+,.2f}</code>",
        f"",
        f"⚠️ Daily DD: <code>{daily_dd:.2f}%</code>  |  Weekly DD: <code>{weekly_dd:.2f}%</code>",
    ]

    # T-4: Sharpe ratio and monthly max drawdown (only shown when available)
    sharpe_30d = portfolio.get("sharpe_30d")
    monthly_max_dd = portfolio.get("monthly_max_dd")
    if sharpe_30d is not None or monthly_max_dd is not None:
        sharpe_val = _safe_float(sharpe_30d)
        dd_val = _safe_float(monthly_max_dd)
        lines.append(
            f"\U0001f4c9 Sharpe(30d): <code>{sharpe_val:.2f}</code>  |  Monthly Max DD: <code>{dd_val:.2f}%</code>"
        )

    strategies = portfolio.get("strategies", [])
    if strategies:
        lines.append("")
        lines.append("<b>Strategy Breakdown:</b>")
        for s in strategies:
            s_pnl = _safe_float(s.get("current_pnl"))
            s_emoji = "\U0001f7e2" if s_pnl >= 0 else "\U0001f534"
            lines.append(
                f"  {s_emoji} {_esc(s.get('strategy_id'))}: "
                f"<code>${s_pnl:+,.2f}</code> ({s.get('position_count', 0)} pos)"
            )

    kill = portfolio.get("kill_switch_triggered", False)
    if kill:
        lines.append("")
        lines.append("\U0001f6a8 <b>KILL SWITCH ACTIVE</b>")

    return "\n".join(lines)


def format_daily_report(report: dict[str, Any]) -> str:
    """Format end-of-day report for Telegram — 숫자 요약은 <pre> 블록 하나로
    정렬해 작은 표처럼 보이게 한다(텔레그램 HTML에 실제 표 태그는 없음)."""
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

    grid = (
        f"PnL        {pnl:+,.2f} USD\n"
        f"Equity     {equity:,.2f} USD\n"
        f"Trades     {trades:<6d} WinRate {win_rate:.1f}%\n"
        f"Sharpe     {sharpe:<6.2f} MaxDD   {max_dd:.2f}%\n"
        f"Fees       {fees:,.2f} USD\n"
        f"Funding    {funding_earned:+,.2f} USD"
    )

    lines = [
        f"\U0001f4c5 <b>Daily Report — {_esc(date_str)}</b>",
        f"{pnl_emoji}",
        f"<pre>{grid}</pre>",
    ]

    strategy_breakdown = report.get("strategy_breakdown", [])
    if strategy_breakdown:
        lines.append("<b>Per-Strategy:</b>")
        for s in strategy_breakdown:
            s_pnl = s.get("pnl", 0.0)
            s_emoji = "\U0001f7e2" if s_pnl >= 0 else "\U0001f534"
            lines.append(
                f"  {s_emoji} {_esc(s.get('strategy_id', '?'))}: "
                f"<code>${s_pnl:+,.2f}</code> ({s.get('trades', 0)} trades)"
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
        "error_log": _format_anomaly_alert,
        "daily_report": format_daily_report,
    }
    formatter = formatters.get(alert_type, _format_generic_alert)
    return formatter(data)


def _format_entry_alert(data: dict[str, Any]) -> str:
    side = str(data.get("side", "N/A")).lower()
    side_emoji = "\U0001f7e2" if side == "buy" else "\U0001f534"
    notional = data.get("notional_usd")
    notional_line = (
        f"Notional: <code>${_safe_float(notional):,.2f}</code>\n" if notional is not None else ""
    )
    return (
        f"{side_emoji} <b>매수 체결 — New Position</b>\n"
        f"\n"
        f"Strategy: <code>{_esc(data.get('strategy_id'))}</code>\n"
        f"Symbol: <code>{_esc(data.get('symbol'))}</code>\n"
        f"Side: <code>{_esc(side.upper())}</code>\n"
        f"Size: <code>{_safe_float(data.get('quantity')):.6f}</code>\n"
        f"Price: <code>${_safe_float(data.get('filled_price')):,.2f}</code>\n"
        f"{notional_line}"
        f"Fee: <code>${_safe_float(data.get('fee')):.4f}</code>"
    )


def _format_exit_alert(data: dict[str, Any]) -> str:
    pnl = data.get("realized_pnl")
    has_pnl = pnl is not None
    pnl_val = _safe_float(pnl)
    pnl_emoji = "✅" if (not has_pnl or pnl_val >= 0) else "❌"
    if has_pnl:
        pct = data.get("realized_pnl_pct")
        pct_str = f" <code>({_safe_float(pct):+.2f}%)</code>" if pct is not None else ""
        header = f"{pnl_emoji} <b>청산 — {pnl_val:+,.2f} USD (근사)</b>{pct_str}"
        pnl_line = f"Realized PnL: <code>${pnl_val:+,.2f}</code> (진입수수료 미반영 근사치)\n"
    else:
        header = f"{pnl_emoji} <b>매도 체결 — Position Closed</b>"
        pnl_line = "Realized PnL: <code>N/A</code>\n"
    return (
        f"{header}\n"
        f"\n"
        f"Strategy: <code>{_esc(data.get('strategy_id'))}</code>\n"
        f"Symbol: <code>{_esc(data.get('symbol'))}</code>\n"
        f"Exit Price: <code>${_safe_float(data.get('filled_price')):,.2f}</code>\n"
        f"{pnl_line}"
        f"Hold Duration: <code>{_esc(data.get('hold_duration'))}</code>"
    )


def _format_funding_alert(data: dict[str, Any]) -> str:
    rate = _safe_float(data.get("rate"))
    rate_annual = rate * 3 * 365 * 100  # 8h intervals
    payment = _safe_float(data.get("payment"))
    payment_emoji = "\U0001f4b5" if payment >= 0 else "\U0001f4b8"
    return (
        f"{payment_emoji} <b>Funding Payment</b>\n"
        f"\n"
        f"Symbol: <code>{_esc(data.get('symbol'))}</code>\n"
        f"Rate: <code>{rate:.6f}</code> ({rate_annual:+.1f}% annualized)\n"
        f"Payment: <code>${payment:+,.4f}</code>\n"
        f"Next Funding: <code>{_esc(data.get('next_funding_time'))}</code>"
    )


_KILL_LEVEL_NAMES = {
    1: "Strategy Pause",
    2: "Reduce Exposure / Portfolio Halt",
    3: "System Shutdown",
    4: "Manual Emergency Close",
}


def _format_kill_switch_alert(data: dict[str, Any]) -> str:
    level = data.get("level", 0)
    action = data.get("action") or _KILL_LEVEL_NAMES.get(int(level) if str(level).isdigit() else 0, "Unknown")
    return (
        f"\U0001f6a8\U0001f6a8\U0001f6a8 <b>KILL SWITCH — LEVEL {_esc(level)}</b> \U0001f6a8\U0001f6a8\U0001f6a8\n"
        f"\n"
        f"<blockquote>Action: <b>{_esc(action)}</b>\n"
        f"Trigger: <code>{_esc(data.get('trigger_reason'))}</code></blockquote>\n"
        f"\n"
        f"Daily DD <code>{_safe_float(data.get('daily_drawdown')):.2f}%</code>  |  "
        f"Weekly DD <code>{_safe_float(data.get('weekly_drawdown')):.2f}%</code>\n"
        f"<i>자동 대응이 시작되었습니다.</i>"
    )


def _format_anomaly_alert(data: dict[str, Any]) -> str:
    severity = _derive_severity(data)
    sev_emoji = "\U0001f6a8" if severity == "critical" else ("⚠️" if severity in ("warning", "error") else "ℹ️")
    # Support both explicit anomaly format and error_log format from logging_config.py
    component = data.get("component") or data.get("service", "N/A")
    anomaly_type = data.get("anomaly_type") or data.get("event") or data.get("type", "N/A")
    details = data.get("details") or data.get("message", "N/A")
    details = str(details)[:700]
    return (
        f"{sev_emoji} <b>System Anomaly Detected</b>\n"
        f"\n"
        f"Component: <code>{_esc(component)}</code>\n"
        f"Type: <code>{_esc(anomaly_type)}</code>\n"
        f"<blockquote expandable>{_esc(details)}</blockquote>\n"
        f"Severity: <code>{_esc(severity.upper())}</code>"
    )


def _format_generic_alert(data: dict[str, Any]) -> str:
    return (
        f"\U0001f514 <b>Alert</b>\n"
        f"\n"
        f"Type: <code>{_esc(data.get('type', 'unknown'))}</code>\n"
        f"Message: <code>{_esc(data.get('message', 'No details'))}</code>"
    )
