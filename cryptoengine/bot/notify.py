"""Telegram notifier: operator alerts over the plain Bot HTTP API.

Independent of command polling (works from the CLI too). Never raises.
Messages are Korean, parse_mode=HTML. Dedup is in-memory (key -> last sent).
"""

from __future__ import annotations

import asyncio
import html
import re
import time
from typing import Any

import aiohttp
import structlog

log = structlog.get_logger("notify")

DEDUP_WINDOW_S = 120.0
SEND_TIMEOUT_S = 8.0
SEND_ATTEMPTS = 3
RATE_LIMIT_PER_MIN = 20
MAX_LEN = 4096
_API = "https://api.telegram.org"
_TAG_RE = re.compile(r"</?([a-zA-Z]+)[^>]*>")


def esc(v: Any, default: str = "N/A") -> str:
    """Escape for Telegram HTML. Every dynamic string must pass through this."""
    if v is None or v == "":
        return default
    return html.escape(str(v), quote=False)


def truncate_html(msg: str, limit: int = MAX_LEN) -> str:
    """Cut to the Telegram limit without leaving tags open."""
    if len(msg) <= limit:
        return msg
    suffix = "\n…"
    cut = msg[: limit - len(suffix) - 40]
    cut = re.sub(r"<[^>]*$", "", cut)  # drop a half-written tag
    stack: list[str] = []
    for m in _TAG_RE.finditer(cut):
        tag = m.group(1).lower()
        if m.group(0).startswith("</"):
            if stack and stack[-1] == tag:
                stack.pop()
        elif not m.group(0).endswith("/>"):
            stack.append(tag)
    return cut + suffix + "".join(f"</{t}>" for t in reversed(stack))


def _num(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _usd(v: Any, signed: bool = False) -> str:
    n = _num(v)
    if n is None:
        return "N/A"
    return f"${n:+,.2f}" if signed else f"${n:,.2f}"


class TelegramNotifier:
    """Implements ``bot.interfaces.Notifier``."""

    def __init__(self, token: str, chat_id: str, *, api_base: str = _API) -> None:
        self._token = (token or "").strip()
        self._chat_id = str(chat_id or "").strip()
        self._api_base = api_base.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._last_sent: dict[str, float] = {}
        self._sent_times: list[float] = []
        self._warned_disabled = False
        self._clock = time.monotonic

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    # ── core send ───────────────────────────────────────────────────────────

    async def send(
        self,
        html: str,  # noqa: A002 - name fixed by the Notifier protocol
        *,
        dedup_key: str | None = None,
        reply_markup: Any = None,
        silent: bool = False,
        rate_exempt: bool = False,
    ) -> None:
        try:
            await self._send(html, dedup_key, reply_markup, silent, rate_exempt)
        except Exception as exc:  # never raise
            log.warning("telegram_send_failed", error=repr(exc))

    async def _send(self, text: str, dedup_key, reply_markup, silent, rate_exempt) -> None:
        if not self.enabled:
            if not self._warned_disabled:
                self._warned_disabled = True
                log.warning("telegram_disabled", reason="token or chat_id empty")
            return
        now = self._clock()
        if dedup_key:
            last = self._last_sent.get(dedup_key)
            if last is not None and now - last < DEDUP_WINDOW_S:
                return
            self._last_sent[dedup_key] = now
            if len(self._last_sent) > 500:
                self._last_sent = {k: t for k, t in self._last_sent.items() if now - t < DEDUP_WINDOW_S}
        if not rate_exempt:
            self._sent_times = [t for t in self._sent_times if now - t < 60.0]
            if len(self._sent_times) >= RATE_LIMIT_PER_MIN:
                log.warning("telegram_rate_limited", dropped=text[:60])
                return
        self._sent_times.append(now)

        payload: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": truncate_html(text),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": silent,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup.to_dict() if hasattr(reply_markup, "to_dict") else reply_markup

        status, body = await self._post("sendMessage", payload)
        if status == 400 and "parse entities" in str(body.get("description", "")).lower():
            # HTML rejected: resend as plain text rather than losing the alert
            payload.pop("parse_mode")
            payload["text"] = html.unescape(re.sub(r"<[^>]+>", "", text))[:MAX_LEN]
            status, body = await self._post("sendMessage", payload)
        if status == 429:
            wait = min(float(body.get("parameters", {}).get("retry_after", 1)), 30.0)
            await asyncio.sleep(wait)
            status, body = await self._post("sendMessage", payload)
        if status != 200 or not body.get("ok", False):
            log.warning("telegram_send_rejected", status=status, description=body.get("description"))

    async def _post(self, method: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=SEND_TIMEOUT_S))
        last_exc: Exception | None = None
        for attempt in range(SEND_ATTEMPTS):  # a single network hiccup must not lose an alert
            try:
                async with self._session.post(f"{self._api_base}/bot{self._token}/{method}", json=payload) as resp:
                    try:
                        body = await resp.json(content_type=None)
                    except Exception:
                        body = {}
                    return resp.status, body if isinstance(body, dict) else {}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                log.warning("telegram_send_retry", attempt=attempt + 1, error=repr(exc))
        assert last_exc is not None
        raise last_exc

    async def close(self) -> None:
        try:
            if self._session is not None and not self._session.closed:
                await self._session.close()
        except Exception as exc:
            log.warning("telegram_close_failed", error=repr(exc))
        self._session = None

    # ── Notifier protocol ───────────────────────────────────────────────────

    async def entry(self, *, qty: float, price: float, stop_loss: float | None, equity: float | None) -> None:
        notional = (_num(qty) or 0.0) * (_num(price) or 0.0)
        lines = [
            "\U0001f7e2 <b>매수 체결 — 신규 포지션</b>",
            "",
            "Symbol: <code>BTCUSDT</code> (LONG)",
            f"Size: <code>{_num(qty) or 0.0:.6f}</code>",
            f"Price: <code>{_usd(price)}</code>",
            f"Notional: <code>{_usd(notional)}</code>",
        ]
        if stop_loss is not None:
            lines.append(f"Stop Loss: <code>{_usd(stop_loss)}</code>")
        if equity is not None:
            lines.append(f"Equity: <code>{_usd(equity)}</code>")
        await self.send("\n".join(lines), silent=True, rate_exempt=True)

    async def exit(self, *, qty: float, entry_price: float | None, exit_price: float,
                   pnl: float | None, reason: str, hold: str | None) -> None:
        pn = _num(pnl)
        emoji = "✅" if (pn is None or pn >= 0) else "❌"
        if pn is None:
            header = f"{emoji} <b>청산 — Position Closed</b>"
            pnl_line = "Realized PnL: <code>N/A</code>"
        else:
            header = f"{emoji} <b>청산 — {pn:+,.2f} USD</b>"
            pnl_line = f"Realized PnL: <code>{_usd(pn, signed=True)}</code>"
        lines = [
            header,
            "",
            "Symbol: <code>BTCUSDT</code>",
            f"Size: <code>{_num(qty) or 0.0:.6f}</code>",
            f"Entry: <code>{_usd(entry_price)}</code>",
            f"Exit: <code>{_usd(exit_price)}</code>",
            pnl_line,
            f"Reason: <code>{esc(reason)}</code>",
            f"Hold: <code>{esc(hold)}</code>",
        ]
        await self.send("\n".join(lines), silent=True, rate_exempt=True)

    async def order_rejected(self, *, side: str, reason: str, will_retry: bool) -> None:
        retry = "60초 후 재시도합니다." if will_retry else "재시도하지 않습니다."
        text = (
            "⚠️ <b>주문 거부</b>\n\n"
            f"Side: <code>{esc(str(side).upper())}</code>\n"
            f"사유: <code>{esc(reason)}</code>\n"
            f"<i>{retry}</i>"
        )
        await self.send(text, dedup_key=f"order_rejected:{side}:{reason}", silent=True)

    async def kill_switch(self, *, level: str, reason: str, action: str,
                          daily_dd_pct: float, weekly_dd_pct: float) -> None:
        text = (
            f"\U0001f6a8\U0001f6a8\U0001f6a8 <b>KILL SWITCH — {esc(level)}</b> \U0001f6a8\U0001f6a8\U0001f6a8\n\n"
            f"<blockquote>Action: <b>{esc(action)}</b>\n"
            f"Trigger: <code>{esc(reason)}</code></blockquote>\n\n"
            f"Daily DD <code>{_num(daily_dd_pct) or 0.0:.2f}%</code>  |  "
            f"Weekly DD <code>{_num(weekly_dd_pct) or 0.0:.2f}%</code>"
        )
        await self.send(text, rate_exempt=True)

    async def anomaly(self, *, kind: str, message: str) -> None:
        text = (
            "\U0001f6a8 <b>System Anomaly Detected</b>\n\n"
            f"Type: <code>{esc(kind)}</code>\n"
            f"<blockquote expandable>{esc(str(message)[:700])}</blockquote>"
        )
        await self.send(text, dedup_key=f"anomaly:{kind}")
