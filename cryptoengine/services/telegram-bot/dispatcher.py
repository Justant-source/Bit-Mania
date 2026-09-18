"""Alert dispatcher with rate limiting, batching, and funding accumulator.

T-1: AlertDispatcher — batch_window_seconds, max_messages_per_minute, CRITICAL bypass
T-3: FundingAccumulator — normal funding buffered, anomalies sent immediately
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import structlog

from formatters import alert_is_silent, alert_tier, format_alert, _truncate_html
from shared.log_events import (
    TELEGRAM_NOTIFICATION_SENT,
    SERVICE_HEALTH_FAIL,
    FUNDING_COLLECTED,
)

log = structlog.get_logger(__name__)

# Alert priority: CRITICAL / ERROR always bypass batching (and rate limit)
_HIGH_PRIORITY_TYPES = {"kill_switch", "anomaly"}
# entry/exit는 배칭(5초 윈도우)은 타지만 레이트리밋으로는 절대 드롭되면 안 된다
# (2026-09-19: 매수/매도는 금액 무관 항상 알림하기로 결정 — 필터 삭제와 짝을 이루는 보장)
_RATE_LIMIT_EXEMPT = _HIGH_PRIORITY_TYPES | {"entry", "exit"}

# 텔레그램이 HTML 파싱 실패 시 돌려주는 에러 메시지 패턴 (BadRequest 텍스트)
_HTML_PARSE_ERROR_PATTERNS = ("parse entities", "can't find end")

# Funding anomaly thresholds
_FUNDING_NEGATIVE_THRESHOLD = 0.0          # payment < 0 → anomaly
_FUNDING_LOW_RATIO_THRESHOLD = 0.5         # payment < 50% of expected → anomaly
_FUNDING_MIN_EXPECTED = 0.0001             # skip ratio check when expected is tiny


class FundingAccumulator:
    """Accumulates normal funding payments for daily summary.

    Anomalous payments (negative or < 50% of expected) are returned
    immediately for real-time dispatch.
    """

    def __init__(self) -> None:
        # date_str → cumulative payment
        self._daily: dict[str, float] = defaultdict(float)

    def record(self, data: dict[str, Any]) -> bool:
        """Record a funding event.

        Returns True if the event is anomalous and should be dispatched
        immediately. Returns False if it was buffered for daily summary.
        """
        payment = float(data.get("payment", 0.0))
        expected = float(data.get("expected_payment", 0.0))

        # Negative payment: paying funding instead of receiving
        if payment < _FUNDING_NEGATIVE_THRESHOLD:
            return True  # anomaly → dispatch now

        # Below 50% of expected (only check when expected is meaningful)
        if expected > _FUNDING_MIN_EXPECTED and payment < expected * _FUNDING_LOW_RATIO_THRESHOLD:
            return True  # anomaly → dispatch now

        # Normal: accumulate
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._daily[today] += payment
        return False

    def get_daily_total(self, date_str: str | None = None) -> float:
        """Return accumulated funding for the given date (default: today UTC)."""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self._daily.get(date_str, 0.0)

    def reset_day(self, date_str: str | None = None) -> None:
        """Reset accumulator for the given date after a daily report is sent."""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._daily.pop(date_str, None)


class AlertDispatcher:
    """Batching and rate-limited alert dispatcher.

    Features
    --------
    - Groups messages of the same alert_type arriving within
      ``batch_window_seconds`` into a single Telegram message.
    - Enforces ``max_messages_per_minute`` across all alert types.
      High-priority alerts (kill_switch, anomaly) always bypass the limit.
    - Handles funding accumulation via :class:`FundingAccumulator`.
    - Renders via Telegram HTML (``parse_mode``) with a plain-text fallback
      if the HTML fails to parse, and marks low-tier alerts silent
      (``disable_notification``) per :func:`formatters.alert_tier`.

    2026-09-19: 매수/매도 소액 필터(``min_trade_size_usd``)를 완전히 제거했다 —
    사용자 결정으로 금액과 무관하게 모든 체결을 알린다. 파라미터 자체를
    없애서 나중에 설정값 하나로 부활하지 못하게 한다.
    """

    def __init__(
        self,
        bot: Any,
        chat_id: int,
        *,
        batch_window_seconds: float = 5.0,
        max_messages_per_minute: int = 30,
        parse_mode: str = "HTML",
        disable_web_preview: bool = True,
        silent_tiers: frozenset[str] = frozenset({"trade", "info"}),
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._batch_window = batch_window_seconds
        self._max_per_min = max_messages_per_minute
        self._parse_mode = parse_mode
        self._disable_web_preview = disable_web_preview
        self._silent_tiers = silent_tiers

        # type → list[dict] pending batched data payloads
        self._pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # type → asyncio.TimerHandle (flush timer)
        self._timers: dict[str, asyncio.TimerHandle] = {}

        # Sliding window for rate limiting: timestamps of sent messages
        self._sent_ts: deque[float] = deque()

        # Funding accumulator
        self.funding = FundingAccumulator()

    # ── Public API ────────────────────────────────────────────────

    async def dispatch(self, alert_type: str, data: dict[str, Any]) -> None:
        """Receive an alert and apply batching / rate-limiting logic."""
        try:
            # ── Funding special handling (T-3) ───────────────────
            if alert_type == "funding":
                is_anomaly = self.funding.record(data)
                if not is_anomaly:
                    # Buffered for daily report — skip real-time dispatch
                    log.debug(
                        FUNDING_COLLECTED,
                        message="펀딩 누적 (일일 리포트용)",
                        payment=data.get("payment"),
                    )
                    return
                # Anomaly: fall through to immediate dispatch

            # ── High priority: bypass batching and rate limit ─────
            if alert_type in _HIGH_PRIORITY_TYPES:
                await self._send_now(alert_type, [data])
                return

            # ── Normal: add to batch buffer ───────────────────────
            self._pending[alert_type].append(data)
            self._schedule_flush(alert_type)

        except Exception:
            log.exception(SERVICE_HEALTH_FAIL, message="dispatch 처리 실패", alert_type=alert_type)

    async def flush_all(self) -> None:
        """Flush all pending batches (call on shutdown or before daily report)."""
        for alert_type in list(self._pending.keys()):
            await self._flush(alert_type)

    # ── Internal helpers ──────────────────────────────────────────

    def _schedule_flush(self, alert_type: str) -> None:
        """Schedule a flush after batch_window_seconds (idempotent)."""
        if alert_type in self._timers:
            return  # timer already running

        loop = asyncio.get_event_loop()
        handle = loop.call_later(
            self._batch_window,
            lambda: asyncio.ensure_future(self._flush(alert_type)),
        )
        self._timers[alert_type] = handle

    async def _flush(self, alert_type: str) -> None:
        """Send accumulated messages for alert_type as one batched message."""
        self._timers.pop(alert_type, None)
        payloads = self._pending.pop(alert_type, [])
        if not payloads:
            return
        await self._send_now(alert_type, payloads)

    async def _send_now(
        self, alert_type: str, payloads: list[dict[str, Any]]
    ) -> None:
        """Format and send messages, respecting rate limit."""
        is_rate_exempt = alert_type in _RATE_LIMIT_EXEMPT

        # Rate limit check (kill_switch/anomaly/entry/exit는 절대 드롭되지 않는다)
        if not is_rate_exempt and not self._check_rate():
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="레이트 리밋 초과로 알림 드롭",
                alert_type=alert_type,
                dropped_count=len(payloads),
            )
            return

        tier = alert_tier(alert_type, payloads[0])

        if len(payloads) == 1:
            msg = format_alert(alert_type, payloads[0])
        else:
            # Batch: combine into single message
            parts = [f"<b>[{len(payloads)}개 배치 알림 — {alert_type}]</b>\n"]
            for i, p in enumerate(payloads, 1):
                parts.append(f"<b>#{i}</b>\n{format_alert(alert_type, p)}")
            msg = "\n\n".join(parts)

        # Telegram 4096-char limit — 태그 중간이 잘려 파싱 에러가 나지 않게 안전 절단
        msg = _truncate_html(msg)

        sent = await self._send_text(alert_type, tier, msg, batch_size=len(payloads))
        if sent:
            self._record_sent()

    async def _send_text(
        self, alert_type: str, tier: str, msg: str, *, batch_size: int
    ) -> bool:
        """실제 전송 1회 + HTML 파싱 실패 시 평문 폴백 1회. 성공 시 True."""
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=msg,
                parse_mode=self._parse_mode,
                disable_notification=alert_is_silent(tier),
                disable_web_page_preview=self._disable_web_preview,
            )
            log.info(
                TELEGRAM_NOTIFICATION_SENT,
                message="알림 전송 완료",
                alert_type=alert_type,
                batch_size=batch_size,
            )
            return True
        except Exception as exc:
            if self._parse_mode and any(p in str(exc).lower() for p in _HTML_PARSE_ERROR_PATTERNS):
                # HTML 파싱 실패 — 태그를 벗겨낸 평문으로 1회 재전송 (알림 유실 방지)
                plain = re.sub(r"<[^>]+>", "", msg)
                try:
                    await self._bot.send_message(
                        chat_id=self._chat_id,
                        text=plain,
                        parse_mode=None,
                        disable_notification=alert_is_silent(tier),
                        disable_web_page_preview=self._disable_web_preview,
                    )
                    log.error(
                        SERVICE_HEALTH_FAIL,
                        message="HTML 파싱 실패 — 평문 재전송",
                        alert_type=alert_type,
                    )
                    return True
                except Exception:
                    pass
            log.exception(
                SERVICE_HEALTH_FAIL,
                message="알림 전송 실패",
                alert_type=alert_type,
            )
            return False

    def _check_rate(self) -> bool:
        """Return True if another message can be sent within the rate limit."""
        now = asyncio.get_event_loop().time()
        cutoff = now - 60.0
        # Remove entries older than 1 minute
        while self._sent_ts and self._sent_ts[0] < cutoff:
            self._sent_ts.popleft()
        return len(self._sent_ts) < self._max_per_min

    def _record_sent(self) -> None:
        """Record the timestamp of a sent message for rate limiting."""
        self._sent_ts.append(asyncio.get_event_loop().time())
