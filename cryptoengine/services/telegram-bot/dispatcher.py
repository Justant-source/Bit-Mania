"""Alert dispatcher with rate limiting, batching, and funding accumulator.

T-1: AlertDispatcher — batch_window_seconds, max_messages_per_minute, CRITICAL bypass
T-3: FundingAccumulator — normal funding buffered, anomalies sent immediately
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import structlog

from formatters import format_alert
from shared.log_events import (
    TELEGRAM_NOTIFICATION_SENT,
    SERVICE_HEALTH_FAIL,
    FA_FUNDING_COLLECTED,
)

log = structlog.get_logger(__name__)

# Alert priority: CRITICAL / ERROR always bypass rate limit
_HIGH_PRIORITY_TYPES = {"kill_switch", "anomaly"}

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
    - Filters trade alerts below ``min_trade_size_usd``.
    - Handles funding accumulation via :class:`FundingAccumulator`.
    """

    def __init__(
        self,
        bot: Any,
        chat_id: int,
        *,
        batch_window_seconds: float = 5.0,
        max_messages_per_minute: int = 30,
        min_trade_size_usd: float = 50.0,
    ) -> None:
        self._bot = bot
        self._chat_id = chat_id
        self._batch_window = batch_window_seconds
        self._max_per_min = max_messages_per_minute
        self._min_trade_usd = min_trade_size_usd

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
                        FA_FUNDING_COLLECTED,
                        message="펀딩 누적 (일일 리포트용)",
                        payment=data.get("payment"),
                    )
                    return
                # Anomaly: fall through to immediate dispatch

            # ── Trade size filter ─────────────────────────────────
            if alert_type in ("entry", "exit"):
                notional = float(data.get("notional_usd", data.get("quantity", 0)) or 0)
                if notional > 0 and notional < self._min_trade_usd:
                    log.debug(
                        TELEGRAM_NOTIFICATION_SENT,
                        message="소액 거래 알림 필터링",
                        notional_usd=notional,
                        min_usd=self._min_trade_usd,
                    )
                    return

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
        is_high_priority = alert_type in _HIGH_PRIORITY_TYPES

        # Rate limit check
        if not is_high_priority and not self._check_rate():
            log.warning(
                SERVICE_HEALTH_FAIL,
                message="레이트 리밋 초과로 알림 드롭",
                alert_type=alert_type,
                dropped_count=len(payloads),
            )
            return

        try:
            if len(payloads) == 1:
                msg = format_alert(alert_type, payloads[0])
            else:
                # Batch: combine into single message
                parts = [f"*[{len(payloads)}개 배치 알림 — {alert_type}]*\n"]
                for i, p in enumerate(payloads, 1):
                    parts.append(f"*#{i}*\n{format_alert(alert_type, p)}")
                msg = "\n\n".join(parts)

            # Telegram 4096-char limit
            if len(msg) > 4096:
                msg = msg[:4090] + "\n…"

            await self._bot.send_message(
                chat_id=self._chat_id,
                text=msg,
                parse_mode="Markdown",
            )
            self._record_sent()
            log.info(
                TELEGRAM_NOTIFICATION_SENT,
                message="알림 전송 완료",
                alert_type=alert_type,
                batch_size=len(payloads),
            )

        except Exception:
            log.exception(
                SERVICE_HEALTH_FAIL,
                message="알림 전송 실패",
                alert_type=alert_type,
            )

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
