"""structlog JSON to stdout; ERROR/CRITICAL also go to the notifier as anomalies."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import sys
import time
from typing import Any

import structlog

DEDUP_WINDOW_S = 120.0

_notifier: Any = None
_last_alert: dict[str, float] = {}
_tasks: set[asyncio.Task] = set()
_in_alert: contextvars.ContextVar[bool] = contextvars.ContextVar("in_alert", default=False)


def attach_notifier(notifier: Any) -> None:
    """Route every ERROR/CRITICAL log to ``notifier.anomaly`` (None detaches)."""
    global _notifier
    _notifier = notifier
    _last_alert.clear()


async def _deliver(notifier: Any, kind: str, message: str) -> None:
    _in_alert.set(True)  # errors logged while alerting must not alert again
    try:
        await notifier.anomaly(kind=kind, message=message)
    except Exception:
        pass


def _alert_processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    try:
        notifier = _notifier
        if notifier is None or _in_alert.get():
            return event_dict
        level = str(event_dict.get("level", method_name) or method_name).lower()
        if level not in ("error", "critical", "fatal", "exception"):
            return event_dict
        event = str(event_dict.get("event", ""))
        message = str(event_dict.get("message") or event)
        if event_dict.get("error") and event_dict.get("message") is None:
            message = f"{event}: {event_dict['error']}"
        now = time.monotonic()
        last = _last_alert.get(event)
        if last is not None and now - last < DEDUP_WINDOW_S:
            return event_dict
        _last_alert[event] = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return event_dict  # no loop: nothing to schedule on
        task = loop.create_task(_deliver(notifier, event, message[:700]))
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)
    except Exception:
        pass  # a log processor must never raise
    return event_dict


def setup_logging(level: str = "INFO") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        _alert_processor,
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(default=str),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level)
    for noisy in ("ccxt", "asyncio", "asyncpg", "telegram", "httpx", "httpcore", "aiohttp.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
