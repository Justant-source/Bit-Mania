"""Structured JSON logging via structlog with correlation IDs."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from contextvars import ContextVar
from typing import Any

import structlog

from shared.timezone_utils import kst_timestamper  # noqa: E402

# ── correlation ID ContextVar ────────────────────────────────────────────

_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    cid = _correlation_id.get()
    if not cid:
        cid = uuid.uuid4().hex[:12]
        _correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    _correlation_id.set(cid)


def new_correlation_id() -> str:
    cid = uuid.uuid4().hex[:12]
    _correlation_id.set(cid)
    return cid


def _add_correlation_id(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    event_dict["correlation_id"] = get_correlation_id()
    return event_dict


# ── level name → number mapping ─────────────────────────────────────────

_LEVEL_NO: dict[str, int] = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}

# Keys stripped from context before saving to DB
_CONTEXT_EXCLUDE = frozenset(
    {"event", "level", "timestamp", "_record", "logger", "exc_info", "stack_info", "service"}
)


# ── DB log processor ─────────────────────────────────────────────────────


def _make_db_log_processor(min_db_level: int):
    """Return a structlog processor that fire-and-forgets log entries to DB."""

    def db_log_processor(
        logger: Any,
        method_name: str,
        event_dict: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            from shared.log_writer import get_log_writer  # lazy import to avoid circular deps

            writer = get_log_writer()
            if writer is None:
                return event_dict

            level_str = event_dict.get("level", method_name) or method_name
            level_no = _LEVEL_NO.get(level_str.lower(), 20)

            if level_no < min_db_level:
                return event_dict

            event = event_dict.get("event", "")
            message = event_dict.get("message")

            # Build context from all remaining keys
            context: dict[str, Any] = {
                k: v for k, v in event_dict.items() if k not in _CONTEXT_EXCLUDE
            }

            trace_id = context.pop("trace_id", None)
            if trace_id is not None:
                trace_id = str(trace_id)

            # Extract error info from exc_info if present
            error_type: str | None = None
            error_stack: str | None = None
            exc_info = event_dict.get("exc_info")
            # exc_info=True means "use current sys.exc_info()" — resolve it here
            if exc_info is True:
                import sys as _sys
                exc_info = _sys.exc_info()
                if exc_info[0] is None:
                    exc_info = None
            if exc_info:
                try:
                    if isinstance(exc_info, tuple) and len(exc_info) == 3:
                        exc_cls, exc_val, exc_tb = exc_info
                        if exc_cls is not None:
                            error_type = f"{exc_cls.__module__}.{exc_cls.__qualname__}"
                        if exc_tb is not None:
                            error_stack = "".join(
                                traceback.format_exception(exc_cls, exc_val, exc_tb)
                            )
                    elif isinstance(exc_info, BaseException):
                        error_type = type(exc_info).__qualname__
                        error_stack = "".join(
                            traceback.format_exception(type(exc_info), exc_info, exc_info.__traceback__)
                        )
                except Exception:
                    pass

            asyncio.ensure_future(
                writer.write_log(
                    level=level_str.upper(),
                    level_no=level_no,
                    event=str(event),
                    message=str(message) if message is not None else None,
                    context=context or None,
                    trace_id=trace_id,
                    error_type=error_type,
                    error_stack=error_stack,
                )
            )
        except Exception:
            pass  # processor must never raise

        return event_dict

    return db_log_processor


# ── error/critical → Telegram alert (via Redis) ──────────────────────────

_ERROR_ALERT_CHANNEL = "ce:alerts:anomaly"
# Dedup: suppress identical alerts within this window (seconds)
_ERROR_ALERT_DEDUP_SECONDS = 300
_last_error_alerts: dict[str, float] = {}
_error_alert_redis: Any = None


def _publish_error_alert(payload: str) -> None:
    """Fire-and-forget publish to Redis for error alerting."""
    global _error_alert_redis
    try:
        import redis.asyncio as aioredis

        if _error_alert_redis is None:
            url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
            _error_alert_redis = aioredis.from_url(url, decode_responses=True)

        asyncio.ensure_future(_error_alert_redis.publish(_ERROR_ALERT_CHANNEL, payload))
    except Exception:
        pass


def _make_error_alert_processor(service_name: str):
    """Return a structlog processor that publishes ERROR+ logs to Redis for Telegram alerting."""

    def error_alert_processor(
        logger: Any,
        method_name: str,
        event_dict: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            level_str = event_dict.get("level", method_name) or method_name
            level_no = _LEVEL_NO.get(level_str.lower(), 20)

            # Only ERROR (40) and CRITICAL (50)
            if level_no < 40:
                return event_dict

            event = str(event_dict.get("event", ""))
            message = str(event_dict.get("message", "")) if event_dict.get("message") else event

            # Dedup: skip if same service+event was alerted recently
            dedup_key = f"{service_name}:{event}"
            now = time.time()
            last_sent = _last_error_alerts.get(dedup_key, 0)
            if now - last_sent < _ERROR_ALERT_DEDUP_SECONDS:
                return event_dict
            _last_error_alerts[dedup_key] = now

            # Publish to Redis (fire-and-forget)
            import json as _json
            alert_payload = _json.dumps({
                "type": "error_log",
                "level": level_str.upper(),
                "service": service_name,
                "event": event,
                "message": message[:300],
                "ts": datetime.now(timezone.utc).isoformat(),
            }, default=str)

            _publish_error_alert(alert_payload)
        except Exception:
            pass  # processor must never raise

        return event_dict

    return error_alert_processor


# ── setup ────────────────────────────────────────────────────────────────


def setup_logging(
    level: str = "INFO",
    json_output: bool = True,
    service_name: str = "cryptoengine",
    db_pool=None,
    min_db_level: int = 20,
) -> None:
    """Configure structlog + stdlib logging.

    Parameters
    ----------
    level : root log level name (DEBUG, INFO, WARNING, ...).
    json_output : if True render as JSON; otherwise use coloured console output.
    service_name : added to every log event under the ``service`` key.
    db_pool : optional asyncpg pool; when provided, logs are persisted to DB.
    min_db_level : minimum numeric level to write to DB (default 20=INFO).
                   Overridable via ``LOG_DB_MIN_LEVEL`` env var.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Allow env-var override of min_db_level
    min_db_level = int(os.environ.get("LOG_DB_MIN_LEVEL", min_db_level))

    # Initialise LogWriter if a db_pool was supplied
    if db_pool is not None:
        import asyncio as _asyncio
        from shared.log_writer import init_log_writer as _init_log_writer

        loop = _asyncio.get_event_loop()
        if loop.is_running():
            _asyncio.ensure_future(_init_log_writer(service_name, db_pool))
        else:
            loop.run_until_complete(_init_log_writer(service_name, db_pool))

    # shared processors
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_correlation_id,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        kst_timestamper,  # UTC 저장, KST 표시 (+09:00)
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        # DB writer: fire-and-forget, placed before final renderer
        _make_db_log_processor(min_db_level),
        # ERROR/CRITICAL → Telegram alert via Redis
        _make_error_alert_processor(service_name),
    ]

    if json_output:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # foreign_pre_chain: stdlib(third-party) 로거도 KST 타임스탬프 적용
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            kst_timestamper,
        ],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.EventRenamer("msg"),
            structlog.processors.add_log_level,
            # inject static service name
            lambda _, __, ed: {**ed, "service": service_name},
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Silence noisy third-party loggers
    for noisy in (
        "ccxt", "ccxt.base.exchange",
        "asyncio",
        "websockets", "websockets.client", "websockets.server",
        "aioredis",
        "asyncpg", "asyncpg.pool",
        "telegram", "telegram.ext", "httpx", "hpack",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)
