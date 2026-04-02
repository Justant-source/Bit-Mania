"""Structured JSON logging via structlog with correlation IDs."""

from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any

import structlog

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


# ── setup ────────────────────────────────────────────────────────────────


def setup_logging(
    level: str = "INFO",
    json_output: bool = True,
    service_name: str = "cryptoengine",
) -> None:
    """Configure structlog + stdlib logging.

    Parameters
    ----------
    level : root log level name (DEBUG, INFO, WARNING, ...).
    json_output : if True render as JSON; otherwise use coloured console output.
    service_name : added to every log event under the ``service`` key.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # shared processors
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_correlation_id,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
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
    for noisy in ("ccxt", "ccxt.base.exchange", "asyncio", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
