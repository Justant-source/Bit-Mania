"""Strategy Orchestrator — entry point.

Boots up the main orchestration loop that coordinates strategy weights,
monitors portfolio risk, and enforces kill-switch conditions.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from typing import Any

import logging
import structlog
import yaml

from services.orchestrator.core import StrategyOrchestrator

log = structlog.get_logger(__name__)


def _load_config() -> dict[str, Any]:
    """Load orchestrator configuration from YAML."""
    config_path = os.getenv("CONFIG_PATH", "/app/config/orchestrator.yaml")
    try:
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        log.warning("config_not_found", path=config_path)
        cfg = {}

    # Environment variable overrides
    cfg.setdefault("redis", {})["url"] = os.getenv("REDIS_URL", "redis://localhost:6379")
    cfg.setdefault("postgres", {})["dsn"] = os.getenv(
        "DATABASE_URL",
        f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}:"
        f"{os.getenv('DB_PASSWORD', '')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'cryptoengine')}",
    )
    return cfg


def _configure_logging() -> None:
    """Set up structlog with JSON rendering."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def main() -> None:
    """Start the orchestrator service."""
    _configure_logging()
    config = _load_config()
    log.info("orchestrator_starting", config_keys=list(config.keys()))

    orchestrator = StrategyOrchestrator(config)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        await orchestrator.start()
        await shutdown_event.wait()
    finally:
        await orchestrator.stop()
        log.info("orchestrator_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
