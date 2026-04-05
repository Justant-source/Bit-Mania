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

import structlog
import yaml

from services.orchestrator.core import StrategyOrchestrator
from shared.logging_config import setup_logging
from shared.log_events import *

SERVICE_NAME = "strategy-orchestrator"

log = structlog.get_logger(__name__)


def _load_config() -> dict[str, Any]:
    """Load orchestrator configuration from YAML."""
    config_path = os.getenv("CONFIG_PATH", "/app/config/orchestrator.yaml")
    try:
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        log.warning(ORCH_CONFIG_RELOADED, message="config not found", path=config_path)
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


async def main() -> None:
    """Start the orchestrator service."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    setup_logging(level=log_level, service_name=SERVICE_NAME)
    config = _load_config()
    log.info(SERVICE_STARTED, message="orchestrator 서비스 시작", config_keys=list(config.keys()))

    orchestrator = StrategyOrchestrator(config)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info(SERVICE_STOPPING, message="shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        await orchestrator.start()
        await shutdown_event.wait()
    finally:
        await orchestrator.stop()
        log.info(SERVICE_STOPPED, message="orchestrator 서비스 종료")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
