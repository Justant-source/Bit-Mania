"""Entry point for the Adaptive DCA strategy service."""

from __future__ import annotations

import asyncio
import os
import signal

import structlog

from shared import load_config, setup_logging
from shared.redis_client import close_redis
from strategy.strategy import AdaptiveDCAStrategy

logger = structlog.get_logger()


async def _shutdown(strategy: AdaptiveDCAStrategy) -> None:
    """Graceful shutdown handler."""
    logger.info("shutdown_requested")
    await strategy.on_stop(reason="service_shutdown")
    await close_redis()


async def main() -> None:
    setup_logging(service_name="adaptive-dca")
    config = load_config(os.getenv("CONFIG_PATH", "/app/config/strategies/adaptive-dca.yaml"))

    strategy_id = os.getenv("STRATEGY_ID", "adaptive-dca-01")
    strategy = AdaptiveDCAStrategy(strategy_id=strategy_id, config=config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda: asyncio.create_task(_shutdown(strategy))
        )

    logger.info("adaptive_dca_service_starting", strategy_id=strategy_id)
    await strategy.run()


if __name__ == "__main__":
    asyncio.run(main())
