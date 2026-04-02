"""Entry point for the Grid Trading strategy service."""

from __future__ import annotations

import asyncio
import os
import signal

import structlog

from shared import load_config, setup_logging
from shared.redis_client import close_redis
from strategy.strategy import GridStrategy

logger = structlog.get_logger()


async def _shutdown(strategy: GridStrategy) -> None:
    """Graceful shutdown handler."""
    logger.info("shutdown_requested")
    await strategy.on_stop(reason="service_shutdown")
    await close_redis()


async def main() -> None:
    setup_logging(service_name="grid-trading")
    config = load_config(os.getenv("CONFIG_PATH", "/app/config/strategies/grid-trading.yaml"))

    strategy_id = os.getenv("STRATEGY_ID", "grid-trading-01")
    strategy = GridStrategy(strategy_id=strategy_id, config=config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda: asyncio.create_task(_shutdown(strategy))
        )

    logger.info("grid_trading_service_starting", strategy_id=strategy_id)
    await strategy.run()


if __name__ == "__main__":
    asyncio.run(main())
