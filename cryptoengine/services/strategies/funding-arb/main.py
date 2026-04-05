"""Entry point for the Funding-Rate Arbitrage strategy service."""

from __future__ import annotations

import asyncio
import os
import signal

import structlog

from shared import load_config, setup_logging
from shared.log_events import *
from shared.redis_client import close_redis
from strategy.strategy import FundingArbStrategy

SERVICE_NAME = "funding-arb"

logger = structlog.get_logger()


async def _shutdown(strategy: FundingArbStrategy) -> None:
    """Graceful shutdown handler."""
    logger.info(SERVICE_STOPPING, message="종료 요청 수신")
    await strategy.on_stop(reason="service_shutdown")
    await close_redis()


async def main() -> None:
    setup_logging(service_name=SERVICE_NAME)
    config = load_config(os.getenv("CONFIG_PATH", "/app/config/strategies/funding-arb.yaml"))

    strategy_id = os.getenv("STRATEGY_ID", "funding-arb")
    strategy = FundingArbStrategy(strategy_id=strategy_id, config=config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda: asyncio.create_task(_shutdown(strategy))
        )

    logger.info(SERVICE_STARTED, message="funding-arb 서비스 시작", strategy_id=strategy_id)
    await strategy.run()


if __name__ == "__main__":
    asyncio.run(main())
