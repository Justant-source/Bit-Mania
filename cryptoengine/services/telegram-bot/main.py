"""CryptoEngine Telegram Bot — entry point.

Starts the Telegram bot for command handling and subscribes to Redis
pub/sub channels for real-time alert delivery.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal

import asyncpg
import redis.asyncio as aioredis
import structlog
from telegram.ext import ApplicationBuilder, CommandHandler

from handlers import BotHandlers

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Redis channels to subscribe to for alerts
ALERT_CHANNELS = [
    "ce:alerts:entry",
    "ce:alerts:exit",
    "ce:alerts:funding",
    "ce:alerts:kill_switch",
    "ce:alerts:anomaly",
    "ce:alerts:daily_report",
]


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if LOG_LEVEL == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, LOG_LEVEL, logging.INFO)  # type: ignore[arg-type]
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def _redis_alert_subscriber(
    redis_client: aioredis.Redis,
    handlers: BotHandlers,
    bot: object,
    shutdown_event: asyncio.Event,
) -> None:
    """Subscribe to Redis pub/sub channels and forward alerts to Telegram."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(*ALERT_CHANNELS)
    log.info("redis_subscriber_started", channels=ALERT_CHANNELS)

    try:
        while not shutdown_event.is_set():
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )
            if message is None:
                await asyncio.sleep(0.1)
                continue

            channel = message.get("channel", "")
            data_raw = message.get("data", "{}")

            try:
                data = json.loads(data_raw) if isinstance(data_raw, str) else {}
            except json.JSONDecodeError:
                log.warning("invalid_alert_json", channel=channel, raw=data_raw)
                continue

            # Determine alert type from channel name
            alert_type = channel.split(":")[-1] if ":" in channel else "generic"

            await handlers.dispatch_alert(bot, alert_type, data)
            log.debug("alert_forwarded", channel=channel, alert_type=alert_type)

    except asyncio.CancelledError:
        log.info("redis_subscriber_cancelled")
    finally:
        await pubsub.unsubscribe(*ALERT_CHANNELS)
        await pubsub.aclose()


async def main() -> None:
    """Start the Telegram bot and Redis alert subscriber."""
    _configure_logging()
    log.info("telegram_bot_starting")

    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN not set")
        return

    if TELEGRAM_CHAT_ID == 0:
        log.error("TELEGRAM_CHAT_ID not set")
        return

    # --- Connection pools ---
    db_pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DB_DSN, min_size=1, max_size=5, command_timeout=30
    )
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    log.info("connections_established", redis=REDIS_URL)

    # --- Handlers ---
    handlers = BotHandlers(redis_client=redis_client, db_pool=db_pool)

    # --- Telegram application ---
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("status", handlers.status_command))
    app.add_handler(CommandHandler("emergency_close", handlers.emergency_close_command))
    app.add_handler(CommandHandler("stop", handlers.stop_command))
    app.add_handler(CommandHandler("start", handlers.start_command))
    app.add_handler(CommandHandler("weight", handlers.weight_command))
    app.add_handler(CommandHandler("report", handlers.report_command))

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # --- Launch ---
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)  # type: ignore[union-attr]

    subscriber_task = asyncio.create_task(
        _redis_alert_subscriber(redis_client, handlers, app.bot, shutdown_event),
        name="redis_subscriber",
    )

    log.info("telegram_bot_running", chat_id=TELEGRAM_CHAT_ID)

    # Wait for shutdown
    await shutdown_event.wait()
    log.info("shutting_down")

    # Cleanup
    subscriber_task.cancel()
    await asyncio.gather(subscriber_task, return_exceptions=True)

    await app.updater.stop()  # type: ignore[union-attr]
    await app.stop()
    await app.shutdown()
    await redis_client.aclose()
    await db_pool.close()
    log.info("telegram_bot_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
