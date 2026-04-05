"""CryptoEngine Telegram Bot — entry point.

Starts the Telegram bot for command handling and subscribes to Redis
pub/sub channels for real-time alert delivery.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal

import asyncpg
import redis.asyncio as aioredis
import structlog
from telegram.ext import ApplicationBuilder, CommandHandler

from handlers import BotHandlers
from shared.log_events import *
from shared.logging_config import setup_logging
from shared.log_writer import init_log_writer, close_log_writer

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERVICE_NAME = "telegram-bot"
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

# Redis channels to subscribe to for alerts
ALERT_CHANNELS = [
    "ce:alerts:entry",
    "ce:alerts:exit",
    "ce:alerts:funding",
    "ce:alerts:kill_switch",
    "ce:alerts:anomaly",
    "ce:alerts:daily_report",
]


HEARTBEAT_INTERVAL_SECONDS = 30 * 60  # 30 minutes


async def _heartbeat_task(
    redis_client: aioredis.Redis,
    bot: object,
    shutdown_event: asyncio.Event,
) -> None:
    """Send a system-alive heartbeat message every 30 minutes."""
    log.info(TELEGRAM_HEARTBEAT, message="하트비트 태스크 시작", interval_seconds=HEARTBEAT_INTERVAL_SECONDS)
    try:
        while not shutdown_event.is_set():
            try:
                # Read equity and positions from Redis
                portfolio_raw = await redis_client.get("ce:portfolio:state")
                portfolio = json.loads(portfolio_raw) if portfolio_raw else {}
                equity = portfolio.get("total_equity", 0.0)

                positions_raw = await redis_client.get("ce:positions:all")
                positions = json.loads(positions_raw) if positions_raw else []
                position_count = len(positions) if isinstance(positions, list) else 0

                msg = (
                    f"\u2705 System alive | "
                    f"equity: {equity:.2f} USDT | "
                    f"positions: {position_count}"
                )
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)  # type: ignore[attr-defined]
                log.info(TELEGRAM_HEARTBEAT, message="하트비트 전송", equity=equity, positions=position_count)

            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="하트비트 전송 실패")

            # Wait for next interval, checking shutdown frequently
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=HEARTBEAT_INTERVAL_SECONDS
                )
                break  # shutdown_event was set
            except asyncio.TimeoutError:
                pass  # normal — interval elapsed, send next heartbeat

    except asyncio.CancelledError:
        log.info(SERVICE_STOPPING, message="하트비트 태스크 취소됨")


async def _redis_alert_subscriber(
    redis_client: aioredis.Redis,
    handlers: BotHandlers,
    bot: object,
    shutdown_event: asyncio.Event,
) -> None:
    """Subscribe to Redis pub/sub channels and forward alerts to Telegram."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(*ALERT_CHANNELS)
    log.info(REDIS_CONNECTED, message="Redis 알림 구독 시작", channels=ALERT_CHANNELS)

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
                log.warning(SERVICE_HEALTH_FAIL, message="잘못된 알림 JSON", channel=channel, raw=data_raw)
                continue

            # Determine alert type from channel name
            alert_type = channel.split(":")[-1] if ":" in channel else "generic"

            await handlers.dispatch_alert(bot, alert_type, data)
            log.debug(TELEGRAM_NOTIFICATION_SENT, message="알림 전달", channel=channel, alert_type=alert_type)

    except asyncio.CancelledError:
        log.info(SERVICE_STOPPING, message="Redis 구독자 취소됨")
    finally:
        await pubsub.unsubscribe(*ALERT_CHANNELS)
        await pubsub.aclose()


async def main() -> None:
    """Start the Telegram bot and Redis alert subscriber."""
    # --- Connection pools (created before logging so we can pass db_pool) ---
    db_pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DB_DSN, min_size=1, max_size=5, command_timeout=30
    )
    await init_log_writer(SERVICE_NAME, db_pool)
    setup_logging(service_name=SERVICE_NAME, db_pool=db_pool)
    log = structlog.get_logger()

    log.info(SERVICE_STARTED, message="텔레그램 봇 시작 중")

    if not TELEGRAM_BOT_TOKEN:
        log.error(SERVICE_HEALTH_FAIL, message="TELEGRAM_BOT_TOKEN 미설정")
        return

    if TELEGRAM_CHAT_ID == 0:
        log.error(SERVICE_HEALTH_FAIL, message="TELEGRAM_CHAT_ID 미설정")
        return

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    log.info(REDIS_CONNECTED, message="Redis 연결 완료", redis=REDIS_URL)

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
    app.add_handler(CommandHandler("pause_all", handlers.pause_all_command))
    app.add_handler(CommandHandler("resume_all", handlers.resume_all_command))

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        log.info(SERVICE_STOPPING, message="종료 신호 수신")
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
    heartbeat_task = asyncio.create_task(
        _heartbeat_task(redis_client, app.bot, shutdown_event),
        name="heartbeat",
    )

    log.info(SERVICE_STARTED, message="텔레그램 봇 실행 중", chat_id=TELEGRAM_CHAT_ID)

    # Wait for shutdown
    await shutdown_event.wait()
    log.info(SERVICE_STOPPING, message="종료 중")

    # Cleanup
    subscriber_task.cancel()
    heartbeat_task.cancel()
    await asyncio.gather(subscriber_task, heartbeat_task, return_exceptions=True)

    await app.updater.stop()  # type: ignore[union-attr]
    await app.stop()
    await app.shutdown()
    await redis_client.aclose()
    log.info(SERVICE_STOPPED, message="텔레그램 봇 종료")
    await db_pool.close()
    await close_log_writer()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
