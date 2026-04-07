"""CryptoEngine Telegram Bot — entry point.

Starts the Telegram bot for command handling and subscribes to Redis
pub/sub channels for real-time alert delivery.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import redis.asyncio as aioredis
import structlog
import yaml
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from dispatcher import AlertDispatcher
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

# Load telegram.yaml for dispatcher and scheduler settings
_TELEGRAM_YAML = Path(os.getenv("TELEGRAM_CONFIG", "/app/config/telegram.yaml"))

def _load_telegram_config() -> dict:
    """Load telegram.yaml, return empty dict on any error."""
    try:
        if _TELEGRAM_YAML.exists():
            with open(_TELEGRAM_YAML) as f:
                return yaml.safe_load(f) or {}
    except Exception:
        pass
    return {}

_tg_cfg = _load_telegram_config()
_msg_cfg = _tg_cfg.get("telegram", {}).get("messages", {})
_trade_cfg = _tg_cfg.get("telegram", {}).get("notifications", {}).get("trades", {})
_schedule_cfg = _tg_cfg.get("telegram", {}).get("notifications", {}).get("portfolio", {})

BATCH_WINDOW_SECONDS: float = float(_msg_cfg.get("batch_window_seconds", 5))
MAX_MESSAGES_PER_MINUTE: int = int(_msg_cfg.get("max_messages_per_minute", 30))
MIN_TRADE_SIZE_USD: float = float(_trade_cfg.get("min_trade_size_usd", 50))
# List of "HH:MM" strings in UTC for automatic daily report (T-5)
SCHEDULE_UTC: list[str] = _schedule_cfg.get("schedule_utc", ["08:00", "20:00"])

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
    "ce:alerts:grafana",   # Grafana AlertManager → dashboard webhook → 여기로 통합
]

# Alert types that may arrive from both Grafana and the bot itself.
# For these types, fingerprint-based dedup (120s TTL) prevents duplicate sends.
_DEDUP_ALERT_TYPES = frozenset({"kill_switch", "anomaly"})


HEARTBEAT_INTERVAL_SECONDS = 30 * 60  # 30 minutes

# ── Phase 5 강화 모니터링 ─────────────────────────────────────
# BYBIT_TESTNET=false AND STRICT_MONITORING_HOURS > 0 이면 활성화
_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"
STRICT_MONITORING_HOURS: float = float(os.getenv("STRICT_MONITORING_HOURS", "0"))
STRICT_MONITORING_ENABLED: bool = (
    not _TESTNET and STRICT_MONITORING_HOURS > 0
)
# STRICT 모드에서의 마진비율 경고 임계값 (기본 10x → 20x로 보수적)
STRICT_MARGIN_WARN_THRESHOLD: float = float(os.getenv("STRICT_MARGIN_WARN_THRESHOLD", "20"))
# Phase 5: 잔고 검증 기준 (EXPECTED_INITIAL_BALANCE_USD)
EXPECTED_INITIAL_BALANCE_USD: float = float(os.getenv("EXPECTED_INITIAL_BALANCE_USD", "0"))


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


async def _dedup_check(redis_client: aioredis.Redis, alert_type: str, fingerprint: str) -> bool:
    """중복 알림 확인. True면 전송해도 됨, False면 이미 전송된 중복.

    Redis SET NX + TTL 120초를 사용하여 동일 fingerprint의 알림이
    120초 내 두 번 전달되면 두 번째를 스킵한다.
    """
    key = f"dedup:alert:{alert_type}:{fingerprint}"
    result = await redis_client.set(key, "1", nx=True, ex=120)
    return result is not None  # None이면 이미 존재 → 중복


async def _redis_alert_subscriber(
    redis_client: aioredis.Redis,
    handlers: BotHandlers,
    bot: object,
    shutdown_event: asyncio.Event,
) -> None:
    """Subscribe to Redis pub/sub channels and forward alerts to Telegram.

    중복 제거:
    - ce:alerts:grafana와 ce:alerts:kill_switch/anomaly 등이 동시에 오면
      동일 alert_type + fingerprint 기반 120초 dedup으로 한 번만 전송.
    - Grafana 페이로드에는 `fingerprint` 필드가 포함되어 있고,
      봇 자체 알림은 alert_type을 fingerprint로 사용 (분 단위 버킷).
    """
    import hashlib

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

            # alert_type: Grafana 채널은 data 내 alert_type 사용, 나머지는 채널명 suffix
            if channel == "ce:alerts:grafana":
                alert_type = data.get("alert_type", "grafana_unknown")
            else:
                alert_type = channel.split(":")[-1] if ":" in channel else "generic"

            # Dedup: kill_switch, anomaly는 Grafana와 봇 양쪽에서 올 수 있음
            if alert_type in _DEDUP_ALERT_TYPES:
                # Grafana fingerprint 우선 사용, 없으면 alert_type + 1분 버킷
                raw_fp = data.get("fingerprint") or alert_type
                minute_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
                fingerprint = hashlib.md5(f"{raw_fp}:{minute_bucket}".encode()).hexdigest()[:16]

                if not await _dedup_check(redis_client, alert_type, fingerprint):
                    log.info(
                        TELEGRAM_NOTIFICATION_SENT,
                        message="중복 알림 스킵",
                        alert_type=alert_type,
                        channel=channel,
                        fingerprint=fingerprint,
                    )
                    continue

            await handlers.dispatch_alert(bot, alert_type, data)
            log.debug(TELEGRAM_NOTIFICATION_SENT, message="알림 전달", channel=channel, alert_type=alert_type)

    except asyncio.CancelledError:
        log.info(SERVICE_STOPPING, message="Redis 구독자 취소됨")
    finally:
        await pubsub.unsubscribe(*ALERT_CHANNELS)
        await pubsub.aclose()


async def _scheduled_report_task(
    handlers: BotHandlers,
    bot: object,
    shutdown_event: asyncio.Event,
    schedule_utc: list[str],
) -> None:
    """T-5: Send daily report automatically at each UTC time in schedule_utc.

    Waits until the next scheduled time, sends the report, then repeats.
    Checks every 30 seconds to avoid missing a window.
    """
    from formatters import format_daily_report

    log.info(
        SERVICE_STARTED,
        message="일일 리포트 스케줄러 시작",
        schedule_utc=schedule_utc,
    )

    # Track which (date, time_str) pairs have been sent to avoid duplicates
    sent: set[tuple[str, str]] = set()

    try:
        while not shutdown_event.is_set():
            now_utc = datetime.now(timezone.utc)
            date_str = now_utc.strftime("%Y-%m-%d")
            hm = now_utc.strftime("%H:%M")

            for time_str in schedule_utc:
                key = (date_str, time_str)
                if key in sent:
                    continue
                # Check if we are within 1-minute window of the scheduled time
                sched_h, sched_m = map(int, time_str.split(":"))
                sched_minutes = sched_h * 60 + sched_m
                now_minutes = now_utc.hour * 60 + now_utc.minute
                # Allow a 1-minute send window
                if sched_minutes <= now_minutes <= sched_minutes + 1:
                    try:
                        report = await handlers._build_daily_report(date_str)
                        msg = format_daily_report(report)
                        await bot.send_message(  # type: ignore[attr-defined]
                            chat_id=TELEGRAM_CHAT_ID,
                            text=msg,
                            parse_mode="Markdown",
                        )
                        sent.add(key)
                        log.info(
                            TELEGRAM_NOTIFICATION_SENT,
                            message="일일 리포트 자동 전송",
                            date=date_str,
                            schedule_utc=time_str,
                        )
                    except Exception:
                        log.exception(
                            SERVICE_HEALTH_FAIL,
                            message="일일 리포트 자동 전송 실패",
                            schedule_utc=time_str,
                        )

            # Purge old sent keys (only keep today's)
            sent = {k for k in sent if k[0] == date_str}

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=30)
                break  # shutdown requested
            except asyncio.TimeoutError:
                pass  # normal — check again

    except asyncio.CancelledError:
        log.info(SERVICE_STOPPING, message="일일 리포트 스케줄러 취소됨")


async def _strict_monitoring_task(
    redis_client: aioredis.Redis,
    bot: object,
    shutdown_event: asyncio.Event,
    start_time: float,
) -> None:
    """Phase 5 강화 모니터링 태스크.

    매 1시간마다 강제 상태 리포트를 전송하고, STRICT_MONITORING_HOURS 경과 후
    자동으로 일반 모드 전환 안내를 전송한다.
    """
    import time as _time
    import json as _json

    REPORT_INTERVAL = 3600  # 1시간
    log.info("strict_monitoring_started", hours=STRICT_MONITORING_HOURS)

    # 시작 알림
    try:
        await bot.send_message(  # type: ignore[attr-defined]
            chat_id=TELEGRAM_CHAT_ID,
            text=(
                "🔴 *Phase 5 강화 모니터링 활성*\n"
                f"• 배치 알림 비활성 → 모든 알림 즉시 전송\n"
                f"• 마진비율 < {STRICT_MARGIN_WARN_THRESHOLD:.0f}x 경고\n"
                f"• 1시간마다 강제 상태 리포트\n"
                f"• {STRICT_MONITORING_HOURS:.0f}시간 후 자동 해제"
            ),
            parse_mode="Markdown",
        )
    except Exception:
        log.exception("strict_monitoring_start_notify_failed")

    report_count = 0
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=REPORT_INTERVAL)
            break  # shutdown
        except asyncio.TimeoutError:
            pass  # 1시간 경과 → 리포트 전송

        elapsed_hours = (_time.monotonic() - start_time) / 3600

        # STRICT_MONITORING_HOURS 경과 시 자동 해제
        if elapsed_hours >= STRICT_MONITORING_HOURS:
            try:
                await bot.send_message(  # type: ignore[attr-defined]
                    chat_id=TELEGRAM_CHAT_ID,
                    text=(
                        "✅ *Phase 5 강화 모니터링 자동 해제*\n"
                        f"• {STRICT_MONITORING_HOURS:.0f}시간 경과 → 일반 모드 전환\n"
                        "• 마진비율 경고 임계값: 10x (표준)\n"
                        "• 알림 배치 모드 복귀는 서비스 재시작 필요"
                    ),
                    parse_mode="Markdown",
                )
                log.info("strict_monitoring_auto_released", elapsed_hours=elapsed_hours)
            except Exception:
                log.exception("strict_monitoring_release_notify_failed")
            break

        # 1시간 주기 상태 리포트
        report_count += 1
        try:
            portfolio_raw = await redis_client.get("ce:portfolio:state")
            portfolio = _json.loads(portfolio_raw) if portfolio_raw else {}
            equity = portfolio.get("total_equity", 0.0)
            daily_pnl = portfolio.get("daily_pnl", 0.0)
            daily_pnl_pct = (daily_pnl / equity * 100) if equity > 0 else 0.0

            positions_raw = await redis_client.get("ce:positions:all")
            positions = _json.loads(positions_raw) if positions_raw else []
            position_count = len(positions) if isinstance(positions, list) else 0

            # 마진비율 체크
            margin_warn = ""
            for pos in (positions if isinstance(positions, list) else []):
                margin_ratio = pos.get("margin_ratio", 999)
                if margin_ratio < STRICT_MARGIN_WARN_THRESHOLD:
                    margin_warn = f"\n⚠️ *마진비율 경고*: {margin_ratio:.1f}x < {STRICT_MARGIN_WARN_THRESHOLD:.0f}x"

            balance_note = ""
            if EXPECTED_INITIAL_BALANCE_USD > 0:
                change = equity - EXPECTED_INITIAL_BALANCE_USD
                change_pct = (change / EXPECTED_INITIAL_BALANCE_USD * 100)
                balance_note = f"\n• 시작 잔고 대비: {change:+.2f} USD ({change_pct:+.1f}%)"

            await bot.send_message(  # type: ignore[attr-defined]
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    f"📊 *Phase 5 상태 리포트 #{report_count}*\n"
                    f"• 경과: {elapsed_hours:.1f}h / {STRICT_MONITORING_HOURS:.0f}h\n"
                    f"• 자산: {equity:.2f} USDT{balance_note}\n"
                    f"• 일간 PnL: {daily_pnl:+.2f} USD ({daily_pnl_pct:+.2f}%)\n"
                    f"• 포지션: {position_count}개{margin_warn}"
                ),
                parse_mode="Markdown",
            )
            log.info("strict_monitoring_report_sent", report_count=report_count, elapsed_hours=elapsed_hours)
        except Exception:
            log.exception("strict_monitoring_report_failed")


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

    # --- Handlers (dispatcher wired in after bot is available) ---
    handlers = BotHandlers(redis_client=redis_client, db_pool=db_pool)

    # --- Telegram application ---
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("help", handlers.help_command))
    app.add_handler(CommandHandler("status", handlers.status_command))
    app.add_handler(CommandHandler("emergency_close", handlers.emergency_close_command))
    app.add_handler(CommandHandler("stop", handlers.stop_command))
    app.add_handler(CommandHandler("start", handlers.start_command))
    app.add_handler(CommandHandler("weight", handlers.weight_command))
    app.add_handler(CommandHandler("report", handlers.report_command))
    app.add_handler(CommandHandler("pause_all", handlers.pause_all_command))
    app.add_handler(CommandHandler("resume_all", handlers.resume_all_command))
    # Work request / result file management
    app.add_handler(CommandHandler("requests", handlers.requests_command))
    app.add_handler(CommandHandler("results", handlers.results_command))
    app.add_handler(CommandHandler("get", handlers.get_result_command))
    app.add_handler(MessageHandler(filters.Document.ALL, handlers.handle_document))
    # Inline keyboard callbacks
    app.add_handler(CallbackQueryHandler(handlers.handle_callback))
    # Fallback: unknown command
    app.add_handler(MessageHandler(filters.COMMAND, handlers.unknown_command))

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

    # T-1: Wire AlertDispatcher now that app.bot is available
    # STRICT_MONITORING 모드에서는 배치 비활성화 (모든 알림 즉시 전송)
    _effective_batch_window = 0.0 if STRICT_MONITORING_ENABLED else BATCH_WINDOW_SECONDS
    dispatcher = AlertDispatcher(
        bot=app.bot,
        chat_id=TELEGRAM_CHAT_ID,
        batch_window_seconds=_effective_batch_window,
        max_messages_per_minute=MAX_MESSAGES_PER_MINUTE,
        min_trade_size_usd=MIN_TRADE_SIZE_USD,
    )
    handlers.set_dispatcher(dispatcher)
    log.info(
        SERVICE_STARTED,
        message="AlertDispatcher 설정 완료",
        batch_window=_effective_batch_window,
        max_per_min=MAX_MESSAGES_PER_MINUTE,
        min_trade_usd=MIN_TRADE_SIZE_USD,
        strict_monitoring=STRICT_MONITORING_ENABLED,
    )

    subscriber_task = asyncio.create_task(
        _redis_alert_subscriber(redis_client, handlers, app.bot, shutdown_event),
        name="redis_subscriber",
    )
    heartbeat_task = asyncio.create_task(
        _heartbeat_task(redis_client, app.bot, shutdown_event),
        name="heartbeat",
    )
    # T-5: Scheduled daily report
    scheduler_task = asyncio.create_task(
        _scheduled_report_task(handlers, app.bot, shutdown_event, SCHEDULE_UTC),
        name="report_scheduler",
    )

    # T-6: Phase 5 강화 모니터링 (STRICT_MONITORING_ENABLED일 때만)
    import time as _time_module
    _strict_start = _time_module.monotonic()
    strict_task: asyncio.Task | None = None
    if STRICT_MONITORING_ENABLED:
        strict_task = asyncio.create_task(
            _strict_monitoring_task(redis_client, app.bot, shutdown_event, _strict_start),
            name="strict_monitoring",
        )
        log.info(
            "strict_monitoring_mode_active",
            hours=STRICT_MONITORING_HOURS,
            margin_warn_threshold=STRICT_MARGIN_WARN_THRESHOLD,
        )

    log.info(
        SERVICE_STARTED,
        message="텔레그램 봇 실행 중",
        chat_id=TELEGRAM_CHAT_ID,
        schedule_utc=SCHEDULE_UTC,
    )

    # Wait for shutdown
    await shutdown_event.wait()
    log.info(SERVICE_STOPPING, message="종료 중")

    # Flush any pending batched alerts before shutdown
    try:
        await dispatcher.flush_all()
    except Exception:
        log.exception(SERVICE_HEALTH_FAIL, message="종료 전 알림 플러시 실패")

    # Cleanup
    subscriber_task.cancel()
    heartbeat_task.cancel()
    scheduler_task.cancel()
    gather_tasks = [subscriber_task, heartbeat_task, scheduler_task]
    if strict_task is not None:
        strict_task.cancel()
        gather_tasks.append(strict_task)
    await asyncio.gather(*gather_tasks, return_exceptions=True)

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
