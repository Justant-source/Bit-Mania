"""Entry point: ``python -m bot.main``.

Refuses to start unless BYBIT_TESTNET is "false" (mainnet only) and all
credentials are present. Heavy imports happen inside ``amain`` so the settings
check stays importable and testable.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from dataclasses import dataclass
from typing import Mapping

import structlog

log = structlog.get_logger("bot.main")


class SettingsError(RuntimeError):
    pass


@dataclass
class Settings:
    api_key: str
    api_secret: str
    telegram_token: str
    telegram_chat_id: str
    leverage_limit: float
    dashboard_port: int
    dashboard_token: str
    dashboard_url: str
    log_level: str
    dry_run: bool
    config_path: str


def load_settings(env: Mapping[str, str] | None = None, *, need_telegram: bool = True) -> Settings:
    e = os.environ if env is None else env
    if str(e.get("BYBIT_TESTNET", "")).strip().lower() != "false":
        raise SettingsError('BYBIT_TESTNET must be exactly "false" (mainnet only); refusing to start')
    required = ["BYBIT_API_KEY", "BYBIT_API_SECRET", "DB_PASSWORD"]
    if need_telegram:
        required += ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
    missing = [k for k in required if not e.get(k)]
    if missing:
        raise SettingsError(f"missing required environment variables: {', '.join(missing)}")
    return Settings(
        api_key=e["BYBIT_API_KEY"], api_secret=e["BYBIT_API_SECRET"],
        telegram_token=e.get("TELEGRAM_BOT_TOKEN", ""), telegram_chat_id=e.get("TELEGRAM_CHAT_ID", ""),
        leverage_limit=float(e.get("SAFETY_LEVERAGE_LIMIT", "3.0")),
        dashboard_port=int(e.get("DASHBOARD_PORT", "3000")),
        dashboard_token=e.get("DASHBOARD_TOKEN", ""),
        dashboard_url=e.get("DASHBOARD_URL", ""),
        log_level=e.get("LOG_LEVEL", "INFO"),
        dry_run=e.get("BOT_DRY_RUN", "") == "1",
        config_path=e.get("BOT_CONFIG", "config/bot.yaml"),
    )


async def amain() -> int:
    from bot.app import BotApp
    from bot.db import Database
    from bot.exchange import BybitConnector
    from bot.execution import OrderExecutor
    from bot.logging_setup import attach_notifier, setup_logging
    from bot.notify import TelegramNotifier
    from bot.report import build_daily_report, report_keyboard
    from bot.replay_data import sync as replay_sync
    from bot.risk import RiskConfig, RiskManager
    from bot.safety import SafetyGuard
    from bot.state import BotState
    from bot.strategy import Trader
    from bot.telegram_bot import TelegramCommands
    from bot.web import start_web
    from bot.web_supertrend import make_providers

    try:
        s = load_settings()
    except SettingsError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    setup_logging(s.log_level)
    # Telegram links carry the dashboard token so one tap opens the page.
    dashboard_link = ""
    if s.dashboard_url and s.dashboard_token:
        sep = "&" if "?" in s.dashboard_url else "?"
        dashboard_link = f"{s.dashboard_url}{sep}t={s.dashboard_token}"
    cfg = RiskConfig.from_yaml(s.config_path)

    db = Database()
    await db.connect()
    await db.ensure_schema()
    exchange = BybitConnector(s.api_key, s.api_secret, testnet=False)
    notifier = TelegramNotifier(s.telegram_token, s.telegram_chat_id)
    attach_notifier(notifier)

    state = BotState(db)
    safety = SafetyGuard(exchange, state, leverage_limit=s.leverage_limit)
    executor = OrderExecutor(exchange, db, safety, dry_run=s.dry_run)
    trader = Trader(executor, db, state, notifier, max_capital_usd=cfg.max_capital_usd)
    risk = RiskManager(db, state, executor, notifier, config=cfg,
                       on_position_closed=trader.after_forced_close)

    app_holder: dict[str, BotApp] = {}

    async def report_fn():
        html = await build_daily_report(db, app_holder["app"])
        return html, (report_keyboard(dashboard_link) if dashboard_link else None)

    app = BotApp(db=db, exchange=exchange, executor=executor, notifier=notifier, state=state,
                 trader=trader, risk=risk, report_fn=report_fn, replay_sync=replay_sync)
    app_holder["app"] = app
    trader.on_fill = app.snapshot_after_fill

    commands = TelegramCommands(s.telegram_token, s.telegram_chat_id, app, db,
                                dashboard_url=dashboard_link)
    app.add_service(commands.start, commands.stop)
    if s.dashboard_token:
        web_holder: dict[str, object] = {}

        async def _start_web():
            web_holder["web"] = await start_web(
                db, port=s.dashboard_port, token=s.dashboard_token,
                supertrend=make_providers(app=app, executor=executor, exchange=exchange))

        async def _stop_web():
            if "web" in web_holder:
                await web_holder["web"].stop()  # type: ignore[attr-defined]

        app.add_service(_start_web, _stop_web)
    else:
        log.warning("dashboard_disabled", message="DASHBOARD_TOKEN 미설정 — 대시보드 비활성")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, app.request_stop)

    try:
        await app.run()  # SIGTERM leaves the position open
    finally:
        await notifier.close()
        await db.close()
    return 0


def main() -> None:
    sys.exit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
