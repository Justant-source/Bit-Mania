"""``bot.main.amain`` wiring. amain() builds everything inline from env, so it cannot accept fakes:
the constructors it imports (BybitConnector, TelegramNotifier, TelegramCommands, replay sync) are
monkeypatched; everything else (Database, BotState, SafetyGuard, OrderExecutor, Trader, RiskManager,
BotApp, web server, signal handlers, logging->notifier) is the production code with the real
SystemClock. The process is stopped with SIGINT, which amain wires to app.request_stop like SIGTERM."""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pytest

import bot.main as main_mod
from bot.telegram_bot import TelegramCommands
from e2e_harness import (
    DASH_TOKEN, DB_NAME, PG, E2ENotifier, cb_update, cmd_update, buttons, floor_boundary, free_port,
    make_feed, entry_plan,
)

pytestmark = pytest.mark.asyncio

ROOT = Path(__file__).resolve().parents[2]


async def test_amain_wiring_start_recover_emergency_close_and_sigint(db, fx, monkeypatch):
    plan = entry_plan()
    feed = make_feed(fx, plan["bars"], end=floor_boundary(datetime.now(timezone.utc)))
    feed.publish(datetime.now(timezone.utc))
    fx.set_position("long", 0.03, 60_000.0)                   # a position from "before the restart"
    notifier = E2ENotifier()
    captured: dict = {}

    class CapturedCommands(TelegramCommands):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            captured["commands"], captured["args"], captured["kw"] = self, a, kw

        async def start(self):                                  # no long polling against the real API
            captured["started"] = True

        async def stop(self):
            captured["stopped"] = True

    ctor: dict = {}

    def bybit(api_key, api_secret, testnet=False):
        ctor.update(api_key=api_key, api_secret=api_secret, testnet=testnet)
        return fx

    async def noop_sync(_db):
        captured["replay_sync_calls"] = captured.get("replay_sync_calls", 0) + 1

    import bot.exchange, bot.notify, bot.replay_data, bot.telegram_bot
    monkeypatch.setattr(bot.exchange, "BybitConnector", bybit)
    monkeypatch.setattr(bot.notify, "TelegramNotifier", lambda token, chat: notifier)
    monkeypatch.setattr(bot.telegram_bot, "TelegramCommands", CapturedCommands)
    monkeypatch.setattr(bot.replay_data, "sync", noop_sync)
    port = free_port()
    env = {
        "BYBIT_TESTNET": "false", "BYBIT_API_KEY": "k", "BYBIT_API_SECRET": "s",
        "TELEGRAM_BOT_TOKEN": "tok", "TELEGRAM_CHAT_ID": "555", "DB_HOST": PG["host"],
        "DB_PORT": str(PG["port"]), "DB_NAME": DB_NAME, "DB_USER": PG["user"], "DB_PASSWORD": PG["password"],
        "DASHBOARD_TOKEN": DASH_TOKEN, "DASHBOARD_PORT": str(port), "DASHBOARD_URL": "https://d.example/x",
        "BOT_CONFIG": str(ROOT / "config" / "bot.yaml"), "LOG_LEVEL": "WARNING", "SAFETY_LEVERAGE_LIMIT": "3.0",
    }
    for k in ("BOT_DRY_RUN",):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    task = asyncio.create_task(main_mod.amain())
    try:
        for _ in range(400):                                    # wait for startup
            if captured.get("started") and any("봇 시작" in t for t in notifier.texts()):
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail(f"bot did not start: {notifier.calls}")
        assert ctor == {"api_key": "k", "api_secret": "s", "testnet": False}
        assert fx.margin_calls == ["isolated"] and fx.leverage_calls == [3]
        assert captured["args"][:2] == ("tok", "555") and captured["kw"]["dashboard_url"] == f"https://d.example/x?t={DASH_TOKEN}"
        # position recovered from the exchange, not closed, not re-entered
        assert fx.position is not None and fx.position.size == pytest.approx(0.03)
        assert fx.placed == []
        assert any("보유" in t for t in notifier.texts())
        # dashboard server is up with the token
        async with aiohttp.ClientSession() as http:
            assert (await http.get(f"http://127.0.0.1:{port}/healthz")).status == 200
            assert (await http.get(f"http://127.0.0.1:{port}/api/summary")).status == 403
            r = await http.get(f"http://127.0.0.1:{port}/api/summary?t={DASH_TOKEN}")
            assert r.status == 200
        # the production-built TelegramCommands drives the production-built BotApp
        cmds = captured["commands"]
        upd = cmd_update()
        await cmds.cmd_emergency_close(upd)
        yes, _ = buttons(upd)
        cb, q = cb_update(yes)
        await cmds.on_callback(cb)
        assert fx.position is None, q.edits
        assert "긴급 청산 완료" in q.edits[-1]
        reduce = [o for o in fx.placed if o.reduce_only]
        assert len(reduce) == 1 and reduce[0].order_type == "market"
        # an unsolicited ERROR log reaches the notifier (attach_notifier wiring)
        import structlog
        structlog.get_logger("e2e").error("e2e_probe", message="probe")
        await asyncio.sleep(0.05)
        assert any(a["kind"] == "e2e_probe" for a in notifier.anomalies())
    finally:
        os.kill(os.getpid(), signal.SIGINT)
        rc = await asyncio.wait_for(task, 20)
    assert rc == 0
    assert captured.get("stopped") is True
    assert not fx.connected
    assert any("봇 종료" in t for t in notifier.texts())
    # restart of the same wiring keeps the halt
    async with db.pool.acquire() as c:
        v = await c.fetchval("SELECT value FROM bot_state WHERE key='manual_halt'")
    assert v is not None and '"active": true' in str(v).replace('"active":true', '"active": true')
