"""Operator CLI for when the bot process is down or unresponsive.

    python -m bot.cli status
    python -m bot.cli emergency-close [--yes]
    python -m bot.cli resume

Works directly on the exchange and Postgres. A running bot re-reads bot_state
every loop tick (<= 60 s), so a halt set here reaches it too.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Any, Callable

from bot.state import MANUAL_HALT, BotState, parse_ts

KEY_KILL = "kill_switch"


async def cmd_status(executor: Any, state: BotState, out: Callable[[str], None] = print) -> dict[str, Any]:
    info: dict[str, Any] = {}
    try:
        pos = await executor.get_position()
        info["position"] = None if pos is None else {
            "side": pos.side, "size": pos.size, "entry_price": pos.entry_price,
            "unrealized_pnl": pos.unrealized_pnl}
    except Exception as exc:
        info["position"] = f"ERROR {type(exc).__name__}: {exc}"
    try:
        bal = await executor.get_balance()
        info["wallet"] = bal.get("wallet", bal.get("total"))
    except Exception as exc:
        info["wallet"] = f"ERROR {type(exc).__name__}: {exc}"
    blocked, why = state.entries_blocked()
    info["entries_blocked"] = blocked
    info["reason"] = why or None
    info["kill_switch"] = state.get(KEY_KILL)
    hb = state.get("heartbeat")
    at = parse_ts(hb.get("at")) if isinstance(hb, dict) else None
    info["heartbeat_age_s"] = int((datetime.now(timezone.utc) - at).total_seconds()) if at else None
    out(json.dumps(info, ensure_ascii=False, indent=2, default=str))
    return info


async def cmd_emergency_close(
    executor: Any, state: BotState, *, assume_yes: bool,
    ask: Callable[[str], str] = input, out: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Set the manual halt (blocks entries) and close the position reduce-only."""
    pos = await executor.get_position()
    out("보유 포지션: " + ("없음" if pos is None else f"{pos.side} {pos.size} BTC @ {pos.entry_price}"))
    if not assume_yes and ask("긴급 청산 및 진입 정지를 실행합니까? [yes/N] ").strip().lower() != "yes":
        out("취소됨")
        return {"aborted": True}
    await state.set(MANUAL_HALT, {"active": True, "by": "cli", "reason": "Manual emergency via CLI",
                                  "at": datetime.now(timezone.utc).isoformat()})
    res = await executor.close_position("emergency_close_cli")
    after = await executor.get_position()
    flat = after is None
    if pos is not None and flat:
        await _close_position_row(state)
    out("결과: " + ("flat" if flat else "아직 포지션 남음 — 수동 확인 필요"))
    return {"had_position": pos is not None, "flat": flat, "status": getattr(res, "status", None)}


async def _close_position_row(state: BotState) -> None:
    async with state._db.pool.acquire() as conn:  # noqa: SLF001 - same DB handle
        await conn.execute(
            "UPDATE positions SET closed_at=NOW(), close_reason='emergency_close_cli' "
            "WHERE closed_at IS NULL")


async def cmd_resume(state: BotState, out: Callable[[str], None] = print) -> dict[str, Any]:
    halt = state.get(MANUAL_HALT)
    was = isinstance(halt, dict) and bool(halt.get("active"))
    await state.delete(MANUAL_HALT)
    kill = state.get(KEY_KILL)
    if isinstance(kill, dict) and int(kill.get("level", 0)) == 4:  # MANUAL
        await state.delete(KEY_KILL)
    out("수동 정지 해제" if was else "수동 정지 상태가 아니었음")
    return {"was_halted": was}


async def _amain(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="python -m bot.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    ec = sub.add_parser("emergency-close")
    ec.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    sub.add_parser("resume")
    args = ap.parse_args(argv)

    from bot.db import Database
    from bot.exchange import BybitConnector
    from bot.execution import OrderExecutor
    from bot.main import SettingsError, load_settings
    from bot.safety import SafetyGuard

    try:
        s = load_settings(need_telegram=False)
    except SettingsError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    db = Database()
    await db.connect()
    await db.ensure_schema()
    state = BotState(db)
    await state.load()
    exchange = BybitConnector(s.api_key, s.api_secret, testnet=False)
    await exchange.connect()
    try:
        executor = OrderExecutor(exchange, db, SafetyGuard(exchange, state, leverage_limit=s.leverage_limit))
        if args.cmd == "status":
            await cmd_status(executor, state)
        elif args.cmd == "emergency-close":
            res = await cmd_emergency_close(executor, state, assume_yes=args.yes)
            return 0 if res.get("aborted") or res.get("flat") else 1
        else:
            await cmd_resume(state)
    finally:
        await exchange.disconnect()
        await db.close()
    return 0


def main() -> None:
    sys.exit(asyncio.run(_amain(sys.argv[1:])))


if __name__ == "__main__":
    main()
