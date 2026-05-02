"""분기물 만기/신규 자동 처리.

Bybit은 분기 만료 시 신규 분기물 자동 출시. 매주 1회 정기 점검하여
QUARTERLY_SYMBOLS_USDT 리스트를 동기화.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime

import httpx
import structlog

log = structlog.get_logger(__name__)

BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"


async def fetch_active_quarterly_symbols() -> list[str]:
    """활성 분기물 리스트를 Bybit API에서 조회."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(BYBIT_INSTRUMENTS_URL, params={
            "category": "linear",
            "baseCoin": "BTC",
        })
        r.raise_for_status()
        instruments = r.json()["result"]["list"]

    quarterly = [
        inst["symbol"] for inst in instruments
        if inst.get("contractType") == "LinearFutures"
        and inst.get("status") == "Trading"
    ]
    log.info("quarterly_symbols_fetched", count=len(quarterly), symbols=quarterly)
    return quarterly


async def run_lifecycle_check(
    current_symbols: list[str],
    ws_subscribe_cb,
    ws_unsubscribe_cb,
    notify_cb=None,
) -> list[str]:
    """새 분기물 구독, 만기 분기물 구독 해제.

    Returns updated symbol list.
    """
    active = await fetch_active_quarterly_symbols()

    new_symbols = set(active) - set(current_symbols)
    expired_symbols = set(current_symbols) - set(active)

    for sym in new_symbols:
        await ws_subscribe_cb(f"kline.1.{sym}")
        await ws_subscribe_cb(f"tickers.{sym}")
        log.info("quarterly_symbol_added", symbol=sym)
        if notify_cb:
            await notify_cb(f"신규 분기물 추가: {sym}")

    for sym in expired_symbols:
        await ws_unsubscribe_cb(f"kline.1.{sym}")
        await ws_unsubscribe_cb(f"tickers.{sym}")
        log.info("quarterly_symbol_expired", symbol=sym)
        if notify_cb:
            await notify_cb(f"만기 분기물 제거: {sym}")

    return active


if __name__ == "__main__":
    async def main():
        symbols = await fetch_active_quarterly_symbols()
        print(f"Active quarterly symbols: {symbols}")

    asyncio.run(main())
