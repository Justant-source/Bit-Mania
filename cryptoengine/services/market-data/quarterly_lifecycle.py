"""분기물 만기/신규 자동 처리.

Bybit은 분기 만료 시 신규 분기물 자동 출시. 수집기는 기동 시·주기적으로
활성 LinearFutures(분기 MAR/JUN/SEP/DEC)만 구독한다.
만기된 심볼을 하드코딩해 두면 Bybit이 subscribe 배치 전체를 거부해
BTCUSDT OHLCV까지 중단된다.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any

import aiohttp
import structlog

from shared.log_events import MARKET_TICKER_RECEIVED, SERVICE_HEALTH_FAIL

log = structlog.get_logger(__name__)

BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"
# True quarterlies only — exclude weeklies/monthlies (e.g. 14AUG26, 21AUG26).
_QUARTERLY_RE = re.compile(r"^BTCUSDT-\d{2}(MAR|JUN|SEP|DEC)\d{2}$")

# Fallback when REST instruments-info is unavailable (nearest open quarterlies).
FALLBACK_QUARTERLY_SYMBOLS: list[str] = [
    "BTCUSDT-25SEP26",
    "BTCUSDT-25DEC26",
    "BTCUSDT-26MAR27",
    "BTCUSDT-25JUN27",
]


async def fetch_active_quarterly_symbols(
    *,
    rest_base: str = "https://api.bybit.com",
    session: aiohttp.ClientSession | None = None,
) -> list[str]:
    """활성 BTC USDT 분기물 심볼을 Bybit instruments-info에서 조회."""
    url = f"{rest_base}/v5/market/instruments-info"
    params = {"category": "linear", "baseCoin": "BTC"}
    owns_session = session is None
    if owns_session:
        session = aiohttp.ClientSession()
    assert session is not None
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            payload: dict[str, Any] = await resp.json()
    finally:
        if owns_session:
            await session.close()

    if payload.get("retCode") != 0:
        raise RuntimeError(f"instruments-info error: {payload}")

    instruments = payload.get("result", {}).get("list", []) or []
    quarterly = sorted(
        inst["symbol"]
        for inst in instruments
        if inst.get("contractType") == "LinearFutures"
        and inst.get("status") == "Trading"
        and _QUARTERLY_RE.match(inst.get("symbol", ""))
    )
    log.info(
        MARKET_TICKER_RECEIVED,
        message="quarterly symbols fetched",
        count=len(quarterly),
        symbols=quarterly,
    )
    return quarterly


async def resolve_quarterly_symbols(*, rest_base: str = "https://api.bybit.com") -> list[str]:
    """Fetch active quarterlies; fall back to known open contracts on failure."""
    try:
        symbols = await fetch_active_quarterly_symbols(rest_base=rest_base)
        if symbols:
            return symbols
        log.warning(
            SERVICE_HEALTH_FAIL,
            message="no active quarterly symbols from API, using fallback",
            fallback=FALLBACK_QUARTERLY_SYMBOLS,
        )
    except Exception as exc:
        log.warning(
            SERVICE_HEALTH_FAIL,
            message="quarterly symbol fetch failed, using fallback",
            exc=str(exc),
            fallback=FALLBACK_QUARTERLY_SYMBOLS,
        )
    return list(FALLBACK_QUARTERLY_SYMBOLS)


async def run_lifecycle_check(
    current_symbols: list[str],
    ws_subscribe_cb,
    ws_unsubscribe_cb,
    notify_cb=None,
    *,
    rest_base: str = "https://api.bybit.com",
) -> list[str]:
    """새 분기물 구독, 만기 분기물 구독 해제.

    Returns updated symbol list.
    """
    active = await resolve_quarterly_symbols(rest_base=rest_base)

    new_symbols = set(active) - set(current_symbols)
    expired_symbols = set(current_symbols) - set(active)

    for sym in sorted(new_symbols):
        await ws_subscribe_cb(f"kline.1.{sym}")
        await ws_subscribe_cb(f"tickers.{sym}")
        log.info(MARKET_TICKER_RECEIVED, message="quarterly symbol added", symbol=sym)
        if notify_cb:
            await notify_cb(f"신규 분기물 추가: {sym}")

    for sym in sorted(expired_symbols):
        await ws_unsubscribe_cb(f"kline.1.{sym}")
        await ws_unsubscribe_cb(f"tickers.{sym}")
        log.info(MARKET_TICKER_RECEIVED, message="quarterly symbol expired", symbol=sym)
        if notify_cb:
            await notify_cb(f"만기 분기물 제거: {sym}")

    return active


if __name__ == "__main__":
    async def main():
        symbols = await resolve_quarterly_symbols()
        print(f"Active quarterly symbols: {symbols}")

    asyncio.run(main())
