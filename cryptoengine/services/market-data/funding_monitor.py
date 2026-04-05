"""Funding Rate Monitor — tracks funding rates and alerts on extremes.

Responsibilities:
  - Track Bybit predicted + confirmed 8h funding rate (via WS ticker)
  - Multi-exchange comparison via CoinGlass REST API
  - Publish to Redis channel ``market:funding:{exchange}:{symbol}``
  - Alert when funding rate exceeds configurable thresholds
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import aiohttp
import asyncpg
import redis.asyncio as aioredis
import structlog

from shared.log_events import *

log = structlog.get_logger(__name__)

COINGLASS_BASE_URL = "https://open-api.coinglass.com/public/v2"

# Default thresholds (annualised basis points)
FUNDING_WARN_THRESHOLD = 0.0005     # 0.05% per 8h  (~21.9% APR)
FUNDING_CRITICAL_THRESHOLD = 0.001  # 0.10% per 8h  (~43.8% APR)

BYBIT_REST_MAINNET = "https://api.bybit.com"
BYBIT_REST_TESTNET = "https://api-testnet.bybit.com"

POLL_INTERVAL_BYBIT = 60         # seconds — Bybit REST funding
POLL_INTERVAL_COINGLASS = 300    # seconds — multi-exchange comparison

# Gap recovery: Bybit perpetuals fund every 8 hours; backfill cap on startup
FUNDING_INTERVAL_HOURS = 8
BACKFILL_MAX_DAYS = 3  # cap at 3 days to avoid excessive API calls on long outages


class FundingMonitor:
    """Monitors funding rates from Bybit and CoinGlass."""

    def __init__(
        self,
        *,
        exchange: str,
        symbol: str,
        api_key: str,
        api_secret: str,
        testnet: bool,
        coinglass_api_key: str,
        redis: aioredis.Redis,
        db_pool: asyncpg.Pool,
        warn_threshold: float = FUNDING_WARN_THRESHOLD,
        critical_threshold: float = FUNDING_CRITICAL_THRESHOLD,
    ) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.coinglass_api_key = coinglass_api_key
        self.redis = redis
        self.db_pool = db_pool
        self.warn_threshold = warn_threshold
        self.critical_threshold = critical_threshold

        self._rest_base = BYBIT_REST_TESTNET if testnet else BYBIT_REST_MAINNET
        self._last_persisted_funding_time: str | None = None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, shutdown: asyncio.Event) -> None:
        """Launch concurrent funding-rate pollers."""
        log.info(SERVICE_STARTED, message="funding monitor starting", symbol=self.symbol)
        await self.backfill_funding_gaps()

        tasks = [
            asyncio.create_task(self._poll_loop(shutdown, self._poll_bybit_funding, POLL_INTERVAL_BYBIT), name="bybit_funding"),
        ]
        if self.coinglass_api_key:
            tasks.append(
                asyncio.create_task(
                    self._poll_loop(shutdown, self._poll_coinglass, POLL_INTERVAL_COINGLASS),
                    name="coinglass_funding",
                )
            )
        else:
            log.warning(SERVICE_HEALTH_FAIL, message="CoinGlass API key missing, multi-exchange funding comparison disabled")

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info(SERVICE_STOPPED, message="funding monitor stopped")

    # ------------------------------------------------------------------
    # Startup gap recovery
    # ------------------------------------------------------------------

    async def backfill_funding_gaps(self) -> None:
        """Detect missing funding rate records since last persisted entry and backfill via REST.

        Uses Bybit's /v5/market/funding/history endpoint (confirmed rates only).
        Caps backfill at BACKFILL_MAX_DAYS (3 days) on startup.
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        funding_interval_ms = FUNDING_INTERVAL_HOURS * 3600 * 1000
        max_lookback_ms = BACKFILL_MAX_DAYS * 24 * 3600 * 1000

        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MAX(timestamp) AS last_ts FROM funding_rate_history "
                "WHERE exchange = $1 AND symbol = $2",
                self.exchange, self.symbol,
            )

        if row is None or row["last_ts"] is None:
            log.info(
                SERVICE_STARTED,
                message="no existing funding rate data, skipping backfill",
                symbol=self.symbol,
            )
            return

        last_ms = int(row["last_ts"].timestamp() * 1000)
        gap_ms = now_ms - last_ms

        if gap_ms <= funding_interval_ms:
            return  # At most ~8h missing — will be filled by next poll

        start_ms = max(last_ms + funding_interval_ms, now_ms - max_lookback_ms)
        gap_hours = gap_ms / 3_600_000

        log.info(
            SERVICE_STARTED,
            message="funding rate gap detected, backfilling",
            gap_hours=round(gap_hours, 1), symbol=self.symbol,
            backfill_from=datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
        )

        url = f"{self._rest_base}/v5/market/funding/history"
        total = 0
        cursor_ms = start_ms

        async with aiohttp.ClientSession() as session:
            while cursor_ms < now_ms:
                params = {
                    "category": "linear",
                    "symbol": self.symbol,
                    "startTime": cursor_ms,
                    "endTime": now_ms,
                    "limit": 200,
                }
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        data = await resp.json()
                except Exception as exc:
                    log.error(SERVICE_HEALTH_FAIL, message="funding history fetch error", exc=str(exc))
                    break

                if data.get("retCode") != 0:
                    log.warning(SERVICE_HEALTH_FAIL, message="funding history API error", response=data)
                    break

                items = data.get("result", {}).get("list", [])
                if not items:
                    break

                # Bybit returns newest-first; reverse for chronological processing
                items = list(reversed(items))

                records = []
                for item in items:
                    try:
                        ts_ms = int(item["fundingRateTimestamp"])
                        if ts_ms < start_ms:
                            continue
                        ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                        rate = float(item["fundingRate"])
                        records.append((self.exchange, self.symbol, rate, rate, ts_dt))
                    except (KeyError, ValueError, TypeError):
                        continue

                if records:
                    async with self.db_pool.acquire() as conn:
                        await conn.executemany(
                            """
                            INSERT INTO funding_rate_history
                                (exchange, symbol, rate, predicted_rate, timestamp)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (exchange, symbol, timestamp) DO NOTHING
                            """,
                            records,
                        )
                    total += len(records)

                if len(items) < 200:
                    break

                # Advance cursor past the newest item in this batch
                cursor_ms = int(items[-1]["fundingRateTimestamp"]) + funding_interval_ms
                await asyncio.sleep(0.1)

        log.info(
            SERVICE_STARTED,
            message="funding rate backfill complete",
            records_inserted=total, symbol=self.symbol,
        )

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    async def _poll_loop(
        self,
        shutdown: asyncio.Event,
        poll_fn,
        interval: float,
    ) -> None:
        while not shutdown.is_set():
            try:
                await poll_fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="funding poll error", poller=poll_fn.__name__)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    # Bybit REST funding
    # ------------------------------------------------------------------

    async def _poll_bybit_funding(self) -> None:
        """Fetch current and predicted funding rate from Bybit V5."""
        url = f"{self._rest_base}/v5/market/tickers"
        params = {"category": "linear", "symbol": self.symbol}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

        if data.get("retCode") != 0:
            log.warning(SERVICE_HEALTH_FAIL, message="Bybit funding API error", response=data)
            return

        tickers = data.get("result", {}).get("list", [])
        if not tickers:
            return

        ticker = tickers[0]
        rate_str = ticker.get("fundingRate")
        next_time_str = ticker.get("nextFundingTime")

        if rate_str is None:
            return

        rate = float(rate_str)
        next_funding_time = datetime.fromtimestamp(
            int(next_time_str) / 1000, tz=timezone.utc
        ) if next_time_str else datetime.now(tz=timezone.utc)

        funding_msg: dict[str, Any] = {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "rate": rate,
            "predicted_rate": rate,
            "next_funding_time": next_funding_time.isoformat(),
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
        }

        channel = f"market:funding:{self.exchange}:{self.symbol}"
        await self.redis.publish(channel, json.dumps(funding_msg))
        await self.redis.hset(f"cache:funding:{self.exchange}:{self.symbol}", mapping={
            "rate": str(rate),
            "next_funding_time": next_funding_time.isoformat(),
        })

        # Persist (deduplicate by next_funding_time)
        funding_key = next_funding_time.isoformat()
        if funding_key != self._last_persisted_funding_time:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO funding_rate_history (exchange, symbol, rate, predicted_rate, timestamp)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (exchange, symbol, timestamp) DO UPDATE
                    SET rate = EXCLUDED.rate, predicted_rate = EXCLUDED.predicted_rate
                    """,
                    self.exchange,
                    self.symbol,
                    rate,
                    rate,
                    next_funding_time,
                )
            self._last_persisted_funding_time = funding_key

        # Alert check
        await self._check_alerts(rate, funding_msg)

        log.debug(MARKET_FUNDING_RATE, message="Bybit funding polled", rate=rate)

    # ------------------------------------------------------------------
    # CoinGlass multi-exchange
    # ------------------------------------------------------------------

    async def _poll_coinglass(self) -> None:
        """Fetch funding rates across exchanges from CoinGlass."""
        url = f"{COINGLASS_BASE_URL}/funding"
        headers = {"coinglassSecret": self.coinglass_api_key}
        params = {"symbol": "BTC", "time_type": "all"}

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    log.warning(SERVICE_HEALTH_FAIL, message="CoinGlass rate limited")
                    return
                data = await resp.json()

        if data.get("code") != "0" and data.get("success") is not True:
            log.warning(SERVICE_HEALTH_FAIL, message="CoinGlass API error", response=data)
            return

        exchange_rates: dict[str, float] = {}
        for item in data.get("data", []):
            ex_name = item.get("exchangeName", "").lower()
            fr = item.get("uMarginList", [{}])
            if fr and len(fr) > 0:
                rate_val = fr[0].get("rate")
                if rate_val is not None:
                    exchange_rates[ex_name] = float(rate_val)

        if not exchange_rates:
            return

        # Publish per-exchange rates
        for ex, rate in exchange_rates.items():
            channel = f"market:funding:{ex}:{self.symbol}"
            msg = {
                "exchange": ex,
                "symbol": self.symbol,
                "rate": rate,
                "source": "coinglass",
                "collected_at": datetime.now(tz=timezone.utc).isoformat(),
            }
            await self.redis.publish(channel, json.dumps(msg))

        # Publish aggregated comparison
        agg_channel = "market:funding:comparison"
        agg_msg = {
            "symbol": self.symbol,
            "rates": exchange_rates,
            "spread": max(exchange_rates.values()) - min(exchange_rates.values()) if len(exchange_rates) > 1 else 0.0,
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        await self.redis.publish(agg_channel, json.dumps(agg_msg))
        await self.redis.set(
            f"cache:funding:comparison:{self.symbol}",
            json.dumps(agg_msg),
            ex=600,
        )

        log.debug(
            MARKET_FUNDING_RATE,
            message="CoinGlass funding polled",
            exchanges=len(exchange_rates),
            spread=agg_msg["spread"],
        )

    # ------------------------------------------------------------------
    # Alerting
    # ------------------------------------------------------------------

    async def _check_alerts(self, rate: float, funding_msg: dict[str, Any]) -> None:
        """Publish alerts when funding rate exceeds thresholds."""
        abs_rate = abs(rate)

        if abs_rate >= self.critical_threshold:
            level = "critical"
        elif abs_rate >= self.warn_threshold:
            level = "warning"
        else:
            return

        direction = "positive (longs pay shorts)" if rate > 0 else "negative (shorts pay longs)"
        alert = {
            "level": level,
            "type": "funding_rate",
            "exchange": self.exchange,
            "symbol": self.symbol,
            "rate": rate,
            "direction": direction,
            "threshold": self.critical_threshold if level == "critical" else self.warn_threshold,
            "message": (
                f"Funding rate {level.upper()}: {rate:.6f} ({direction}) "
                f"on {self.exchange} {self.symbol}"
            ),
            "ts": datetime.now(tz=timezone.utc).isoformat(),
        }

        await self.redis.publish("alerts:funding", json.dumps(alert))
        log.warning(
            MARKET_FUNDING_RATE,
            message="funding rate alert",
            level=level,
            rate=rate,
            exchange=self.exchange,
            symbol=self.symbol,
        )
