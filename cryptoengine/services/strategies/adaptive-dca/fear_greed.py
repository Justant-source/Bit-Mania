"""Fear & Greed Index collector with Redis caching.

Fetches the CNN-style Fear & Greed index from Alternative.me and caches
results in Redis with a 1-day TTL to avoid excessive API calls.
"""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog

from shared.log_events import *
from shared.redis_client import RedisClient

logger = structlog.get_logger()

FNG_API_URL = "https://api.alternative.me/fng/"
CACHE_KEY = "fng:current"
HISTORY_KEY_PREFIX = "fng:history"
CACHE_TTL = 86400  # 1 day in seconds


class FearGreedCollector:
    """Fetch and cache the Fear & Greed Index."""

    def __init__(self, redis: RedisClient) -> None:
        self._redis = redis
        self._log = logger.bind(component="fear_greed_collector")

    async def get_current_index(self) -> int:
        """Return the current F&G index value (0-100).

        Checks Redis cache first; fetches from API on cache miss.
        """
        # Try cache
        cached = await self._redis.get(CACHE_KEY)
        if cached is not None:
            try:
                value = int(cached)
                self._log.debug("fng_cache_hit", value=value)
                return value
            except (ValueError, TypeError):
                pass

        # Fetch from API
        value = await self._fetch_from_api()
        if value is not None:
            await self._redis.set(CACHE_KEY, str(value), ttl=CACHE_TTL)
            return value

        self._log.warning("fng_unavailable_returning_default")
        return 50  # neutral default

    async def get_historical(self, days: int = 30) -> list[dict[str, Any]]:
        """Fetch historical F&G data for the last *days*.

        Returns a list of dicts: [{value: int, timestamp: str, classification: str}]
        """
        cache_key = f"{HISTORY_KEY_PREFIX}:{days}"
        cached = await self._redis.get(cache_key)
        if cached is not None:
            try:
                import json
                data = json.loads(cached)
                self._log.debug("fng_history_cache_hit", days=days)
                return data
            except (ValueError, TypeError):
                pass

        try:
            async with aiohttp.ClientSession() as session:
                params = {"limit": days, "format": "json"}
                async with session.get(FNG_API_URL, params=params) as resp:
                    if resp.status != 200:
                        self._log.warning("fng_api_error", status=resp.status)
                        return []

                    body = await resp.json()
                    raw_data = body.get("data", [])

                    result: list[dict[str, Any]] = []
                    for entry in raw_data:
                        result.append(
                            {
                                "value": int(entry.get("value", 50)),
                                "timestamp": entry.get("timestamp", ""),
                                "classification": entry.get(
                                    "value_classification", "Neutral"
                                ),
                            }
                        )

                    # Cache the result
                    import json
                    await self._redis.set(
                        cache_key,
                        json.dumps(result),
                        ttl=CACHE_TTL,
                    )

                    self._log.info("fng_history_fetched", days=days, count=len(result))
                    return result

        except Exception:
            self._log.exception("fng_history_fetch_error")
            return []

    async def _fetch_from_api(self) -> int | None:
        """Fetch the latest F&G index value from Alternative.me."""
        try:
            async with aiohttp.ClientSession() as session:
                params = {"limit": 1, "format": "json"}
                async with session.get(FNG_API_URL, params=params) as resp:
                    if resp.status != 200:
                        self._log.warning("fng_api_error", status=resp.status)
                        return None

                    body = await resp.json()
                    data = body.get("data", [])
                    if not data:
                        self._log.warning("fng_api_empty_response")
                        return None

                    value = int(data[0].get("value", 50))
                    self._log.info(
                        DCA_MULTIPLIER_CALC,
                        message="공포탐욕지수 조회 완료",
                        value=value,
                        classification=data[0].get("value_classification"),
                    )
                    return value

        except Exception:
            self._log.exception("fng_fetch_error")
            return None
