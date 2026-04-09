"""Base class for external data sources."""

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class SourceHealth:
    """Health status of a data source fetch."""
    status: Literal["HEALTHY", "DEGRADED", "BROKEN"]
    last_success_at: datetime | None = None
    failure_reason: str | None = None
    failure_stage: str | None = None  # http|structural|schema|sanity|unhandled
    fields_available: list[str] = field(default_factory=list)
    fields_missing: list[str] = field(default_factory=list)
    raw_error: str | None = None  # first 500 chars of error for debugging


class BaseDataSource(ABC):
    """Abstract base for all external data sources.

    Subclasses must implement fetch_raw() and summarize().
    Caching is handled automatically via Redis.
    """

    SOURCE_NAME: str = "unknown"
    CACHE_KEY_PREFIX: str = "datasource"
    CACHE_TTL_SECONDS: int = 900  # 15 min default

    def __init__(self, redis_client=None, http_session: aiohttp.ClientSession | None = None):
        self._redis = redis_client
        self._http_session = http_session
        self._own_session = False

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "CryptoEngine-LLMAdvisor/1.0"},
            )
            self._own_session = True
        return self._http_session

    async def close(self):
        if self._own_session and self._http_session and not self._http_session.closed:
            await self._http_session.close()

    @abstractmethod
    async def fetch_raw(self) -> dict:
        """Fetch raw data from external API. Subclasses implement this."""
        ...

    @abstractmethod
    def summarize(self, raw: dict) -> dict:
        """Transform raw data into LLM-ready summary dict.

        Return rules:
        - All numbers rounded to 2 decimal places
        - Must include 'narrative' field (1-2 sentence Korean/English summary)
        - Must include 'as_of', 'source', 'confidence' fields
        """
        ...

    def _cache_key(self) -> str:
        return f"{self.CACHE_KEY_PREFIX}:{self.SOURCE_NAME}:raw"

    async def _cached_fetch(self) -> dict:
        """Fetch with Redis caching. Returns raw data dict."""
        # Try cache first
        if self._redis:
            try:
                cached = await self._redis.get(self._cache_key())
                if cached:
                    logger.debug("cache_hit", source=self.SOURCE_NAME)
                    return json.loads(cached)
            except Exception as e:
                logger.warning("cache_read_error", source=self.SOURCE_NAME, error=str(e))

        # Cache miss — fetch from API
        raw = await self.fetch_raw()

        # Store in cache
        if self._redis:
            try:
                await self._redis.set(
                    self._cache_key(),
                    json.dumps(raw, default=str),
                    ex=self.CACHE_TTL_SECONDS,
                )
                logger.debug("cache_stored", source=self.SOURCE_NAME, ttl=self.CACHE_TTL_SECONDS)
            except Exception as e:
                logger.warning("cache_write_error", source=self.SOURCE_NAME, error=str(e))

        return raw

    async def get_context(self) -> tuple[dict, SourceHealth]:
        """Main entry point: fetch (cached) + summarize. Returns (summary_dict, health)."""
        try:
            raw = await self._cached_fetch()
            summary = self.summarize(raw)
            health = SourceHealth(
                status="HEALTHY",
                last_success_at=datetime.now(timezone.utc),
                fields_available=list(summary.keys()),
            )
            logger.info("context_ready", source=self.SOURCE_NAME, confidence=summary.get("confidence", 0))
            return summary, health
        except Exception as e:
            logger.error("context_fetch_failed", source=self.SOURCE_NAME, error=str(e))
            health = SourceHealth(
                status="BROKEN",
                failure_reason=str(e)[:500],
                failure_stage="unhandled",
            )
            return self._fallback_context(str(e)), health

    def _fallback_context(self, error_msg: str = "") -> dict:
        """Return a safe fallback when data fetch fails."""
        return {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "source": self.SOURCE_NAME,
            "confidence": 0.0,
            "narrative": f"데이터 없음 ({self.SOURCE_NAME}): {error_msg[:100]}",
            "_error": True,
        }
