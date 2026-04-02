"""Async Redis client — pub/sub, cache, and key-value helpers."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisClient:
    """Thin async wrapper around ``redis.asyncio``."""

    def __init__(
        self,
        url: str | None = None,
        decode_responses: bool = True,
    ) -> None:
        self._url = url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._decode = decode_responses
        self._redis: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None

    # ── lifecycle ────────────────────────────────────────────────────────

    async def connect(self) -> None:
        if self._redis is not None:
            return
        self._redis = aioredis.from_url(
            self._url,
            decode_responses=self._decode,
        )
        await self._redis.ping()
        logger.info("redis connected (%s)", self._url)

    async def disconnect(self) -> None:
        if self._pubsub is not None:
            await self._pubsub.close()
            self._pubsub = None
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
            logger.info("redis disconnected")

    @property
    def client(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("RedisClient not connected — call connect() first")
        return self._redis

    # ── pub / sub ────────────────────────────────────────────────────────

    async def publish(self, channel: str, message: Any) -> int:
        payload = json.dumps(message) if not isinstance(message, str) else message
        return await self.client.publish(channel, payload)

    async def subscribe(self, *channels: str) -> AsyncIterator[dict[str, Any]]:
        """Yield messages from one or more channels (blocking iterator)."""
        self._pubsub = self.client.pubsub()
        await self._pubsub.subscribe(*channels)
        async for raw_msg in self._pubsub.listen():
            if raw_msg["type"] != "message":
                continue
            data = raw_msg["data"]
            try:
                parsed = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                parsed = data
            yield {
                "channel": raw_msg["channel"],
                "data": parsed,
            }

    # ── key / value ──────────────────────────────────────────────────────

    async def get(self, key: str) -> str | None:
        return await self.client.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> None:
        payload = json.dumps(value) if not isinstance(value, (str, bytes)) else value
        if ttl is not None:
            await self.client.setex(key, ttl, payload)
        else:
            await self.client.set(key, payload)

    # ── cache helpers ────────────────────────────────────────────────────

    async def cache_get(self, key: str) -> Any | None:
        raw = await self.client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    async def cache_set(self, key: str, value: Any, ttl: int = 60) -> None:
        await self.set(key, value, ttl=ttl)

    async def cache_delete(self, key: str) -> None:
        await self.client.delete(key)

    async def cache_exists(self, key: str) -> bool:
        return bool(await self.client.exists(key))
