"""Model Manager — rate-limited Claude Code invocation manager.

Wraps :class:`ClaudeCodeBridge` with token-bucket rate limiting,
success/failure tracking, and a health-check endpoint.  On failure the
manager logs a warning and returns ``None`` so that downstream strategies
can continue independently.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import structlog

from services.llm_advisor.claude_bridge import ClaudeCodeBridge

log = structlog.get_logger(__name__)

_DEFAULT_RPM = 10  # requests per minute


@dataclass
class _Stats:
    """Mutable invocation statistics."""

    total: int = 0
    success: int = 0
    failure: int = 0
    last_success_ts: float | None = None
    last_failure_ts: float | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def record_success(self) -> None:
        async with self._lock:
            self.total += 1
            self.success += 1
            self.last_success_ts = time.time()

    async def record_failure(self) -> None:
        async with self._lock:
            self.total += 1
            self.failure += 1
            self.last_failure_ts = time.time()

    @property
    def success_rate(self) -> float:
        return self.success / self.total if self.total else 0.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "success": self.success,
            "failure": self.failure,
            "success_rate": round(self.success_rate, 4),
            "last_success_ts": self.last_success_ts,
            "last_failure_ts": self.last_failure_ts,
        }


class ModelManager:
    """Manages Claude Code invocations with rate limiting and tracking."""

    def __init__(
        self,
        bridge: ClaudeCodeBridge,
        *,
        requests_per_minute: int = _DEFAULT_RPM,
    ) -> None:
        self._bridge = bridge
        self._stats = _Stats()

        # Token-bucket rate limiter
        self._rpm = requests_per_minute
        self._semaphore = asyncio.Semaphore(requests_per_minute)
        self._refill_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the rate-limit refill loop."""
        if self._refill_task is None:
            self._refill_task = asyncio.create_task(
                self._refill_loop(), name="model-manager-refill"
            )

    async def stop(self) -> None:
        """Stop the refill loop."""
        if self._refill_task and not self._refill_task.done():
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            self._refill_task = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def invoke(
        self,
        task: str,
        context: dict[str, Any] | None = None,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any] | None:
        """Rate-limited invocation.  Returns ``None`` on failure."""
        # Acquire a rate-limit token (blocks until available)
        await self._semaphore.acquire()

        try:
            result = await self._bridge.invoke(task, context, timeout=timeout)
        except Exception as exc:
            await self._stats.record_failure()
            log.warning(
                "model_manager_invocation_failed",
                error=str(exc),
                stats=self._stats.snapshot(),
            )
            return None

        if result is None:
            await self._stats.record_failure()
            log.warning(
                "model_manager_no_result",
                stats=self._stats.snapshot(),
            )
            return None

        await self._stats.record_success()
        log.debug(
            "model_manager_invocation_ok",
            stats=self._stats.snapshot(),
        )
        return result

    def health_check(self) -> dict[str, Any]:
        """Return health information about the model manager."""
        stats = self._stats.snapshot()
        healthy = (
            stats["total"] == 0
            or stats["success_rate"] >= 0.5
        )
        return {
            "healthy": healthy,
            "stats": stats,
            "rate_limit_rpm": self._rpm,
        }

    @property
    def stats(self) -> dict[str, Any]:
        """Return a snapshot of invocation statistics."""
        return self._stats.snapshot()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _refill_loop(self) -> None:
        """Refill one rate-limit token per ``60 / rpm`` seconds."""
        interval = 60.0 / self._rpm
        while True:
            try:
                await asyncio.sleep(interval)
                # Release one token if below the max
                if self._semaphore._value < self._rpm:  # noqa: SLF001
                    self._semaphore.release()
            except asyncio.CancelledError:
                break
