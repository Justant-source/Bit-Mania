"""LLM Advisor — entry point.

Schedules periodic analysis every 4 hours and subscribes to the
llm:request Redis channel for on-demand analysis requests.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

import structlog
import yaml

from services.llm_advisor.agent_graph import TradingAnalysisGraph
from services.llm_advisor.claude_bridge import ClaudeCodeBridge
from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.reflection import DailyReflection
from services.llm_advisor.vision_chart import ChartAnalyzer

log = structlog.get_logger(__name__)

ANALYSIS_INTERVAL_HOURS = 4
REQUEST_CHANNEL = "llm:request"
ADVISORY_CHANNEL = "llm:advisory"


def _load_config() -> dict[str, Any]:
    """Load LLM advisor configuration."""
    config_path = os.getenv("CONFIG_PATH", "/app/config/orchestrator.yaml")
    try:
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        cfg = {}

    cfg.setdefault("redis", {})["url"] = os.getenv("REDIS_URL", "redis://localhost:6379")
    cfg["claude_code_path"] = os.getenv("CLAUDE_CODE_PATH", "/usr/local/bin/claude")
    return cfg


def _configure_logging() -> None:
    """Set up structlog with JSON rendering."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


class LLMAdvisorService:
    """Manages the LLM advisory lifecycle."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._redis_url = config.get("redis", {}).get("url", "redis://localhost:6379")
        self._redis: Any = None

        self._bridge = ClaudeCodeBridge(config.get("claude_code_path", "claude"))
        self._model_manager = ModelManager(self._bridge)
        self._chart_analyzer = ChartAnalyzer()
        self._analysis_graph: TradingAnalysisGraph | None = None
        self._reflection: DailyReflection | None = None

        self._running = False
        self._analysis_task: asyncio.Task[None] | None = None
        self._request_task: asyncio.Task[None] | None = None
        self._reflection_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize connections and start scheduled tasks."""
        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        log.info("llm_advisor_redis_connected")

        self._analysis_graph = TradingAnalysisGraph(
            self._model_manager, self._chart_analyzer
        )
        self._reflection = DailyReflection(self._redis)

        self._running = True
        self._analysis_task = asyncio.create_task(
            self._scheduled_analysis_loop(), name="scheduled-analysis"
        )
        self._request_task = asyncio.create_task(
            self._subscribe_requests(), name="request-subscriber"
        )
        self._reflection_task = asyncio.create_task(
            self._daily_reflection_loop(), name="daily-reflection"
        )
        log.info("llm_advisor_started")

    async def stop(self) -> None:
        """Shut down all tasks."""
        self._running = False
        for task in (self._analysis_task, self._request_task, self._reflection_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._redis:
            await self._redis.aclose()
        log.info("llm_advisor_stopped")

    async def _scheduled_analysis_loop(self) -> None:
        """Run full analysis every 4 hours."""
        while self._running:
            try:
                await self._run_analysis("scheduled")
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("scheduled_analysis_error")
            await asyncio.sleep(ANALYSIS_INTERVAL_HOURS * 3600)

    async def _subscribe_requests(self) -> None:
        """Listen for on-demand analysis requests on Redis pub/sub."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(REQUEST_CHANNEL)
        log.info("llm_request_subscribed", channel=REQUEST_CHANNEL)

        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                try:
                    request = json.loads(message["data"])
                    trigger = request.get("trigger", "on_demand")
                    await self._run_analysis(trigger, request)
                except json.JSONDecodeError:
                    log.warning("invalid_request_data")
                except Exception:
                    log.exception("on_demand_analysis_error")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(REQUEST_CHANNEL)
            await pubsub.aclose()

    async def _run_analysis(
        self, trigger: str, request: dict[str, Any] | None = None
    ) -> None:
        """Execute the full analysis pipeline and publish advisory."""
        log.info("analysis_started", trigger=trigger)

        # Gather market context from Redis
        context = await self._gather_market_context()
        if not context:
            log.warning("no_market_context_available")
            return

        # Run the analysis graph
        assert self._analysis_graph is not None
        result = await self._analysis_graph.run(context)

        if result is None:
            log.warning("analysis_produced_no_result")
            return

        # Publish advisory to orchestrator
        advisory = {
            "rating": result.get("rating", "hold"),
            "confidence": result.get("confidence", 0.0),
            "weight_adjustments": result.get("weight_adjustments", {}),
            "reasoning": result.get("reasoning", ""),
            "regime_assessment": result.get("regime_assessment", ""),
            "trigger": trigger,
        }

        await self._redis.publish(ADVISORY_CHANNEL, json.dumps(advisory))
        await self._redis.set(
            "llm:latest_advisory", json.dumps(advisory), ex=28800  # 8 hours
        )

        log.info(
            "analysis_complete",
            rating=advisory["rating"],
            confidence=advisory["confidence"],
        )

    async def _gather_market_context(self) -> dict[str, Any]:
        """Collect market data from Redis for analysis context."""
        context: dict[str, Any] = {}

        keys_to_fetch = {
            "regime": "market:regime:current",
            "features": "features:latest",
            "portfolio": "cache:portfolio_state",
            "btc_price": "market:ticker:BTCUSDT",
            "funding_rate": "market:funding:BTCUSDT",
            "orderbook_depth": "market:orderbook_summary:BTCUSDT",
        }

        for name, key in keys_to_fetch.items():
            raw = await self._redis.get(key)
            if raw:
                try:
                    context[name] = json.loads(raw)
                except json.JSONDecodeError:
                    context[name] = raw

        return context

    async def _daily_reflection_loop(self) -> None:
        """Run daily reflection at UTC 00:00."""
        while self._running:
            try:
                now = asyncio.get_event_loop().time()
                from datetime import datetime, timezone

                utc_now = datetime.now(timezone.utc)
                # Calculate seconds until next UTC midnight
                next_midnight = utc_now.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                if utc_now >= next_midnight:
                    from datetime import timedelta

                    next_midnight += timedelta(days=1)
                wait_seconds = (next_midnight - utc_now).total_seconds()
                await asyncio.sleep(wait_seconds)

                if self._reflection and self._running:
                    await self._reflection.run_daily_reflection()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("daily_reflection_error")
                await asyncio.sleep(3600)


async def main() -> None:
    """Start the LLM Advisor service."""
    _configure_logging()
    config = _load_config()
    log.info("llm_advisor_starting")

    service = LLMAdvisorService(config)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        await service.start()
        await shutdown_event.wait()
    finally:
        await service.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
