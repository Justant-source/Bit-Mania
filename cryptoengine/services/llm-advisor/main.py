"""LLM Advisor — entry point.

Schedules periodic analysis every 4 hours and subscribes to the
llm:request Redis channel for on-demand analysis requests.
Full analysis reports are persisted to the ``llm_reports`` table
for dashboard browsing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import structlog
import yaml

from shared.log_events import *
from shared.logging_config import setup_logging
from shared.log_writer import init_log_writer, close_log_writer
from services.llm_advisor.agent_graph import TradingAnalysisGraph
from services.llm_advisor.claude_bridge import ClaudeCodeBridge
from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.prompt_templates.asset_report import ASSET_REPORT_PROMPT
from services.llm_advisor.reflection import DailyReflection
from services.llm_advisor.vision_chart import ChartAnalyzer
from shared.db.connection import create_pool, close_pool, get_pool

log = structlog.get_logger(__name__)

SERVICE_NAME = "llm-advisor"
ANALYSIS_INTERVAL_HOURS = 6
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

        # Database pool must be created before setup_logging so db_pool can be passed
        db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://cryptoengine:cryptoengine@localhost:5432/cryptoengine",
        )
        await create_pool(dsn=db_url)
        pool = get_pool()
        await init_log_writer(SERVICE_NAME, pool)
        setup_logging(service_name=SERVICE_NAME, db_pool=pool)

        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        log.info(REDIS_CONNECTED, message="LLM Advisor Redis 연결 성공")
        log.info(DB_POOL_CREATED, message="LLM Advisor DB 풀 생성 완료")

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
        log.info(SERVICE_STARTED, message="LLM Advisor 서비스 시작")

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
        log.info(SERVICE_STOPPED, message="LLM Advisor 서비스 종료")
        await close_pool()
        await close_log_writer()

    async def _scheduled_analysis_loop(self) -> None:
        """Run full analysis every 6 hours."""
        while self._running:
            try:
                await self._run_analysis("scheduled")
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(LLM_API_ERROR, message="정기 분석 오류")
            await asyncio.sleep(ANALYSIS_INTERVAL_HOURS * 3600)

    async def _subscribe_requests(self) -> None:
        """Listen for on-demand analysis requests on Redis pub/sub."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(REQUEST_CHANNEL)
        log.info(REDIS_CONNECTED, message="LLM 요청 채널 구독 시작", channel=REQUEST_CHANNEL)

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
                    log.warning(LLM_API_ERROR, message="잘못된 요청 데이터")
                except Exception:
                    log.exception(LLM_API_ERROR, message="온디맨드 분석 오류")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(REQUEST_CHANNEL)
            await pubsub.aclose()

    async def _run_analysis(
        self, trigger: str, request: dict[str, Any] | None = None
    ) -> None:
        """Execute the full analysis pipeline, persist report, and publish advisory."""
        log.info(LLM_ANALYSIS_START, message="분석 시작", trigger=trigger)

        # Gather market context from Redis
        context = await self._gather_market_context()
        if not context:
            log.warning(LLM_API_ERROR, message="시장 컨텍스트 없음")
            return

        # Run the analysis graph
        assert self._analysis_graph is not None
        result = await self._analysis_graph.run(context)

        if result is None:
            log.warning(LLM_API_ERROR, message="분석 결과 없음")
            return

        rating = result.get("rating", "hold")
        confidence = result.get("confidence", 0.0)

        # Publish advisory to orchestrator
        advisory = {
            "rating": rating,
            "confidence": confidence,
            "weight_adjustments": result.get("weight_adjustments", {}),
            "reasoning": result.get("reasoning", ""),
            "regime_assessment": result.get("regime_assessment", ""),
            "trigger": trigger,
        }

        await self._redis.publish(ADVISORY_CHANNEL, json.dumps(advisory))
        await self._redis.set(
            "llm:latest_advisory", json.dumps(advisory), ex=21600  # 6 hours
        )

        # Generate 6-hour Korean narrative asset report
        asset_report = await self._generate_asset_report(result, context)

        # Persist full report to DB
        await self._save_report(trigger, result, context, asset_report)

        log.info(
            LLM_ANALYSIS_COMPLETE,
            message="분석 완료",
            rating=advisory["rating"],
            confidence=advisory["confidence"],
        )

    async def _generate_asset_report(
        self,
        result: dict[str, Any],
        context: dict[str, Any],
    ) -> str | None:
        """Generate a comprehensive 6-hour Korean narrative asset report."""
        try:
            # Extract BTC price
            ticker = context.get("btc_price", {})
            if isinstance(ticker, dict):
                btc_price = ticker.get("last") or ticker.get("close") or "N/A"
            else:
                btc_price = ticker or "N/A"

            # Extract 24h change from features
            features = context.get("features", {})
            price_change_24h = features.get("price_change_24h", "N/A")
            if isinstance(price_change_24h, float):
                price_change_24h = f"{price_change_24h:.2f}"

            # Extract funding rate
            funding = context.get("funding_rate", {})
            if isinstance(funding, dict):
                funding_rate = funding.get("rate") or funding.get("funding_rate") or "N/A"
            else:
                funding_rate = funding or "N/A"
            if isinstance(funding_rate, float):
                funding_rate = f"{funding_rate:.6f}"

            regime = result.get("regime_assessment") or context.get("regime", "unknown")
            rating = result.get("rating", "hold")
            confidence = result.get("confidence", 0.0)

            portfolio = context.get("portfolio", {})
            if isinstance(portfolio, dict):
                portfolio_state = json.dumps(portfolio, ensure_ascii=False, indent=2)
            else:
                portfolio_state = str(portfolio) if portfolio else "데이터 없음"

            def _fmt(d: Any) -> str:
                if not d:
                    return "데이터 없음"
                if isinstance(d, str):
                    return d
                return json.dumps(d, ensure_ascii=False)

            prompt = ASSET_REPORT_PROMPT.format(
                btc_price=btc_price,
                price_change_24h=price_change_24h,
                funding_rate=funding_rate,
                regime=regime,
                technical_report=_fmt(result.get("_technical_report")),
                sentiment_report=_fmt(result.get("_sentiment_report")),
                bull_argument=_fmt(result.get("_bull_argument")),
                bear_argument=_fmt(result.get("_bear_argument")),
                debate_conclusion=_fmt(result.get("_debate_conclusion")),
                risk_assessment=_fmt(result.get("_risk_assessment")),
                rating=rating,
                confidence=f"{confidence:.2f}",
                portfolio_state=portfolio_state,
            )

            report_data = await self._model_manager.invoke(prompt)
            if report_data is None:
                log.warning(LLM_API_ERROR, message="자산 리포트 생성 결과 없음")
                return None

            # Return full JSON as string for storage; full_report is the narrative
            return json.dumps(report_data, ensure_ascii=False)

        except Exception:
            log.exception(LLM_API_ERROR, message="자산 리포트 생성 오류")
            return None

    async def _save_report(
        self,
        trigger: str,
        result: dict[str, Any],
        context: dict[str, Any],
        asset_report: str | None = None,
    ) -> None:
        """Save the full analysis report to the llm_reports table."""
        try:
            pool = get_pool()
        except RuntimeError:
            log.warning(SERVICE_HEALTH_FAIL, message="DB 풀 없음, 리포트 저장 생략")
            return

        rating = result.get("rating", "hold")
        confidence = result.get("confidence", 0.0)
        regime = result.get("regime_assessment", "")
        reasoning = result.get("reasoning", "")

        # Extract BTC price from context
        btc_price = None
        ticker = context.get("btc_price")
        if isinstance(ticker, dict):
            btc_price = ticker.get("last") or ticker.get("close")
        elif isinstance(ticker, (int, float)):
            btc_price = ticker

        # Build title
        utc_now = datetime.now(timezone.utc)
        title = f"[{rating.upper()}] {regime or 'N/A'} — {utc_now:%Y-%m-%d %H:%M} UTC"

        def _summarise(d: dict | Any) -> str | None:
            if not d:
                return None
            if isinstance(d, str):
                return d
            return json.dumps(d, ensure_ascii=False, default=str)

        try:
            await pool.execute(
                """
                INSERT INTO llm_reports (
                    title, trigger, rating, confidence, regime, symbol,
                    btc_price, technical_summary, sentiment_summary,
                    bull_summary, bear_summary, debate_conclusion,
                    risk_assessment, reasoning, weight_adjustments, risk_flags,
                    asset_report
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14, $15, $16,
                    $17
                )
                """,
                title,
                trigger,
                rating,
                confidence,
                regime or None,
                "BTCUSDT",
                btc_price,
                _summarise(result.get("_technical_report")),
                _summarise(result.get("_sentiment_report")),
                _summarise(result.get("_bull_argument")),
                _summarise(result.get("_bear_argument")),
                _summarise(result.get("_debate_conclusion")),
                _summarise(result.get("_risk_assessment")),
                reasoning or None,
                json.dumps(result.get("weight_adjustments", {})),
                json.dumps(result.get("risk_flags", [])),
                asset_report,
            )
            log.info(LLM_ANALYSIS_COMPLETE, message="LLM 리포트 저장 완료", title=title)
        except Exception:
            log.exception(LLM_API_ERROR, message="LLM 리포트 저장 오류")

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
                log.exception(LLM_API_ERROR, message="일일 반성 오류")
                await asyncio.sleep(3600)


async def main() -> None:
    """Start the LLM Advisor service."""
    setup_logging(service_name=SERVICE_NAME)
    log = structlog.get_logger()
    config = _load_config()
    log.info(SERVICE_STARTED, message="LLM Advisor 서비스 시작 중")

    service = LLMAdvisorService(config)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info(SERVICE_STOPPING, message="종료 신호 수신")
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
