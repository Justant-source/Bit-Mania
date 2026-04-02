"""StrategyOrchestrator — main coordination loop.

Every 5 minutes:
1. Receive market regime from Redis
2. Adjust strategy weights based on regime
3. Evaluate portfolio risk
4. Check kill-switch conditions
5. Issue capital allocation commands to strategies via Redis
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Literal

import redis.asyncio as aioredis
import structlog
from pydantic import BaseModel, Field

from services.orchestrator.dissimilarity_index import DissimilarityIndex
from services.orchestrator.portfolio_monitor import PortfolioMonitor
from services.orchestrator.regime_ml_model import RegimeMLModel
from services.orchestrator.weight_manager import WeightManager

log = structlog.get_logger(__name__)

StrategyName = Literal["funding_arb", "grid", "dca", "cash"]
RegimeType = Literal["trending_up", "trending_down", "ranging", "volatile", "uncertain"]

STRATEGY_CHANNELS: dict[str, str] = {
    "funding_arb": "strategy:funding_arb:command",
    "grid": "strategy:grid:command",
    "dca": "strategy:dca:command",
}


class AllocationCommand(BaseModel):
    """Capital allocation directive sent to a strategy."""

    strategy_id: str
    allocated_capital: float
    weight: float
    regime: str
    max_drawdown: float | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class KillSwitchState(BaseModel):
    """Kill switch status tracking."""

    triggered: bool = False
    reason: str | None = None
    triggered_at: datetime | None = None
    cooldown_until: datetime | None = None


class StrategyOrchestrator:
    """Central coordinator for strategy weight management and risk monitoring."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._orch_config = config.get("orchestrator", {})
        self._loop_interval = self._orch_config.get("loop_interval_seconds", 300)
        self._kill_switch_config = config.get("kill_switch", {})

        self._redis: aioredis.Redis | None = None
        self._weight_manager = WeightManager(config)
        self._portfolio_monitor: PortfolioMonitor | None = None
        self._regime_model: RegimeMLModel | None = None
        self._di_index: DissimilarityIndex | None = None

        self._kill_switch = KillSwitchState()
        self._current_regime: RegimeType = "ranging"
        self._current_weights: dict[str, float] = {}
        self._total_equity: float = 0.0
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._llm_advisory_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize connections and begin the orchestration loop."""
        redis_url = self._config.get("redis", {}).get("url", "redis://localhost:6379")
        self._redis = aioredis.from_url(redis_url, decode_responses=True)
        await self._redis.ping()
        log.info("redis_connected", url=redis_url)

        pg_dsn = self._config.get("postgres", {}).get("dsn")
        self._portfolio_monitor = PortfolioMonitor(self._redis, pg_dsn)
        await self._portfolio_monitor.start()

        ml_config = self._config.get("regime_ml", {})
        if ml_config.get("enabled", True):
            self._regime_model = RegimeMLModel(self._redis, ml_config)
            await self._regime_model.start()
            self._di_index = DissimilarityIndex(ml_config)

        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="orchestrator-loop")
        self._llm_advisory_task = asyncio.create_task(
            self._subscribe_llm_advisory(), name="llm-advisory-sub"
        )
        log.info("orchestrator_started", interval=self._loop_interval)

    async def stop(self) -> None:
        """Gracefully shut down the orchestrator."""
        self._running = False
        for task in (self._task, self._llm_advisory_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._regime_model:
            await self._regime_model.stop()
        if self._portfolio_monitor:
            await self._portfolio_monitor.stop()
        if self._redis:
            await self._redis.aclose()
        log.info("orchestrator_stopped")

    async def _run_loop(self) -> None:
        """Main orchestration loop executing every 5 minutes."""
        while self._running:
            try:
                await self._orchestration_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("orchestration_cycle_error")
            await asyncio.sleep(self._loop_interval)

    async def _orchestration_cycle(self) -> None:
        """Single orchestration cycle."""
        log.info("orchestration_cycle_start")

        # 1. Receive market regime
        regime = await self._get_market_regime()
        self._current_regime = regime
        log.info("regime_detected", regime=regime)

        # 2. Adjust strategy weights based on regime
        target_weights = self._weight_manager.get_target_weights(regime)
        smoothed_weights = self._weight_manager.smooth_transition(
            self._current_weights, target_weights
        )
        self._current_weights = smoothed_weights
        log.info("weights_updated", weights=smoothed_weights, regime=regime)

        # 3. Evaluate portfolio risk
        assert self._portfolio_monitor is not None
        portfolio_state = await self._portfolio_monitor.evaluate()
        self._total_equity = portfolio_state.total_equity
        log.info(
            "portfolio_evaluated",
            equity=portfolio_state.total_equity,
            daily_dd=portfolio_state.daily_drawdown,
            weekly_dd=portfolio_state.weekly_drawdown,
        )

        # 4. Check kill-switch conditions
        kill_triggered = await self._check_kill_switch(portfolio_state)
        if kill_triggered:
            log.critical(
                "kill_switch_triggered",
                reason=self._kill_switch.reason,
                equity=portfolio_state.total_equity,
            )
            await self._execute_kill_switch()
            return

        # Check if kill switch is in cooldown
        if self._kill_switch.cooldown_until:
            now = datetime.now(timezone.utc)
            if now < self._kill_switch.cooldown_until:
                log.warning(
                    "kill_switch_cooldown",
                    until=self._kill_switch.cooldown_until.isoformat(),
                )
                return
            self._kill_switch = KillSwitchState()
            log.info("kill_switch_cooldown_expired")

        # 5. Issue capital allocation commands
        await self._issue_allocations(smoothed_weights, portfolio_state.total_equity)

        # Cache current state in Redis
        await self._cache_orchestrator_state()
        log.info("orchestration_cycle_complete")

    async def _get_market_regime(self) -> RegimeType:
        """Retrieve current market regime from Redis or ML model."""
        assert self._redis is not None

        # Try ML model first
        if self._regime_model:
            try:
                ml_regime, confidence = await self._regime_model.predict()

                # Check dissimilarity index
                if self._di_index and self._di_index.is_uncertain():
                    log.warning("di_threshold_exceeded", regime="uncertain")
                    return "uncertain"

                if confidence > 0.3:
                    return ml_regime
                log.warning("ml_regime_low_confidence", confidence=confidence)
            except Exception:
                log.exception("ml_regime_prediction_failed")

        # Fallback: read from Redis (set by market-data service)
        raw = await self._redis.get("market:regime:current")
        if raw:
            try:
                data = json.loads(raw)
                return data.get("regime", "ranging")
            except (json.JSONDecodeError, KeyError):
                log.warning("invalid_regime_data", raw=raw)

        return "ranging"

    async def _check_kill_switch(self, portfolio_state: Any) -> bool:
        """Evaluate kill-switch conditions against portfolio state."""
        ks = self._kill_switch_config
        max_daily_dd = ks.get("max_daily_drawdown_pct", 5.0) / 100.0
        max_weekly_dd = ks.get("max_weekly_drawdown_pct", 10.0) / 100.0
        max_monthly_dd = ks.get("max_monthly_drawdown_pct", 15.0) / 100.0

        if portfolio_state.daily_drawdown >= max_daily_dd:
            self._kill_switch = KillSwitchState(
                triggered=True,
                reason=f"Daily drawdown {portfolio_state.daily_drawdown:.2%} >= {max_daily_dd:.2%}",
                triggered_at=datetime.now(timezone.utc),
            )
            return True

        if portfolio_state.weekly_drawdown >= max_weekly_dd:
            self._kill_switch = KillSwitchState(
                triggered=True,
                reason=f"Weekly drawdown {portfolio_state.weekly_drawdown:.2%} >= {max_weekly_dd:.2%}",
                triggered_at=datetime.now(timezone.utc),
            )
            return True

        monthly_dd = getattr(portfolio_state, "monthly_drawdown", 0.0)
        if monthly_dd >= max_monthly_dd:
            self._kill_switch = KillSwitchState(
                triggered=True,
                reason=f"Monthly drawdown {monthly_dd:.2%} >= {max_monthly_dd:.2%}",
                triggered_at=datetime.now(timezone.utc),
            )
            return True

        return False

    async def _execute_kill_switch(self) -> None:
        """Halt all strategies and move to 100% cash."""
        assert self._redis is not None

        emergency_weights = {"funding_arb": 0.0, "grid": 0.0, "dca": 0.0, "cash": 1.0}
        self._current_weights = emergency_weights

        for strategy_id, channel in STRATEGY_CHANNELS.items():
            cmd = AllocationCommand(
                strategy_id=strategy_id,
                allocated_capital=0.0,
                weight=0.0,
                regime="volatile",
                max_drawdown=0.0,
            )
            await self._redis.publish(channel, cmd.model_dump_json())
            log.info("kill_switch_halt_sent", strategy=strategy_id)

        cooldown_min = self._kill_switch_config.get("cooldown_minutes", 60)
        from datetime import timedelta

        self._kill_switch.cooldown_until = datetime.now(timezone.utc) + timedelta(
            minutes=cooldown_min
        )

        # Publish kill switch event
        await self._redis.publish(
            "system:kill_switch",
            json.dumps(
                {
                    "triggered": True,
                    "reason": self._kill_switch.reason,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "cooldown_minutes": cooldown_min,
                }
            ),
        )

        # Cache state
        await self._redis.set(
            "orchestrator:kill_switch",
            json.dumps(
                {
                    "triggered": True,
                    "reason": self._kill_switch.reason,
                    "triggered_at": self._kill_switch.triggered_at.isoformat()
                    if self._kill_switch.triggered_at
                    else None,
                    "cooldown_until": self._kill_switch.cooldown_until.isoformat()
                    if self._kill_switch.cooldown_until
                    else None,
                }
            ),
            ex=7200,
        )

    async def _issue_allocations(
        self, weights: dict[str, float], total_equity: float
    ) -> None:
        """Send capital allocation commands to each strategy via Redis."""
        assert self._redis is not None

        risk_cfg = self._config.get("risk", {})
        max_single_pct = risk_cfg.get("max_single_strategy_pct", 50.0) / 100.0
        min_cash_pct = risk_cfg.get("min_cash_reserve_pct", 5.0) / 100.0

        # Enforce minimum cash reserve
        cash_weight = weights.get("cash", 0.0)
        if cash_weight < min_cash_pct:
            deficit = min_cash_pct - cash_weight
            non_cash = {k: v for k, v in weights.items() if k != "cash" and v > 0}
            total_non_cash = sum(non_cash.values())
            if total_non_cash > 0:
                for k in non_cash:
                    weights[k] -= deficit * (non_cash[k] / total_non_cash)
                weights["cash"] = min_cash_pct

        for strategy_id, channel in STRATEGY_CHANNELS.items():
            weight = weights.get(strategy_id, 0.0)

            # Enforce max single strategy cap
            weight = min(weight, max_single_pct)

            allocated = total_equity * weight
            cmd = AllocationCommand(
                strategy_id=strategy_id,
                allocated_capital=round(allocated, 2),
                weight=round(weight, 4),
                regime=self._current_regime,
                max_drawdown=self._kill_switch_config.get("max_daily_drawdown_pct", 5.0),
            )
            await self._redis.publish(channel, cmd.model_dump_json())
            log.info(
                "allocation_issued",
                strategy=strategy_id,
                weight=weight,
                capital=allocated,
            )

    async def _subscribe_llm_advisory(self) -> None:
        """Listen for LLM advisor weight adjustments on Redis pub/sub."""
        assert self._redis is not None

        llm_cfg = self._config.get("llm_advisor", {})
        if not llm_cfg.get("enabled", True):
            log.info("llm_advisor_disabled")
            return

        channel = llm_cfg.get("redis_channel", "llm:advisory")
        min_confidence = llm_cfg.get("min_confidence", 0.5)
        max_adj = llm_cfg.get("max_adjustment", 0.15)

        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        log.info("llm_advisory_subscribed", channel=channel)

        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                try:
                    advisory = json.loads(message["data"])
                    confidence = advisory.get("confidence", 0.0)
                    if confidence < min_confidence:
                        log.info(
                            "llm_advisory_skipped_low_confidence",
                            confidence=confidence,
                        )
                        continue

                    adjustments = advisory.get("weight_adjustments", {})
                    self._weight_manager.apply_llm_adjustments(
                        adjustments, max_adj, confidence
                    )
                    log.info(
                        "llm_advisory_applied",
                        confidence=confidence,
                        adjustments=adjustments,
                    )
                except (json.JSONDecodeError, KeyError):
                    log.warning("invalid_llm_advisory", data=message.get("data"))
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    async def _cache_orchestrator_state(self) -> None:
        """Cache current orchestrator state in Redis."""
        assert self._redis is not None

        state = {
            "regime": self._current_regime,
            "weights": self._current_weights,
            "total_equity": self._total_equity,
            "kill_switch": {
                "triggered": self._kill_switch.triggered,
                "reason": self._kill_switch.reason,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._redis.set(
            "orchestrator:state", json.dumps(state), ex=600
        )
