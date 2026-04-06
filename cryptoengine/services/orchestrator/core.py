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
import os
from datetime import datetime, timezone
from typing import Any, Literal

import redis.asyncio as aioredis
import structlog
from pydantic import BaseModel, Field

from services.orchestrator.dissimilarity_index import DissimilarityIndex
from services.orchestrator.portfolio_monitor import PortfolioMonitor
from services.orchestrator.regime_ml_model import RegimeMLModel
from services.orchestrator.weight_manager import WeightManager
from shared.kill_switch import KillLevel, KillSwitch
from shared.models.strategy import StrategyCommand
from shared.log_events import *

log = structlog.get_logger(__name__)

StrategyName = Literal["funding_arb", "dca", "cash"]
RegimeType = Literal["trending_up", "trending_down", "ranging", "volatile", "uncertain"]

STRATEGY_CHANNELS: dict[str, str] = {
    "funding_arb": "strategy:command:funding-arb-01",
    "dca": "strategy:command:adaptive-dca-01",
}



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

        ks_cfg = self._kill_switch_config
        cooldown_min = ks_cfg.get("cooldown_minutes", 60)
        self._kill_switch = KillSwitch(
            daily_limit=-(ks_cfg.get("max_daily_drawdown_pct", 5.0) / 100.0),
            weekly_limit=-(ks_cfg.get("max_weekly_drawdown_pct", 10.0) / 100.0),
            monthly_limit=-(ks_cfg.get("max_monthly_drawdown_pct", 15.0) / 100.0),
            cooldown_hours=cooldown_min / 60.0,
            on_trigger=self._on_kill_switch_trigger,
        )
        self._current_regime: RegimeType = "ranging"
        self._current_weights: dict[str, float] = {}
        self._total_equity: float = 0.0

        # 가중치 전환 추적
        self._previous_weights: dict[str, float] = {}
        self._target_weights: dict[str, float] = {}
        self._transition_cycle_count: int = 0
        self._transition_started_at: str | None = None
        self._in_transition: bool = False

        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._llm_advisory_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._heartbeat_timeout_seconds: float = 300.0  # 5분
        self._monitored_services: list[str] = [
            "execution-engine",
            "market-data",
            "funding-arb",
            "adaptive-dca",
        ]

        self._config_path: str = os.environ.get("CONFIG_PATH", "/app/config/orchestrator.yaml")
        self._config_mtime: float = 0.0
        self._config_reload_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize connections and begin the orchestration loop."""
        redis_url = self._config.get("redis", {}).get("url", "redis://localhost:6379")
        self._redis = aioredis.from_url(redis_url, decode_responses=True)
        await self._redis.ping()
        log.info(REDIS_CONNECTED, message="redis connected", url=redis_url)

        pg_dsn = self._config.get("postgres", {}).get("dsn")
        snapshot_interval = self._config.get("portfolio_snapshot_interval_seconds", 900)
        self._portfolio_monitor = PortfolioMonitor(self._redis, pg_dsn, snapshot_interval=snapshot_interval)
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
        self._config_mtime = self._get_config_mtime()
        self._config_reload_task = asyncio.create_task(
            self._config_reload_loop(), name="config-reload-watcher"
        )
        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(), name="service-watchdog"
        )
        log.info(SERVICE_STARTED, message="orchestrator started", interval=self._loop_interval)

    async def stop(self) -> None:
        """Gracefully shut down the orchestrator."""
        self._running = False
        if self._config_reload_task and not self._config_reload_task.done():
            self._config_reload_task.cancel()
            try:
                await self._config_reload_task
            except asyncio.CancelledError:
                pass
        for task in (self._task, self._llm_advisory_task, self._watchdog_task):
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
        log.info(SERVICE_STOPPED, message="orchestrator stopped")

    async def _run_loop(self) -> None:
        """Main orchestration loop executing every 5 minutes."""
        while self._running:
            try:
                await self._orchestration_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(ORCH_CYCLE_START, message="orchestration cycle error")
            await asyncio.sleep(self._loop_interval)

    async def _orchestration_cycle(self) -> None:
        """Single orchestration cycle."""
        log.info(ORCH_CYCLE_START, message="orchestration cycle start")

        # 1. Receive market regime
        regime = await self._get_market_regime()
        self._current_regime = regime
        log.info(MARKET_REGIME_CHANGED, message="regime detected", regime=regime)

        # 2. Adjust strategy weights based on regime
        target_weights = self._weight_manager.get_target_weights(regime)

        # 전환 추적: 목표 가중치가 크게 바뀐 경우 새 전환 시작
        if self._is_new_transition(target_weights):
            self._previous_weights = dict(self._current_weights)
            self._target_weights = dict(target_weights)
            self._transition_cycle_count = 0
            self._transition_started_at = datetime.now(timezone.utc).isoformat()
            self._in_transition = True

        smoothed_weights = self._weight_manager.smooth_transition(
            self._current_weights, target_weights
        )
        self._current_weights = smoothed_weights

        # 전환 진행 카운터 업데이트
        if self._in_transition:
            self._transition_cycle_count += 1
            # 5사이클(25분) 후 또는 목표와 충분히 가까워지면 전환 완료
            if self._transition_cycle_count >= 5 or self._is_near_target(smoothed_weights, target_weights):
                self._in_transition = False

        log.info(ORCH_WEIGHT_CHANGED, message="weights updated", weights=smoothed_weights, regime=regime)

        # 3. Evaluate portfolio risk
        assert self._portfolio_monitor is not None
        portfolio_state = await self._portfolio_monitor.evaluate()
        self._total_equity = portfolio_state.total_equity
        log.info(
            ORCH_CAPITAL_ALLOCATED,
            message="portfolio evaluated",
            equity=portfolio_state.total_equity,
            daily_dd=portfolio_state.daily_drawdown,
            weekly_dd=portfolio_state.weekly_drawdown,
        )

        # 4. Check kill-switch conditions (delegates to shared KillSwitch)
        monthly_dd = getattr(portfolio_state, "monthly_drawdown", 0.0)
        active_level = await self._kill_switch.check(
            portfolio_state,
            monthly_drawdown=monthly_dd,
            system_healthy=True,
        )
        if active_level > KillLevel.NONE:
            log.critical(
                KILL_SWITCH_TRIGGERED,
                message="kill switch triggered",
                level=active_level.name,
                reason=self._kill_switch.reason,
                equity=portfolio_state.total_equity,
            )
            # _on_kill_switch_trigger callback already fired inside KillSwitch.check()
            return

        # If kill switch is still cooling down, skip allocations
        if self._kill_switch.is_triggered:
            log.warning(KILL_SWITCH_COOLDOWN, message="kill switch cooldown", triggered_at=self._kill_switch.triggered_at)
            return

        # 5. Issue capital allocation commands
        await self._issue_allocations(smoothed_weights, portfolio_state.total_equity)

        # Cache current state in Redis
        await self._cache_orchestrator_state()
        log.info(ORCH_CYCLE_START, message="orchestration cycle complete")

    async def _get_market_regime(self) -> RegimeType:
        """Retrieve current market regime from Redis or ML model."""
        assert self._redis is not None

        # Try ML model first
        if self._regime_model:
            try:
                ml_regime, confidence = await self._regime_model.predict()

                # Check dissimilarity index
                if self._di_index and self._di_index.is_uncertain():
                    log.warning(MARKET_REGIME_CHANGED, message="di threshold exceeded", regime="uncertain")
                    return "uncertain"

                if confidence > 0.3:
                    return ml_regime
                log.warning(MARKET_REGIME_CHANGED, message="ml regime low confidence", confidence=confidence)
            except Exception:
                log.exception(MARKET_REGIME_CHANGED, message="ml regime prediction failed")

        # Fallback: read from Redis (set by market-data service)
        raw = await self._redis.get("market:regime:current")
        if raw:
            try:
                data = json.loads(raw)
                return data.get("regime", "ranging")
            except (json.JSONDecodeError, KeyError):
                log.warning(MARKET_REGIME_CHANGED, message="invalid regime data", raw=raw)

        return "ranging"

    def _get_drawdown_size_multiplier(self, drawdown_pct: float) -> float:
        """Return position size multiplier based on current drawdown level.

        0-20%  drawdown → 1.0x (normal)
        20-30% drawdown → 0.5x (reduced)
        30-50% drawdown → 0.1x (minimal — only highest conviction)
        50%+   drawdown → 0.0x (halt all trading)
        """
        if drawdown_pct < 0.20:
            return 1.0
        elif drawdown_pct < 0.30:
            return 0.5
        elif drawdown_pct < 0.50:
            return 0.1
        else:
            return 0.0

    async def _on_kill_switch_trigger(self, level: KillLevel, reason: str) -> None:
        """Callback fired by shared KillSwitch on every trigger.

        Halts all strategies, sets emergency weights, and publishes
        the kill-switch event to Redis so downstream services react.
        """
        if self._redis is None:
            return

        emergency_weights = {"funding_arb": 0.0, "dca": 0.0, "cash": 1.0}
        self._current_weights = emergency_weights

        for strategy_id, channel in STRATEGY_CHANNELS.items():
            cmd = StrategyCommand(
                strategy_id=strategy_id,
                action="stop",
                allocated_capital=0.0,
                params={"reason": "kill_switch"},
            )
            await self._redis.publish(channel, cmd.model_dump_json())
            log.info(KILL_SWITCH_TRIGGERED, message="kill switch halt sent", strategy=strategy_id)

        cooldown_min = self._kill_switch_config.get("cooldown_minutes", 60)

        # Publish kill switch event
        await self._redis.publish(
            "system:kill_switch",
            json.dumps(
                {
                    "triggered": True,
                    "level": level.name,
                    "reason": reason,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "cooldown_minutes": cooldown_min,
                }
            ),
        )

        # Cache state
        triggered_at = self._kill_switch.triggered_at
        await self._redis.set(
            "orchestrator:kill_switch",
            json.dumps(
                {
                    "triggered": True,
                    "level": level.name,
                    "reason": reason,
                    "triggered_at": triggered_at.isoformat() if triggered_at else None,
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

        # Cap deployed capital to max_capital_usd (Phase 4 trial: 1_000 USD)
        max_capital_usd = risk_cfg.get("max_capital_usd")
        if max_capital_usd is not None:
            capped = min(total_equity, float(max_capital_usd))
            if capped < total_equity:
                log.info(
                    ORCH_CAPITAL_ALLOCATED,
                    message="capital capped",
                    actual_equity=total_equity,
                    cap=max_capital_usd,
                    deployed=capped,
                )
            total_equity = capped

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

            # Apply graduated drawdown multiplier
            if self._portfolio_monitor:
                portfolio_state = await self._portfolio_monitor.evaluate()
                max_dd = max(
                    getattr(portfolio_state, "daily_drawdown", 0.0),
                    getattr(portfolio_state, "weekly_drawdown", 0.0),
                )
                multiplier = self._get_drawdown_size_multiplier(max_dd)
                weight = weight * multiplier
                if multiplier < 1.0:
                    log.warning(
                        ORCH_DRAWDOWN_WARNING,
                        message="drawdown size reduction",
                        strategy=strategy_id,
                        drawdown_pct=round(max_dd * 100, 2),
                        multiplier=multiplier,
                        original_weight=weights.get(strategy_id, 0.0),
                        adjusted_weight=round(weight, 4),
                    )

            allocated = total_equity * weight
            cmd = StrategyCommand(
                strategy_id=strategy_id,
                action="start",
                allocated_capital=round(allocated, 2),
                max_drawdown=self._kill_switch_config.get("max_daily_drawdown_pct", 5.0),
                params={"weight": round(weight, 4), "regime": self._current_regime},
            )
            await self._redis.publish(channel, cmd.model_dump_json())
            log.info(
                ORCH_CAPITAL_ALLOCATED,
                message="allocation issued",
                strategy=strategy_id,
                weight=weight,
                capital=allocated,
            )

    async def _subscribe_llm_advisory(self) -> None:
        """Listen for LLM advisor weight adjustments on Redis pub/sub."""
        assert self._redis is not None

        llm_cfg = self._config.get("llm_advisor", {})
        if not llm_cfg.get("enabled", True):
            log.info(LLM_ANALYSIS_START, message="llm advisor disabled")
            return

        channel = llm_cfg.get("redis_channel", "llm:advisory")
        min_confidence = llm_cfg.get("min_confidence", 0.5)
        max_adj = llm_cfg.get("max_adjustment", 0.15)

        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        log.info(LLM_ANALYSIS_START, message="llm advisory subscribed", channel=channel)

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
                            LLM_ANALYSIS_COMPLETE,
                            message="llm advisory skipped low confidence",
                            confidence=confidence,
                        )
                        continue

                    adjustments = advisory.get("weight_adjustments", {})
                    self._weight_manager.apply_llm_adjustments(
                        adjustments, max_adj, confidence
                    )
                    log.info(
                        LLM_WEIGHT_SUGGESTION,
                        message="llm advisory applied",
                        confidence=confidence,
                        adjustments=adjustments,
                    )
                except (json.JSONDecodeError, KeyError):
                    log.warning(LLM_API_ERROR, message="invalid llm advisory", data=message.get("data"))
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
                "triggered": self._kill_switch.is_triggered,
                "level": self._kill_switch.level.name,
                "reason": self._kill_switch.reason,
            },
            "weight_transition": {
                "in_progress": self._in_transition,
                "current_step": min(self._transition_cycle_count, 5),
                "total_steps": 5,
                "previous_weights": self._previous_weights,
                "target_weights": self._target_weights,
                "current_weights": self._current_weights,
                "started_at": self._transition_started_at,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._redis.set(
            "orchestrator:state", json.dumps(state), ex=600
        )

        # orchestrator:weight_transition 별도 키에도 발행 (대시보드용)
        transition_data = state["weight_transition"]
        await self._redis.set(
            "orchestrator:weight_transition",
            json.dumps(transition_data),
            ex=600,
        )

    def _is_new_transition(self, new_target: dict[str, float]) -> bool:
        """목표 가중치가 현재 목표와 유의미하게 다른지 확인."""
        if not self._target_weights:
            return bool(self._current_weights)
        for key in new_target:
            if abs(new_target.get(key, 0.0) - self._target_weights.get(key, 0.0)) > 0.05:
                return True
        return False

    def _is_near_target(self, current: dict[str, float], target: dict[str, float]) -> bool:
        """현재 가중치가 목표에 충분히 가까운지 확인 (모든 전략 1% 이내)."""
        for key in target:
            if abs(current.get(key, 0.0) - target.get(key, 0.0)) > 0.01:
                return False
        return True

    async def _watchdog_loop(self) -> None:
        """모니터링 서비스들의 하트비트를 60초마다 체크.

        5분(300초) 이상 하트비트 없는 서비스 감지 시 kill switch 발동.
        전략 서비스는 strategy:status:{id} 키 (TTL=90s) 존재 여부로 확인.
        """
        check_interval = 60.0  # 1분마다 체크

        # 전략 서비스 ID → Redis 키 매핑
        strategy_key_map: dict[str, str] = {
            "funding-arb": "strategy:status:funding-arb-01",
            "adaptive-dca": "strategy:status:adaptive-dca-01",
        }
        # 인프라 서비스: heartbeat:{service} 키 사용
        infra_services = ["execution-engine", "market-data"]

        while self._running:
            try:
                await asyncio.sleep(check_interval)
                if not self._running:
                    break

                assert self._redis is not None
                dead_services: list[str] = []

                # 인프라 서비스 하트비트 확인
                for service in infra_services:
                    key = f"heartbeat:{service}"
                    try:
                        raw = await self._redis.get(key)
                        if raw is None:
                            dead_services.append(service)
                            log.warning(
                                SERVICE_HEALTH_FAIL,
                                message="heartbeat missing",
                                service=service,
                                timeout_seconds=self._heartbeat_timeout_seconds,
                            )
                        else:
                            log.debug(SERVICE_HEALTH_OK, message="heartbeat ok", service=service)
                    except Exception:
                        log.warning(SERVICE_HEALTH_FAIL, message="heartbeat check failed", service=service)

                # 전략 서비스 상태 키 확인
                for service, key in strategy_key_map.items():
                    try:
                        raw = await self._redis.get(key)
                        if raw is None:
                            dead_services.append(service)
                            log.warning(
                                SERVICE_HEALTH_FAIL,
                                message="heartbeat missing",
                                service=service,
                                timeout_seconds=90,
                            )
                        else:
                            log.debug(SERVICE_HEALTH_OK, message="heartbeat ok", service=service)
                    except Exception:
                        log.warning(SERVICE_HEALTH_FAIL, message="heartbeat check failed", service=service)

                # 핵심 서비스(execution-engine)가 죽은 경우 kill switch 발동
                critical_dead = [s for s in dead_services if s == "execution-engine"]
                if critical_dead:
                    log.critical(
                        ORCH_DEAD_MAN_SWITCH,
                        message="dead man's switch triggered",
                        dead_services=critical_dead,
                        action="triggering_kill_switch",
                    )
                    await self._kill_switch.trigger(
                        KillLevel.SYSTEM,
                        f"Dead man's switch: services not responding: {critical_dead}",
                    )

                    # Telegram 알림 발행
                    await self._redis.publish(
                        "telegram:notification",
                        json.dumps({
                            "level": "critical",
                            "message": f"Dead Man's Switch triggered! Services down: {critical_dead}. All positions closed.",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        })
                    )

                elif dead_services:
                    # 비핵심 서비스 경고만
                    log.warning(
                        SERVICE_HEALTH_FAIL,
                        message="non-critical services missing heartbeat",
                        dead_services=dead_services,
                    )
                    await self._redis.set(
                        "system:service_health",
                        json.dumps({
                            "status": "degraded",
                            "dead_services": dead_services,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }),
                        ex=120,
                    )
                else:
                    # 모든 서비스 정상
                    await self._redis.set(
                        "system:service_health",
                        json.dumps({
                            "status": "healthy",
                            "services": self._monitored_services,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }),
                        ex=120,
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(SERVICE_HEALTH_FAIL, message="watchdog loop error")

    # ------------------------------------------------------------------
    # Config hot-reload (7.5 개선사항)
    # ------------------------------------------------------------------

    def _get_config_mtime(self) -> float:
        """config 파일의 최종 수정 시각 반환. 파일 없으면 0.0."""
        try:
            return os.path.getmtime(self._config_path)
        except OSError:
            return 0.0

    async def _config_reload_loop(self) -> None:
        """30초마다 config 파일 수정 시각 폴링. 변경 감지 시 kill_switch 설정 핫 리로드."""
        check_interval = 30.0

        while self._running:
            try:
                await asyncio.sleep(check_interval)
                if not self._running:
                    break

                current_mtime = self._get_config_mtime()
                if current_mtime == 0.0 or current_mtime <= self._config_mtime:
                    continue  # 변경 없음

                # 파일 변경 감지!
                log.info(ORCH_CONFIG_RELOADED, message="config change detected", path=self._config_path, mtime=current_mtime)
                await self._reload_kill_switch_config()
                self._config_mtime = current_mtime

            except asyncio.CancelledError:
                break
            except Exception:
                log.exception(ORCH_CONFIG_RELOADED, message="config reload loop error")

    async def _reload_kill_switch_config(self) -> None:
        """orchestrator.yaml에서 kill_switch 섹션만 리로드.

        재시작 없이 임계값 변경 가능. 변경 사항은 즉시 다음 _orchestration_cycle에 반영됨.
        """
        try:
            import yaml

            with open(self._config_path) as f:
                new_config = yaml.safe_load(f)

            new_ks = new_config.get("kill_switch", {})
            old_ks = self._kill_switch_config.copy()

            if new_ks == old_ks:
                log.debug(ORCH_CONFIG_RELOADED, message="config reload no changes in kill switch")
                return

            # 변경된 키 목록 로깅
            changed_keys = [
                k
                for k in set(list(new_ks.keys()) + list(old_ks.keys()))
                if new_ks.get(k) != old_ks.get(k)
            ]

            self._kill_switch_config = new_ks
            log.info(
                ORCH_CONFIG_RELOADED,
                message="kill switch config reloaded",
                changed_keys=changed_keys,
                new_values={k: new_ks.get(k) for k in changed_keys},
                old_values={k: old_ks.get(k) for k in changed_keys},
            )

            # Redis에 감사 로그 발행
            if self._redis:
                await self._redis.publish(
                    "system:config_reload",
                    json.dumps(
                        {
                            "section": "kill_switch",
                            "changed_keys": changed_keys,
                            "new_values": {k: new_ks.get(k) for k in changed_keys},
                            "old_values": {k: old_ks.get(k) for k in changed_keys},
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ),
                )
                log.info(ORCH_CONFIG_RELOADED, message="config reload audit published")

            # orchestrator 설정도 동시에 리로드 (loop_interval 등)
            new_orch = new_config.get("orchestrator", {})
            if new_orch.get("loop_interval_seconds") != self._orch_config.get(
                "loop_interval_seconds"
            ):
                self._loop_interval = new_orch.get("loop_interval_seconds", self._loop_interval)
                self._orch_config = new_orch
                log.info(ORCH_CONFIG_RELOADED, message="orchestrator loop interval reloaded", new_interval=self._loop_interval)

        except FileNotFoundError:
            log.warning(ORCH_CONFIG_RELOADED, message="config reload file not found", path=self._config_path)
        except Exception:
            log.exception(ORCH_CONFIG_RELOADED, message="config reload failed", path=self._config_path)
