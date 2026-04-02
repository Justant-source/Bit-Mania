"""Abstract base strategy with composable controller pattern (Hummingbot V2 style).

Every concrete strategy inherits from :class:`BaseStrategy` and implements the
abstract lifecycle hooks.  The base class owns:

* The main ``run()`` loop — subscribes to Redis commands and calls ``tick()``
  on a configurable interval.
* Order submission via Redis ``order:request`` pub/sub channel.
* Command handling (start / stop / pause / resume / reconfigure).
* Capital rebalancing on orchestrator request.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import structlog

from shared.models.order import OrderRequest
from shared.models.strategy import StrategyCommand, StrategyStatus
from shared.redis_client import RedisClient

logger = structlog.get_logger()


class BaseStrategy(ABC):
    """Composable strategy base.

    Sub-strategies (controllers) can be attached at runtime to extend
    behaviour without subclassing — mirroring the Hummingbot V2 controller
    architecture.
    """

    # ── construction ────────────────────────────────────────────────────

    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        self.strategy_id = strategy_id
        self.config = config

        # Runtime state
        self.is_running: bool = False
        self.is_paused: bool = False
        self.allocated_capital: float = 0.0
        self.current_pnl: float = 0.0
        self.position_count: int = 0
        self.max_drawdown: float = config.get("max_drawdown", 0.05)

        # Tick interval in seconds
        self.tick_interval: float = config.get("tick_interval", 5.0)

        # Redis client — connected lazily in run()
        self._redis = RedisClient()

        # Composable controllers (Hummingbot V2 style)
        self._controllers: dict[str, Any] = {}

        self._log = logger.bind(strategy_id=strategy_id)

    # ── composable controllers ──────────────────────────────────────────

    def register_controller(self, name: str, controller: Any) -> None:
        """Attach a composable controller (risk, signal, executor, etc.)."""
        self._controllers[name] = controller
        self._log.info("controller_registered", controller=name)

    def get_controller(self, name: str) -> Any | None:
        return self._controllers.get(name)

    # ── main loop ───────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main event loop: subscribe to Redis commands and tick."""
        await self._redis.connect()
        command_channel = f"strategy:command:{self.strategy_id}"

        pubsub = self._redis.client.pubsub()
        await pubsub.subscribe(command_channel)
        self._log.info("strategy_loop_started", channel=command_channel)

        try:
            while True:
                # --- drain pending commands ---
                while True:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=0.01
                    )
                    if msg is None:
                        break
                    if msg["type"] == "message":
                        try:
                            cmd = StrategyCommand.model_validate_json(msg["data"])
                            await self._handle_command(cmd)
                        except Exception:
                            self._log.exception("command_parse_error", raw=msg["data"])

                # --- tick ---
                if self.is_running and not self.is_paused:
                    try:
                        await self.tick()
                    except Exception:
                        self._log.exception("tick_error")

                # --- heartbeat ---
                await self._publish_status()

                await asyncio.sleep(self.tick_interval)
        except asyncio.CancelledError:
            self._log.info("strategy_loop_cancelled")
        finally:
            await pubsub.unsubscribe(command_channel)
            await pubsub.close()
            await self._redis.disconnect()

    # ── abstract hooks ──────────────────────────────────────────────────

    @abstractmethod
    async def tick(self) -> None:
        """Called every tick interval while the strategy is running."""
        ...

    @abstractmethod
    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialise strategy resources when started by orchestrator."""
        ...

    @abstractmethod
    async def on_stop(self, reason: str) -> None:
        """Tear down positions / cancel orders on stop."""
        ...

    @abstractmethod
    async def get_status(self) -> StrategyStatus:
        """Return current status snapshot."""
        ...

    @abstractmethod
    async def _rebalance(self, new_capital: float) -> None:
        """Adjust positions when allocated capital changes."""
        ...

    # ── order submission ────────────────────────────────────────────────

    async def submit_order(self, order: OrderRequest) -> None:
        """Publish an order request to the execution service via Redis."""
        await self._redis.publish("order:request", order.model_dump_json())
        self._log.info(
            "order_submitted",
            request_id=order.request_id,
            symbol=order.symbol,
            side=order.side,
            qty=order.quantity,
            price=order.price,
        )

    # ── command handling ────────────────────────────────────────────────

    async def _handle_command(self, command: StrategyCommand) -> None:
        """Dispatch orchestrator commands."""
        self._log.info("command_received", action=command.action)

        match command.action:
            case "start":
                capital = command.allocated_capital or 0.0
                self.allocated_capital = capital
                if command.max_drawdown is not None:
                    self.max_drawdown = command.max_drawdown
                await self.on_start(capital, command.params)
                self.is_running = True
                self.is_paused = False

            case "stop":
                self.is_running = False
                self.is_paused = False
                reason = command.params.get("reason", "orchestrator_stop")
                await self.on_stop(reason)

            case "pause":
                self.is_paused = True

            case "resume":
                self.is_paused = False

            case "reconfigure":
                if command.allocated_capital is not None:
                    old = self.allocated_capital
                    self.allocated_capital = command.allocated_capital
                    await self._rebalance(command.allocated_capital)
                    self._log.info(
                        "capital_rebalanced", old=old, new=command.allocated_capital
                    )
                if command.max_drawdown is not None:
                    self.max_drawdown = command.max_drawdown
                self.config.update(command.params)

            case _:
                self._log.warning("unknown_command", action=command.action)

    # ── internal helpers ────────────────────────────────────────────────

    async def _publish_status(self) -> None:
        """Publish heartbeat status to Redis."""
        try:
            status = await self.get_status()
            await self._redis.set(
                f"strategy:status:{self.strategy_id}",
                status.model_dump_json(),
                ttl=30,  # 30s TTL — acts as a liveness probe
            )
        except Exception:
            self._log.exception("status_publish_error")
