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
import os
import re
import time
from abc import ABC, abstractmethod
from typing import Any

import asyncpg
import structlog

from shared.models.order import OrderRequest
from shared.models.strategy import StrategyCommand, StrategyStatus
from shared.redis_client import RedisClient

logger = structlog.get_logger()


class OrderSubmitRateLimitError(Exception):
    """Raised when an order submission exceeds the configured rate limit."""


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

        # Order rate-limit settings (read from config)
        self._max_orders_per_second: int = int(config.get("max_orders_per_second", 2))
        self._max_orders_per_minute: int = int(config.get("max_orders_per_minute", 30))
        # Sliding-window timestamps of past submitted orders
        self._order_timestamps: list[float] = []

        # Redis client — connected lazily in run()
        self._redis = RedisClient()

        # DB pool — connected lazily in run()
        self._db_pool: asyncpg.Pool | None = None

        # Composable controllers (Hummingbot V2 style)
        self._controllers: dict[str, Any] = {}

        self._log = logger.bind(strategy_id=strategy_id)

    @property
    def _db_strategy_id(self) -> str:
        """Normalise strategy_id for DB: strip numeric suffix, replace hyphens."""
        return re.sub(r"-\d+$", "", self.strategy_id).replace("-", "_")

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

        # Initialise DB pool
        db_host = os.environ.get("DB_HOST", "postgres")
        db_port = os.environ.get("DB_PORT", "5432")
        db_name = os.environ.get("DB_NAME", "cryptoengine")
        db_user = os.environ.get("DB_USER", "cryptoengine")
        db_pass = os.environ.get("DB_PASSWORD", "cryptoengine")
        db_url = os.environ.get(
            "DATABASE_URL",
            f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}",
        )
        try:
            self._db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
            self._log.info("db_pool_created")
        except Exception:
            self._log.exception("db_pool_create_failed")

        command_channel = f"strategy:command:{self.strategy_id}"

        pubsub = self._redis.client.pubsub()
        await pubsub.subscribe(command_channel)
        self._log.info("strategy_loop_started", channel=command_channel)

        try:
            while True:
                # --- drain pending commands ---
                try:
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
                except Exception:
                    # pubsub may have broken — attempt reconnect & resubscribe
                    self._log.warning("pubsub_read_error — attempting reconnect")
                    try:
                        await pubsub.close()
                    except Exception:
                        pass
                    await self._reconnect_and_sync()
                    pubsub = self._redis.client.pubsub()
                    await pubsub.subscribe(command_channel)
                    self._log.info("pubsub_resubscribed", channel=command_channel)

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
            if self._db_pool:
                await self._db_pool.close()

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

    async def _check_order_rate_limit(self) -> None:
        """Enforce sliding-window rate limits before accepting an order.

        Raises :class:`OrderSubmitRateLimitError` if either the per-second or
        per-minute limit would be exceeded.  On success, records the current
        timestamp so future calls can account for this order.
        """
        now = time.monotonic()

        # Drop timestamps older than 1 minute
        cutoff_1min = now - 60.0
        self._order_timestamps = [ts for ts in self._order_timestamps if ts >= cutoff_1min]

        # Per-minute check
        if len(self._order_timestamps) >= self._max_orders_per_minute:
            self._log.warning(
                "order_rate_limit_per_minute",
                count=len(self._order_timestamps),
                limit=self._max_orders_per_minute,
            )
            raise OrderSubmitRateLimitError(
                f"Per-minute order limit reached "
                f"({len(self._order_timestamps)}/{self._max_orders_per_minute})"
            )

        # Per-second check
        cutoff_1sec = now - 1.0
        recent = sum(1 for ts in self._order_timestamps if ts >= cutoff_1sec)
        if recent >= self._max_orders_per_second:
            self._log.warning(
                "order_rate_limit_per_second",
                recent=recent,
                limit=self._max_orders_per_second,
            )
            raise OrderSubmitRateLimitError(
                f"Per-second order limit reached ({recent}/{self._max_orders_per_second})"
            )

        # Record this submission attempt
        self._order_timestamps.append(now)

    async def submit_order(self, order: OrderRequest) -> None:
        """Publish an order request to the execution service via Redis.

        Raises :class:`OrderSubmitRateLimitError` if the configured rate limits
        (``max_orders_per_second`` / ``max_orders_per_minute``) are exceeded.
        """
        await self._check_order_rate_limit()
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

    async def _reconnect_and_sync(self) -> None:
        """Reconnect to Redis and re-apply the last known orchestrator command.

        Called automatically when a pubsub read error is detected in the main
        loop.  After reconnection it reads the last command snapshot from
        ``strategy:command_last:{strategy_id}`` (if the orchestrator publishes
        one) and re-handles it so the strategy does not drift from its intended
        state.
        """
        self._log.info("redis_reconnect_sync_start")
        try:
            await self._redis.ensure_connected()
        except Exception:
            self._log.exception("redis_reconnect_failed")
            return

        self._log.info("redis_reconnect_success")

        # Re-apply the last command snapshot published by the orchestrator
        snapshot_key = f"strategy:command_last:{self.strategy_id}"
        try:
            raw = await self._redis.get(snapshot_key)
            if raw:
                cmd = StrategyCommand.model_validate_json(raw)
                self._log.info("state_sync_from_snapshot", action=cmd.action)
                await self._handle_command(cmd)
            else:
                self._log.info("no_command_snapshot_found", key=snapshot_key)
        except Exception:
            self._log.exception("state_sync_error")

    async def _publish_status(self) -> None:
        """Publish heartbeat status to Redis and upsert to strategy_states DB."""
        try:
            status = await self.get_status()
            await self._redis.set(
                f"strategy:status:{self.strategy_id}",
                status.model_dump_json(),
                ttl=90,  # 90s TTL — 워치독(60s 주기)이 두 사이클 놓쳐도 감지 가능
            )
        except Exception:
            self._log.exception("status_publish_error")
            return

        if self._db_pool:
            try:
                async with self._db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO strategy_states
                            (strategy_id, is_running, allocated_capital, current_pnl, position_count, updated_at)
                        VALUES ($1, $2, $3, $4, $5, NOW())
                        ON CONFLICT (strategy_id) DO UPDATE SET
                            is_running = EXCLUDED.is_running,
                            allocated_capital = EXCLUDED.allocated_capital,
                            current_pnl = EXCLUDED.current_pnl,
                            position_count = EXCLUDED.position_count,
                            updated_at = NOW()
                        """,
                        self._db_strategy_id,
                        self.is_running,
                        self.allocated_capital,
                        self.current_pnl,
                        self.position_count,
                    )
            except Exception:
                self._log.exception("strategy_state_db_write_error")
