"""PortfolioMonitor — tracks equity, PnL, drawdown, and Sharpe ratio.

Caches PortfolioState in Redis and snapshots to PostgreSQL periodically.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import redis.asyncio as aioredis
import structlog
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

REDIS_KEY_PORTFOLIO = "cache:portfolio_state"
REDIS_KEY_EQUITY_HISTORY = "history:equity:daily"
SNAPSHOT_INTERVAL = 900  # 15 minutes


class PortfolioState(BaseModel):
    """Aggregate portfolio state used by risk / kill-switch."""

    total_equity: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl_today: float = 0.0
    daily_drawdown: float = 0.0
    weekly_drawdown: float = 0.0
    monthly_drawdown: float = 0.0
    sharpe_ratio_30d: float | None = None
    strategies: list[dict[str, Any]] = Field(default_factory=list)
    kill_switch_triggered: bool = False
    snapshot_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PortfolioMonitor:
    """Monitors portfolio metrics, computes drawdown and Sharpe ratio."""

    def __init__(
        self,
        redis: aioredis.Redis,
        pg_dsn: str | None = None,
        snapshot_interval: int = SNAPSHOT_INTERVAL,
    ) -> None:
        self._redis = redis
        self._pg_dsn = pg_dsn
        self._pg_pool: asyncpg.Pool | None = None
        self._snapshot_interval = snapshot_interval

        self._equity_history: list[tuple[datetime, float]] = []
        self._peak_equity: float = 0.0
        self._daily_peak: float = 0.0
        self._weekly_peak: float = 0.0
        self._monthly_peak: float = 0.0
        self._last_peak_reset: datetime = datetime.now(timezone.utc)

        self._running = False
        self._snapshot_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize database connection and start snapshot loop."""
        if self._pg_dsn:
            try:
                self._pg_pool = await asyncpg.create_pool(
                    self._pg_dsn, min_size=1, max_size=3
                )
                await self._ensure_tables()
                log.info("portfolio_monitor_pg_connected")
            except Exception:
                log.exception("portfolio_monitor_pg_connection_failed")
                self._pg_pool = None

        # Load historical equity from Redis
        await self._load_equity_history()

        self._running = True
        self._snapshot_task = asyncio.create_task(
            self._snapshot_loop(), name="portfolio-snapshot"
        )
        log.info("portfolio_monitor_started")

    async def stop(self) -> None:
        """Shut down the monitor."""
        self._running = False
        if self._snapshot_task and not self._snapshot_task.done():
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
        if self._pg_pool:
            await self._pg_pool.close()
        log.info("portfolio_monitor_stopped")

    async def evaluate(self) -> PortfolioState:
        """Collect strategy states and compute portfolio metrics."""
        strategies = await self._collect_strategy_states()
        unrealized_pnl = sum(s.get("current_pnl", 0.0) for s in strategies)

        # Prefer actual wallet balance published by execution-engine
        total_equity = await self._get_wallet_balance()
        if total_equity == 0.0:
            total_equity = sum(s.get("allocated_capital", 0.0) for s in strategies)
        realized_today = await self._get_realized_pnl_today()

        total_equity = max(total_equity, 0.0)
        now = datetime.now(timezone.utc)

        # Track equity history
        self._equity_history.append((now, total_equity))
        self._trim_equity_history()

        # Update peaks
        if total_equity > self._peak_equity:
            self._peak_equity = total_equity
        self._update_period_peaks(now, total_equity)

        # Calculate drawdowns
        daily_dd = self._calculate_drawdown(total_equity, self._daily_peak)
        weekly_dd = self._calculate_drawdown(total_equity, self._weekly_peak)
        monthly_dd = self._calculate_drawdown(total_equity, self._monthly_peak)

        # Calculate Sharpe ratio
        sharpe = self._calculate_sharpe_30d()

        state = PortfolioState(
            total_equity=total_equity,
            unrealized_pnl=unrealized_pnl,
            realized_pnl_today=realized_today,
            daily_drawdown=daily_dd,
            weekly_drawdown=weekly_dd,
            monthly_drawdown=monthly_dd,
            sharpe_ratio_30d=sharpe,
            strategies=strategies,
            snapshot_at=now,
        )

        # Cache in Redis
        await self._cache_state(state)

        return state

    async def _get_wallet_balance(self) -> float:
        """Read actual wallet balance published by execution-engine."""
        try:
            raw = await self._redis.get("cache:wallet_balance")
            if raw:
                data = json.loads(raw)
                return float(data.get("total", 0.0))
        except Exception:
            pass
        return 0.0

    async def _collect_strategy_states(self) -> list[dict[str, Any]]:
        """Read strategy status from Redis."""
        strategies: list[dict[str, Any]] = []
        strategy_ids = ["funding_arb", "adaptive_dca"]

        for sid in strategy_ids:
            key = f"strategy:{sid}:status"
            raw = await self._redis.get(key)
            if raw:
                try:
                    data = json.loads(raw)
                    strategies.append(data)
                except json.JSONDecodeError:
                    log.warning("invalid_strategy_status", strategy=sid)
            else:
                strategies.append(
                    {
                        "strategy_id": sid,
                        "is_running": False,
                        "allocated_capital": 0.0,
                        "current_pnl": 0.0,
                        "position_count": 0,
                    }
                )

        return strategies

    async def _get_realized_pnl_today(self) -> float:
        """Read today's realized PnL from Redis."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw = await self._redis.get(f"pnl:realized:{today}")
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return 0.0

    def _update_period_peaks(self, now: datetime, equity: float) -> None:
        """Reset period peaks when the period rolls over."""
        # Daily peak reset at UTC midnight
        if now.date() != self._last_peak_reset.date():
            self._daily_peak = equity
            # Weekly reset on Monday
            if now.weekday() == 0:
                self._weekly_peak = equity
            # Monthly reset on 1st
            if now.day == 1:
                self._monthly_peak = equity
            self._last_peak_reset = now

        self._daily_peak = max(self._daily_peak, equity)
        self._weekly_peak = max(self._weekly_peak, equity)
        self._monthly_peak = max(self._monthly_peak, equity)

    @staticmethod
    def _calculate_drawdown(current: float, peak: float) -> float:
        """Calculate drawdown as a fraction (0.0 - 1.0)."""
        if peak <= 0:
            return 0.0
        return max(0.0, (peak - current) / peak)

    def _calculate_sharpe_30d(self) -> float | None:
        """Calculate 30-day rolling Sharpe ratio from equity history."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        recent = [eq for ts, eq in self._equity_history if ts >= cutoff]

        if len(recent) < 10:
            return None

        # Calculate daily returns
        returns = []
        for i in range(1, len(recent)):
            if recent[i - 1] > 0:
                ret = (recent[i] - recent[i - 1]) / recent[i - 1]
                returns.append(ret)

        if len(returns) < 5:
            return None

        arr = np.array(returns)
        mean_ret = float(np.mean(arr))
        std_ret = float(np.std(arr, ddof=1))

        if std_ret == 0:
            return 0.0

        # Annualize (assume ~288 5-min periods per day for crypto)
        # If data is daily, use 365; otherwise scale appropriately
        annualization_factor = np.sqrt(365)
        risk_free_daily = 0.05 / 365  # 5% annual risk-free rate
        sharpe = (mean_ret - risk_free_daily) / std_ret * float(annualization_factor)

        return round(sharpe, 4)

    def _trim_equity_history(self) -> None:
        """Keep only last 30 days of equity history in memory."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=35)
        self._equity_history = [
            (ts, eq) for ts, eq in self._equity_history if ts >= cutoff
        ]

    async def _load_equity_history(self) -> None:
        """Load equity history from Redis on startup."""
        raw = await self._redis.lrange(REDIS_KEY_EQUITY_HISTORY, 0, -1)
        for entry in raw:
            try:
                data = json.loads(entry)
                ts = datetime.fromisoformat(data["timestamp"])
                self._equity_history.append((ts, data["equity"]))
            except (json.JSONDecodeError, KeyError, ValueError):
                continue

        if self._equity_history:
            self._peak_equity = max(eq for _, eq in self._equity_history)
            self._daily_peak = self._peak_equity
            self._weekly_peak = self._peak_equity
            self._monthly_peak = self._peak_equity
            log.info(
                "equity_history_loaded",
                entries=len(self._equity_history),
                peak=self._peak_equity,
            )

    async def _cache_state(self, state: PortfolioState) -> None:
        """Cache portfolio state in Redis."""
        await self._redis.set(
            REDIS_KEY_PORTFOLIO,
            state.model_dump_json(),
            ex=600,
        )

        # Append to equity history list
        entry = json.dumps(
            {
                "timestamp": state.snapshot_at.isoformat(),
                "equity": state.total_equity,
            }
        )
        await self._redis.rpush(REDIS_KEY_EQUITY_HISTORY, entry)
        # Trim to ~30 days of 5-min intervals
        await self._redis.ltrim(REDIS_KEY_EQUITY_HISTORY, -8640, -1)

    async def _snapshot_loop(self) -> None:
        """Periodically snapshot portfolio state to PostgreSQL."""
        while self._running:
            try:
                await asyncio.sleep(self._snapshot_interval)
                if self._pg_pool:
                    await self._snapshot_to_pg()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("portfolio_snapshot_error")

    async def _snapshot_to_pg(self) -> None:
        """Write current portfolio state to PostgreSQL."""
        if not self._pg_pool:
            return

        raw = await self._redis.get(REDIS_KEY_PORTFOLIO)
        if not raw:
            return

        try:
            state = PortfolioState.model_validate_json(raw)
        except Exception:
            log.exception("portfolio_state_parse_error")
            return

        async with self._pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO portfolio_snapshots (
                    total_equity, unrealized_pnl, realized_pnl_today,
                    daily_drawdown, weekly_drawdown, monthly_drawdown,
                    sharpe_ratio_30d, strategies, snapshot_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                state.total_equity,
                state.unrealized_pnl,
                state.realized_pnl_today,
                state.daily_drawdown,
                state.weekly_drawdown,
                state.monthly_drawdown,
                state.sharpe_ratio_30d,
                json.dumps(state.strategies),
                state.snapshot_at,
            )
        log.debug("portfolio_snapshot_saved", equity=state.total_equity)

    async def _ensure_tables(self) -> None:
        """Create portfolio_snapshots table if it does not exist."""
        if not self._pg_pool:
            return

        async with self._pg_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    total_equity DOUBLE PRECISION NOT NULL,
                    unrealized_pnl DOUBLE PRECISION DEFAULT 0,
                    realized_pnl_today DOUBLE PRECISION DEFAULT 0,
                    daily_drawdown DOUBLE PRECISION DEFAULT 0,
                    weekly_drawdown DOUBLE PRECISION DEFAULT 0,
                    monthly_drawdown DOUBLE PRECISION DEFAULT 0,
                    sharpe_ratio_30d DOUBLE PRECISION,
                    strategies JSONB DEFAULT '[]',
                    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_at
                    ON portfolio_snapshots (snapshot_at DESC);
                """
            )
