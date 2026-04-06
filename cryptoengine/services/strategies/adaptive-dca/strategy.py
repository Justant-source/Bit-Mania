"""Adaptive Dollar-Cost Averaging Strategy.

Executes weekly BTC purchases whose size is modulated by the Fear & Greed
Index.  Buys aggressively during extreme fear and sells a fraction during
extreme greed.

Weekly schedule: Monday UTC 00:00

F&G multiplier table:
    0-10   (Extreme Fear)  → 3.0x base amount
    11-25  (Fear)           → 2.0x base amount
    26-50  (Neutral-ish)    → 1.0x base amount
    51-75  (Greed)          → skip (no buy)
    76-100 (Extreme Greed)  → sell 20% of holdings

Base amount: 2% of allocated capital.
Buy order: spot BTC limit at current_price - 0.1%.
"""

from __future__ import annotations

import os
import sys
from typing import Any

import structlog

from shared.log_events import *

# Allow import of base_strategy from parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from base_strategy import BaseStrategy  # noqa: E402

from shared.exchange import ExchangeConnector, exchange_factory
from shared.models.order import OrderRequest
from shared.models.strategy import StrategyStatus

from strategy.fear_greed import FearGreedCollector
from strategy.scheduler import DCAScheduler

logger = structlog.get_logger()

# ── F&G multiplier thresholds ──────────────────────────────────────────
FNG_EXTREME_FEAR_MAX = 10
FNG_FEAR_MAX = 25
FNG_NEUTRAL_MAX = 50
FNG_GREED_MAX = 75

FNG_MULTIPLIERS: list[tuple[int, int, float | str]] = [
    (0, FNG_EXTREME_FEAR_MAX, 3.0),
    (FNG_EXTREME_FEAR_MAX + 1, FNG_FEAR_MAX, 2.0),
    (FNG_FEAR_MAX + 1, FNG_NEUTRAL_MAX, 1.0),
    (FNG_NEUTRAL_MAX + 1, FNG_GREED_MAX, "skip"),
    (FNG_GREED_MAX + 1, 100, "sell"),
]

BASE_AMOUNT_PCT = 0.02   # 2% of allocated capital
BUY_DISCOUNT_PCT = 0.001  # limit order at price - 0.1%
SELL_FRACTION = 0.20       # sell 20% of holdings during extreme greed


class AdaptiveDCAStrategy(BaseStrategy):
    """Fear & Greed modulated weekly DCA strategy."""

    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        super().__init__(strategy_id, config)

        # Config
        self.symbol: str = config.get("symbol", "BTC/USDT")
        self.exchange_id: str = config.get("exchange", "binance")
        self.base_amount_pct: float = config.get("base_amount_pct", BASE_AMOUNT_PCT)
        self.buy_discount_pct: float = config.get("buy_discount_pct", BUY_DISCOUNT_PCT)
        self.sell_fraction: float = config.get("sell_fraction", SELL_FRACTION)

        # Components (initialised on start)
        self._exchange: ExchangeConnector | None = None
        self._fng_collector: FearGreedCollector | None = None
        self._scheduler: DCAScheduler | None = None

        # Position tracking
        self._btc_held: float = 0.0

        self._log = logger.bind(strategy_id=strategy_id, strategy="adaptive_dca")

    # ── lifecycle ──────────────────────────────────────────────────────

    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialise exchange, F&G collector, and scheduler."""
        self._exchange = exchange_factory(
            "bybit",
            api_key=os.environ.get("BYBIT_API_KEY", ""),
            api_secret=os.environ.get("BYBIT_API_SECRET", ""),
            testnet=os.environ.get("BYBIT_TESTNET", "true").lower() == "true",
        )
        await self._exchange.connect()

        self._fng_collector = FearGreedCollector(redis=self._redis)
        self.register_controller("fear_greed", self._fng_collector)

        self._scheduler = DCAScheduler(
            redis=self._redis,
            strategy_id=self.strategy_id,
        )
        await self._scheduler.load_from_redis()
        self._btc_held = self._scheduler.total_btc_held
        self.register_controller("scheduler", self._scheduler)

        self._log.info(
            STRATEGY_STARTED,
            message="적응형 DCA 전략 시작",
            capital=capital,
            symbol=self.symbol,
            btc_held=self._btc_held,
            avg_cost_basis=self._scheduler.average_cost_basis,
            purchase_count=self._scheduler.purchase_count,
        )

    async def on_stop(self, reason: str) -> None:
        """Disconnect exchange."""
        self._log.info(SERVICE_STOPPING, message="전략 종료 중", reason=reason)

        if self._exchange:
            await self._exchange.disconnect()

        self._log.info(STRATEGY_STOPPED, message="전략 종료 완료", reason=reason)

    async def get_status(self) -> StrategyStatus:
        return StrategyStatus(
            strategy_id=self.strategy_id,
            is_running=self.is_running,
            allocated_capital=self.allocated_capital,
            current_pnl=self.current_pnl,
            position_count=1 if self._btc_held > 0 else 0,
        )

    async def _rebalance(self, new_capital: float) -> None:
        """Base amount recalculates automatically from allocated_capital."""
        self._log.info(
            STRATEGY_REBALANCE,
            message="자본 업데이트",
            new_capital=new_capital,
            new_base_amount=new_capital * self.base_amount_pct,
        )

    # ── main tick ──────────────────────────────────────────────────────

    async def tick(self) -> None:
        """Check if it is buy time and execute the DCA logic."""
        assert self._exchange is not None
        assert self._fng_collector is not None
        assert self._scheduler is not None

        if not self._scheduler.is_buy_time():
            return

        # Fetch current F&G index
        fng_index = await self._fng_collector.get_current_index()

        # Determine action from F&G multiplier table
        action = self._get_fng_action(fng_index)

        ticker = await self._exchange.get_ticker(self.symbol)
        current_price = float(ticker.get("last", 0))
        if current_price <= 0:
            self._log.warning("invalid_price", price=current_price)
            return

        self._log.info(
            STRATEGY_TICK,
            message="DCA 틱 실행",
            fng_index=fng_index,
            action=str(action),
            current_price=current_price,
            next_buy=self._scheduler.calculate_next_buy_time().isoformat(),
        )

        if action == "skip":
            self._log.info(DCA_MULTIPLIER_CALC, message="공포탐욕 탐욕 구간, DCA 스킵", fng_index=fng_index)
            # Record a "skip" so we don't retry this week
            await self._scheduler.record_purchase(
                price=current_price,
                quantity=0.0,
                fng_index=fng_index,
                multiplier=0.0,
            )
            return

        if action == "sell":
            await self._execute_sell(current_price, fng_index)
            return

        # Buy with multiplier
        multiplier = float(action)
        await self._execute_buy(current_price, fng_index, multiplier)

    # ── F&G action lookup ──────────────────────────────────────────────

    @staticmethod
    def _get_fng_action(fng_index: int) -> float | str:
        """Map F&G index to a multiplier or action string."""
        for low, high, action in FNG_MULTIPLIERS:
            if low <= fng_index <= high:
                return action
        return 1.0  # fallback neutral

    # ── buy execution ──────────────────────────────────────────────────

    async def _execute_buy(
        self, current_price: float, fng_index: int, multiplier: float
    ) -> None:
        """Place a limit buy order with F&G-adjusted size."""
        assert self._exchange is not None
        assert self._scheduler is not None

        base_amount = self.allocated_capital * self.base_amount_pct
        buy_amount = base_amount * multiplier

        # Limit price: current_price - 0.1%
        limit_price = current_price * (1 - self.buy_discount_pct)
        quantity = buy_amount / limit_price

        self._log.info(
            DCA_PURCHASE,
            message="DCA 매수 주문",
            fng_index=fng_index,
            multiplier=multiplier,
            buy_amount_usd=round(buy_amount, 2),
            limit_price=round(limit_price, 2),
            quantity=quantity,
        )

        order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.symbol,
            side="buy",
            order_type="limit",
            quantity=quantity,
            price=limit_price,
            post_only=True,
        )
        await self.submit_order(order)

        # Track purchase
        self._btc_held += quantity
        self.current_pnl = self._calculate_unrealised_pnl(current_price)

        await self._scheduler.record_purchase(
            price=limit_price,
            quantity=quantity,
            fng_index=fng_index,
            multiplier=multiplier,
        )

    # ── sell execution ─────────────────────────────────────────────────

    async def _execute_sell(self, current_price: float, fng_index: int) -> None:
        """Sell a fraction of BTC holdings during extreme greed."""
        assert self._exchange is not None
        assert self._scheduler is not None

        if self._btc_held <= 0:
            self._log.info(DCA_MULTIPLIER_CALC, message="DCA 매도 스킵: 보유 없음")
            await self._scheduler.record_purchase(
                price=current_price,
                quantity=0.0,
                fng_index=fng_index,
                multiplier=0.0,
            )
            return

        sell_qty = self._btc_held * self.sell_fraction

        self._log.info(
            DCA_TAKE_PROFIT,
            message="DCA 익절 매도 (극단 탐욕)",
            fng_index=fng_index,
            sell_qty=sell_qty,
            total_held=self._btc_held,
            sell_fraction=self.sell_fraction,
        )

        order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.symbol,
            side="sell",
            order_type="limit",
            quantity=sell_qty,
            price=current_price,
            post_only=True,
        )
        await self.submit_order(order)

        # Update holdings
        self._btc_held -= sell_qty
        self.current_pnl = self._calculate_unrealised_pnl(current_price)

        # Record as negative quantity purchase
        await self._scheduler.record_purchase(
            price=current_price,
            quantity=-sell_qty,
            fng_index=fng_index,
            multiplier=0.0,
        )

    # ── helpers ────────────────────────────────────────────────────────

    def _calculate_unrealised_pnl(self, current_price: float) -> float:
        """Estimate unrealised PnL based on cost basis vs current price."""
        if self._scheduler is None or self._btc_held <= 0:
            return 0.0

        avg_cost = self._scheduler.average_cost_basis
        if avg_cost <= 0:
            return 0.0

        return self._btc_held * (current_price - avg_cost)
