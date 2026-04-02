"""Grid Trading Strategy.

Places a grid of Post-Only limit buy and sell orders across a defined price
range, profiting from sideways oscillations.  The strategy activates only
when the market is range-bound (low ADX, Bollinger Band squeeze) and
auto-stops when a trend is detected.

Activation conditions:
    * ADX(14) < 20  — no directional trend
    * Bollinger Band squeeze detected (bandwidth below threshold)

Grid mechanics:
    * Range: current_price +/- ATR(14) * 3
    * 20-40 Post-Only limit orders placed across the range
    * On fill: auto-create opposite direction order at next grid level
    * Supports arithmetic and geometric spacing

Auto-stop triggers:
    * Price escapes range by +/-5%
    * ADX > 25 (trend emerging)
    * On stop: cancel all pending orders, close open positions
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import numpy as np
import structlog

# Allow import of base_strategy from parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from base_strategy import BaseStrategy  # noqa: E402

from shared.exchange import ExchangeConnector, exchange_factory
from shared.models.order import OrderRequest
from shared.models.strategy import StrategyStatus

from strategy.grid_calculator import GridCalculator, GridLevel, SpacingMode
from strategy.grid_state import GridStateTracker, LevelStatus

logger = structlog.get_logger()

# ── defaults ──────────────────────────────────────────────────────────────
DEFAULT_NUM_GRIDS = 20
MAX_NUM_GRIDS = 40
ADX_ACTIVATION_THRESHOLD = 20
ADX_STOP_THRESHOLD = 25
ATR_PERIOD = 14
BB_PERIOD = 20
BB_SQUEEZE_THRESHOLD = 0.04  # bandwidth < 4% → squeeze
RANGE_ESCAPE_PCT = 0.05      # price escapes range by 5%


class GridStrategy(BaseStrategy):
    """Range-bound grid trading with dynamic grid placement."""

    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        super().__init__(strategy_id, config)

        # Config
        self.symbol: str = config.get("symbol", "BTC/USDT")
        self.exchange_id: str = config.get("exchange", "binance")
        self.num_grids: int = config.get("num_grids", DEFAULT_NUM_GRIDS)
        self.spacing_mode: str = config.get("spacing_mode", "arithmetic")
        self.adx_activation: float = config.get("adx_activation", ADX_ACTIVATION_THRESHOLD)
        self.adx_stop: float = config.get("adx_stop", ADX_STOP_THRESHOLD)

        # Components (initialised on start)
        self._exchange: ExchangeConnector | None = None
        self._calculator = GridCalculator(
            spacing_mode=SpacingMode(self.spacing_mode),
        )
        self._state = GridStateTracker()

        # Runtime state
        self._grid_active: bool = False
        self._grid_center: float = 0.0
        self._grid_range_width: float = 0.0
        self._ohlcv_cache: list[list[float]] = []

        self._log = logger.bind(strategy_id=strategy_id, strategy="grid_trading")

    # ── lifecycle ──────────────────────────────────────────────────────

    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialise exchange connection."""
        self._exchange = exchange_factory(self.exchange_id)
        await self._exchange.connect()

        self._log.info(
            "strategy_started",
            capital=capital,
            symbol=self.symbol,
            num_grids=self.num_grids,
        )

    async def on_stop(self, reason: str) -> None:
        """Cancel all grid orders and close positions."""
        self._log.info("strategy_stopping", reason=reason)

        if self._grid_active:
            await self._teardown_grid(reason=reason)

        if self._exchange:
            await self._exchange.disconnect()

        self._log.info("strategy_stopped", reason=reason)

    async def get_status(self) -> StrategyStatus:
        return StrategyStatus(
            strategy_id=self.strategy_id,
            is_running=self.is_running,
            allocated_capital=self.allocated_capital,
            current_pnl=self.current_pnl,
            position_count=self._state.filled_count,
        )

    async def _rebalance(self, new_capital: float) -> None:
        """Rebuild grid with updated capital allocation."""
        if not self._grid_active:
            return

        self._log.info("rebalancing_grid", new_capital=new_capital)
        await self._teardown_grid(reason="rebalance")
        # Grid will be rebuilt on next tick if conditions still hold
        self._grid_active = False

    # ── main tick ──────────────────────────────────────────────────────

    async def tick(self) -> None:
        """Main strategy tick.

        1. Fetch OHLCV and indicators.
        2. If grid is active: check fills, auto-stop conditions, recalculate if needed.
        3. If grid is inactive: check activation conditions and place grid.
        """
        assert self._exchange is not None

        # Fetch recent OHLCV for indicator calculation
        ohlcv = await self._exchange.get_ohlcv(
            self.symbol, timeframe="1h", limit=50
        )
        if not ohlcv or len(ohlcv) < BB_PERIOD:
            self._log.debug("insufficient_ohlcv", count=len(ohlcv) if ohlcv else 0)
            return

        self._ohlcv_cache = ohlcv

        ticker = await self._exchange.get_ticker(self.symbol)
        current_price = float(ticker.get("last", 0))
        if current_price <= 0:
            return

        # Calculate indicators
        closes = np.array([c[4] for c in ohlcv], dtype=np.float64)
        highs = np.array([c[2] for c in ohlcv], dtype=np.float64)
        lows = np.array([c[3] for c in ohlcv], dtype=np.float64)

        adx = self._calculate_adx(highs, lows, closes, period=ATR_PERIOD)
        atr = self._calculate_atr(highs, lows, closes, period=ATR_PERIOD)
        bb_squeeze = self._detect_bb_squeeze(closes, period=BB_PERIOD)

        self._log.debug(
            "indicators",
            adx=round(adx, 2),
            atr=round(atr, 2),
            bb_squeeze=bb_squeeze,
            current_price=current_price,
        )

        if self._grid_active:
            await self._tick_active_grid(current_price, adx, atr)
        else:
            await self._tick_inactive_grid(current_price, adx, atr, bb_squeeze)

    # ── active grid tick ───────────────────────────────────────────────

    async def _tick_active_grid(
        self, current_price: float, adx: float, atr: float
    ) -> None:
        """Handle tick when grid is active: check fills, auto-stop, recalc."""
        assert self._exchange is not None

        # 1. Check auto-stop conditions
        if await self._should_auto_stop(current_price, adx):
            return

        # 2. Check for fills and create opposite orders
        await self._process_fills(current_price)

        # 3. Check if grid needs recalculation (price drift)
        if self._state.should_reset_grid(current_price):
            self._log.info(
                "grid_drift_detected",
                current_price=current_price,
                center=self._grid_center,
            )
            await self._teardown_grid(reason="price_drift")
            await self._place_grid(current_price, atr)

    async def _tick_inactive_grid(
        self,
        current_price: float,
        adx: float,
        atr: float,
        bb_squeeze: bool,
    ) -> None:
        """Check activation conditions and place grid if met."""
        if adx >= self.adx_activation:
            self._log.debug("adx_too_high_for_activation", adx=round(adx, 2))
            return

        if not bb_squeeze:
            self._log.debug("no_bb_squeeze")
            return

        # Conditions met — activate grid
        self._log.info(
            "grid_activation_conditions_met",
            adx=round(adx, 2),
            bb_squeeze=bb_squeeze,
            price=current_price,
        )
        await self._place_grid(current_price, atr)

    # ── auto-stop logic ────────────────────────────────────────────────

    async def _should_auto_stop(self, current_price: float, adx: float) -> bool:
        """Check whether auto-stop triggers have fired."""
        # Trend detected
        if adx > self.adx_stop:
            self._log.warning("auto_stop_adx", adx=round(adx, 2))
            await self._teardown_grid(reason="trend_detected")
            return True

        # Price escape
        if self._grid_center > 0 and self._grid_range_width > 0:
            upper = self._grid_center + self._grid_range_width
            lower = self._grid_center - self._grid_range_width
            escape_margin = (upper - lower) * RANGE_ESCAPE_PCT

            if current_price > upper + escape_margin or current_price < lower - escape_margin:
                self._log.warning(
                    "auto_stop_price_escape",
                    current_price=current_price,
                    upper=upper,
                    lower=lower,
                )
                await self._teardown_grid(reason="price_escape")
                return True

        return False

    # ── grid placement ─────────────────────────────────────────────────

    async def _place_grid(self, current_price: float, atr: float) -> None:
        """Calculate and place a full grid of limit orders."""
        assert self._exchange is not None

        grid_levels = self._calculator.calculate_grid(
            center_price=current_price,
            atr=atr,
            num_grids=min(self.num_grids, MAX_NUM_GRIDS),
            total_capital=self.allocated_capital,
        )

        if not grid_levels:
            self._log.warning("empty_grid_calculated")
            return

        self._grid_center = current_price
        self._grid_range_width = atr * 3

        # Build state tracker levels
        state_levels: list[dict[str, Any]] = []
        for i, level in enumerate(grid_levels):
            state_levels.append(
                {
                    "level_id": f"grid_{i:03d}",
                    "price": level.price,
                    "side": level.side,
                    "quantity": level.quantity,
                }
            )

        self._state.set_grid(state_levels, current_price, self._grid_range_width)

        # Place orders concurrently
        tasks = []
        for lvl in state_levels:
            if not self._state.can_place_more_orders():
                self._log.warning("max_open_orders_reached")
                break

            order = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.symbol,
                side=lvl["side"],
                order_type="limit",
                quantity=lvl["quantity"],
                price=lvl["price"],
                post_only=True,
            )
            tasks.append(self._place_grid_order(lvl["level_id"], order))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._grid_active = True
        self._log.info(
            "grid_placed",
            num_orders=len(tasks),
            center=current_price,
            range_width=self._grid_range_width,
        )

    async def _place_grid_order(self, level_id: str, order: OrderRequest) -> None:
        """Place a single grid order and register it in state."""
        assert self._exchange is not None
        try:
            result = await self._exchange.place_order(order)
            if result and result.order_id:
                self._state.register_order(level_id, result.order_id)
        except Exception:
            self._log.exception("grid_order_error", level_id=level_id)

    # ── fill processing ────────────────────────────────────────────────

    async def _process_fills(self, current_price: float) -> None:
        """Detect filled orders and create opposite-direction orders."""
        assert self._exchange is not None

        open_order_ids = self._state.get_open_order_ids()
        if not open_order_ids:
            return

        # Fetch open orders from exchange to determine which have been filled
        try:
            exchange_orders = await self._exchange.get_open_orders(self.symbol)
            exchange_open_ids = {o.get("id") for o in exchange_orders if o.get("id")}
        except Exception:
            self._log.exception("fetch_open_orders_error")
            return

        # Orders that were open in our state but are no longer open on exchange
        filled_ids = set(open_order_ids) - exchange_open_ids
        if not filled_ids:
            return

        newly_filled = self._state.detect_fills(filled_ids)

        for filled_level in newly_filled:
            self.current_pnl += self._estimate_fill_pnl(filled_level, current_price)

            # Create opposite order
            if self._state.can_place_more_orders():
                opposite_side = self._state.get_opposite_side(filled_level.side)
                opposite_order = OrderRequest(
                    strategy_id=self.strategy_id,
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    side=opposite_side,
                    order_type="limit",
                    quantity=filled_level.quantity,
                    price=filled_level.price,
                    post_only=True,
                )
                await self.submit_order(opposite_order)
                self._log.info(
                    "opposite_order_placed",
                    filled_price=filled_level.price,
                    opposite_side=opposite_side,
                )

    @staticmethod
    def _estimate_fill_pnl(filled_level: Any, current_price: float) -> float:
        """Rough PnL estimate from a grid fill (spread capture)."""
        # Real PnL tracking should come from execution reports; this is an
        # approximation based on the grid spacing.
        return 0.0  # Placeholder — precise PnL from execution service

    # ── teardown ───────────────────────────────────────────────────────

    async def _teardown_grid(self, reason: str) -> None:
        """Cancel all open grid orders and close any positions."""
        assert self._exchange is not None

        self._log.info("tearing_down_grid", reason=reason)

        # Cancel all open orders
        open_ids = self._state.get_open_order_ids()
        cancel_tasks = [
            self._cancel_order_safe(oid) for oid in open_ids
        ]
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

        # Close any open positions
        try:
            position = await self._exchange.get_position(self.symbol)
            if position and abs(float(position.get("contracts", 0))) > 0:
                qty = abs(float(position["contracts"]))
                side = "sell" if float(position["contracts"]) > 0 else "buy"
                close_order = OrderRequest(
                    strategy_id=self.strategy_id,
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    side=side,
                    order_type="market",
                    quantity=qty,
                    post_only=False,
                )
                await self.submit_order(close_order)
                self._log.info("position_closed", side=side, qty=qty)
        except Exception:
            self._log.exception("position_close_error")

        self._state.reset()
        self._grid_active = False

    async def _cancel_order_safe(self, order_id: str) -> None:
        """Cancel a single order, swallowing errors."""
        assert self._exchange is not None
        try:
            await self._exchange.cancel_order(order_id, self.symbol)
        except Exception:
            self._log.exception("cancel_order_error", order_id=order_id)

    # ── indicator calculations ─────────────────────────────────────────

    @staticmethod
    def _calculate_atr(
        highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14
    ) -> float:
        """Calculate Average True Range."""
        if len(closes) < period + 1:
            return 0.0

        tr_list: list[float] = []
        for i in range(1, len(closes)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr_list.append(max(hl, hc, lc))

        tr = np.array(tr_list)
        if len(tr) < period:
            return float(np.mean(tr))

        # Simple moving average of TR for the last *period* values
        return float(np.mean(tr[-period:]))

    @staticmethod
    def _calculate_adx(
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        period: int = 14,
    ) -> float:
        """Calculate Average Directional Index (simplified Wilder's ADX)."""
        if len(closes) < period * 2:
            return 0.0

        plus_dm: list[float] = []
        minus_dm: list[float] = []
        tr_list: list[float] = []

        for i in range(1, len(closes)):
            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]

            plus_dm.append(up if up > down and up > 0 else 0.0)
            minus_dm.append(down if down > up and down > 0 else 0.0)

            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr_list.append(max(hl, hc, lc))

        # Smoothed averages (simple for this implementation)
        n = period
        if len(tr_list) < n:
            return 0.0

        atr = np.mean(tr_list[-n:])
        if atr <= 0:
            return 0.0

        plus_di = 100 * np.mean(plus_dm[-n:]) / atr
        minus_di = 100 * np.mean(minus_dm[-n:]) / atr

        di_sum = plus_di + minus_di
        if di_sum <= 0:
            return 0.0

        dx = 100 * abs(plus_di - minus_di) / di_sum
        return float(dx)

    @staticmethod
    def _detect_bb_squeeze(closes: np.ndarray, period: int = 20) -> bool:
        """Detect Bollinger Band squeeze (low bandwidth)."""
        if len(closes) < period:
            return False

        window = closes[-period:]
        sma = np.mean(window)
        std = np.std(window)

        if sma <= 0:
            return False

        bandwidth = (2 * std * 2) / sma  # (upper - lower) / middle
        return bool(bandwidth < BB_SQUEEZE_THRESHOLD)
