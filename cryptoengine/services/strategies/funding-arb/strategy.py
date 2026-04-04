"""Funding-Rate Arbitrage Strategy.

Captures perpetual-swap funding payments by maintaining a delta-neutral
position: spot long + perp short.  Entry/exit decisions are driven by
the :class:`BasisSpreadStateMachine`.

Entry flow:
    1. Check funding rate > 0.005% and basis spread > 0.3%
    2. Simultaneously buy spot + short perp (Post-Only limit orders)
    3. One-side fill recovery: wait 3 min, cancel unfilled, clean up filled

Exit triggers:
    * Funding rate reverses (negative)
    * Basis divergence exceeds max threshold (1.0%)
    * Basis converges below min threshold (0.1%) -- profit take
    * Kill-switch signal from orchestrator

Exit order: close perp first (higher risk leg), then sell spot.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import structlog

# Allow import of base_strategy from parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from base_strategy import BaseStrategy  # noqa: E402

from shared.exchange import ExchangeConnector, exchange_factory
from shared.models.market import FundingRate
from shared.models.order import OrderRequest, OrderResult
from shared.models.strategy import StrategyStatus

from strategy.basis_spread_sm import BasisAction, BasisSpreadStateMachine
from strategy.delta_neutral import DeltaNeutralManager
from strategy.funding_tracker import FundingTracker

logger = structlog.get_logger()

# ── defaults ────────────────────────────────────────────────────────────
MIN_FUNDING_RATE = 0.00005       # 0.005%
MAX_SPREAD_ENTRY = 0.003         # 0.3%
ONE_SIDE_FILL_TIMEOUT = 180      # 3 minutes
ENTRY_ORDER_TYPE = "limit"


class FundingArbStrategy(BaseStrategy):
    """Delta-neutral funding-rate arbitrage on a single exchange."""

    def __init__(self, strategy_id: str, config: dict[str, Any]) -> None:
        super().__init__(strategy_id, config)

        # Config
        self.spot_symbol: str = config.get("spot_symbol", "BTC/USDT")
        self.perp_symbol: str = config.get("perp_symbol", "BTC/USDT:USDT")
        self.exchange_id: str = config.get("exchange", "binance")
        self.min_funding_rate: float = config.get("min_funding_rate", MIN_FUNDING_RATE)
        self.max_spread_entry: float = config.get("max_spread_entry", MAX_SPREAD_ENTRY)
        self.leverage: float = config.get("leverage", 1.0)

        # Fee rates (Bybit: spot taker 0.01%, perp taker 0.055%)
        self.spot_fee_rate: float = config.get("fees", {}).get("spot_fee_rate", 0.0001)
        self.perp_fee_rate: float = config.get("fees", {}).get("perp_fee_rate", 0.00055)
        self._entry_fee_rate: float = self.spot_fee_rate + self.perp_fee_rate   # 0.00065 편도
        self._exit_fee_rate: float = self.spot_fee_rate + self.perp_fee_rate    # 0.00065 편도
        self._round_trip_fee: float = self._entry_fee_rate + self._exit_fee_rate  # 0.0013 왕복

        # Components (initialised on start)
        self._exchange: ExchangeConnector | None = None
        self._delta_mgr: DeltaNeutralManager | None = None
        self._funding_tracker: FundingTracker | None = None
        self._basis_sm: BasisSpreadStateMachine | None = None

        # Position state
        self._spot_qty: float = 0.0
        self._perp_qty: float = 0.0
        self._entry_price: float = 0.0
        self._pending_entry: bool = False

        self._log = logger.bind(strategy_id=strategy_id, strategy="funding_arb")

    # ── lifecycle ───────────────────────────────────────────────────────

    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialise exchange connection and controllers."""
        self._exchange = exchange_factory(self.exchange_id)
        await self._exchange.connect()

        self._delta_mgr = DeltaNeutralManager(
            strategy_id=self.strategy_id,
            exchange=self._exchange,
            spot_symbol=self.spot_symbol,
            perp_symbol=self.perp_symbol,
        )
        self.register_controller("delta_neutral", self._delta_mgr)

        self._funding_tracker = FundingTracker(
            strategy_id=self.strategy_id,
            symbol=self.perp_symbol,
            redis=self._redis,
        )
        await self._funding_tracker.load_from_redis()
        self.register_controller("funding_tracker", self._funding_tracker)

        sm_config = self.config.get("basis_spread", {})
        self._basis_sm = BasisSpreadStateMachine(
            min_divergence=sm_config.get("min_divergence", 0.003),
            min_convergence=sm_config.get("min_convergence", 0.001),
            max_divergence=sm_config.get("max_divergence", 0.010),
        )
        self.register_controller("basis_sm", self._basis_sm)

        self._log.info(
            "strategy_started",
            capital=capital,
            spot_symbol=self.spot_symbol,
            perp_symbol=self.perp_symbol,
        )

    async def on_stop(self, reason: str) -> None:
        """Exit all positions and disconnect."""
        self._log.info("strategy_stopping", reason=reason)

        if self._basis_sm and self._basis_sm.is_open:
            await self._exit_position(reason=reason)

        if self._exchange:
            await self._exchange.disconnect()

        self._log.info("strategy_stopped", reason=reason)

    async def get_status(self) -> StrategyStatus:
        return StrategyStatus(
            strategy_id=self.strategy_id,
            is_running=self.is_running,
            allocated_capital=self.allocated_capital,
            current_pnl=self.current_pnl,
            position_count=1 if (self._basis_sm and self._basis_sm.is_open) else 0,
        )

    async def _rebalance(self, new_capital: float) -> None:
        """Adjust position size when capital allocation changes."""
        if not self._basis_sm or not self._basis_sm.is_open:
            return

        old_capital = self.allocated_capital
        ratio = new_capital / old_capital if old_capital > 0 else 1.0

        if abs(ratio - 1.0) < 0.05:
            return  # less than 5% change -- skip

        target_qty = self._spot_qty * ratio
        diff = target_qty - self._spot_qty

        self._log.info(
            "rebalancing_position",
            old_capital=old_capital,
            new_capital=new_capital,
            qty_diff=diff,
        )

        if diff > 0:
            await self._adjust_position(abs(diff), increase=True)
        elif diff < 0:
            await self._adjust_position(abs(diff), increase=False)

    # ── main tick ───────────────────────────────────────────────────────

    async def tick(self) -> None:
        """Main strategy tick -- called every interval."""
        assert self._exchange is not None
        assert self._funding_tracker is not None
        assert self._delta_mgr is not None
        assert self._basis_sm is not None

        # 1. Fetch current market data
        funding = await self._exchange.get_funding_rate(self.perp_symbol)
        spot_ticker = await self._exchange.get_ticker(self.spot_symbol)
        perp_ticker = await self._exchange.get_ticker(self.perp_symbol)

        spot_price = float(spot_ticker.get("last", 0))
        perp_price = float(perp_ticker.get("last", 0))

        if spot_price <= 0 or perp_price <= 0:
            self._log.warning("invalid_prices", spot=spot_price, perp=perp_price)
            return

        # 2. Calculate basis spread
        basis_spread = (perp_price - spot_price) / spot_price

        # 3. Funding timing checks
        if self._funding_tracker.should_verify_position():
            await self._verify_position_for_funding()

        if self._funding_tracker.is_post_funding():
            await self._process_funding_payment(funding)

        # 4. Update delta neutral manager
        perp_position = await self._exchange.get_position(self.perp_symbol)
        self._delta_mgr.update_margin(perp_position)
        self._delta_mgr.update_quantities(self._spot_qty, self._perp_qty)

        # 5. Check margin health
        if not self._delta_mgr.is_margin_healthy():
            margin_orders = await self._delta_mgr.check_margin_risk(spot_price)
            for order in margin_orders:
                await self.submit_order(order)
            return

        # 6. Check delta balance
        if not self._delta_mgr.is_balanced():
            rebal_orders = await self._delta_mgr.check_and_rebalance()
            for order in rebal_orders:
                await self.submit_order(order)

        # 7. State machine evaluation
        action = self._basis_sm.evaluate(basis_spread)

        match action:
            case BasisAction.ENTER:
                if self._check_entry_conditions(funding, basis_spread):
                    await self._enter_position(spot_price, perp_price, basis_spread)

            case BasisAction.EXIT_PROFIT:
                if not self._funding_tracker.is_liquidation_blocked():
                    await self._exit_position(reason="basis_convergence")

            case BasisAction.EXIT_RISK:
                await self._exit_position(reason="basis_divergence_risk")

            case BasisAction.HOLD:
                pass

        # 8. Check funding reversal exit
        if self._basis_sm.is_open and funding.rate < 0:
            if not self._funding_tracker.is_liquidation_blocked():
                self._log.warning("funding_rate_negative", rate=funding.rate)
                await self._exit_position(reason="funding_reversal")

    # ── entry logic ─────────────────────────────────────────────────────

    def _check_entry_conditions(self, funding: FundingRate, basis_spread: float) -> bool:
        """Verify all conditions for entering a position."""
        if self._pending_entry:
            return False

        if funding.rate < self.min_funding_rate:
            self._log.debug(
                "funding_rate_too_low",
                rate=funding.rate,
                threshold=self.min_funding_rate,
            )
            return False

        if basis_spread > self.max_spread_entry:
            self._log.debug(
                "spread_too_wide",
                spread=round(basis_spread * 100, 4),
                threshold=round(self.max_spread_entry * 100, 4),
            )
            return False

        return True

    async def _enter_position(
        self, spot_price: float, perp_price: float, basis_spread: float
    ) -> None:
        """Enter delta-neutral position: spot buy + perp short simultaneously."""
        assert self._exchange is not None
        assert self._basis_sm is not None

        self._pending_entry = True
        quantity = self._calculate_position_size(spot_price)

        self._log.info(
            "entering_position",
            quantity=quantity,
            spot_price=spot_price,
            perp_price=perp_price,
            basis_spread=round(basis_spread * 100, 4),
        )

        # Submit both legs simultaneously
        spot_order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.spot_symbol,
            side="buy",
            order_type=ENTRY_ORDER_TYPE,
            quantity=quantity,
            price=spot_price,
            post_only=True,
        )
        perp_order = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.perp_symbol,
            side="sell",
            order_type=ENTRY_ORDER_TYPE,
            quantity=quantity,
            price=perp_price,
            post_only=True,
        )

        spot_result, perp_result = await asyncio.gather(
            self._place_and_track(spot_order),
            self._place_and_track(perp_order),
        )

        # One-side fill recovery
        await self._handle_partial_fill(spot_result, perp_result, quantity)

        if self._spot_qty > 0 and self._perp_qty > 0:
            self._basis_sm.enter_position(basis_spread)
            self._entry_price = spot_price

            # Deduct entry fees: spot taker + perp taker
            entry_notional = self._spot_qty * spot_price
            entry_fee = entry_notional * self._entry_fee_rate * 2  # both legs
            self.current_pnl -= entry_fee

            self._log.info(
                "position_entered",
                spot_qty=self._spot_qty,
                perp_qty=self._perp_qty,
                entry_fee=round(entry_fee, 6),
            )
        else:
            self._log.warning("entry_failed_no_fill")

        self._pending_entry = False

    async def _handle_partial_fill(
        self,
        spot_result: OrderResult | None,
        perp_result: OrderResult | None,
        intended_qty: float,
    ) -> None:
        """Handle one-side fill scenario.

        Wait up to 3 minutes for the other leg to fill.  If it does not,
        cancel the pending order and unwind the filled leg.
        """
        assert self._exchange is not None

        spot_filled = spot_result is not None and spot_result.status == "filled"
        perp_filled = perp_result is not None and perp_result.status == "filled"

        if spot_filled and perp_filled:
            self._spot_qty = spot_result.filled_qty  # type: ignore[union-attr]
            self._perp_qty = perp_result.filled_qty  # type: ignore[union-attr]
            return

        if not spot_filled and not perp_filled:
            self._log.warning("both_legs_unfilled")
            return

        # One side filled, other pending
        self._log.info(
            "one_side_fill_recovery",
            spot_filled=spot_filled,
            perp_filled=perp_filled,
            wait_seconds=ONE_SIDE_FILL_TIMEOUT,
        )

        filled_result = spot_result if spot_filled else perp_result
        unfilled_result = perp_result if spot_filled else spot_result

        waited = 0.0
        check_interval = 10.0
        while waited < ONE_SIDE_FILL_TIMEOUT:
            await asyncio.sleep(check_interval)
            waited += check_interval
            self._log.debug("waiting_for_fill", waited=waited)

        # Timeout -- cancel unfilled leg
        if unfilled_result is not None:
            try:
                unfilled_symbol = self.perp_symbol if spot_filled else self.spot_symbol
                await self._exchange.cancel_order(unfilled_result.order_id, unfilled_symbol)
                self._log.info("unfilled_leg_cancelled", order_id=unfilled_result.order_id)
            except Exception:
                self._log.exception("cancel_unfilled_error")

        # Clean up the filled leg
        if spot_filled and filled_result is not None:
            cleanup = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.spot_symbol,
                side="sell",
                order_type="market",
                quantity=filled_result.filled_qty,
                post_only=False,
            )
            await self.submit_order(cleanup)
            self._log.info("spot_cleanup_submitted", qty=filled_result.filled_qty)
        elif perp_filled and filled_result is not None:
            cleanup = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.perp_symbol,
                side="buy",
                order_type="market",
                quantity=filled_result.filled_qty,
                reduce_only=True,
                post_only=False,
            )
            await self.submit_order(cleanup)
            self._log.info("perp_cleanup_submitted", qty=filled_result.filled_qty)

        # Reset -- entry failed
        self._spot_qty = 0.0
        self._perp_qty = 0.0

    # ── exit logic ──────────────────────────────────────────────────────

    async def _exit_position(self, reason: str) -> None:
        """Exit position.  Close perp first (higher risk), then sell spot."""
        assert self._exchange is not None
        assert self._basis_sm is not None

        self._log.info("exiting_position", reason=reason)

        # 1. Close perp short first (higher risk leg)
        if self._perp_qty > 0:
            perp_close = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.perp_symbol,
                side="buy",
                order_type="market",
                quantity=self._perp_qty,
                reduce_only=True,
                post_only=False,
            )
            await self.submit_order(perp_close)
            self._log.info("perp_close_submitted", qty=self._perp_qty)

        # 2. Then sell spot
        if self._spot_qty > 0:
            spot_close = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.spot_symbol,
                side="sell",
                order_type="market",
                quantity=self._spot_qty,
                post_only=False,
            )
            await self.submit_order(spot_close)
            self._log.info("spot_close_submitted", qty=self._spot_qty)

        # Finalise state machine
        perp_ticker = await self._exchange.get_ticker(self.perp_symbol)
        spot_ticker = await self._exchange.get_ticker(self.spot_symbol)
        spot_price = float(spot_ticker.get("last", 0))
        perp_price = float(perp_ticker.get("last", 0))
        exit_spread = (perp_price - spot_price) / spot_price if spot_price > 0 else 0.0

        # Deduct exit fees: spot taker (sell) + perp taker (buy to close)
        exit_notional = self._spot_qty * spot_price if spot_price > 0 else 0.0
        exit_fee = exit_notional * self._exit_fee_rate * 2  # both legs
        self.current_pnl -= exit_fee

        pnl = self._basis_sm.exit_position(exit_spread)
        self.current_pnl += pnl.total_pnl

        self._spot_qty = 0.0
        self._perp_qty = 0.0
        self._entry_price = 0.0

        self._log.info(
            "position_exited",
            reason=reason,
            basis_pnl=round(pnl.basis_pnl * 100, 4),
            funding_pnl=round(pnl.funding_pnl, 6),
            exit_fee=round(exit_fee, 6),
        )

    # ── position adjustment ─────────────────────────────────────────────

    async def _adjust_position(self, qty_diff: float, increase: bool) -> None:
        """Increase or decrease both legs proportionally."""
        if increase:
            spot_order = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.spot_symbol,
                side="buy",
                order_type="limit",
                quantity=qty_diff,
                post_only=True,
            )
            perp_order = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.perp_symbol,
                side="sell",
                order_type="limit",
                quantity=qty_diff,
                post_only=True,
            )
        else:
            spot_order = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.spot_symbol,
                side="sell",
                order_type="limit",
                quantity=qty_diff,
                post_only=True,
            )
            perp_order = OrderRequest(
                strategy_id=self.strategy_id,
                exchange=self.exchange_id,
                symbol=self.perp_symbol,
                side="buy",
                order_type="limit",
                quantity=qty_diff,
                reduce_only=True,
                post_only=True,
            )

        await asyncio.gather(
            self.submit_order(spot_order),
            self.submit_order(perp_order),
        )

    # ── helpers ─────────────────────────────────────────────────────────

    def _calculate_position_size(self, price: float) -> float:
        """Determine BTC quantity based on allocated capital and leverage."""
        if price <= 0:
            return 0.0
        # Use 95% of allocated capital (5% buffer for fees/slippage)
        usable_capital = self.allocated_capital * 0.95
        return usable_capital / price

    async def _place_and_track(self, order: OrderRequest) -> OrderResult | None:
        """Place order via exchange and return result."""
        assert self._exchange is not None
        try:
            result = await self._exchange.place_order(order)
            self._log.info(
                "order_result",
                request_id=order.request_id,
                status=result.status,
                filled_qty=result.filled_qty,
            )
            return result
        except Exception:
            self._log.exception("order_placement_error", request_id=order.request_id)
            return None

    async def _verify_position_for_funding(self) -> None:
        """5 minutes before funding: verify position size to maximise payment."""
        assert self._delta_mgr is not None
        if not self._delta_mgr.is_balanced():
            self._log.warning(
                "position_unbalanced_before_funding",
                divergence=self._delta_mgr.quantity_divergence,
            )
            rebal_orders = await self._delta_mgr.check_and_rebalance()
            for order in rebal_orders:
                await self.submit_order(order)

    async def _process_funding_payment(self, funding: FundingRate) -> None:
        """Record funding payment after it occurs."""
        assert self._funding_tracker is not None
        assert self._basis_sm is not None

        if not self._basis_sm.is_open or self._perp_qty <= 0:
            return

        payment = self._perp_qty * funding.rate
        await self._funding_tracker.record_payment(
            rate=funding.rate,
            position_size=self._perp_qty,
            payment=payment,
        )
        self._basis_sm.record_funding(payment)
        self.current_pnl += payment
