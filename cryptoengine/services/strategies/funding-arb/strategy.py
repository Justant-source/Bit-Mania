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
import time
from typing import Any

import structlog

from shared.log_events import *

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
ONE_SIDE_FILL_TIMEOUT = 60       # 1 minute (delta-neutral gap 최소화)
ENTRY_ORDER_TYPE = "limit"

# 재시작 시 포지션을 청산하지 않고 유지할 종료 사유
_SHUTDOWN_NO_LIQUIDATE = frozenset({"service_shutdown"})
_SAVED_STATE_KEY_PREFIX = "strategy:saved_state"
_SAVED_STATE_TTL = 3600  # 1시간: 이 시간 안에 재시작되면 포지션 복구


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
        # fa80_lev5_r30: leverage=5, fa_capital_ratio=0.80, reinvest_ratio=0.30
        self.leverage: float = config.get("leverage", 5.0)
        # fa_capital_ratio: informational — orchestrator allocates capital; logged on start
        self.fa_capital_ratio: float = config.get("fa_capital_ratio", 0.80)
        # reinvest_ratio: fraction of realized funding profit to reinvest into spot BTC
        self.reinvest_ratio: float = config.get("reinvest_ratio", 0.30)
        # Accumulated spot BTC from reinvestment (tracked separately from hedge position)
        self._reinvested_btc: float = 0.0
        self._total_reinvested_usd: float = 0.0

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

        # Max holding period (seconds).  Tiered exits fire at 50% / 75% / 100%.
        _max_holding_hours: float = config.get("exit", {}).get("max_holding_hours", 720)
        self._max_hold_seconds: float = _max_holding_hours * 3600.0

        # Position state
        self._spot_qty: float = 0.0
        self._perp_qty: float = 0.0
        self._entry_price: float = 0.0
        self._pending_entry: bool = False
        # Monotonic timestamp recorded when the position is opened
        self._position_open_ts: float = 0.0
        # Track which tiered-exit tiers have already been executed
        # Tier indices: 0 = 50% close at 50% duration
        #               1 = 30% close at 75% duration
        #               2 = 20% close at 100% duration (full exit path)
        self._tiered_exit_done: list[bool] = [False, False, False]

        self._log = logger.bind(strategy_id=strategy_id, strategy="funding_arb")

    # ── lifecycle ───────────────────────────────────────────────────────

    async def on_start(self, capital: float, params: dict[str, Any]) -> None:
        """Initialise exchange connection and controllers."""
        self._exchange = exchange_factory(
            self.exchange_id,
            api_key=os.environ.get("BYBIT_API_KEY", ""),
            api_secret=os.environ.get("BYBIT_API_SECRET", ""),
            testnet=os.environ.get("BYBIT_TESTNET", "true").lower() == "true",
        )
        await self._exchange.connect()

        # Set isolated margin mode and leverage before any order placement
        # This must be called after connect() and before any order
        try:
            await self._exchange.set_margin_mode(self.perp_symbol, "isolated")
            await self._exchange.set_leverage(self.perp_symbol, int(self.leverage))
        except Exception as exc:
            self._log.warning("leverage_margin_setup_warning", exc=str(exc))

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

        # 재시작 후 저장된 포지션 상태 복구 시도
        recovered = await self._restore_position_state()

        self._log.info(
            STRATEGY_STARTED,
            message="펀딩비 차익거래 전략 시작",
            capital=capital,
            spot_symbol=self.spot_symbol,
            perp_symbol=self.perp_symbol,
            leverage=self.leverage,
            fa_capital_ratio=self.fa_capital_ratio,
            reinvest_ratio=self.reinvest_ratio,
            position_recovered=recovered,
        )

    async def on_stop(self, reason: str) -> None:
        """Exit all positions and disconnect.

        service_shutdown (배포·재시작) 시에는 포지션을 청산하지 않고
        Redis에 상태를 저장한다. 재시작 후 on_start()에서 복구한다.
        그 외 사유(kill_switch, 리스크 트리거 등)는 즉시 청산한다.
        """
        if reason in _SHUTDOWN_NO_LIQUIDATE:
            if self._basis_sm and self._basis_sm.is_open:
                await self._save_position_state()
                self._log.info(
                    SERVICE_STOPPING,
                    message="포지션 유지하며 종료 — 재시작 후 복구 예정",
                    reason=reason,
                    spot_qty=self._spot_qty,
                    perp_qty=self._perp_qty,
                )
            else:
                self._log.info(SERVICE_STOPPING, message="포지션 없음, 종료", reason=reason)
        else:
            self._log.info(SERVICE_STOPPING, message="전략 종료 중", reason=reason)
            if self._basis_sm and self._basis_sm.is_open:
                await self._exit_position(reason=reason)

        if self._exchange:
            await self._exchange.disconnect()

        self._log.info(STRATEGY_STOPPED, message="전략 종료 완료", reason=reason)

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
            STRATEGY_REBALANCE,
            message="포지션 재조정",
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

        # 8. Check tiered exits based on hold duration
        await self._check_tiered_exits()

        # 9. Check funding reversal exit
        if self._basis_sm.is_open and funding.rate < 0:
            if not self._funding_tracker.is_liquidation_blocked():
                self._log.warning(STRATEGY_CIRCUIT_BREAKER, message="펀딩비 음수 전환, 포지션 종료", rate=funding.rate)
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
            FA_ENTRY_CONDITION_MET,
            message="진입 조건 충족, 포지션 진입 중",
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
            self._position_open_ts = time.monotonic()
            self._tiered_exit_done = [False, False, False]

            # Deduct entry fees: spot taker + perp taker
            entry_notional = self._spot_qty * spot_price
            entry_fee = entry_notional * self._entry_fee_rate * 2  # both legs
            self.current_pnl -= entry_fee

            self._log.info(
                FA_POSITION_OPENED,
                message="포지션 진입 완료",
                spot_qty=self._spot_qty,
                perp_qty=self._perp_qty,
                entry_fee=round(entry_fee, 6),
                max_hold_hours=self._max_hold_seconds / 3600,
            )
        else:
            self._log.warning(FA_ONE_SIDE_FILL, message="진입 실패: 미체결")

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
            FA_ONE_SIDE_RECOVERY,
            message="한쪽 레그 체결, 반대편 대기 중",
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

        self._log.info(FA_POSITION_CLOSED, message="포지션 종료 중", reason=reason)

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
        self._position_open_ts = 0.0
        self._tiered_exit_done = [False, False, False]

        self._log.info(
            FA_POSITION_CLOSED,
            message="포지션 종료 완료",
            reason=reason,
            basis_pnl=round(pnl.basis_pnl * 100, 4),
            funding_pnl=round(pnl.funding_pnl, 6),
            exit_fee=round(exit_fee, 6),
        )

    # ── tiered exit ─────────────────────────────────────────────────────

    async def _check_tiered_exits(self) -> None:
        """Check whether a tiered partial exit should fire based on hold duration.

        Tier schedule (fractions of max_hold_seconds):
          Tier 0 — 50% of max_hold: close 50% of position
          Tier 1 — 75% of max_hold: close 30% more  (80% total closed)
          Tier 2 — 100% of max_hold: close remaining 20% (full exit)

        The final tier delegates to ``_exit_position`` so that the state
        machine and fee accounting are finalised correctly.
        """
        if not self._basis_sm or not self._basis_sm.is_open:
            return
        if self._position_open_ts <= 0 or self._max_hold_seconds <= 0:
            return
        if self._funding_tracker and self._funding_tracker.is_liquidation_blocked():
            return

        elapsed = time.monotonic() - self._position_open_ts
        fraction = elapsed / self._max_hold_seconds

        # Tier 0: ≥50% of max_hold — close 50%
        if not self._tiered_exit_done[0] and fraction >= 0.50:
            close_qty = self._spot_qty * 0.50
            self._log.info(
                "tiered_exit_tier0",
                elapsed_h=round(elapsed / 3600, 2),
                close_qty=round(close_qty, 6),
                pct=50,
            )
            await self._partial_exit(close_qty, reason="tiered_exit_50pct")
            self._tiered_exit_done[0] = True

        # Tier 1: ≥75% of max_hold — close 30% of *original* size (≈37.5% of remaining)
        if not self._tiered_exit_done[1] and fraction >= 0.75:
            # Remaining after tier 0 is ~50%.  We want to close another 30% of original.
            # Guard against rounding: cap at current qty.
            close_qty = min(self._spot_qty * 0.60, self._spot_qty)
            self._log.info(
                "tiered_exit_tier1",
                elapsed_h=round(elapsed / 3600, 2),
                close_qty=round(close_qty, 6),
                pct=30,
            )
            await self._partial_exit(close_qty, reason="tiered_exit_80pct_total")
            self._tiered_exit_done[1] = True

        # Tier 2: ≥100% of max_hold — close remainder via full exit
        if not self._tiered_exit_done[2] and fraction >= 1.00:
            self._log.info(
                "tiered_exit_tier2",
                elapsed_h=round(elapsed / 3600, 2),
                remaining_qty=round(self._spot_qty, 6),
                pct=20,
            )
            self._tiered_exit_done[2] = True
            await self._exit_position(reason="max_hold_tiered_final")

    async def _partial_exit(self, qty: float, reason: str) -> None:
        """Close *qty* of the position on both legs without finalising the state machine.

        Fees for the partial close are deducted from ``current_pnl`` immediately.
        The state machine remains open; only a full ``_exit_position`` call closes it.
        """
        assert self._exchange is not None

        if qty <= 0:
            return

        # Close perp short first (higher risk leg)
        perp_close = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.perp_symbol,
            side="buy",
            order_type="market",
            quantity=qty,
            reduce_only=True,
            post_only=False,
        )
        await self.submit_order(perp_close)
        self._log.info("partial_perp_close_submitted", qty=qty, reason=reason)

        # Then sell spot
        spot_close = OrderRequest(
            strategy_id=self.strategy_id,
            exchange=self.exchange_id,
            symbol=self.spot_symbol,
            side="sell",
            order_type="market",
            quantity=qty,
            post_only=False,
        )
        await self.submit_order(spot_close)
        self._log.info("partial_spot_close_submitted", qty=qty, reason=reason)

        # Estimate price for fee calculation
        spot_ticker = await self._exchange.get_ticker(self.spot_symbol)
        spot_price = float(spot_ticker.get("last", self._entry_price))
        exit_notional = qty * spot_price if spot_price > 0 else 0.0
        exit_fee = exit_notional * self._exit_fee_rate * 2  # both legs
        self.current_pnl -= exit_fee

        # Reduce tracked quantities
        self._spot_qty = max(0.0, self._spot_qty - qty)
        self._perp_qty = max(0.0, self._perp_qty - qty)

        self._log.info(
            "partial_exit_done",
            reason=reason,
            closed_qty=qty,
            exit_fee=round(exit_fee, 6),
            remaining_spot=round(self._spot_qty, 6),
            remaining_perp=round(self._perp_qty, 6),
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

    # ── restart recovery ────────────────────────────────────────────────

    async def _save_position_state(self) -> None:
        """종료 전 포지션 상태를 Redis에 저장 (service_shutdown 전용)."""
        assert self._basis_sm is not None
        state = {
            "spot_qty": self._spot_qty,
            "perp_qty": self._perp_qty,
            "entry_price": self._entry_price,
            "saved_wall_time": time.time(),
            "tiered_exit_done": self._tiered_exit_done,
            "reinvested_btc": self._reinvested_btc,
            "total_reinvested_usd": self._total_reinvested_usd,
            "entry_spread": self._basis_sm.pnl.entry_spread,
            "funding_pnl": self._basis_sm.pnl.funding_pnl,
            "funding_payments_collected": self._basis_sm.pnl.funding_payments_collected,
        }
        key = f"{_SAVED_STATE_KEY_PREFIX}:{self.strategy_id}"
        await self._redis.set(key, state, ttl=_SAVED_STATE_TTL)
        self._log.info(
            SERVICE_STOPPING,
            message="포지션 상태 저장 완료",
            spot_qty=self._spot_qty,
            perp_qty=self._perp_qty,
            ttl_sec=_SAVED_STATE_TTL,
        )

    async def _restore_position_state(self) -> bool:
        """Redis에서 포지션 상태 복구 시도. 복구 성공 시 True 반환.

        거래소 실제 포지션을 ground truth로 사용한다.
        저장된 상태가 없거나 거래소에 포지션이 없으면 신규 시작으로 처리한다.
        """
        assert self._exchange is not None
        assert self._basis_sm is not None

        key = f"{_SAVED_STATE_KEY_PREFIX}:{self.strategy_id}"
        raw = await self._redis.get(key)
        if not raw:
            return False

        import json as _json
        try:
            state = _json.loads(raw)
        except Exception:
            self._log.warning("position_state_parse_error", message="저장 상태 파싱 실패")
            await self._redis.cache_delete(key)
            return False

        # 거래소 실제 포지션 확인 (ground truth)
        exchange_pos = await self._exchange.get_position(self.perp_symbol)
        if exchange_pos is None or exchange_pos.size == 0:
            self._log.warning(
                "position_state_mismatch",
                message="저장 상태 있으나 거래소 포지션 없음 — 신규 시작",
            )
            await self._redis.cache_delete(key)
            return False

        # 포지션 크기는 거래소 값 우선, 나머지는 저장 상태에서 복원
        self._perp_qty = exchange_pos.size
        self._spot_qty = state.get("spot_qty", exchange_pos.size)
        self._entry_price = state.get("entry_price", float(exchange_pos.entry_price or 0))
        self._tiered_exit_done = state.get("tiered_exit_done", [False, False, False])
        self._reinvested_btc = state.get("reinvested_btc", 0.0)
        self._total_reinvested_usd = state.get("total_reinvested_usd", 0.0)

        # monotonic 타임스탬프: 저장 시각 기준 경과 시간으로 역산
        saved_wall_time = state.get("saved_wall_time", time.time())
        elapsed = time.time() - saved_wall_time
        self._position_open_ts = time.monotonic() - elapsed

        # BasisSM 상태 복원
        from datetime import datetime, timezone
        from strategy.basis_spread_sm import BasisPnL, BasisState
        self._basis_sm.state = BasisState.OPENED
        self._basis_sm._entry_time = datetime.now(timezone.utc)
        self._basis_sm.pnl = BasisPnL(
            entry_spread=state.get("entry_spread", 0.0),
            funding_pnl=state.get("funding_pnl", 0.0),
            funding_payments_collected=state.get("funding_payments_collected", 0),
        )

        await self._redis.cache_delete(key)

        self._log.info(
            STRATEGY_STARTED,
            message="포지션 상태 복구 완료 (재시작)",
            spot_qty=self._spot_qty,
            perp_qty=self._perp_qty,
            entry_price=self._entry_price,
            elapsed_since_save_sec=round(elapsed, 1),
        )
        return True

    # ── helpers ─────────────────────────────────────────────────────────

    def _calculate_position_size(self, price: float) -> float:
        """Determine BTC quantity for delta-neutral position (spot qty = perp qty).

        Capital allocation model (fa80_lev5_r30):
          - allocated_capital = orchestrator-assigned capital (already reflects fa_capital_ratio)
          - With leverage L on perp: margin_needed = qty * price / L
          - Total capital = spot_value + perp_margin = qty * price * (1 + 1/L)
          - Solving: qty = usable / (price * (1 + 1/L))

        At 5x leverage: capital_factor = 1.2  → 20% more BTC per dollar vs 2x (factor=1.5)
        """
        if price <= 0:
            return 0.0
        usable_capital = self.allocated_capital * 0.95  # 5% buffer for fees/slippage
        leverage_factor = 1.0 + (1.0 / self.leverage) if self.leverage > 0 else 2.0
        return usable_capital / (price * leverage_factor)

    async def _place_and_track(self, order: OrderRequest) -> OrderResult | None:
        """Place order via execution engine (Redis) for safety validation.

        Routes through order:request channel → ExecutionEngine → SafetyGuard → exchange.
        Falls back to direct exchange call only when execution engine is unavailable.
        """
        assert self._exchange is not None
        try:
            # Primary path: route through execution engine via Redis (SafetyGuard applied)
            await self.submit_order(order)
            self._log.info(
                ORDER_SUBMITTED,
                message="주문 실행엔진 경유 제출",
                request_id=order.request_id,
                symbol=order.symbol,
                side=order.side,
                qty=order.quantity,
            )
            # Return a synthetic pending result — actual fill tracked via order:update channel
            from shared.models.order import OrderResult
            from datetime import datetime, timezone
            return OrderResult(
                request_id=order.request_id,
                order_id="",
                status="pending",
                filled_qty=0.0,
                filled_price=None,
                fee=0.0,
                fee_currency="USDT",
                timestamp=datetime.now(tz=timezone.utc),
            )
        except Exception:
            self._log.exception("order_submission_error", request_id=order.request_id)
            return None

    async def _verify_position_for_funding(self) -> None:
        """5 minutes before funding: verify position size to maximise payment."""
        assert self._delta_mgr is not None
        if not self._delta_mgr.is_balanced():
            self._log.warning(
                FA_HEDGE_DRIFT,
                message="펀딩 수취 전 포지션 불균형 감지",
                divergence=self._delta_mgr.quantity_divergence,
            )
            rebal_orders = await self._delta_mgr.check_and_rebalance()
            for order in rebal_orders:
                await self.submit_order(order)

    async def _process_funding_payment(self, funding: FundingRate) -> None:
        """Record funding payment and optionally reinvest into spot BTC.

        Reinvest logic (reinvest_ratio=0.30 per fa80_lev5_r30):
          - On each positive funding payment, 30% is used to buy additional spot BTC
          - Reinvested BTC is held separately from the hedge position (_reinvested_btc)
          - This does NOT affect the delta-neutral hedge; it is an additive BTC accumulation
        """
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

        # Reinvest positive funding into spot BTC accumulation
        if payment > 0 and self.reinvest_ratio > 0 and self._exchange is not None:
            reinvest_usd = payment * self.reinvest_ratio
            try:
                spot_ticker = await self._exchange.get_ticker(self.spot_symbol)
                spot_price = float(spot_ticker.get("last", 0))
                if spot_price > 0:
                    btc_qty = reinvest_usd / spot_price
                    reinvest_order = OrderRequest(
                        strategy_id=self.strategy_id,
                        exchange=self.exchange_id,
                        symbol=self.spot_symbol,
                        side="buy",
                        order_type="market",
                        quantity=btc_qty,
                        post_only=False,
                    )
                    await self.submit_order(reinvest_order)
                    self._reinvested_btc += btc_qty
                    self._total_reinvested_usd += reinvest_usd
                    self.current_pnl -= reinvest_usd  # deducted from FA PnL; held as spot
                    self._log.info(
                        FA_REINVEST,
                        message="펀딩비 재투자 완료",
                        payment=round(payment, 6),
                        reinvest_usd=round(reinvest_usd, 6),
                        btc_qty=round(btc_qty, 8),
                        total_reinvested_btc=round(self._reinvested_btc, 8),
                    )
            except Exception:
                self._log.exception("reinvest_order_error", reinvest_usd=reinvest_usd)
