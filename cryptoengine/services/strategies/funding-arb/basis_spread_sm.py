"""Hummingbot-style Basis Spread State Machine.

Manages the lifecycle of a funding-arbitrage position through two states:

* **Closed** — no position; watching for entry opportunity.
* **Opened** — position active; collecting funding and watching for exit.

Transition rules::

    Closed --(basis_spread > min_divergence 0.3%)--> Opened
    Opened --(basis < min_convergence 0.1%)--------> Closed   (profit exit)
    Opened --(basis > max_divergence 1.0%)---------> Closed   (risk exit)
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone

import structlog

from shared.log_events import *

logger = structlog.get_logger()


class BasisState(str, enum.Enum):
    """Position lifecycle state."""

    CLOSED = "closed"
    OPENED = "opened"


@dataclass
class BasisPnL:
    """Track P&L breakdown: basis convergence vs. funding income."""

    basis_pnl: float = 0.0
    funding_pnl: float = 0.0
    entry_spread: float = 0.0
    funding_payments_collected: int = 0

    @property
    def total_pnl(self) -> float:
        return self.basis_pnl + self.funding_pnl


@dataclass
class BasisSpreadStateMachine:
    """Hummingbot-style state machine for basis spread arbitrage.

    Parameters
    ----------
    min_divergence : float
        Minimum basis spread to trigger entry (default 0.3%).
    min_convergence : float
        Basis spread below which we exit for profit (default 0.1%).
    max_divergence : float
        Basis spread above which we exit for risk (default 1.0%).
    """

    min_divergence: float = 0.003   # 0.3%
    min_convergence: float = 0.001  # 0.1%
    max_divergence: float = 0.010   # 1.0%

    state: BasisState = field(default=BasisState.CLOSED)
    pnl: BasisPnL = field(default_factory=BasisPnL)
    _entry_time: datetime | None = field(default=None, repr=False)
    _log: structlog.BoundLogger = field(
        default_factory=lambda: logger.bind(component="basis_spread_sm"),
        repr=False,
    )

    # ── state evaluation ────────────────────────────────────────────────

    def evaluate(self, basis_spread: float) -> BasisAction:
        """Evaluate current basis spread and return the required action.

        Parameters
        ----------
        basis_spread : float
            Current basis spread as a decimal (e.g. 0.005 = 0.5%).

        Returns
        -------
        BasisAction
            ``ENTER``, ``EXIT_PROFIT``, ``EXIT_RISK``, or ``HOLD``.
        """
        if self.state == BasisState.CLOSED:
            if basis_spread >= self.min_divergence:
                self._log.info(
                    FA_ENTRY_CONDITION_MET,
                    message="베이시스 진입 신호",
                    spread=round(basis_spread * 100, 4),
                    threshold=round(self.min_divergence * 100, 4),
                )
                return BasisAction.ENTER
            return BasisAction.HOLD

        # State is OPENED
        if basis_spread <= self.min_convergence:
            self._log.info(
                STRATEGY_SIGNAL,
                message="베이시스 수렴, 익절 종료",
                spread=round(basis_spread * 100, 4),
                threshold=round(self.min_convergence * 100, 4),
            )
            return BasisAction.EXIT_PROFIT

        if basis_spread >= self.max_divergence:
            self._log.warning(
                STRATEGY_CIRCUIT_BREAKER,
                message="베이시스 과도 확대, 리스크 종료",
                spread=round(basis_spread * 100, 4),
                threshold=round(self.max_divergence * 100, 4),
            )
            return BasisAction.EXIT_RISK

        return BasisAction.HOLD

    # ── state transitions ───────────────────────────────────────────────

    def enter_position(self, entry_spread: float) -> None:
        """Transition from CLOSED -> OPENED."""
        if self.state != BasisState.CLOSED:
            self._log.warning("enter_called_while_opened")
            return
        self.state = BasisState.OPENED
        self._entry_time = datetime.now(timezone.utc)
        self.pnl = BasisPnL(entry_spread=entry_spread)
        self._log.info(FA_POSITION_OPENED, message="상태 전환: closed → opened")

    def exit_position(self, exit_spread: float) -> BasisPnL:
        """Transition from OPENED -> CLOSED and finalize P&L.

        Returns the completed P&L record.
        """
        if self.state != BasisState.OPENED:
            self._log.warning("exit_called_while_closed")
            return self.pnl

        # Basis convergence profit: we entered at a wider spread
        self.pnl.basis_pnl = self.pnl.entry_spread - exit_spread
        self.state = BasisState.CLOSED

        self._log.info(
            FA_POSITION_CLOSED,
            message="상태 전환: opened → closed",
            basis_pnl=round(self.pnl.basis_pnl * 100, 4),
            funding_pnl=round(self.pnl.funding_pnl, 6),
            total_pnl=round(self.pnl.total_pnl, 6),
            payments_collected=self.pnl.funding_payments_collected,
        )

        completed = self.pnl
        self._entry_time = None
        return completed

    def record_funding(self, payment: float) -> None:
        """Record a funding payment while position is open."""
        if self.state != BasisState.OPENED:
            return
        self.pnl.funding_pnl += payment
        self.pnl.funding_payments_collected += 1

    # ── introspection ───────────────────────────────────────────────────

    @property
    def is_open(self) -> bool:
        return self.state == BasisState.OPENED

    @property
    def hold_duration_hours(self) -> float:
        """How long the current position has been held, in hours."""
        if self._entry_time is None:
            return 0.0
        delta = datetime.now(timezone.utc) - self._entry_time
        return delta.total_seconds() / 3600.0


class BasisAction(str, enum.Enum):
    """Action returned by the state machine evaluation."""

    ENTER = "enter"
    EXIT_PROFIT = "exit_profit"
    EXIT_RISK = "exit_risk"
    HOLD = "hold"
