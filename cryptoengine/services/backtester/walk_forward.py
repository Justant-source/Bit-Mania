"""Walk-Forward Analyser with Monte Carlo confidence intervals.

Sliding-window approach:
  1. Train window  (default 180 days) — optimise strategy parameters.
  2. Test window   (default 90 days)  — out-of-sample evaluation.
  3. Slide forward by ``test_days`` and repeat until data is exhausted.

Monte Carlo:
  Randomly shuffle the sequence of per-window returns and re-compute
  aggregate metrics to produce confidence intervals for Sharpe, profit,
  and drawdown.
"""

from __future__ import annotations

import math
import random
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import structlog

from freqtrade_bridge import BacktestResult, FreqtradeBridge

log = structlog.get_logger(__name__)

# Sharpe threshold — alert if aggregate falls below this value
SHARPE_ALERT_THRESHOLD = 1.5


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class WindowRange:
    """A single (train, test) date range."""

    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime

    @property
    def train_label(self) -> str:
        return f"{self.train_start:%Y-%m-%d}..{self.train_end:%Y-%m-%d}"

    @property
    def test_label(self) -> str:
        return f"{self.test_start:%Y-%m-%d}..{self.test_end:%Y-%m-%d}"


@dataclass
class WindowResult:
    """Backtest result for one walk-forward window."""

    window: WindowRange
    train_result: BacktestResult
    test_result: BacktestResult


@dataclass
class MonteCarloResult:
    """Confidence intervals produced by Monte Carlo shuffling."""

    n_simulations: int
    sharpe_mean: float
    sharpe_std: float
    sharpe_ci_95: tuple[float, float]
    profit_mean: float
    profit_std: float
    profit_ci_95: tuple[float, float]
    max_dd_mean: float
    max_dd_std: float
    max_dd_ci_95: tuple[float, float]
    win_probability: float  # fraction of simulations with positive profit


@dataclass
class WalkForwardResult:
    """Aggregate walk-forward analysis output."""

    strategy: str
    windows: list[WindowResult] = field(default_factory=list)
    aggregate_sharpe: float = 0.0
    aggregate_profit_pct: float = 0.0
    aggregate_max_drawdown_pct: float = 0.0
    aggregate_win_rate: float = 0.0
    aggregate_total_trades: int = 0
    consistency_ratio: float = 0.0  # fraction of positive OOS windows
    monte_carlo: MonteCarloResult | None = None
    sharpe_alert: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Walk-Forward Analyser
# ---------------------------------------------------------------------------

class WalkForwardAnalyzer:
    """Sliding-window walk-forward analysis with Monte Carlo."""

    def __init__(
        self,
        train_days: int = 180,
        test_days: int = 90,
        monte_carlo_runs: int = 100,
    ) -> None:
        self.train_days = train_days
        self.test_days = test_days
        self.monte_carlo_runs = monte_carlo_runs
        self._bridge = FreqtradeBridge()

    # ------------------------------------------------------------------
    # Sliding windows
    # ------------------------------------------------------------------

    @staticmethod
    def sliding_windows(
        start: datetime,
        end: datetime,
        train_days: int = 180,
        test_days: int = 90,
    ) -> Generator[WindowRange, None, None]:
        """Yield ``(train_range, test_range)`` windows from *start* to *end*.

        The generator advances by ``test_days`` on each iteration.
        """
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        cursor = start
        while True:
            train_start = cursor
            train_end = train_start + timedelta(days=train_days)
            test_start = train_end
            test_end = test_start + timedelta(days=test_days)

            if test_end > end:
                break

            yield WindowRange(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
            cursor += timedelta(days=test_days)

    # ------------------------------------------------------------------
    # Run walk-forward
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame | None = None,
        strategy: str = "funding_arb",
        initial_capital: float = 10_000.0,
    ) -> WalkForwardResult:
        """Execute walk-forward analysis over the provided data.

        Returns
        -------
        Aggregated ``WalkForwardResult`` including per-window breakdowns
        and optional Monte Carlo confidence intervals.
        """
        if ohlcv.empty:
            log.warning("walk_forward_empty_data")
            return WalkForwardResult(strategy=strategy)

        idx = ohlcv.index
        data_start = idx.min().to_pydatetime()
        data_end = idx.max().to_pydatetime()

        if data_start.tzinfo is None:
            data_start = data_start.replace(tzinfo=timezone.utc)
        if data_end.tzinfo is None:
            data_end = data_end.replace(tzinfo=timezone.utc)

        windows: list[WindowResult] = []
        window_gen = self.sliding_windows(
            data_start, data_end, self.train_days, self.test_days
        )

        for wnd in window_gen:
            log.info(
                "walk_forward_window",
                train=wnd.train_label,
                test=wnd.test_label,
            )

            train_data = ohlcv.loc[wnd.train_start : wnd.train_end]
            test_data = ohlcv.loc[wnd.test_start : wnd.test_end]

            train_funding = (
                funding.loc[wnd.train_start : wnd.train_end]
                if funding is not None and not funding.empty
                else None
            )
            test_funding = (
                funding.loc[wnd.test_start : wnd.test_end]
                if funding is not None and not funding.empty
                else None
            )

            if train_data.empty or test_data.empty:
                log.warning("walk_forward_insufficient_data", window=wnd.test_label)
                continue

            train_result = self._bridge.run_backtest(
                strategy=strategy,
                ohlcv=train_data,
                funding=train_funding,
                initial_capital=initial_capital,
            )
            test_result = self._bridge.run_backtest(
                strategy=strategy,
                ohlcv=test_data,
                funding=test_funding,
                initial_capital=initial_capital,
            )

            windows.append(
                WindowResult(
                    window=wnd,
                    train_result=train_result,
                    test_result=test_result,
                )
            )

        if not windows:
            log.warning("walk_forward_no_windows")
            return WalkForwardResult(strategy=strategy)

        result = self._aggregate(strategy, windows)

        # Monte Carlo
        if self.monte_carlo_runs > 0 and windows:
            result.monte_carlo = self.monte_carlo(windows, self.monte_carlo_runs)

        # Sharpe alert
        if result.aggregate_sharpe < SHARPE_ALERT_THRESHOLD:
            result.sharpe_alert = True
            log.warning(
                "walk_forward_sharpe_alert",
                sharpe=round(result.aggregate_sharpe, 4),
                threshold=SHARPE_ALERT_THRESHOLD,
            )

        log.info(
            "walk_forward_complete",
            windows=len(windows),
            sharpe=round(result.aggregate_sharpe, 4),
            profit_pct=round(result.aggregate_profit_pct, 4),
            consistency=round(result.consistency_ratio, 4),
            sharpe_alert=result.sharpe_alert,
        )

        return result

    # ------------------------------------------------------------------
    # Monte Carlo
    # ------------------------------------------------------------------

    @staticmethod
    def monte_carlo(
        windows: list[WindowResult],
        n_simulations: int = 100,
        seed: int | None = None,
    ) -> MonteCarloResult:
        """Shuffle per-window OOS returns and compute confidence intervals.

        Parameters
        ----------
        windows : walk-forward window results.
        n_simulations : number of random permutations.
        seed : optional random seed for reproducibility.

        Returns
        -------
        ``MonteCarloResult`` with 95% confidence intervals for Sharpe,
        cumulative profit, and maximum drawdown.
        """
        if seed is not None:
            random.seed(seed)

        # Collect per-window OOS daily returns
        all_returns: list[list[float]] = []
        for w in windows:
            rets = w.test_result.daily_returns
            if rets:
                all_returns.append(rets)

        if not all_returns:
            return MonteCarloResult(
                n_simulations=n_simulations,
                sharpe_mean=0.0,
                sharpe_std=0.0,
                sharpe_ci_95=(0.0, 0.0),
                profit_mean=0.0,
                profit_std=0.0,
                profit_ci_95=(0.0, 0.0),
                max_dd_mean=0.0,
                max_dd_std=0.0,
                max_dd_ci_95=(0.0, 0.0),
                win_probability=0.0,
            )

        # Flatten into one list
        flat_returns = [r for window_rets in all_returns for r in window_rets]

        sim_sharpes: list[float] = []
        sim_profits: list[float] = []
        sim_max_dds: list[float] = []

        for _ in range(n_simulations):
            shuffled = flat_returns.copy()
            random.shuffle(shuffled)

            # Compute equity curve
            equity = [1.0]
            for r in shuffled:
                equity.append(equity[-1] * (1 + r))

            cum_profit = (equity[-1] - 1.0) * 100
            sim_profits.append(cum_profit)

            # Drawdown
            peak = equity[0]
            max_dd = 0.0
            for val in equity:
                if val > peak:
                    peak = val
                dd = (peak - val) / peak if peak > 0 else 0.0
                if dd > max_dd:
                    max_dd = dd
            sim_max_dds.append(max_dd * 100)

            # Sharpe
            if len(shuffled) > 1:
                mean_r = sum(shuffled) / len(shuffled)
                var = sum((r - mean_r) ** 2 for r in shuffled) / (len(shuffled) - 1)
                std = math.sqrt(var) if var > 0 else 0.0
                sharpe = (mean_r / std * math.sqrt(365)) if std > 0 else 0.0
            else:
                sharpe = 0.0
            sim_sharpes.append(sharpe)

        def _stats(values: list[float]) -> tuple[float, float, tuple[float, float]]:
            n = len(values)
            mean = sum(values) / n
            var = sum((v - mean) ** 2 for v in values) / max(n - 1, 1)
            std = math.sqrt(var) if var > 0 else 0.0
            sorted_v = sorted(values)
            lo = sorted_v[max(int(n * 0.025), 0)]
            hi = sorted_v[min(int(n * 0.975), n - 1)]
            return mean, std, (lo, hi)

        s_mean, s_std, s_ci = _stats(sim_sharpes)
        p_mean, p_std, p_ci = _stats(sim_profits)
        d_mean, d_std, d_ci = _stats(sim_max_dds)
        win_prob = sum(1 for p in sim_profits if p > 0) / len(sim_profits)

        return MonteCarloResult(
            n_simulations=n_simulations,
            sharpe_mean=round(s_mean, 4),
            sharpe_std=round(s_std, 4),
            sharpe_ci_95=(round(s_ci[0], 4), round(s_ci[1], 4)),
            profit_mean=round(p_mean, 4),
            profit_std=round(p_std, 4),
            profit_ci_95=(round(p_ci[0], 4), round(p_ci[1], 4)),
            max_dd_mean=round(d_mean, 4),
            max_dd_std=round(d_std, 4),
            max_dd_ci_95=(round(d_ci[0], 4), round(d_ci[1], 4)),
            win_probability=round(win_prob, 4),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate(strategy: str, windows: list[WindowResult]) -> WalkForwardResult:
        """Aggregate OOS (test) results across all windows."""
        oos_profits = [w.test_result.total_profit_pct for w in windows]
        oos_sharpes = [w.test_result.sharpe_ratio for w in windows]
        oos_dds = [w.test_result.max_drawdown_pct for w in windows]
        oos_trades = [w.test_result.total_trades for w in windows]
        oos_win_rates = [w.test_result.win_rate for w in windows]

        n = len(windows)
        positive_windows = sum(1 for p in oos_profits if p > 0)

        return WalkForwardResult(
            strategy=strategy,
            windows=windows,
            aggregate_sharpe=sum(oos_sharpes) / n if n > 0 else 0.0,
            aggregate_profit_pct=sum(oos_profits) / n if n > 0 else 0.0,
            aggregate_max_drawdown_pct=max(oos_dds) if oos_dds else 0.0,
            aggregate_win_rate=sum(oos_win_rates) / n if n > 0 else 0.0,
            aggregate_total_trades=sum(oos_trades),
            consistency_ratio=positive_windows / n if n > 0 else 0.0,
        )
