"""Backtest: Combined strategies with orchestrator-style allocation.

Runs all three strategies (funding arb, grid, adaptive DCA) simultaneously
with regime-based weight allocation, simulating the orchestrator's behaviour.

Usage:
  python tests/backtest/bt_combined.py
  python tests/backtest/bt_combined.py --months 6 --capital 50000 --synthetic
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import logging

import numpy as np
import pandas as pd
import structlog

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "backtester"))

from freqtrade_bridge import BacktestResult, FreqtradeBridge
from report_generator import ReportGenerator
from walk_forward import WalkForwardAnalyzer

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("bt_combined")

RESULTS_DIR = Path(os.getenv("RESULTS_DIR", str(PROJECT_ROOT / "backtest-results")))

# Weight matrix from orchestrator config
WEIGHT_MATRIX = {
    "ranging": {"funding_arb": 0.25, "grid_trading": 0.40, "adaptive_dca": 0.15, "cash": 0.20},
    "trending_up": {"funding_arb": 0.15, "grid_trading": 0.05, "adaptive_dca": 0.50, "cash": 0.30},
    "trending_down": {"funding_arb": 0.20, "grid_trading": 0.05, "adaptive_dca": 0.10, "cash": 0.65},
    "volatile": {"funding_arb": 0.10, "grid_trading": 0.05, "adaptive_dca": 0.05, "cash": 0.80},
}


def _detect_regime_simple(lookback: pd.DataFrame) -> str:
    """Simplified regime detection for backtesting."""
    if len(lookback) < 20:
        return "ranging"

    close = lookback["close"].values
    returns = np.diff(close) / close[:-1]

    # Volatility
    vol = np.std(returns) * np.sqrt(24)  # annualize hourly vol
    avg_vol = np.mean(np.abs(returns))

    # Trend: SMA crossover
    sma20 = np.mean(close[-20:])
    sma50 = np.mean(close[-min(50, len(close)):])
    current = close[-1]

    if vol > 0.03:  # high volatility
        return "volatile"
    elif current > sma20 > sma50 and np.mean(returns[-20:]) > 0:
        return "trending_up"
    elif current < sma20 < sma50 and np.mean(returns[-20:]) < 0:
        return "trending_down"
    else:
        return "ranging"


def _generate_combined_data(months: int = 6) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate synthetic data with regime changes."""
    np.random.seed(42)
    n = months * 30 * 24
    dates = pd.date_range(end=datetime.now(timezone.utc), periods=n, freq="1h")

    base = 65000.0
    prices = np.zeros(n)
    prices[0] = base

    # Mix of trending and ranging periods
    segment = n // 4
    for i in range(1, n):
        if i < segment:
            # Trending up
            drift = 0.0003
        elif i < 2 * segment:
            # Ranging
            drift = 0.0
        elif i < 3 * segment:
            # Volatile
            drift = -0.0001
        else:
            # Trending down then recovery
            drift = 0.0002 if i > 3.5 * segment else -0.0002

        noise = np.random.normal(0, 0.002)
        prices[i] = prices[i - 1] * (1 + drift + noise)

    high = prices * (1 + np.abs(np.random.normal(0, 0.001, n)))
    low = prices * (1 - np.abs(np.random.normal(0, 0.001, n)))
    open_p = np.roll(prices, 1)
    open_p[0] = base
    volume = np.random.uniform(500, 5000, n)

    ohlcv = pd.DataFrame({
        "open": open_p, "high": high, "low": low, "close": prices, "volume": volume,
    }, index=dates)
    ohlcv.index.name = "ts"

    # Funding rates
    n_f = n // 8
    f_dates = pd.date_range(end=dates[-1], periods=n_f, freq="8h")
    rates = np.random.normal(0.0001, 0.00005, n_f)
    funding = pd.DataFrame({"rate": rates, "predicted_rate": rates * 0.9}, index=f_dates)
    funding.index.name = "ts"

    return ohlcv, funding


async def run_combined_backtest(
    months: int = 6,
    capital: float = 50_000.0,
    walk_forward: bool = False,
) -> dict[str, BacktestResult]:
    """Run all strategies with regime-based allocation."""
    log.info("bt_combined_start", months=months, capital=capital)

    ohlcv, funding = _generate_combined_data(months)
    log.info("data_loaded", bars=len(ohlcv), funding_rows=len(funding))

    strategies = ["funding_arb", "grid_trading", "adaptive_dca"]
    bridge = FreqtradeBridge()
    results: dict[str, BacktestResult] = {}

    # Detect regime to determine capital allocation
    lookback_size = min(200, len(ohlcv))
    regime = _detect_regime_simple(ohlcv.tail(lookback_size))
    weights = WEIGHT_MATRIX.get(regime, WEIGHT_MATRIX["ranging"])
    log.info("detected_regime", regime=regime, weights=weights)

    for strategy in strategies:
        weight_key = strategy
        allocated = capital * weights.get(weight_key, 0.0)

        if allocated <= 0:
            log.info("strategy_skipped_zero_allocation", strategy=strategy)
            results[strategy] = bridge._empty_result(strategy, 0)
            continue

        log.info("running_strategy", strategy=strategy, allocated=allocated)

        result = bridge.run_backtest(
            strategy=strategy,
            ohlcv=ohlcv,
            funding=funding if strategy == "funding_arb" else None,
            initial_capital=allocated,
        )
        results[strategy] = result

    # Aggregate results
    total_profit = sum(r.total_profit for r in results.values())
    total_capital = capital * (1 - weights.get("cash", 0))

    # Combined backtest result
    combined = BacktestResult(
        strategy="combined",
        start_date=str(ohlcv.index[0]),
        end_date=str(ohlcv.index[-1]),
        initial_capital=capital,
        final_capital=capital + total_profit,
        total_profit=total_profit,
        total_profit_pct=(total_profit / capital * 100) if capital > 0 else 0.0,
        max_drawdown=max(r.max_drawdown for r in results.values()),
        max_drawdown_pct=max(r.max_drawdown_pct for r in results.values()),
        sharpe_ratio=0.0,
        sortino_ratio=0.0,
        win_rate=0.0,
        total_trades=sum(r.total_trades for r in results.values()),
        avg_trade_duration_hours=0.0,
        profit_factor=0.0,
    )
    results["combined"] = combined

    # Generate reports
    report_gen = ReportGenerator(results_dir=RESULTS_DIR)
    for name, result in results.items():
        report_gen.generate_backtest_report(result, f"combined_{name}")

    # Optional walk-forward
    if walk_forward:
        log.info("running_walk_forward_analysis")
        wfa = WalkForwardAnalyzer(train_days=90, test_days=30, monte_carlo_runs=50)
        wf_result = wfa.run(
            ohlcv=ohlcv,
            funding=funding,
            strategy="combined",
            initial_capital=capital,
        )
        report_gen.generate_walk_forward_report(wf_result, "combined")
        log.info(
            "walk_forward_complete",
            sharpe=wf_result.aggregate_sharpe,
            consistency=wf_result.consistency_ratio,
        )

    log.info(
        "bt_combined_complete",
        regime=regime,
        total_profit=round(total_profit, 2),
        total_profit_pct=round(combined.total_profit_pct, 4),
        total_trades=combined.total_trades,
    )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Combined Strategies")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--capital", type=float, default=50_000.0)
    parser.add_argument("--walk-forward", action="store_true")
    parser.add_argument("--synthetic", action="store_true", default=True)
    args = parser.parse_args()

    results = asyncio.run(
        run_combined_backtest(args.months, args.capital, args.walk_forward)
    )

    print("\n" + "=" * 60)
    print("  Combined Strategy Backtest Results")
    print("=" * 60)
    for name, r in results.items():
        print(f"  {name:20s} | {r.total_profit_pct:+7.2f}% | "
              f"Trades: {r.total_trades:4d} | DD: {r.max_drawdown_pct:6.2f}%")
    print("=" * 60)


if __name__ == "__main__":
    main()
