"""Backtest: Funding Rate Arbitrage Strategy.

Runs the funding arb strategy against historical OHLCV + funding rate data
using the in-process backtest engine.

Usage:
  python tests/backtest/bt_funding_arb.py
  python tests/backtest/bt_funding_arb.py --symbol ETHUSDT --months 3
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import logging

import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "backtester"))

from freqtrade_bridge import FreqtradeBridge, BacktestResult
from report_generator import ReportGenerator

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        kst_timestamper,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("bt_funding_arb")

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", str(PROJECT_ROOT / "backtest-results")))


async def _load_data_from_db(
    symbol: str, months: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load OHLCV + funding data from PostgreSQL."""
    import asyncpg

    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=3)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=months * 30)

    async with pool.acquire() as conn:
        ohlcv_rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = '1h'
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
        funding_rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate, predicted_rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )

    await pool.close()

    if ohlcv_rows:
        ohlcv = pd.DataFrame(ohlcv_rows, columns=["ts", "open", "high", "low", "close", "volume"])
        ohlcv["ts"] = pd.to_datetime(ohlcv["ts"], utc=True)
        ohlcv.set_index("ts", inplace=True)
        for col in ["open", "high", "low", "close", "volume"]:
            ohlcv[col] = ohlcv[col].astype(float)
    else:
        ohlcv = pd.DataFrame()

    if funding_rows:
        funding = pd.DataFrame(funding_rows, columns=["ts", "rate", "predicted_rate"])
        funding["ts"] = pd.to_datetime(funding["ts"], utc=True)
        funding.set_index("ts", inplace=True)
        funding["rate"] = funding["rate"].astype(float)
    else:
        funding = pd.DataFrame()

    return ohlcv, funding


def _generate_synthetic_data(
    months: int = 6,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate synthetic data when DB is unavailable."""
    import numpy as np

    np.random.seed(42)
    n_hours = months * 30 * 24
    dates = pd.date_range(
        end=datetime.now(timezone.utc),
        periods=n_hours,
        freq="1h",
    )

    base_price = 65000.0
    returns = np.random.normal(0.0001, 0.002, n_hours)
    close = base_price * np.cumprod(1 + returns)
    high = close * (1 + np.abs(np.random.normal(0, 0.001, n_hours)))
    low = close * (1 - np.abs(np.random.normal(0, 0.001, n_hours)))
    open_price = np.roll(close, 1)
    open_price[0] = base_price
    volume = np.random.uniform(500, 5000, n_hours)

    ohlcv = pd.DataFrame({
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=dates)
    ohlcv.index.name = "ts"

    # Funding rates every 8 hours
    n_funding = n_hours // 8
    funding_dates = pd.date_range(end=dates[-1], periods=n_funding, freq="8h")
    rates = np.random.normal(0.0001, 0.00005, n_funding)
    funding = pd.DataFrame({
        "rate": rates,
        "predicted_rate": rates + np.random.normal(0, 0.00002, n_funding),
    }, index=funding_dates)
    funding.index.name = "ts"

    return ohlcv, funding


async def run_backtest(
    symbol: str = "BTCUSDT",
    months: int = 6,
    capital: float = 10_000.0,
    use_db: bool = True,
) -> BacktestResult:
    """Execute the funding arb backtest."""
    log.info("bt_funding_arb_start", symbol=symbol, months=months, capital=capital)

    if use_db:
        try:
            ohlcv, funding = await _load_data_from_db(symbol, months)
        except Exception as exc:
            log.warning("db_load_failed_using_synthetic", error=str(exc))
            ohlcv, funding = _generate_synthetic_data(months)
    else:
        ohlcv, funding = _generate_synthetic_data(months)

    if ohlcv.empty:
        log.warning("no_data_using_synthetic")
        ohlcv, funding = _generate_synthetic_data(months)

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    bridge = FreqtradeBridge()
    result = bridge.run_backtest(
        strategy="funding_arb",
        ohlcv=ohlcv,
        funding=funding if not funding.empty else None,
        initial_capital=capital,
    )

    # Generate report
    report_gen = ReportGenerator(results_dir=RESULTS_DIR)
    report_path = report_gen.generate_backtest_report(result, "funding_arb")

    log.info(
        "bt_funding_arb_complete",
        trades=result.total_trades,
        profit_pct=round(result.total_profit_pct, 4),
        sharpe=round(result.sharpe_ratio, 4),
        max_dd_pct=round(result.max_drawdown_pct, 4),
        report=str(report_path),
    )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Funding Arb Strategy")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair")
    parser.add_argument("--months", type=int, default=6, help="Months of history")
    parser.add_argument("--capital", type=float, default=10_000.0, help="Initial capital")
    parser.add_argument("--synthetic", action="store_true", help="Use synthetic data only")
    args = parser.parse_args()

    result = asyncio.run(
        run_backtest(args.symbol, args.months, args.capital, use_db=not args.synthetic)
    )

    print("\n" + "=" * 50)
    print(f"  Funding Arb Backtest Results")
    print("=" * 50)
    print(f"  Profit:     {result.total_profit_pct:+.2f}%")
    print(f"  Sharpe:     {result.sharpe_ratio:.4f}")
    print(f"  Max DD:     {result.max_drawdown_pct:.2f}%")
    print(f"  Trades:     {result.total_trades}")
    print(f"  Win Rate:   {result.win_rate:.1f}%")
    print("=" * 50)


if __name__ == "__main__":
    main()
