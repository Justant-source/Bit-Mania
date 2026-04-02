"""Backtest: Grid Trading Strategy.

Runs the grid trading strategy against historical OHLCV data.

Usage:
  python tests/backtest/bt_grid.py
  python tests/backtest/bt_grid.py --symbol ETHUSDT --months 3 --synthetic
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import structlog

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "backtester"))

from freqtrade_bridge import FreqtradeBridge, BacktestResult
from report_generator import ReportGenerator

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(structlog.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("bt_grid")

RESULTS_DIR = Path(os.getenv("RESULTS_DIR", str(PROJECT_ROOT / "backtest-results")))

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)


def _generate_ranging_data(months: int = 6) -> pd.DataFrame:
    """Generate synthetic ranging market data for grid testing."""
    np.random.seed(123)
    n = months * 30 * 24

    dates = pd.date_range(end=datetime.now(timezone.utc), periods=n, freq="1h")

    # Mean-reverting price (Ornstein-Uhlenbeck)
    base = 65000.0
    theta = 0.05  # mean reversion speed
    mu = base
    sigma = 200.0
    price = np.zeros(n)
    price[0] = base

    for i in range(1, n):
        price[i] = price[i - 1] + theta * (mu - price[i - 1]) + sigma * np.random.randn()

    high = price + np.abs(np.random.normal(0, 50, n))
    low = price - np.abs(np.random.normal(0, 50, n))
    open_price = np.roll(price, 1)
    open_price[0] = base
    volume = np.random.uniform(100, 3000, n)

    df = pd.DataFrame({
        "open": open_price,
        "high": high,
        "low": low,
        "close": price,
        "volume": volume,
    }, index=dates)
    df.index.name = "ts"
    return df


async def _load_db_data(symbol: str, months: int) -> pd.DataFrame:
    """Load OHLCV from database."""
    import asyncpg
    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=3)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=months * 30)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = '1h'
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    await pool.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


async def run_backtest(
    symbol: str = "BTCUSDT",
    months: int = 6,
    capital: float = 10_000.0,
    use_db: bool = True,
) -> BacktestResult:
    log.info("bt_grid_start", symbol=symbol, months=months, capital=capital)

    ohlcv = pd.DataFrame()
    if use_db:
        try:
            ohlcv = await _load_db_data(symbol, months)
        except Exception as exc:
            log.warning("db_load_failed", error=str(exc))

    if ohlcv.empty:
        log.info("using_synthetic_ranging_data")
        ohlcv = _generate_ranging_data(months)

    log.info("data_loaded", bars=len(ohlcv))

    bridge = FreqtradeBridge()
    result = bridge.run_backtest(
        strategy="grid_trading",
        ohlcv=ohlcv,
        initial_capital=capital,
    )

    report_gen = ReportGenerator(results_dir=RESULTS_DIR)
    report_path = report_gen.generate_backtest_report(result, "grid_trading")

    log.info(
        "bt_grid_complete",
        trades=result.total_trades,
        profit_pct=round(result.total_profit_pct, 4),
        sharpe=round(result.sharpe_ratio, 4),
        max_dd_pct=round(result.max_drawdown_pct, 4),
        report=str(report_path),
    )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Grid Trading Strategy")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--synthetic", action="store_true")
    args = parser.parse_args()

    result = asyncio.run(
        run_backtest(args.symbol, args.months, args.capital, not args.synthetic)
    )

    print(f"\nGrid Trading: {result.total_profit_pct:+.2f}%, "
          f"Sharpe {result.sharpe_ratio:.4f}, "
          f"{result.total_trades} trades, "
          f"DD {result.max_drawdown_pct:.2f}%")


if __name__ == "__main__":
    main()
