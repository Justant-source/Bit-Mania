"""CryptoEngine Backtester — entry point.

Loads historical data from PostgreSQL, runs strategy backtests,
walk-forward analysis, and generates reports.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pandas as pd
import structlog

from freqtrade_bridge import FreqtradeBridge
from report_generator import ReportGenerator
from walk_forward import WalkForwardAnalyzer

log = structlog.get_logger(__name__)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "/app/results"))


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if LOG_LEVEL == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog, LOG_LEVEL, structlog.INFO)  # type: ignore[arg-type]
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def _load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Load OHLCV data from the database into a DataFrame."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = $1 AND timeframe = $2
              AND ts >= $3 AND ts <= $4
            ORDER BY ts ASC
            """,
            symbol,
            timeframe,
            start,
            end,
        )

    if not rows:
        log.warning("no_ohlcv_data", symbol=symbol, timeframe=timeframe)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


async def _load_funding_rates(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Load funding rate history from the database."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT collected_at, rate, predicted_rate
            FROM funding_rates
            WHERE symbol = $1
              AND collected_at >= $2 AND collected_at <= $3
            ORDER BY collected_at ASC
            """,
            symbol,
            start,
            end,
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "rate", "predicted_rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


async def run_backtest(args: argparse.Namespace) -> None:
    """Main backtest execution flow."""
    _configure_logging()
    log.info(
        "backtester_starting",
        strategy=args.strategy,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
    )

    pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn=DB_DSN, min_size=2, max_size=5, command_timeout=60
    )

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Load data
    log.info("loading_market_data")
    ohlcv = await _load_ohlcv(pool, args.symbol, args.timeframe, start_dt, end_dt)
    funding = await _load_funding_rates(pool, args.symbol, start_dt, end_dt)

    if ohlcv.empty:
        log.error("no_data_available", symbol=args.symbol)
        await pool.close()
        return

    log.info("data_loaded", ohlcv_rows=len(ohlcv), funding_rows=len(funding))

    report_gen = ReportGenerator(results_dir=RESULTS_DIR)

    if args.walk_forward:
        # Walk-forward analysis
        log.info("starting_walk_forward_analysis")
        wf = WalkForwardAnalyzer(
            train_days=args.wf_train_days,
            test_days=args.wf_test_days,
            monte_carlo_runs=args.monte_carlo_runs,
        )
        wf_results = wf.run(
            ohlcv=ohlcv,
            funding=funding,
            strategy=args.strategy,
            initial_capital=args.capital,
        )
        report_gen.generate_walk_forward_report(wf_results, args.strategy)
    else:
        # Simple backtest
        log.info("running_simple_backtest")
        bridge = FreqtradeBridge()
        bt_results = bridge.run_backtest(
            strategy=args.strategy,
            ohlcv=ohlcv,
            funding=funding,
            initial_capital=args.capital,
        )
        report_gen.generate_backtest_report(bt_results, args.strategy)

    await pool.close()
    log.info("backtester_finished", results_dir=str(RESULTS_DIR))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CryptoEngine Backtester")
    parser.add_argument(
        "--strategy",
        type=str,
        default="funding_arb",
        choices=["funding_arb", "grid_trading", "adaptive_dca", "combined"],
        help="Strategy to backtest",
    )
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair")
    parser.add_argument("--timeframe", type=str, default="1h", help="OHLCV timeframe")
    parser.add_argument(
        "--start",
        type=str,
        default=(datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--capital", type=float, default=10000.0, help="Initial capital (USD)"
    )
    parser.add_argument(
        "--walk-forward", action="store_true", help="Run walk-forward analysis"
    )
    parser.add_argument(
        "--wf-train-days", type=int, default=180, help="Walk-forward training window (days)"
    )
    parser.add_argument(
        "--wf-test-days", type=int, default=90, help="Walk-forward test window (days)"
    )
    parser.add_argument(
        "--monte-carlo-runs",
        type=int,
        default=100,
        help="Monte Carlo simulation runs",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(run_backtest(args))
    except KeyboardInterrupt:
        pass
