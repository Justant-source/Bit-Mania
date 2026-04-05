#!/usr/bin/env python3
"""Generate monthly performance report.

Steps:
  1. Fetch daily_reports and portfolio_snapshots from DB for the target month.
  2. Calculate aggregate stats (return, drawdown, Sharpe, per-strategy P&L).
  3. Call LLM (Claude via CLI) for a natural-language summary.
  4. Save the report to the daily_reports table.

Usage:
  python scripts/generate_monthly_report.py                   # previous month
  python scripts/generate_monthly_report.py --month 2026-03   # specific month
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import logging

import asyncpg
import structlog

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
log = structlog.get_logger("monthly_report")

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CLAUDE_PATH = os.getenv("CLAUDE_CODE_PATH", "claude")


# ------------------------------------------------------------------
# Data fetching
# ------------------------------------------------------------------

async def _fetch_daily_reports(
    pool: asyncpg.Pool,
    start_date: date,
    end_date: date,
) -> list[asyncpg.Record]:
    """Fetch daily report rows for the month."""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT date, starting_equity, ending_equity, daily_pnl,
                   daily_return, trade_count, funding_income,
                   grid_income, dca_value, max_drawdown
            FROM daily_reports
            WHERE date >= $1 AND date <= $2
            ORDER BY date ASC
            """,
            start_date,
            end_date,
        )


async def _fetch_snapshots(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> list[asyncpg.Record]:
    """Fetch portfolio snapshots for the month."""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT total_equity, unrealized_pnl, realized_pnl,
                   drawdown, sharpe_30d, strategy_weights,
                   market_regime, snapshot_at
            FROM portfolio_snapshots
            WHERE snapshot_at >= $1 AND snapshot_at <= $2
            ORDER BY snapshot_at ASC
            """,
            start,
            end,
        )


async def _fetch_trade_count(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT count(*) FROM trades WHERE created_at >= $1 AND created_at <= $2",
            start,
            end,
        )
        return row or 0


async def _fetch_funding_income(
    pool: asyncpg.Pool,
    start: datetime,
    end: datetime,
) -> float:
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            "SELECT COALESCE(SUM(payment), 0) FROM funding_payments WHERE collected_at >= $1 AND collected_at <= $2",
            start,
            end,
        )
        return float(row or 0)


# ------------------------------------------------------------------
# Statistics calculation
# ------------------------------------------------------------------

def _calculate_monthly_stats(
    daily_rows: list[asyncpg.Record],
    snapshots: list[asyncpg.Record],
    trade_count: int,
    funding_income: float,
) -> dict[str, Any]:
    """Calculate aggregate monthly statistics."""
    stats: dict[str, Any] = {
        "trading_days": len(daily_rows),
        "total_trades": trade_count,
        "funding_income": funding_income,
    }

    if daily_rows:
        daily_returns = [
            float(r["daily_return"]) for r in daily_rows
            if r["daily_return"] is not None
        ]
        daily_pnls = [
            float(r["daily_pnl"]) for r in daily_rows
            if r["daily_pnl"] is not None
        ]
        drawdowns = [
            float(r["max_drawdown"]) for r in daily_rows
            if r["max_drawdown"] is not None
        ]

        stats["starting_equity"] = float(daily_rows[0]["starting_equity"] or 0)
        stats["ending_equity"] = float(daily_rows[-1]["ending_equity"] or 0)
        stats["total_pnl"] = sum(daily_pnls)
        stats["monthly_return_pct"] = (
            (stats["ending_equity"] - stats["starting_equity"])
            / stats["starting_equity"]
            * 100
            if stats["starting_equity"] > 0
            else 0.0
        )
        stats["max_drawdown"] = min(drawdowns) if drawdowns else 0.0
        stats["avg_daily_return"] = (
            sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
        )
        stats["win_days"] = sum(1 for r in daily_returns if r > 0)
        stats["loss_days"] = sum(1 for r in daily_returns if r < 0)

        # Sharpe
        if len(daily_returns) > 1:
            mean_r = sum(daily_returns) / len(daily_returns)
            var = sum((r - mean_r) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
            std = math.sqrt(var) if var > 0 else 0.0
            stats["sharpe_ratio"] = (mean_r / std * math.sqrt(365)) if std > 0 else 0.0
        else:
            stats["sharpe_ratio"] = 0.0

        # Per-strategy income
        grid_incomes = [float(r["grid_income"] or 0) for r in daily_rows]
        dca_values = [float(r["dca_value"] or 0) for r in daily_rows]
        stats["grid_income"] = sum(grid_incomes)
        stats["dca_value"] = sum(dca_values)
    else:
        stats.update({
            "starting_equity": 0,
            "ending_equity": 0,
            "total_pnl": 0,
            "monthly_return_pct": 0,
            "max_drawdown": 0,
            "avg_daily_return": 0,
            "sharpe_ratio": 0,
            "win_days": 0,
            "loss_days": 0,
            "grid_income": 0,
            "dca_value": 0,
        })

    # Regime distribution from snapshots
    if snapshots:
        regime_counts: dict[str, int] = {}
        for s in snapshots:
            regime = s["market_regime"] or "unknown"
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        stats["regime_distribution"] = regime_counts

    return stats


# ------------------------------------------------------------------
# LLM summary
# ------------------------------------------------------------------

def _generate_llm_summary(
    stats: dict[str, Any],
    month_str: str,
) -> str:
    """Call Claude CLI to generate a natural-language monthly summary."""
    prompt = f"""다음은 CryptoEngine 자동매매 시스템의 {month_str} 월간 성과 데이터입니다.
한국어로 간결한 월간 리포트 요약을 작성해 주세요. 3~5문장으로 핵심 성과, 주요 이벤트, 개선점을 포함하세요.

데이터:
- 시작 자본: ${stats.get('starting_equity', 0):,.2f}
- 종료 자본: ${stats.get('ending_equity', 0):,.2f}
- 총 손익: ${stats.get('total_pnl', 0):,.2f}
- 월간 수익률: {stats.get('monthly_return_pct', 0):.2f}%
- 최대 낙폭: {stats.get('max_drawdown', 0):.4f}
- Sharpe 비율: {stats.get('sharpe_ratio', 0):.4f}
- 거래 횟수: {stats.get('total_trades', 0)}
- 펀딩비 수입: ${stats.get('funding_income', 0):,.2f}
- 승일/패일: {stats.get('win_days', 0)}/{stats.get('loss_days', 0)}
- 거래일수: {stats.get('trading_days', 0)}
"""

    try:
        result = subprocess.run(
            [CLAUDE_PATH, "-p", prompt],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        log.warning(
            "llm_summary_failed",
            returncode=result.returncode,
            stderr=result.stderr[:500],
        )
    except FileNotFoundError:
        log.warning("claude_cli_not_found", path=CLAUDE_PATH)
    except subprocess.TimeoutExpired:
        log.warning("llm_summary_timeout")
    except Exception as exc:
        log.warning("llm_summary_error", error=str(exc))

    # Fallback: generate a simple summary
    return (
        f"{month_str} 월간 요약: "
        f"수익률 {stats.get('monthly_return_pct', 0):.2f}%, "
        f"총 손익 ${stats.get('total_pnl', 0):,.2f}, "
        f"Sharpe {stats.get('sharpe_ratio', 0):.4f}, "
        f"거래 {stats.get('total_trades', 0)}건, "
        f"최대 낙폭 {stats.get('max_drawdown', 0):.4f}."
    )


# ------------------------------------------------------------------
# Save to DB
# ------------------------------------------------------------------

async def _save_report(
    pool: asyncpg.Pool,
    report_date: date,
    stats: dict[str, Any],
    summary: str,
) -> None:
    """Upsert the monthly report into the daily_reports table."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO daily_reports
                (date, starting_equity, ending_equity, daily_pnl,
                 daily_return, trade_count, funding_income,
                 grid_income, dca_value, max_drawdown, llm_summary)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (date) DO UPDATE SET
                starting_equity = EXCLUDED.starting_equity,
                ending_equity = EXCLUDED.ending_equity,
                daily_pnl = EXCLUDED.daily_pnl,
                daily_return = EXCLUDED.daily_return,
                trade_count = EXCLUDED.trade_count,
                funding_income = EXCLUDED.funding_income,
                grid_income = EXCLUDED.grid_income,
                dca_value = EXCLUDED.dca_value,
                max_drawdown = EXCLUDED.max_drawdown,
                llm_summary = EXCLUDED.llm_summary
            """,
            report_date,
            stats.get("starting_equity", 0),
            stats.get("ending_equity", 0),
            stats.get("total_pnl", 0),
            stats.get("monthly_return_pct", 0),
            stats.get("total_trades", 0),
            stats.get("funding_income", 0),
            stats.get("grid_income", 0),
            stats.get("dca_value", 0),
            stats.get("max_drawdown", 0),
            summary,
        )


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def generate_report(month_str: str | None = None) -> dict[str, Any]:
    """Generate and save the monthly report."""
    if month_str:
        year, month = map(int, month_str.split("-"))
    else:
        today = date.today()
        first_of_month = today.replace(day=1)
        last_month = first_of_month - timedelta(days=1)
        year, month = last_month.year, last_month.month
        month_str = f"{year}-{month:02d}"

    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)

    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

    log.info("generating_monthly_report", month=month_str, start=str(start_date), end=str(end_date))

    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=5)

    try:
        daily_rows = await _fetch_daily_reports(pool, start_date, end_date)
        snapshots = await _fetch_snapshots(pool, start_dt, end_dt)
        trade_count = await _fetch_trade_count(pool, start_dt, end_dt)
        funding_income = await _fetch_funding_income(pool, start_dt, end_dt)

        stats = _calculate_monthly_stats(daily_rows, snapshots, trade_count, funding_income)

        log.info(
            "monthly_stats_calculated",
            month=month_str,
            pnl=stats.get("total_pnl"),
            return_pct=stats.get("monthly_return_pct"),
            sharpe=stats.get("sharpe_ratio"),
        )

        # Generate LLM summary
        summary = _generate_llm_summary(stats, month_str)
        log.info("llm_summary_generated", length=len(summary))

        # Save to DB
        report_date = end_date  # Use last day of month as the report date
        await _save_report(pool, report_date, stats, summary)
        log.info("monthly_report_saved", date=str(report_date))

        stats["summary"] = summary
        stats["month"] = month_str

        return stats

    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate monthly performance report")
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Target month in YYYY-MM format (default: previous month)",
    )
    args = parser.parse_args()

    result = asyncio.run(generate_report(args.month))

    # Print summary
    print("\n" + "=" * 60)
    print(f"  Monthly Report: {result.get('month', '?')}")
    print("=" * 60)
    print(f"  Return:      {result.get('monthly_return_pct', 0):.2f}%")
    print(f"  Total P&L:   ${result.get('total_pnl', 0):,.2f}")
    print(f"  Sharpe:      {result.get('sharpe_ratio', 0):.4f}")
    print(f"  Max DD:      {result.get('max_drawdown', 0):.4f}")
    print(f"  Trades:      {result.get('total_trades', 0)}")
    print("=" * 60)
    print(f"\n{result.get('summary', '')}\n")


if __name__ == "__main__":
    main()
