"""
Comprehensive Backtest Scenario Runner
ohlcv_history / funding_rate_history 를 직접 읽어 다양한 시나리오를 실행하고
backtest_results 테이블에 저장합니다.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
from freqtrade_bridge import FreqtradeBridge

log = structlog.get_logger(__name__)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# ── 실행할 시나리오 정의 ──────────────────────────────────────────────────
SCENARIOS = [
    # (scenario_label, strategy, symbol, start, end, timeframe, capital)
    # 6개월 전체
    ("6m_full",    "funding_arb",  "BTCUSDT", "2025-10-01", "2026-03-31", "1h",  10000),
    ("6m_full",    "adaptive_dca", "BTCUSDT", "2025-10-01", "2026-03-31", "1h",  10000),
    ("6m_full",    "combined",     "BTCUSDT", "2025-10-01", "2026-03-31", "1h",  10000),
    # Q4 2025 (3개월)
    ("Q4_2025",    "funding_arb",  "BTCUSDT", "2025-10-01", "2025-12-31", "1h",  10000),
    ("Q4_2025",    "adaptive_dca", "BTCUSDT", "2025-10-01", "2025-12-31", "1h",  10000),
    ("Q4_2025",    "combined",     "BTCUSDT", "2025-10-01", "2025-12-31", "1h",  10000),
    # Q1 2026 (3개월)
    ("Q1_2026",    "funding_arb",  "BTCUSDT", "2026-01-01", "2026-03-31", "1h",  10000),
    ("Q1_2026",    "adaptive_dca", "BTCUSDT", "2026-01-01", "2026-03-31", "1h",  10000),
    ("Q1_2026",    "combined",     "BTCUSDT", "2026-01-01", "2026-03-31", "1h",  10000),
    # 소자본 시나리오 (1000 USD)
    ("small_cap",  "funding_arb",  "BTCUSDT", "2025-10-01", "2026-03-31", "1h",  1000),
    ("small_cap",  "combined",     "BTCUSDT", "2025-10-01", "2026-03-31", "1h",  1000),
    # 대자본 시나리오 (100000 USD)
    ("large_cap",  "funding_arb",  "BTCUSDT", "2025-10-01", "2026-03-31", "1h", 100000),
    ("large_cap",  "combined",     "BTCUSDT", "2025-10-01", "2026-03-31", "1h", 100000),
    # 4시간봉 시나리오
    ("4h_tf",      "funding_arb",  "BTCUSDT", "2025-10-01", "2026-03-31", "4h",  10000),
    ("4h_tf",      "combined",     "BTCUSDT", "2025-10-01", "2026-03-31", "4h",  10000),
    # 15분봉 시나리오
    ("15m_tf",     "funding_arb",  "BTCUSDT", "2025-10-01", "2026-03-31", "15m", 10000),
    # ── 3년 전체 (2023-04-01 ~ 2026-03-31) ────────────────────────────────
    ("3y_full",    "funding_arb",  "BTCUSDT", "2023-04-01", "2026-03-31", "1h",  10000),
    ("3y_full",    "adaptive_dca", "BTCUSDT", "2023-04-01", "2026-03-31", "1h",  10000),
    ("3y_full",    "combined",     "BTCUSDT", "2023-04-01", "2026-03-31", "1h",  10000),
    # ── 연도별 분석 ────────────────────────────────────────────────────────
    # 2023년 (BTC 상승 사이클 초입)
    ("Y2023",      "funding_arb",  "BTCUSDT", "2023-04-01", "2023-12-31", "1h",  10000),
    ("Y2023",      "adaptive_dca", "BTCUSDT", "2023-04-01", "2023-12-31", "1h",  10000),
    ("Y2023",      "combined",     "BTCUSDT", "2023-04-01", "2023-12-31", "1h",  10000),
    # 2024년 (반감기 + 사상최고가)
    ("Y2024",      "funding_arb",  "BTCUSDT", "2024-01-01", "2024-12-31", "1h",  10000),
    ("Y2024",      "adaptive_dca", "BTCUSDT", "2024-01-01", "2024-12-31", "1h",  10000),
    ("Y2024",      "combined",     "BTCUSDT", "2024-01-01", "2024-12-31", "1h",  10000),
    # 2025년 (상승장 후반~조정)
    ("Y2025",      "funding_arb",  "BTCUSDT", "2025-01-01", "2025-12-31", "1h",  10000),
    ("Y2025",      "adaptive_dca", "BTCUSDT", "2025-01-01", "2025-12-31", "1h",  10000),
    ("Y2025",      "combined",     "BTCUSDT", "2025-01-01", "2025-12-31", "1h",  10000),
    # ── 3년 4시간봉 ────────────────────────────────────────────────────────
    ("3y_4h",      "funding_arb",  "BTCUSDT", "2023-04-01", "2026-03-31", "4h",  10000),
    ("3y_4h",      "combined",     "BTCUSDT", "2023-04-01", "2026-03-31", "4h",  10000),
    # ── 3년 소자본 / 대자본 ────────────────────────────────────────────────
    ("3y_small",   "funding_arb",  "BTCUSDT", "2023-04-01", "2026-03-31", "1h",  1000),
    ("3y_small",   "combined",     "BTCUSDT", "2023-04-01", "2026-03-31", "1h",  1000),
    ("3y_large",   "funding_arb",  "BTCUSDT", "2023-04-01", "2026-03-31", "1h", 100000),
    ("3y_large",   "combined",     "BTCUSDT", "2023-04-01", "2026-03-31", "1h", 100000),
]


async def load_ohlcv(pool: asyncpg.Pool, symbol: str, timeframe: str,
                     start: datetime, end: datetime) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM ohlcv_history
            WHERE symbol = $1 AND timeframe = $2
              AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            """,
            symbol, timeframe, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


async def load_funding(pool: asyncpg.Pool, symbol: str,
                       start: datetime, end: datetime) -> pd.DataFrame:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate, NULL::double precision AS predicted_rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate", "predicted_rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


def _safe_float(v: float, default: float = 0.0) -> float:
    """Infinity / NaN → default (JSON 직렬화 안전)."""
    import math
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return v


async def save_result(pool: asyncpg.Pool, scenario: str, result, symbol: str,
                      start: str, end: str, timeframe: str) -> None:
    # equity_curve를 월별 수익률로 변환
    monthly_returns: dict[str, float] = {}
    if result.daily_returns:
        dr = result.daily_returns
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        for i, ret in enumerate(dr):
            day = pd.Timestamp(start_dt) + pd.Timedelta(days=i)
            key = day.strftime("%Y-%m")
            monthly_returns[key] = monthly_returns.get(key, 0.0) + _safe_float(ret)

    # equity curve 샘플 (최대 200포인트)
    eq_curve = result.equity_curve
    if len(eq_curve) > 200:
        step = len(eq_curve) // 200
        eq_curve = eq_curve[::step]

    metadata = {
        **result.metadata,
        "scenario": scenario,
        "timeframe": timeframe,
        "sortino_ratio": _safe_float(result.sortino_ratio),
        "profit_factor": _safe_float(result.profit_factor),
        "avg_trade_duration_hours": _safe_float(result.avg_trade_duration_hours),
        "monthly_returns": monthly_returns,
        "equity_curve_sample": [round(_safe_float(v), 2) for v in eq_curve],
        "drawdown_curve_sample": [round(_safe_float(v), 4) for v in result.drawdown_curve[::max(1, len(result.drawdown_curve)//200)]],
    }

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backtest_results
                (strategy, symbol, start_date, end_date, initial_capital,
                 final_equity, total_return, sharpe_ratio, max_drawdown,
                 win_rate, total_trades, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
            """,
            result.strategy,
            symbol,
            datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            result.initial_capital,
            result.final_capital,
            result.total_profit_pct,
            result.sharpe_ratio,
            result.max_drawdown_pct,
            result.win_rate,
            result.total_trades,
            json.dumps(metadata),
        )
    log.info("saved", scenario=scenario, strategy=result.strategy,
             return_pct=round(result.total_profit_pct, 2),
             sharpe=round(result.sharpe_ratio, 2))


async def main() -> None:
    import logging
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            kst_timestamper,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)
    bridge = FreqtradeBridge()

    # 기존 시나리오 데이터 삭제 후 재실행 (중복 방지)
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM backtest_results WHERE metadata->>'scenario' IS NOT NULL"
        )
        log.info("cleared_scenario_results", deleted=deleted)

    total = len(SCENARIOS)
    for i, (scenario, strategy, symbol, start, end, timeframe, capital) in enumerate(SCENARIOS):
        log.info("running", num=f"{i+1}/{total}", scenario=scenario,
                 strategy=strategy, timeframe=timeframe, capital=capital)
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_dt   = datetime.strptime(end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

            ohlcv   = await load_ohlcv(pool, symbol, timeframe, start_dt, end_dt)
            funding = await load_funding(pool, symbol, start_dt, end_dt)

            if ohlcv.empty:
                log.warning("no_data_skip", scenario=scenario, strategy=strategy)
                continue

            result = bridge.run_backtest(
                strategy=strategy,
                ohlcv=ohlcv,
                funding=funding,
                initial_capital=float(capital),
            )
            await save_result(pool, scenario, result, symbol, start, end, timeframe)
        except Exception:
            log.exception("scenario_failed", scenario=scenario, strategy=strategy)

    # 결과 요약
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT metadata->>'scenario' AS scenario, strategy,
                   round(total_return::numeric, 2) AS ret,
                   round(sharpe_ratio::numeric, 2) AS sharpe,
                   round(max_drawdown::numeric, 2) AS mdd,
                   total_trades
            FROM backtest_results
            WHERE metadata->>'scenario' IS NOT NULL
            ORDER BY scenario, strategy
            """
        )
    print("\n" + "=" * 80)
    print(f"{'시나리오':<12} {'전략':<15} {'수익률%':>8} {'Sharpe':>8} {'MDD%':>8} {'거래수':>6}")
    print("=" * 80)
    for r in rows:
        print(f"{r['scenario']:<12} {r['strategy']:<15} {r['ret']:>8} {r['sharpe']:>8} {r['mdd']:>8} {r['total_trades']:>6}")
    print("=" * 80)

    await pool.close()
    log.info("all_scenarios_complete")


if __name__ == "__main__":
    asyncio.run(main())
