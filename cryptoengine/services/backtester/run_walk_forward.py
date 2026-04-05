"""Stage 4: Walk-Forward 검증 실행 및 DB 저장

2개 전략(funding_arb, combined)에 대해 walk-forward를 실행하고
결과를 walk_forward_results 테이블에 저장한다.

기본 설정:
  - 기간: 2025-10-01 ~ 2026-03-31 (6개월)
  - 윈도우: train=90일, test=30일  → 약 3개 윈도우
  - Monte Carlo: 100회
  - 심볼: BTCUSDT / 타임프레임: 1h

실행 예시:
  # 도커 내부
  python run_walk_forward.py

  # 환경변수 지정
  DB_HOST=localhost DB_PASSWORD=secret python run_walk_forward.py

  # 기간/윈도우 커스텀
  python run_walk_forward.py \
      --start 2025-07-01 --end 2026-03-31 \
      --train-days 120 --test-days 30 \
      --strategies funding_arb combined
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
from freqtrade_bridge import FreqtradeBridge
from walk_forward import WalkForwardAnalyzer, WalkForwardResult, WindowResult

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _configure_logging(level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            kst_timestamper,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# DB 연결 문자열 (환경변수 우선)
# ---------------------------------------------------------------------------

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)


# ---------------------------------------------------------------------------
# DB 헬퍼
# ---------------------------------------------------------------------------

async def _load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """ohlcv_history 테이블에서 OHLCV 데이터를 로드한다."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, open, high, low, close, volume
            FROM   ohlcv_history
            WHERE  symbol    = $1
              AND  timeframe = $2
              AND  timestamp >= $3
              AND  timestamp <= $4
            ORDER  BY timestamp ASC
            """,
            symbol,
            timeframe,
            start,
            end,
        )

    if not rows:
        log.warning("ohlcv_empty", symbol=symbol, timeframe=timeframe)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df


async def _load_funding_rates(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """funding_rate_history 테이블에서 펀딩비 히스토리를 로드한다."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timestamp AS ts, rate
            FROM   funding_rate_history
            WHERE  symbol    = $1
              AND  timestamp >= $2
              AND  timestamp <= $3
            ORDER  BY timestamp ASC
            """,
            symbol,
            start,
            end,
        )

    if not rows:
        log.warning("funding_empty", symbol=symbol)
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    df["rate"] = df["rate"].astype(float)
    return df


# ---------------------------------------------------------------------------
# DB 저장
# ---------------------------------------------------------------------------

def _monte_carlo_to_dict(mc: Any) -> dict | None:
    """MonteCarloResult 인스턴스를 JSON-직렬화 가능한 dict로 변환한다."""
    if mc is None:
        return None
    if dataclasses.is_dataclass(mc):
        return dataclasses.asdict(mc)
    return None


async def _save_window_result(
    conn: asyncpg.Connection,
    *,
    strategy: str,
    window_id: int,
    window: WindowResult,
    run_id: str,
) -> None:
    """단일 walk-forward 윈도우 결과를 DB에 저장한다."""
    wnd = window.window
    tr = window.train_result
    te = window.test_result

    await conn.execute(
        """
        INSERT INTO walk_forward_results (
            strategy,
            window_id,
            train_start, train_end,
            test_start,  test_end,
            train_sharpe,
            train_return_pct,
            train_max_drawdown_pct,
            test_sharpe,
            test_return_pct,
            test_max_drawdown_pct,
            test_win_rate,
            test_total_trades,
            aggregate_sharpe,
            consistency_ratio,
            sharpe_alert,
            monte_carlo,
            run_id
        ) VALUES (
            $1, $2,
            $3, $4,
            $5, $6,
            $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, $16, $17,
            $18,
            $19
        )
        """,
        strategy,
        window_id,
        wnd.train_start,
        wnd.train_end,
        wnd.test_start,
        wnd.test_end,
        tr.sharpe_ratio,
        tr.total_profit_pct,
        tr.max_drawdown_pct,
        te.sharpe_ratio,
        te.total_profit_pct,
        te.max_drawdown_pct,
        te.win_rate,
        te.total_trades,
        None,   # aggregate_sharpe — 개별 윈도우에는 NULL
        None,   # consistency_ratio — 개별 윈도우에는 NULL
        False,  # sharpe_alert — 개별 윈도우에는 false
        None,   # monte_carlo — 개별 윈도우에는 NULL
        run_id,
    )


async def _save_aggregate_result(
    conn: asyncpg.Connection,
    *,
    result: WalkForwardResult,
    run_id: str,
) -> None:
    """집계 walk-forward 결과(마지막 행)를 DB에 저장한다."""
    mc_json = _monte_carlo_to_dict(result.monte_carlo)

    await conn.execute(
        """
        INSERT INTO walk_forward_results (
            strategy,
            window_id,
            train_start, train_end,
            test_start,  test_end,
            train_sharpe,
            train_return_pct,
            train_max_drawdown_pct,
            test_sharpe,
            test_return_pct,
            test_max_drawdown_pct,
            test_win_rate,
            test_total_trades,
            aggregate_sharpe,
            consistency_ratio,
            sharpe_alert,
            monte_carlo,
            run_id
        ) VALUES (
            $1, $2,
            $3, $4,
            $5, $6,
            $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, $16, $17,
            $18::jsonb,
            $19
        )
        """,
        result.strategy,
        -1,       # window_id = -1 → 집계 행 표시
        None,     # train_start
        None,     # train_end
        None,     # test_start
        None,     # test_end
        None,     # train_sharpe
        None,     # train_return_pct
        None,     # train_max_drawdown_pct
        result.aggregate_sharpe,
        result.aggregate_profit_pct,
        result.aggregate_max_drawdown_pct,
        result.aggregate_win_rate,
        result.aggregate_total_trades,
        result.aggregate_sharpe,
        result.consistency_ratio,
        result.sharpe_alert,
        json.dumps(mc_json) if mc_json is not None else None,
        run_id,
    )


async def _persist_results(
    pool: asyncpg.Pool,
    result: WalkForwardResult,
    run_id: str,
) -> int:
    """walk_forward_results 테이블에 모든 윈도우 + 집계 행을 저장한다.

    Returns
    -------
    저장된 총 행 수 (윈도우 수 + 집계 행 1).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for idx, wnd_result in enumerate(result.windows):
                await _save_window_result(
                    conn,
                    strategy=result.strategy,
                    window_id=idx,
                    window=wnd_result,
                    run_id=run_id,
                )
            await _save_aggregate_result(conn, result=result, run_id=run_id)

    total_rows = len(result.windows) + 1
    log.info(
        "db_save_complete",
        strategy=result.strategy,
        rows=total_rows,
        run_id=run_id,
    )
    return total_rows


# ---------------------------------------------------------------------------
# 콘솔 결과표 출력
# ---------------------------------------------------------------------------

def _print_separator(width: int = 110) -> None:
    print("─" * width)


def _print_result_table(result: WalkForwardResult, run_id: str) -> None:
    """Walk-Forward 결과를 콘솔에 표 형식으로 출력한다."""
    strategy = result.strategy.upper()
    print()
    print(f"  Walk-Forward 결과 — {strategy}  (run_id: {run_id})")
    _print_separator()
    print(
        f"  {'Win#':>4}  {'Train 기간':^22}  {'Test 기간':^22}"
        f"  {'Train Sharpe':>12}  {'Train Ret%':>10}"
        f"  {'Test Sharpe':>11}  {'Test Ret%':>9}"
        f"  {'MaxDD%':>7}  {'Trades':>6}  {'WinRate%':>8}"
    )
    _print_separator()

    for idx, wnd_result in enumerate(result.windows):
        wnd = wnd_result.window
        tr  = wnd_result.train_result
        te  = wnd_result.test_result
        print(
            f"  {idx:>4}  "
            f"{wnd.train_start:%Y-%m-%d} ~ {wnd.train_end:%Y-%m-%d}  "
            f"{wnd.test_start:%Y-%m-%d} ~ {wnd.test_end:%Y-%m-%d}  "
            f"{tr.sharpe_ratio:>12.3f}  "
            f"{tr.total_profit_pct:>10.2f}  "
            f"{te.sharpe_ratio:>11.3f}  "
            f"{te.total_profit_pct:>9.2f}  "
            f"{te.max_drawdown_pct:>7.2f}  "
            f"{te.total_trades:>6}  "
            f"{te.win_rate:>8.1f}"
        )

    _print_separator()

    # 집계
    alert_tag = "  [!] SHARPE ALERT" if result.sharpe_alert else ""
    print(
        f"  {'집계':>4}  {'':22}  {'':22}"
        f"  {'':12}  {'':10}"
        f"  {result.aggregate_sharpe:>11.3f}  "
        f"{result.aggregate_profit_pct:>9.2f}  "
        f"{result.aggregate_max_drawdown_pct:>7.2f}  "
        f"{result.aggregate_total_trades:>6}  "
        f"{result.aggregate_win_rate:>8.1f}"
        f"{alert_tag}"
    )
    print(f"  consistency_ratio = {result.consistency_ratio:.3f}  "
          f"(양수 OOS 윈도우: "
          f"{sum(1 for w in result.windows if w.test_result.total_profit_pct > 0)}"
          f" / {len(result.windows)})")

    # Monte Carlo
    mc = result.monte_carlo
    if mc is not None:
        print()
        print(f"  Monte Carlo ({mc.n_simulations}회)")
        print(f"    Sharpe  : {mc.sharpe_mean:.3f} ± {mc.sharpe_std:.3f}  "
              f"95% CI [{mc.sharpe_ci_95[0]:.3f}, {mc.sharpe_ci_95[1]:.3f}]")
        print(f"    Profit% : {mc.profit_mean:.2f} ± {mc.profit_std:.2f}  "
              f"95% CI [{mc.profit_ci_95[0]:.2f}, {mc.profit_ci_95[1]:.2f}]")
        print(f"    MaxDD%  : {mc.max_dd_mean:.2f} ± {mc.max_dd_std:.2f}  "
              f"95% CI [{mc.max_dd_ci_95[0]:.2f}, {mc.max_dd_ci_95[1]:.2f}]")
        print(f"    승률    : {mc.win_probability * 100:.1f}%  (시뮬레이션 중 수익 양수 비율)")

    _print_separator()


# ---------------------------------------------------------------------------
# 메인 실행 흐름
# ---------------------------------------------------------------------------

async def _run_all(args: argparse.Namespace) -> None:
    """지정된 모든 전략에 대해 Walk-Forward를 실행하고 결과를 DB에 저장한다."""
    _configure_logging(os.getenv("LOG_LEVEL", "INFO"))

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    log.info(
        "walk_forward_runner_start",
        strategies=args.strategies,
        start=args.start,
        end=args.end,
        train_days=args.train_days,
        test_days=args.test_days,
        monte_carlo_runs=args.monte_carlo_runs,
        symbol=args.symbol,
        timeframe=args.timeframe,
        dry_run=args.dry_run,
    )

    # DB 연결 풀
    try:
        pool: asyncpg.Pool = await asyncpg.create_pool(
            dsn=DB_DSN, min_size=2, max_size=5, command_timeout=60
        )
    except Exception as exc:
        log.error("db_connect_failed", error=str(exc))
        sys.exit(1)

    # 데이터 로드 (전략에 무관하게 공통)
    log.info("loading_market_data", symbol=args.symbol, timeframe=args.timeframe)
    ohlcv   = await _load_ohlcv(pool, args.symbol, args.timeframe, start_dt, end_dt)
    funding = await _load_funding_rates(pool, args.symbol, start_dt, end_dt)

    if ohlcv.empty:
        log.error(
            "no_ohlcv_available",
            symbol=args.symbol,
            timeframe=args.timeframe,
            hint="백테스트 전 fetch_real_ohlcv.py 또는 seed_historical.py 를 먼저 실행하세요.",
        )
        await pool.close()
        sys.exit(1)

    log.info(
        "data_loaded",
        ohlcv_rows=len(ohlcv),
        funding_rows=len(funding),
        ohlcv_start=str(ohlcv.index.min()),
        ohlcv_end=str(ohlcv.index.max()),
    )

    # WalkForwardAnalyzer 공통 인스턴스
    analyzer = WalkForwardAnalyzer(
        train_days=args.train_days,
        test_days=args.test_days,
        monte_carlo_runs=args.monte_carlo_runs,
    )

    summary_rows: list[dict] = []

    for strategy in args.strategies:
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        run_id = f"{strategy}_{run_ts}"

        log.info("strategy_walk_forward_start", strategy=strategy, run_id=run_id)

        result: WalkForwardResult = analyzer.run(
            ohlcv=ohlcv,
            funding=funding if strategy in ("funding_arb", "combined_v2") else None,
            strategy=strategy,
            initial_capital=args.capital,
        )

        if not result.windows:
            log.warning(
                "no_windows_produced",
                strategy=strategy,
                hint=(
                    f"데이터 기간이 train({args.train_days}d) + test({args.test_days}d) = "
                    f"{args.train_days + args.test_days}일보다 짧을 수 있습니다."
                ),
            )
        else:
            # 콘솔 출력
            _print_result_table(result, run_id)

        # DB 저장 (dry_run=True 이면 건너뜀)
        rows_saved = 0
        if not args.dry_run and result.windows:
            rows_saved = await _persist_results(pool, result, run_id)
        elif args.dry_run:
            log.info("dry_run_skip_db_save", strategy=strategy)

        summary_rows.append({
            "strategy": strategy,
            "run_id": run_id,
            "windows": len(result.windows),
            "aggregate_sharpe": round(result.aggregate_sharpe, 4),
            "aggregate_profit_pct": round(result.aggregate_profit_pct, 4),
            "max_drawdown_pct": round(result.aggregate_max_drawdown_pct, 4),
            "consistency_ratio": round(result.consistency_ratio, 4),
            "sharpe_alert": result.sharpe_alert,
            "rows_saved": rows_saved,
        })

    await pool.close()

    # 최종 요약 출력
    print()
    print("=" * 90)
    print("  최종 요약")
    print("=" * 90)
    print(
        f"  {'전략':<20}  {'윈도우':>6}  {'Sharpe':>8}  "
        f"{'수익률%':>9}  {'MaxDD%':>7}  {'일관성':>7}  "
        f"{'경고':>6}  {'DB행':>6}"
    )
    print("-" * 90)
    for row in summary_rows:
        alert = "YES" if row["sharpe_alert"] else "-"
        print(
            f"  {row['strategy']:<20}  {row['windows']:>6}  "
            f"{row['aggregate_sharpe']:>8.3f}  "
            f"{row['aggregate_profit_pct']:>9.2f}  "
            f"{row['max_drawdown_pct']:>7.2f}  "
            f"{row['consistency_ratio']:>7.3f}  "
            f"{alert:>6}  "
            f"{row['rows_saved']:>6}"
        )
    print("=" * 90)

    # Sharpe 경고가 있는 전략이 있으면 종료 코드 1
    if any(row["sharpe_alert"] for row in summary_rows):
        log.warning(
            "sharpe_alert_raised",
            strategies=[r["strategy"] for r in summary_rows if r["sharpe_alert"]],
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 4: Walk-Forward 검증 실행 및 DB 저장",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start",
        default="2025-10-01",
        help="백테스트 시작일 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        default="2026-03-31",
        help="백테스트 종료일 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=["funding_arb", "adaptive_dca", "combined_v2"],
        choices=["funding_arb", "adaptive_dca", "adaptive_dca_graduated", "combined", "combined_v2"],
        help="실행할 전략 목록",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="거래 심볼",
    )
    parser.add_argument(
        "--timeframe",
        default="1h",
        help="OHLCV 타임프레임",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=10_000.0,
        help="초기 자본 (USDT)",
    )
    parser.add_argument(
        "--train-days",
        type=int,
        default=90,
        help="Walk-Forward 훈련 윈도우 (일)",
    )
    parser.add_argument(
        "--test-days",
        type=int,
        default=30,
        help="Walk-Forward 테스트 윈도우 (일)",
    )
    parser.add_argument(
        "--monte-carlo-runs",
        type=int,
        default=100,
        help="Monte Carlo 시뮬레이션 횟수",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB 저장 없이 결과만 출력 (테스트용)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(_run_all(args))
    except KeyboardInterrupt:
        log.info("interrupted_by_user")
        sys.exit(0)
