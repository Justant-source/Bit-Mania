"""Test E — 최적 가중치 ±20% 변동 시 Sharpe 안정성 측정

Stage 3의 최적 가중치를 기준으로 레짐별 활성 전략 비중을 5단계 변동하여
Sharpe 표준편차가 0.3 미만이면 "안정적", 이상이면 "불안정 (과적합 의심)"으로 판정.

결과 → strategy_variant_results 테이블 저장 (test_name="test_e_weight_sensitivity")

콘솔 출력 예시:
  레짐        | 변동값 | Sharpe | 판정
  ----------+-------+--------+------
  ranging   |  0.30 |  x.xxx |  ...
  ...
  표준편차: 0.xxx → 안정/불안정
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import statistics
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import structlog

from freqtrade_bridge import BacktestResult
from weight_optimizer import (
    combine_curves_from_cache,
    precompute_strategy_curves,
    split_by_regime,
    load_ohlcv,
    load_funding,
    INITIAL_CAPITAL,
)

log = structlog.get_logger(__name__)

# ── DB 연결 ──────────────────────────────────────────────────────────────────
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL     = "BTCUSDT"
TIMEFRAME  = "1h"
START_DATE = "2023-04-01"
END_DATE   = "2026-03-31"
TEST_NAME  = "test_e_weight_sensitivity"

# ── Stage 3 최적 가중치 기준 (FA, DCA, Cash) ────────────────────────────────
BASE_WEIGHTS: dict[str, tuple[float, float, float]] = {
    "ranging":       (0.50, 0.00, 0.50),
    "trending_up":   (0.00, 0.50, 0.50),
    "trending_down": (0.00, 0.00, 1.00),
    "volatile":      (0.50, 0.00, 0.50),
}

# ── 레짐별 변동 테스트 정의 ──────────────────────────────────────────────────
# 각 레짐에서 변동할 전략 인덱스와 5개 테스트 값
#   (strategy_idx, test_values)
#   strategy_idx: 0=FA, 1=DCA, 2=Cash
#   test_values: 활성 전략 비중 5개, Cash = 1 - active_weight (나머지 0 고정)
REGIME_VARIATIONS: dict[str, dict[str, Any]] = {
    "ranging": {
        "active_strategy_idx": 0,       # FA 변동
        "active_strategy_name": "FA",
        "test_values": [0.30, 0.40, 0.50, 0.60, 0.70],
        "base_value": 0.50,
        "description": "FA 비중 변동 (Cash = 1 - FA)",
    },
    "trending_up": {
        "active_strategy_idx": 2,       # DCA 변동
        "active_strategy_name": "DCA",
        "test_values": [0.30, 0.40, 0.50, 0.60, 0.70],
        "base_value": 0.50,
        "description": "DCA 비중 변동 (Cash = 1 - DCA)",
    },
    "trending_down": {
        "active_strategy_idx": 0,       # FA 소량 테스트 (기준은 0, 즉 Cash=1)
        "active_strategy_name": "FA",
        "test_values": [0.00, 0.05, 0.10, 0.15, 0.20],
        "base_value": 0.00,
        "description": "FA 소량 투입 테스트 (Cash = 1 - FA)",
    },
    "volatile": {
        "active_strategy_idx": 0,       # FA 변동 (ranging과 동일 패턴)
        "active_strategy_name": "FA",
        "test_values": [0.30, 0.40, 0.50, 0.60, 0.70],
        "base_value": 0.50,
        "description": "FA 비중 변동 (Cash = 1 - FA)",
    },
}

STABILITY_THRESHOLD = 0.3   # 표준편차 < 0.3 → 안정적


def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return v


def _build_weights_for_regime(
    regime: str,
    variation: dict[str, Any],
    active_value: float,
) -> tuple[float, float, float]:
    """레짐 기준 가중치에서 지정 전략 비중만 active_value로 변경, Cash = 1 - active_value."""
    if regime in ("ranging", "volatile"):
        # FA만 활성, DCA는 0
        fa_w = active_value
        dca_w = 0.0
    elif regime == "trending_up":
        # DCA만 활성, FA는 0
        fa_w = 0.0
        dca_w = active_value
    elif regime == "trending_down":
        # FA 소량 투입, DCA는 0
        fa_w = active_value
        dca_w = 0.0
    else:
        fa_w = active_value
        dca_w = 0.0

    cash_w = max(0.0, 1.0 - fa_w - dca_w)
    return (fa_w, dca_w, cash_w)


async def save_sensitivity_result(
    pool: asyncpg.Pool,
    regime: str,
    active_value: float,
    result: BacktestResult,
    weights: tuple[float, float, float, float],
    variation: dict[str, Any],
    is_base: bool,
) -> None:
    monthly: dict[str, float] = {}
    if result.daily_returns:
        try:
            start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            for i, ret in enumerate(result.daily_returns):
                day = pd.Timestamp(start_dt) + pd.Timedelta(hours=i)
                key = day.strftime("%Y-%m")
                monthly[key] = monthly.get(key, 0.0) + _safe_float(ret)
        except Exception:
            pass

    variant_name = f"{regime}_{variation['active_strategy_name']}_{active_value:.2f}"

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO strategy_variant_results
                (test_name, variant_name, data_range,
                 total_return, sharpe_ratio, max_drawdown,
                 trade_count, win_rate, profit_factor,
                 monthly_returns, params)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
            ON CONFLICT DO NOTHING
            """,
            TEST_NAME,
            variant_name,
            f"{START_DATE}~{END_DATE}",
            _safe_float(result.total_profit_pct),
            _safe_float(result.sharpe_ratio),
            _safe_float(result.max_drawdown_pct),
            result.total_trades,
            _safe_float(result.win_rate),
            _safe_float(result.profit_factor, default=0.0),
            json.dumps(monthly),
            json.dumps({
                "regime": regime,
                "active_strategy": variation["active_strategy_name"],
                "active_value": active_value,
                "weights": {"FA": weights[0], "Grid": weights[1],
                            "DCA": weights[2], "Cash": weights[3]},
                "is_base": is_base,
                "description": variation["description"],
            }),
        )


async def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(END_DATE,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    log.info("loading_data", symbol=SYMBOL, timeframe=TIMEFRAME,
             start=START_DATE, end=END_DATE)
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        log.error("no_ohlcv_data",
                  hint="먼저 fetch_real_ohlcv.py 또는 seed_historical.py를 실행하세요.")
        await pool.close()
        return

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # ── 레짐별 데이터 분할 ───────────────────────────────────────────────────
    log.info("splitting_by_regime")
    regime_data = split_by_regime(ohlcv, funding)
    log.info("regime_split_done", regimes=list(regime_data.keys()))

    # ── 전략 곡선 사전 계산 (레짐별) ────────────────────────────────────────
    regime_curves: dict[str, dict[str, list[float]]] = {}
    for regime, (r_ohlcv, r_funding) in regime_data.items():
        log.info("precomputing_curves", regime=regime, bars=len(r_ohlcv))
        curves = precompute_strategy_curves(r_ohlcv, r_funding, INITIAL_CAPITAL)
        regime_curves[regime] = curves
        log.info("curves_ready", regime=regime,
                 strategies=list(curves.keys()))

    # 기존 결과 삭제
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = $1",
            TEST_NAME,
        )
        log.info("cleared_previous", deleted=deleted)

    # ── 20회 백테스트 실행 + 결과 수집 ──────────────────────────────────────
    # regime → list of (active_value, sharpe)
    regime_sharpes: dict[str, list[tuple[float, float]]] = {
        r: [] for r in REGIME_VARIATIONS
    }

    for regime, variation in REGIME_VARIATIONS.items():
        r_ohlcv, _ = regime_data.get(regime, (pd.DataFrame(), pd.DataFrame()))
        if r_ohlcv.empty:
            log.warning("regime_no_data", regime=regime)
            # 데이터 없는 레짐은 전체 데이터로 fallback
            r_ohlcv = ohlcv

        curves = regime_curves.get(regime, {})
        if not curves:
            log.warning("regime_no_curves", regime=regime)
            curves = precompute_strategy_curves(r_ohlcv, funding, INITIAL_CAPITAL)

        for active_value in variation["test_values"]:
            weights = _build_weights_for_regime(regime, variation, active_value)
            is_base = (active_value == variation["base_value"])

            result = combine_curves_from_cache(
                curves=curves,
                weights=weights,
                initial_capital=INITIAL_CAPITAL,
                ohlcv=r_ohlcv,
            )

            sharpe = _safe_float(result.sharpe_ratio)
            regime_sharpes[regime].append((active_value, sharpe))

            log.info(
                "sensitivity_result",
                regime=regime,
                active_strategy=variation["active_strategy_name"],
                active_value=active_value,
                weights=weights,
                sharpe=round(sharpe, 4),
                is_base=is_base,
            )

            await save_sensitivity_result(
                pool, regime, active_value, result, weights, variation, is_base
            )

    # ── 콘솔 출력 ────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(f"Test E — 가중치 민감도 분석 ({START_DATE} ~ {END_DATE})")
    print("=" * 65)
    print(f"{'레짐':<14} | {'변동 전략':<8} | {'비중':>6} | {'Sharpe':>8} | {'판정'}")
    print("-" * 65)

    overall_stability: dict[str, str] = {}

    for regime, variation in REGIME_VARIATIONS.items():
        sharpe_list = [s for _, s in regime_sharpes[regime]]
        sharpe_values = [s for s in sharpe_list if not math.isnan(s) and not math.isinf(s)]

        std_val = statistics.stdev(sharpe_values) if len(sharpe_values) >= 2 else 0.0
        stability = "안정적" if std_val < STABILITY_THRESHOLD else "불안정 (과적합 의심)"
        overall_stability[regime] = stability

        for active_value, sharpe in regime_sharpes[regime]:
            is_base = (active_value == variation["base_value"])
            base_marker = " [기준]" if is_base else ""
            sharpe_str = f"{sharpe:8.3f}" if not (math.isnan(sharpe) or math.isinf(sharpe)) else "     N/A"
            print(
                f"{regime:<14} | "
                f"{variation['active_strategy_name']:<8} | "
                f"{active_value:>6.2f} | "
                f"{sharpe_str} | "
                f"{base_marker}"
            )

        print(
            f"{'':14}   {'':8}   {'':6}   {'표준편차':>8}: "
            f"{std_val:.3f} → {stability}"
        )
        print("-" * 65)

    print()
    print("[ 레짐별 안정성 요약 ]")
    print(f"{'레짐':<16} {'표준편차':>10} {'판정'}")
    print("-" * 45)
    for regime, variation in REGIME_VARIATIONS.items():
        sharpe_values = [
            s for _, s in regime_sharpes[regime]
            if not math.isnan(s) and not math.isinf(s)
        ]
        std_val = statistics.stdev(sharpe_values) if len(sharpe_values) >= 2 else 0.0
        print(f"{regime:<16} {std_val:>10.3f}   {overall_stability[regime]}")
    print("=" * 45)

    await pool.close()
    log.info("test_e_complete")


if __name__ == "__main__":
    asyncio.run(main())
