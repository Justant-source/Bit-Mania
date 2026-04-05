"""
Stage 3: 가중치 매트릭스 최적화

- 레짐별로 독립적으로 최적 가중치 탐색
- 목표 함수: Sharpe*0.4 + pct_positive_months*0.4 - max_dd_penalty*0.2
- weight_optimization_results 테이블에 저장
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone
from itertools import product
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

from freqtrade_bridge import BacktestResult, FreqtradeBridge

log = structlog.get_logger(__name__)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL         = "BTCUSDT"
TIMEFRAME      = "1h"
START          = datetime(2025, 10, 1, tzinfo=timezone.utc)
END            = datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc)
INITIAL_CAPITAL = 10_000.0
MIN_BARS_FOR_REGIME = 100  # 레짐별 데이터 최소 봉 수

# ── 비교 기준선 가중치 정의 ────────────────────────────────────────────────
# (funding_arb, grid, dca, cash)
BASELINES: dict[str, dict[str, tuple[float, float, float, float]]] = {
    "ranging": {
        "baseline":      (0.25, 0.40, 0.15, 0.20),
        "conservative":  (0.25, 0.40, 0.15, 0.30),   # cash +10%
        "aggressive":    (0.30, 0.45, 0.15, 0.10),   # cash -10% (min 0.10)
        "funding_arb_100": (0.50, 0.20, 0.20, 0.10), # funding_arb max (cash min)
        "equal_weight":  (0.225, 0.225, 0.225, 0.325),
    },
    "trending_up": {
        "baseline":      (0.15, 0.05, 0.50, 0.30),
        "conservative":  (0.15, 0.05, 0.50, 0.30),
        "aggressive":    (0.20, 0.05, 0.50, 0.25),
        "funding_arb_100": (0.50, 0.20, 0.20, 0.10),
        "equal_weight":  (0.225, 0.225, 0.225, 0.325),
    },
    "trending_down": {
        "baseline":      (0.20, 0.05, 0.10, 0.65),
        "conservative":  (0.15, 0.05, 0.10, 0.70),
        "aggressive":    (0.25, 0.10, 0.10, 0.55),
        "funding_arb_100": (0.50, 0.20, 0.20, 0.10),
        "equal_weight":  (0.225, 0.225, 0.225, 0.325),
    },
    "volatile": {
        "baseline":      (0.10, 0.05, 0.05, 0.80),
        "conservative":  (0.10, 0.05, 0.05, 0.80),
        "aggressive":    (0.15, 0.10, 0.10, 0.65),
        "funding_arb_100": (0.50, 0.20, 0.20, 0.10),
        "equal_weight":  (0.225, 0.225, 0.225, 0.325),
    },
}


# ---------------------------------------------------------------------------
# DB 헬퍼
# ---------------------------------------------------------------------------

async def load_ohlcv(
    pool: asyncpg.Pool,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
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
        log.warning("no_ohlcv", symbol=symbol, timeframe=timeframe)
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


async def load_funding(
    pool: asyncpg.Pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
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


async def load_best_regime_method(pool: asyncpg.Pool) -> str | None:
    """regime_accuracy_results 테이블에서 composite_score 최고 방법 반환."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT method FROM regime_accuracy_results
            ORDER BY composite_score DESC LIMIT 1
            """
        )
    return row["method"] if row else None


# ---------------------------------------------------------------------------
# 가중치 후보 생성
# ---------------------------------------------------------------------------

def generate_weight_candidates(
    step: float = 0.05,
) -> list[tuple[float, float, float]]:
    """0.05 단위, 합=1.0, 제약조건:
      - cash >= 0.10
      - funding_arb, dca 각각 <= 0.80
      - 3-tuple (funding_arb, dca, cash)
    """
    candidates: list[tuple[float, float, float]] = []
    vals = [round(v * step, 10) for v in range(0, int(1.0 / step) + 1)]

    for fa in vals:
        if fa > 0.80:
            continue
        for dca in vals:
            if dca > 0.80:
                continue
            cash = round(1.0 - fa - dca, 10)
            if cash < 0.10 or cash > 1.0:
                continue
            # 합 검증
            if abs(fa + dca + cash - 1.0) > 1e-9:
                continue
            candidates.append((fa, dca, cash))

    return candidates


# ---------------------------------------------------------------------------
# 목적 함수
# ---------------------------------------------------------------------------

def compute_positive_months(daily_returns: list[float], start_dt: datetime) -> float:
    """월별로 누적 수익률 계산 → 양수인 달 비율 반환."""
    if not daily_returns:
        return 0.0

    monthly: dict[str, float] = {}
    for i, ret in enumerate(daily_returns):
        day = pd.Timestamp(start_dt) + pd.Timedelta(hours=i)  # 1h bar 기준
        key = day.strftime("%Y-%m")
        if math.isnan(ret) or math.isinf(ret):
            ret = 0.0
        monthly[key] = monthly.get(key, 0.0) + ret

    if not monthly:
        return 0.0

    positive = sum(1 for v in monthly.values() if v > 0)
    return positive / len(monthly)


def objective_function(
    result: BacktestResult,
    start_dt: datetime,
) -> float:
    """Sharpe*0.4 + pct_positive_months*0.4 - max_dd_penalty*0.2."""
    sharpe = result.sharpe_ratio
    if math.isnan(sharpe) or math.isinf(sharpe):
        sharpe = 0.0

    max_dd = result.max_drawdown_pct / 100.0  # 0~1 스케일
    positive_months = compute_positive_months(result.daily_returns, start_dt)

    penalty = max(0.0, max_dd - 0.05) * 10.0  # 5% 초과 시 패널티

    score = sharpe * 0.4 + positive_months * 0.4 - penalty * 0.2

    if math.isnan(score) or math.isinf(score):
        return -999.0
    return score


# ---------------------------------------------------------------------------
# 레짐 판별 (간단 버전 — regime_accuracy.py에서 재구현)
# ---------------------------------------------------------------------------

def _compute_adx_simple(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = 14,
) -> float:
    n = len(closes)
    if n < period * 2:
        return 0.0
    plus_dm_list, minus_dm_list, tr_list = [], [], []
    for i in range(1, n):
        up   = float(highs[i])    - float(highs[i - 1])
        down = float(lows[i - 1]) - float(lows[i])
        plus_dm_list.append(up   if up   > down and up   > 0 else 0.0)
        minus_dm_list.append(down if down > up   and down > 0 else 0.0)
        hl = float(highs[i])  - float(lows[i])
        hc = abs(float(highs[i])  - float(closes[i - 1]))
        lc = abs(float(lows[i])   - float(closes[i - 1]))
        tr_list.append(max(hl, hc, lc))
    if len(tr_list) < period:
        return 0.0
    atr_val = float(np.mean(tr_list[-period:]))
    if atr_val <= 0:
        return 0.0
    plus_di  = 100.0 * float(np.mean(plus_dm_list[-period:]))  / atr_val
    minus_di = 100.0 * float(np.mean(minus_dm_list[-period:])) / atr_val
    di_sum   = plus_di + minus_di
    if di_sum <= 0:
        return 0.0
    return float(100.0 * abs(plus_di - minus_di) / di_sum)


def classify_dominant_regime(df: pd.DataFrame) -> str:
    """전체 데이터프레임에서 가장 많이 나온 레짐 반환."""
    from collections import Counter

    closes  = df["close"].values.astype(float)
    highs   = df["high"].values.astype(float)  if "high"  in df.columns else closes.copy()
    lows    = df["low"].values.astype(float)   if "low"   in df.columns else closes.copy()

    n = len(closes)
    warmup = 28
    regimes = []

    for i in range(warmup, n):
        lo = max(0, i - 100)
        adx = _compute_adx_simple(highs[lo:i+1], lows[lo:i+1], closes[lo:i+1])

        bb_start = max(0, i - 19)
        bb_w = closes[bb_start:i+1]
        sma20 = float(np.mean(bb_w))
        std20 = float(np.std(bb_w))

        atr_start = max(1, i - 13)
        tr_vals = []
        for j in range(atr_start, i+1):
            hl = highs[j] - lows[j]
            hc = abs(highs[j] - closes[j-1])
            lc = abs(lows[j]  - closes[j-1])
            tr_vals.append(max(hl, hc, lc))
        atr = float(np.mean(tr_vals)) if tr_vals else 0.0
        atr_ratio = atr / closes[i] if closes[i] > 0 else 0.0

        if atr_ratio > 0.03 and adx > 30:
            regimes.append("volatile")
        elif adx > 25 and closes[i] > sma20 * 1.02:
            regimes.append("trending_up")
        elif adx > 25 and closes[i] < sma20 * 0.98:
            regimes.append("trending_down")
        else:
            regimes.append("ranging")

    if not regimes:
        return "ranging"

    counter = Counter(regimes)
    return counter.most_common(1)[0][0]


# ---------------------------------------------------------------------------
# 가중치 적용 백테스트 (혼합 전략)
# ---------------------------------------------------------------------------

def precompute_strategy_curves(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, list[float]]:
    """각 전략을 1번씩 실행하고 정규화된 equity curve를 반환 (0 ~ 1 → return ratio).

    정규화: curve[t] = equity[t] / initial_capital
    조합 시: combined[t] = sum(w_i * normalized_curve_i[t]) * initial_capital
    """
    bridge = FreqtradeBridge()
    curves: dict[str, list[float]] = {}

    for strategy_name in ["funding_arb", "adaptive_dca"]:
        r = bridge.run_backtest(
            strategy=strategy_name,
            ohlcv=ohlcv,
            funding=funding if strategy_name == "funding_arb" else None,
            initial_capital=initial_capital,
        )
        # 정규화 (각 시점의 자본 비율)
        if r.equity_curve and r.initial_capital > 0:
            curves[strategy_name] = [v / r.initial_capital for v in r.equity_curve]
        else:
            curves[strategy_name] = [1.0, r.final_capital / r.initial_capital if r.initial_capital > 0 else 1.0]

    return curves


def combine_curves_from_cache(
    curves: dict[str, list[float]],
    weights: tuple[float, float, float],
    initial_capital: float = INITIAL_CAPITAL,
    ohlcv: pd.DataFrame | None = None,
) -> BacktestResult:
    """캐시된 정규화 equity curve와 가중치로 포트폴리오 결과를 수학적으로 조합."""
    from freqtrade_bridge import (_compute_drawdown, _compute_daily_returns,
                                  _compute_sharpe, _compute_sortino, _drawdown_series)

    fa_w, dca_w, cash_w = weights

    # 최소 길이 기준으로 맞춤
    active = [(fa_w, "funding_arb"), (dca_w, "adaptive_dca")]
    active_with_data = [(w, curves[s]) for w, s in active if w > 0 and s in curves]

    if not active_with_data:
        min_len = 2
    else:
        min_len = min(len(c) for _, c in active_with_data)

    # 가중 합산 (cash는 원금 유지)
    combined: list[float] = []
    for t in range(min_len):
        val = cash_w * initial_capital
        for w, curve in active_with_data:
            val += w * initial_capital * (curve[t] if t < len(curve) else curve[-1])
        combined.append(val)

    if len(combined) < 2:
        combined = [initial_capital, initial_capital]

    total_profit = combined[-1] - initial_capital
    profit_pct = (total_profit / initial_capital * 100) if initial_capital > 0 else 0.0

    max_dd, max_dd_pct = _compute_drawdown(combined)
    daily_returns = _compute_daily_returns(combined)
    sharpe = _compute_sharpe(daily_returns)
    sortino = _compute_sortino(daily_returns)
    dd_curve = _drawdown_series(combined)

    start_date = str(ohlcv.index[0]) if ohlcv is not None and len(ohlcv) > 0 else ""
    end_date = str(ohlcv.index[-1]) if ohlcv is not None and len(ohlcv) > 0 else ""

    return BacktestResult(
        strategy="weighted_portfolio",
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        final_capital=combined[-1],
        total_profit=total_profit,
        total_profit_pct=profit_pct,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        win_rate=0.0,
        total_trades=0,
        avg_trade_duration_hours=0.0,
        profit_factor=0.0,
        equity_curve=combined,
        drawdown_curve=dd_curve,
        daily_returns=daily_returns,
    )


def run_weighted_backtest(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    weights: tuple[float, float, float],
    initial_capital: float = INITIAL_CAPITAL,
    _cached_curves: dict[str, list[float]] | None = None,
) -> BacktestResult:
    """3개 전략에 가중치를 적용해 자본 분배 후 백테스트 실행.

    _cached_curves가 제공되면 미리 계산된 정규화 equity curve를 사용.
    제공되지 않으면 직접 실행 (느림, 하위 호환성 유지).
    """
    if _cached_curves is not None:
        return combine_curves_from_cache(_cached_curves, weights, initial_capital, ohlcv)

    fa_w, dca_w, cash_w = weights
    bridge = FreqtradeBridge()

    strategies = [
        ("funding_arb",  fa_w),
        ("adaptive_dca", dca_w),
    ]

    sub_results: list[tuple[float, BacktestResult]] = []
    for strategy_name, w in strategies:
        if w <= 0:
            continue
        sub_capital = initial_capital * w
        if sub_capital < 1.0:  # 너무 작은 자본은 스킵
            continue
        r = bridge.run_backtest(
            strategy=strategy_name,
            ohlcv=ohlcv,
            funding=funding,
            initial_capital=sub_capital,
        )
        sub_results.append((w, r))

    if not sub_results:
        # 전략 없음 — cash만 보유
        return BacktestResult(
            strategy="weighted_portfolio",
            start_date=str(ohlcv.index[0])  if len(ohlcv) > 0 else "",
            end_date=str(ohlcv.index[-1])   if len(ohlcv) > 0 else "",
            initial_capital=initial_capital,
            final_capital=initial_capital,
            total_profit=0.0,
            total_profit_pct=0.0,
            max_drawdown=0.0,
            max_drawdown_pct=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            win_rate=0.0,
            total_trades=0,
            avg_trade_duration_hours=0.0,
            profit_factor=0.0,
        )

    # ── 서브 결과 집계 ────────────────────────────────────────────────
    total_profit    = sum(r.total_profit   for _, r in sub_results)
    total_trades    = sum(r.total_trades   for _, r in sub_results)
    final_capital   = initial_capital + total_profit
    profit_pct      = (total_profit / initial_capital * 100) if initial_capital > 0 else 0.0

    # equity curve 가중 합산 (최소 길이 기준)
    min_len = min(len(r.equity_curve) for _, r in sub_results) if sub_results else 0
    if min_len > 0:
        # cash 포션은 고정 자본으로 추가
        cash_capital = initial_capital * cash_w
        combined_equity = [cash_capital] * min_len
        for w, r in sub_results:
            for i in range(min_len):
                combined_equity[i] += r.equity_curve[i] if i < len(r.equity_curve) else r.final_capital
    else:
        combined_equity = [initial_capital, final_capital]

    # drawdown
    from freqtrade_bridge import _compute_drawdown, _compute_daily_returns, _compute_sharpe, _compute_sortino, _drawdown_series
    max_dd, max_dd_pct = _compute_drawdown(combined_equity)
    daily_returns = _compute_daily_returns(combined_equity)
    sharpe   = _compute_sharpe(daily_returns)
    sortino  = _compute_sortino(daily_returns)
    dd_curve = _drawdown_series(combined_equity)

    start_date = str(ohlcv.index[0])  if len(ohlcv) > 0 else ""
    end_date   = str(ohlcv.index[-1]) if len(ohlcv) > 0 else ""

    return BacktestResult(
        strategy="weighted_portfolio",
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        final_capital=final_capital,
        total_profit=total_profit,
        total_profit_pct=profit_pct,
        max_drawdown=max_dd,
        max_drawdown_pct=max_dd_pct,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        win_rate=0.0,
        total_trades=total_trades,
        avg_trade_duration_hours=0.0,
        profit_factor=0.0,
        equity_curve=combined_equity,
        drawdown_curve=dd_curve,
        daily_returns=daily_returns,
    )


# ---------------------------------------------------------------------------
# 레짐별 데이터 분할
# ---------------------------------------------------------------------------

def split_by_regime(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    """레짐별 데이터를 판별해 분할.

    각 레짐 레이블별로 연속된 구간의 ohlcv / funding 데이터를 반환.
    데이터가 MIN_BARS_FOR_REGIME 미만이면 해당 레짐 키가 없음.
    """
    from collections import defaultdict

    closes = ohlcv["close"].values.astype(float)
    highs  = ohlcv["high"].values.astype(float)  if "high"  in ohlcv.columns else closes.copy()
    lows   = ohlcv["low"].values.astype(float)   if "low"   in ohlcv.columns else closes.copy()

    n = len(closes)
    warmup = 28
    regime_labels: list[str] = []

    for i in range(n):
        if i < warmup:
            regime_labels.append("ranging")
            continue
        lo = max(0, i - 100)
        adx = _compute_adx_simple(highs[lo:i+1], lows[lo:i+1], closes[lo:i+1])
        bb_start = max(0, i - 19)
        bb_w = closes[bb_start:i+1]
        sma20 = float(np.mean(bb_w))

        atr_start = max(1, i - 13)
        tr_vals = []
        for j in range(atr_start, i+1):
            hl = highs[j] - lows[j]
            hc = abs(highs[j] - closes[j-1])
            lc = abs(lows[j]  - closes[j-1])
            tr_vals.append(max(hl, hc, lc))
        atr = float(np.mean(tr_vals)) if tr_vals else 0.0
        atr_ratio = atr / closes[i] if closes[i] > 0 else 0.0

        if atr_ratio > 0.03 and adx > 30:
            regime_labels.append("volatile")
        elif adx > 25 and closes[i] > sma20 * 1.02:
            regime_labels.append("trending_up")
        elif adx > 25 and closes[i] < sma20 * 0.98:
            regime_labels.append("trending_down")
        else:
            regime_labels.append("ranging")

    regime_series = pd.Series(regime_labels, index=ohlcv.index)

    # 레짐별 인덱스 분류
    regime_indices: dict[str, list] = defaultdict(list)
    for ts, label in regime_series.items():
        regime_indices[label].append(ts)

    result: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for label, indices in regime_indices.items():
        if len(indices) < MIN_BARS_FOR_REGIME:
            log.warning(
                "regime_data_too_small",
                regime=label,
                bars=len(indices),
                min_bars=MIN_BARS_FOR_REGIME,
            )
            continue
        sub_ohlcv = ohlcv.loc[ohlcv.index.isin(indices)].sort_index()
        sub_funding = (
            funding.loc[funding.index.isin(indices)].sort_index()
            if not funding.empty
            else pd.DataFrame()
        )
        result[label] = (sub_ohlcv, sub_funding)

    return result


# ---------------------------------------------------------------------------
# DB 저장
# ---------------------------------------------------------------------------

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return v


async def save_optimization_result(
    pool: asyncpg.Pool,
    regime: str,
    variant: str,
    weights: tuple[float, float, float],
    result: BacktestResult,
    score: float,
    start_dt: datetime,
) -> None:
    fa_w, dca_w, cash_w = weights

    monthly_returns: dict[str, float] = {}
    if result.daily_returns:
        for i, ret in enumerate(result.daily_returns):
            day = pd.Timestamp(start_dt) + pd.Timedelta(hours=i)
            key = day.strftime("%Y-%m")
            r_val = ret if not (math.isnan(ret) or math.isinf(ret)) else 0.0
            monthly_returns[key] = monthly_returns.get(key, 0.0) + r_val

    pos_months = compute_positive_months(result.daily_returns, start_dt)

    metadata = {
        "variant": variant,
        "weights": {
            "funding_arb": fa_w,
            "dca": dca_w,
            "cash": cash_w,
        },
        "objective_score": _safe_float(score),
        "pct_positive_months": _safe_float(pos_months),
        "monthly_returns": monthly_returns,
        "total_profit_pct": _safe_float(result.total_profit_pct),
        "sortino_ratio": _safe_float(result.sortino_ratio),
        "total_trades": result.total_trades,
    }

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO weight_optimization_results
                (variant, regime, funding_arb_weight, dca_weight, cash_weight,
                 sharpe_ratio, max_drawdown_pct, total_return_pct,
                 pct_positive_months, composite_score, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            """,
            variant,
            regime,
            fa_w,
            dca_w,
            cash_w,
            _safe_float(result.sharpe_ratio),
            _safe_float(result.max_drawdown_pct),
            _safe_float(result.total_profit_pct * 2.0),  # 6개월 → 연환산
            _safe_float(pos_months),
            _safe_float(score),
            json.dumps(metadata),
        )

    log.info(
        "saved_weight_result",
        regime=regime,
        variant=variant,
        weights=f"FA={fa_w:.2f}/DCA={dca_w:.2f}/C={cash_w:.2f}",
        sharpe=round(_safe_float(result.sharpe_ratio), 3),
        score=round(_safe_float(score), 4),
    )


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

async def main() -> None:
    import logging as _logging

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(_logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    log.info("stage3_weight_optimizer_start")

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5,
                                     command_timeout=60)

    # ── 데이터 로드 ─────────────────────────────────────────────────────
    log.info("loading_ohlcv_and_funding")
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, START, END)
    funding = await load_funding(pool, SYMBOL, START, END)

    if ohlcv.empty:
        log.error("no_ohlcv_data_abort")
        await pool.close()
        return

    log.info("data_loaded", bars=len(ohlcv), funding_bars=len(funding))

    # ── 가중치 후보 생성 ────────────────────────────────────────────────
    log.info("generating_weight_candidates")
    candidates = generate_weight_candidates(step=0.05)
    log.info("weight_candidates_generated", count=len(candidates))

    # ── 레짐별 데이터 분할 ──────────────────────────────────────────────
    log.info("splitting_data_by_regime")
    regime_data = split_by_regime(ohlcv, funding)

    available_regimes = list(regime_data.keys())
    log.info("regimes_available", regimes=available_regimes)

    # ── 기존 결과 삭제 ───────────────────────────────────────────────────
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM weight_optimization_results")
    log.info("cleared_previous_weight_results")

    # ── 전체 데이터 준비 (레짐별 데이터 부족 시 대체용) ─────────────────
    all_data = {"all": (ohlcv, funding)}

    # ── 레짐별 파라미터 서치 + 기준선 비교 ──────────────────────────────
    summary_rows: list[dict] = []

    # 처리할 레짐 목록 (레짐별 + "all" fallback)
    target_regimes = ["ranging", "trending_up", "trending_down", "volatile"]

    for regime in target_regimes:
        if regime in regime_data:
            sub_ohlcv, sub_funding = regime_data[regime]
            log.info("regime_data_ok", regime=regime, bars=len(sub_ohlcv))
        else:
            sub_ohlcv, sub_funding = ohlcv, funding
            log.warning("regime_fallback_to_all", regime=regime,
                        bars=len(sub_ohlcv))

        # ── 전략 equity curve 사전 계산 (캐싱) ──────────────────────────
        log.info("precomputing_strategy_curves", regime=regime, bars=len(sub_ohlcv))
        cached_curves = precompute_strategy_curves(sub_ohlcv, sub_funding, INITIAL_CAPITAL)
        log.info("strategy_curves_ready", regime=regime, strategies=list(cached_curves.keys()))

        # ── 파라미터 서치 (캐시 활용 — 수학적 조합만 수행) ──────────────
        log.info("param_search_start", regime=regime, candidates=len(candidates))
        best_score  = -999.0
        best_weights: tuple[float, float, float] = (0.45, 0.10, 0.45)
        best_result: BacktestResult | None = None

        for i, w in enumerate(candidates):
            if i % 100 == 0:
                log.info("param_search_progress",
                         regime=regime,
                         progress=f"{i}/{len(candidates)}",
                         best_score=round(best_score, 4))
            try:
                result = run_weighted_backtest(sub_ohlcv, sub_funding, w,
                                               INITIAL_CAPITAL,
                                               _cached_curves=cached_curves)
                score  = objective_function(result, START)
                if score > best_score:
                    best_score   = score
                    best_weights = w
                    best_result  = result
            except Exception as exc:
                log.warning("backtest_failed_skip",
                            regime=regime, weights=w, error=str(exc))
                continue

        log.info("param_search_complete",
                 regime=regime,
                 best_weights=best_weights,
                 best_score=round(best_score, 4))

        if best_result is not None:
            await save_optimization_result(
                pool, regime, "param_search_optimal",
                best_weights, best_result, best_score, START,
            )
            summary_rows.append({
                "regime":   regime,
                "variant":  "param_search_optimal",
                "weights":  best_weights,
                "sharpe":   _safe_float(best_result.sharpe_ratio),
                "max_dd":   _safe_float(best_result.max_drawdown_pct),
                "annual_ret": _safe_float(best_result.total_profit_pct * 2.0),
                "pos_months": _safe_float(
                    compute_positive_months(best_result.daily_returns, START)
                ),
            })

        # ── 기준선 5가지 (캐시 활용) ─────────────────────────────────────
        baseline_weights = BASELINES.get(regime, BASELINES["ranging"])
        for variant, w in baseline_weights.items():
            try:
                result = run_weighted_backtest(sub_ohlcv, sub_funding, w,
                                               INITIAL_CAPITAL,
                                               _cached_curves=cached_curves)
                score  = objective_function(result, START)
                await save_optimization_result(
                    pool, regime, variant, w, result, score, START,
                )
                summary_rows.append({
                    "regime":   regime,
                    "variant":  variant,
                    "weights":  w,
                    "sharpe":   _safe_float(result.sharpe_ratio),
                    "max_dd":   _safe_float(result.max_drawdown_pct),
                    "annual_ret": _safe_float(result.total_profit_pct * 2.0),
                    "pos_months": _safe_float(
                        compute_positive_months(result.daily_returns, START)
                    ),
                })
            except Exception as exc:
                log.warning("baseline_backtest_failed",
                            regime=regime, variant=variant, error=str(exc))

    # ── 콘솔 비교표 출력 ─────────────────────────────────────────────
    print("\n" + "=" * 110)
    print("Stage 3: 가중치 최적화 결과")
    print("=" * 110)
    print(
        f"{'레짐':<14} {'변형':<22} {'FA':>5} {'DCA':>5} {'Cash':>5} "
        f"{'Sharpe':>8} {'MaxDD%':>8} {'연수익%':>8} {'양수월':>8}"
    )
    print("-" * 100)

    for r in summary_rows:
        fa_w, dca_w, cash_w = r["weights"]
        print(
            f"{r['regime']:<14} {r['variant']:<22} "
            f"{fa_w:>5.2f} {dca_w:>5.2f} {cash_w:>5.2f} "
            f"{r['sharpe']:>8.3f} {r['max_dd']:>8.2f} "
            f"{r['annual_ret']:>8.2f} {r['pos_months']:>8.3f}"
        )

        # 레짐 경계에 구분선
        idx = summary_rows.index(r)
        if idx < len(summary_rows) - 1:
            next_regime = summary_rows[idx + 1]["regime"]
            if next_regime != r["regime"]:
                print("-" * 110)

    print("=" * 110)

    # ── 레짐별 최우수 가중치 요약 ─────────────────────────────────────
    print("\n[레짐별 최우수 가중치 (파라미터 서치)]")
    print(f"{'레짐':<16} {'FA':>6} {'DCA':>6} {'Cash':>6} {'Sharpe':>8}")
    print("-" * 50)
    for r in summary_rows:
        if r["variant"] == "param_search_optimal":
            fa_w, dca_w, cash_w = r["weights"]
            print(
                f"{r['regime']:<16} {fa_w:>6.2f} "
                f"{dca_w:>6.2f} {cash_w:>6.2f} {r['sharpe']:>8.3f}"
            )

    await pool.close()
    log.info("stage3_complete", total_results=len(summary_rows))


if __name__ == "__main__":
    asyncio.run(main())
