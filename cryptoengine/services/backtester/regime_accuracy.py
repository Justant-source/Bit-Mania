"""
Stage 2: 레짐 판별 정확도 테스트

- 후행 확정 레짐(정답지) 생성
- 3개 타임프레임(15m, 1h, 4h)에서 ADX+BB+ATR 기반 레짐 판별
- 4가지 멀티 타임프레임 합의 방법 테스트
- 정확도 + 전환 빈도 + 가짜 전환 비율 → 종합 점수
- regime_accuracy_results 테이블에 저장
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL = "BTCUSDT"
START = datetime(2025, 10, 1, tzinfo=timezone.utc)
END   = datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

REGIME_LABELS = ["volatile", "trending_up", "trending_down", "ranging"]


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


# ---------------------------------------------------------------------------
# 정답 레짐 라벨링 (look-ahead 후행 확정)
# ---------------------------------------------------------------------------

def label_true_regime(df: pd.DataFrame, window: int = 48) -> pd.Series:
    """look-ahead 허용 후행 확정 레짐 라벨.

    window=48 캔들 후의 결과로 현재 레짐 확정:
      - volatility = pct_change().std() * sqrt(252*24)  → 연환산 기준
      - if volatility > 0.8:          "volatile"
      - elif price_change > 0.05:     "trending_up"
      - elif price_change < -0.05:    "trending_down"
      - else:                         "ranging"
    """
    closes = df["close"].values.astype(float)
    n = len(closes)
    regimes = []

    for i in range(n):
        end_idx = min(i + window, n - 1)
        segment = closes[i: end_idx + 1]

        if len(segment) < 2:
            regimes.append("ranging")
            continue

        # 연환산 변동성 (1h 기준: 252*24 periods/year)
        pct_changes = np.diff(segment) / segment[:-1]
        vol = float(np.std(pct_changes) * math.sqrt(252 * 24))

        price_change = (segment[-1] - segment[0]) / segment[0]

        if vol > 0.8:
            regimes.append("volatile")
        elif price_change > 0.05:
            regimes.append("trending_up")
        elif price_change < -0.05:
            regimes.append("trending_down")
        else:
            regimes.append("ranging")

    return pd.Series(regimes, index=df.index, name="true_regime")


# ---------------------------------------------------------------------------
# 지표 계산 유틸
# ---------------------------------------------------------------------------

def _compute_adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                 period: int = 14) -> float:
    """ADX(14) 계산 — rolling 방식."""
    n = len(closes)
    if n < period * 2:
        return 0.0

    plus_dm_list: list[float] = []
    minus_dm_list: list[float] = []
    tr_list: list[float] = []

    for i in range(1, n):
        up   = float(highs[i])   - float(highs[i - 1])
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


# ---------------------------------------------------------------------------
# 단일 타임프레임 레짐 판별
# ---------------------------------------------------------------------------

def detect_regime_single(df: pd.DataFrame) -> pd.Series:
    """ADX + BB + ATR 기반 레짐 판별.

    판별 우선순위:
      1. atr_ratio > 0.03 and adx > 30  → "volatile"
      2. adx > 25 and close > sma20*1.02 → "trending_up"
      3. adx > 25 and close < sma20*0.98 → "trending_down"
      4. else                            → "ranging"
    """
    closes  = df["close"].values.astype(float)
    highs   = df["high"].values.astype(float)  if "high"  in df.columns else closes.copy()
    lows    = df["low"].values.astype(float)   if "low"   in df.columns else closes.copy()

    n = len(closes)
    period_adx = 14
    period_bb  = 20
    period_atr = 14
    warmup     = max(period_adx * 2, period_bb, period_atr * 2)

    regimes = []

    for i in range(n):
        if i < warmup:
            regimes.append("ranging")
            continue

        # ── ADX ─────────────────────────────────────────────────────────
        lo_idx = max(0, i - 100)  # 최대 100봉 사용
        adx = _compute_adx(
            highs[lo_idx: i + 1],
            lows[lo_idx:  i + 1],
            closes[lo_idx: i + 1],
            period_adx,
        )

        # ── BB width ────────────────────────────────────────────────────
        bb_start = max(0, i - period_bb + 1)
        bb_window = closes[bb_start: i + 1]
        sma20 = float(np.mean(bb_window))
        std20 = float(np.std(bb_window))
        bb_width = (4.0 * std20 / sma20) if sma20 > 0 else 1.0

        # ── ATR ratio ───────────────────────────────────────────────────
        atr_start = max(1, i - period_atr + 1)
        tr_vals = []
        for j in range(atr_start, i + 1):
            hl = highs[j] - lows[j]
            hc = abs(highs[j] - closes[j - 1])
            lc = abs(lows[j]  - closes[j - 1])
            tr_vals.append(max(hl, hc, lc))
        atr = float(np.mean(tr_vals)) if tr_vals else 0.0
        atr_ratio = atr / closes[i] if closes[i] > 0 else 0.0

        close_now = closes[i]

        # ── 판별 ────────────────────────────────────────────────────────
        if atr_ratio > 0.03 and adx > 30:
            regimes.append("volatile")
        elif adx > 25 and close_now > sma20 * 1.02:
            regimes.append("trending_up")
        elif adx > 25 and close_now < sma20 * 0.98:
            regimes.append("trending_down")
        else:
            regimes.append("ranging")

    return pd.Series(regimes, index=df.index, name="detected_regime")


# ---------------------------------------------------------------------------
# 리샘플링 (비교를 위해 1h 기준 timestamp로 정렬)
# ---------------------------------------------------------------------------

def resample_to_1h(regime_series: pd.Series, ref_index: pd.DatetimeIndex) -> pd.Series:
    """regime_series를 ref_index(1h) 기준으로 forward-fill 리샘플."""
    # 합집합 인덱스로 reindex 후 ffill
    combined = regime_series.reindex(regime_series.index.union(ref_index))
    combined = combined.ffill()
    return combined.reindex(ref_index)


# ---------------------------------------------------------------------------
# 4가지 멀티 TF 합의 방법
# ---------------------------------------------------------------------------

def method_majority(
    r15m: pd.Series,
    r1h: pd.Series,
    r4h: pd.Series,
) -> pd.Series:
    """다수결: 3개 TF 중 2개 이상 일치하면 그 레짐, 아니면 ranging."""
    result = []
    for v15, v1h, v4h in zip(r15m, r1h, r4h):
        votes: dict[str, int] = {}
        for v in [v15, v1h, v4h]:
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                votes[v] = votes.get(v, 0) + 1
        winner = max(votes, key=lambda k: votes[k]) if votes else "ranging"
        # 과반수 (2/3 이상)
        if votes.get(winner, 0) >= 2:
            result.append(winner)
        else:
            result.append("ranging")
    return pd.Series(result, index=r1h.index, name="majority")


def method_weighted(
    r15m: pd.Series,
    r1h: pd.Series,
    r4h: pd.Series,
) -> pd.Series:
    """가중 투표: weights = (0.2, 0.3, 0.5)."""
    weights = {"15m": 0.2, "1h": 0.3, "4h": 0.5}
    result = []

    for v15, v1h, v4h in zip(r15m, r1h, r4h):
        scores: dict[str, float] = {}
        for regime, w in zip([v15, v1h, v4h], [0.2, 0.3, 0.5]):
            if regime is not None and not (isinstance(regime, float) and math.isnan(regime)):
                scores[regime] = scores.get(regime, 0.0) + w
        winner = max(scores, key=lambda k: scores[k]) if scores else "ranging"
        result.append(winner)

    return pd.Series(result, index=r1h.index, name="weighted")


def method_hierarchical(
    r15m: pd.Series,
    r1h: pd.Series,
    r4h: pd.Series,
) -> pd.Series:
    """계층적 필터: 4h 대분류 → 1h 세분류.

    - 4h가 volatile → volatile (최우선)
    - 4h가 trending_up/trending_down → 1h가 같은 방향이면 그 레짐, 아니면 ranging
    - 4h가 ranging → 1h 결과 사용 (세분류는 1h가 더 정확)
    """
    result = []
    for v4h, v1h in zip(r4h, r1h):
        # None/NaN 처리
        def _clean(v: Any) -> str:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "ranging"
            return str(v)

        c4h = _clean(v4h)
        c1h = _clean(v1h)

        if c4h == "volatile":
            result.append("volatile")
        elif c4h in ("trending_up", "trending_down"):
            if c1h == c4h:
                result.append(c4h)
            else:
                result.append("ranging")
        else:  # c4h == "ranging"
            result.append(c1h)

    return pd.Series(result, index=r1h.index, name="hierarchical")


def method_consensus(
    r15m: pd.Series,
    r1h: pd.Series,
    r4h: pd.Series,
) -> pd.Series:
    """컨센서스 필터: 3개 모두 같으면 확정, 아니면 uncertain→ranging."""
    result = []
    for v15, v1h, v4h in zip(r15m, r1h, r4h):
        def _clean(v: Any) -> str:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "ranging"
            return str(v)

        c15 = _clean(v15)
        c1h_  = _clean(v1h)
        c4h_  = _clean(v4h)

        if c15 == c1h_ == c4h_:
            result.append(c1h_)
        else:
            result.append("ranging")

    return pd.Series(result, index=r1h.index, name="consensus")


# ---------------------------------------------------------------------------
# 평가 지표
# ---------------------------------------------------------------------------

def evaluate(
    predicted: pd.Series,
    true_regime: pd.Series,
    method_name: str,
    timeframe: str = "multi",
) -> dict[str, float]:
    """정확도, 전환 빈도, 가짜 전환 비율, 종합 점수 계산."""
    # 공통 인덱스
    common_idx = predicted.index.intersection(true_regime.index)
    pred = predicted.reindex(common_idx).fillna("ranging")
    true = true_regime.reindex(common_idx).fillna("ranging")

    n = len(pred)
    if n == 0:
        return {
            "method": method_name,
            "timeframe": timeframe,
            "accuracy": 0.0,
            "transition_rate": 0.0,
            "false_transition_rate": 0.0,
            "composite_score": 0.0,
        }

    # ── 정확도 ─────────────────────────────────────────────────────────
    accuracy = float((pred == true).sum()) / n

    # ── 전환 빈도 ───────────────────────────────────────────────────────
    pred_vals = pred.values
    transitions = sum(
        1 for i in range(1, len(pred_vals)) if pred_vals[i] != pred_vals[i - 1]
    )
    transition_rate = transitions / (n - 1) if n > 1 else 0.0

    # ── 가짜 전환 비율 (2캔들 내 원복) ──────────────────────────────────
    false_transitions = 0
    total_transitions = 0
    for i in range(1, len(pred_vals) - 1):
        if pred_vals[i] != pred_vals[i - 1]:
            total_transitions += 1
            # 2캔들 내에 이전 레짐으로 원복?
            if pred_vals[i + 1] == pred_vals[i - 1]:
                false_transitions += 1
    false_transition_rate = (
        false_transitions / total_transitions if total_transitions > 0 else 0.0
    )

    # ── 종합 점수 ────────────────────────────────────────────────────────
    composite_score = (
        accuracy * 0.5
        + (1.0 - transition_rate) * 0.3
        + (1.0 - false_transition_rate) * 0.2
    )

    return {
        "method": method_name,
        "timeframe": timeframe,
        "accuracy": round(accuracy, 6),
        "transition_rate": round(transition_rate, 6),
        "false_transition_rate": round(false_transition_rate, 6),
        "composite_score": round(composite_score, 6),
    }


# ---------------------------------------------------------------------------
# DB 저장
# ---------------------------------------------------------------------------

async def save_result(
    pool: asyncpg.Pool,
    metrics: dict[str, float],
    extra_meta: dict | None = None,
) -> None:
    meta = extra_meta or {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO regime_accuracy_results
                (method, timeframe, accuracy, transition_rate,
                 false_transition_rate, composite_score, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            metrics["method"],
            metrics["timeframe"],
            metrics["accuracy"],
            metrics["transition_rate"],
            metrics["false_transition_rate"],
            metrics["composite_score"],
            json.dumps(meta),
        )
    log.info(
        "saved_regime_result",
        method=metrics["method"],
        timeframe=metrics["timeframe"],
        accuracy=metrics["accuracy"],
        composite=metrics["composite_score"],
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

    log.info("stage2_regime_accuracy_start",
             start=START.isoformat(), end=END.isoformat())

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5,
                                     command_timeout=60)

    # ── 데이터 로드 ─────────────────────────────────────────────────────
    log.info("loading_ohlcv_data")
    df_1h  = await load_ohlcv(pool, SYMBOL, "1h",  START, END)
    df_4h  = await load_ohlcv(pool, SYMBOL, "4h",  START, END)
    df_15m = await load_ohlcv(pool, SYMBOL, "15m", START, END)

    if df_1h.empty:
        log.error("no_1h_data_abort")
        await pool.close()
        return

    log.info("data_loaded",
             bars_1h=len(df_1h),
             bars_4h=len(df_4h),
             bars_15m=len(df_15m))

    # ── 정답 레짐 라벨링 (1h 기준) ────────────────────────────────────
    log.info("labeling_true_regime")
    true_regime = label_true_regime(df_1h, window=48)

    # ── 각 TF별 레짐 판별 ──────────────────────────────────────────────
    log.info("detecting_regimes_per_timeframe")

    regime_1h = detect_regime_single(df_1h)

    regime_4h_raw  = detect_regime_single(df_4h)  if not df_4h.empty  else pd.Series(dtype=str)
    regime_15m_raw = detect_regime_single(df_15m) if not df_15m.empty else pd.Series(dtype=str)

    # 1h 기준으로 리샘플
    ref_idx = df_1h.index
    regime_4h  = resample_to_1h(regime_4h_raw,  ref_idx) if not regime_4h_raw.empty  else pd.Series("ranging", index=ref_idx)
    regime_15m = resample_to_1h(regime_15m_raw, ref_idx) if not regime_15m_raw.empty else pd.Series("ranging", index=ref_idx)

    # NaN → "ranging" 채우기
    regime_4h  = regime_4h.fillna("ranging")
    regime_15m = regime_15m.fillna("ranging")
    regime_1h  = regime_1h.fillna("ranging")

    # ── 4가지 멀티 TF 방법 ────────────────────────────────────────────
    log.info("computing_multi_tf_methods")
    regime_majority     = method_majority(regime_15m, regime_1h, regime_4h)
    regime_weighted     = method_weighted(regime_15m, regime_1h, regime_4h)
    regime_hierarchical = method_hierarchical(regime_15m, regime_1h, regime_4h)
    regime_consensus    = method_consensus(regime_15m, regime_1h, regime_4h)

    # ── 평가 ────────────────────────────────────────────────────────────
    log.info("evaluating_all_methods")
    all_results: list[dict] = []

    # 단독 TF 3개
    all_results.append(evaluate(regime_15m, true_regime, "single_15m", "15m"))
    all_results.append(evaluate(regime_1h,  true_regime, "single_1h",  "1h"))
    all_results.append(evaluate(
        regime_4h, true_regime, "single_4h", "4h"
    ))

    # 멀티 TF 4가지
    all_results.append(evaluate(regime_majority,     true_regime, "multi_majority",     "multi"))
    all_results.append(evaluate(regime_weighted,     true_regime, "multi_weighted",     "multi"))
    all_results.append(evaluate(regime_hierarchical, true_regime, "multi_hierarchical", "multi"))
    all_results.append(evaluate(regime_consensus,    true_regime, "multi_consensus",    "multi"))

    # ── 기존 결과 삭제 후 DB 저장 ────────────────────────────────────
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM regime_accuracy_results")
    log.info("cleared_previous_regime_results")

    for metrics in all_results:
        await save_result(pool, metrics)

    # ── 콘솔 비교표 출력 ─────────────────────────────────────────────
    print("\n" + "=" * 90)
    print("Stage 2: 레짐 판별 정확도 결과")
    print("=" * 90)
    header = f"{'방법':<25} {'TF':<8} {'정확도':>8} {'전환율':>8} {'가짜전환':>8} {'종합점수':>10}"
    print(header)
    print("-" * 90)

    sorted_results = sorted(all_results, key=lambda x: x["composite_score"], reverse=True)
    for r in sorted_results:
        print(
            f"{r['method']:<25} {r['timeframe']:<8} "
            f"{r['accuracy']:>8.4f} {r['transition_rate']:>8.4f} "
            f"{r['false_transition_rate']:>8.4f} {r['composite_score']:>10.4f}"
        )

    best = sorted_results[0]
    print("=" * 90)
    print(f"최우수 방법: {best['method']}  (종합점수: {best['composite_score']:.4f}, 정확도: {best['accuracy']:.4f})")
    print("=" * 90)

    await pool.close()
    log.info("stage2_complete")


if __name__ == "__main__":
    asyncio.run(main())
