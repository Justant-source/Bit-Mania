#!/usr/bin/env python3
"""monthly_wf_runner.py — 월간 Walk-Forward 자동 실행 파이프라인.

매월 1일 새벽 2시(KST)에 Walk-Forward 분석을 실행하여:
  1. 최근 6개월 데이터로 Walk-Forward 분석 (train=120d, test=60d, 3개 윈도우)
  2. 이전 달 결과와 비교하여 파라미터·지표 변화 감지
  3. 결과를 DB(walk_forward_monthly 테이블)에 저장
  4. Redis ce:alerts:daily_report 채널에 요약 PUBLISH (Telegram 전달)

실행:
    # Docker 내부 (자동 스케줄)
    python scripts/monthly_wf_runner.py

    # 즉시 실행 (테스트)
    WF_ON_STARTUP=true python scripts/monthly_wf_runner.py

환경변수:
    DB_HOST, DB_PASSWORD, DB_USER, DB_PORT, DB_NAME
    REDIS_URL   (기본: redis://:RedisPass2026!@redis:6379)
    WF_SYMBOL   (기본: BTCUSDT)
    WF_CAPITAL  (기본: 10000.0)
    WF_LOOKBACK_DAYS  (기본: 180 — 분석할 최근 일수)
    WF_TRAIN_DAYS     (기본: 120)
    WF_TEST_DAYS      (기본: 60)
    WF_MC_RUNS        (기본: 200)
    MONTHLY_WF_CRON   (기본: 0 17 1 * * — 매월 1일 02:00 KST)
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

# ── PYTHONPATH 보정 ──────────────────────────────────────────────────────────
sys.path.insert(0, "/app")

try:
    import asyncpg
except ImportError:
    print("[FATAL] asyncpg 패키지 없음. pip install asyncpg")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("[FATAL] pandas 패키지 없음. pip install pandas")
    sys.exit(1)

try:
    import redis.asyncio as aioredis
except ImportError:
    print("[FATAL] redis 패키지 없음. pip install redis")
    sys.exit(1)

try:
    import structlog
    from shared.timezone_utils import kst_timestamper
    _HAS_STRUCTLOG = True
except ImportError:
    _HAS_STRUCTLOG = False

# core 유틸
from tests.backtest.core import (
    load_ohlcv,
    load_funding,
    make_pool,
    save_result,
    FAEngine,
    sharpe,
    mdd,
    cagr,
    safe_float,
)

# ── 환경변수 ─────────────────────────────────────────────────────────────────
SYMBOL          = os.getenv("WF_SYMBOL", "BTCUSDT")
CAPITAL         = float(os.getenv("WF_CAPITAL", "10000.0"))
LOOKBACK_DAYS   = int(os.getenv("WF_LOOKBACK_DAYS", "180"))
TRAIN_DAYS      = int(os.getenv("WF_TRAIN_DAYS", "120"))
TEST_DAYS       = int(os.getenv("WF_TEST_DAYS", "60"))
MC_RUNS         = int(os.getenv("WF_MC_RUNS", "200"))
REDIS_URL       = os.getenv("REDIS_URL", "redis://:RedisPass2026!@redis:6379")

# FA 전략 파라미터 (현재 채택 설정: fa80_lev5_r30)
FA_CAPITAL_RATIO    = float(os.getenv("WF_FA_RATIO", "0.80"))
FA_LEVERAGE         = float(os.getenv("WF_LEVERAGE", "5.0"))
FA_REINVEST_RATIO   = float(os.getenv("WF_REINVEST", "0.30"))
FA_MIN_FUNDING_RATE = float(os.getenv("WF_MIN_FUNDING", "0.0001"))

# 결과 저장 테이블
WF_TABLE = "walk_forward_monthly"

# Sharpe 기준선 (이 이하로 떨어지면 경고)
SHARPE_ALERT_THRESHOLD = 1.5

# ── DDL ──────────────────────────────────────────────────────────────────────

CREATE_WF_TABLE = f"""
CREATE TABLE IF NOT EXISTS {WF_TABLE} (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    period_label    TEXT        NOT NULL,   -- "2026-04" 형태 (실행월)
    data_start      TIMESTAMPTZ NOT NULL,
    data_end        TIMESTAMPTZ NOT NULL,
    n_windows       INTEGER     NOT NULL,
    agg_sharpe      DOUBLE PRECISION,
    agg_profit_pct  DOUBLE PRECISION,
    agg_mdd_pct     DOUBLE PRECISION,
    agg_win_rate    DOUBLE PRECISION,
    agg_total_trades INTEGER,
    consistency_ratio DOUBLE PRECISION,
    mc_sharpe_mean  DOUBLE PRECISION,
    mc_sharpe_ci_lo DOUBLE PRECISION,
    mc_sharpe_ci_hi DOUBLE PRECISION,
    mc_win_prob     DOUBLE PRECISION,
    sharpe_alert    BOOLEAN     NOT NULL DEFAULT FALSE,
    windows_json    JSONB,
    params          JSONB
);
"""

# ── 로깅 설정 ─────────────────────────────────────────────────────────────────

def _configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    if _HAS_STRUCTLOG:
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                kst_timestamper,
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )


def _log(msg: str, **kw: Any) -> None:
    if _HAS_STRUCTLOG:
        structlog.get_logger(__name__).info(msg, **kw)
    else:
        parts = " ".join(f"{k}={v}" for k, v in kw.items())
        logging.getLogger(__name__).info(f"{msg} {parts}".strip())


def _warn(msg: str, **kw: Any) -> None:
    if _HAS_STRUCTLOG:
        structlog.get_logger(__name__).warning(msg, **kw)
    else:
        parts = " ".join(f"{k}={v}" for k, v in kw.items())
        logging.getLogger(__name__).warning(f"{msg} {parts}".strip())


# ── Walk-Forward 단순 구현 (core/engine.FAEngine 기반) ───────────────────────

def _run_window(
    ohlcv: "pd.DataFrame",
    funding: "pd.DataFrame",
    train_start: datetime,
    train_end: datetime,
    test_start: datetime,
    test_end: datetime,
) -> dict[str, Any]:
    """단일 윈도우 train/test 백테스트 실행 후 OOS 결과 반환."""
    def _slice(df: "pd.DataFrame", s: datetime, e: datetime) -> "pd.DataFrame":
        if df.empty:
            return df
        return df.loc[s:e]

    train_ohlcv   = _slice(ohlcv, train_start, train_end)
    test_ohlcv    = _slice(ohlcv, test_start, test_end)
    train_funding = _slice(funding, train_start, train_end)
    test_funding  = _slice(funding, test_start, test_end)

    if train_ohlcv.empty or test_ohlcv.empty:
        return {}

    # Train 구간 실행 (파라미터 확인용)
    train_eng = FAEngine(
        train_ohlcv, train_funding,
        initial_capital=CAPITAL,
        fa_capital_ratio=FA_CAPITAL_RATIO,
        leverage=FA_LEVERAGE,
        reinvest_ratio=FA_REINVEST_RATIO,
        min_funding_rate=FA_MIN_FUNDING_RATE,
    )
    train_res = train_eng.run()

    # Test(OOS) 구간 실행
    test_eng = FAEngine(
        test_ohlcv, test_funding,
        initial_capital=CAPITAL,
        fa_capital_ratio=FA_CAPITAL_RATIO,
        leverage=FA_LEVERAGE,
        reinvest_ratio=FA_REINVEST_RATIO,
        min_funding_rate=FA_MIN_FUNDING_RATE,
    )
    test_res = test_eng.run()

    equity_series = pd.Series(test_res.get("equity_curve", [CAPITAL]))

    return {
        "train_label": f"{train_start:%Y-%m-%d}..{train_end:%Y-%m-%d}",
        "test_label":  f"{test_start:%Y-%m-%d}..{test_end:%Y-%m-%d}",
        "train_profit_pct": safe_float(train_res.get("total_profit_pct", 0.0)),
        "train_sharpe":     safe_float(train_res.get("sharpe_ratio", 0.0)),
        "test_profit_pct":  safe_float(test_res.get("total_profit_pct", 0.0)),
        "test_sharpe":      safe_float(sharpe(equity_series)),
        "test_mdd_pct":     safe_float(mdd(equity_series)),
        "test_trades":      int(test_res.get("trades", 0)),
        "test_win_rate":    safe_float(test_res.get("win_rate", 0.0)),
        "is_positive":      test_res.get("total_profit_pct", 0.0) > 0,
    }


def _monte_carlo(windows: list[dict], n: int = MC_RUNS) -> dict[str, float]:
    """OOS 수익률 시뮬레이션으로 Sharpe 신뢰구간 추정."""
    import random
    profits = [w["test_profit_pct"] for w in windows if w]
    if not profits:
        return {"sharpe_mean": 0.0, "ci_lo": 0.0, "ci_hi": 0.0, "win_prob": 0.0}

    sims: list[float] = []
    for _ in range(n):
        sampled = random.choices(profits, k=len(profits))
        mean_r = sum(sampled) / len(sampled)
        std_r  = (sum((r - mean_r) ** 2 for r in sampled) / max(len(sampled) - 1, 1)) ** 0.5
        sr = (mean_r / std_r) if std_r > 0 else 0.0
        sims.append(sr)

    sims.sort()
    n_sim = len(sims)
    mean  = sum(sims) / n_sim
    ci_lo = sims[max(int(n_sim * 0.025), 0)]
    ci_hi = sims[min(int(n_sim * 0.975), n_sim - 1)]
    win_p = sum(1 for p in profits if p > 0) / len(profits)

    return {
        "sharpe_mean": round(mean, 4),
        "ci_lo":       round(ci_lo, 4),
        "ci_hi":       round(ci_hi, 4),
        "win_prob":    round(win_p, 4),
    }


# ── DB 저장 ──────────────────────────────────────────────────────────────────

async def _save_wf_result(
    pool: asyncpg.Pool,
    period_label: str,
    data_start: datetime,
    data_end: datetime,
    windows: list[dict],
    mc: dict[str, float],
    sharpe_alert: bool,
) -> None:
    valid = [w for w in windows if w]
    n = len(valid)
    if n == 0:
        return

    agg_sharpe   = sum(w["test_sharpe"]     for w in valid) / n
    agg_profit   = sum(w["test_profit_pct"] for w in valid) / n
    agg_mdd      = min(w["test_mdd_pct"]    for w in valid)
    agg_win_rate = sum(w["test_win_rate"]   for w in valid) / n
    agg_trades   = sum(w["test_trades"]     for w in valid)
    consistency  = sum(1 for w in valid if w["is_positive"]) / n

    params = {
        "fa_capital_ratio": FA_CAPITAL_RATIO,
        "leverage":         FA_LEVERAGE,
        "reinvest_ratio":   FA_REINVEST_RATIO,
        "min_funding_rate": FA_MIN_FUNDING_RATE,
        "symbol":           SYMBOL,
        "lookback_days":    LOOKBACK_DAYS,
        "train_days":       TRAIN_DAYS,
        "test_days":        TEST_DAYS,
    }

    async with pool.acquire() as conn:
        await conn.execute(CREATE_WF_TABLE)
        await conn.execute(
            f"""
            INSERT INTO {WF_TABLE} (
                period_label, data_start, data_end,
                n_windows, agg_sharpe, agg_profit_pct, agg_mdd_pct,
                agg_win_rate, agg_total_trades, consistency_ratio,
                mc_sharpe_mean, mc_sharpe_ci_lo, mc_sharpe_ci_hi, mc_win_prob,
                sharpe_alert, windows_json, params
            ) VALUES (
                $1, $2, $3,
                $4, $5, $6, $7,
                $8, $9, $10,
                $11, $12, $13, $14,
                $15, $16::jsonb, $17::jsonb
            )
            """,
            period_label, data_start, data_end,
            n,
            round(agg_sharpe, 4), round(agg_profit, 4), round(agg_mdd, 4),
            round(agg_win_rate, 4), agg_trades, round(consistency, 4),
            mc["sharpe_mean"], mc["ci_lo"], mc["ci_hi"], mc["win_prob"],
            sharpe_alert,
            json.dumps(valid),
            json.dumps(params),
        )
    _log("wf_result_saved", period=period_label, n_windows=n)


# ── 이전 결과 로드 및 비교 ────────────────────────────────────────────────────

async def _load_previous_result(pool: asyncpg.Pool) -> dict | None:
    """직전 월 Walk-Forward 결과 로드."""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT period_label, agg_sharpe, agg_profit_pct, agg_mdd_pct,
                       consistency_ratio, mc_win_prob, params
                FROM {WF_TABLE}
                ORDER BY run_at DESC
                LIMIT 1
                """
            )
        if not row:
            return None
        return dict(row)
    except Exception:
        return None


def _build_comparison_text(
    current: dict[str, Any],
    prev: dict | None,
) -> str:
    """현재 결과와 이전 결과를 비교하는 텍스트 생성."""
    lines = []
    lines.append(f"📊 *Walk-Forward 월간 리포트*")
    lines.append(f"기간: {current['period_label']}")
    lines.append(f"데이터: {current['data_start']:%Y-%m-%d} ~ {current['data_end']:%Y-%m-%d}")
    lines.append(f"윈도우 수: {current['n_windows']}")
    lines.append("")

    def _delta(label: str, cur: float, old: float | None, higher_is_better: bool = True) -> str:
        if old is None:
            return f"  {label}: {cur:+.4f} (이전 없음)"
        diff = cur - old
        arrow = "↑" if (diff > 0) == higher_is_better else "↓"
        sign  = "+" if diff > 0 else ""
        return f"  {label}: {cur:.4f} ({sign}{diff:.4f} {arrow})"

    lines.append("*[OOS 집계 성과]*")
    lines.append(_delta("Sharpe",       current["agg_sharpe"],   prev["agg_sharpe"] if prev else None))
    lines.append(_delta("수익률%",      current["agg_profit"],   prev["agg_profit"] if prev else None))
    lines.append(_delta("MDD%",         current["agg_mdd"],      prev["agg_mdd"] if prev else None, higher_is_better=False))
    lines.append(_delta("Consistency",  current["consistency"],  prev["consistency_ratio"] if prev else None))
    lines.append(_delta("MC 승률",      current["mc_win_prob"],  prev["mc_win_prob"] if prev else None))
    lines.append("")

    if current["sharpe_alert"]:
        lines.append(f"⚠️  Sharpe {current['agg_sharpe']:.4f} < {SHARPE_ALERT_THRESHOLD} — 파라미터 재검토 권장!")
        lines.append("")

    # 파라미터 비교
    if prev and prev.get("params"):
        prev_params = prev["params"] if isinstance(prev["params"], dict) else json.loads(prev["params"])
        cur_params  = current["params"]
        changes = []
        for k in ["fa_capital_ratio", "leverage", "reinvest_ratio", "min_funding_rate"]:
            pv = prev_params.get(k)
            cv = cur_params.get(k)
            if pv is not None and cv is not None and abs(pv - cv) > 1e-9:
                changes.append(f"  {k}: {pv} → {cv}")
        if changes:
            lines.append("*[파라미터 변경]*")
            lines.extend(changes)
        else:
            lines.append("파라미터 변경 없음 (현재 fa80_lev5_r30 유지)")
    else:
        lines.append(f"파라미터: FA={FA_CAPITAL_RATIO*100:.0f}% Lev={FA_LEVERAGE}x Reinv={FA_REINVEST_RATIO*100:.0f}%")

    lines.append("")
    lines.append("⚡ 파라미터 자동 변경 안 함 — 수동 검토 후 적용 요망")

    return "\n".join(lines)


# ── Redis 알림 발행 ───────────────────────────────────────────────────────────

async def _publish_alert(text: str) -> None:
    """Redis ce:alerts:daily_report 채널에 Telegram 알림 PUBLISH."""
    try:
        r = await aioredis.from_url(REDIS_URL, decode_responses=True)
        payload = json.dumps({
            "type": "walk_forward_monthly",
            "text": text,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        n = await r.publish("ce:alerts:daily_report", payload)
        await r.aclose()
        _log("alert_published", channel="ce:alerts:daily_report", subscribers=n)
    except Exception as exc:
        _warn("alert_publish_failed", error=str(exc))


# ── 메인 실행 로직 ────────────────────────────────────────────────────────────

async def run_monthly_wf() -> None:
    """Walk-Forward 월간 분석 전체 실행 흐름."""
    _configure_logging()

    now_utc  = datetime.now(timezone.utc)
    end_dt   = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    period_label = now_utc.strftime("%Y-%m")

    _log(
        "monthly_wf_start",
        period=period_label,
        symbol=SYMBOL,
        lookback_days=LOOKBACK_DAYS,
        train_days=TRAIN_DAYS,
        test_days=TEST_DAYS,
    )

    pool = await make_pool()

    # ── 데이터 로드 ────────────────────────────────────────────────────────────
    _log("loading_data", start=str(start_dt.date()), end=str(end_dt.date()))
    ohlcv   = await load_ohlcv(pool, SYMBOL, "1h", start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        _warn("no_ohlcv_data", symbol=SYMBOL, start=str(start_dt.date()))
        await pool.close()
        await _publish_alert(f"⚠️ Walk-Forward 실패: {SYMBOL} OHLCV 데이터 없음 ({start_dt.date()} ~ {end_dt.date()})")
        return

    _log("data_loaded", ohlcv_rows=len(ohlcv), funding_rows=len(funding))

    # ── 슬라이딩 윈도우 실행 ───────────────────────────────────────────────────
    windows: list[dict] = []
    cursor = start_dt
    window_no = 0

    while True:
        train_start = cursor
        train_end   = train_start + timedelta(days=TRAIN_DAYS)
        test_start  = train_end
        test_end    = test_start + timedelta(days=TEST_DAYS)

        if test_end > end_dt:
            break

        window_no += 1
        _log(
            "wf_window_running",
            window=window_no,
            train=f"{train_start:%Y-%m-%d}..{train_end:%Y-%m-%d}",
            test=f"{test_start:%Y-%m-%d}..{test_end:%Y-%m-%d}",
        )

        result = _run_window(ohlcv, funding, train_start, train_end, test_start, test_end)
        if result:
            windows.append(result)
            _log(
                "wf_window_done",
                window=window_no,
                test_profit=round(result["test_profit_pct"], 4),
                test_sharpe=round(result["test_sharpe"], 4),
                test_mdd=round(result["test_mdd_pct"], 4),
            )
        else:
            _warn("wf_window_skipped", window=window_no, reason="insufficient_data")

        cursor += timedelta(days=TEST_DAYS)

    if not windows:
        _warn("no_valid_windows", lookback_days=LOOKBACK_DAYS, train_days=TRAIN_DAYS)
        await pool.close()
        await _publish_alert(f"⚠️ Walk-Forward 실패: 유효한 윈도우 없음 (lookback={LOOKBACK_DAYS}d, train={TRAIN_DAYS}d)")
        return

    # ── 집계 ──────────────────────────────────────────────────────────────────
    n          = len(windows)
    agg_sharpe = sum(w["test_sharpe"]     for w in windows) / n
    agg_profit = sum(w["test_profit_pct"] for w in windows) / n
    agg_mdd    = min(w["test_mdd_pct"]    for w in windows)
    consistency = sum(1 for w in windows if w["is_positive"]) / n
    sharpe_alert = agg_sharpe < SHARPE_ALERT_THRESHOLD

    mc = _monte_carlo(windows)

    _log(
        "monthly_wf_aggregate",
        n_windows=n,
        agg_sharpe=round(agg_sharpe, 4),
        agg_profit=round(agg_profit, 4),
        agg_mdd=round(agg_mdd, 4),
        consistency=round(consistency, 4),
        mc_win_prob=mc["win_prob"],
        sharpe_alert=sharpe_alert,
    )

    if sharpe_alert:
        _warn(
            "sharpe_below_threshold",
            sharpe=round(agg_sharpe, 4),
            threshold=SHARPE_ALERT_THRESHOLD,
        )

    # ── 이전 결과 로드 ─────────────────────────────────────────────────────────
    prev = await _load_previous_result(pool)

    # ── DB 저장 ────────────────────────────────────────────────────────────────
    current_summary = {
        "period_label":  period_label,
        "data_start":    start_dt,
        "data_end":      end_dt,
        "n_windows":     n,
        "agg_sharpe":    round(agg_sharpe, 4),
        "agg_profit":    round(agg_profit, 4),
        "agg_mdd":       round(agg_mdd, 4),
        "consistency":   round(consistency, 4),
        "mc_win_prob":   mc["win_prob"],
        "sharpe_alert":  sharpe_alert,
        "params": {
            "fa_capital_ratio": FA_CAPITAL_RATIO,
            "leverage":         FA_LEVERAGE,
            "reinvest_ratio":   FA_REINVEST_RATIO,
            "min_funding_rate": FA_MIN_FUNDING_RATE,
            "symbol":           SYMBOL,
            "lookback_days":    LOOKBACK_DAYS,
            "train_days":       TRAIN_DAYS,
            "test_days":        TEST_DAYS,
        },
    }

    await _save_wf_result(
        pool,
        period_label=period_label,
        data_start=start_dt,
        data_end=end_dt,
        windows=windows,
        mc=mc,
        sharpe_alert=sharpe_alert,
    )

    await pool.close()

    # ── 알림 발행 ──────────────────────────────────────────────────────────────
    alert_text = _build_comparison_text(current_summary, prev)
    await _publish_alert(alert_text)

    _log("monthly_wf_complete", period=period_label, windows=n)


if __name__ == "__main__":
    try:
        asyncio.run(run_monthly_wf())
    except KeyboardInterrupt:
        pass
