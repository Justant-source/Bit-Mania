"""bt_dca_v2_redesign.py — Test I: DCA v2 재설계 (RSI + EMA 이중 조건) 백테스트.

변형:
  1. baseline_ref     : Test C의 graduated (기준선, 비교용)
  2. rsi_ema_dual      : RSI(14)<40 + EMA200 이중 조건, RSI 깊이별 크기 조정
  3. rsi_ema_consec    : rsi_ema_dual + 2연속 봉 조건 (노이즈 제거)
  4. rsi_trend_guard   : RSI + EMA 구조(EMA50 > EMA200) 확인 + 넓은 TP
  5. rsi_macd_momentum : RSI + MACD 히스토그램 전환 확인
  6. adaptive_v2       : RSI 3단계 크기 + 2연속 + SL 강화 종합판

기간: 2020-04-01 ~ 2026-03-31 (6년)
Walk-Forward: 최우수 변형 대상 (train=180d, test=90d, 22개 윈도우)
저장: strategy_variant_results 테이블 (test_name="test_i_dca_v2_redesign")
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

INITIAL_CAPITAL = 10_000.0
FEE_RATE = 0.00055        # Bybit taker
MAKER_FEE = 0.00020       # Bybit maker (Post-Only 목표)
SYMBOL = "BTCUSDT"
TIMEFRAME = "1h"
START_DATE = "2020-04-01"
END_DATE = "2026-03-31"
WARMUP_BARS = 300         # MACD(26) + EMA200 + 여유

# WF 설정
WF_TRAIN_DAYS = 180
WF_TEST_DAYS = 90

# 서브기간
SUB_PERIODS = {
    "bull_2020_21": ("2020-04-01", "2021-11-30"),
    "bear_2022":    ("2021-12-01", "2022-12-31"),
    "bull_2023_24": ("2023-01-01", "2024-12-31"),
    "bear_2025h2":  ("2025-10-01", "2026-03-31"),
}

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

CREATE_VARIANT_RESULTS = """
CREATE TABLE IF NOT EXISTS strategy_variant_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    test_name       TEXT        NOT NULL,
    variant         TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    start_date      TIMESTAMPTZ NOT NULL,
    end_date        TIMESTAMPTZ NOT NULL,
    initial_capital DOUBLE PRECISION NOT NULL,
    final_equity    DOUBLE PRECISION NOT NULL,
    total_return    DOUBLE PRECISION NOT NULL,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    win_rate        DOUBLE PRECISION,
    total_trades    INTEGER,
    metadata        JSONB
);
"""

CREATE_WF_RESULTS = """
CREATE TABLE IF NOT EXISTS walk_forward_results (
    id                SERIAL PRIMARY KEY,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id            TEXT        NOT NULL,
    strategy          TEXT        NOT NULL,
    window_id         INTEGER     NOT NULL,
    train_start       TIMESTAMPTZ,
    train_end         TIMESTAMPTZ,
    test_start        TIMESTAMPTZ,
    test_end          TIMESTAMPTZ,
    train_sharpe      DOUBLE PRECISION,
    train_return      DOUBLE PRECISION,
    train_max_dd      DOUBLE PRECISION,
    train_trades      INTEGER,
    test_sharpe       DOUBLE PRECISION,
    test_return       DOUBLE PRECISION,
    test_max_dd       DOUBLE PRECISION,
    test_trades       INTEGER,
    aggregate_sharpe  DOUBLE PRECISION,
    aggregate_return  DOUBLE PRECISION,
    aggregate_max_dd  DOUBLE PRECISION,
    consistency_ratio DOUBLE PRECISION,
    sharpe_alert      BOOLEAN,
    monte_carlo       JSONB
);
"""


# ── 지표 계산 ──────────────────────────────────────────────────────────────────

def _ema(closes: np.ndarray, period: int) -> float:
    """EMA 마지막 값 반환."""
    if len(closes) == 0:
        return 0.0
    k = 2.0 / (period + 1)
    val = float(closes[0])
    for c in closes[1:]:
        val = float(c) * k + val * (1.0 - k)
    return val


def _ema_series(closes: np.ndarray, period: int) -> np.ndarray:
    """EMA 전체 시리즈 반환."""
    if len(closes) == 0:
        return np.array([])
    k = 2.0 / (period + 1)
    result = np.empty(len(closes))
    result[0] = float(closes[0])
    for i in range(1, len(closes)):
        result[i] = float(closes[i]) * k + result[i - 1] * (1.0 - k)
    return result


def _rsi(closes: np.ndarray, period: int = 14) -> float:
    """RSI(period) 마지막 값 반환 (Wilder 방식)."""
    if len(closes) < period + 1:
        return 50.0
    diffs = np.diff(closes.astype(float))
    gains = np.where(diffs > 0, diffs, 0.0)
    losses = np.where(diffs < 0, -diffs, 0.0)

    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    for i in range(period, len(diffs)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _macd_hist(closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float]:
    """MACD 히스토그램 현재값과 이전값 반환 (hist_now, hist_prev)."""
    if len(closes) < slow + signal:
        return 0.0, 0.0
    ema_fast = _ema_series(closes, fast)
    ema_slow = _ema_series(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema_series(macd_line, signal)
    hist = macd_line - signal_line
    if len(hist) < 2:
        return 0.0, 0.0
    return float(hist[-1]), float(hist[-2])


# ── 신호 함수 ──────────────────────────────────────────────────────────────────
# 반환: None | "buy" | ("buy", capital_ratio) | "close"

def signal_baseline_ref(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """baseline_ref: Test C graduated 그대로 (비교 기준선).

    EMA50 위 → 100%, EMA50~EMA200 → 50%, EMA200 아래 → 스킵
    TP +5%, SL -3%
    """
    closes = lookback["close"].values.astype(float)
    ema50 = _ema(closes[-50:] if len(closes) >= 50 else closes, 50)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    price = float(bar["close"])

    if idx % 24 == 0 and position is None:
        if price > ema50:
            return ("buy", 1.0)
        elif price > ema200:
            return ("buy", 0.5)
    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.05 or pct <= -0.03:
            return "close"
    return None


def signal_rsi_ema_dual(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """rsi_ema_dual: RSI(14)<40 AND price>EMA200 이중 조건.

    RSI 깊이별 크기:
      RSI < 25 → 100%
      25 ≤ RSI < 35 → 75%
      35 ≤ RSI < 40 → 50%
    TP +5%, SL -1.5% (더 타이트)
    24봉(일) 쿨다운 유지
    """
    closes = lookback["close"].values.astype(float)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    rsi = _rsi(closes[-30:] if len(closes) >= 30 else closes, 14)
    price = float(bar["close"])

    cooldown = state.get("last_buy_idx", -999)
    if idx % 24 == 0 and position is None and (idx - cooldown) >= 24:
        if rsi < 40 and price > ema200:
            if rsi < 25:
                ratio = 1.0
            elif rsi < 35:
                ratio = 0.75
            else:
                ratio = 0.5
            state["last_buy_idx"] = idx
            return ("buy", ratio)

    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.05 or pct <= -0.015:
            return "close"
    return None


def signal_rsi_ema_consec(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """rsi_ema_consec: rsi_ema_dual + 2연속 봉 조건 (노이즈 감소).

    2개 연속 봉에서 RSI < 40 AND price > EMA200 확인 후 진입.
    TP +5%, SL -1.5%
    """
    closes = lookback["close"].values.astype(float)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    rsi = _rsi(closes[-30:] if len(closes) >= 30 else closes, 14)
    price = float(bar["close"])

    # 연속 조건 카운터
    cond_met = (rsi < 40 and price > ema200)
    if cond_met:
        state["consec_count"] = state.get("consec_count", 0) + 1
    else:
        state["consec_count"] = 0

    cooldown = state.get("last_buy_idx", -999)
    if position is None and state.get("consec_count", 0) >= 2 and (idx - cooldown) >= 24:
        if rsi < 25:
            ratio = 1.0
        elif rsi < 35:
            ratio = 0.75
        else:
            ratio = 0.5
        state["last_buy_idx"] = idx
        state["consec_count"] = 0
        return ("buy", ratio)

    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.05 or pct <= -0.015:
            return "close"
    return None


def signal_rsi_trend_guard(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """rsi_trend_guard: RSI + EMA 구조 확인 (EMA50 > EMA200 = 상승 추세).

    진입: RSI(14) < 40 AND price > EMA200 AND EMA50 > EMA200
    크기: RSI < 30 → 100%, RSI 30-40 → 60%
    TP +7%, SL -2%
    """
    closes = lookback["close"].values.astype(float)
    ema50 = _ema(closes[-50:] if len(closes) >= 50 else closes, 50)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    rsi = _rsi(closes[-30:] if len(closes) >= 30 else closes, 14)
    price = float(bar["close"])

    cooldown = state.get("last_buy_idx", -999)
    if idx % 24 == 0 and position is None and (idx - cooldown) >= 24:
        if rsi < 40 and price > ema200 and ema50 > ema200:
            ratio = 1.0 if rsi < 30 else 0.6
            state["last_buy_idx"] = idx
            return ("buy", ratio)

    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.07 or pct <= -0.02:
            return "close"
    return None


def signal_rsi_macd_momentum(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """rsi_macd_momentum: RSI + MACD 히스토그램 상승 전환 확인.

    진입: RSI(14) < 45 AND MACD 히스트 음→양 전환 AND price > EMA200
    크기: RSI < 35 → 100%, RSI 35-45 → 70%
    TP +6%, SL -2%
    """
    closes = lookback["close"].values.astype(float)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    rsi = _rsi(closes[-30:] if len(closes) >= 30 else closes, 14)
    hist_now, hist_prev = _macd_hist(closes)
    price = float(bar["close"])

    # MACD 히스토그램 상승 전환: 이전 음수, 현재 양수
    macd_cross = (hist_prev < 0 and hist_now >= 0)

    cooldown = state.get("last_buy_idx", -999)
    if position is None and macd_cross and rsi < 45 and price > ema200 and (idx - cooldown) >= 24:
        ratio = 1.0 if rsi < 35 else 0.7
        state["last_buy_idx"] = idx
        return ("buy", ratio)

    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.06 or pct <= -0.02:
            return "close"
    return None


def signal_adaptive_v2(
    bar: Any,
    lookback: pd.DataFrame,
    idx: int,
    position: dict | None,
    state: dict,
) -> Any:
    """adaptive_v2: RSI 3단계 + 2연속 + SL 강화 종합판.

    진입: RSI(14) < 40 AND price > EMA200 AND 2연속 조건
    크기: RSI < 20 → 100%, 20-30 → 80%, 30-40 → 60%
    TP +8%, SL -1.5%
    추가 조건: 급락 과매도(RSI < 20)에서는 연속 조건 면제 (즉시 진입)
    """
    closes = lookback["close"].values.astype(float)
    ema200 = _ema(closes[-200:] if len(closes) >= 200 else closes, 200)
    rsi = _rsi(closes[-30:] if len(closes) >= 30 else closes, 14)
    price = float(bar["close"])

    cond_met = (rsi < 40 and price > ema200)
    if cond_met:
        state["consec_count"] = state.get("consec_count", 0) + 1
    else:
        state["consec_count"] = 0

    cooldown = state.get("last_buy_idx", -999)
    # 극단 과매도: 즉시 진입 (2연속 면제)
    extreme_oversold = (rsi < 20 and price > ema200)

    if position is None and (idx - cooldown) >= 24:
        should_enter = extreme_oversold or (cond_met and state.get("consec_count", 0) >= 2)
        if should_enter:
            if rsi < 20:
                ratio = 1.0
            elif rsi < 30:
                ratio = 0.8
            else:
                ratio = 0.6
            state["last_buy_idx"] = idx
            state["consec_count"] = 0
            return ("buy", ratio)

    if position is not None:
        pct = (price - position["entry_price"]) / position["entry_price"]
        if pct >= 0.08 or pct <= -0.015:
            return "close"
    return None


# ── 백테스트 엔진 ──────────────────────────────────────────────────────────────

class SimpleBacktester:
    """이벤트 루프 기반 단순 백테스터 (상태 딕셔너리 지원)."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
        warmup: int = WARMUP_BARS,
    ) -> None:
        self.df = ohlcv.reset_index() if ohlcv.index.name else ohlcv.copy()
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.fee_rate = fee_rate
        self.warmup = warmup
        self.position: dict | None = None
        self.trades: list[dict] = []
        self.equity_curve: list[float] = [initial_capital]

    def run(self, signal_fn) -> dict:
        bars = self.df
        state: dict = {}
        for idx in range(self.warmup, len(bars)):
            bar = bars.iloc[idx]
            lookback = bars.iloc[max(0, idx - 300): idx + 1]
            signal = signal_fn(bar, lookback, idx, self.position, state)
            self._process_signal(signal, bar)
            self.equity_curve.append(self.capital + self._unrealized_pnl(bar))

        if self.position is not None:
            self._close(bars.iloc[-1])
        if self.equity_curve:
            self.equity_curve[-1] = self.capital

        return self._build_result()

    def _process_signal(self, signal: Any, bar: Any) -> None:
        if signal is None:
            return
        if isinstance(signal, tuple):
            action, ratio = signal
        else:
            action, ratio = signal, 1.0

        if action == "buy" and self.position is None:
            self._open(bar, "buy", ratio)
        elif action == "close" and self.position is not None:
            self._close(bar)

    def _open(self, bar: Any, side: str, capital_ratio: float = 1.0) -> None:
        price = float(bar["close"])
        alloc = self.capital * 0.95 * capital_ratio
        if alloc <= 0:
            return
        size = alloc / price
        fee = price * size * self.fee_rate
        self.capital -= fee
        ts = bar.get("ts", str(bar.name)) if hasattr(bar, "get") else str(bar.name)
        self.position = {
            "side": side,
            "entry_price": price,
            "size": size,
            "entry_ts": ts,
            "fee_paid": fee,
        }

    def _close(self, bar: Any) -> None:
        if self.position is None:
            return
        price = float(bar["close"])
        entry = self.position["entry_price"]
        size = self.position["size"]
        fee_exit = price * size * self.fee_rate
        pnl = (price - entry) * size - fee_exit
        self.capital += pnl
        close_ts = bar.get("ts", str(bar.name)) if hasattr(bar, "get") else str(bar.name)
        self.trades.append({
            "entry_price": entry,
            "exit_price": price,
            "pnl": pnl,
            "fee": self.position["fee_paid"] + fee_exit,
            "entry_ts": str(self.position.get("entry_ts", "")),
            "close_ts": str(close_ts),
        })
        self.position = None

    def _unrealized_pnl(self, bar: Any) -> float:
        if self.position is None:
            return 0.0
        price = float(bar["close"])
        entry = self.position["entry_price"]
        size = self.position["size"]
        return (price - entry) * size

    def _build_result(self) -> dict:
        total_profit = self.capital - self.initial_capital
        total_profit_pct = (total_profit / self.initial_capital * 100) if self.initial_capital > 0 else 0.0
        winning = [t for t in self.trades if t["pnl"] > 0]
        losing = [t for t in self.trades if t["pnl"] <= 0]
        gross_profit = sum(t["pnl"] for t in winning)
        gross_loss = abs(sum(t["pnl"] for t in losing))
        win_rate = len(winning) / len(self.trades) * 100 if self.trades else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
        max_dd_pct = _compute_mdd_pct(self.equity_curve)
        daily_returns = _compute_daily_returns(self.equity_curve)
        sharpe = _compute_sharpe(daily_returns)
        return {
            "initial_capital": self.initial_capital,
            "final_equity": round(self.capital, 4),
            "total_profit": round(total_profit, 4),
            "total_profit_pct": round(total_profit_pct, 4),
            "max_drawdown_pct": round(max_dd_pct, 4),
            "sharpe_ratio": round(sharpe, 4),
            "win_rate": round(win_rate, 2),
            "total_trades": len(self.trades),
            "profit_factor": _safe_float(profit_factor),
            "equity_curve": self.equity_curve,
            "daily_returns": daily_returns,
            "trades": self.trades,
        }


# ── Walk-Forward ───────────────────────────────────────────────────────────────

def _wf_windows(
    df: pd.DataFrame,
    train_days: int,
    test_days: int,
) -> list[tuple]:
    """슬라이딩 윈도우 생성 (train_start, train_end, test_start, test_end)."""
    if "ts" in df.columns:
        timestamps = pd.to_datetime(df["ts"], utc=True)
    else:
        timestamps = pd.to_datetime(df.index, utc=True)
    start = timestamps.iloc[0]
    end = timestamps.iloc[-1]

    windows = []
    train_start = start
    while True:
        train_end = train_start + timedelta(days=train_days)
        test_start = train_end
        test_end = test_start + timedelta(days=test_days)
        if test_end > end:
            break
        windows.append((train_start, train_end, test_start, test_end))
        train_start = train_start + timedelta(days=test_days)
    return windows


def _slice_df(df: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    if "ts" in df.columns:
        ts = pd.to_datetime(df["ts"], utc=True)
        mask = (ts >= start) & (ts < end)
        return df[mask].copy()
    else:
        ts = pd.to_datetime(df.index, utc=True)
        mask = (ts >= start) & (ts < end)
        return df[mask].copy()


def run_walk_forward(
    ohlcv: pd.DataFrame,
    signal_fn,
    train_days: int = WF_TRAIN_DAYS,
    test_days: int = WF_TEST_DAYS,
    n_mc: int = 100,
) -> dict:
    """Walk-Forward 실행 후 집계 결과 반환."""
    windows = _wf_windows(ohlcv, train_days, test_days)
    oos_sharpes: list[float] = []
    oos_returns: list[float] = []
    oos_mdd: list[float] = []
    oos_daily_returns: list[float] = []
    window_details: list[dict] = []

    for i, (tr_s, tr_e, te_s, te_e) in enumerate(windows):
        train_slice = _slice_df(ohlcv, tr_s, tr_e)
        test_slice = _slice_df(ohlcv, te_s, te_e)

        if len(train_slice) < WARMUP_BARS + 10 or len(test_slice) < 10:
            continue

        bt_train = SimpleBacktester(train_slice, warmup=WARMUP_BARS)
        r_train = bt_train.run(signal_fn)

        bt_test = SimpleBacktester(test_slice, warmup=min(WARMUP_BARS, len(test_slice) // 2))
        r_test = bt_test.run(signal_fn)

        oos_sharpes.append(r_test["sharpe_ratio"])
        oos_returns.append(r_test["total_profit_pct"])
        oos_mdd.append(r_test["max_drawdown_pct"])
        oos_daily_returns.extend(r_test.get("daily_returns", []))

        window_details.append({
            "window_id": i,
            "train_start": tr_s,
            "train_end": tr_e,
            "test_start": te_s,
            "test_end": te_e,
            "train_sharpe": r_train["sharpe_ratio"],
            "train_return": r_train["total_profit_pct"],
            "train_max_dd": r_train["max_drawdown_pct"],
            "train_trades": r_train["total_trades"],
            "test_sharpe": r_test["sharpe_ratio"],
            "test_return": r_test["total_profit_pct"],
            "test_max_dd": r_test["max_drawdown_pct"],
            "test_trades": r_test["total_trades"],
        })

        log.info(
            "wf_window",
            win=i,
            test=f"{te_s:%Y-%m-%d}~{te_e:%Y-%m-%d}",
            oos_sharpe=round(r_test["sharpe_ratio"], 3),
            oos_ret=round(r_test["total_profit_pct"], 2),
            mdd=round(r_test["max_drawdown_pct"], 2),
            trades=r_test["total_trades"],
        )

    if not oos_sharpes:
        return {"windows": [], "aggregate_sharpe": 0.0, "consistency_ratio": 0.0,
                "aggregate_return": 0.0, "aggregate_max_dd": 0.0, "monte_carlo": {}}

    agg_sharpe = sum(oos_sharpes) / len(oos_sharpes)
    agg_return = sum(oos_returns) / len(oos_returns)
    agg_mdd = max(oos_mdd)
    consistency = sum(1 for s in oos_sharpes if s > 0) / len(oos_sharpes)

    # Monte Carlo
    mc = _monte_carlo(oos_daily_returns, n_mc)

    return {
        "windows": window_details,
        "aggregate_sharpe": round(agg_sharpe, 4),
        "aggregate_return": round(agg_return, 4),
        "aggregate_max_dd": round(agg_mdd, 4),
        "consistency_ratio": round(consistency, 4),
        "total_windows": len(oos_sharpes),
        "monte_carlo": mc,
        "oos_sharpes": oos_sharpes,
        "oos_returns": oos_returns,
    }


def _monte_carlo(daily_returns: list[float], n_simulations: int = 100) -> dict:
    """OOS 일별 수익률을 셔플하여 Sharpe 분포 계산."""
    if len(daily_returns) < 10:
        return {}
    sharpes, profits, mdds = [], [], []
    arr = np.array(daily_returns, dtype=float)
    for _ in range(n_simulations):
        shuffled = arr.copy()
        np.random.shuffle(shuffled)
        equity = np.cumprod(1 + shuffled) * INITIAL_CAPITAL
        mean_r = float(np.mean(shuffled))
        std_r = float(np.std(shuffled, ddof=1)) if len(shuffled) > 1 else 0.0
        sharpe = (mean_r / std_r) * math.sqrt(8760) if std_r > 0 else 0.0
        sharpes.append(sharpe)
        profits.append(float(equity[-1] / INITIAL_CAPITAL - 1) * 100)
        peak = np.maximum.accumulate(equity)
        dd = float(np.max((peak - equity) / np.where(peak > 0, peak, 1))) * 100
        mdds.append(dd)

    def _ci(arr_):
        return (float(np.percentile(arr_, 2.5)), float(np.percentile(arr_, 97.5)))

    win_prob = sum(1 for p in profits if p > 0) / len(profits)
    return {
        "n_simulations": n_simulations,
        "sharpe_mean": round(float(np.mean(sharpes)), 4),
        "sharpe_std": round(float(np.std(sharpes)), 4),
        "sharpe_ci_95": [round(v, 4) for v in _ci(sharpes)],
        "profit_mean": round(float(np.mean(profits)), 4),
        "profit_ci_95": [round(v, 4) for v in _ci(profits)],
        "max_dd_mean": round(float(np.mean(mdds)), 4),
        "max_dd_ci_95": [round(v, 4) for v in _ci(mdds)],
        "win_probability": round(win_prob, 4),
    }


# ── 통계 유틸 ─────────────────────────────────────────────────────────────────

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


def _compute_mdd_pct(equity_curve: list[float]) -> float:
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd * 100.0


def _compute_daily_returns(equity_curve: list[float]) -> list[float]:
    if len(equity_curve) < 2:
        return []
    return [
        (equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1]
        if equity_curve[i - 1] != 0 else 0.0
        for i in range(1, len(equity_curve))
    ]


def _compute_sharpe(returns: list[float], periods: int = 8760) -> float:
    if len(returns) < 2:
        return 0.0
    mean_r = sum(returns) / len(returns)
    var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    std = math.sqrt(var) if var > 0 else 0.0
    return (mean_r / std) * math.sqrt(periods) if std > 0 else 0.0


def _subperiod_return(
    equity_curve: list[float],
    df_bars: pd.DataFrame,
    start_str: str,
    end_str: str,
) -> float:
    start_dt = pd.Timestamp(start_str, tz="UTC")
    end_dt = pd.Timestamp(end_str, tz="UTC")
    ts_col = df_bars["ts"] if "ts" in df_bars.columns else pd.Series(df_bars.index)
    ts_col = pd.to_datetime(ts_col, utc=True)
    mask_start = ts_col >= start_dt
    mask_end = ts_col <= end_dt
    if not mask_start.any() or not mask_end.any():
        return 0.0
    idx_start = int(mask_start.idxmax())
    idx_end = int(ts_col[mask_end].index[-1])
    eq_offset = WARMUP_BARS
    eq_s = max(0, idx_start - eq_offset + 1)
    eq_e = min(len(equity_curve) - 1, idx_end - eq_offset + 1)
    if eq_s >= len(equity_curve) or eq_e < eq_s:
        return 0.0
    eq_start = equity_curve[eq_s]
    eq_end = equity_curve[eq_e]
    if eq_start <= 0:
        return 0.0
    return (eq_end - eq_start) / eq_start * 100.0


# ── DB 연결 ────────────────────────────────────────────────────────────────────

async def _load_ohlcv(
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
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df


async def _save_variant(
    pool: asyncpg.Pool,
    test_name: str,
    variant: str,
    result: dict,
    symbol: str,
    start: datetime,
    end: datetime,
    extra_meta: dict | None = None,
) -> None:
    metadata = {
        "profit_factor": _safe_float(result["profit_factor"]),
        "win_rate": result["win_rate"],
        "total_trades": result["total_trades"],
        "equity_curve_sample": [
            round(_safe_float(v), 2)
            for v in result["equity_curve"][::max(1, len(result["equity_curve"]) // 200)]
        ],
        **(extra_meta or {}),
    }
    async with pool.acquire() as conn:
        await conn.execute(CREATE_VARIANT_RESULTS)
        await conn.execute(
            """
            INSERT INTO strategy_variant_results
                (test_name, variant_name, symbol, start_date, end_date,
                 initial_capital, final_equity, total_return,
                 sharpe_ratio, max_drawdown, win_rate, total_trades, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
            """,
            test_name, variant, symbol, start, end,
            result["initial_capital"],
            result["final_equity"],
            result["total_profit_pct"],
            result["sharpe_ratio"],
            result["max_drawdown_pct"],
            result["win_rate"],
            result["total_trades"],
            json.dumps(metadata),
        )


async def _save_wf_results(
    pool: asyncpg.Pool,
    strategy_name: str,
    wf: dict,
    run_id: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_WF_RESULTS)
        for w in wf["windows"]:
            await conn.execute(
                """
                INSERT INTO walk_forward_results
                    (run_id, strategy, window_id,
                     train_start, train_end, test_start, test_end,
                     train_sharpe, train_return_pct, train_max_drawdown_pct, test_total_trades,
                     test_sharpe, test_return_pct, test_max_drawdown_pct, test_win_rate,
                     aggregate_sharpe, consistency_ratio, sharpe_alert, monte_carlo)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                        $16,$17,$18,$19::jsonb)
                """,
                run_id, strategy_name, w["window_id"],
                w["train_start"], w["train_end"], w["test_start"], w["test_end"],
                w["train_sharpe"], w["train_return"], w["train_max_dd"], w["test_trades"],
                w["test_sharpe"], w["test_return"], w["test_max_dd"], 0.0,
                wf["aggregate_sharpe"], wf["consistency_ratio"],
                wf["aggregate_sharpe"] < 1.5,
                json.dumps(wf.get("monte_carlo", {})),
            )
        # 집계 행
        await conn.execute(
            """
            INSERT INTO walk_forward_results
                (run_id, strategy, window_id,
                 aggregate_sharpe, consistency_ratio, sharpe_alert, monte_carlo)
            VALUES ($1,$2,-1,$3,$4,$5,$6::jsonb)
            """,
            run_id, strategy_name,
            wf["aggregate_sharpe"], wf["consistency_ratio"],
            wf["aggregate_sharpe"] < 1.5,
            json.dumps(wf.get("monte_carlo", {})),
        )


# ── 변형 목록 ──────────────────────────────────────────────────────────────────

VARIANTS = {
    "baseline_ref":      signal_baseline_ref,
    "rsi_ema_dual":      signal_rsi_ema_dual,
    "rsi_ema_consec":    signal_rsi_ema_consec,
    "rsi_trend_guard":   signal_rsi_trend_guard,
    "rsi_macd_momentum": signal_rsi_macd_momentum,
    "adaptive_v2":       signal_adaptive_v2,
}


# ── 메인 ──────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
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

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    log.info("connecting_db", dsn=DB_DSN.split("@")[1])
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)

    log.info("loading_ohlcv", start=args.start, end=args.end)
    ohlcv = await _load_ohlcv(pool, args.symbol, TIMEFRAME, start_dt, end_dt)
    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음.")
        await pool.close()
        sys.exit(1)
    log.info("ohlcv_loaded", rows=len(ohlcv))

    # 기존 test_i 데이터 삭제
    async with pool.acquire() as conn:
        await conn.execute(CREATE_VARIANT_RESULTS)
        await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = 'test_i_dca_v2_redesign'"
        )

    results: dict[str, dict] = {}

    # ── 6년 단일 백테스트 ────────────────────────────────────────────────────
    print("\n" + "━" * 80)
    print("▶ 6년 단일 백테스트 (2020-04-01 ~ 2026-03-31)")
    print("━" * 80)

    df_bars = ohlcv.copy()

    for variant_name, signal_fn in VARIANTS.items():
        log.info("running_variant", variant=variant_name)
        bt = SimpleBacktester(ohlcv, initial_capital=args.capital, fee_rate=FEE_RATE)
        result = bt.run(signal_fn)
        results[variant_name] = result

        # 서브기간 수익률
        sub_returns: dict[str, float] = {}
        for sub_label, (sub_start, sub_end) in SUB_PERIODS.items():
            ret = _subperiod_return(result["equity_curve"], df_bars, sub_start, sub_end)
            sub_returns[sub_label] = round(ret, 4)

        await _save_variant(
            pool,
            test_name="test_i_dca_v2_redesign",
            variant=variant_name,
            result=result,
            symbol=args.symbol,
            start=start_dt,
            end=end_dt,
            extra_meta={"sub_period_returns": sub_returns},
        )
        log.info(
            "variant_done",
            variant=variant_name,
            return_pct=round(result["total_profit_pct"], 2),
            sharpe=round(result["sharpe_ratio"], 4),
            mdd=round(result["max_drawdown_pct"], 2),
            trades=result["total_trades"],
        )

    # ── 비교표 출력 ───────────────────────────────────────────────────────────
    _print_comparison_table(results, df_bars)

    # ── Walk-Forward: 최우수 변형 선정 ───────────────────────────────────────
    best_variant = max(results.items(), key=lambda x: x[1]["sharpe_ratio"])
    best_name = best_variant[0]
    log.info("best_variant_selected", variant=best_name, sharpe=best_variant[1]["sharpe_ratio"])

    if not args.skip_wf:
        print(f"\n{'━' * 80}")
        print(f"▶ Walk-Forward 검증: {best_name} (train={WF_TRAIN_DAYS}d, test={WF_TEST_DAYS}d)")
        print("━" * 80)

        wf = run_walk_forward(ohlcv, VARIANTS[best_name])
        _print_wf_table(wf, best_name)

        import uuid
        run_id = str(uuid.uuid4())[:8]
        await _save_wf_results(pool, f"dca_v2_{best_name}", wf, run_id)
        log.info(
            "wf_complete",
            strategy=best_name,
            agg_sharpe=wf["aggregate_sharpe"],
            consistency=wf["consistency_ratio"],
            windows=wf["total_windows"],
        )

        # Walk-Forward 결과도 results에 추가
        results["__wf__"] = wf
        results["__best__"] = best_name

    await pool.close()
    log.info("test_i_complete")
    return results


def _print_comparison_table(results: dict, df_bars: pd.DataFrame) -> None:
    w = 110
    print("\n" + "=" * w)
    print(f"{'변형':<22} | {'6년수익률':>9} | {'Sharpe':>7} | {'MDD':>7} | {'거래수':>5} | "
          f"{'승률':>6} | {'불장(20-21)':>11} | {'폭락(22)':>9} | {'불장(23-24)':>11} | {'약세(25H2)':>10}")
    print("=" * w)

    for variant_name, result in results.items():
        if variant_name.startswith("__"):
            continue
        ret_6y = result["total_profit_pct"]
        sharpe = result["sharpe_ratio"]
        mdd = result["max_drawdown_pct"]
        trades = result["total_trades"]
        win_rate = result["win_rate"]

        bull2021 = _subperiod_return(result["equity_curve"], df_bars,
                                     *SUB_PERIODS["bull_2020_21"])
        bear2022 = _subperiod_return(result["equity_curve"], df_bars,
                                     *SUB_PERIODS["bear_2022"])
        bull2324 = _subperiod_return(result["equity_curve"], df_bars,
                                     *SUB_PERIODS["bull_2023_24"])
        bear2025 = _subperiod_return(result["equity_curve"], df_bars,
                                     *SUB_PERIODS["bear_2025h2"])

        print(
            f"{variant_name:<22} | {ret_6y:>+8.2f}% | {sharpe:>7.3f} | {mdd:>6.2f}% | "
            f"{trades:>5} | {win_rate:>5.1f}% | {bull2021:>+10.2f}% | {bear2022:>+8.2f}% | "
            f"{bull2324:>+10.2f}% | {bear2025:>+9.2f}%"
        )
    print("=" * w)


def _print_wf_table(wf: dict, strategy_name: str) -> None:
    print(f"\n Walk-Forward: {strategy_name} ({wf['total_windows']}개 윈도우)")
    print("-" * 85)
    print(f"{'Win#':>4} | {'Test 기간':>25} | {'Train Sharpe':>12} | "
          f"{'OOS Sharpe':>10} | {'OOS Ret%':>8} | {'MDD%':>6} | {'거래수':>5}")
    print("-" * 85)
    for w in wf["windows"]:
        ts = w["test_start"]
        te = w["test_end"]
        ts_str = f"{ts:%Y-%m-%d}~{te:%Y-%m-%d}" if isinstance(ts, datetime) else str(ts)[:21]
        print(
            f"{w['window_id']:>4} | {ts_str:>25} | {w['train_sharpe']:>12.3f} | "
            f"{w['test_sharpe']:>10.3f} | {w['test_return']:>+7.2f}% | "
            f"{w['test_max_dd']:>5.2f}% | {w['test_trades']:>5}"
        )
    print("-" * 85)
    alert = "⚠️  경보" if wf["aggregate_sharpe"] < 1.5 else "✅ 통과"
    print(
        f"{'집계':>4} | {'':>25} | {'':>12} | "
        f"{wf['aggregate_sharpe']:>10.3f} | {wf['aggregate_return']:>+7.2f}% | "
        f"{wf['aggregate_max_dd']:>5.2f}% |  {alert}"
    )
    print(f"     Consistency: {wf['consistency_ratio']:.3f} "
          f"({int(wf['consistency_ratio']*wf['total_windows'])}/{wf['total_windows']} 양수)")
    mc = wf.get("monte_carlo", {})
    if mc:
        print(f"     MC Sharpe: {mc['sharpe_mean']:.3f} ± {mc['sharpe_std']:.3f}  "
              f"95%CI [{mc['sharpe_ci_95'][0]:.3f}, {mc['sharpe_ci_95'][1]:.3f}]  "
              f"승률: {mc['win_probability']*100:.1f}%")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test I: DCA v2 재설계 백테스트")
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--start", default=START_DATE)
    parser.add_argument("--end", default=END_DATE)
    parser.add_argument("--capital", default=INITIAL_CAPITAL, type=float)
    parser.add_argument("--skip-wf", action="store_true", help="Walk-Forward 건너뜀")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
