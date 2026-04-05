"""bt_fa_plus_tf.py — Stage 2: FA + 추세추종 결합 백테스트

역할:
  - FA short_hold + TF(ema_cross 기본, donchian 선택) 결합
  - 레짐별 가중치 그리드 서치로 최적 포트폴리오 탐색
  - 비교: FA단독, TF단독, 결합보수적, 결합공격적, 균등배분, 최적그리드, BTC B&H

기간: 2020-04-01 ~ 2026-03-31 (6년)
초기 자본: 10,000 USDT
저장: strategy_variant_results 테이블 (test_name="test_11_stage2_fa_tf")

실행 방법:
    python bt_fa_plus_tf.py
    python bt_fa_plus_tf.py --start 2020-04-01 --end 2026-03-31 --initial-capital 10000
    python bt_fa_plus_tf.py --tf-variant donchian
    DB_HOST=postgres DB_PASSWORD=CryptoEngine2026! python bt_fa_plus_tf.py
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
WARMUP_BARS     = 200
TEST_NAME       = "test_11_stage2_fa_tf"

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# 공통 리스크 파라미터 (TF용)
COMMON_RISK: dict[str, Any] = {
    "risk_per_trade_pct":        0.01,
    "max_position_pct":          0.20,
    "stop_loss_atr_mult":        2.0,
    "trailing_stop_atr_mult":    2.5,
    "take_profit_atr_mult":      6.0,
    "max_trades_per_day":        3,
    "min_trade_interval_hours":  4,
    "consecutive_loss_cooldown": 3,
    "max_leverage":              2.0,
    "taker_fee_pct":             0.00055,
    "maker_fee_pct":             0.00020,
    "assumed_maker_ratio":       0.5,
    "slippage_pct":              0.0003,
}

EFFECTIVE_FEE = (
    COMMON_RISK["assumed_maker_ratio"]       * COMMON_RISK["maker_fee_pct"] +
    (1 - COMMON_RISK["assumed_maker_ratio"]) * COMMON_RISK["taker_fee_pct"] +
    COMMON_RISK["slippage_pct"]
)

FA_FEE_RATE = 0.00055  # FA 전략 수수료

# FA 파라미터 (short_hold 변형)
FA_PARAMS: dict[str, Any] = {
    "exit_on_flip":              True,
    "negative_hours_before_exit": 0,
    "consecutive_intervals":     3,
    "min_funding_rate":          0.0001,
    "max_hold_bars":             168,  # 7일
}

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_VARIANT_RESULTS = """
CREATE TABLE IF NOT EXISTS strategy_variant_results (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    test_name       TEXT        NOT NULL,
    variant_name    TEXT        NOT NULL,
    data_range      TEXT,
    total_return    DOUBLE PRECISION,
    sharpe_ratio    DOUBLE PRECISION,
    max_drawdown    DOUBLE PRECISION,
    trade_count     INTEGER,
    win_rate        DOUBLE PRECISION,
    profit_factor   DOUBLE PRECISION,
    monthly_returns JSONB,
    params          JSONB
);
"""

# ── 가중치 후보 ────────────────────────────────────────────────────────────────

WEIGHT_CANDIDATES: dict[str, list[tuple[float, float, float]]] = {
    "ranging": [
        (0.30, 0.10, 0.60),
        (0.40, 0.10, 0.50),
        (0.30, 0.20, 0.50),
        (0.50, 0.00, 0.50),
    ],
    "trending_up": [
        (0.10, 0.50, 0.40),
        (0.10, 0.60, 0.30),
        (0.20, 0.40, 0.40),
        (0.00, 0.60, 0.40),
        (0.10, 0.40, 0.50),
    ],
    "trending_down": [
        (0.10, 0.50, 0.40),
        (0.10, 0.40, 0.50),
        (0.00, 0.50, 0.50),
        (0.10, 0.30, 0.60),
    ],
    "volatile": [
        (0.20, 0.00, 0.80),
        (0.30, 0.00, 0.70),
        (0.10, 0.00, 0.90),
    ],
}

# 고정 비교 설정 목록 (레짐별 고정 가중치로 동작)
FIXED_CONFIGS = [
    {"name": "FA_단독",   "fa_w": 0.50, "tf_w": 0.00},
    {"name": "TF_단독",   "fa_w": 0.00, "tf_w": 0.50},
    {"name": "결합_보수적", "fa_w": 0.30, "tf_w": 0.20},
    {"name": "결합_공격적", "fa_w": 0.10, "tf_w": 0.60},
    {"name": "균등배분",   "fa_w": 0.33, "tf_w": 0.33},
]


# =========================================================================
# 공통 헬퍼 함수
# =========================================================================

def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def calc_sharpe(equity_curve: list[float], periods_per_year: int = 8760) -> float:
    returns = pd.Series(equity_curve).pct_change().dropna()
    if len(returns) < 2 or returns.std() == 0:
        return 0.0
    return float(
        (returns.mean() * periods_per_year) /
        (returns.std() * math.sqrt(periods_per_year))
    )


def calc_max_drawdown(equity_curve: list[float]) -> float:
    eq   = pd.Series(equity_curve)
    peak = eq.cummax()
    dd   = (eq - peak) / peak * 100
    return float(abs(dd.min()))


def calc_monthly_returns(equity_curve: list[float], timestamps: list) -> list[float]:
    df = pd.DataFrame(
        {"equity": equity_curve},
        index=pd.to_datetime(timestamps),
    )
    monthly = df.resample("ME").last()
    monthly["return"] = monthly["equity"].pct_change() * 100
    return monthly["return"].dropna().tolist()


def calc_annualized_return(total_return_pct: float, years: float) -> float:
    if years <= 0:
        return 0.0
    factor = 1.0 + total_return_pct / 100.0
    if factor <= 0:
        return -100.0
    return ((factor ** (1.0 / years)) - 1.0) * 100.0


def calc_calmar(annualized_return_pct: float, max_drawdown_pct: float) -> float:
    if max_drawdown_pct == 0:
        return 0.0
    return annualized_return_pct / max_drawdown_pct


def calc_profit_factor(trades: list[dict]) -> float:
    wins   = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    losses = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    return wins / losses if losses > 0 else float("inf")


def calculate_position_size(
    equity: float,
    entry_price: float,
    atr: float,
    risk_pct: float = 0.01,
    stop_atr_mult: float = 2.0,
    max_position_pct: float = 0.20,
    max_leverage: float = 2.0,
) -> float:
    if atr <= 0 or entry_price <= 0:
        return 0.0
    risk_amount    = equity * risk_pct
    stop_dist      = atr * stop_atr_mult
    raw_size       = risk_amount / stop_dist
    max_notional   = (equity * max_position_pct) / entry_price
    max_lev_size   = (equity * max_leverage) / entry_price
    return min(raw_size, max_notional, max_lev_size)


def objective(result: dict) -> float:
    """최적화 목표 함수."""
    annual_return = result.get("annualized_return_pct", 0.0)
    sharpe        = result.get("sharpe_ratio", 0.0)
    max_dd        = result.get("max_drawdown_pct", 0.0)
    consistency   = result.get("pct_positive_months", 0.0)
    calmar        = result.get("calmar_ratio", 0.0)

    if max_dd > 20:
        dd_penalty = (max_dd - 20) * 0.5
    elif max_dd > 10:
        dd_penalty = (max_dd - 10) * 0.2
    else:
        dd_penalty = 0.0

    score = (
        annual_return * 0.30 +
        sharpe        * 5.0 * 0.25 +
        calmar        * 2.0 * 0.20 +
        consistency   * 10.0 * 0.15 +
        (-dd_penalty) * 0.10
    )
    return score


# =========================================================================
# 레짐 감지
# =========================================================================

def precompute_regime_indicators(df: pd.DataFrame) -> dict:
    """EMA/ATR 지표를 한 번에 사전 계산 (O(n²) 방지)."""
    closes = df["close"]
    ema20_series  = closes.ewm(span=20,  adjust=False).mean()
    ema200_series = closes.ewm(span=200, adjust=False).mean()
    atr_series    = calc_atr(df, period=14)
    return {
        "ema20":  ema20_series.values,
        "ema200": ema200_series.values,
        "atr":    atr_series.values,
        "close":  closes.values,
    }


def detect_regime(df: pd.DataFrame, idx: int,
                  _precomp: dict | None = None) -> str:
    """
    Returns: 'trending_up', 'trending_down', 'ranging', 'volatile'
    _precomp: precompute_regime_indicators() 결과 (없으면 즉석 계산)
    """
    if idx < WARMUP_BARS:
        return "ranging"

    if _precomp is not None:
        atr_val = float(_precomp["atr"][idx])
        price   = float(_precomp["close"][idx])
        ema20   = float(_precomp["ema20"][idx])
        ema200  = float(_precomp["ema200"][idx])
        lookback = max(0, idx - 24)
        ema20_1d_ago = float(_precomp["ema20"][lookback])
    else:
        window = df.iloc[max(0, idx - 48):idx]
        atr_val = float(calc_atr(window).iloc[-1]) if len(window) >= 14 else 0.0
        price   = float(df["close"].iloc[idx])
        ema20_series  = df["close"].ewm(span=20,  adjust=False).mean()
        ema200_series = df["close"].ewm(span=200, adjust=False).mean()
        ema20  = float(ema20_series.iloc[idx])
        ema200 = float(ema200_series.iloc[idx])
        lookback = max(0, idx - 24)
        ema20_1d_ago = float(ema20_series.iloc[lookback])

    atr_pct = atr_val / price if price > 0 else 0.0
    if atr_pct > 0.025:
        return "volatile"

    if ema20 > ema200 and ema20 > ema20_1d_ago * 1.001:
        return "trending_up"
    elif ema20 < ema200 and ema20 < ema20_1d_ago * 0.999:
        return "trending_down"
    return "ranging"


# =========================================================================
# 지표 사전 계산 (TF용)
# =========================================================================

def precompute_tf_indicators(df: pd.DataFrame, variant: str) -> dict[str, pd.Series]:
    ind: dict[str, pd.Series] = {}
    ind["atr"]   = calc_atr(df, 14)
    ind["close"] = df["close"]

    if variant == "ema_cross":
        closes = df["close"]
        ind["ema20"] = closes.ewm(span=20, adjust=False).mean()
        ind["ema50"] = closes.ewm(span=50, adjust=False).mean()

    elif variant == "donchian":
        ind["upper96"] = df["high"].rolling(96).max()
        ind["lower96"] = df["low"].rolling(96).min()
        ind["upper48"] = df["high"].rolling(48).max()
        ind["lower48"] = df["low"].rolling(48).min()

    return ind


# =========================================================================
# TF 진입/청산 신호
# =========================================================================

def tf_check_entry(idx: int, variant: str, ind: dict[str, pd.Series]) -> int:
    """반환: 1=Long, -1=Short, 0=없음"""
    if variant == "ema_cross":
        ema20 = float(ind["ema20"].iloc[idx])
        ema50 = float(ind["ema50"].iloc[idx])
        close = float(ind["close"].iloc[idx])
        if ema20 > ema50 and close > ema20:
            return 1
        if ema20 < ema50 and close < ema20:
            return -1

    elif variant == "donchian":
        if idx < 96:
            return 0
        upper96 = float(ind["upper96"].iloc[idx - 1])
        lower96 = float(ind["lower96"].iloc[idx - 1])
        close   = float(ind["close"].iloc[idx])
        if not math.isnan(upper96) and close > upper96:
            return 1
        if not math.isnan(lower96) and close < lower96:
            return -1

    return 0


def tf_check_exit(idx: int, variant: str, ind: dict[str, pd.Series], direction: int) -> bool:
    """전략별 추가 청산 조건."""
    if variant == "ema_cross":
        ema20 = float(ind["ema20"].iloc[idx])
        ema50 = float(ind["ema50"].iloc[idx])
        if direction == 1 and ema20 < ema50:
            return True
        if direction == -1 and ema20 > ema50:
            return True

    elif variant == "donchian":
        if idx < 48:
            return False
        upper48 = float(ind["upper48"].iloc[idx - 1])
        lower48 = float(ind["lower48"].iloc[idx - 1])
        close   = float(ind["close"].iloc[idx])
        if direction == 1 and not math.isnan(lower48) and close < lower48:
            return True
        if direction == -1 and not math.isnan(upper48) and close > upper48:
            return True

    return False


# =========================================================================
# 결합 백테스트 엔진
# =========================================================================

class CombinedEngine:
    """FA + TF 결합 이벤트 루프.

    weight_map: {"ranging": (fa_w, tf_w), "trending_up": ..., ...}
    고정 가중치 모드: fixed_fa_w / fixed_tf_w 사용 (레짐 무시)
    """

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        tf_variant: str = "ema_cross",
        weight_map: dict[str, tuple[float, float]] | None = None,
        fixed_fa_w: float | None = None,
        fixed_tf_w: float | None = None,
        initial_capital: float = INITIAL_CAPITAL,
    ) -> None:
        self.df              = ohlcv.copy()
        self.funding         = funding
        self.tf_variant      = tf_variant
        self.weight_map      = weight_map          # 레짐별 가중치
        self.fixed_fa_w      = fixed_fa_w          # 고정 가중치 모드
        self.fixed_tf_w      = fixed_tf_w
        self.initial_capital = initial_capital

        self._equity         = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._fa_trades: list[dict]  = []
        self._tf_trades: list[dict]  = []

        # FA 포지션
        self._fa_pos: dict[str, Any] | None = None
        self._fa_pos_consec  = 0
        self._fa_neg_consec  = 0

        # TF 포지션
        self._tf_pos: dict[str, Any] | None = None
        self._tf_trail_stop: float | None   = None
        self._tf_consec_losses = 0
        self._tf_cooldown_until: pd.Timestamp | None = None
        self._tf_trades_today  = 0
        self._tf_last_trade_day: Any = None
        self._tf_last_entry_ts: pd.Timestamp | None  = None

        # 지표 사전 계산
        self._tf_ind = precompute_tf_indicators(self.df, tf_variant)

    # ------------------------------------------------------------------
    # 유틸
    # ------------------------------------------------------------------

    def _get_weights(self, regime: str) -> tuple[float, float]:
        if self.fixed_fa_w is not None and self.fixed_tf_w is not None:
            return self.fixed_fa_w, self.fixed_tf_w
        if self.weight_map and regime in self.weight_map:
            fa_w, tf_w = self.weight_map[regime]
            return fa_w, tf_w
        return 0.25, 0.25

    def _get_funding_rate(self, ts: pd.Timestamp) -> float:
        if self.funding is None or self.funding.empty:
            return 0.0001
        try:
            mask = self.funding.index <= ts
            if mask.any():
                return float(self.funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    # ------------------------------------------------------------------
    # FA 로직
    # ------------------------------------------------------------------

    def _fa_step(
        self,
        bar: pd.Series,
        ts: pd.Timestamp,
        idx: int,
        fa_capital: float,
    ) -> None:
        params  = FA_PARAMS
        funding = self._get_funding_rate(ts)

        is_settlement = (ts.hour % 8 == 0) and (ts.minute == 0)

        # 펀딩비 정산
        if self._fa_pos is not None and is_settlement:
            direction = self._fa_pos.get("funding_direction", 1)
            pos_value = self._fa_pos["size"] * self._fa_pos["entry_price"]
            net_fund  = pos_value * funding * direction
            self._equity += net_fund
            self._fa_pos["funding_accumulated"] = (
                self._fa_pos.get("funding_accumulated", 0.0) + net_fund
            )

        close = float(bar["close"])
        min_rate = params["min_funding_rate"]
        max_hold = params["max_hold_bars"]
        consec   = params["consecutive_intervals"]

        if self._fa_pos is None:
            if is_settlement:
                if funding >= min_rate:
                    self._fa_pos_consec += 1
                    self._fa_neg_consec  = 0
                elif funding <= -min_rate:
                    self._fa_neg_consec  += 1
                    self._fa_pos_consec   = 0
                else:
                    self._fa_pos_consec = 0
                    self._fa_neg_consec  = 0

                if self._fa_pos_consec >= consec:
                    self._fa_open(bar, ts, idx, "sell", fa_capital)
                    self._fa_pos_consec = 0
                elif self._fa_neg_consec >= consec:
                    self._fa_open(bar, ts, idx, "buy", fa_capital)
                    self._fa_neg_consec = 0
        else:
            direction    = self._fa_pos.get("funding_direction", 1)
            bars_held    = idx - self._fa_pos.get("entry_idx", idx)
            reversed_now = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)

            should_close = False
            if is_settlement:
                if reversed_now:
                    self._fa_pos["reverse_count"] = (
                        self._fa_pos.get("reverse_count", 0) + 1
                    )
                else:
                    self._fa_pos["reverse_count"] = 0
                if self._fa_pos.get("reverse_count", 0) >= 3:
                    should_close = True

            if bars_held >= max_hold:
                should_close = True

            if should_close:
                self._fa_close(bar, ts)
                self._fa_pos_consec = 0
                self._fa_neg_consec  = 0

    def _fa_open(
        self, bar: pd.Series, ts: pd.Timestamp, idx: int,
        side: str, fa_capital: float,
    ) -> None:
        entry = float(bar["close"])
        size  = (fa_capital * 0.5) / entry  # leverage 0.5
        fee   = entry * size * FA_FEE_RATE
        if fee > self._equity * 0.01:       # 수수료 > 자본 1% 방어
            return
        self._equity -= fee
        self._fa_pos = {
            "side":                side,
            "entry_price":         entry,
            "size":                size,
            "entry_ts":            ts,
            "entry_idx":           idx,
            "fee_paid":            fee,
            "funding_direction":   1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count":       0,
        }

    def _fa_close(self, bar: pd.Series, ts: pd.Timestamp) -> None:
        if self._fa_pos is None:
            return
        size        = self._fa_pos["size"]
        entry       = self._fa_pos["entry_price"]
        fee_entry   = self._fa_pos.get("fee_paid", 0.0)
        exit_price  = float(bar["close"])
        fee_exit    = exit_price * size * FA_FEE_RATE
        self._equity -= fee_exit

        net_pnl = (
            self._fa_pos.get("funding_accumulated", 0.0)
            - fee_entry
            - fee_exit
        )
        self._fa_trades.append({
            "entry_price": entry,
            "exit_price":  exit_price,
            "pnl":         net_pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._fa_pos["entry_ts"]),
            "close_ts":    str(ts),
            "type":        "fa",
        })
        self._fa_pos = None

    # ------------------------------------------------------------------
    # TF 로직
    # ------------------------------------------------------------------

    def _tf_step(
        self,
        bar: pd.Series,
        ts: pd.Timestamp,
        idx: int,
        tf_capital: float,
    ) -> None:
        close   = float(bar["close"])
        atr_val = float(self._tf_ind["atr"].iloc[idx])
        params  = COMMON_RISK

        # 일별 거래 수 초기화
        trade_day = ts.date()
        if self._tf_last_trade_day is None or trade_day != self._tf_last_trade_day:
            self._tf_trades_today   = 0
            self._tf_last_trade_day = trade_day

        # 포지션 청산 체크
        if self._tf_pos is not None:
            direction  = self._tf_pos["direction"]
            tsmult     = params["trailing_stop_atr_mult"]

            # 트레일링 스탑 업데이트
            if direction == 1:
                new_stop = close - atr_val * tsmult
                if self._tf_trail_stop is None:
                    self._tf_trail_stop = new_stop
                else:
                    self._tf_trail_stop = max(self._tf_trail_stop, new_stop)
                hit_stop = (close <= self._tf_trail_stop)
            else:
                new_stop = close + atr_val * tsmult
                if self._tf_trail_stop is None:
                    self._tf_trail_stop = new_stop
                else:
                    self._tf_trail_stop = min(self._tf_trail_stop, new_stop)
                hit_stop = (close >= self._tf_trail_stop)

            # Take Profit
            tp_mult   = params["take_profit_atr_mult"]
            entry     = self._tf_pos["entry_price"]
            if direction == 1:
                hit_tp = close >= entry + atr_val * tp_mult
            else:
                hit_tp = close <= entry - atr_val * tp_mult

            strategy_exit = tf_check_exit(idx, self.tf_variant, self._tf_ind, direction)

            if hit_stop or hit_tp or strategy_exit:
                self._tf_close(close, ts, atr_val)
                self._tf_trail_stop = None
                return

            # unrealized PnL 반영은 equity_curve에서 처리
            return

        # 진입 체크
        if self._tf_pos is None:
            # 쿨다운 체크
            if self._tf_cooldown_until is not None:
                if ts < self._tf_cooldown_until:
                    return

            if self._tf_trades_today >= params["max_trades_per_day"]:
                return

            if self._tf_last_entry_ts is not None:
                min_interval = timedelta(hours=params["min_trade_interval_hours"])
                if ts - self._tf_last_entry_ts < min_interval:
                    return

            signal = tf_check_entry(idx, self.tf_variant, self._tf_ind)
            if signal != 0 and atr_val > 0:
                qty = calculate_position_size(
                    equity=tf_capital,
                    entry_price=close,
                    atr=atr_val,
                    risk_pct=params["risk_per_trade_pct"],
                    stop_atr_mult=params["stop_loss_atr_mult"],
                    max_position_pct=params["max_position_pct"],
                    max_leverage=params["max_leverage"],
                )
                if qty > 0:
                    self._tf_open(close, signal, qty, ts, atr_val)
                    self._tf_trail_stop  = None
                    self._tf_trades_today += 1
                    self._tf_last_entry_ts = ts

    def _tf_open(
        self,
        price: float,
        direction: int,
        qty: float,
        ts: pd.Timestamp,
        atr: float,
    ) -> None:
        fee = price * qty * EFFECTIVE_FEE
        if fee > self._equity * 0.05:
            return
        self._equity -= fee
        self._tf_pos = {
            "direction":   direction,
            "entry_price": price,
            "qty":         qty,
            "entry_ts":    ts,
            "fee_paid":    fee,
            "entry_atr":   atr,
        }

    def _tf_close(self, price: float, ts: pd.Timestamp, atr: float) -> None:
        if self._tf_pos is None:
            return
        direction  = self._tf_pos["direction"]
        entry      = self._tf_pos["entry_price"]
        qty        = self._tf_pos["qty"]
        fee_entry  = self._tf_pos["fee_paid"]
        fee_exit   = price * qty * EFFECTIVE_FEE
        self._equity -= fee_exit

        raw_pnl = (price - entry) * qty * direction
        pnl     = raw_pnl - fee_exit  # fee_entry 이미 차감

        self._equity += raw_pnl

        if pnl > 0:
            self._tf_consec_losses = 0
        else:
            self._tf_consec_losses += 1
            cooldown_thresh = COMMON_RISK["consecutive_loss_cooldown"]
            if self._tf_consec_losses >= cooldown_thresh:
                self._tf_cooldown_until  = ts + timedelta(hours=24)
                self._tf_consec_losses = 0

        try:
            dur_hours = float(
                (ts - pd.Timestamp(self._tf_pos["entry_ts"])).total_seconds() / 3600
            )
        except Exception:
            dur_hours = 0.0

        self._tf_trades.append({
            "entry_price": entry,
            "exit_price":  price,
            "direction":   direction,
            "qty":         qty,
            "pnl":         pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._tf_pos["entry_ts"]),
            "close_ts":    str(ts),
            "dur_hours":   dur_hours,
            "type":        "tf",
        })
        self._tf_pos = None

    def _unrealized_pnl(self, close: float) -> float:
        pnl = 0.0
        if self._tf_pos is not None:
            d   = self._tf_pos["direction"]
            e   = self._tf_pos["entry_price"]
            qty = self._tf_pos["qty"]
            pnl += (close - e) * qty * d
        # FA는 델타 뉴트럴 → 가격 손익 0
        return pnl

    # ------------------------------------------------------------------
    # 메인 루프
    # ------------------------------------------------------------------

    def run(self, compute_regime: bool = True) -> dict:
        df = self.df
        n  = len(df)

        # 지표 사전 계산 (O(n²) 방지)
        _precomp = precompute_regime_indicators(df) if compute_regime else None

        for idx in range(WARMUP_BARS, n):
            row   = df.iloc[idx]
            ts    = df.index[idx]
            close = float(row["close"])

            # 레짐 감지
            if compute_regime:
                regime = detect_regime(df, idx, _precomp=_precomp)
            else:
                regime = "ranging"

            fa_w, tf_w = self._get_weights(regime)
            fa_capital = self._equity * fa_w
            tf_capital = self._equity * tf_w

            # FA 스텝
            if fa_w > 0:
                self._fa_step(row, ts, idx, fa_capital)

            # TF 스텝
            if tf_w > 0:
                self._tf_step(row, ts, idx, tf_capital)

            self._equity_curve.append(
                self._equity + self._unrealized_pnl(close)
            )

        # 강제 청산
        if self._fa_pos is not None:
            self._fa_close(df.iloc[-1], df.index[-1])
        if self._tf_pos is not None:
            last_close = float(df["close"].iloc[-1])
            last_atr   = float(self._tf_ind["atr"].iloc[-1])
            self._tf_close(last_close, df.index[-1], last_atr)

        if self._equity_curve:
            self._equity_curve[-1] = self._equity

        return self._build_result(df)

    def _build_result(self, df: pd.DataFrame) -> dict:
        total_profit  = self._equity - self.initial_capital
        total_pct     = (total_profit / self.initial_capital * 100) if self.initial_capital > 0 else 0.0

        eq   = self._equity_curve
        mdd  = calc_max_drawdown(eq)
        sharpe = calc_sharpe(eq)

        n_years = (df.index[-1] - df.index[WARMUP_BARS]).days / 365.25 if len(df) > WARMUP_BARS else 6.0
        annual  = calc_annualized_return(total_pct, n_years)
        calmar  = calc_calmar(annual, mdd)

        all_trades = self._fa_trades + self._tf_trades
        n_trades   = len(all_trades)
        winning    = [t for t in all_trades if t["pnl"] > 0]
        win_rate   = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf         = calc_profit_factor(all_trades)

        # 월별 수익률
        max_idx = len(df.index) - WARMUP_BARS
        ts_list = [df.index[WARMUP_BARS + i] for i in range(min(len(eq), max_idx))]
        eq = eq[:len(ts_list)]
        try:
            monthly = calc_monthly_returns(eq, ts_list)
            pos_months = sum(1 for r in monthly if r > 0)
            pct_pos_months = (pos_months / len(monthly) * 100) if monthly else 0.0
        except Exception:
            monthly = []
            pct_pos_months = 0.0

        result = {
            "total_profit_pct":       round(total_pct, 4),
            "annualized_return_pct":  round(annual, 4),
            "sharpe_ratio":            round(sharpe, 4),
            "max_drawdown_pct":        round(mdd, 4),
            "calmar_ratio":            round(calmar, 4),
            "pct_positive_months":     round(pct_pos_months, 2),
            "win_rate":                round(win_rate, 2),
            "total_trades":            n_trades,
            "fa_trades":               len(self._fa_trades),
            "tf_trades":               len(self._tf_trades),
            "profit_factor":           pf,
            "final_equity":            round(self._equity, 4),
            "equity_curve":            eq,
            "monthly_returns":         monthly,
        }
        result["score"] = objective(result)
        return result


# =========================================================================
# BTC Buy & Hold 벤치마크
# =========================================================================

def btc_buy_hold(ohlcv: pd.DataFrame, initial_capital: float) -> dict:
    entry_price = float(ohlcv.iloc[0]["open"])
    exit_price  = float(ohlcv.iloc[-1]["close"])
    qty         = (initial_capital * (1 - EFFECTIVE_FEE)) / entry_price
    final_value = qty * exit_price * (1 - EFFECTIVE_FEE)
    total_pct   = (final_value - initial_capital) / initial_capital * 100.0

    eq     = [qty * float(c) for c in ohlcv["close"]]
    eq[0]  = initial_capital
    mdd    = calc_max_drawdown(eq)
    sharpe = calc_sharpe(eq)

    n_days = (ohlcv.index[-1] - ohlcv.index[0]).days
    n_years = n_days / 365.25
    annual  = calc_annualized_return(total_pct, n_years)
    calmar  = calc_calmar(annual, mdd)

    return {
        "name":                   "BTC_B&H",
        "annualized_return_pct":  round(annual, 4),
        "total_profit_pct":       round(total_pct, 4),
        "sharpe_ratio":           round(sharpe, 4),
        "max_drawdown_pct":       round(mdd, 4),
        "calmar_ratio":           round(calmar, 4),
        "pct_positive_months":    None,
        "total_trades":           1,
        "score":                  None,
        "equity_curve":           eq,
    }


# =========================================================================
# DB 헬퍼
# =========================================================================

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
            SELECT timestamp AS ts, rate
            FROM funding_rate_history
            WHERE symbol = $1
              AND timestamp >= $2 AND timestamp <= $3
            ORDER BY timestamp ASC
            """,
            symbol, start, end,
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts", "rate"])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("ts", inplace=True)
    return df


async def save_result(
    pool: asyncpg.Pool,
    variant_name: str,
    result: dict,
    extra_params: dict,
    start: datetime,
    end: datetime,
) -> None:
    data_range = f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}"
    eq = result.get("equity_curve", [])
    step = max(1, len(eq) // 200)
    eq_sample = [round(_safe_float(v), 2) for v in eq[::step]]

    monthly_dict: dict[str, float] = {}
    for i, r in enumerate(result.get("monthly_returns", [])):
        monthly_dict[str(i)] = _safe_float(r)

    params_json = json.dumps({
        **extra_params,
        "annualized_return_pct":  _safe_float(result.get("annualized_return_pct", 0)),
        "calmar_ratio":           _safe_float(result.get("calmar_ratio", 0)),
        "pct_positive_months":    _safe_float(result.get("pct_positive_months", 0)),
        "score":                  _safe_float(result.get("score", 0)),
        "fa_trades":              result.get("fa_trades", 0),
        "tf_trades":              result.get("tf_trades", 0),
        "equity_curve_sample":    eq_sample,
    })

    try:
        async with pool.acquire() as conn:
            await conn.execute(CREATE_VARIANT_RESULTS)
            await conn.execute(
                """
                INSERT INTO strategy_variant_results
                    (test_name, variant_name, data_range,
                     total_return, sharpe_ratio, max_drawdown,
                     trade_count, win_rate, profit_factor,
                     monthly_returns, params)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
                """,
                TEST_NAME,
                variant_name,
                data_range,
                _safe_float(result.get("total_profit_pct", 0)),
                _safe_float(result.get("sharpe_ratio", 0)),
                _safe_float(result.get("max_drawdown_pct", 0)),
                result.get("total_trades", 0),
                _safe_float(result.get("win_rate", 0)),
                _safe_float(result.get("profit_factor", 0), 0.0),
                json.dumps(monthly_dict),
                params_json,
            )
        log.info("saved", variant=variant_name)
    except Exception as exc:
        log.warning("db_save_failed", variant=variant_name, error=str(exc))


# =========================================================================
# 그리드 서치 — 합리적인 subset
# =========================================================================

def build_grid_subset() -> list[dict[str, tuple[float, float]]]:
    """
    대표적인 4개 조합만 사용 (속도 최적화 — 시뮬레이션당 1분 소요로 전체 4분)
    ranging: FA 보수 vs FA 적극, trending: TF 위주, volatile: 현금 위주
    """
    return [
        # 조합 1: FA 단독 우세 (ranging만 FA 강화, trending에서도 FA 유지)
        {
            "ranging":       (0.40, 0.10),
            "trending_up":   (0.10, 0.50),
            "trending_down": (0.10, 0.50),
            "volatile":      (0.20, 0.00),
        },
        # 조합 2: TF 공격적 (trending에서 TF 극대화)
        {
            "ranging":       (0.30, 0.10),
            "trending_up":   (0.10, 0.60),
            "trending_down": (0.10, 0.40),
            "volatile":      (0.10, 0.00),
        },
        # 조합 3: 균형 (각 레짐에서 FA+TF 균형)
        {
            "ranging":       (0.30, 0.20),
            "trending_up":   (0.20, 0.40),
            "trending_down": (0.10, 0.40),
            "volatile":      (0.30, 0.00),
        },
        # 조합 4: 보수적 현금 위주
        {
            "ranging":       (0.50, 0.00),
            "trending_up":   (0.10, 0.40),
            "trending_down": (0.10, 0.30),
            "volatile":      (0.10, 0.00),
        },
    ]


# =========================================================================
# 메인
# =========================================================================

async def main(args: argparse.Namespace) -> None:
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

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    initial_capital = args.initial_capital
    tf_variant      = args.tf_variant
    n_years         = (end_dt - start_dt).days / 365.25

    log.info("connecting_db", host=os.getenv("DB_HOST", "postgres"))
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as exc:
        print(f"[ERROR] DB 연결 실패: {exc}")
        print("[HINT]  DB_HOST, DB_PASSWORD 환경변수를 확인하세요.")
        sys.exit(1)

    log.info("loading_data", symbol=SYMBOL, start=args.start, end=args.end)
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # 기존 결과 삭제
    try:
        async with pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM strategy_variant_results WHERE test_name = $1",
                TEST_NAME,
            )
            log.info("cleared_previous", deleted=deleted)
    except Exception:
        pass

    all_results: dict[str, dict] = {}

    # ── 고정 비교 설정 실행 ──────────────────────────────────────────────────
    for cfg in FIXED_CONFIGS:
        name = cfg["name"]
        log.info("running_fixed", config=name)
        engine = CombinedEngine(
            ohlcv=ohlcv,
            funding=funding,
            tf_variant=tf_variant,
            fixed_fa_w=cfg["fa_w"],
            fixed_tf_w=cfg["tf_w"],
            initial_capital=initial_capital,
        )
        result = engine.run(compute_regime=False)
        all_results[name] = result
        await save_result(
            pool, name, result,
            {"fa_w": cfg["fa_w"], "tf_w": cfg["tf_w"], "tf_variant": tf_variant},
            start_dt, end_dt,
        )

    # ── 그리드 서치 ─────────────────────────────────────────────────────────
    log.info("running_grid_search")
    grid_configs = build_grid_subset()
    best_score   = float("-inf")
    best_result  = None
    best_wmap    = None

    for i, wmap in enumerate(grid_configs):
        engine = CombinedEngine(
            ohlcv=ohlcv,
            funding=funding,
            tf_variant=tf_variant,
            weight_map=wmap,
            initial_capital=initial_capital,
        )
        result = engine.run(compute_regime=True)
        sc     = result["score"]
        if sc > best_score:
            best_score  = sc
            best_result = result
            best_wmap   = wmap
        if (i + 1) % 4 == 0:
            log.info("grid_progress", done=i + 1, total=len(grid_configs), best_score=round(best_score, 3))

    log.info("grid_search_done", best_score=round(best_score, 3), best_wmap=best_wmap)

    if best_result is not None:
        all_results["결합_최적_그리드"] = best_result
        await save_result(
            pool,
            "결합_최적_그리드",
            best_result,
            {
                "weight_map": {k: list(v) for k, v in (best_wmap or {}).items()},
                "tf_variant": tf_variant,
            },
            start_dt, end_dt,
        )

    # ── BTC B&H ─────────────────────────────────────────────────────────────
    bh = btc_buy_hold(ohlcv, initial_capital)
    all_results["BTC_B&H"] = bh
    await save_result(
        pool, "BTC_B&H", bh, {"type": "benchmark"}, start_dt, end_dt,
    )

    # ── 결과 출력 ────────────────────────────────────────────────────────────
    print()
    print("=" * 100)
    print(f"=== Stage 2: FA + 추세추종 결합 백테스트 ({args.start} ~ {args.end}) ===")
    print(f"TF 변형: {tf_variant}")
    print("=" * 100)

    header = (
        f"{'설정':<16} {'연수익%':>8} {'Sharpe':>7} {'MDD%':>7} "
        f"{'Calmar':>7} {'일관성%':>8} {'거래수':>6} {'Score':>8}"
    )
    print(header)
    print("-" * 100)

    def fmt_row(name: str, r: dict) -> str:
        ann   = _safe_float(r.get("annualized_return_pct", 0))
        sh    = _safe_float(r.get("sharpe_ratio", 0))
        mdd   = _safe_float(r.get("max_drawdown_pct", 0))
        cal   = _safe_float(r.get("calmar_ratio", 0))
        cons  = r.get("pct_positive_months")
        cons_s = f"{cons:.1f}" if cons is not None else "  N/A"
        tr    = r.get("total_trades", 0)
        sc    = r.get("score")
        sc_s  = f"{sc:.2f}" if sc is not None else "  N/A"
        return (
            f"{name:<16} {ann:>8.2f} {sh:>7.3f} {mdd:>7.2f} "
            f"{cal:>7.3f} {cons_s:>8} {tr:>6d} {sc_s:>8}"
        )

    order = [cfg["name"] for cfg in FIXED_CONFIGS] + ["결합_최적_그리드", "BTC_B&H"]
    for name in order:
        if name in all_results:
            print(fmt_row(name, all_results[name]))

    print("=" * 100)

    if best_wmap:
        print()
        print("최적 레짐별 가중치:")
        for regime, (fa_w, tf_w) in best_wmap.items():
            cash_w = 1.0 - fa_w - tf_w
            print(f"  {regime:<16}: FA={fa_w*100:.0f}%, TF={tf_w*100:.0f}%, Cash={cash_w*100:.0f}%")
        print(f"  최적 Score: {best_score:.3f}")

    print()

    await pool.close()
    log.info("stage2_complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 2: FA + TF 결합 백테스트")
    parser.add_argument("--start",           default=START_DATE,      help="시작일 YYYY-MM-DD")
    parser.add_argument("--end",             default=END_DATE,        help="종료일 YYYY-MM-DD")
    parser.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, help="초기 자본 (USDT)")
    parser.add_argument(
        "--tf-variant",
        default="ema_cross",
        choices=["ema_cross", "donchian"],
        help="TF 전략 변형 (기본: ema_cross)",
    )
    args = parser.parse_args()
    asyncio.run(main(args))
