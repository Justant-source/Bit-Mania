"""bt_trend_following.py — Stage 1: 추세추종 5개 변형 독립 백테스트

변형:
  A. ema_cross      : EMA 20/50 크로스
  B. triple_ema     : EMA 20/50/200 삼중 정렬
  C. donchian       : Donchian Breakout (진입96봉, 청산48봉)
  D. adx_momentum   : ADX + RSI 모멘텀 필터
  E. macd_bb        : MACD 히스토그램 전환 + 볼린저밴드

기간: 2020-04-01 ~ 2026-03-31 (6년)
초기 자본: 10,000 USDT
롱+숏 양방향, ATR 기반 포지션 사이징
BTC Buy&Hold 벤치마크 포함
저장: strategy_variant_results 테이블 (test_name="test_11_stage1_tf")

실행 방법:
    # DB 연결 (기본: localhost:5432)
    python bt_trend_following.py

    # 커스텀 기간/자본
    python bt_trend_following.py --start 2020-04-01 --end 2026-03-31 --initial-capital 10000

    # 환경변수로 DB 설정
    DB_HOST=postgres DB_PASSWORD=CryptoEngine2026! python bt_trend_following.py
"""

from __future__ import annotations

import argparse
import asyncio
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
from shared.timezone_utils import kst_timestamper
log = structlog.get_logger(__name__)

# ── 상수 ──────────────────────────────────────────────────────────────────────

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
START_DATE      = "2020-04-01"
END_DATE        = "2026-03-31"
INITIAL_CAPITAL = 10_000.0
WARMUP_BARS     = 200     # EMA200 안정화용
MIN_TRADES_6Y   = 200

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# 서브기간
SUB_PERIODS = {
    "불장_2020_21": ("2020-04-01", "2021-12-31"),
    "폭락_2022":    ("2022-01-01", "2022-12-31"),
    "불장_2023_24": ("2023-01-01", "2024-12-31"),
    "약세_2025H2":  ("2025-07-01", "2026-03-31"),
}

# 공통 리스크 파라미터
COMMON_RISK: dict[str, Any] = {
    "risk_per_trade_pct":       0.01,
    "max_position_pct":         0.20,
    "stop_loss_atr_mult":       2.0,
    "trailing_stop_atr_mult":   2.5,
    "take_profit_atr_mult":     6.0,
    "max_trades_per_day":       3,
    "min_trade_interval_hours": 4,
    "consecutive_loss_cooldown": 3,
    "max_leverage":             2.0,
    "taker_fee_pct":            0.00055,
    "maker_fee_pct":            0.00020,
    "assumed_maker_ratio":      0.5,
    "slippage_pct":             0.0003,
}

# 편도 유효 수수료 = 0.5*0.00055 + 0.5*0.00020 + 0.0003 = 0.000675
EFFECTIVE_FEE = (
    COMMON_RISK["assumed_maker_ratio"]   * COMMON_RISK["maker_fee_pct"] +
    (1 - COMMON_RISK["assumed_maker_ratio"]) * COMMON_RISK["taker_fee_pct"] +
    COMMON_RISK["slippage_pct"]
)

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


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


def calc_sharpe(equity_curve: list[float], periods_per_year: int = 8760) -> float:
    """1h봉 기준 연환산 Sharpe."""
    returns = pd.Series(equity_curve).pct_change().dropna()
    if len(returns) < 2 or returns.std() == 0:
        return 0.0
    return float(
        (returns.mean() * periods_per_year) /
        (returns.std() * math.sqrt(periods_per_year))
    )


def calc_max_drawdown(equity_curve: list[float]) -> float:
    """MDD (%) 계산."""
    eq = pd.Series(equity_curve)
    peak = eq.cummax()
    dd = (eq - peak) / peak * 100
    return float(abs(dd.min()))


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR (EWM 방식)."""
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI."""
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def calc_adx(df: pd.DataFrame, period: int = 14) -> tuple[pd.Series, pd.Series, pd.Series]:
    """ADX, +DI, -DI 계산."""
    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)

    plus_dm  = (high - prev_high).clip(lower=0)
    minus_dm = (prev_low - low).clip(lower=0)
    # +DM > -DM → +DM 유효, 그 외 0
    cond_plus  = (plus_dm > minus_dm) & (plus_dm > 0)
    cond_minus = (minus_dm > plus_dm) & (minus_dm > 0)
    plus_dm  = plus_dm.where(cond_plus,  0.0)
    minus_dm = minus_dm.where(cond_minus, 0.0)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    smooth_tr   = tr.ewm(span=period, adjust=False).mean()
    smooth_plus  = plus_dm.ewm(span=period, adjust=False).mean()
    smooth_minus = minus_dm.ewm(span=period, adjust=False).mean()

    plus_di  = 100 * smooth_plus  / smooth_tr.replace(0, float("nan"))
    minus_di = 100 * smooth_minus / smooth_tr.replace(0, float("nan"))

    dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float("nan"))
    adx = dx.ewm(span=period, adjust=False).mean()

    return adx.fillna(0), plus_di.fillna(0), minus_di.fillna(0)


def check_trade_frequency(total_trades: int, n_years: float = 6.0) -> str:
    if total_trades < MIN_TRADES_6Y:
        return f"탈락: {total_trades}회 < 최소 {MIN_TRADES_6Y}회"
    avg_per_window = total_trades / (n_years * 365 / 90)
    if avg_per_window < 10:
        return f"경고: 윈도우당 {avg_per_window:.1f}회 < 최소 10회"
    return f"통과: {total_trades}회, 윈도우당 {avg_per_window:.1f}회"


def calc_profit_factor(trades: list[dict]) -> float:
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss   = abs(sum(t["pnl"] for t in trades if t["pnl"] <= 0))
    if gross_loss == 0:
        return float("inf")
    return gross_profit / gross_loss


def calculate_position_size(
    equity: float,
    entry_price: float,
    atr: float,
    risk_pct: float   = 0.01,
    stop_atr_mult: float = 2.0,
    max_position_pct: float = 0.20,
    max_leverage: float    = 2.0,
) -> float:
    """ATR 기반 포지션 사이징 (BTC 단위)."""
    if atr <= 0 or entry_price <= 0:
        return 0.0
    risk_amount  = equity * risk_pct
    stop_dist    = atr * stop_atr_mult
    raw_size     = risk_amount / stop_dist
    max_notional = (equity * max_position_pct) / entry_price
    max_leverage_ = (equity * max_leverage) / entry_price
    return min(raw_size, max_notional, max_leverage_)


# ── 공통 이벤트 루프 ──────────────────────────────────────────────────────────

class TrendFollowingEngine:
    """추세추종 공통 백테스트 이벤트 루프."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        variant: str,
        params: dict[str, Any],
        initial_capital: float = INITIAL_CAPITAL,
    ) -> None:
        self.df             = ohlcv.copy()
        self.variant        = variant
        self.params         = {**COMMON_RISK, **params}
        self.initial_capital = initial_capital

        self._equity        = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[dict] = []
        self._position: dict[str, Any] | None = None

        # 쿨다운
        self._consecutive_losses = 0
        self._cooldown_until: pd.Timestamp | None = None

        # 일별 거래 수 추적
        self._trades_today  = 0
        self._last_trade_day: pd.Timestamp | None = None
        self._last_entry_ts: pd.Timestamp | None  = None

        # 지표 사전 계산
        self._indicators: dict[str, pd.Series] = {}
        self._precompute_indicators()

    # ------------------------------------------------------------------
    # 지표 사전 계산
    # ------------------------------------------------------------------

    def _precompute_indicators(self) -> None:
        df  = self.df
        v   = self.variant

        self._indicators["atr"]   = calc_atr(df, 14)
        self._indicators["close"] = df["close"]

        if v in ("ema_cross", "triple_ema"):
            closes = df["close"]
            self._indicators["ema20"]  = closes.ewm(span=20,  adjust=False).mean()
            self._indicators["ema50"]  = closes.ewm(span=50,  adjust=False).mean()
            self._indicators["ema200"] = closes.ewm(span=200, adjust=False).mean()

        elif v == "donchian":
            self._indicators["upper96"] = df["high"].rolling(96).max()
            self._indicators["lower96"] = df["low"].rolling(96).min()
            self._indicators["upper48"] = df["high"].rolling(48).max()
            self._indicators["lower48"] = df["low"].rolling(48).min()

        elif v == "adx_momentum":
            adx, plus_di, minus_di = calc_adx(df, 14)
            self._indicators["adx"]      = adx
            self._indicators["plus_di"]  = plus_di
            self._indicators["minus_di"] = minus_di
            self._indicators["rsi"]      = calc_rsi(df["close"], 14)

        elif v == "macd_bb":
            closes = df["close"]
            ema12  = closes.ewm(span=12, adjust=False).mean()
            ema26  = closes.ewm(span=26, adjust=False).mean()
            macd_line   = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            self._indicators["macd_hist"] = macd_line - signal_line
            bb_mid = closes.rolling(20).mean()
            bb_std = closes.rolling(20).std()
            self._indicators["bb_mid"]   = bb_mid
            self._indicators["bb_upper"] = bb_mid + 2.0 * bb_std
            self._indicators["bb_lower"] = bb_mid - 2.0 * bb_std

    # ------------------------------------------------------------------
    # 메인 루프
    # ------------------------------------------------------------------

    def run(self) -> dict:
        df  = self.df
        n   = len(df)

        trail_stop: float | None = None

        for idx in range(WARMUP_BARS, n):
            row      = df.iloc[idx]
            close    = float(row["close"])
            atr_val  = float(self._indicators["atr"].iloc[idx])
            ts       = df.index[idx]

            # 일별 거래 수 초기화
            trade_day = pd.Timestamp(ts).date()
            if self._last_trade_day is None or trade_day != self._last_trade_day:
                self._trades_today  = 0
                self._last_trade_day = trade_day

            # ── 포지션 청산 체크 ──────────────────────────────────────
            if self._position is not None:
                direction = self._position["direction"]

                # 트레일링 스탑 업데이트
                tsmult = self.params["trailing_stop_atr_mult"]
                if direction == 1:  # Long
                    new_stop = close - atr_val * tsmult
                    if trail_stop is None:
                        trail_stop = new_stop
                    else:
                        trail_stop = max(trail_stop, new_stop)
                    hit_stop = (close <= trail_stop)
                else:              # Short
                    new_stop = close + atr_val * tsmult
                    if trail_stop is None:
                        trail_stop = new_stop
                    else:
                        trail_stop = min(trail_stop, new_stop)
                    hit_stop = (close >= trail_stop)

                # Take Profit
                tp_mult   = self.params["take_profit_atr_mult"]
                entry     = self._position["entry_price"]
                if direction == 1:
                    hit_tp = close >= entry + atr_val * tp_mult
                else:
                    hit_tp = close <= entry - atr_val * tp_mult

                # 전략별 추가 청산 조건
                strategy_exit = self._check_exit(idx, direction)

                if hit_stop or hit_tp or strategy_exit:
                    self._close_position(close, ts, atr_val)
                    trail_stop = None
                    continue

            # ── 진입 체크 ─────────────────────────────────────────────
            if self._position is None:
                # 쿨다운 체크
                if self._cooldown_until is not None:
                    ts_pd = pd.Timestamp(ts)
                    if ts_pd < self._cooldown_until:
                        self._equity_curve.append(self._equity)
                        continue

                # 일별 최대 거래 수 체크
                if self._trades_today >= self.params["max_trades_per_day"]:
                    self._equity_curve.append(self._equity)
                    continue

                # 최소 거래 간격 체크
                if self._last_entry_ts is not None:
                    min_interval = timedelta(
                        hours=self.params["min_trade_interval_hours"]
                    )
                    if pd.Timestamp(ts) - self._last_entry_ts < min_interval:
                        self._equity_curve.append(self._equity)
                        continue

                direction = self._check_entry(idx)
                if direction != 0 and atr_val > 0:
                    qty = calculate_position_size(
                        equity=self._equity,
                        entry_price=close,
                        atr=atr_val,
                        risk_pct=self.params["risk_per_trade_pct"],
                        stop_atr_mult=self.params["stop_loss_atr_mult"],
                        max_position_pct=self.params["max_position_pct"],
                        max_leverage=self.params["max_leverage"],
                    )
                    if qty > 0:
                        self._open_position(close, direction, qty, ts, atr_val)
                        trail_stop = None
                        self._trades_today  += 1
                        self._last_entry_ts  = pd.Timestamp(ts)

            self._equity_curve.append(
                self._equity + self._unrealized_pnl(close)
            )

        # 강제 청산
        if self._position is not None:
            last_close = float(df["close"].iloc[-1])
            last_atr   = float(self._indicators["atr"].iloc[-1])
            self._close_position(last_close, df.index[-1], last_atr)
        if self._equity_curve:
            self._equity_curve[-1] = self._equity

        return self._build_result()

    # ------------------------------------------------------------------
    # 전략별 진입/청산 신호
    # ------------------------------------------------------------------

    def _check_entry(self, idx: int) -> int:
        """진입 신호 반환 (1=Long, -1=Short, 0=없음)."""
        v = self.variant

        if v == "ema_cross":
            ema20 = float(self._indicators["ema20"].iloc[idx])
            ema50 = float(self._indicators["ema50"].iloc[idx])
            close = float(self._indicators["close"].iloc[idx])
            if ema20 > ema50 and close > ema20:
                return 1
            if ema20 < ema50 and close < ema20:
                return -1

        elif v == "triple_ema":
            ema20  = float(self._indicators["ema20"].iloc[idx])
            ema50  = float(self._indicators["ema50"].iloc[idx])
            ema200 = float(self._indicators["ema200"].iloc[idx])
            close  = float(self._indicators["close"].iloc[idx])
            if ema20 > ema50 > ema200 and close > ema20:
                return 1
            if ema20 < ema50 < ema200 and close < ema20:
                return -1

        elif v == "donchian":
            if idx < 96:
                return 0
            upper96 = float(self._indicators["upper96"].iloc[idx - 1])
            lower96 = float(self._indicators["lower96"].iloc[idx - 1])
            close   = float(self._indicators["close"].iloc[idx])
            if not math.isnan(upper96) and close > upper96:
                return 1
            if not math.isnan(lower96) and close < lower96:
                return -1

        elif v == "adx_momentum":
            adx      = float(self._indicators["adx"].iloc[idx])
            plus_di  = float(self._indicators["plus_di"].iloc[idx])
            minus_di = float(self._indicators["minus_di"].iloc[idx])
            rsi      = float(self._indicators["rsi"].iloc[idx])
            if adx > 25 and plus_di > minus_di and rsi > 50:
                return 1
            if adx > 25 and minus_di > plus_di and rsi < 50:
                return -1

        elif v == "macd_bb":
            if idx < 1:
                return 0
            hist_now  = float(self._indicators["macd_hist"].iloc[idx])
            hist_prev = float(self._indicators["macd_hist"].iloc[idx - 1])
            bb_mid    = float(self._indicators["bb_mid"].iloc[idx])
            close     = float(self._indicators["close"].iloc[idx])
            # 히스토그램 양전환 (음→양)
            if hist_prev <= 0 < hist_now and close > bb_mid:
                return 1
            # 히스토그램 음전환 (양→음)
            if hist_prev >= 0 > hist_now and close < bb_mid:
                return -1

        return 0

    def _check_exit(self, idx: int, direction: int) -> bool:
        """추가 청산 조건 (전략별)."""
        v = self.variant

        if v == "ema_cross":
            ema20 = float(self._indicators["ema20"].iloc[idx])
            ema50 = float(self._indicators["ema50"].iloc[idx])
            if direction == 1 and ema20 < ema50:
                return True
            if direction == -1 and ema20 > ema50:
                return True

        elif v == "triple_ema":
            ema20  = float(self._indicators["ema20"].iloc[idx])
            ema50  = float(self._indicators["ema50"].iloc[idx])
            ema200 = float(self._indicators["ema200"].iloc[idx])
            if direction == 1 and not (ema20 > ema50 > ema200):
                return True
            if direction == -1 and not (ema20 < ema50 < ema200):
                return True

        elif v == "donchian":
            if idx < 48:
                return False
            upper48 = float(self._indicators["upper48"].iloc[idx - 1])
            lower48 = float(self._indicators["lower48"].iloc[idx - 1])
            close   = float(self._indicators["close"].iloc[idx])
            if direction == 1 and not math.isnan(lower48) and close < lower48:
                return True
            if direction == -1 and not math.isnan(upper48) and close > upper48:
                return True

        elif v == "adx_momentum":
            adx = float(self._indicators["adx"].iloc[idx])
            rsi = float(self._indicators["rsi"].iloc[idx])
            if adx < 20:
                return True
            if direction == 1 and rsi < 40:
                return True
            if direction == -1 and rsi > 60:
                return True

        elif v == "macd_bb":
            if idx < 1:
                return False
            hist_now  = float(self._indicators["macd_hist"].iloc[idx])
            hist_prev = float(self._indicators["macd_hist"].iloc[idx - 1])
            close     = float(self._indicators["close"].iloc[idx])
            bb_upper  = float(self._indicators["bb_upper"].iloc[idx])
            bb_lower  = float(self._indicators["bb_lower"].iloc[idx])
            # MACD 반전 신호
            if direction == 1 and hist_prev >= 0 > hist_now:
                return True
            if direction == -1 and hist_prev <= 0 < hist_now:
                return True
            # BB 반대 밴드 도달
            if direction == 1 and not math.isnan(bb_upper) and close >= bb_upper:
                return True
            if direction == -1 and not math.isnan(bb_lower) and close <= bb_lower:
                return True

        return False

    # ------------------------------------------------------------------
    # 포지션 관리
    # ------------------------------------------------------------------

    def _open_position(
        self,
        price: float,
        direction: int,
        qty: float,
        ts: Any,
        atr: float,
    ) -> None:
        fee = price * qty * EFFECTIVE_FEE
        self._equity -= fee
        self._position = {
            "direction":   direction,
            "entry_price": price,
            "qty":         qty,
            "entry_ts":    ts,
            "fee_paid":    fee,
            "entry_atr":   atr,
        }

    def _close_position(self, price: float, ts: Any, atr: float) -> None:
        if self._position is None:
            return
        direction  = self._position["direction"]
        entry      = self._position["entry_price"]
        qty        = self._position["qty"]
        fee_entry  = self._position["fee_paid"]
        fee_exit   = price * qty * EFFECTIVE_FEE
        self._equity -= fee_exit

        # pnl = (exit-entry)/entry * qty * entry_price * direction
        raw_pnl = (price - entry) * qty * direction
        pnl     = raw_pnl - fee_exit  # fee_entry 이미 진입 시 차감

        self._equity += raw_pnl  # 원래 자본에 손익 반영

        if pnl > 0:
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1
            if self._consecutive_losses >= self.params["consecutive_loss_cooldown"]:
                self._cooldown_until = pd.Timestamp(ts) + timedelta(hours=24)
                self._consecutive_losses = 0

        entry_ts = self._position.get("entry_ts")
        try:
            dur_hours = float(
                (pd.Timestamp(ts) - pd.Timestamp(entry_ts)).total_seconds() / 3600
            )
        except Exception:
            dur_hours = 0.0

        self._trades.append({
            "entry_price": entry,
            "exit_price":  price,
            "direction":   direction,
            "qty":         qty,
            "pnl":         pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(entry_ts),
            "close_ts":    str(ts),
            "dur_hours":   dur_hours,
        })
        self._position = None

    def _unrealized_pnl(self, close: float) -> float:
        if self._position is None:
            return 0.0
        direction = self._position["direction"]
        entry     = self._position["entry_price"]
        qty       = self._position["qty"]
        return (close - entry) * qty * direction

    # ------------------------------------------------------------------
    # 결과 빌드
    # ------------------------------------------------------------------

    def _build_result(self) -> dict:
        total_profit = self._equity - self.initial_capital
        n_trades     = len(self._trades)
        winning      = [t for t in self._trades if t["pnl"] > 0]
        total_pct    = (total_profit / self.initial_capital * 100) \
                       if self.initial_capital > 0 else 0.0
        win_rate     = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf           = calc_profit_factor(self._trades)
        mdd          = calc_max_drawdown(self._equity_curve)
        sharpe       = calc_sharpe(self._equity_curve)
        freq_check   = check_trade_frequency(n_trades)

        return {
            "variant":         self.variant,
            "initial_capital": self.initial_capital,
            "final_equity":    round(self._equity, 4),
            "total_profit":    round(total_profit, 4),
            "total_profit_pct": round(total_pct, 4),
            "sharpe_ratio":    round(sharpe, 4),
            "max_drawdown_pct": round(mdd, 4),
            "win_rate":        round(win_rate, 2),
            "total_trades":    n_trades,
            "profit_factor":   pf,
            "equity_curve":    self._equity_curve,
            "trades":          self._trades,
            "freq_check":      freq_check,
        }


# ── BTC Buy & Hold 벤치마크 ────────────────────────────────────────────────────

def btc_buy_hold(
    ohlcv: pd.DataFrame,
    initial_capital: float,
    start: datetime,
    end: datetime,
) -> dict:
    """2020-04-01 오픈 가격에 전액 매수, 2026-03-31에 청산."""
    entry_price = float(ohlcv.iloc[0]["open"])
    exit_price  = float(ohlcv.iloc[-1]["close"])
    qty         = (initial_capital * (1 - EFFECTIVE_FEE)) / entry_price
    final_value = qty * exit_price * (1 - EFFECTIVE_FEE)
    total_pct   = (final_value - initial_capital) / initial_capital * 100.0

    # equity curve: 매봉 종가 * qty
    equity_curve = [qty * float(c) for c in ohlcv["close"]]
    equity_curve[0] = initial_capital

    mdd    = calc_max_drawdown(equity_curve)
    sharpe = calc_sharpe(equity_curve)

    n_years = (end - start).days / 365.25
    factor  = 1.0 + total_pct / 100.0
    annual  = ((factor ** (1.0 / n_years)) - 1.0) * 100.0 if (factor > 0 and n_years > 0) else 0.0

    return {
        "variant":          "btc_buy_hold",
        "initial_capital":  initial_capital,
        "final_equity":     round(final_value, 4),
        "total_profit_pct": round(total_pct, 4),
        "annual_return_pct": round(annual, 4),
        "sharpe_ratio":     round(sharpe, 4),
        "max_drawdown_pct": round(mdd, 4),
        "win_rate":         None,
        "total_trades":     1,
        "profit_factor":    None,
        "equity_curve":     equity_curve,
    }


# ── 서브기간 수익률 ───────────────────────────────────────────────────────────

def subperiod_return(
    equity_curve: list[float],
    ohlcv: pd.DataFrame,
    start_str: str,
    end_str: str,
) -> float:
    start_dt = pd.Timestamp(start_str, tz="UTC")
    end_dt   = pd.Timestamp(end_str,   tz="UTC")
    idx_all  = ohlcv.index

    mask_s = idx_all >= start_dt
    mask_e = idx_all <= end_dt

    if not mask_s.any() or not mask_e.any():
        return 0.0

    i_start = int(np.argmax(np.array(mask_s)))
    i_end   = int(len(idx_all) - 1 - np.argmax(np.array(mask_e)[::-1]))

    # equity_curve의 인덱스 오프셋 (WARMUP_BARS 고려)
    offset = WARMUP_BARS
    eq_s = max(0, i_start - offset + 1)
    eq_e = min(len(equity_curve) - 1, i_end - offset + 1)

    if eq_s >= len(equity_curve) or eq_e < eq_s:
        return 0.0

    v_start = equity_curve[eq_s]
    v_end   = equity_curve[eq_e]
    if v_start <= 0:
        return 0.0
    return (v_end - v_start) / v_start * 100.0


# ── DB 저장 ───────────────────────────────────────────────────────────────────

TEST_NAME = "test_11_stage1_tf"


async def save_variant(
    pool: asyncpg.Pool,
    result: dict,
    params_extra: dict,
    start: datetime,
    end: datetime,
) -> None:
    variant_name = result["variant"]
    data_range   = f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}"

    eq = result.get("equity_curve", [])
    step = max(1, len(eq) // 200)
    eq_sample = [round(_safe_float(v), 2) for v in eq[::step]]

    params_json = json.dumps({
        **params_extra,
        "equity_curve_sample": eq_sample,
        "freq_check":  result.get("freq_check", ""),
        "profit_factor": _safe_float(result.get("profit_factor", 0.0), 0.0),
        "win_rate": _safe_float(result.get("win_rate", 0.0)),
        "total_trades": result.get("total_trades", 0),
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
                _safe_float(result["total_profit_pct"]),
                _safe_float(result["sharpe_ratio"]),
                _safe_float(result["max_drawdown_pct"]),
                result.get("total_trades", 0),
                _safe_float(result.get("win_rate") or 0.0),
                _safe_float(result.get("profit_factor") or 0.0, 0.0),
                "{}",
                params_json,
            )
        log.info("saved", variant=variant_name)
    except Exception as exc:
        log.warning("db_save_failed", variant=variant_name, error=str(exc))


# ── DB 로드 ───────────────────────────────────────────────────────────────────

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


# ── 연수익률 계산 ─────────────────────────────────────────────────────────────

def calc_annual_return(total_pct: float, n_years: float = 6.0) -> float:
    if n_years <= 0:
        return 0.0
    factor = 1.0 + total_pct / 100.0
    if factor <= 0:
        return -100.0
    return ((factor ** (1.0 / n_years)) - 1.0) * 100.0


# ── 메인 ──────────────────────────────────────────────────────────────────────

VARIANTS_CONFIG: dict[str, dict[str, Any]] = {
    "ema_cross":    {"fast_period": 20, "slow_period": 50},
    "triple_ema":   {"fast_period": 20, "mid_period": 50, "slow_period": 200,
                     "stop_loss_atr_mult": 2.5},
    "donchian":     {"entry_period": 96, "exit_period": 48},
    "adx_momentum": {"adx_period": 14, "adx_threshold": 25, "rsi_period": 14},
    "macd_bb":      {"macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
                     "bb_period": 20, "bb_std": 2.0},
}


async def main(args: argparse.Namespace) -> None:
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
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    initial_capital = args.initial_capital
    n_years  = (end_dt - start_dt).days / 365.25

    log.info("connecting_db", host=os.getenv("DB_HOST", "postgres"))
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
    except Exception as exc:
        print(f"[ERROR] DB 연결 실패: {exc}")
        print("[HINT]  DB_HOST, DB_PASSWORD 환경변수를 확인하세요.")
        sys.exit(1)

    log.info("loading_ohlcv", symbol=SYMBOL, start=args.start, end=args.end)
    ohlcv = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        await pool.close()
        sys.exit(1)

    log.info("ohlcv_loaded", rows=len(ohlcv))

    # 기존 결과 삭제
    try:
        async with pool.acquire() as conn:
            await conn.execute(CREATE_VARIANT_RESULTS)
            deleted = await conn.execute(
                "DELETE FROM strategy_variant_results WHERE test_name = $1",
                TEST_NAME,
            )
            log.info("cleared_previous", deleted=deleted)
    except Exception as exc:
        log.warning("clear_failed", error=str(exc))

    # ── 변형별 백테스트 실행 ──────────────────────────────────────────
    results: dict[str, dict] = {}

    for variant_name, variant_params in VARIANTS_CONFIG.items():
        log.info("running_variant", variant=variant_name)
        engine = TrendFollowingEngine(
            ohlcv=ohlcv,
            variant=variant_name,
            params=variant_params,
            initial_capital=initial_capital,
        )
        result = engine.run()
        results[variant_name] = result

        # 서브기간 수익률 계산
        sub_returns: dict[str, float] = {}
        for sub_label, (sub_s, sub_e) in SUB_PERIODS.items():
            ret = subperiod_return(result["equity_curve"], ohlcv, sub_s, sub_e)
            sub_returns[sub_label] = round(ret, 2)
        result["sub_returns"] = sub_returns

        params_extra = {
            **variant_params,
            "sub_period_returns": sub_returns,
        }
        await save_variant(pool, result, params_extra, start_dt, end_dt)
        log.info(
            "variant_done",
            variant=variant_name,
            return_pct=round(result["total_profit_pct"], 2),
            sharpe=round(result["sharpe_ratio"], 4),
            mdd=round(result["max_drawdown_pct"], 2),
            trades=result["total_trades"],
        )

    # ── BTC Buy & Hold 벤치마크 ───────────────────────────────────────
    log.info("running_btc_buy_hold")
    bh = btc_buy_hold(ohlcv, initial_capital, start_dt, end_dt)
    bh["sub_returns"] = {
        sub_label: round(subperiod_return(bh["equity_curve"], ohlcv, sub_s, sub_e), 2)
        for sub_label, (sub_s, sub_e) in SUB_PERIODS.items()
    }
    results["btc_buy_hold"] = bh
    await save_variant(
        pool, bh,
        {"type": "benchmark", "sub_period_returns": bh["sub_returns"]},
        start_dt, end_dt,
    )

    # ── 결과 출력 ─────────────────────────────────────────────────────
    print()
    print("=" * 100)
    print("=== Stage 1: 추세추종 5개 변형 독립 백테스트 ===")
    print("=" * 100)
    print()

    col_w = [14, 9, 9, 8, 7, 7, 6, 7, 30]
    header = (
        f"{'변형':<{col_w[0]}} | "
        f"{'수익률%':>{col_w[1]}} | "
        f"{'연수익%':>{col_w[2]}} | "
        f"{'Sharpe':>{col_w[3]}} | "
        f"{'MDD%':>{col_w[4]}} | "
        f"{'승률%':>{col_w[5]}} | "
        f"{'PF':>{col_w[6]}} | "
        f"{'거래수':>{col_w[7]}} | "
        f"{'빈도검증'}"
    )
    print(header)
    print("-" * len(header))

    stage2_candidates: list[str] = []

    for vname, r in results.items():
        total_pct = _safe_float(r["total_profit_pct"])
        ann_pct   = calc_annual_return(total_pct, n_years)
        sharpe    = _safe_float(r["sharpe_ratio"])
        mdd       = _safe_float(r["max_drawdown_pct"])
        win_rate  = _safe_float(r.get("win_rate") or 0.0)
        pf_val    = r.get("profit_factor")
        pf_str    = f"{_safe_float(pf_val):.2f}" if pf_val is not None and not math.isinf(pf_val or 0) else ("inf" if pf_val is not None else " — ")
        trades    = r.get("total_trades", 0)
        freq      = r.get("freq_check", " — ")

        if "통과" in (freq or ""):
            stage2_candidates.append(vname)

        if vname == "btc_buy_hold":
            freq = " — (벤치마크)"

        print(
            f"{vname:<{col_w[0]}} | "
            f"{total_pct:>+{col_w[1]}.2f}% | "
            f"{ann_pct:>+{col_w[2]}.2f}% | "
            f"{sharpe:>{col_w[3]}.3f} | "
            f"{mdd:>{col_w[4]}.2f}% | "
            f"{win_rate:>{col_w[5]}.1f}% | "
            f"{pf_str:>{col_w[6]}} | "
            f"{trades:>{col_w[7]}d} | "
            f"{freq}"
        )

    print("=" * len(header))
    print()

    # ── 서브기간별 수익률 ─────────────────────────────────────────────
    sub_keys = list(SUB_PERIODS.keys())
    sub_header = f"{'변형':<14} | " + " | ".join(f"{k:>14}" for k in sub_keys)
    print("--- 서브기간별 수익률 ---")
    print(sub_header)
    print("-" * len(sub_header))
    for vname, r in results.items():
        sub = r.get("sub_returns", {})
        row = f"{vname:<14} | " + " | ".join(
            f"{_safe_float(sub.get(k, 0.0)):>+13.2f}%" for k in sub_keys
        )
        print(row)
    print("-" * len(sub_header))
    print()

    # ── Stage 2 진출 후보 ─────────────────────────────────────────────
    print("--- 거래 빈도 200회+ 통과 변형 (Stage 2 진출) ---")
    if stage2_candidates:
        print("  " + ", ".join(stage2_candidates))
    else:
        print("  (없음 — 파라미터 조정 필요)")
    print()
    print(f"[DB 저장 완료] test_name='{TEST_NAME}'")
    print()

    await pool.close()
    log.info("stage1_complete")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 1: 추세추종 5개 변형 독립 백테스트"
    )
    parser.add_argument("--start",           default=START_DATE,
                        help="시작일 YYYY-MM-DD (기본: 2020-04-01)")
    parser.add_argument("--end",             default=END_DATE,
                        help="종료일 YYYY-MM-DD (기본: 2026-03-31)")
    parser.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float,
                        dest="initial_capital",
                        help="초기 자본 USDT (기본: 10000)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 중단")
        sys.exit(0)
