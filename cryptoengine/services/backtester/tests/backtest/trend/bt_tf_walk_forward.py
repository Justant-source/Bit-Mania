"""bt_tf_walk_forward.py — Stage 4: Walk-Forward 검증 (22개 윈도우)

역할:
  - FA + TF 결합 전략의 Walk-Forward 검증
  - 22개 윈도우 (train=180일, test=90일)
  - 전략: fa_only, fa_plus_tf(ema_cross), fa_plus_tf(donchian)
  - 윈도우당 최소 거래수 10회 검증 (Test I 교훈)
  - 파라미터 민감도 분석 (±20%)

기간: 2020-04-01 ~ 2026-03-31
초기 자본: 10,000 USDT
저장: strategy_variant_results 테이블 (test_name="test_11_stage4_wf")

실행 방법:
    python bt_tf_walk_forward.py
    python bt_tf_walk_forward.py --start 2020-04-01 --end 2026-03-31 --initial-capital 10000
    python bt_tf_walk_forward.py --tf-variant donchian
    DB_HOST=postgres DB_PASSWORD=CryptoEngine2026! python bt_tf_walk_forward.py
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
WARMUP_BARS     = 200
TEST_NAME       = "test_11_stage4_wf"

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# Walk-Forward 설정
WF_CONFIG = {
    "train_days":     180,
    "test_days":      90,
    "total_windows":  22,
    "start_date":     "2020-04-01",
    "end_date":       "2026-03-31",
}

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

FA_FEE_RATE = 0.00055

# FA 파라미터 (short_hold 변형)
FA_PARAMS: dict[str, Any] = {
    "exit_on_flip":               True,
    "negative_hours_before_exit": 0,
    "consecutive_intervals":      3,
    "min_funding_rate":           0.0001,
    "max_hold_bars":              168,
}

# WF 통과 기준
WF_PASS_CRITERIA = {
    "fa_only": {
        "oos_sharpe":        1.0,
        "oos_annual_return": 2.0,
        "oos_mdd":           5.0,
        "consistency":       70.0,
    },
    "fa_plus_tf": {
        "oos_sharpe":        1.0,
        "oos_annual_return": 10.0,
        "oos_mdd":           15.0,
        "consistency":       60.0,
    },
}

# 파라미터 민감도 분석 설정
SENSITIVITY_PARAMS = {
    "ema_fast_period":      [16, 20, 24],
    "ema_slow_period":      [40, 50, 60],
    "atr_stop_multiplier":  [1.6, 2.0, 2.4],
    "risk_per_trade_pct":   [0.008, 0.01, 0.012],
}

# DDL
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


# =========================================================================
# 유틸 함수
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
    high       = df["high"]
    low        = df["low"]
    close      = df["close"]
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
    risk_pct: float    = 0.01,
    stop_atr_mult: float = 2.0,
    max_position_pct: float = 0.20,
    max_leverage: float = 2.0,
) -> float:
    if atr <= 0 or entry_price <= 0:
        return 0.0
    risk_amount  = equity * risk_pct
    stop_dist    = atr * stop_atr_mult
    raw_size     = risk_amount / stop_dist
    max_notional = (equity * max_position_pct) / entry_price
    max_lev_size = (equity * max_leverage) / entry_price
    return min(raw_size, max_notional, max_lev_size)


# =========================================================================
# WF 윈도우 생성
# =========================================================================

def generate_wf_windows(
    start_date: str,
    end_date: str,
    train_days: int   = 180,
    test_days: int    = 90,
    total_windows: int = 22,
) -> list[dict]:
    """22개 Walk-Forward 윈도우 생성."""
    windows: list[dict] = []
    oos_start = pd.Timestamp(start_date, tz="UTC") + pd.Timedelta(days=train_days)

    for i in range(total_windows):
        train_start = oos_start - pd.Timedelta(days=train_days) + pd.Timedelta(days=i * test_days)
        train_end   = train_start + pd.Timedelta(days=train_days)
        oos_end     = train_end   + pd.Timedelta(days=test_days)

        if oos_end > pd.Timestamp(end_date, tz="UTC"):
            break

        windows.append({
            "window_id":   i + 1,
            "train_start": train_start,
            "train_end":   train_end,
            "oos_start":   train_end,
            "oos_end":     oos_end,
        })

    return windows


# =========================================================================
# 지표 사전 계산 (TF용)
# =========================================================================

def precompute_tf_indicators(
    df: pd.DataFrame,
    variant: str,
    ema_fast: int = 20,
    ema_slow: int = 50,
) -> dict[str, pd.Series]:
    ind: dict[str, pd.Series] = {}
    ind["atr"]   = calc_atr(df, 14)
    ind["close"] = df["close"]

    if variant == "ema_cross":
        closes        = df["close"]
        ind["ema_fast"] = closes.ewm(span=ema_fast, adjust=False).mean()
        ind["ema_slow"] = closes.ewm(span=ema_slow, adjust=False).mean()

    elif variant == "donchian":
        ind["upper96"] = df["high"].rolling(96).max()
        ind["lower96"] = df["low"].rolling(96).min()
        ind["upper48"] = df["high"].rolling(48).max()
        ind["lower48"] = df["low"].rolling(48).min()

    return ind


def tf_check_entry(idx: int, variant: str, ind: dict[str, pd.Series]) -> int:
    """반환: 1=Long, -1=Short, 0=없음."""
    if variant == "ema_cross":
        fast  = float(ind["ema_fast"].iloc[idx])
        slow  = float(ind["ema_slow"].iloc[idx])
        close = float(ind["close"].iloc[idx])
        if fast > slow and close > fast:
            return 1
        if fast < slow and close < fast:
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


def tf_check_exit(
    idx: int,
    variant: str,
    ind: dict[str, pd.Series],
    direction: int,
) -> bool:
    """전략별 추가 청산 조건."""
    if variant == "ema_cross":
        fast = float(ind["ema_fast"].iloc[idx])
        slow = float(ind["ema_slow"].iloc[idx])
        if direction == 1 and fast < slow:
            return True
        if direction == -1 and fast > slow:
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
# FA 엔진
# =========================================================================

class FAEngine:
    """FA short_hold 전략 단독 실행기."""

    PARAMS: dict[str, Any] = {
        "exit_on_flip":               True,
        "consecutive_intervals":      3,
        "min_funding_rate":           0.0001,
        "max_hold_bars":              168,
        "fee_rate":                   0.00055,
    }

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float = 10_000.0,
        fa_capital_ratio: float = 1.0,
    ) -> None:
        self.ohlcv            = ohlcv
        self.funding          = funding
        self.initial_capital  = initial_capital * fa_capital_ratio
        self._equity          = self.initial_capital
        self._equity_curve: list[float] = [self.initial_capital]
        self._trades: list[dict]        = []
        self._position: dict[str, Any] | None = None
        self._pos_consec = 0
        self._neg_consec = 0

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

    def run(self) -> dict:
        p         = self.PARAMS
        fee_rate  = p["fee_rate"]
        consec    = p["consecutive_intervals"]
        min_rate  = p["min_funding_rate"]
        max_hold  = p["max_hold_bars"]
        df        = self.ohlcv
        n         = len(df)

        for idx in range(20, n):
            row   = df.iloc[idx]
            ts    = df.index[idx]
            close = float(row["close"])
            funding = self._get_funding_rate(ts)

            is_settlement = (ts.hour % 8 == 0) and (ts.minute == 0)

            # 펀딩비 정산
            if self._position is not None and is_settlement:
                direction = self._position.get("funding_direction", 1)
                pos_value = self._position["size"] * self._position["entry_price"]
                net_fund  = pos_value * funding * direction
                self._equity += net_fund
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_fund
                )

            if self._position is None:
                if is_settlement:
                    if funding >= min_rate:
                        self._pos_consec += 1
                        self._neg_consec  = 0
                    elif funding <= -min_rate:
                        self._neg_consec  += 1
                        self._pos_consec   = 0
                    else:
                        self._pos_consec = 0
                        self._neg_consec  = 0

                    if self._pos_consec >= consec:
                        self._open("sell", close, ts, idx, fee_rate)
                        self._pos_consec = 0
                    elif self._neg_consec >= consec:
                        self._open("buy", close, ts, idx, fee_rate)
                        self._neg_consec = 0
            else:
                direction    = self._position.get("funding_direction", 1)
                bars_held    = idx - self._position.get("entry_idx", idx)
                reversed_now = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)

                should_close = False
                if is_settlement:
                    if reversed_now:
                        self._position["reverse_count"] = self._position.get("reverse_count", 0) + 1
                    else:
                        self._position["reverse_count"] = 0
                    if self._position.get("reverse_count", 0) >= 3:
                        should_close = True

                if bars_held >= max_hold:
                    should_close = True

                if should_close:
                    self._close(close, ts, fee_rate)
                    self._pos_consec = 0
                    self._neg_consec  = 0

            self._equity_curve.append(self._equity)

        if self._position is not None:
            last_close = float(df["close"].iloc[-1])
            last_ts    = df.index[-1]
            self._close(last_close, last_ts, fee_rate)
            self._equity_curve[-1] = self._equity

        return self._build_result(df)

    def _open(self, side: str, close: float, ts: pd.Timestamp, idx: int, fee_rate: float) -> None:
        size = (self._equity * 0.50) / close
        fee  = close * size * fee_rate
        if fee > self._equity * 0.05:
            return
        self._equity -= fee
        self._position = {
            "side":                side,
            "entry_price":         close,
            "size":                size,
            "entry_ts":            ts,
            "entry_idx":           idx,
            "fee_paid":            fee,
            "funding_direction":   1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count":       0,
        }

    def _close(self, close: float, ts: pd.Timestamp, fee_rate: float) -> None:
        if self._position is None:
            return
        size       = self._position["size"]
        fee_entry  = self._position.get("fee_paid", 0.0)
        fee_exit   = close * size * fee_rate
        self._equity -= fee_exit

        net_pnl = (
            self._position.get("funding_accumulated", 0.0)
            - fee_entry
            - fee_exit
        )
        self._trades.append({
            "entry_price": self._position["entry_price"],
            "exit_price":  close,
            "pnl":         net_pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._position["entry_ts"]),
            "close_ts":    str(ts),
            "type":        "fa",
        })
        self._position = None

    def _build_result(self, df: pd.DataFrame) -> dict:
        total_pct  = (self._equity - self.initial_capital) / self.initial_capital * 100.0
        mdd        = calc_max_drawdown(self._equity_curve)
        sharpe     = calc_sharpe(self._equity_curve)
        n_years    = max((df.index[-1] - df.index[0]).days / 365.25, 0.1)
        annual     = calc_annualized_return(total_pct, n_years)
        calmar     = calc_calmar(annual, mdd)
        n_trades   = len(self._trades)
        winning    = [t for t in self._trades if t["pnl"] > 0]
        win_rate   = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf         = calc_profit_factor(self._trades)

        return {
            "total_profit_pct":      round(total_pct, 4),
            "annualized_return_pct": round(annual, 4),
            "sharpe_ratio":          round(sharpe, 4),
            "max_drawdown_pct":      round(mdd, 4),
            "calmar_ratio":          round(calmar, 4),
            "win_rate":              round(win_rate, 2),
            "total_trades":          n_trades,
            "profit_factor":         pf,
            "final_equity":          round(self._equity, 4),
            "equity_curve":          self._equity_curve,
        }


# =========================================================================
# TF 엔진 (단독)
# =========================================================================

class TFEngine:
    """추세추종 단독 실행기."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        initial_capital: float  = 10_000.0,
        tf_capital_ratio: float = 1.0,
        variant: str            = "ema_cross",
        extra_params: dict      | None = None,
    ) -> None:
        self.df              = ohlcv
        self.initial_capital = initial_capital * tf_capital_ratio
        self.variant         = variant
        self.params          = {**COMMON_RISK, **(extra_params or {})}

        self._equity         = self.initial_capital
        self._equity_curve: list[float] = [self.initial_capital]
        self._trades: list[dict]        = []
        self._position: dict[str, Any] | None = None
        self._trail_stop: float | None   = None
        self._consec_losses  = 0
        self._cooldown_until: pd.Timestamp | None = None
        self._trades_today   = 0
        self._last_trade_day: Any        = None
        self._last_entry_ts: pd.Timestamp | None = None

        ema_fast = int(self.params.get("ema_fast_period", 20))
        ema_slow = int(self.params.get("ema_slow_period", 50))
        self._ind = precompute_tf_indicators(ohlcv, variant, ema_fast, ema_slow)

    def run(self) -> dict:
        df     = self.df
        n      = len(df)
        params = self.params

        for idx in range(WARMUP_BARS, n):
            row   = df.iloc[idx]
            ts    = df.index[idx]
            close = float(row["close"])
            atr   = float(self._ind["atr"].iloc[idx])

            # 일별 거래 수 초기화
            trade_day = ts.date()
            if self._last_trade_day is None or trade_day != self._last_trade_day:
                self._trades_today   = 0
                self._last_trade_day = trade_day

            # 포지션 청산 체크
            if self._position is not None:
                direction = self._position["direction"]
                tsmult    = params["trailing_stop_atr_mult"]

                if direction == 1:
                    new_stop = close - atr * tsmult
                    self._trail_stop = max(self._trail_stop or new_stop, new_stop)
                    hit_stop = close <= self._trail_stop
                else:
                    new_stop = close + atr * tsmult
                    self._trail_stop = min(self._trail_stop or new_stop, new_stop)
                    hit_stop = close >= self._trail_stop

                entry   = self._position["entry_price"]
                tp_mult = params["take_profit_atr_mult"]
                hit_tp  = (close >= entry + atr * tp_mult) if direction == 1 else (close <= entry - atr * tp_mult)
                strat_exit = tf_check_exit(idx, self.variant, self._ind, direction)

                if hit_stop or hit_tp or strat_exit:
                    self._close_pos(close, ts, atr)
                    self._trail_stop = None
                    self._equity_curve.append(self._equity)
                    continue

                upnl = (close - entry) * self._position["qty"] * direction
                self._equity_curve.append(self._equity + upnl)
                continue

            # 진입 체크
            if self._cooldown_until is not None and ts < self._cooldown_until:
                self._equity_curve.append(self._equity)
                continue

            if self._trades_today >= params["max_trades_per_day"]:
                self._equity_curve.append(self._equity)
                continue

            if self._last_entry_ts is not None:
                if ts - self._last_entry_ts < timedelta(hours=params["min_trade_interval_hours"]):
                    self._equity_curve.append(self._equity)
                    continue

            signal = tf_check_entry(idx, self.variant, self._ind)
            if signal != 0 and atr > 0:
                stop_mult = _safe_float(params.get("atr_stop_multiplier", params["stop_loss_atr_mult"]), 2.0)
                qty = calculate_position_size(
                    equity=self._equity,
                    entry_price=close,
                    atr=atr,
                    risk_pct=_safe_float(params.get("risk_per_trade_pct", 0.01), 0.01),
                    stop_atr_mult=stop_mult,
                    max_position_pct=params["max_position_pct"],
                    max_leverage=params["max_leverage"],
                )
                if qty > 0:
                    fee = close * qty * EFFECTIVE_FEE
                    if fee <= self._equity * 0.05:
                        self._equity -= fee
                        self._position = {
                            "direction":   signal,
                            "entry_price": close,
                            "qty":         qty,
                            "entry_ts":    ts,
                            "fee_paid":    fee,
                        }
                        self._trail_stop     = None
                        self._trades_today  += 1
                        self._last_entry_ts  = ts

            self._equity_curve.append(self._equity)

        if self._position is not None:
            last_close = float(df["close"].iloc[-1])
            last_atr   = float(self._ind["atr"].iloc[-1])
            self._close_pos(last_close, df.index[-1], last_atr)
            self._equity_curve[-1] = self._equity

        return self._build_result(df)

    def _close_pos(self, price: float, ts: pd.Timestamp, atr: float) -> None:
        if self._position is None:
            return
        direction = self._position["direction"]
        entry     = self._position["entry_price"]
        qty       = self._position["qty"]
        fee_entry = self._position["fee_paid"]
        fee_exit  = price * qty * EFFECTIVE_FEE
        self._equity -= fee_exit
        raw_pnl = (price - entry) * qty * direction
        pnl     = raw_pnl - fee_exit
        self._equity += raw_pnl

        if pnl > 0:
            self._consec_losses = 0
        else:
            self._consec_losses += 1
            if self._consec_losses >= COMMON_RISK["consecutive_loss_cooldown"]:
                self._cooldown_until  = ts + timedelta(hours=24)
                self._consec_losses = 0

        self._trades.append({
            "entry_price": entry,
            "exit_price":  price,
            "direction":   direction,
            "qty":         qty,
            "pnl":         pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._position["entry_ts"]),
            "close_ts":    str(ts),
            "type":        "tf",
        })
        self._position = None

    def _build_result(self, df: pd.DataFrame) -> dict:
        total_pct  = (self._equity - self.initial_capital) / self.initial_capital * 100.0
        mdd        = calc_max_drawdown(self._equity_curve)
        sharpe     = calc_sharpe(self._equity_curve)
        n_years    = max((df.index[-1] - df.index[0]).days / 365.25, 0.1)
        annual     = calc_annualized_return(total_pct, n_years)
        calmar     = calc_calmar(annual, mdd)
        n_trades   = len(self._trades)
        winning    = [t for t in self._trades if t["pnl"] > 0]
        win_rate   = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf         = calc_profit_factor(self._trades)

        return {
            "total_profit_pct":      round(total_pct, 4),
            "annualized_return_pct": round(annual, 4),
            "sharpe_ratio":          round(sharpe, 4),
            "max_drawdown_pct":      round(mdd, 4),
            "calmar_ratio":          round(calmar, 4),
            "win_rate":              round(win_rate, 2),
            "total_trades":          n_trades,
            "profit_factor":         pf,
            "final_equity":          round(self._equity, 4),
            "equity_curve":          self._equity_curve,
        }


# =========================================================================
# 결합 실행기
# =========================================================================

class CombinedRunner:
    """FA + TF 결합 실행기 (고정 가중치)."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float,
        fa_weight: float = 0.30,
        tf_weight: float = 0.20,
        tf_variant: str  = "ema_cross",
        tf_extra_params: dict | None = None,
    ) -> None:
        self.df              = ohlcv
        self.funding         = funding
        self.initial_capital = initial_capital
        self.fa_weight       = fa_weight
        self.tf_weight       = tf_weight
        self.tf_variant      = tf_variant
        self.tf_extra_params = tf_extra_params or {}

        self._equity         = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._fa_trades: list[dict]     = []
        self._tf_trades: list[dict]     = []

        # FA 상태
        self._fa_pos: dict[str, Any] | None = None
        self._fa_pos_consec = 0
        self._fa_neg_consec  = 0

        # TF 상태
        self._tf_pos: dict[str, Any] | None  = None
        self._tf_trail_stop: float | None    = None
        self._tf_consec_losses = 0
        self._tf_cooldown_until: pd.Timestamp | None = None
        self._tf_trades_today  = 0
        self._tf_last_trade_day: Any         = None
        self._tf_last_entry_ts: pd.Timestamp | None  = None

        ema_fast = int(self.tf_extra_params.get("ema_fast_period", 20))
        ema_slow = int(self.tf_extra_params.get("ema_slow_period", 50))
        self._tf_ind = precompute_tf_indicators(ohlcv, tf_variant, ema_fast, ema_slow)

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

    def _fa_step(self, row: pd.Series, ts: pd.Timestamp, idx: int) -> None:
        fa_capital = self._equity * self.fa_weight
        funding    = self._get_funding_rate(ts)
        is_settlement = (ts.hour % 8 == 0) and (ts.minute == 0)
        close         = float(row["close"])
        params        = FA_PARAMS

        if self._fa_pos is not None and is_settlement:
            direction = self._fa_pos.get("funding_direction", 1)
            pos_value = self._fa_pos["size"] * self._fa_pos["entry_price"]
            net_fund  = pos_value * funding * direction
            self._equity += net_fund
            self._fa_pos["funding_accumulated"] = (
                self._fa_pos.get("funding_accumulated", 0.0) + net_fund
            )

        if self._fa_pos is None:
            if is_settlement:
                min_rate = params["min_funding_rate"]
                consec   = params["consecutive_intervals"]
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
                    self._fa_open(close, ts, idx, "sell", fa_capital)
                    self._fa_pos_consec = 0
                elif self._fa_neg_consec >= consec:
                    self._fa_open(close, ts, idx, "buy", fa_capital)
                    self._fa_neg_consec = 0
        else:
            direction    = self._fa_pos.get("funding_direction", 1)
            bars_held    = idx - self._fa_pos.get("entry_idx", idx)
            reversed_now = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)

            should_close = False
            if is_settlement:
                if reversed_now:
                    self._fa_pos["reverse_count"] = self._fa_pos.get("reverse_count", 0) + 1
                else:
                    self._fa_pos["reverse_count"] = 0
                if self._fa_pos.get("reverse_count", 0) >= 3:
                    should_close = True

            if bars_held >= params["max_hold_bars"]:
                should_close = True

            if should_close:
                self._fa_close(close, ts)
                self._fa_pos_consec = 0
                self._fa_neg_consec  = 0

    def _fa_open(self, close: float, ts: pd.Timestamp, idx: int, side: str, fa_capital: float) -> None:
        size = (fa_capital * 0.5) / close
        fee  = close * size * FA_FEE_RATE
        if fee > self._equity * 0.01:
            return
        self._equity -= fee
        self._fa_pos = {
            "side":                side,
            "entry_price":         close,
            "size":                size,
            "entry_ts":            ts,
            "entry_idx":           idx,
            "fee_paid":            fee,
            "funding_direction":   1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count":       0,
        }

    def _fa_close(self, close: float, ts: pd.Timestamp) -> None:
        if self._fa_pos is None:
            return
        size      = self._fa_pos["size"]
        fee_entry = self._fa_pos.get("fee_paid", 0.0)
        fee_exit  = close * size * FA_FEE_RATE
        self._equity -= fee_exit
        net_pnl = self._fa_pos.get("funding_accumulated", 0.0) - fee_entry - fee_exit
        self._fa_trades.append({
            "entry_price": self._fa_pos["entry_price"],
            "exit_price":  close,
            "pnl":         net_pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._fa_pos["entry_ts"]),
            "close_ts":    str(ts),
            "type":        "fa",
        })
        self._fa_pos = None

    def _tf_step(self, row: pd.Series, ts: pd.Timestamp, idx: int) -> None:
        tf_capital = self._equity * self.tf_weight
        close      = float(row["close"])
        atr        = float(self._tf_ind["atr"].iloc[idx])
        params     = {**COMMON_RISK, **self.tf_extra_params}

        trade_day = ts.date()
        if self._tf_last_trade_day is None or trade_day != self._tf_last_trade_day:
            self._tf_trades_today   = 0
            self._tf_last_trade_day = trade_day

        if self._tf_pos is not None:
            direction = self._tf_pos["direction"]
            tsmult    = params["trailing_stop_atr_mult"]

            if direction == 1:
                new_stop = close - atr * tsmult
                self._tf_trail_stop = max(self._tf_trail_stop or new_stop, new_stop)
                hit_stop = close <= self._tf_trail_stop
            else:
                new_stop = close + atr * tsmult
                self._tf_trail_stop = min(self._tf_trail_stop or new_stop, new_stop)
                hit_stop = close >= self._tf_trail_stop

            entry   = self._tf_pos["entry_price"]
            tp_mult = params["take_profit_atr_mult"]
            hit_tp  = (close >= entry + atr * tp_mult) if direction == 1 else (close <= entry - atr * tp_mult)
            strat_exit = tf_check_exit(idx, self.tf_variant, self._tf_ind, direction)

            if hit_stop or hit_tp or strat_exit:
                self._tf_close(close, ts, atr)
                self._tf_trail_stop = None
            return

        if self._tf_cooldown_until is not None and ts < self._tf_cooldown_until:
            return
        if self._tf_trades_today >= params["max_trades_per_day"]:
            return
        if self._tf_last_entry_ts is not None:
            if ts - self._tf_last_entry_ts < timedelta(hours=params["min_trade_interval_hours"]):
                return

        signal = tf_check_entry(idx, self.tf_variant, self._tf_ind)
        if signal != 0 and atr > 0:
            stop_mult = _safe_float(params.get("atr_stop_multiplier", params["stop_loss_atr_mult"]), 2.0)
            qty = calculate_position_size(
                equity=tf_capital,
                entry_price=close,
                atr=atr,
                risk_pct=_safe_float(params.get("risk_per_trade_pct", 0.01), 0.01),
                stop_atr_mult=stop_mult,
                max_position_pct=params["max_position_pct"],
                max_leverage=params["max_leverage"],
            )
            if qty > 0:
                fee = close * qty * EFFECTIVE_FEE
                if fee <= self._equity * 0.05:
                    self._equity -= fee
                    self._tf_pos = {
                        "direction":   signal,
                        "entry_price": close,
                        "qty":         qty,
                        "entry_ts":    ts,
                        "fee_paid":    fee,
                    }
                    self._tf_trail_stop     = None
                    self._tf_trades_today  += 1
                    self._tf_last_entry_ts  = ts

    def _tf_close(self, price: float, ts: pd.Timestamp, atr: float) -> None:
        if self._tf_pos is None:
            return
        direction = self._tf_pos["direction"]
        entry     = self._tf_pos["entry_price"]
        qty       = self._tf_pos["qty"]
        fee_entry = self._tf_pos["fee_paid"]
        fee_exit  = price * qty * EFFECTIVE_FEE
        self._equity -= fee_exit
        raw_pnl = (price - entry) * qty * direction
        pnl     = raw_pnl - fee_exit
        self._equity += raw_pnl

        if pnl > 0:
            self._tf_consec_losses = 0
        else:
            self._tf_consec_losses += 1
            if self._tf_consec_losses >= COMMON_RISK["consecutive_loss_cooldown"]:
                self._tf_cooldown_until  = ts + timedelta(hours=24)
                self._tf_consec_losses = 0

        self._tf_trades.append({
            "entry_price": entry,
            "exit_price":  price,
            "direction":   direction,
            "qty":         qty,
            "pnl":         pnl,
            "fee":         fee_entry + fee_exit,
            "entry_ts":    str(self._tf_pos["entry_ts"]),
            "close_ts":    str(ts),
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
        return pnl

    def run(self) -> dict:
        df = self.df
        n  = len(df)

        for idx in range(WARMUP_BARS, n):
            row   = df.iloc[idx]
            ts    = df.index[idx]
            close = float(row["close"])

            if self.fa_weight > 0:
                self._fa_step(row, ts, idx)
            if self.tf_weight > 0:
                self._tf_step(row, ts, idx)

            self._equity_curve.append(self._equity + self._unrealized_pnl(close))

        if self._fa_pos is not None:
            self._fa_close(float(df["close"].iloc[-1]), df.index[-1])
        if self._tf_pos is not None:
            self._tf_close(float(df["close"].iloc[-1]), df.index[-1], float(self._tf_ind["atr"].iloc[-1]))

        if self._equity_curve:
            self._equity_curve[-1] = self._equity

        return self._build_result(df)

    def _build_result(self, df: pd.DataFrame) -> dict:
        total_pct  = (self._equity - self.initial_capital) / self.initial_capital * 100.0
        mdd        = calc_max_drawdown(self._equity_curve)
        sharpe     = calc_sharpe(self._equity_curve)
        n_years    = max((df.index[-1] - df.index[0]).days / 365.25, 0.1)
        annual     = calc_annualized_return(total_pct, n_years)
        calmar     = calc_calmar(annual, mdd)
        all_trades = self._fa_trades + self._tf_trades
        n_trades   = len(all_trades)
        winning    = [t for t in all_trades if t["pnl"] > 0]
        win_rate   = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf         = calc_profit_factor(all_trades)

        return {
            "total_profit_pct":      round(total_pct, 4),
            "annualized_return_pct": round(annual, 4),
            "sharpe_ratio":          round(sharpe, 4),
            "max_drawdown_pct":      round(mdd, 4),
            "calmar_ratio":          round(calmar, 4),
            "win_rate":              round(win_rate, 2),
            "total_trades":          n_trades,
            "fa_trades":             len(self._fa_trades),
            "tf_trades":             len(self._tf_trades),
            "profit_factor":         pf,
            "final_equity":          round(self._equity, 4),
            "equity_curve":          self._equity_curve,
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
    pool: asyncpg.Pool | None,
    variant_name: str,
    result: dict,
    extra_params: dict,
    data_range: str,
) -> None:
    if pool is None:
        return
    eq = result.get("equity_curve", [])
    step = max(1, len(eq) // 200)
    eq_sample = [round(_safe_float(v), 2) for v in eq[::step]]

    params_json = json.dumps({
        **extra_params,
        "annualized_return_pct": _safe_float(result.get("annualized_return_pct", 0)),
        "calmar_ratio":          _safe_float(result.get("calmar_ratio", 0)),
        "fa_trades":             result.get("fa_trades", 0),
        "tf_trades":             result.get("tf_trades", 0),
        "equity_curve_sample":   eq_sample,
    })
    monthly_json = json.dumps({})

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
                monthly_json,
                params_json,
            )
    except Exception as exc:
        log.warning("db_save_failed", variant=variant_name, error=str(exc))


# =========================================================================
# 단일 윈도우 실행
# =========================================================================

def run_window_fa_only(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    oos_start: pd.Timestamp,
    oos_end: pd.Timestamp,
    initial_capital: float,
) -> dict:
    """FA 단독 OOS 실행."""
    mask   = (ohlcv.index >= oos_start) & (ohlcv.index < oos_end)
    f_mask = (funding.index >= oos_start) & (funding.index < oos_end) if not funding.empty else pd.Series(False)
    oos_df = ohlcv.loc[mask]
    oos_fn = funding.loc[f_mask] if not funding.empty else pd.DataFrame()

    if len(oos_df) < 20:
        return {"sharpe_ratio": 0.0, "annualized_return_pct": 0.0,
                "max_drawdown_pct": 0.0, "total_trades": 0, "calmar_ratio": 0.0}

    engine = FAEngine(oos_df, oos_fn, initial_capital, fa_capital_ratio=1.0)
    return engine.run()


def run_window_combined(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    oos_start: pd.Timestamp,
    oos_end: pd.Timestamp,
    initial_capital: float,
    tf_variant: str = "ema_cross",
    fa_weight: float = 0.30,
    tf_weight: float = 0.20,
    tf_extra_params: dict | None = None,
) -> dict:
    """FA+TF 결합 OOS 실행."""
    mask   = (ohlcv.index >= oos_start) & (ohlcv.index < oos_end)
    f_mask = (funding.index >= oos_start) & (funding.index < oos_end) if not funding.empty else pd.Series(False)
    oos_df = ohlcv.loc[mask]
    oos_fn = funding.loc[f_mask] if not funding.empty else pd.DataFrame()

    if len(oos_df) < WARMUP_BARS + 20:
        return {"sharpe_ratio": 0.0, "annualized_return_pct": 0.0,
                "max_drawdown_pct": 0.0, "total_trades": 0,
                "fa_trades": 0, "tf_trades": 0, "calmar_ratio": 0.0}

    runner = CombinedRunner(
        ohlcv=oos_df,
        funding=oos_fn,
        initial_capital=initial_capital,
        fa_weight=fa_weight,
        tf_weight=tf_weight,
        tf_variant=tf_variant,
        tf_extra_params=tf_extra_params or {},
    )
    return runner.run()


# =========================================================================
# 파라미터 민감도 분석
# =========================================================================

def run_sensitivity_analysis(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    initial_capital: float,
    tf_variant: str = "ema_cross",
) -> list[dict]:
    """각 파라미터 ±20% 변화 시 전체 6년 Sharpe 계산."""
    results: list[dict] = []

    for param_name, values in SENSITIVITY_PARAMS.items():
        sharpes: list[float] = []
        for val in values:
            extra: dict[str, Any] = {}
            base_params = {**COMMON_RISK}

            if param_name == "ema_fast_period":
                extra["ema_fast_period"] = val
            elif param_name == "ema_slow_period":
                extra["ema_slow_period"] = val
            elif param_name == "atr_stop_multiplier":
                extra["atr_stop_multiplier"] = val
            elif param_name == "risk_per_trade_pct":
                extra["risk_per_trade_pct"] = val

            if len(ohlcv) < WARMUP_BARS + 20:
                sharpes.append(0.0)
                continue

            runner = CombinedRunner(
                ohlcv=ohlcv,
                funding=funding,
                initial_capital=initial_capital,
                fa_weight=0.30,
                tf_weight=0.20,
                tf_variant=tf_variant,
                tf_extra_params=extra,
            )
            r = runner.run()
            sharpes.append(_safe_float(r.get("sharpe_ratio", 0.0)))

        std_val = float(np.std(sharpes)) if len(sharpes) > 1 else 0.0
        stable  = std_val < 0.3

        results.append({
            "param":     param_name,
            "values":    values,
            "sharpes":   sharpes,
            "std":       round(std_val, 4),
            "stable":    stable,
        })

    return results


# =========================================================================
# 집계 통계
# =========================================================================

def aggregate_windows(
    window_results: list[dict],
    strategy_key: str,
    criteria: dict,
) -> dict:
    """윈도우 결과 집계 및 통과 판정."""
    sharpes  = [_safe_float(w.get("sharpe_ratio", 0.0)) for w in window_results]
    annuals  = [_safe_float(w.get("annualized_return_pct", 0.0)) for w in window_results]
    mdds     = [_safe_float(w.get("max_drawdown_pct", 0.0)) for w in window_results]
    calmars  = [_safe_float(w.get("calmar_ratio", 0.0)) for w in window_results]
    trades   = [w.get("total_trades", 0) for w in window_results]

    n = len(sharpes)
    if n == 0:
        return {"pass": False, "reason": "no windows"}

    avg_sharpe = float(np.mean(sharpes))
    std_sharpe = float(np.std(sharpes)) if n > 1 else 0.0
    avg_annual = float(np.mean(annuals))
    avg_mdd    = float(np.mean(mdds))
    avg_calmar = float(np.mean(calmars))

    # 일관성: Sharpe > 0 윈도우 비율
    positive = sum(1 for s in sharpes if s > 0)
    consistency = positive / n * 100.0

    # 통과 기준 판정
    pass_sharpe  = avg_sharpe        >= criteria.get("oos_sharpe", 1.0)
    pass_return  = avg_annual        >= criteria.get("oos_annual_return", 2.0)
    pass_mdd     = avg_mdd           <= criteria.get("oos_mdd", 15.0)
    pass_consist = consistency       >= criteria.get("consistency", 60.0)
    min_trades   = all(t >= 10 for t in trades)

    passed = pass_sharpe and pass_return and pass_mdd and pass_consist and min_trades

    reasons: list[str] = []
    if not pass_sharpe:
        reasons.append(f"Sharpe {avg_sharpe:.3f} < {criteria.get('oos_sharpe', 1.0)}")
    if not pass_return:
        reasons.append(f"연수익 {avg_annual:.1f}% < {criteria.get('oos_annual_return', 2.0)}%")
    if not pass_mdd:
        reasons.append(f"MDD {avg_mdd:.2f}% > {criteria.get('oos_mdd', 15.0)}%")
    if not pass_consist:
        reasons.append(f"일관성 {consistency:.1f}% < {criteria.get('consistency', 60.0)}%")
    if not min_trades:
        low_windows = [i + 1 for i, t in enumerate(trades) if t < 10]
        reasons.append(f"거래수 부족 윈도우: {low_windows}")

    return {
        "pass":        passed,
        "avg_sharpe":  round(avg_sharpe, 4),
        "std_sharpe":  round(std_sharpe, 4),
        "avg_annual":  round(avg_annual, 4),
        "avg_mdd":     round(avg_mdd, 4),
        "avg_calmar":  round(avg_calmar, 4),
        "consistency": round(consistency, 2),
        "n_windows":   n,
        "reasons":     reasons,
    }


# =========================================================================
# 메인
# =========================================================================

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

    start_dt        = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt          = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    initial_capital = args.initial_capital
    tf_variant      = args.tf_variant

    # DB 연결
    pool: asyncpg.Pool | None = None
    try:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=4)
        log.info("db_connected", host=os.getenv("DB_HOST", "postgres"))
    except Exception as exc:
        print(f"[WARNING] DB 연결 실패: {exc}")
        print("[INFO]    콘솔 출력만 진행합니다.")

    # 전체 데이터 로드
    log.info("loading_data", symbol=SYMBOL, start=args.start, end=args.end)
    if pool is not None:
        ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
        funding = await load_funding(pool, SYMBOL, start_dt, end_dt)
    else:
        ohlcv   = pd.DataFrame()
        funding = pd.DataFrame()

    if ohlcv.empty:
        print("[ERROR] OHLCV 데이터 없음. seed_historical.py를 먼저 실행하세요.")
        if pool:
            await pool.close()
        sys.exit(1)

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # 기존 결과 삭제
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                await conn.execute(CREATE_VARIANT_RESULTS)
                deleted = await conn.execute(
                    "DELETE FROM strategy_variant_results WHERE test_name = $1",
                    TEST_NAME,
                )
                log.info("cleared_previous", deleted=deleted)
        except Exception as exc:
            log.warning("db_clear_failed", error=str(exc))

    # 윈도우 생성
    windows = generate_wf_windows(
        start_date=WF_CONFIG["start_date"],
        end_date=WF_CONFIG["end_date"],
        train_days=WF_CONFIG["train_days"],
        test_days=WF_CONFIG["test_days"],
        total_windows=WF_CONFIG["total_windows"],
    )

    actual_windows = len(windows)
    print(f"\n{'=' * 80}")
    print(f"=== Stage 4: Walk-Forward 검증 ({actual_windows}개 윈도우) ===")
    print(f"{'=' * 80}")
    print(f"전략: FA 단독 / FA+TF({tf_variant}) / FA+TF(donchian)")
    print(f"기간: {WF_CONFIG['start_date']} ~ {WF_CONFIG['end_date']}")
    print(f"초기 자본: ${initial_capital:,.0f}")
    print()

    # 윈도우별 결과 수집
    fa_window_results:   list[dict] = []
    ema_window_results:  list[dict] = []
    don_window_results:  list[dict] = []
    window_summary:      list[dict] = []

    print("윈도우별 OOS 성과:")
    header = (
        f"{'윈도우':>5} | {'OOS 기간':<20} | {'FA Sharpe':>9} | "
        f"{'FA+EMA Sharpe':>13} | {'FA+DON Sharpe':>13} | "
        f"{'FA거래':>6} | {'EMA거래':>7} | {'DON거래':>7} | {'신뢰'}"
    )
    print(header)
    print("-" * len(header))

    for w in windows:
        wid       = w["window_id"]
        oos_start = w["oos_start"]
        oos_end   = w["oos_end"]

        print(f"윈도우 {wid}/{actual_windows} 실행 중...", end="\r")

        # FA 단독
        fa_r = run_window_fa_only(ohlcv, funding, oos_start, oos_end, initial_capital)

        # FA + TF (ema_cross)
        ema_r = run_window_combined(
            ohlcv, funding, oos_start, oos_end, initial_capital,
            tf_variant="ema_cross", fa_weight=0.30, tf_weight=0.20,
        )

        # FA + TF (donchian)
        don_r = run_window_combined(
            ohlcv, funding, oos_start, oos_end, initial_capital,
            tf_variant="donchian", fa_weight=0.30, tf_weight=0.20,
        )

        fa_sharpe  = _safe_float(fa_r.get("sharpe_ratio", 0))
        ema_sharpe = _safe_float(ema_r.get("sharpe_ratio", 0))
        don_sharpe = _safe_float(don_r.get("sharpe_ratio", 0))

        fa_trades  = fa_r.get("total_trades", 0)
        ema_trades = ema_r.get("total_trades", 0)
        don_trades = don_r.get("total_trades", 0)

        # 신뢰: 모든 전략 거래수 >= 10
        min_trades_ok = fa_trades >= 10 and ema_trades >= 10 and don_trades >= 10
        trust_icon    = "✅" if min_trades_ok else "⚠️"

        oos_label = f"{oos_start.strftime('%Y-%m')}~{oos_end.strftime('%Y-%m')}"
        print(
            f"{wid:>5} | {oos_label:<20} | {fa_sharpe:>9.3f} | "
            f"{ema_sharpe:>13.3f} | {don_sharpe:>13.3f} | "
            f"{fa_trades:>6} | {ema_trades:>7} | {don_trades:>7} | {trust_icon}"
        )

        fa_window_results.append(fa_r)
        ema_window_results.append(ema_r)
        don_window_results.append(don_r)
        window_summary.append({
            "window_id": wid,
            "oos_label": oos_label,
            "fa_sharpe": fa_sharpe,
            "ema_sharpe": ema_sharpe,
            "don_sharpe": don_sharpe,
            "fa_trades": fa_trades,
            "ema_trades": ema_trades,
            "don_trades": don_trades,
            "trust": "OK" if min_trades_ok else "WARN",
        })

        # DB 저장 (윈도우별)
        if pool is not None:
            data_range = f"oos_{oos_label}"
            await save_result(pool, f"fa_only_w{wid}", fa_r,
                              {"window_id": wid, "strategy": "fa_only"}, data_range)
            await save_result(pool, f"fa_ema_w{wid}", ema_r,
                              {"window_id": wid, "strategy": "fa_plus_ema",
                               "fa_weight": 0.30, "tf_weight": 0.20}, data_range)
            await save_result(pool, f"fa_don_w{wid}", don_r,
                              {"window_id": wid, "strategy": "fa_plus_donchian",
                               "fa_weight": 0.30, "tf_weight": 0.20}, data_range)

    # ── 집계 통계 ────────────────────────────────────────────────────────────
    fa_agg  = aggregate_windows(fa_window_results,  "fa_only",    WF_PASS_CRITERIA["fa_only"])
    ema_agg = aggregate_windows(ema_window_results, "fa_plus_tf", WF_PASS_CRITERIA["fa_plus_tf"])
    don_agg = aggregate_windows(don_window_results, "fa_plus_tf", WF_PASS_CRITERIA["fa_plus_tf"])

    print()
    print("=" * 80)
    print("집계 통계:")
    hdr2 = (
        f"{'전략':<16} | {'평균Sharpe':>10} | {'Sharpe std':>10} | "
        f"{'연수익%':>8} | {'MDD%':>7} | {'일관성%':>8} | {'판정'}"
    )
    print(hdr2)
    print("-" * len(hdr2))

    for label, agg in [("FA 단독", fa_agg), (f"FA+TF(ema)", ema_agg), ("FA+TF(don)", don_agg)]:
        icon = "✅" if agg["pass"] else "❌"
        print(
            f"{label:<16} | {agg['avg_sharpe']:>10.4f} | {agg['std_sharpe']:>10.4f} | "
            f"{agg['avg_annual']:>8.2f} | {agg['avg_mdd']:>7.2f} | "
            f"{agg['consistency']:>8.1f} | {icon}"
        )
        if not agg["pass"] and agg["reasons"]:
            for r in agg["reasons"]:
                print(f"  {'':>16}   탈락 사유: {r}")

    # ── 파라미터 민감도 분석 ─────────────────────────────────────────────────
    print()
    print("=" * 80)
    print("파라미터 민감도 분석:")

    sensitivity_results = run_sensitivity_analysis(ohlcv, funding, initial_capital, tf_variant)

    hdr3 = (
        f"{'파라미터':<22} | {'-20% Sharpe':>11} | {'기본 Sharpe':>11} | "
        f"{'+20% Sharpe':>11} | {'Std':>7} | {'안정성'}"
    )
    print(hdr3)
    print("-" * len(hdr3))

    for s in sensitivity_results:
        sharpes = s["sharpes"]
        icon    = "✅ 안정" if s["stable"] else "⚠️ 과적합경고"
        low_s   = f"{sharpes[0]:.4f}" if len(sharpes) > 0 else "N/A"
        mid_s   = f"{sharpes[1]:.4f}" if len(sharpes) > 1 else "N/A"
        high_s  = f"{sharpes[2]:.4f}" if len(sharpes) > 2 else "N/A"
        print(
            f"{s['param']:<22} | {low_s:>11} | {mid_s:>11} | "
            f"{high_s:>11} | {s['std']:>7.4f} | {icon}"
        )

    # ── 최종 판정 ────────────────────────────────────────────────────────────
    print()
    print("=" * 80)
    print("최종 판정:")
    all_pass = fa_agg["pass"] and (ema_agg["pass"] or don_agg["pass"])
    sens_stable = all(s["stable"] for s in sensitivity_results)

    total_pass = (1 if fa_agg["pass"] else 0) + \
                 (1 if ema_agg["pass"] else 0) + \
                 (1 if don_agg["pass"] else 0) + \
                 (1 if sens_stable else 0)
    total_criteria = 4

    print(f"  FA 단독:          {'✅ PASS' if fa_agg['pass'] else '❌ FAIL'}")
    print(f"  FA+TF(ema_cross): {'✅ PASS' if ema_agg['pass'] else '❌ FAIL'}")
    print(f"  FA+TF(donchian):  {'✅ PASS' if don_agg['pass'] else '❌ FAIL'}")
    print(f"  파라미터 안정성:  {'✅ PASS' if sens_stable else '⚠️ WARN'}")
    print()
    verdict = "STAGE 4 통과 ✅" if all_pass else "STAGE 4 미통과 ❌"
    print(f"  종합: {total_pass}/{total_criteria} 항목 통과 → {verdict}")
    print("=" * 80)

    if pool is not None:
        await pool.close()


# =========================================================================
# CLI
# =========================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 4: Walk-Forward 검증 (22개 윈도우)"
    )
    p.add_argument("--start",           default=START_DATE,      help="전체 시작일 (YYYY-MM-DD)")
    p.add_argument("--end",             default=END_DATE,        help="전체 종료일 (YYYY-MM-DD)")
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, help="초기 자본 (USDT)")
    p.add_argument("--tf-variant",      default="ema_cross",     choices=["ema_cross", "donchian"],
                   help="TF 변형 선택")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
