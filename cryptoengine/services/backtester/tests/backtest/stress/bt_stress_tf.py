"""bt_tf_stress_test.py — Stage 5: 스트레스 테스트

역할:
  - 6개 극단 시나리오에서 FA+TF 포트폴리오 성능 검증
  - 각 시나리오마다 PASS/FAIL 판정

시나리오:
  1. flash_crash          — 2021-05-19 전후 30일 구간 (BTC -20% 순간 폭락)
  2. prolonged_sideways   — ATR 역사적 최저 6개월 구간 (횡보)
  3. api_downtime         — 4시간 × 10회 무작위 다운타임 시뮬레이션
  4. high_slippage        — 슬리피지 3배 (0.03% → 0.09%)
  5. funding_reversal     — 펀딩비 급반전 구간 (±0.1% 이상 변화)
  6. consecutive_whipsaws — 횡보장 최악 구간: 2022-01~2022-06 (하락+횡보)

저장: strategy_variant_results 테이블 (test_name="test_11_stage5_stress")

실행 방법:
    python bt_tf_stress_test.py
    python bt_tf_stress_test.py --start 2020-04-01 --end 2026-03-31 --initial-capital 10000
    python bt_tf_stress_test.py --tf-variant donchian
    DB_HOST=postgres DB_PASSWORD=CryptoEngine2026! python bt_tf_stress_test.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
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
TEST_NAME       = "test_11_stage5_stress"

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'CryptoEngine2026!')}"
    f"@{os.getenv('DB_HOST', 'postgres')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

# 공통 리스크 파라미터
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

# FA 파라미터
FA_PARAMS: dict[str, Any] = {
    "exit_on_flip":               True,
    "negative_hours_before_exit": 0,
    "consecutive_intervals":      3,
    "min_funding_rate":           0.0001,
    "max_hold_bars":              168,
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


def calc_profit_factor(trades: list[dict]) -> float:
    wins   = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    losses = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    return wins / losses if losses > 0 else float("inf")


def calculate_position_size(
    equity: float,
    entry_price: float,
    atr: float,
    risk_pct: float     = 0.01,
    stop_atr_mult: float = 2.0,
    max_position_pct: float = 0.20,
    max_leverage: float  = 2.0,
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
        closes          = df["close"]
        ind["ema_fast"] = closes.ewm(span=ema_fast, adjust=False).mean()
        ind["ema_slow"] = closes.ewm(span=ema_slow, adjust=False).mean()

    elif variant == "donchian":
        ind["upper96"] = df["high"].rolling(96).max()
        ind["lower96"] = df["low"].rolling(96).min()
        ind["upper48"] = df["high"].rolling(48).max()
        ind["lower48"] = df["low"].rolling(48).min()

    return ind


def tf_check_entry(idx: int, variant: str, ind: dict[str, pd.Series]) -> int:
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
# 결합 엔진 (스트레스 테스트용 — 블랙아웃/슬리피지 옵션 지원)
# =========================================================================

class StressCombinedEngine:
    """FA + TF 결합 엔진 (스트레스 조건 지원).

    blackout_periods : [(start_ts, end_ts), ...] — 해당 시간대 거래 불가
    slippage_override: float | None — 슬리피지 오버라이드
    tf_only          : bool — TF 전략만 실행 (FA 비활성화)
    """

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float = 10_000.0,
        fa_weight: float = 0.30,
        tf_weight: float = 0.20,
        tf_variant: str  = "ema_cross",
        blackout_periods: list[tuple[pd.Timestamp, pd.Timestamp]] | None = None,
        slippage_override: float | None = None,
        tf_only: bool = False,
    ) -> None:
        self.df               = ohlcv
        self.funding          = funding
        self.initial_capital  = initial_capital
        self.fa_weight        = fa_weight if not tf_only else 0.0
        self.tf_weight        = tf_weight
        self.tf_variant       = tf_variant
        self.blackout_periods = blackout_periods or []
        self.tf_only          = tf_only

        # 슬리피지 오버라이드
        if slippage_override is not None:
            self._eff_fee = (
                COMMON_RISK["assumed_maker_ratio"] * COMMON_RISK["maker_fee_pct"] +
                (1 - COMMON_RISK["assumed_maker_ratio"]) * COMMON_RISK["taker_fee_pct"] +
                slippage_override
            )
        else:
            self._eff_fee = EFFECTIVE_FEE

        self._equity          = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._fa_trades: list[dict]     = []
        self._tf_trades: list[dict]     = []
        self._forced_liquidations       = 0

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

        self._tf_ind = precompute_tf_indicators(ohlcv, tf_variant)

    def _is_blackout(self, ts: pd.Timestamp) -> bool:
        for start, end in self.blackout_periods:
            if start <= ts < end:
                return True
        return False

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
        if self.fa_weight <= 0 or self._is_blackout(ts):
            return

        fa_capital    = self._equity * self.fa_weight
        funding       = self._get_funding_rate(ts)
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
        if self.tf_weight <= 0:
            return
        if self._is_blackout(ts):
            # 블랙아웃 중에는 새 진입 불가, 기존 포지션도 청산 불가 (보유 유지)
            return

        tf_capital = self._equity * self.tf_weight
        close      = float(row["close"])
        atr        = float(self._tf_ind["atr"].iloc[idx])
        params     = COMMON_RISK

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
            qty = calculate_position_size(
                equity=tf_capital,
                entry_price=close,
                atr=atr,
                risk_pct=params["risk_per_trade_pct"],
                stop_atr_mult=params["stop_loss_atr_mult"],
                max_position_pct=params["max_position_pct"],
                max_leverage=params["max_leverage"],
            )
            if qty > 0:
                fee = close * qty * self._eff_fee
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
        fee_exit  = price * qty * self._eff_fee
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

        # 웜업 없는 짧은 구간 지원
        start_idx = min(WARMUP_BARS, max(0, n - 10))

        for idx in range(start_idx, n):
            row   = df.iloc[idx]
            ts    = df.index[idx]
            close = float(row["close"])

            # 강제 마진 콜 감지 (equity < 초기자본의 30%)
            margin_level = self.initial_capital * 0.30
            if self._equity < margin_level and (self._fa_pos is not None or self._tf_pos is not None):
                if self._fa_pos is not None:
                    self._fa_close(close, ts)
                if self._tf_pos is not None:
                    atr = float(self._tf_ind["atr"].iloc[idx])
                    self._tf_close(close, ts, atr)
                    self._tf_trail_stop = None
                self._forced_liquidations += 1

            if self.fa_weight > 0:
                self._fa_step(row, ts, idx)
            if self.tf_weight > 0:
                self._tf_step(row, ts, idx)

            self._equity_curve.append(self._equity + self._unrealized_pnl(close))

        if self._fa_pos is not None:
            self._fa_close(float(df["close"].iloc[-1]), df.index[-1])
        if self._tf_pos is not None:
            atr_last = float(self._tf_ind["atr"].iloc[-1])
            self._tf_close(float(df["close"].iloc[-1]), df.index[-1], atr_last)

        if self._equity_curve:
            self._equity_curve[-1] = self._equity

        return self._build_result(df)

    def _build_result(self, df: pd.DataFrame) -> dict:
        total_pct  = (self._equity - self.initial_capital) / self.initial_capital * 100.0
        mdd        = calc_max_drawdown(self._equity_curve)
        sharpe     = calc_sharpe(self._equity_curve)
        n_years    = max((df.index[-1] - df.index[0]).days / 365.25, 0.001)
        annual     = calc_annualized_return(total_pct, n_years)
        all_trades = self._fa_trades + self._tf_trades
        n_trades   = len(all_trades)
        winning    = [t for t in all_trades if t["pnl"] > 0]
        win_rate   = (len(winning) / n_trades * 100) if n_trades > 0 else 0.0
        pf         = calc_profit_factor(all_trades)

        fa_profit  = sum(t["pnl"] for t in self._fa_trades)
        tf_profit  = sum(t["pnl"] for t in self._tf_trades)

        return {
            "total_profit_pct":      round(total_pct, 4),
            "annualized_return_pct": round(annual, 4),
            "sharpe_ratio":          round(sharpe, 4),
            "max_drawdown_pct":      round(mdd, 4),
            "win_rate":              round(win_rate, 2),
            "total_trades":          n_trades,
            "fa_trades":             len(self._fa_trades),
            "tf_trades":             len(self._tf_trades),
            "profit_factor":         pf,
            "final_equity":          round(self._equity, 4),
            "equity_curve":          self._equity_curve,
            "fa_profit":             round(fa_profit, 4),
            "tf_profit":             round(tf_profit, 4),
            "forced_liquidations":   self._forced_liquidations,
        }


# =========================================================================
# 시나리오 1: Flash Crash (2021-05-19 재현)
# =========================================================================

def scenario_flash_crash(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
) -> dict:
    """2021-05-19 전후 30일 구간 실행.

    pass_condition: 강제청산 0건 AND 포트폴리오 MDD < 10%
    """
    start = pd.Timestamp("2021-04-19", tz="UTC")
    end   = pd.Timestamp("2021-06-19", tz="UTC")

    mask  = (ohlcv_df.index >= start) & (ohlcv_df.index < end)
    f_mask = (funding_df.index >= start) & (funding_df.index < end) if not funding_df.empty else pd.Series(False)

    oos_df = ohlcv_df.loc[mask]
    oos_fn = funding_df.loc[f_mask] if not funding_df.empty else pd.DataFrame()

    if len(oos_df) < 20:
        return {
            "scenario": "flash_crash",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0,
            "total_trades": 0, "final_equity": initial_capital,
            "forced_liquidations": 0,
        }

    engine = StressCombinedEngine(
        ohlcv=oos_df,
        funding=oos_fn,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
    )
    result = engine.run()

    mdd     = _safe_float(result.get("max_drawdown_pct", 0))
    forced  = result.get("forced_liquidations", 0)
    passed  = (forced == 0) and (mdd < 10.0)

    reason_parts: list[str] = []
    if forced > 0:
        reason_parts.append(f"강제청산 {forced}건")
    if mdd >= 10.0:
        reason_parts.append(f"MDD {mdd:.2f}% >= 10%")

    return {
        **result,
        "scenario":     "flash_crash",
        "period":       f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}",
        "pass":         passed,
        "reason":       " | ".join(reason_parts) if reason_parts else "OK",
        "pass_criteria": "강제청산 0건 AND MDD < 10%",
    }


# =========================================================================
# 시나리오 2: Prolonged Sideways (6개월 횡보)
# =========================================================================

def scenario_prolonged_sideways(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
) -> dict:
    """ATR 역사적 최저 구간 찾기 (6개월).

    pass_condition: TF 손절 누적 < FA 수익 (합산 양수)
    """
    # ATR 6개월 이동평균 계산
    atr_series = calc_atr(ohlcv_df, 14)
    rolling_6m = atr_series.rolling(window=24 * 30 * 6, min_periods=100).mean()

    if rolling_6m.dropna().empty:
        # 데이터 부족 — 2022-07~2022-12 구간 fallback
        start = pd.Timestamp("2022-07-01", tz="UTC")
        end   = pd.Timestamp("2022-12-31", tz="UTC")
    else:
        # 최저 ATR 지점 탐색
        min_idx = rolling_6m.idxmin()
        end     = min_idx
        start   = end - pd.Timedelta(days=180)
        # 범위 클리핑
        if start < ohlcv_df.index[WARMUP_BARS]:
            start = ohlcv_df.index[WARMUP_BARS]
        if end > ohlcv_df.index[-1]:
            end   = ohlcv_df.index[-1]
            start = end - pd.Timedelta(days=180)

    mask  = (ohlcv_df.index >= start) & (ohlcv_df.index < end)
    f_mask = (funding_df.index >= start) & (funding_df.index < end) if not funding_df.empty else pd.Series(False)

    oos_df = ohlcv_df.loc[mask]
    oos_fn = funding_df.loc[f_mask] if not funding_df.empty else pd.DataFrame()

    if len(oos_df) < WARMUP_BARS + 20:
        return {
            "scenario": "prolonged_sideways",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0,
            "total_trades": 0, "final_equity": initial_capital,
        }

    engine = StressCombinedEngine(
        ohlcv=oos_df,
        funding=oos_fn,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
    )
    result = engine.run()

    fa_profit = _safe_float(result.get("fa_profit", 0))
    tf_profit = _safe_float(result.get("tf_profit", 0))
    combined  = fa_profit + tf_profit
    passed    = combined >= 0  # FA 수익 > TF 손실 (합산 양수)

    return {
        **result,
        "scenario":      "prolonged_sideways",
        "period":        f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}",
        "pass":          passed,
        "reason":        "OK" if passed else f"합산 손익 ${combined:.2f} < 0",
        "pass_criteria": "FA수익 + TF손익 >= 0",
        "fa_profit":     round(fa_profit, 4),
        "tf_profit":     round(tf_profit, 4),
    }


# =========================================================================
# 시나리오 3: API Downtime (4시간 × 10회)
# =========================================================================

def scenario_api_downtime(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
    seed: int = 42,
) -> dict:
    """무작위 4시간 다운타임 10회 시뮬레이션.

    pass_condition: Sharpe가 기준선(1.0) 대비 -20% 이내
    """
    # 기준선 Sharpe (다운타임 없음)
    if len(ohlcv_df) < WARMUP_BARS + 20:
        return {
            "scenario": "api_downtime",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0, "total_trades": 0,
            "final_equity": initial_capital,
        }

    base_engine = StressCombinedEngine(
        ohlcv=ohlcv_df,
        funding=funding_df,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
    )
    base_result = base_engine.run()
    base_sharpe = _safe_float(base_result.get("sharpe_ratio", 1.0))

    # 무작위 다운타임 생성
    rng         = random.Random(seed)
    timestamps  = list(ohlcv_df.index)
    n           = len(timestamps)
    downtime_hrs = 4
    n_downtimes  = 10

    blackout_periods: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    if n > downtime_hrs * n_downtimes:
        idx_candidates = list(range(WARMUP_BARS, n - downtime_hrs))
        chosen_starts  = rng.sample(idx_candidates, min(n_downtimes, len(idx_candidates)))
        for start_idx in chosen_starts:
            bo_start = timestamps[start_idx]
            bo_end   = timestamps[min(start_idx + downtime_hrs, n - 1)]
            blackout_periods.append((bo_start, bo_end))

    stress_engine = StressCombinedEngine(
        ohlcv=ohlcv_df,
        funding=funding_df,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
        blackout_periods=blackout_periods,
    )
    stress_result = stress_engine.run()
    stress_sharpe = _safe_float(stress_result.get("sharpe_ratio", 0))

    # 기준선의 -20% 이내 조건
    threshold = base_sharpe * 0.80
    passed    = stress_sharpe >= threshold

    return {
        **stress_result,
        "scenario":      "api_downtime",
        "period":        "전체 6년",
        "pass":          passed,
        "reason":        f"OK (stress={stress_sharpe:.3f} >= threshold={threshold:.3f})" if passed
                         else f"Sharpe {stress_sharpe:.3f} < threshold {threshold:.3f}",
        "pass_criteria": "Sharpe >= 기준선 × 80%",
        "base_sharpe":   round(base_sharpe, 4),
        "stress_sharpe": round(stress_sharpe, 4),
        "n_blackouts":   len(blackout_periods),
    }


# =========================================================================
# 시나리오 4: High Slippage (슬리피지 3배)
# =========================================================================

def scenario_high_slippage(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
) -> dict:
    """슬리피지 0.03% → 0.09%로 악화.

    pass_condition: Sharpe > 0
    """
    if len(ohlcv_df) < WARMUP_BARS + 20:
        return {
            "scenario": "high_slippage",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0, "total_trades": 0,
            "final_equity": initial_capital,
        }

    # 슬리피지 3배
    high_slippage = COMMON_RISK["slippage_pct"] * 3.0  # 0.0009

    engine = StressCombinedEngine(
        ohlcv=ohlcv_df,
        funding=funding_df,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
        slippage_override=high_slippage,
    )
    result = engine.run()
    sharpe = _safe_float(result.get("sharpe_ratio", 0))
    passed = sharpe > 0

    return {
        **result,
        "scenario":        "high_slippage",
        "period":          "전체 6년",
        "pass":            passed,
        "reason":          "OK" if passed else f"Sharpe {sharpe:.4f} <= 0",
        "pass_criteria":   "Sharpe > 0",
        "slippage_used":   high_slippage,
        "slippage_normal": COMMON_RISK["slippage_pct"],
    }


# =========================================================================
# 시나리오 5: Funding Reversal
# =========================================================================

def scenario_funding_reversal(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
) -> dict:
    """펀딩비 급반전 구간 (전회 대비 ±0.1% 이상 변화).

    pass_condition: 포트폴리오 MDD < 10%
    """
    REVERSAL_THRESHOLD = 0.001  # 0.1%

    if funding_df.empty or len(funding_df) < 10:
        # 펀딩 데이터 없음 → 전체 구간 기본값으로 실행
        start = pd.Timestamp(START_DATE, tz="UTC")
        end   = pd.Timestamp(END_DATE,   tz="UTC")
    else:
        # 급반전 구간 탐색: 전회 대비 변화량 >= 0.1%
        fund_rate = funding_df["rate"]
        diff      = fund_rate.diff().abs()
        reversal_ts = diff[diff >= REVERSAL_THRESHOLD].index

        if len(reversal_ts) == 0:
            # 해당 구간 없음 → 전체 데이터 사용
            start = funding_df.index[0]
            end   = funding_df.index[-1]
        else:
            # 첫 번째 급반전 전후 60일
            first_reversal = reversal_ts[0]
            start = first_reversal - pd.Timedelta(days=30)
            end   = first_reversal + pd.Timedelta(days=30)

            # 클리핑
            if start < ohlcv_df.index[0]:
                start = ohlcv_df.index[0]
            if end > ohlcv_df.index[-1]:
                end   = ohlcv_df.index[-1]

    mask  = (ohlcv_df.index >= start) & (ohlcv_df.index < end)
    f_mask = (funding_df.index >= start) & (funding_df.index < end) if not funding_df.empty else pd.Series(False)

    oos_df = ohlcv_df.loc[mask]
    oos_fn = funding_df.loc[f_mask] if not funding_df.empty else pd.DataFrame()

    if len(oos_df) < 20:
        return {
            "scenario": "funding_reversal",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0, "total_trades": 0,
            "final_equity": initial_capital,
        }

    engine = StressCombinedEngine(
        ohlcv=oos_df,
        funding=oos_fn,
        initial_capital=initial_capital,
        fa_weight=0.30,
        tf_weight=0.20,
        tf_variant=tf_variant,
    )
    result = engine.run()

    mdd    = _safe_float(result.get("max_drawdown_pct", 0))
    passed = mdd < 10.0

    return {
        **result,
        "scenario":      "funding_reversal",
        "period":        f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}",
        "pass":          passed,
        "reason":        "OK" if passed else f"MDD {mdd:.2f}% >= 10%",
        "pass_criteria": "MDD < 10%",
        "reversal_threshold": REVERSAL_THRESHOLD,
    }


# =========================================================================
# 시나리오 6: Consecutive Whipsaws (2022 하락+횡보)
# =========================================================================

def scenario_consecutive_whipsaws(
    ohlcv_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    initial_capital: float = 10_000.0,
    tf_variant: str = "ema_cross",
) -> dict:
    """횡보장 최악 구간: 2022-01-01 ~ 2022-06-30 (하락+횡보).
    TF 전략만 실행.

    pass_condition: TF 손실 <= risk_per_trade × 10 = 자본의 10%
    """
    start = pd.Timestamp("2022-01-01", tz="UTC")
    end   = pd.Timestamp("2022-06-30", tz="UTC")

    mask  = (ohlcv_df.index >= start) & (ohlcv_df.index < end)
    f_mask = (funding_df.index >= start) & (funding_df.index < end) if not funding_df.empty else pd.Series(False)

    oos_df = ohlcv_df.loc[mask]
    oos_fn = funding_df.loc[f_mask] if not funding_df.empty else pd.DataFrame()

    if len(oos_df) < WARMUP_BARS + 20:
        return {
            "scenario": "consecutive_whipsaws",
            "pass": False,
            "reason": "데이터 부족",
            "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0, "total_trades": 0,
            "final_equity": initial_capital,
        }

    # TF 전략만 실행
    engine = StressCombinedEngine(
        ohlcv=oos_df,
        funding=oos_fn,
        initial_capital=initial_capital,
        fa_weight=0.0,
        tf_weight=0.50,  # TF에 50% 배분
        tf_variant=tf_variant,
        tf_only=True,
    )
    result = engine.run()

    tf_profit = _safe_float(result.get("tf_profit", 0))
    # 손실 비율 = |TF 손실| / 초기 자본
    tf_loss_pct = abs(min(tf_profit, 0)) / initial_capital * 100.0
    # 통과 기준: TF 손실 <= 10% of initial_capital
    max_loss_pct = COMMON_RISK["risk_per_trade_pct"] * 10 * 100  # 0.01 * 10 * 100 = 10%
    passed = tf_loss_pct <= max_loss_pct

    return {
        **result,
        "scenario":      "consecutive_whipsaws",
        "period":        f"{start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')}",
        "pass":          passed,
        "reason":        "OK" if passed else f"TF 손실 {tf_loss_pct:.2f}% > {max_loss_pct:.0f}%",
        "pass_criteria": f"TF 손실 <= {max_loss_pct:.0f}% of 자본",
        "tf_loss_pct":   round(tf_loss_pct, 4),
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


async def save_scenario_result(
    pool: asyncpg.Pool | None,
    scenario_name: str,
    result: dict,
) -> None:
    if pool is None:
        return
    eq = result.get("equity_curve", [])
    step = max(1, len(eq) // 200)
    eq_sample = [round(_safe_float(v), 2) for v in eq[::step]]

    extra_keys = [
        "scenario", "period", "pass", "reason", "pass_criteria",
        "fa_profit", "tf_profit", "forced_liquidations",
        "base_sharpe", "stress_sharpe", "n_blackouts",
        "slippage_used", "slippage_normal", "reversal_threshold", "tf_loss_pct",
    ]
    extra = {k: result[k] for k in extra_keys if k in result}
    params_json = json.dumps({**extra, "equity_curve_sample": eq_sample})
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
                scenario_name,
                result.get("period", ""),
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
        log.warning("db_save_failed", scenario=scenario_name, error=str(exc))


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

    # ── 헤더 출력 ────────────────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print(f"=== Stage 5: 스트레스 테스트 ===")
    print(f"{'=' * 90}")
    print(f"FA+TF 결합 (보수적 가중치: FA=30%, TF=20%) / TF 변형: {tf_variant}")
    print(f"기간: {args.start} ~ {args.end}")
    print(f"초기 자본: ${initial_capital:,.0f}")
    print()

    # ── 6개 시나리오 실행 ────────────────────────────────────────────────────
    scenarios_to_run = [
        ("1. Flash Crash",            "flash_crash",          scenario_flash_crash),
        ("2. Prolonged Sideways",     "prolonged_sideways",   scenario_prolonged_sideways),
        ("3. API Downtime",           "api_downtime",         scenario_api_downtime),
        ("4. High Slippage",          "high_slippage",        scenario_high_slippage),
        ("5. Funding Reversal",       "funding_reversal",     scenario_funding_reversal),
        ("6. Consecutive Whipsaws",   "consecutive_whipsaws", scenario_consecutive_whipsaws),
    ]

    all_scenario_results: list[dict] = []
    pass_count = 0

    for label, name, func in scenarios_to_run:
        print(f"  실행 중: {label}...", end="\r")
        try:
            result = func(ohlcv, funding, initial_capital, tf_variant)
        except Exception as exc:
            log.error("scenario_failed", scenario=name, error=str(exc))
            result = {
                "scenario": name, "pass": False, "reason": str(exc),
                "sharpe_ratio": 0.0, "max_drawdown_pct": 0.0,
                "total_trades": 0, "final_equity": initial_capital,
                "period": "ERROR", "pass_criteria": "",
            }

        result["label"] = label
        all_scenario_results.append(result)

        if result.get("pass", False):
            pass_count += 1

        if pool is not None:
            await save_scenario_result(pool, name, result)

    # ── 결과 출력 ────────────────────────────────────────────────────────────
    print()
    hdr = (
        f"{'시나리오':<22} | {'실행기간':<22} | {'최종자산':>10} | "
        f"{'MDD%':>7} | {'Sharpe':>7} | {'기준':<20} | {'판정'}"
    )
    print(hdr)
    print("-" * len(hdr))

    for r in all_scenario_results:
        name      = r.get("scenario", "?")
        period    = r.get("period", "?")
        equity    = _safe_float(r.get("final_equity", INITIAL_CAPITAL))
        mdd       = _safe_float(r.get("max_drawdown_pct", 0))
        sharpe    = _safe_float(r.get("sharpe_ratio", 0))
        criteria  = r.get("pass_criteria", "")
        passed    = r.get("pass", False)
        icon      = "✅" if passed else "❌"

        print(
            f"{name:<22} | {period:<22} | ${equity:>9,.2f} | "
            f"{mdd:>7.2f} | {sharpe:>7.3f} | {criteria:<20} | {icon}"
        )

    print()
    print("=" * 90)
    print()

    # ── 상세 사유 출력 ───────────────────────────────────────────────────────
    print("상세 결과:")
    for r in all_scenario_results:
        name   = r.get("scenario", "?")
        passed = r.get("pass", False)
        reason = r.get("reason", "")
        icon   = "✅" if passed else "❌"
        print(f"  {icon} {name:<22}: {reason}")

        # 추가 세부 정보
        if name == "api_downtime":
            bs = _safe_float(r.get("base_sharpe", 0))
            ss = _safe_float(r.get("stress_sharpe", 0))
            nb = r.get("n_blackouts", 0)
            print(f"     기준 Sharpe: {bs:.4f} | 스트레스 Sharpe: {ss:.4f} | 블랙아웃 {nb}회")
        elif name == "high_slippage":
            su = _safe_float(r.get("slippage_used", 0))
            sn = _safe_float(r.get("slippage_normal", 0))
            print(f"     정상 슬리피지: {sn:.4%} → 악화 슬리피지: {su:.4%}")
        elif name == "prolonged_sideways":
            fa_p = _safe_float(r.get("fa_profit", 0))
            tf_p = _safe_float(r.get("tf_profit", 0))
            print(f"     FA 손익: ${fa_p:.2f} | TF 손익: ${tf_p:.2f} | 합산: ${fa_p + tf_p:.2f}")
        elif name == "consecutive_whipsaws":
            tlp = _safe_float(r.get("tf_loss_pct", 0))
            print(f"     TF 손실: {tlp:.2f}% of 초기 자본")
        elif name == "flash_crash":
            fl = r.get("forced_liquidations", 0)
            print(f"     강제청산: {fl}건")

    print()
    print("=" * 90)

    # ── 종합 판정 ────────────────────────────────────────────────────────────
    total = len(scenarios_to_run)
    verdict = "STAGE 5 통과 ✅" if pass_count == total else (
        f"STAGE 5 부분 통과 ⚠️" if pass_count >= total // 2 else "STAGE 5 미통과 ❌"
    )
    print(f"종합: {pass_count}/{total} 통과 → {verdict}")
    print("=" * 90)

    if pool is not None:
        await pool.close()


# =========================================================================
# CLI
# =========================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 5: FA+TF 스트레스 테스트 (6개 극단 시나리오)"
    )
    p.add_argument("--start",           default=START_DATE,      help="전체 시작일 (YYYY-MM-DD)")
    p.add_argument("--end",             default=END_DATE,        help="전체 종료일 (YYYY-MM-DD)")
    p.add_argument("--initial-capital", default=INITIAL_CAPITAL, type=float, help="초기 자본 (USDT)")
    p.add_argument("--tf-variant",      default="ema_cross",     choices=["ema_cross", "donchian"],
                   help="TF 변형 선택")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
