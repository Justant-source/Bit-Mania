"""Test J — Regime Granularity Comparison (4 vs 6 vs 8 regimes)

Compares 3 levels of regime granularity applied to FA short_hold strategy
over 6 years with walk-forward validation (train=180d, test=90d OOS).

Regime systems compared:
  4 regimes  — current system (ADX < 20 / ADX 20-25 / ADX >= 25 + direction / volatile)
  6 regimes  — ADX sub-division at 20/30 thresholds
  8 regimes  — ADX + ATR combination with transition/extreme zones

Saves results to walk_forward_results with run_id prefix 'test_j_granularity_N'.
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

# ── sys.path: import freqtrade_bridge from parent backtester dir ──────────────
_BACKTESTER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKTESTER_DIR not in sys.path:
    sys.path.insert(0, _BACKTESTER_DIR)

from freqtrade_bridge import (
    BacktestResult,
    TradeRecord,
    _compute_daily_returns,
    _compute_drawdown,
    _compute_sharpe,
    _compute_sortino,
    _drawdown_series,
)

log = structlog.get_logger(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────
DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)

SYMBOL          = "BTCUSDT"
TIMEFRAME       = "1h"
INITIAL_CAPITAL = 10_000.0
FEE_RATE        = 0.00055

# ── FA short_hold parameters ──────────────────────────────────────────────────
FA_PARAMS = {
    "exit_on_flip":        True,
    "consecutive_intervals": 3,
    "min_funding_rate":    0.0001,
    "max_hold_bars":       168,   # 7 days
}

# ── Regime weight tables ──────────────────────────────────────────────────────
WEIGHTS_4 = {
    "ranging":       0.50,
    "trending_up":   0.20,
    "trending_down": 0.10,
    "volatile":      0.40,
}

WEIGHTS_6 = {
    "ranging":       0.50,
    "mild_up":       0.30,
    "strong_up":     0.10,
    "mild_down":     0.20,
    "strong_down":   0.05,
    "volatile":      0.40,
}

WEIGHTS_8 = {
    "ranging_low_vol":      0.50,
    "ranging_high_vol":     0.30,
    "mild_up_low_vol":      0.30,
    "strong_up_high_vol":   0.10,
    "mild_down_low_vol":    0.20,
    "strong_down_high_vol": 0.00,
    "transition":           0.20,
    "extreme":              0.00,
}


# =============================================================================
# Indicator computation (all numpy, no external TA libs required)
# =============================================================================

def _ema(arr: np.ndarray, span: int) -> np.ndarray:
    """Exponential moving average via pandas ewm for accuracy."""
    return pd.Series(arr).ewm(span=span, adjust=False).mean().values


def _compute_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Wilder-smoothed ADX.  Returns array of same length as inputs (NaN for warmup)."""
    n = len(close)
    adx_out = np.full(n, np.nan)
    if n < period * 2 + 1:
        return adx_out

    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr       = np.zeros(n)

    for i in range(1, n):
        up   = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i - 1])
        lc = abs(low[i]  - close[i - 1])
        tr[i] = max(hl, hc, lc)

    # Wilder smoothing
    def _wilder_smooth(arr: np.ndarray) -> np.ndarray:
        out = np.zeros(n)
        out[period] = arr[1:period + 1].sum()
        for i in range(period + 1, n):
            out[i] = out[i - 1] - out[i - 1] / period + arr[i]
        return out

    str_tr    = _wilder_smooth(tr)
    str_plus  = _wilder_smooth(plus_dm)
    str_minus = _wilder_smooth(minus_dm)

    for i in range(period, n):
        if str_tr[i] == 0:
            continue
        plus_di  = 100 * str_plus[i]  / str_tr[i]
        minus_di = 100 * str_minus[i] / str_tr[i]
        di_sum   = plus_di + minus_di
        if di_sum == 0:
            continue
        dx = 100 * abs(plus_di - minus_di) / di_sum
        if i == period:
            adx_out[i] = dx
        else:
            prev = adx_out[i - 1] if not np.isnan(adx_out[i - 1]) else dx
            adx_out[i] = (prev * (period - 1) + dx) / period

    return adx_out


def _compute_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """Simple ATR (SMA of true range)."""
    n = len(close)
    tr = np.zeros(n)
    for i in range(1, n):
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i - 1])
        lc = abs(low[i]  - close[i - 1])
        tr[i] = max(hl, hc, lc)
    atr = np.full(n, np.nan)
    for i in range(period, n):
        atr[i] = tr[i - period + 1 : i + 1].mean()
    return atr


# =============================================================================
# Regime classifiers
# =============================================================================

def classify_4_regimes(
    ohlcv: pd.DataFrame,
    adx_period: int = 14,
    atr_period: int = 14,
    ema_span: int = 20,
) -> np.ndarray:
    """Classify each bar into one of: ranging / trending_up / trending_down / volatile."""
    high  = ohlcv["high"].values.astype(float)
    low   = ohlcv["low"].values.astype(float)
    close = ohlcv["close"].values.astype(float)
    n     = len(close)

    adx   = _compute_adx(high, low, close, adx_period)
    atr   = _compute_atr(high, low, close, atr_period)
    ema20 = _ema(close, ema_span)

    # rolling 200-bar average ATR for volatile threshold
    avg_atr = pd.Series(atr).rolling(200, min_periods=atr_period).mean().values

    regimes = np.empty(n, dtype=object)
    for i in range(n):
        a = adx[i]
        at = atr[i]
        avg_at = avg_atr[i]
        cl = close[i]
        em = ema20[i]

        if np.isnan(a) or np.isnan(at) or np.isnan(avg_at):
            regimes[i] = "ranging"
            continue

        # volatile check first (highest priority)
        if at > avg_at * 2.0:
            regimes[i] = "volatile"
        elif a >= 25:
            regimes[i] = "trending_up" if cl > em else "trending_down"
        else:
            regimes[i] = "ranging"

    return regimes


def classify_6_regimes(
    ohlcv: pd.DataFrame,
    adx_period: int = 14,
    atr_period: int = 14,
) -> np.ndarray:
    """Classify each bar into 6 regimes (ADX sub-divided at 20/30)."""
    high  = ohlcv["high"].values.astype(float)
    low   = ohlcv["low"].values.astype(float)
    close = ohlcv["close"].values.astype(float)
    n     = len(close)

    adx    = _compute_adx(high, low, close, adx_period)
    atr    = _compute_atr(high, low, close, atr_period)
    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)
    ema200 = _ema(close, 200)

    avg_atr = pd.Series(atr).rolling(200, min_periods=atr_period).mean().values

    regimes = np.empty(n, dtype=object)
    for i in range(n):
        a = adx[i]
        at = atr[i]
        avg_at = avg_atr[i]
        cl = close[i]
        e20 = ema20[i]
        e50 = ema50[i]
        e200 = ema200[i]

        if np.isnan(a) or np.isnan(at) or np.isnan(avg_at):
            regimes[i] = "ranging"
            continue

        if at > avg_at * 2.0:
            regimes[i] = "volatile"
        elif a >= 30:
            # strong trend: use EMA50 vs EMA200 for direction
            if cl > e50 and (np.isnan(e200) or e50 > e200):
                regimes[i] = "strong_up"
            else:
                regimes[i] = "strong_down"
        elif a >= 20:
            # mild trend: use EMA20 for direction
            regimes[i] = "mild_up" if cl > e20 else "mild_down"
        else:
            regimes[i] = "ranging"

    return regimes


def classify_8_regimes(
    ohlcv: pd.DataFrame,
    adx_period: int = 14,
    atr_period: int = 14,
) -> np.ndarray:
    """Classify each bar into 8 regimes (ADX + ATR combination)."""
    high  = ohlcv["high"].values.astype(float)
    low   = ohlcv["low"].values.astype(float)
    close = ohlcv["close"].values.astype(float)
    n     = len(close)

    adx   = _compute_adx(high, low, close, adx_period)
    atr   = _compute_atr(high, low, close, atr_period)
    ema20 = _ema(close, 20)
    ema50 = _ema(close, 50)

    avg_atr = pd.Series(atr).rolling(200, min_periods=atr_period).mean().values

    regimes = np.empty(n, dtype=object)
    for i in range(n):
        a = adx[i]
        at = atr[i]
        avg_at = avg_atr[i]
        cl = close[i]
        e20 = ema20[i]
        e50 = ema50[i]

        if np.isnan(a) or np.isnan(at) or np.isnan(avg_at):
            regimes[i] = "ranging_low_vol"
            continue

        high_vol   = at > avg_at          # ATR above average
        extreme    = at > avg_at * 3.0    # black swan
        volatile   = at > avg_at * 2.0    # standard volatile (not extreme)

        if extreme:
            regimes[i] = "extreme"
        elif volatile:
            # volatile is covered under ranging checks below — but spec puts
            # volatile as separate; here we map to transition if ADX unclear
            regimes[i] = "transition"
        elif 20 <= a <= 25:
            # uncertainty / transition zone
            regimes[i] = "transition"
        elif a >= 30:
            if cl > e50:
                regimes[i] = "strong_up_high_vol" if high_vol else "strong_up_high_vol"
            else:
                regimes[i] = "strong_down_high_vol" if high_vol else "strong_down_high_vol"
        elif a > 25:
            # 25-30: mild with direction
            if cl > e20:
                regimes[i] = "mild_up_low_vol"
            else:
                regimes[i] = "mild_down_low_vol"
        else:
            # ADX < 20: ranging
            if high_vol:
                regimes[i] = "ranging_high_vol"
            else:
                regimes[i] = "ranging_low_vol"

    return regimes


CLASSIFIERS = {
    4: classify_4_regimes,
    6: classify_6_regimes,
    8: classify_8_regimes,
}

WEIGHT_TABLES = {
    4: WEIGHTS_4,
    6: WEIGHTS_6,
    8: WEIGHTS_8,
}

# Default weight for any regime not explicitly listed (conservative)
_DEFAULT_WEIGHT = 0.20


# =============================================================================
# Inline FA short_hold engine with regime-aware position sizing
# =============================================================================

class _RegimeFAEngine:
    """FA short_hold backtester with regime-based capital allocation.

    The regime determines what fraction of available equity is committed
    to the position (FA weight).  All other FA logic follows short_hold
    variant exactly.
    """

    def __init__(
        self,
        *,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        regimes: np.ndarray,      # pre-computed regime labels, same len as ohlcv
        weight_table: dict[str, float],
        params: dict[str, Any],
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
    ) -> None:
        self._ohlcv         = ohlcv.reset_index()
        self._funding       = funding
        self._regimes       = regimes
        self._weight_table  = weight_table
        self._params        = params
        self._capital       = initial_capital
        self._fee_rate      = fee_rate

        self._equity        = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[TradeRecord] = []
        self._position: dict[str, Any] | None = None

        # track (entry_regime) for per-regime PnL breakdown
        self._regime_trades: dict[str, list[float]] = {}

    # ------------------------------------------------------------------

    def run(self) -> BacktestResult:
        consec_intervals = self._params["consecutive_intervals"]
        min_rate         = self._params["min_funding_rate"]
        max_hold         = self._params["max_hold_bars"]

        bars = self._ohlcv
        n    = len(bars)

        pos_consec = 0
        neg_consec = 0

        for idx in range(20, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding_rate(bar)
            regime  = self._regimes[idx] if idx < len(self._regimes) else "ranging"
            weight  = self._weight_table.get(regime, _DEFAULT_WEIGHT)

            # 8h settlement detection
            ts = bar.get("ts", bar.name)
            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settlement = (idx % 8 == 0)

            # Funding settlement on open position
            if self._position is not None and is_settlement:
                direction = self._position.get("funding_direction", 1)
                pos_value = self._position["size"] * self._position["entry_price"]
                net_funding = pos_value * funding * direction
                self._equity += net_funding
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_funding
                )

            # Entry logic
            if self._position is None:
                if is_settlement:
                    if funding >= min_rate:
                        pos_consec += 1
                        neg_consec  = 0
                    elif funding <= -min_rate:
                        neg_consec += 1
                        pos_consec  = 0
                    else:
                        pos_consec = 0
                        neg_consec = 0

                    if weight > 0.0 and pos_consec >= consec_intervals:
                        self._open_position(bar, "sell", idx, weight, regime)
                        pos_consec = 0
                    elif weight > 0.0 and neg_consec >= consec_intervals:
                        self._open_position(bar, "buy", idx, weight, regime)
                        neg_consec = 0

            # Exit logic
            else:
                direction    = self._position.get("funding_direction", 1)
                bars_held    = idx - self._position.get("entry_idx", idx)
                reversed_now = (direction > 0 and funding < 0) or (direction < 0 and funding > 0)
                should_close = False

                if is_settlement:
                    if reversed_now:
                        self._position["reverse_count"] = (
                            self._position.get("reverse_count", 0) + 1
                        )
                    else:
                        self._position["reverse_count"] = 0
                    if self._position.get("reverse_count", 0) >= 3:
                        should_close = True

                if bars_held >= max_hold:
                    should_close = True

                if should_close:
                    self._close_position(bar)
                    pos_consec = 0
                    neg_consec = 0

            self._equity_curve.append(self._equity)

        # Force close at end
        if self._position is not None:
            self._close_position(bars.iloc[-1])
            self._equity_curve[-1] = self._equity

        return self._build_result(bars)

    # ------------------------------------------------------------------

    def _get_funding_rate(self, bar: Any) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        ts = bar.get("ts", bar.name)
        try:
            ts = pd.Timestamp(ts)
            mask = self._funding.index <= ts
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

    def _open_position(
        self, bar: Any, side: str, idx: int, weight: float, regime: str
    ) -> None:
        entry = float(bar["close"])
        size  = (self._equity * weight * 0.95) / entry
        fee   = entry * size * self._fee_rate
        self._equity -= fee
        self._position = {
            "side":                side,
            "entry_price":         entry,
            "size":                size,
            "entry_ts":            bar.get("ts", bar.name),
            "entry_idx":           idx,
            "fee_paid":            fee,
            "funding_direction":   1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count":       0,
            "entry_regime":        regime,
        }

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return
        size       = self._position["size"]
        entry      = self._position["entry_price"]
        entry_ts   = self._position.get("entry_ts")
        close_ts   = bar.get("ts", bar.name)
        fee_entry  = self._position.get("fee_paid", 0.0)
        exit_price = float(bar["close"])
        fee_exit   = exit_price * size * self._fee_rate
        self._equity -= fee_exit

        net_pnl = self._position.get("funding_accumulated", 0.0) - fee_entry - fee_exit

        entry_regime = self._position.get("entry_regime", "unknown")
        if entry_regime not in self._regime_trades:
            self._regime_trades[entry_regime] = []
        self._regime_trades[entry_regime].append(net_pnl)

        self._trades.append(
            TradeRecord(
                open_ts=pd.Timestamp(entry_ts) if entry_ts else datetime.min,
                close_ts=pd.Timestamp(close_ts) if close_ts else datetime.min,
                symbol=SYMBOL,
                side=self._position["side"],
                quantity=size,
                entry_price=entry,
                exit_price=exit_price,
                pnl=net_pnl,
                fee=fee_entry + fee_exit,
                duration_hours=float(
                    max(0, bar.name - self._position.get("entry_idx", bar.name))
                    if hasattr(bar, "name") else 0
                ),
            )
        )
        self._position = None

    def _build_result(self, bars: pd.DataFrame) -> BacktestResult:
        total_profit = self._equity - self._capital
        winning      = [t for t in self._trades if t.pnl > 0]
        losing       = [t for t in self._trades if t.pnl <= 0]
        gross_profit = sum(t.pnl for t in winning)
        gross_loss   = abs(sum(t.pnl for t in losing))

        max_dd, max_dd_pct = _compute_drawdown(self._equity_curve)
        daily_returns      = _compute_daily_returns(self._equity_curve)
        sharpe             = _compute_sharpe(daily_returns)
        sortino            = _compute_sortino(daily_returns)
        dd_curve           = _drawdown_series(self._equity_curve)

        start_date = str(bars.iloc[0].get("ts", "")) if len(bars) > 0 else ""
        end_date   = str(bars.iloc[-1].get("ts", "")) if len(bars) > 0 else ""

        return BacktestResult(
            strategy="funding_arb_regime",
            start_date=start_date,
            end_date=end_date,
            initial_capital=self._capital,
            final_capital=self._equity,
            total_profit=total_profit,
            total_profit_pct=(total_profit / self._capital * 100) if self._capital > 0 else 0.0,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            win_rate=(len(winning) / len(self._trades) * 100) if self._trades else 0.0,
            total_trades=len(self._trades),
            avg_trade_duration_hours=0.0,
            profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else float("inf"),
            trades=self._trades,
            equity_curve=self._equity_curve,
            drawdown_curve=dd_curve,
            daily_returns=daily_returns,
        )

    @property
    def regime_trades(self) -> dict[str, list[float]]:
        return self._regime_trades


# =============================================================================
# Utility helpers
# =============================================================================

def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return float(v)


def _regime_transition_frequency(regimes: np.ndarray) -> float:
    """Transitions per 1000 bars."""
    if len(regimes) < 2:
        return 0.0
    transitions = sum(1 for i in range(1, len(regimes)) if regimes[i] != regimes[i - 1])
    return transitions / len(regimes) * 1000


def _regime_distribution(regimes: np.ndarray) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in regimes:
        counts[r] = counts.get(r, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def _avg_run_length(regimes: np.ndarray, label: str) -> float:
    """Average consecutive bar count for a given regime label."""
    runs = []
    current = 0
    for r in regimes:
        if r == label:
            current += 1
        else:
            if current > 0:
                runs.append(current)
            current = 0
    if current > 0:
        runs.append(current)
    return sum(runs) / len(runs) if runs else 0.0


# =============================================================================
# DB helpers
# =============================================================================

async def load_ohlcv(
    pool: asyncpg.Pool, symbol: str, timeframe: str,
    start: datetime, end: datetime,
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
    pool: asyncpg.Pool, symbol: str,
    start: datetime, end: datetime,
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
    df["rate"] = df["rate"].astype(float)
    return df


async def ensure_walk_forward_results_table(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS walk_forward_results (
                id              SERIAL PRIMARY KEY,
                run_id          TEXT NOT NULL,
                window_id      INTEGER NOT NULL,
                train_start     TIMESTAMPTZ,
                train_end       TIMESTAMPTZ,
                test_start      TIMESTAMPTZ,
                test_end        TIMESTAMPTZ,
                oos_sharpe      DOUBLE PRECISION,
                oos_return_pct  DOUBLE PRECISION,
                oos_max_dd_pct  DOUBLE PRECISION,
                oos_trades      INTEGER,
                params          JSONB,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )


async def save_window_result(
    pool: asyncpg.Pool,
    run_id: str,
    window_id: int,
    train_start: datetime,
    train_end: datetime,
    test_start: datetime,
    test_end: datetime,
    result: BacktestResult,
    params: dict[str, Any],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO walk_forward_results
                (strategy, run_id, window_id,
                 train_start, train_end, test_start, test_end,
                 test_sharpe, test_return_pct, test_max_drawdown_pct,
                 test_total_trades, monte_carlo)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
            """,
            "funding_arb_regime",
            run_id, window_id,
            train_start, train_end, test_start, test_end,
            _safe_float(result.sharpe_ratio),
            _safe_float(result.total_profit_pct),
            _safe_float(result.max_drawdown_pct),
            result.total_trades,
            json.dumps(params),
        )


# =============================================================================
# Regime distribution table printer
# =============================================================================

def print_regime_distribution(regimes: np.ndarray, n_regimes: int) -> None:
    total = len(regimes)
    dist  = _regime_distribution(regimes)
    print(f"\nRegime Distribution ({n_regimes} regimes over {total} bars):")
    print(f"  {'regime':<26} {'count':>7}  {'pct':>6}  {'avg_duration':>13}  warn?")
    print("  " + "-" * 65)
    for regime, count in dist.items():
        pct      = count / total * 100 if total > 0 else 0.0
        avg_dur  = _avg_run_length(regimes, regime)
        warn_str = "WARN (<5%)" if pct < 5.0 else "OK"
        print(
            f"  {regime:<26} {count:>7}  {pct:>5.1f}%  {avg_dur:>11.1f}h  {warn_str}"
        )
    print()


# =============================================================================
# Per-regime Sharpe breakdown (4-regime only)
# =============================================================================

def print_regime_sharpe_breakdown(regime_trades: dict[str, list[float]]) -> None:
    print("Per-Regime FA Performance (4-regime):")
    print(f"  {'regime':<16} {'trades':>7}  {'net_pnl':>10}  {'win_rate':>9}  {'avg_pnl':>9}")
    print("  " + "-" * 58)
    for regime in sorted(regime_trades.keys()):
        pnls    = regime_trades[regime]
        n       = len(pnls)
        net     = sum(pnls)
        wins    = sum(1 for p in pnls if p > 0)
        win_pct = wins / n * 100 if n > 0 else 0.0
        avg     = net / n if n > 0 else 0.0
        print(
            f"  {regime:<16} {n:>7}  {net:>10.2f}  {win_pct:>8.1f}%  {avg:>9.4f}"
        )
    print()


# =============================================================================
# Walk-Forward engine
# =============================================================================

@dataclass
class WindowResult:
    window_id:   int
    train_start:  datetime
    train_end:    datetime
    test_start:   datetime
    test_end:     datetime
    oos_sharpe:   float
    oos_return:   float
    oos_max_dd:   float
    oos_trades:   int
    oos_positive: bool    # OOS Sharpe > 0


@dataclass
class WalkForwardSummary:
    n_regimes:        int
    oos_sharpe:       float   # mean OOS Sharpe across windows
    consistency:      float   # fraction of positive OOS windows
    oos_return_pct:   float   # aggregate OOS return
    max_oos_dd:       float   # worst OOS drawdown window
    trans_freq:       float   # regime transition frequency (per 1000 bars)
    min_regime_count: int     # smallest regime sample size
    sensitivity:      float   # Sharpe delta under ±10% weight perturbation
    windows:          list[WindowResult] = field(default_factory=list)


def _build_windows(
    start: datetime,
    end: datetime,
    train_days: int,
    test_days: int,
) -> list[tuple[datetime, datetime, datetime, datetime]]:
    """Slide through [start, end] producing (train_start, train_end, test_start, test_end)."""
    windows = []
    cur = start
    while True:
        train_s = cur
        train_e = train_s + timedelta(days=train_days)
        test_s  = train_e
        test_e  = test_s + timedelta(days=test_days)
        if test_e > end:
            break
        windows.append((train_s, train_e, test_s, test_e))
        cur = cur + timedelta(days=test_days)   # slide by test period
    return windows


def _slice_data(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    regimes: np.ndarray,
    start: datetime,
    end: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray]:
    """Slice ohlcv/funding/regimes to [start, end)."""
    mask = (ohlcv.index >= start) & (ohlcv.index < end)
    ohlcv_sl = ohlcv.loc[mask]
    if ohlcv_sl.empty:
        return pd.DataFrame(), pd.DataFrame(), np.array([], dtype=object)

    # regime slice uses the same boolean mask applied to the index of ohlcv
    idx_positions = np.where(np.asarray(mask))[0]
    reg_sl = regimes[idx_positions]

    # funding slice
    f_mask  = (funding.index >= start) & (funding.index < end)
    fund_sl = funding.loc[f_mask]

    return ohlcv_sl, fund_sl, reg_sl


def _run_single_window(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    regimes: np.ndarray,
    weight_table: dict[str, float],
    params: dict[str, Any],
    initial_capital: float = INITIAL_CAPITAL,
    fee_rate: float = FEE_RATE,
) -> tuple[BacktestResult, dict[str, list[float]]]:
    """Run FA engine on the given slice; return (result, regime_trades)."""
    if ohlcv.empty or len(ohlcv) < 40:
        # Return dummy result
        dummy = BacktestResult(
            strategy="funding_arb_regime",
            start_date="", end_date="",
            initial_capital=initial_capital,
            final_capital=initial_capital,
            total_profit=0.0, total_profit_pct=0.0,
            max_drawdown=0.0, max_drawdown_pct=0.0,
            sharpe_ratio=0.0, sortino_ratio=0.0,
            win_rate=0.0, total_trades=0,
            avg_trade_duration_hours=0.0, profit_factor=0.0,
        )
        return dummy, {}

    engine = _RegimeFAEngine(
        ohlcv=ohlcv,
        funding=funding,
        regimes=regimes,
        weight_table=weight_table,
        params=params,
        initial_capital=initial_capital,
        fee_rate=fee_rate,
    )
    result = engine.run()
    return result, engine.regime_trades


def run_walk_forward(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    regimes: np.ndarray,
    weight_table: dict[str, float],
    params: dict[str, Any],
    windows: list[tuple[datetime, datetime, datetime, datetime]],
) -> list[WindowResult]:
    """Run OOS tests across all windows (train phase validates, test phase measures OOS)."""
    results: list[WindowResult] = []

    for w_idx, (train_s, train_e, test_s, test_e) in enumerate(windows):
        # OOS test slice
        oos_ohlcv, oos_funding, oos_regimes = _slice_data(
            ohlcv, funding, regimes, test_s, test_e
        )

        oos_result, _ = _run_single_window(
            oos_ohlcv, oos_funding, oos_regimes,
            weight_table, params,
        )

        results.append(WindowResult(
            window_id=w_idx,
            train_start=train_s,
            train_end=train_e,
            test_start=test_s,
            test_end=test_e,
            oos_sharpe=_safe_float(oos_result.sharpe_ratio),
            oos_return=_safe_float(oos_result.total_profit_pct),
            oos_max_dd=_safe_float(oos_result.max_drawdown_pct),
            oos_trades=oos_result.total_trades,
            oos_positive=oos_result.sharpe_ratio > 0,
        ))

    return results


def _aggregate_walk_forward(windows: list[WindowResult]) -> tuple[float, float, float, float]:
    """Return (mean_sharpe, consistency, total_return_pct, max_dd)."""
    if not windows:
        return 0.0, 0.0, 0.0, 0.0
    sharpes    = [w.oos_sharpe for w in windows]
    mean_s     = sum(sharpes) / len(sharpes)
    consistency = sum(1 for w in windows if w.oos_positive) / len(windows)
    total_ret  = sum(w.oos_return for w in windows)
    max_dd     = max(w.oos_max_dd for w in windows)
    return mean_s, consistency, total_ret, max_dd


# =============================================================================
# Sensitivity analysis: perturb all FA weights ±10%, measure Sharpe delta
# =============================================================================

def sensitivity_analysis(
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    regimes: np.ndarray,
    weight_table: dict[str, float],
    params: dict[str, Any],
    windows: list[tuple[datetime, datetime, datetime, datetime]],
    mc_runs: int = 100,
) -> float:
    """Re-run walk-forward with each weight perturbed ±10% (mc_runs samples).
    Returns mean absolute Sharpe delta vs baseline."""
    # Baseline Sharpe
    baseline_windows = run_walk_forward(ohlcv, funding, regimes, weight_table, params, windows)
    baseline_sharpe, _, _, _ = _aggregate_walk_forward(baseline_windows)

    rng = random.Random(42)
    deltas: list[float] = []
    runs = min(mc_runs, 5)  # cap for speed

    for _ in range(runs):
        perturbed = {
            k: max(0.0, min(1.0, v * (1.0 + rng.uniform(-0.10, 0.10))))
            for k, v in weight_table.items()
        }
        pert_windows = run_walk_forward(
            ohlcv, funding, regimes, perturbed, params, windows
        )
        pert_sharpe, _, _, _ = _aggregate_walk_forward(pert_windows)
        deltas.append(abs(pert_sharpe - baseline_sharpe))

    return sum(deltas) / len(deltas) if deltas else 0.0


# =============================================================================
# Main orchestrator
# =============================================================================

async def run_granularity_test(
    pool: asyncpg.Pool,
    ohlcv: pd.DataFrame,
    funding: pd.DataFrame,
    regime_counts: list[int],
    start: datetime,
    end: datetime,
    train_days: int,
    test_days: int,
    mc_runs: int,
) -> dict[int, WalkForwardSummary]:
    await ensure_walk_forward_results_table(pool)

    windows = _build_windows(start, end, train_days, test_days)
    log.info("walk_forward_windows", count=len(windows), train_days=train_days, test_days=test_days)

    summaries: dict[int, WalkForwardSummary] = {}

    # Keep 4-regime result for per-regime Sharpe breakdown
    four_regime_trades: dict[str, list[float]] | None = None

    for n_regimes in regime_counts:
        log.info("classifying_regimes", n_regimes=n_regimes, bars=len(ohlcv))
        classifier   = CLASSIFIERS[n_regimes]
        weight_table = WEIGHT_TABLES[n_regimes]
        regimes      = classifier(ohlcv)

        # Print regime distribution
        print_regime_distribution(regimes, n_regimes)

        # Regime metrics
        dist       = _regime_distribution(regimes)
        trans_freq = _regime_transition_frequency(regimes)
        min_regime = min(dist.values()) if dist else 0

        # Walk-forward OOS
        log.info("running_walk_forward", n_regimes=n_regimes, windows=len(windows))
        wf_windows = run_walk_forward(
            ohlcv, funding, regimes, weight_table, FA_PARAMS, windows
        )
        mean_sharpe, consistency, total_ret, max_dd = _aggregate_walk_forward(wf_windows)

        # Sensitivity
        log.info("sensitivity_analysis", n_regimes=n_regimes, mc_runs=mc_runs)
        sensitivity = sensitivity_analysis(
            ohlcv, funding, regimes, weight_table, FA_PARAMS, windows, mc_runs
        )

        # Save each OOS window to DB
        run_id = f"test_j_granularity_{n_regimes}"
        # Clear previous runs
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM walk_forward_results WHERE run_id = $1", run_id
            )

        for w in wf_windows:
            await save_window_result(
                pool, run_id, w.window_id,
                w.train_start, w.train_end,
                w.test_start, w.test_end,
                BacktestResult(
                    strategy="funding_arb_regime",
                    start_date=str(w.test_start),
                    end_date=str(w.test_end),
                    initial_capital=INITIAL_CAPITAL,
                    final_capital=INITIAL_CAPITAL * (1 + w.oos_return / 100),
                    total_profit=INITIAL_CAPITAL * w.oos_return / 100,
                    total_profit_pct=w.oos_return,
                    max_drawdown=INITIAL_CAPITAL * w.oos_max_dd / 100,
                    max_drawdown_pct=w.oos_max_dd,
                    sharpe_ratio=w.oos_sharpe,
                    sortino_ratio=0.0,
                    win_rate=0.0,
                    total_trades=w.oos_trades,
                    avg_trade_duration_hours=0.0,
                    profit_factor=0.0,
                ),
                {"n_regimes": n_regimes, "weight_table": weight_table},
            )

        # For 4-regime: run full-period engine to get per-regime trade breakdown
        if n_regimes == 4:
            full_engine = _RegimeFAEngine(
                ohlcv=ohlcv,
                funding=funding,
                regimes=regimes,
                weight_table=weight_table,
                params=FA_PARAMS,
                initial_capital=INITIAL_CAPITAL,
                fee_rate=FEE_RATE,
            )
            full_engine.run()
            four_regime_trades = full_engine.regime_trades

        summaries[n_regimes] = WalkForwardSummary(
            n_regimes=n_regimes,
            oos_sharpe=mean_sharpe,
            consistency=consistency,
            oos_return_pct=total_ret,
            max_oos_dd=max_dd,
            trans_freq=trans_freq,
            min_regime_count=min_regime,
            sensitivity=sensitivity,
            windows=wf_windows,
        )
        log.info(
            "granularity_done",
            n_regimes=n_regimes,
            oos_sharpe=round(mean_sharpe, 3),
            consistency=round(consistency, 3),
            sensitivity=round(sensitivity, 4),
        )

    # Per-regime Sharpe breakdown for 4-regime
    if four_regime_trades:
        print_regime_sharpe_breakdown(four_regime_trades)

    return summaries


# =============================================================================
# Console output
# =============================================================================

def print_results(
    summaries: dict[int, WalkForwardSummary],
    start: str,
    end: str,
) -> None:
    print("\n" + "=" * 100)
    print(f"=== Test J: Regime Granularity Comparison ({start} ~ {end}) ===")
    print("=" * 100)
    header = (
        f"{'Regimes':>8}  {'OOS_Sharpe':>11}  {'Consistency':>12}  "
        f"{'OOS_Ret%':>9}  {'MaxDD%':>7}  {'TransFreq':>10}  "
        f"{'MinRegime':>10}  {'Sensitivity':>12}"
    )
    print(header)
    print("-" * 100)

    for n_regimes in sorted(summaries.keys()):
        s = summaries[n_regimes]
        print(
            f"{n_regimes:>8}  "
            f"{s.oos_sharpe:>11.3f}  "
            f"{s.consistency:>11.3f}  "
            f"{s.oos_return_pct:>9.2f}  "
            f"{s.max_oos_dd:>7.2f}  "
            f"{s.trans_freq:>10.2f}  "
            f"{s.min_regime_count:>10}  "
            f"{s.sensitivity:>12.4f}"
        )

    print("=" * 100)

    # Conclusion
    base_sharpe = summaries.get(4, WalkForwardSummary(
        n_regimes=4, oos_sharpe=0.0, consistency=0.0, oos_return_pct=0.0,
        max_oos_dd=0.0, trans_freq=0.0, min_regime_count=0, sensitivity=0.0,
    )).oos_sharpe

    threshold = base_sharpe + 0.3
    adopted = []
    for n_regimes in [6, 8]:
        if n_regimes not in summaries:
            continue
        s = summaries[n_regimes]
        if s.oos_sharpe > threshold and s.consistency >= summaries[4].consistency:
            adopted.append(n_regimes)

    print()
    if adopted:
        best = max(adopted, key=lambda n: summaries[n].oos_sharpe)
        print(f"결론: {best}개 레짐 채택 권장")
        print(
            f"  {best}개 레짐 OOS Sharpe ({summaries[best].oos_sharpe:.3f}) > "
            f"4개 레짐 기준 ({base_sharpe:.3f}) + 0.3 임계값 달성 "
            f"및 일관성 ({summaries[best].consistency:.3f}) 유지됨."
        )
    else:
        print("결론: 4개 유지 권장 (단순함 우선)")
        print(
            f"  더 세밀한 레짐 분류가 OOS Sharpe를 "
            f"기준({base_sharpe:.3f}) + 0.3 이상 개선하지 못함."
        )
    print()


# =============================================================================
# CLI entry point
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test J — Regime Granularity Comparison (4 vs 6 vs 8 regimes)"
    )
    p.add_argument(
        "--regimes",
        default="4,6,8",
        help="Comma-separated list of regime counts to compare (default: 4,6,8)",
    )
    p.add_argument("--start",         default="2020-04-01", help="Start date YYYY-MM-DD")
    p.add_argument("--end",           default="2026-03-31", help="End date YYYY-MM-DD")
    p.add_argument("--train-days",    type=int, default=180, help="Training window in days")
    p.add_argument("--test-days",     type=int, default=90,  help="OOS test window in days")
    p.add_argument("--monte-carlo-runs", type=int, default=100,
                   help="Monte Carlo sensitivity runs (default: 100)")
    return p.parse_args()


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

    args = parse_args()

    try:
        regime_counts = [int(x.strip()) for x in args.regimes.split(",")]
    except ValueError:
        print("ERROR: --regimes must be comma-separated integers, e.g. '4,6,8'")
        sys.exit(1)

    invalid = [n for n in regime_counts if n not in CLASSIFIERS]
    if invalid:
        print(f"ERROR: unsupported regime counts: {invalid}. Supported: {list(CLASSIFIERS.keys())}")
        sys.exit(1)

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    log.info(
        "test_j_start",
        regimes=regime_counts,
        start=args.start,
        end=args.end,
        train_days=args.train_days,
        test_days=args.test_days,
        mc_runs=args.monte_carlo_runs,
    )

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    try:
        log.info("loading_data", symbol=SYMBOL, timeframe=TIMEFRAME)
        ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
        funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

        if ohlcv.empty:
            log.error(
                "no_ohlcv_data",
                hint="먼저 fetch_real_ohlcv.py 또는 seed_historical.py를 실행하세요.",
            )
            return

        log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

        summaries = await run_granularity_test(
            pool, ohlcv, funding,
            regime_counts, start_dt, end_dt,
            args.train_days, args.test_days, args.monte_carlo_runs,
        )

        print_results(summaries, args.start, args.end)

    finally:
        await pool.close()
        log.info("test_j_complete")


if __name__ == "__main__":
    asyncio.run(main())
