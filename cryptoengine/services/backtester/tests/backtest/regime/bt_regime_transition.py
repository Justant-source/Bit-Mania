"""Test I — Regime Transition Method Comparison

Compares 3 rebalancing methods for FA positions when market regime changes:
  immediate  — Method A: Close excess position immediately on regime change
  natural    — Method B: Let position run to natural exit; new entries use new weight
  gradual    — Method C: 50% immediate close + 50% natural decay

Regime detection uses last-50-bar lookback with ADX(14), BB width, ATR ratio, EMA20.

Results saved to strategy_variant_results (test_name='test_i_regime_transition').
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog
from shared.timezone_utils import kst_timestamper
# ── sys.path: allow importing from parent backtester directory ───────────────
_THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(os.path.dirname(_THIS_DIR))   # .../backtester/
sys.path.insert(0, _PARENT_DIR)

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
TEST_NAME       = "test_i_regime_transition"

# ── FA weights by regime (Phase 4 final — DCA disabled) ──────────────────────
# Cash weight = 1.0 - FA weight  (DCA disabled per orchestrator.yaml)
FA_WEIGHTS: dict[str, float] = {
    "ranging":       0.50,
    "trending_up":   0.20,
    "trending_down": 0.10,
    "volatile":      0.40,
}

# ── FA engine parameters (short_hold variant) ────────────────────────────────
FA_PARAMS = {
    "exit_on_flip":          True,
    "consecutive_intervals": 3,
    "min_funding_rate":      0.0001,
    "max_hold_bars":         168,
}

# ── Fake-transition threshold (bars) ────────────────────────────────────────
FAKE_TRANSITION_BARS = 24   # regime that reverts within 24 bars is "fake"


# =============================================================================
# Regime Detection
# =============================================================================

def _compute_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                 period: int = 14) -> float:
    """Return a scalar ADX value for the given window."""
    n = len(close)
    if n < period * 2:
        return 0.0

    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr       = np.zeros(n)

    for i in range(1, n):
        up   = float(high[i])  - float(high[i - 1])
        down = float(low[i - 1]) - float(low[i])
        plus_dm[i]  = up   if (up > down and up > 0)   else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        hl  = float(high[i])  - float(low[i])
        hc  = abs(float(high[i])  - float(close[i - 1]))
        lc  = abs(float(low[i])   - float(close[i - 1]))
        tr[i] = max(hl, hc, lc)

    # Smooth over last `period` bars
    atr_val  = float(np.mean(tr[-period:]))
    if atr_val <= 0:
        return 0.0

    plus_di  = 100.0 * float(np.mean(plus_dm[-period:]))  / atr_val
    minus_di = 100.0 * float(np.mean(minus_dm[-period:])) / atr_val
    di_sum   = plus_di + minus_di
    if di_sum <= 0:
        return 0.0
    return float(100.0 * abs(plus_di - minus_di) / di_sum)


def detect_regime(lookback: pd.DataFrame) -> str:
    """Classify market regime from the last 50 bars.

    Returns one of: 'volatile', 'trending_up', 'trending_down', 'ranging'
    """
    if len(lookback) < 20:
        return "ranging"  # not enough data — safe default

    close  = lookback["close"].values.astype(float)
    high   = lookback["high"].values.astype(float)  \
             if "high"  in lookback.columns else close
    low    = lookback["low"].values.astype(float)   \
             if "low"   in lookback.columns else close

    # ── ATR(14) ratio ────────────────────────────────────────────────────
    period = 14
    tr_arr = np.zeros(len(close))
    for i in range(1, len(close)):
        hl = float(high[i]) - float(low[i])
        hc = abs(float(high[i])  - float(close[i - 1]))
        lc = abs(float(low[i])   - float(close[i - 1]))
        tr_arr[i] = max(hl, hc, lc)

    if len(tr_arr) >= period:
        current_atr = float(np.mean(tr_arr[-period:]))
        avg_atr     = float(np.mean(tr_arr[max(0, len(tr_arr) - period * 2): -period + 1]))
        if avg_atr <= 0:
            avg_atr = current_atr if current_atr > 0 else 1e-8
    else:
        current_atr = float(np.mean(tr_arr[tr_arr > 0])) if np.any(tr_arr > 0) else 0.0
        avg_atr     = current_atr if current_atr > 0 else 1e-8

    # Volatile: ATR spikes above 2× average
    if current_atr > avg_atr * 2.0:
        return "volatile"

    # ── ADX(14) ──────────────────────────────────────────────────────────
    adx = _compute_adx(high, low, close, period=period)

    # ── EMA20 for trend direction ─────────────────────────────────────────
    ema20_series = pd.Series(close).ewm(span=20, adjust=False).mean()
    ema20 = float(ema20_series.iloc[-1])
    last_close = float(close[-1])

    if adx >= 25.0:
        return "trending_up"   if last_close > ema20 else "trending_down"

    return "ranging"


# =============================================================================
# Regime-Aware FA Backtest Engine
# =============================================================================

@dataclass
class _RegimeStats:
    """Per-method regime transition statistics."""
    n_regime_changes:     int   = 0
    n_fake_transitions:   int   = 0
    total_rebal_fee:      float = 0.0
    wasted_fee:           float = 0.0   # fees paid on transitions that reversed < 24 bars later


class _RegimeTransitionEngine:
    """Event-loop FA backtester with one of three rebalancing methods."""

    METHOD_IMMEDIATE = "immediate"
    METHOD_NATURAL   = "natural"
    METHOD_GRADUAL   = "gradual"

    def __init__(
        self,
        *,
        method: str,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
    ) -> None:
        self._method   = method
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._capital  = initial_capital
        self._fee_rate = fee_rate

        self._equity: float            = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[TradeRecord] = []
        self._position: dict[str, Any] | None = None

        self._stats = _RegimeStats()

        # Current FA weight used for new entries
        self._current_fa_weight: float = FA_WEIGHTS["ranging"]
        # FA weight used when this position was opened (for rebalancing math)
        self._position_fa_weight: float = FA_WEIGHTS["ranging"]
        # Regime at the previous bar (to detect transitions)
        self._prev_regime: str = "ranging"
        # History of (bar_idx, regime) for fake-transition detection
        self._regime_history: list[tuple[int, str]] = []

        # Entry counters for consecutive-interval logic
        self._pos_consec_count: int = 0
        self._neg_consec_count: int = 0

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    def run(self) -> BacktestResult:
        consec_intervals = FA_PARAMS["consecutive_intervals"]
        min_rate         = FA_PARAMS["min_funding_rate"]
        max_hold         = FA_PARAMS["max_hold_bars"]
        lookback_bars    = 50

        bars = self._ohlcv.reset_index()
        n    = len(bars)

        for idx in range(lookback_bars, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding_rate(bar)

            # ── Regime detection ─────────────────────────────────────────
            lb_start = max(0, idx - lookback_bars)
            lookback = bars.iloc[lb_start : idx + 1]
            regime   = detect_regime(lookback)

            # ── Regime change handling ────────────────────────────────────
            if regime != self._prev_regime:
                self._handle_regime_change(idx, bar, regime)
            self._prev_regime = regime

            # ── 8h settlement timing ──────────────────────────────────────
            ts = bar.get("ts", bar.name)
            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settlement = (idx % 8 == 0)

            # ── Funding settlement: credit/debit equity ───────────────────
            if self._position is not None and is_settlement:
                direction  = self._position.get("funding_direction", 1)
                pos_value  = self._position["size"] * self._position["entry_price"]
                net_fund   = pos_value * funding * direction
                self._equity += net_fund
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_fund
                )

            # ── Entry logic ───────────────────────────────────────────────
            if self._position is None:
                if is_settlement:
                    if funding >= min_rate:
                        self._pos_consec_count += 1
                        self._neg_consec_count  = 0
                    elif funding <= -min_rate:
                        self._neg_consec_count += 1
                        self._pos_consec_count  = 0
                    else:
                        self._pos_consec_count = 0
                        self._neg_consec_count = 0

                    if self._pos_consec_count >= consec_intervals:
                        self._open_position(bar, "sell", idx)
                        self._pos_consec_count = 0
                    elif self._neg_consec_count >= consec_intervals:
                        self._open_position(bar, "buy", idx)
                        self._neg_consec_count = 0

            # ── Exit logic ────────────────────────────────────────────────
            else:
                direction    = self._position.get("funding_direction", 1)
                bars_held    = idx - self._position.get("entry_idx", idx)
                reversed_now = (direction > 0 and funding < 0) or \
                               (direction < 0 and funding > 0)

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
                    self._pos_consec_count = 0
                    self._neg_consec_count = 0

            self._equity_curve.append(self._equity)

        # Force-close remaining position
        if self._position is not None:
            self._close_position(bars.iloc[-1])
            self._equity_curve[-1] = self._equity

        # Post-process fake transitions
        self._evaluate_fake_transitions()

        return self._build_result(bars)

    # -------------------------------------------------------------------------
    # Regime change dispatcher
    # -------------------------------------------------------------------------

    def _handle_regime_change(self, idx: int, bar: Any, new_regime: str) -> None:
        new_weight = FA_WEIGHTS[new_regime]
        old_weight = FA_WEIGHTS[self._prev_regime]

        self._stats.n_regime_changes += 1
        self._regime_history.append((idx, new_regime))
        self._current_fa_weight = new_weight

        # Only need to rebalance if weight decreased AND position is open
        if self._position is None or new_weight >= old_weight:
            return

        excess_fraction = (old_weight - new_weight) / old_weight  # 0.0–1.0

        if self._method == self.METHOD_IMMEDIATE:
            fee_paid = self._partial_close(bar, excess_fraction)
            self._stats.total_rebal_fee += fee_paid
            # tag this bar for fake-transition analysis
            self._regime_history[-1] = (idx, new_regime, fee_paid)  # type: ignore[assignment]

        elif self._method == self.METHOD_NATURAL:
            # Keep existing position; only new entries use the updated weight
            pass  # nothing to do — self._current_fa_weight already updated

        elif self._method == self.METHOD_GRADUAL:
            # Close 50% of the excess immediately; let the rest run
            fee_paid = self._partial_close(bar, excess_fraction * 0.5)
            self._stats.total_rebal_fee += fee_paid
            self._regime_history[-1] = (idx, new_regime, fee_paid)  # type: ignore[assignment]

    # -------------------------------------------------------------------------
    # Partial close (for Methods A and C)
    # -------------------------------------------------------------------------

    def _partial_close(self, bar: Any, fraction: float) -> float:
        """Close `fraction` of the current position. Returns fee charged."""
        if self._position is None or fraction <= 0.0:
            return 0.0

        fraction = min(fraction, 1.0)
        close_size  = self._position["size"] * fraction
        exit_price  = float(bar["close"])
        fee_exit    = exit_price * close_size * self._fee_rate

        # For the closed fraction: its share of the entry fee
        fee_entry_share = self._position.get("fee_paid", 0.0) * fraction

        # Reduce position size
        self._position["size"]     -= close_size
        self._position["fee_paid"] = self._position.get("fee_paid", 0.0) * (1.0 - fraction)

        # Delta-neutral: price PnL cancels; only funding share matters
        fund_share = self._position.get("funding_accumulated", 0.0) * fraction
        self._position["funding_accumulated"] = (
            self._position.get("funding_accumulated", 0.0) * (1.0 - fraction)
        )

        # Charge exit fee
        self._equity -= fee_exit

        net_pnl = fund_share - fee_entry_share - fee_exit

        # Record as a partial trade
        entry_ts = self._position.get("entry_ts")
        close_ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        self._trades.append(
            TradeRecord(
                open_ts=pd.Timestamp(entry_ts) if entry_ts else datetime.min,
                close_ts=pd.Timestamp(close_ts) if close_ts else datetime.min,
                symbol=SYMBOL,
                side=self._position["side"],
                quantity=close_size,
                entry_price=self._position["entry_price"],
                exit_price=exit_price,
                pnl=net_pnl,
                fee=fee_entry_share + fee_exit,
                duration_hours=0.0,
            )
        )

        # If position is now effectively zero, remove it
        if self._position["size"] < 1e-10:
            self._position = None

        return fee_exit + fee_entry_share

    # -------------------------------------------------------------------------
    # Fake-transition analysis (post-loop)
    # -------------------------------------------------------------------------

    def _evaluate_fake_transitions(self) -> None:
        """Count regime changes that reverted within FAKE_TRANSITION_BARS."""
        history = self._regime_history
        for i, entry in enumerate(history):
            idx_i   = entry[0]
            regime_i = entry[1]
            fee_i    = entry[2] if len(entry) > 2 else 0.0  # type: ignore[misc]

            # Was there a reversion within FAKE_TRANSITION_BARS?
            for j in range(i + 1, len(history)):
                idx_j    = history[j][0]
                regime_j = history[j][1]
                if (idx_j - idx_i) <= FAKE_TRANSITION_BARS:
                    if regime_j == self._prev_regime or regime_j != regime_i:
                        # Regime changed again quickly (fake)
                        self._stats.n_fake_transitions += 1
                        self._stats.wasted_fee         += fee_i
                        break
                else:
                    break

    # -------------------------------------------------------------------------
    # Open / close full position
    # -------------------------------------------------------------------------

    def _open_position(self, bar: Any, side: str, idx: int) -> None:
        entry = float(bar["close"])
        size  = (self._equity * self._current_fa_weight * 0.95) / entry
        fee   = entry * size * self._fee_rate
        self._equity -= fee

        self._position = {
            "side":                 side,
            "entry_price":          entry,
            "size":                 size,
            "entry_ts":             bar.get("ts", bar.name) if hasattr(bar, "name") else None,
            "entry_idx":            idx,
            "fee_paid":             fee,
            "funding_direction":    1 if side == "sell" else -1,
            "funding_accumulated":  0.0,
            "reverse_count":        0,
        }
        # Record which weight was used for this position (needed for rebalancing)
        self._position_fa_weight = self._current_fa_weight

    def _close_position(self, bar: Any) -> None:
        if self._position is None:
            return
        size       = self._position["size"]
        entry      = self._position["entry_price"]
        entry_ts   = self._position.get("entry_ts")
        close_ts   = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        fee_entry  = self._position.get("fee_paid", 0.0)
        exit_price = float(bar["close"])
        fee_exit   = exit_price * size * self._fee_rate
        self._equity -= fee_exit

        net_pnl = self._position.get("funding_accumulated", 0.0) - fee_entry - fee_exit

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
                duration_hours=0.0,
            )
        )
        self._position = None

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _get_funding_rate(self, bar: Any) -> float:
        if self._funding is None or self._funding.empty:
            return 0.0001
        ts = bar.get("ts", bar.name) if hasattr(bar, "name") else None
        if ts is None:
            return 0.0001
        try:
            ts_pd = pd.Timestamp(ts)
            mask  = self._funding.index <= ts_pd
            if mask.any():
                return float(self._funding.loc[mask, "rate"].iloc[-1])
        except Exception:
            pass
        return 0.0001

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
            strategy=f"fa_regime_{self._method}",
            start_date=start_date,
            end_date=end_date,
            initial_capital=self._capital,
            final_capital=self._equity,
            total_profit=total_profit,
            total_profit_pct=(total_profit / self._capital * 100)
                             if self._capital > 0 else 0.0,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            win_rate=(len(winning) / len(self._trades) * 100)
                     if self._trades else 0.0,
            total_trades=len(self._trades),
            avg_trade_duration_hours=0.0,
            profit_factor=(gross_profit / gross_loss)
                          if gross_loss > 0 else float("inf"),
            trades=self._trades,
            equity_curve=self._equity_curve,
            drawdown_curve=dd_curve,
            daily_returns=daily_returns,
            metadata={
                "method":                self._method,
                "n_regime_changes":      self._stats.n_regime_changes,
                "n_fake_transitions":    self._stats.n_fake_transitions,
                "total_rebal_fee":       round(self._stats.total_rebal_fee, 4),
                "wasted_fee":            round(self._stats.wasted_fee, 4),
            },
        )

    @property
    def stats(self) -> _RegimeStats:
        return self._stats


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
    return df


def _safe_float(v: float, default: float = 0.0) -> float:
    if v is None or math.isnan(v) or math.isinf(v):
        return default
    return v


def _monthly_returns(daily_returns: list[float], start_str: str) -> dict[str, float]:
    if not daily_returns:
        return {}
    try:
        start_dt = datetime.strptime(start_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return {}
    monthly: dict[str, float] = {}
    for i, ret in enumerate(daily_returns):
        day = pd.Timestamp(start_dt) + pd.Timedelta(hours=i)
        key = day.strftime("%Y-%m")
        monthly[key] = monthly.get(key, 0.0) + _safe_float(ret)
    return monthly


async def save_result(
    pool: asyncpg.Pool,
    method_name: str,
    result: BacktestResult,
    start_str: str,
    end_str: str,
) -> None:
    monthly  = _monthly_returns(result.daily_returns, result.start_date or start_str)
    eq_curve = result.equity_curve
    if len(eq_curve) > 200:
        step    = max(1, len(eq_curve) // 200)
        eq_curve = eq_curve[::step]

    params_payload = {
        **FA_PARAMS,
        "method":           method_name,
        "fa_weights":       FA_WEIGHTS,
        **result.metadata,
        "equity_curve_sample": [round(_safe_float(v), 2) for v in eq_curve],
    }

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
            method_name,
            f"{start_str}~{end_str}",
            _safe_float(result.total_profit_pct),
            _safe_float(result.sharpe_ratio),
            _safe_float(result.max_drawdown_pct),
            result.total_trades,
            _safe_float(result.win_rate),
            _safe_float(result.profit_factor, default=0.0),
            json.dumps(monthly),
            json.dumps(params_payload),
        )
    log.info(
        "result_saved",
        method=method_name,
        return_pct=round(_safe_float(result.total_profit_pct), 2),
        sharpe=round(_safe_float(result.sharpe_ratio), 3),
        trades=result.total_trades,
        rebal_fee=result.metadata.get("total_rebal_fee", 0.0),
    )


# =============================================================================
# Main
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test I — Regime Transition Method Comparison"
    )
    p.add_argument(
        "--methods",
        default="immediate,natural,gradual",
        help="Comma-separated list of methods to run (default: immediate,natural,gradual)",
    )
    p.add_argument(
        "--start",
        default="2020-04-01",
        help="Backtest start date YYYY-MM-DD (default: 2020-04-01)",
    )
    p.add_argument(
        "--end",
        default="2026-03-31",
        help="Backtest end date YYYY-MM-DD (default: 2026-03-31)",
    )
    return p.parse_args()


async def main() -> None:
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

    args = _parse_args()

    methods    = [m.strip() for m in args.methods.split(",") if m.strip()]
    start_date = args.start
    end_date   = args.end

    valid_methods = {
        _RegimeTransitionEngine.METHOD_IMMEDIATE,
        _RegimeTransitionEngine.METHOD_NATURAL,
        _RegimeTransitionEngine.METHOD_GRADUAL,
    }
    for m in methods:
        if m not in valid_methods:
            log.error("unknown_method", method=m, valid=sorted(valid_methods))
            sys.exit(1)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    log.info("loading_data", symbol=SYMBOL, timeframe=TIMEFRAME,
             start=start_date, end=end_date)
    ohlcv   = await load_ohlcv(pool, SYMBOL, TIMEFRAME, start_dt, end_dt)
    funding = await load_funding(pool, SYMBOL, start_dt, end_dt)

    if ohlcv.empty:
        log.error(
            "no_ohlcv_data",
            hint="먼저 fetch_real_ohlcv.py 또는 seed_historical.py를 실행하세요.",
        )
        await pool.close()
        return

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # Clear previous results for this test
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = $1",
            TEST_NAME,
        )
        log.info("cleared_previous", deleted=deleted)

    # ── Run each method ───────────────────────────────────────────────────────
    results: dict[str, tuple[BacktestResult, _RegimeStats]] = {}

    for method in methods:
        log.info("running_method", method=method)
        engine = _RegimeTransitionEngine(
            method=method,
            ohlcv=ohlcv,
            funding=funding,
            initial_capital=INITIAL_CAPITAL,
            fee_rate=FEE_RATE,
        )
        result = engine.run()
        results[method] = (result, engine.stats)
        await save_result(pool, method, result, start_date, end_date)

    # ── Comparison table ──────────────────────────────────────────────────────
    print()
    print("=" * 100)
    print(f"Method A vs B vs C comparison ({start_date} ~ {end_date})")
    print("=" * 100)
    header = (
        f"{'Method':<12} "
        f"{'Return%':>9} "
        f"{'Sharpe':>8} "
        f"{'MDD%':>8} "
        f"{'Trades':>7} "
        f"{'RebalFee$':>11} "
        f"{'RegimeChg':>10} "
        f"{'FakeChg':>8}"
    )
    print(header)
    print("-" * 100)

    method_label = {
        "immediate": "immediate",
        "natural":   "natural",
        "gradual":   "gradual",
    }

    for method in methods:
        result, stats = results[method]
        pf_str = (
            f"{_safe_float(result.profit_factor):.3f}"
            if not math.isinf(result.profit_factor)
            else "    inf"
        )
        print(
            f"{method_label.get(method, method):<12} "
            f"{_safe_float(result.total_profit_pct):>9.2f} "
            f"{_safe_float(result.sharpe_ratio):>8.3f} "
            f"{_safe_float(result.max_drawdown_pct):>8.2f} "
            f"{result.total_trades:>7d} "
            f"{stats.total_rebal_fee:>11.2f} "
            f"{stats.n_regime_changes:>10d} "
            f"{stats.n_fake_transitions:>8d}"
        )

    print("=" * 100)

    # ── Wasted-fee breakdown (Method A / C only) ──────────────────────────────
    has_waste = any(
        results[m][1].wasted_fee > 0
        for m in methods
        if m in results
    )
    if has_waste:
        print()
        print("[ Wasted fees from fake transitions ]")
        print(f"{'Method':<12} {'WastedFee$':>12} {'FakeChg':>10}")
        print("-" * 36)
        for method in methods:
            _, stats = results[method]
            if stats.wasted_fee > 0:
                print(
                    f"{method_label.get(method, method):<12} "
                    f"{stats.wasted_fee:>12.4f} "
                    f"{stats.n_fake_transitions:>10d}"
                )
        print("=" * 36)

    # ── FA weights reference ──────────────────────────────────────────────────
    print()
    print("[ FA Weight by Regime (Phase 4 final) ]")
    print(f"{'Regime':<16} {'FA Weight':>10} {'Cash':>8}")
    print("-" * 36)
    for regime, fa_w in FA_WEIGHTS.items():
        print(f"{regime:<16} {fa_w:>10.2f} {1.0 - fa_w:>8.2f}")
    print("=" * 36)
    print()

    await pool.close()
    log.info("test_i_complete", methods=methods)


if __name__ == "__main__":
    asyncio.run(main())
