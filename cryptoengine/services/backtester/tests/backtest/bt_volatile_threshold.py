"""Test L — Volatile Regime ATR Multiplier Threshold

Finds the ATR multiplier value where volatile regime covers 5-10% of bars.
Baseline: 2.0x  →  currently 3.1% of bars classified volatile.

For each multiplier variant:
  - Count bars classified as volatile (volatile_pct %)
  - Count FA trades that were entered while regime = volatile
  - Run full FA simulation (METHOD_NATURAL / short_hold) with that threshold
  - Record Sharpe, Return, MDD, volatile_pct, n_volatile_bars, n_fa_trades_in_volatile

Results saved to strategy_variant_results (test_name='test_l_volatile_threshold').
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

# ── sys.path: allow importing from parent backtester directory ───────────────
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
TEST_NAME       = "test_l_volatile_threshold"

# FA weights by regime (Phase 4 final)
FA_WEIGHTS: dict[str, float] = {
    "ranging":       0.50,
    "trending_up":   0.20,
    "trending_down": 0.10,
    "volatile":      0.40,
}

FA_PARAMS = {
    "exit_on_flip":          True,
    "consecutive_intervals": 3,
    "min_funding_rate":      0.0001,
    "max_hold_bars":         168,
}


# =============================================================================
# Regime Detection (parameterised by ATR multiplier)
# =============================================================================

def _compute_adx(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> float:
    """Return a scalar ADX value for the given window."""
    n = len(close)
    if n < period * 2:
        return 0.0

    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr       = np.zeros(n)

    for i in range(1, n):
        up   = float(high[i])   - float(high[i - 1])
        down = float(low[i - 1]) - float(low[i])
        plus_dm[i]  = up   if (up > down and up > 0)   else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        hl  = float(high[i])  - float(low[i])
        hc  = abs(float(high[i])  - float(close[i - 1]))
        lc  = abs(float(low[i])   - float(close[i - 1]))
        tr[i] = max(hl, hc, lc)

    atr_val  = float(np.mean(tr[-period:]))
    if atr_val <= 0:
        return 0.0

    plus_di  = 100.0 * float(np.mean(plus_dm[-period:])) / atr_val
    minus_di = 100.0 * float(np.mean(minus_dm[-period:])) / atr_val
    di_sum   = plus_di + minus_di
    if di_sum <= 0:
        return 0.0
    return float(100.0 * abs(plus_di - minus_di) / di_sum)


def detect_regime(lookback: pd.DataFrame, atr_multiplier: float = 2.0) -> str:
    """
    Classify market regime from the lookback window.

    The only configurable parameter is atr_multiplier:
      if current_atr > avg_atr * atr_multiplier → 'volatile'

    All other thresholds are the same as the baseline detect_regime().
    """
    if len(lookback) < 20:
        return "ranging"

    close = lookback["close"].values.astype(float)
    high  = lookback["high"].values.astype(float)  \
            if "high"  in lookback.columns else close
    low   = lookback["low"].values.astype(float)   \
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

    # Volatile threshold — the only parameter we vary
    if current_atr > avg_atr * atr_multiplier:
        return "volatile"

    # ── ADX(14) ──────────────────────────────────────────────────────────
    adx = _compute_adx(high, low, close, period=period)

    ema20      = float(pd.Series(close).ewm(span=20, adjust=False).mean().iloc[-1])
    last_close = float(close[-1])

    if adx >= 25.0:
        return "trending_up" if last_close > ema20 else "trending_down"

    return "ranging"


# =============================================================================
# Per-variant statistics
# =============================================================================

@dataclass
class VolatileStats:
    n_total_bars:           int   = 0
    n_volatile_bars:        int   = 0
    volatile_pct:           float = 0.0
    n_fa_trades_in_volatile: int  = 0


# =============================================================================
# FA Engine (METHOD_NATURAL, parameterised ATR multiplier)
# =============================================================================

class _VolatileThresholdEngine:
    """
    FA backtester with configurable ATR multiplier for volatile detection.
    Uses METHOD_NATURAL: no rebalancing on regime change.
    Tracks per-bar regime for volatile percentage calculation.
    """

    def __init__(
        self,
        *,
        atr_multiplier: float,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        initial_capital: float = INITIAL_CAPITAL,
        fee_rate: float = FEE_RATE,
    ) -> None:
        self._atr_multiplier = atr_multiplier
        self._ohlcv          = ohlcv
        self._funding        = funding
        self._capital        = initial_capital
        self._fee_rate       = fee_rate

        self._equity: float             = initial_capital
        self._equity_curve: list[float] = [initial_capital]
        self._trades: list[TradeRecord] = []
        self._position: dict[str, Any] | None = None

        self._current_fa_weight: float = FA_WEIGHTS["ranging"]
        self._prev_regime: str         = "ranging"

        self._vstats = VolatileStats()

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

            # ── Regime detection ──────────────────────────────────────────
            lb_start = max(0, idx - lookback_bars)
            lookback = bars.iloc[lb_start : idx + 1]
            regime   = detect_regime(lookback, atr_multiplier=self._atr_multiplier)

            self._vstats.n_total_bars += 1
            if regime == "volatile":
                self._vstats.n_volatile_bars += 1

            # ── Update FA weight on regime change ─────────────────────────
            if regime != self._prev_regime:
                self._current_fa_weight = FA_WEIGHTS[regime]
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

            # ── Funding settlement ────────────────────────────────────────
            if self._position is not None and is_settlement:
                direction = self._position.get("funding_direction", 1)
                pos_value = self._position["size"] * self._position["entry_price"]
                net_fund  = pos_value * funding * direction
                self._equity += net_fund
                self._position["funding_accumulated"] = (
                    self._position.get("funding_accumulated", 0.0) + net_fund
                )

            # ── Entry ─────────────────────────────────────────────────────
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
                        self._open_position(bar, "sell", idx, regime)
                        self._pos_consec_count = 0
                    elif self._neg_consec_count >= consec_intervals:
                        self._open_position(bar, "buy", idx, regime)
                        self._neg_consec_count = 0

            # ── Exit ──────────────────────────────────────────────────────
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

        if self._position is not None:
            self._close_position(bars.iloc[-1])
            self._equity_curve[-1] = self._equity

        # Finalize volatile stats
        total = self._vstats.n_total_bars
        self._vstats.volatile_pct = (
            self._vstats.n_volatile_bars / total * 100.0 if total > 0 else 0.0
        )

        return self._build_result(bars)

    # -------------------------------------------------------------------------
    # Position management
    # -------------------------------------------------------------------------

    def _open_position(self, bar: Any, side: str, idx: int, regime: str) -> None:
        entry = float(bar["close"])
        size  = (self._equity * self._current_fa_weight * 0.95) / entry
        fee   = entry * size * self._fee_rate
        self._equity -= fee

        if regime == "volatile":
            self._vstats.n_fa_trades_in_volatile += 1

        self._position = {
            "side":                side,
            "entry_price":         entry,
            "size":                size,
            "entry_ts":            bar.get("ts", bar.name) if hasattr(bar, "name") else None,
            "entry_idx":           idx,
            "fee_paid":            fee,
            "funding_direction":   1 if side == "sell" else -1,
            "funding_accumulated": 0.0,
            "reverse_count":       0,
        }

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
            strategy=f"fa_volatile_{self._atr_multiplier:.1f}x",
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
                "atr_multiplier":          self._atr_multiplier,
                "n_volatile_bars":         self._vstats.n_volatile_bars,
                "n_total_bars":            self._vstats.n_total_bars,
                "volatile_pct":            round(self._vstats.volatile_pct, 2),
                "n_fa_trades_in_volatile": self._vstats.n_fa_trades_in_volatile,
            },
        )

    @property
    def vstats(self) -> VolatileStats:
        return self._vstats


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
    variant_name: str,
    result: BacktestResult,
    start_str: str,
    end_str: str,
) -> None:
    monthly  = _monthly_returns(result.daily_returns, result.start_date or start_str)
    eq_curve = result.equity_curve
    if len(eq_curve) > 200:
        step     = max(1, len(eq_curve) // 200)
        eq_curve = eq_curve[::step]

    params_payload: dict[str, Any] = {
        **FA_PARAMS,
        "fa_weights": FA_WEIGHTS,
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
            variant_name,
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
        variant=variant_name,
        return_pct=round(_safe_float(result.total_profit_pct), 2),
        sharpe=round(_safe_float(result.sharpe_ratio), 3),
        trades=result.total_trades,
        volatile_pct=result.metadata.get("volatile_pct", 0.0),
        n_volatile_bars=result.metadata.get("n_volatile_bars", 0),
    )


# =============================================================================
# CLI
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test L — Volatile Regime ATR Multiplier Threshold"
    )
    p.add_argument(
        "--multipliers",
        default="1.0,1.3,1.5,2.0",
        help="Comma-separated ATR multipliers to test. Default: 1.0,1.3,1.5,2.0",
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


# =============================================================================
# Main
# =============================================================================

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

    args = _parse_args()

    raw_multipliers = [m.strip() for m in args.multipliers.split(",") if m.strip()]
    multipliers     = [float(m) for m in raw_multipliers]

    start_date = args.start
    end_date   = args.end
    start_dt   = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt     = datetime.strptime(end_date,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    log.info(
        "loading_data",
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        start=start_date,
        end=end_date,
    )
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

    # Clear previous results
    async with pool.acquire() as conn:
        deleted = await conn.execute(
            "DELETE FROM strategy_variant_results WHERE test_name = $1",
            TEST_NAME,
        )
        log.info("cleared_previous", deleted=deleted)

    # ── Run each multiplier ───────────────────────────────────────────────────
    @dataclass
    class RunRecord:
        multiplier: float
        variant_name: str
        result: BacktestResult
        vstats: VolatileStats

    records: list[RunRecord] = []

    # Run in descending multiplier order so baseline (2.0) appears last in table
    # matching the requested output format. Sort descending.
    for mult in sorted(multipliers, reverse=True):
        variant_name = f"atr_{str(mult).replace('.', '_')}x"
        # Mark 2.0x explicitly as baseline
        if mult == 2.0:
            variant_name = "atr_2_0x_baseline"

        log.info("running_variant", variant=variant_name, atr_multiplier=mult)
        engine = _VolatileThresholdEngine(
            atr_multiplier=mult,
            ohlcv=ohlcv,
            funding=funding,
            initial_capital=INITIAL_CAPITAL,
            fee_rate=FEE_RATE,
        )
        result = engine.run()
        vstats = engine.vstats
        await save_result(pool, variant_name, result, start_date, end_date)
        records.append(
            RunRecord(
                multiplier=mult,
                variant_name=variant_name,
                result=result,
                vstats=vstats,
            )
        )

    # ── Comparison table (descending multiplier = baseline first) ─────────────
    print()
    print("=" * 100)
    print(f"Test L — Volatile Threshold ({start_date} ~ {end_date})")
    print("  Target: volatile regime covers 5-10% of bars  |  Baseline: 2.0x = 3.1%")
    print("=" * 100)
    header = (
        f"{'Multiplier':>12}  "
        f"{'Return%':>9} "
        f"{'Sharpe':>8} "
        f"{'MDD%':>8} "
        f"{'Trades':>7} "
        f"{'VolatilePct':>12} "
        f"{'VolBars':>8} "
        f"{'VolTrades':>10}"
    )
    print(header)
    print("-" * 100)

    for rec in records:
        r   = rec.result
        vs  = rec.vstats
        vol_pct_str  = f"{vs.volatile_pct:.1f}%"
        # Annotate baseline
        mult_label = f"{rec.multiplier:.1f}x"
        if rec.multiplier == 2.0:
            mult_label += " (base)"
        print(
            f"{mult_label:>12}  "
            f"{_safe_float(r.total_profit_pct):>9.2f} "
            f"{_safe_float(r.sharpe_ratio):>8.3f} "
            f"{_safe_float(r.max_drawdown_pct):>8.2f} "
            f"{r.total_trades:>7d} "
            f"{vol_pct_str:>12} "
            f"{vs.n_volatile_bars:>8d} "
            f"{vs.n_fa_trades_in_volatile:>10d}"
        )

    print("=" * 100)

    # ── Target-zone summary ───────────────────────────────────────────────────
    print()
    print("[ 5-10% target zone ]")
    in_zone = [rec for rec in records if 5.0 <= rec.vstats.volatile_pct <= 10.0]
    if in_zone:
        for rec in in_zone:
            print(
                f"  atr_multiplier={rec.multiplier:.1f}x  "
                f"volatile_pct={rec.vstats.volatile_pct:.2f}%  "
                f"Sharpe={_safe_float(rec.result.sharpe_ratio):.3f}"
            )
    else:
        closest = min(
            records,
            key=lambda rec: abs(rec.vstats.volatile_pct - 7.5),  # midpoint of 5-10
        )
        print(
            f"  No variant in 5-10% zone. Closest: "
            f"atr_multiplier={closest.multiplier:.1f}x  "
            f"volatile_pct={closest.vstats.volatile_pct:.2f}%"
        )
    print()

    await pool.close()
    log.info("test_l_complete", n_variants=len(records))


if __name__ == "__main__":
    asyncio.run(main())
