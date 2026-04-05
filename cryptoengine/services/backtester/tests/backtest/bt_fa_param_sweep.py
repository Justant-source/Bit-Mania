"""Test M — FA Parameter Grid Search with Walk-Forward Validation

Grid-searches 80 parameter combinations (min_rate × consecutive × max_hold_bars)
using a simplified walk-forward approach (train 180d / test 90d, slide 90d).

The OOS (test) Sharpe across all windows is averaged per combination.
Top results are reported with a plateau-stability analysis on the top-10 combos.

Usage:
    python tests/backtest/bt_fa_param_sweep.py \\
        --start 2020-04-01 --end 2026-03-31 \\
        --walk-forward --train-days 180 --test-days 90
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import product
from typing import Any

import asyncpg
import numpy as np
import pandas as pd
import structlog

# ── sys.path: allow importing from parent backtester directory ────────────────
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
TEST_NAME       = "test_m_fa_param_sweep"

# ── FA weights by regime (Phase 4 final) ─────────────────────────────────────
FA_WEIGHTS: dict[str, float] = {
    "ranging":       0.50,
    "trending_up":   0.20,
    "trending_down": 0.10,
    "volatile":      0.40,
}

# ── Parameter grid ────────────────────────────────────────────────────────────
MIN_RATES      = [0.00005, 0.0001, 0.00015, 0.0002, 0.0003]
CONSECUTIVES   = [2, 3, 4, 5]
MAX_HOLD_BARS  = [72, 120, 168, 240]
# Total: 5 × 4 × 4 = 80 combinations


# =============================================================================
# Regime Detection
# =============================================================================

def _compute_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                 period: int = 14) -> float:
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
    atr_val = float(np.mean(tr[-period:]))
    if atr_val <= 0:
        return 0.0
    plus_di  = 100.0 * float(np.mean(plus_dm[-period:]))  / atr_val
    minus_di = 100.0 * float(np.mean(minus_dm[-period:])) / atr_val
    di_sum   = plus_di + minus_di
    if di_sum <= 0:
        return 0.0
    return float(100.0 * abs(plus_di - minus_di) / di_sum)


def detect_regime(lookback: pd.DataFrame) -> str:
    if len(lookback) < 20:
        return "ranging"
    close = lookback["close"].values.astype(float)
    high  = lookback["high"].values.astype(float)  if "high"  in lookback.columns else close
    low   = lookback["low"].values.astype(float)   if "low"   in lookback.columns else close

    period = 14
    tr_arr = np.zeros(len(close))
    for i in range(1, len(close)):
        hl = float(high[i])  - float(low[i])
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

    if current_atr > avg_atr * 2.0:
        return "volatile"

    adx = _compute_adx(high, low, close, period=period)

    ema20_series = pd.Series(close).ewm(span=20, adjust=False).mean()
    ema20 = float(ema20_series.iloc[-1])
    last_close = float(close[-1])

    if adx >= 25.0:
        return "trending_up" if last_close > ema20 else "trending_down"
    return "ranging"


# =============================================================================
# FA Backtest Engine (parameterized, no regime-dependent position sizing here
# — we keep FA_WEIGHTS for consistency with the wider codebase, but use the
# weight for the detected regime at each entry bar)
# =============================================================================

@dataclass
class _SweepResult:
    min_rate:      float
    consecutive:   int
    max_hold:      int
    oos_sharpes:   list[float]   # one value per WF window
    avg_oos_sharpe: float
    total_windows: int


class _FAEngine:
    """Minimal FA engine for parameter sweep — runs one OOS slice at a time."""

    def __init__(
        self,
        *,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        min_rate: float,
        consecutive: int,
        max_hold: int,
        fee_rate: float = FEE_RATE,
        initial_capital: float = INITIAL_CAPITAL,
        lookback_bars: int = 50,
    ) -> None:
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._min_rate = min_rate
        self._consec   = consecutive
        self._max_hold = max_hold
        self._fee_rate = fee_rate
        self._capital  = initial_capital
        self._lookback = lookback_bars

    def run(self) -> BacktestResult:
        min_rate      = self._min_rate
        consec_thresh = self._consec
        max_hold      = self._max_hold

        equity       = self._capital
        equity_curve: list[float] = [equity]
        trades: list[TradeRecord] = []

        bars = self._ohlcv.reset_index()
        n    = len(bars)

        # Warm up: need at least lookback_bars for regime detection
        start_idx = self._lookback

        position: dict[str, Any] | None = None
        pos_consec = 0
        neg_consec = 0

        for idx in range(start_idx, n):
            bar     = bars.iloc[idx]
            funding = self._get_funding(bar)

            # ── 8h settlement ─────────────────────────────────────────────
            ts = bar.get("ts", bar.name)
            try:
                ts_dt = pd.Timestamp(ts)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.tz_localize("UTC")
                is_settlement = (ts_dt.hour % 8 == 0) and (ts_dt.minute == 0)
            except Exception:
                is_settlement = (idx % 8 == 0)

            # ── Funding settlement ────────────────────────────────────────
            if position is not None and is_settlement:
                direction = position["funding_direction"]
                pos_value = position["size"] * position["entry_price"]
                net_fund  = pos_value * funding * direction
                equity   += net_fund
                position["funding_accumulated"] = (
                    position.get("funding_accumulated", 0.0) + net_fund
                )

            # ── Regime-aware FA weight ────────────────────────────────────
            lb_start = max(0, idx - self._lookback)
            lookback = bars.iloc[lb_start: idx + 1]
            regime   = detect_regime(lookback)
            fa_weight = FA_WEIGHTS[regime]

            # ── Entry ─────────────────────────────────────────────────────
            if position is None:
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

                    if pos_consec >= consec_thresh:
                        side  = "sell"
                        entry = float(bar["close"])
                        size  = (equity * fa_weight * 0.95) / entry
                        fee   = entry * size * self._fee_rate
                        equity -= fee
                        position = {
                            "side":               side,
                            "entry_price":        entry,
                            "size":               size,
                            "entry_ts":           ts,
                            "entry_idx":          idx,
                            "fee_paid":           fee,
                            "funding_direction":  1,
                            "funding_accumulated": 0.0,
                            "reverse_count":      0,
                        }
                        pos_consec = 0

                    elif neg_consec >= consec_thresh:
                        side  = "buy"
                        entry = float(bar["close"])
                        size  = (equity * fa_weight * 0.95) / entry
                        fee   = entry * size * self._fee_rate
                        equity -= fee
                        position = {
                            "side":               side,
                            "entry_price":        entry,
                            "size":               size,
                            "entry_ts":           ts,
                            "entry_idx":          idx,
                            "fee_paid":           fee,
                            "funding_direction":  -1,
                            "funding_accumulated": 0.0,
                            "reverse_count":      0,
                        }
                        neg_consec = 0

            # ── Exit ──────────────────────────────────────────────────────
            else:
                direction = position["funding_direction"]
                bars_held = idx - position["entry_idx"]
                reversed_now = (direction > 0 and funding < 0) or \
                               (direction < 0 and funding > 0)

                should_close = False

                if is_settlement:
                    if reversed_now:
                        position["reverse_count"] = position.get("reverse_count", 0) + 1
                    else:
                        position["reverse_count"] = 0
                    if position["reverse_count"] >= 3:
                        should_close = True

                if bars_held >= max_hold:
                    should_close = True

                if should_close:
                    exit_price = float(bar["close"])
                    fee_exit   = exit_price * position["size"] * self._fee_rate
                    equity    -= fee_exit
                    net_pnl    = (
                        position.get("funding_accumulated", 0.0)
                        - position["fee_paid"]
                        - fee_exit
                    )
                    trades.append(TradeRecord(
                        open_ts=pd.Timestamp(position["entry_ts"]) if position["entry_ts"] else datetime.min,
                        close_ts=pd.Timestamp(ts) if ts else datetime.min,
                        symbol=SYMBOL,
                        side=position["side"],
                        quantity=position["size"],
                        entry_price=position["entry_price"],
                        exit_price=exit_price,
                        pnl=net_pnl,
                        fee=position["fee_paid"] + fee_exit,
                        duration_hours=float(bars_held),
                    ))
                    position   = None
                    pos_consec = 0
                    neg_consec = 0

            equity_curve.append(equity)

            if equity <= 0:
                equity = 0.0
                if position is not None:
                    position = None
                break

        # Force-close remaining position
        if position is not None and len(bars) > 0:
            bar_last   = bars.iloc[-1]
            exit_price = float(bar_last["close"])
            fee_exit   = exit_price * position["size"] * self._fee_rate
            equity    -= fee_exit
            net_pnl    = (
                position.get("funding_accumulated", 0.0)
                - position["fee_paid"]
                - fee_exit
            )
            trades.append(TradeRecord(
                open_ts=pd.Timestamp(position["entry_ts"]) if position["entry_ts"] else datetime.min,
                close_ts=pd.Timestamp(bar_last.get("ts", bar_last.name)),
                symbol=SYMBOL,
                side=position["side"],
                quantity=position["size"],
                entry_price=position["entry_price"],
                exit_price=exit_price,
                pnl=net_pnl,
                fee=position["fee_paid"] + fee_exit,
                duration_hours=0.0,
            ))
            if equity_curve:
                equity_curve[-1] = equity

        total_profit = equity - self._capital
        winning      = [t for t in trades if t.pnl > 0]
        losing       = [t for t in trades if t.pnl <= 0]
        gross_profit = sum(t.pnl for t in winning)
        gross_loss   = abs(sum(t.pnl for t in losing))

        max_dd, max_dd_pct = _compute_drawdown(equity_curve)
        daily_returns      = _compute_daily_returns(equity_curve)
        sharpe             = _compute_sharpe(daily_returns)
        sortino            = _compute_sortino(daily_returns)
        dd_curve           = _drawdown_series(equity_curve)

        start_str = str(bars.iloc[0].get("ts", "")) if len(bars) > 0 else ""
        end_str   = str(bars.iloc[-1].get("ts", "")) if len(bars) > 0 else ""

        return BacktestResult(
            strategy="fa_param_sweep",
            start_date=start_str,
            end_date=end_str,
            initial_capital=self._capital,
            final_capital=equity,
            total_profit=total_profit,
            total_profit_pct=(total_profit / self._capital * 100) if self._capital > 0 else 0.0,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            win_rate=(len(winning) / len(trades) * 100) if trades else 0.0,
            total_trades=len(trades),
            avg_trade_duration_hours=(
                sum(t.duration_hours for t in trades) / len(trades) if trades else 0.0
            ),
            profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else float("inf"),
            trades=trades,
            equity_curve=equity_curve,
            drawdown_curve=dd_curve,
            daily_returns=daily_returns,
        )

    def _get_funding(self, bar: Any) -> float:
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


# =============================================================================
# Walk-Forward Windowing
# =============================================================================

def build_wf_windows(
    start_dt: datetime,
    end_dt: datetime,
    train_days: int,
    test_days: int,
) -> list[tuple[datetime, datetime, datetime, datetime]]:
    """Return list of (train_start, train_end, test_start, test_end) tuples."""
    windows = []
    train_start = start_dt
    while True:
        train_end  = train_start + timedelta(days=train_days)
        test_start = train_end
        test_end   = test_start + timedelta(days=test_days)
        if test_end > end_dt:
            break
        windows.append((train_start, train_end, test_start, test_end))
        train_start = train_start + timedelta(days=test_days)  # slide by test_days
    return windows


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


async def save_result(
    pool: asyncpg.Pool,
    variant_name: str,
    data_range: str,
    result: BacktestResult,
    extra_params: dict,
) -> None:
    eq = result.equity_curve
    eq_sample = [round(_safe_float(v), 2) for v in eq[::max(1, len(eq) // 200)]]
    params_payload = {**extra_params, "equity_curve_sample": eq_sample}

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
            data_range,
            _safe_float(result.total_profit_pct),
            _safe_float(result.sharpe_ratio),
            _safe_float(result.max_drawdown_pct),
            result.total_trades,
            _safe_float(result.win_rate),
            _safe_float(result.profit_factor, default=0.0),
            json.dumps({}),
            json.dumps(params_payload),
        )


# =============================================================================
# Plateau Detection
# =============================================================================

def analyze_plateau(
    results: list[_SweepResult],
    top_n: int = 10,
) -> tuple[bool, float, list[_SweepResult]]:
    """Return (is_stable, std_of_top10, top_n_list)."""
    top = results[:top_n]
    if not top:
        return False, 0.0, []
    sharpes = [r.avg_oos_sharpe for r in top]
    std = float(np.std(sharpes)) if len(sharpes) > 1 else 0.0
    is_stable = std < 0.1
    return is_stable, std, top


# =============================================================================
# Main
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test M — FA Parameter Sweep with Walk-Forward"
    )
    p.add_argument("--start",        default="2020-04-01", help="YYYY-MM-DD")
    p.add_argument("--end",          default="2026-03-31", help="YYYY-MM-DD")
    p.add_argument("--walk-forward", action="store_true",  default=True)
    p.add_argument("--train-days",   type=int, default=180)
    p.add_argument("--test-days",    type=int, default=90)
    p.add_argument("--symbol",       default=SYMBOL)
    p.add_argument("--timeframe",    default=TIMEFRAME)
    p.add_argument("--no-db",        action="store_true",
                   help="Skip DB save (useful for offline testing)")
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

    args = _parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ── DB setup ─────────────────────────────────────────────────────────────
    pool = None
    if not args.no_db:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    # ── Load full dataset once ────────────────────────────────────────────────
    if pool is not None:
        log.info("loading_data", symbol=args.symbol, timeframe=args.timeframe,
                 start=args.start, end=args.end)
        ohlcv   = await load_ohlcv(pool, args.symbol, args.timeframe, start_dt, end_dt)
        funding = await load_funding(pool, args.symbol, start_dt, end_dt)
    else:
        log.info("no_db_mode_skipping_data_load")
        ohlcv   = pd.DataFrame()
        funding = pd.DataFrame()

    if ohlcv.empty:
        log.error(
            "no_ohlcv_data",
            hint="Run fetch_real_ohlcv.py or seed_historical.py first.",
        )
        if pool:
            await pool.close()
        return

    log.info("data_loaded", ohlcv_bars=len(ohlcv), funding_rows=len(funding))

    # ── Build walk-forward windows ────────────────────────────────────────────
    windows = build_wf_windows(start_dt, end_dt, args.train_days, args.test_days)
    log.info("walk_forward_windows", n=len(windows),
             train_days=args.train_days, test_days=args.test_days)

    if not windows:
        log.error("no_wf_windows", hint="Date range too short for walk-forward config.")
        if pool:
            await pool.close()
        return

    # ── Build param grid ──────────────────────────────────────────────────────
    param_grid = list(product(MIN_RATES, CONSECUTIVES, MAX_HOLD_BARS))
    total_combos   = len(param_grid)
    total_runs     = total_combos * len(windows)
    log.info("param_grid_size", combos=total_combos, windows=len(windows),
             total_runs=total_runs)

    print()
    print(f"Test M — FA Parameter Sweep")
    print(f"Period  : {args.start} ~ {args.end}")
    print(f"WF      : train={args.train_days}d, test={args.test_days}d, "
          f"windows={len(windows)}")
    print(f"Grid    : {total_combos} combos × {len(windows)} windows = {total_runs} runs")
    print()

    # ── Run sweep ─────────────────────────────────────────────────────────────
    sweep_results: list[_SweepResult] = []

    for combo_idx, (min_rate, consec, max_hold) in enumerate(param_grid):
        oos_sharpes: list[float] = []

        for win_idx, (train_s, train_e, test_s, test_e) in enumerate(windows):
            # Slice OOS data
            oos_ohlcv = ohlcv.loc[
                (ohlcv.index >= test_s) & (ohlcv.index <= test_e)
            ]
            oos_fund  = funding.loc[
                (funding.index >= test_s) & (funding.index <= test_e)
            ] if not funding.empty else pd.DataFrame()

            if len(oos_ohlcv) < 50:
                # Skip windows with insufficient data
                continue

            engine = _FAEngine(
                ohlcv=oos_ohlcv,
                funding=oos_fund,
                min_rate=min_rate,
                consecutive=consec,
                max_hold=max_hold,
                fee_rate=FEE_RATE,
                initial_capital=INITIAL_CAPITAL,
            )
            result = engine.run()
            sharpe = _safe_float(result.sharpe_ratio)
            oos_sharpes.append(sharpe)

        avg_sharpe = float(np.mean(oos_sharpes)) if oos_sharpes else 0.0

        sweep_results.append(_SweepResult(
            min_rate=min_rate,
            consecutive=consec,
            max_hold=max_hold,
            oos_sharpes=oos_sharpes,
            avg_oos_sharpe=avg_sharpe,
            total_windows=len(oos_sharpes),
        ))

        # Progress indicator every 10 combos
        if (combo_idx + 1) % 10 == 0:
            print(f"  ... {combo_idx + 1}/{total_combos} combos processed", flush=True)

    print(f"  ... {total_combos}/{total_combos} combos processed (done)")

    # ── Sort by avg OOS Sharpe ────────────────────────────────────────────────
    sweep_results.sort(key=lambda r: r.avg_oos_sharpe, reverse=True)

    # ── Plateau analysis ──────────────────────────────────────────────────────
    is_stable, top10_std, top10 = analyze_plateau(sweep_results, top_n=10)

    # ── Print top 20 table ────────────────────────────────────────────────────
    print()
    print("=" * 90)
    print(f"Top 20 Combinations — Sorted by Avg OOS Sharpe ({args.start} ~ {args.end})")
    print("=" * 90)
    header = (
        f"{'Rank':>4}  "
        f"{'MinRate':>9}  "
        f"{'Consec':>7}  "
        f"{'MaxHold':>8}  "
        f"{'AvgOOS_Sharpe':>14}  "
        f"{'StdOOS':>8}  "
        f"{'Windows':>8}"
    )
    print(header)
    print("-" * 90)

    for rank, r in enumerate(sweep_results[:20], start=1):
        std_sharpe = float(np.std(r.oos_sharpes)) if len(r.oos_sharpes) > 1 else 0.0
        print(
            f"{rank:>4}  "
            f"{r.min_rate:>9.5f}  "
            f"{r.consecutive:>7d}  "
            f"{r.max_hold:>8d}  "
            f"{r.avg_oos_sharpe:>14.4f}  "
            f"{std_sharpe:>8.4f}  "
            f"{r.total_windows:>8d}"
        )

    print("=" * 90)

    # ── Plateau analysis block ────────────────────────────────────────────────
    print()
    print("[ Plateau Stability Analysis — Top 10 ]")
    print(f"  Top-10 Avg OOS Sharpe values:")
    for i, r in enumerate(top10, start=1):
        print(f"    #{i:>2}: MinRate={r.min_rate:.5f}  Consec={r.consecutive}  "
              f"MaxHold={r.max_hold:>3}  AvgSharpe={r.avg_oos_sharpe:.4f}")
    print()
    print(f"  Std of top-10 avg OOS Sharpe = {top10_std:.4f}")

    if is_stable:
        print(f"  RESULT: *** STABLE PLATEAU FOUND *** (std < 0.1)")
    else:
        print(f"  RESULT: No stable plateau — parameter sensitivity is HIGH (std >= 0.1)")

    # ── Recommendation ────────────────────────────────────────────────────────
    best = sweep_results[0]
    print()
    print("[ Recommendation ]")
    print(f"  Best combo : min_rate={best.min_rate:.5f},  consecutive={best.consecutive},  "
          f"max_hold={best.max_hold}")
    print(f"  Avg OOS Sharpe : {best.avg_oos_sharpe:.4f}  "
          f"(over {best.total_windows} OOS windows)")

    if is_stable and top10_std < 0.05:
        print("  Confidence: HIGH — plateau is very tight (std < 0.05). "
              "Any top-10 combo is robust.")
    elif is_stable:
        print("  Confidence: MEDIUM — stable plateau (0.05 ≤ std < 0.1). "
              "Best combo recommended.")
    else:
        print("  Confidence: LOW — no stable plateau. "
              "Exercise caution; results may be overfitted to specific windows.")

    print()

    # ── Save top 20 to DB ─────────────────────────────────────────────────────
    if pool is not None:
        # Clear previous results
        async with pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM strategy_variant_results WHERE test_name = $1",
                TEST_NAME,
            )
            log.info("cleared_previous", deleted=deleted)

        data_range = f"{args.start}~{args.end}"

        for rank, r in enumerate(sweep_results[:20], start=1):
            # Run full-period backtest for this combo to get a complete result
            engine = _FAEngine(
                ohlcv=ohlcv,
                funding=funding,
                min_rate=r.min_rate,
                consecutive=r.consecutive,
                max_hold=r.max_hold,
                fee_rate=FEE_RATE,
                initial_capital=INITIAL_CAPITAL,
            )
            full_result = engine.run()

            variant_name = (
                f"rank{rank:02d}_mr{r.min_rate:.5f}_c{r.consecutive}_mh{r.max_hold}"
            )
            std_sharpe = float(np.std(r.oos_sharpes)) if len(r.oos_sharpes) > 1 else 0.0
            extra = {
                "rank":              rank,
                "min_rate":          r.min_rate,
                "consecutive":       r.consecutive,
                "max_hold":          r.max_hold,
                "avg_oos_sharpe":    r.avg_oos_sharpe,
                "oos_sharpe_std":    std_sharpe,
                "total_wf_windows":  r.total_windows,
                "is_stable_plateau": is_stable,
                "plateau_std":       top10_std,
            }
            await save_result(pool, variant_name, data_range, full_result, extra)
            log.info("saved_rank", rank=rank, variant=variant_name,
                     avg_oos_sharpe=round(r.avg_oos_sharpe, 4))

        log.info("test_m_db_save_complete", saved_combos=min(20, len(sweep_results)))
        await pool.close()

    print("[DONE] Test M complete.")


if __name__ == "__main__":
    asyncio.run(main())
