"""Test N — Fee Sensitivity Analysis

Compares 4 fee scenarios (taker-only, maker-only, mixed 70/30, VIP3) to quantify
the impact of maker/taker order mix on the FA short_hold strategy.

Strategy: FA short_hold (Method B / natural — no rebalancing on regime change)
  consecutive=3, min_rate=0.0001, max_hold=168 bars (7 days)

Usage:
    python tests/backtest/bt_fee_sensitivity.py \\
        --fee-scenarios taker_only,maker_only,mixed_70_30,vip3 \\
        --start 2020-04-01 --end 2026-03-31
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
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
TEST_NAME       = "test_n_fee_sensitivity"

# ── FA weights by regime (Phase 4 final) ─────────────────────────────────────
FA_WEIGHTS: dict[str, float] = {
    "ranging":       0.50,
    "trending_up":   0.20,
    "trending_down": 0.10,
    "volatile":      0.40,
}

# ── FA short_hold parameters (Method B — natural, no rebalancing) ─────────────
FA_PARAMS = {
    "consecutive_intervals": 3,
    "min_funding_rate":      0.0001,
    "max_hold_bars":         168,
}

# ── Fee scenarios ─────────────────────────────────────────────────────────────
#  effective_fee_rate = maker_rate * maker_pct + taker_rate * (1 - maker_pct)
FEE_SCENARIOS: dict[str, dict[str, float]] = {
    "taker_only":  {"maker": 0.00055, "taker": 0.00055, "maker_pct": 0.0},
    "maker_only":  {"maker": 0.00020, "taker": 0.00020, "maker_pct": 1.0},
    "mixed_70_30": {"maker": 0.00020, "taker": 0.00055, "maker_pct": 0.7},
    "vip3":        {"maker": 0.00014, "taker": 0.00035, "maker_pct": 0.7},
}

ALL_SCENARIO_NAMES = list(FEE_SCENARIOS.keys())


def effective_fee_rate(scenario: dict[str, float]) -> float:
    return scenario["maker"] * scenario["maker_pct"] + \
           scenario["taker"] * (1.0 - scenario["maker_pct"])


# =============================================================================
# Regime Detection  (same as other test scripts)
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

    adx   = _compute_adx(high, low, close, period=period)
    ema20 = float(pd.Series(close).ewm(span=20, adjust=False).mean().iloc[-1])

    if adx >= 25.0:
        return "trending_up" if float(close[-1]) > ema20 else "trending_down"
    return "ranging"


# =============================================================================
# FA Engine (Method B — natural, fee-rate parameterized)
# =============================================================================

class _FAFeeEngine:
    """FA short_hold engine with configurable effective fee rate.

    Method B (natural): no rebalancing on regime change — existing position
    runs to its natural exit. New entries use the current regime weight.
    """

    def __init__(
        self,
        *,
        ohlcv: pd.DataFrame,
        funding: pd.DataFrame,
        fee_rate: float,
        initial_capital: float = INITIAL_CAPITAL,
        lookback_bars: int = 50,
    ) -> None:
        self._ohlcv    = ohlcv
        self._funding  = funding
        self._fee_rate = fee_rate
        self._capital  = initial_capital
        self._lookback = lookback_bars

        # FA short_hold params (fixed for this test)
        self._consec   = FA_PARAMS["consecutive_intervals"]
        self._min_rate = FA_PARAMS["min_funding_rate"]
        self._max_hold = FA_PARAMS["max_hold_bars"]

    def run(self) -> tuple[BacktestResult, float]:
        """Returns (BacktestResult, total_fees_paid_usd)."""
        consec_thresh = self._consec
        min_rate      = self._min_rate
        max_hold      = self._max_hold

        equity       = self._capital
        equity_curve: list[float] = [equity]
        trades: list[TradeRecord] = []
        total_fees   = 0.0

        bars = self._ohlcv.reset_index()
        n    = len(bars)

        start_idx  = self._lookback
        position: dict[str, Any] | None = None
        pos_consec = 0
        neg_consec = 0
        # Natural method: track weight used at entry; don't resize on regime change
        position_fa_weight: float = FA_WEIGHTS["ranging"]
        current_fa_weight:  float = FA_WEIGHTS["ranging"]

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

            # ── Regime detection (natural method: weight used for NEW entries) ──
            lb_start = max(0, idx - self._lookback)
            lookback = bars.iloc[lb_start: idx + 1]
            regime   = detect_regime(lookback)
            current_fa_weight = FA_WEIGHTS[regime]

            # ── Funding settlement ────────────────────────────────────────
            if position is not None and is_settlement:
                direction = position["funding_direction"]
                pos_value = position["size"] * position["entry_price"]
                net_fund  = pos_value * funding * direction
                equity   += net_fund
                position["funding_accumulated"] = (
                    position.get("funding_accumulated", 0.0) + net_fund
                )

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
                        # Use current (new-entry) weight
                        size  = (equity * current_fa_weight * 0.95) / entry
                        fee   = entry * size * self._fee_rate
                        equity    -= fee
                        total_fees += fee
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
                        position_fa_weight = current_fa_weight
                        pos_consec = 0

                    elif neg_consec >= consec_thresh:
                        side  = "buy"
                        entry = float(bar["close"])
                        size  = (equity * current_fa_weight * 0.95) / entry
                        fee   = entry * size * self._fee_rate
                        equity    -= fee
                        total_fees += fee
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
                        position_fa_weight = current_fa_weight
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
                    total_fees += fee_exit
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
                position = None
                break

        # Force-close remaining position
        if position is not None and len(bars) > 0:
            bar_last   = bars.iloc[-1]
            exit_price = float(bar_last["close"])
            fee_exit   = exit_price * position["size"] * self._fee_rate
            equity    -= fee_exit
            total_fees += fee_exit
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

        result = BacktestResult(
            strategy="fa_short_hold_fee_sensitivity",
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
        return result, total_fees

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
    scenario_name: str,
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
            scenario_name,
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
# Main
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Test N — FA Fee Sensitivity Analysis"
    )
    p.add_argument(
        "--fee-scenarios",
        default=",".join(ALL_SCENARIO_NAMES),
        help=(
            "Comma-separated fee scenario names. "
            f"Available: {', '.join(ALL_SCENARIO_NAMES)} "
            f"(default: all)"
        ),
    )
    p.add_argument("--start",     default="2020-04-01", help="YYYY-MM-DD")
    p.add_argument("--end",       default="2026-03-31", help="YYYY-MM-DD")
    p.add_argument("--symbol",    default=SYMBOL)
    p.add_argument("--timeframe", default=TIMEFRAME)
    p.add_argument("--no-db",     action="store_true",
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

    # ── Validate scenario names ───────────────────────────────────────────────
    requested = [s.strip() for s in args.fee_scenarios.split(",") if s.strip()]
    invalid   = [s for s in requested if s not in FEE_SCENARIOS]
    if invalid:
        log.error("unknown_fee_scenarios", invalid=invalid,
                  valid=ALL_SCENARIO_NAMES)
        sys.exit(1)

    scenarios_to_run = requested if requested else ALL_SCENARIO_NAMES

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ── DB setup ─────────────────────────────────────────────────────────────
    pool = None
    if not args.no_db:
        pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=5)

    # ── Load data once ────────────────────────────────────────────────────────
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

    # ── Clear previous results ────────────────────────────────────────────────
    if pool is not None:
        async with pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM strategy_variant_results WHERE test_name = $1",
                TEST_NAME,
            )
            log.info("cleared_previous", deleted=deleted)

    # ── Run each fee scenario ─────────────────────────────────────────────────
    run_results: dict[str, tuple[BacktestResult, float, float]] = {}
    # scenario_name -> (BacktestResult, total_fees_usd, eff_fee_rate)

    for scenario_name in scenarios_to_run:
        scenario   = FEE_SCENARIOS[scenario_name]
        eff_rate   = effective_fee_rate(scenario)

        log.info("running_scenario", scenario=scenario_name,
                 effective_fee_pct=round(eff_rate * 100, 4))

        engine = _FAFeeEngine(
            ohlcv=ohlcv,
            funding=funding,
            fee_rate=eff_rate,
            initial_capital=INITIAL_CAPITAL,
        )
        result, total_fees = engine.run()
        run_results[scenario_name] = (result, total_fees, eff_rate)

    # ── Print comparison table ────────────────────────────────────────────────
    print()
    print("=" * 100)
    print(f"Test N — Fee Sensitivity Analysis ({args.start} ~ {args.end})")
    print(f"Strategy: FA short_hold (Method B / natural)  "
          f"consec={FA_PARAMS['consecutive_intervals']}, "
          f"min_rate={FA_PARAMS['min_funding_rate']}, "
          f"max_hold={FA_PARAMS['max_hold_bars']}")
    print("=" * 100)
    header = (
        f"{'Scenario':<16}  "
        f"{'EffFee%':>8}  "
        f"{'Return%':>9}  "
        f"{'Sharpe':>8}  "
        f"{'MDD%':>7}  "
        f"{'Trades':>7}  "
        f"{'TotalFees$':>12}"
    )
    print(header)
    print("-" * 100)

    for scenario_name in scenarios_to_run:
        result, total_fees, eff_rate = run_results[scenario_name]
        eff_pct    = eff_rate * 100
        ret_pct    = _safe_float(result.total_profit_pct)
        sharpe     = _safe_float(result.sharpe_ratio)
        mdd_pct    = _safe_float(result.max_drawdown_pct)
        trades     = result.total_trades

        print(
            f"{scenario_name:<16}  "
            f"{eff_pct:>7.3f}%  "
            f"{ret_pct:>8.2f}%  "
            f"{sharpe:>8.3f}  "
            f"{mdd_pct:>6.2f}%  "
            f"{trades:>7d}  "
            f"${total_fees:>11.2f}"
        )

    print("=" * 100)

    # ── Delta analysis vs taker_only baseline ────────────────────────────────
    if "taker_only" in run_results:
        baseline_ret    = _safe_float(run_results["taker_only"][0].total_profit_pct)
        baseline_sharpe = _safe_float(run_results["taker_only"][0].sharpe_ratio)
        baseline_fees   = run_results["taker_only"][1]

        print()
        print("[ Delta vs taker_only baseline ]")
        delta_header = (
            f"{'Scenario':<16}  "
            f"{'DeltaReturn%':>13}  "
            f"{'DeltaSharpe':>12}  "
            f"{'FeesSaved$':>12}"
        )
        print(delta_header)
        print("-" * 60)
        for scenario_name in scenarios_to_run:
            if scenario_name == "taker_only":
                continue
            result, total_fees, _ = run_results[scenario_name]
            delta_ret    = _safe_float(result.total_profit_pct) - baseline_ret
            delta_sharpe = _safe_float(result.sharpe_ratio) - baseline_sharpe
            fees_saved   = baseline_fees - total_fees
            sign_r = "+" if delta_ret >= 0 else ""
            sign_s = "+" if delta_sharpe >= 0 else ""
            sign_f = "+" if fees_saved >= 0 else ""
            print(
                f"{scenario_name:<16}  "
                f"{sign_r}{delta_ret:>12.2f}%  "
                f"{sign_s}{delta_sharpe:>11.4f}  "
                f"{sign_f}${fees_saved:>11.2f}"
            )
        print("=" * 60)

    # ── Recommendation ────────────────────────────────────────────────────────
    best_name = max(
        scenarios_to_run,
        key=lambda s: _safe_float(run_results[s][0].sharpe_ratio),
    )
    best_result, best_fees, best_eff = run_results[best_name]
    print()
    print("[ Recommendation ]")
    print(f"  Best scenario  : {best_name}  "
          f"(eff_fee={best_eff*100:.3f}%)")
    print(f"  Sharpe         : {_safe_float(best_result.sharpe_ratio):.4f}")
    print(f"  Return         : {_safe_float(best_result.total_profit_pct):.2f}%")
    print(f"  Total fees paid: ${best_fees:.2f}")

    # Cost of improving maker fill rate
    if "taker_only" in run_results and best_name != "taker_only":
        fees_saved = run_results["taker_only"][1] - best_fees
        print(f"  Fee savings vs taker_only: ${fees_saved:.2f} "
              f"over {args.start} ~ {args.end}")
    print()

    # ── Save to DB ────────────────────────────────────────────────────────────
    if pool is not None:
        data_range = f"{args.start}~{args.end}"
        for scenario_name in scenarios_to_run:
            scenario   = FEE_SCENARIOS[scenario_name]
            eff_rate   = effective_fee_rate(scenario)
            result, total_fees, _ = run_results[scenario_name]

            extra = {
                "scenario":              scenario_name,
                "maker_rate":            scenario["maker"],
                "taker_rate":            scenario["taker"],
                "maker_pct":             scenario["maker_pct"],
                "effective_fee_rate":    eff_rate,
                "effective_fee_pct":     round(eff_rate * 100, 4),
                "total_fees_paid_usd":   round(total_fees, 4),
                "fa_params":             FA_PARAMS,
                "method":                "natural",
            }
            await save_result(pool, scenario_name, data_range, result, extra)
            log.info(
                "result_saved",
                scenario=scenario_name,
                return_pct=round(_safe_float(result.total_profit_pct), 2),
                sharpe=round(_safe_float(result.sharpe_ratio), 3),
                trades=result.total_trades,
                total_fees=round(total_fees, 2),
            )

        await pool.close()
        log.info("test_n_db_save_complete")

    print("[DONE] Test N complete.")


if __name__ == "__main__":
    asyncio.run(main())
