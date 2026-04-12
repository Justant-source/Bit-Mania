"""
Task 14.1 — FA (Funding Arbitrage) Pure Simulation.

Delta-neutral FA: short perp + long spot.
P&L = funding_income - entry_fee - exit_fee
Price P&L = 0 (delta-neutral by design).

Correctly measures fa80_lev5_r30 performance using real funding data.

Usage:
    python scripts/run_fa_backtest.py \
        --start 2023-04-01 --end 2026-04-01 \
        --output storage/results/FundingArbitrage_main.json

Expected (Phase 9 self-engine baseline):
    3yr CAGR: ~13.11%, Sharpe: ~1.5+, MDD: <-5%
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))

# ─── Parameters: fa80_lev5_r30 ────────────────────────────────────────────────

FA_ALLOCATION  = 0.80   # 80% of equity to FA
LEVERAGE       = 5      # 5x leverage
REINVEST_RATIO = 0.30   # 30% of profit reinvested to spot BTC
MIN_FUNDING    = 0.0001 # 0.01% per 8h minimum threshold
CONSEC_NEEDED  = 3      # 3 consecutive 8h periods above threshold
MAX_HOLD_BARS  = 168    # max 168 hours (7 days) before forced exit
EXIT_REVERSE   = 3      # exit after 3 consecutive reversal periods
TAKER_FEE      = 0.00055
STARTING_BAL   = 10_000.0

SETTLEMENT_HOURS = {0, 8, 16}  # UTC


# ─── Data loaders ─────────────────────────────────────────────────────────────

def load_ohlcv(start: str, end: str) -> list[tuple]:
    """Load 1h OHLCV from Binance Vision parquet. Returns list of (ts_ms, open, close, high, low)."""
    try:
        import polars as pl
    except ImportError:
        raise ImportError("polars required")

    base = DATA_DIR / "binance_vision" / "klines" / "BTCUSDT" / "1h"
    if not base.exists():
        raise FileNotFoundError(f"OHLCV data not found: {base}")

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    # Add 60-day warmup buffer for data loading
    from datetime import timedelta
    load_start = start_dt - timedelta(days=1)

    frames = []
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue
        if year < load_start.year or year > end_dt.year:
            continue
        for f in sorted(year_dir.glob("*.parquet")):
            frames.append(pl.scan_parquet(f))

    if not frames:
        raise FileNotFoundError(f"No parquet files found in {base}")

    df = pl.concat(frames).collect()
    ts_col = "open_time" if "open_time" in df.columns else "timestamp"
    df = df.with_columns(pl.col(ts_col).dt.epoch("ms").alias("ts_ms"))

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)
    df = df.filter((pl.col("ts_ms") >= start_ms) & (pl.col("ts_ms") < end_ms))
    df = df.select([
        pl.col("ts_ms").cast(pl.Int64),
        pl.col("open").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
    ]).sort("ts_ms").unique(subset=["ts_ms"], keep="first").sort("ts_ms")

    print(f"  [ohlcv] {len(df):,} candles ({start} → {end})")
    return [(r["ts_ms"], r["open"], r["close"], r["high"], r["low"]) for r in df.iter_rows(named=True)]


def load_funding(start: str, end: str) -> dict[int, float]:
    """
    Load 8h funding rates from CSV or parquet.
    Returns {settlement_ts_ms: rate}.
    """
    # Try CSV first (BTCUSDT_8h.csv)
    csv_path = DATA_DIR / "funding_rates" / "BTCUSDT_8h.csv"
    parquet_path = DATA_DIR / "funding_rates" / "BTCUSDT_8h.parquet"
    coinalyze_path = DATA_DIR / "coinalyze" / "BTCUSDT_funding.parquet"

    data: dict[int, float] = {}

    try:
        import polars as pl

        if csv_path.exists():
            df = pl.read_csv(csv_path)
        elif parquet_path.exists():
            df = pl.read_parquet(parquet_path)
        elif coinalyze_path.exists():
            df = pl.read_parquet(coinalyze_path)
        else:
            raise FileNotFoundError(
                f"Funding rate data not found. Searched:\n"
                f"  {csv_path}\n  {parquet_path}\n  {coinalyze_path}\n"
                "Run: python scripts/data/fetch_coinalyze_funding.py"
            )

        # Normalize column names
        rename = {}
        for col in df.columns:
            low = col.lower()
            if low in ("timestamp", "timestamp_ms", "time", "ts"):
                rename[col] = "ts"
            elif low in ("rate", "funding_rate", "funding", "value"):
                rename[col] = "rate"
        if rename:
            df = df.rename(rename)

        # Convert datetime → epoch ms if needed
        if str(df["ts"].dtype).startswith("Datetime"):
            df = df.with_columns(pl.col("ts").dt.epoch("ms").alias("ts"))

        start_ms = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms   = int(datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() * 1000)
        df = df.filter((pl.col("ts") >= start_ms) & (pl.col("ts") <= end_ms))

        for row in df.iter_rows(named=True):
            data[int(row["ts"])] = float(row["rate"])

    except ImportError:
        import csv as csvmod
        path = csv_path if csv_path.exists() else parquet_path
        with open(path) as f:
            reader = csvmod.DictReader(f)
            for row in reader:
                ts_key   = next((k for k in row if k.lower() in ("timestamp_ms","timestamp","ts","time")), None)
                rate_key = next((k for k in row if k.lower() in ("rate","funding_rate","funding","value")), None)
                if ts_key and rate_key:
                    data[int(row[ts_key])] = float(row[rate_key])

    print(f"  [funding] {len(data):,} settlement records ({start} → {end})")
    return data


# ─── Settlement detection ─────────────────────────────────────────────────────

def is_settlement(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.hour in SETTLEMENT_HOURS and dt.minute == 0


def get_funding_rate(ts_ms: int, funding_data: dict[int, float]) -> float | None:
    """
    Return funding rate at settlement ts_ms.
    Tries exact match first, then nearest prior settlement.
    """
    if ts_ms in funding_data:
        return funding_data[ts_ms]
    # Look back up to 3 periods (24h) for nearest available rate
    for lag in range(1, 4):
        prev = ts_ms - lag * 8 * 3600 * 1000
        if prev in funding_data:
            return funding_data[prev]
    return None


# ─── Metrics ──────────────────────────────────────────────────────────────────

def calc_cagr(start_val: float, end_val: float, years: float) -> float:
    if years <= 0 or start_val <= 0 or end_val <= 0:
        return 0.0
    return (end_val / start_val) ** (1 / years) - 1


def calc_sharpe(returns: list[float], periods_per_year: float = 365 * 3) -> float:
    """Annualized Sharpe from daily returns list."""
    if len(returns) < 5:
        return 0.0
    arr = np.array(returns, dtype=float)
    mu  = arr.mean()
    std = arr.std(ddof=1)
    if std < 1e-10:
        return 0.0
    return float((mu / std) * math.sqrt(periods_per_year))


def calc_mdd(equity_curve: list[float]) -> float:
    """Maximum drawdown as a negative decimal."""
    if len(equity_curve) < 2:
        return 0.0
    peak = equity_curve[0]
    mdd  = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (val - peak) / peak
        if dd < mdd:
            mdd = dd
    return mdd


# ─── FA Simulator ─────────────────────────────────────────────────────────────

def run_fa_simulation(
    candles:      list[tuple],
    funding_data: dict[int, float],
    start:        str,
    end:          str,
    balance:      float = STARTING_BAL,
    min_funding:  float = MIN_FUNDING,
    consec:       int   = CONSEC_NEEDED,
    fa_alloc:     float = FA_ALLOCATION,
    leverage:     int   = LEVERAGE,
    max_hold:     int   = MAX_HOLD_BARS,
    exit_rev:     int   = EXIT_REVERSE,
    reinvest:     float = REINVEST_RATIO,
) -> dict:
    """
    Pure FA simulation. Delta-neutral: P&L = funding_income - fees.
    Price P&L is explicitly excluded (delta-neutral assumption).

    Returns V5 result dict.
    """
    equity     = balance
    peak_eq    = balance
    position   = None   # dict when open, None when flat

    consec_pos = 0   # consecutive settlement candles with funding >= min
    equity_curve  = [balance]
    daily_returns = []
    prev_day_eq   = balance
    trades: list[dict] = []

    # Reinvestment tracking (spot BTC accumulation)
    spot_btc_qty    = 0.0
    spot_btc_cost   = 0.0
    total_funding_earned = 0.0
    total_fees_paid      = 0.0

    for ts_ms, open_p, close_p, high_p, low_p in candles:
        price = close_p

        if not is_settlement(ts_ms):
            # Not a settlement candle — update equity curve for MDD calc
            equity_curve.append(equity)
            if peak_eq < equity:
                peak_eq = equity
            continue

        rate = get_funding_rate(ts_ms, funding_data)

        # ── No position: check entry ─────────────────────────────────────────
        if position is None:
            if rate is not None and rate >= min_funding:
                consec_pos += 1
            else:
                consec_pos = 0

            if consec_pos >= consec and rate is not None:
                # Open position (short perp side of delta-neutral)
                notional   = equity * fa_alloc * leverage
                entry_fee  = notional * TAKER_FEE
                equity    -= entry_fee
                total_fees_paid += entry_fee

                position = {
                    "entry_ts":      ts_ms,
                    "entry_price":   price,
                    "notional":      notional,   # fixed at entry (for FA)
                    "bars_held":     0,
                    "reverse_count": 0,
                    "direction":     1,           # 1 = short perp (receives funding when rate>0)
                    "entry_equity":  equity,
                    "funding_earned": 0.0,
                }
                consec_pos = 0

        # ── Position open: collect funding + check exit ───────────────────────
        else:
            position["bars_held"] += 1

            # Credit funding income
            if rate is not None:
                # Short receives funding when rate > 0
                funding_pnl = position["notional"] * rate * position["direction"]
                equity += funding_pnl
                position["funding_earned"] += funding_pnl
                total_funding_earned += funding_pnl
            else:
                rate = 0.0

            # Track equity curve
            equity_curve.append(equity)
            if equity > peak_eq:
                peak_eq = equity

            # Check reversal
            if rate < 0:
                position["reverse_count"] += 1
            else:
                position["reverse_count"] = 0

            # Exit conditions
            exit_reason = None
            if position["reverse_count"] >= exit_rev:
                exit_reason = "funding_reversal"
            elif position["bars_held"] >= max_hold:
                exit_reason = "max_hold_bars"

            if exit_reason:
                # Close position
                exit_fee = position["notional"] * TAKER_FEE
                equity  -= exit_fee
                total_fees_paid += exit_fee

                trade_pnl = position["funding_earned"] - exit_fee - (
                    position["notional"] * TAKER_FEE  # entry fee already deducted
                )
                # Correct trade pnl = funding_earned - exit_fee (entry fee already charged)
                trade_pnl_net = position["funding_earned"] - exit_fee

                # Reinvestment: 30% of profit goes to spot BTC
                if trade_pnl_net > 0 and reinvest > 0:
                    reinvest_amt = trade_pnl_net * reinvest
                    reinvest_fee = reinvest_amt * TAKER_FEE  # spot buy fee
                    reinvest_net = reinvest_amt - reinvest_fee
                    btc_qty = reinvest_net / price
                    spot_btc_qty  += btc_qty
                    spot_btc_cost += reinvest_amt
                    equity -= reinvest_amt  # deduct reinvestment from tradeable equity

                trades.append({
                    "entry_ts":       position["entry_ts"],
                    "exit_ts":        ts_ms,
                    "entry_price":    position["entry_price"],
                    "exit_price":     price,
                    "funding_earned": position["funding_earned"],
                    "notional":       position["notional"],
                    "pnl_net":        trade_pnl_net,
                    "bars_held":      position["bars_held"],
                    "exit_reason":    exit_reason,
                })
                position = None
                consec_pos = 0

        # Daily equity snapshot for Sharpe calculation
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if dt.hour == 0:
            if prev_day_eq > 0:
                daily_returns.append((equity - prev_day_eq) / prev_day_eq)
            prev_day_eq = equity

    # Close any open position at end of period
    if position is not None:
        exit_fee = position["notional"] * TAKER_FEE
        equity  -= exit_fee
        total_fees_paid += exit_fee
        trade_pnl_net = position["funding_earned"] - exit_fee
        trades.append({
            "entry_ts":    position["entry_ts"],
            "exit_ts":     candles[-1][0],
            "funding_earned": position["funding_earned"],
            "pnl_net":     trade_pnl_net,
            "exit_reason": "end_of_period",
        })

    # Add spot BTC appreciation to final equity
    if spot_btc_qty > 0 and candles:
        final_price = candles[-1][2]
        spot_value  = spot_btc_qty * final_price
        spot_pnl    = spot_value - spot_btc_cost
        equity += spot_pnl  # add unrealized spot BTC appreciation

    # ── Metrics ───────────────────────────────────────────────────────────────
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    years    = (end_dt - start_dt).days / 365.25

    cagr   = calc_cagr(balance, equity, years)
    sharpe = calc_sharpe(daily_returns, periods_per_year=365)
    mdd    = calc_mdd(equity_curve)

    num_trades = len(trades)
    wins       = [t for t in trades if t.get("pnl_net", 0) > 0]
    win_rate   = len(wins) / num_trades if num_trades > 0 else 0.0

    gross_pnl = total_funding_earned
    net_profit_pct = (equity - balance) / balance * 100

    return {
        "strategy":   "FundingArbitrage",
        "config":     "fa80_lev5_r30",
        "start":      start,
        "end":        end,
        "years":      round(years, 2),
        "cagr":       round(cagr, 4),
        "sharpe":     round(sharpe, 4),
        "mdd":        round(mdd, 4),
        "num_trades": num_trades,
        "win_rate":   round(win_rate, 4),
        "gross_pnl":  round(gross_pnl, 2),
        "total_fees": round(total_fees_paid, 2),
        "net_profit_percentage": round(net_profit_pct, 4),
        "starting_balance":  round(balance, 2),
        "final_equity":      round(equity, 2),
        "total_funding_earned": round(total_funding_earned, 2),
        "spot_btc_qty":    round(spot_btc_qty, 6),
        "spot_btc_cost":   round(spot_btc_cost, 2),
        "trades_detail":   trades[:10],  # first 10 trades for inspection
    }


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="FA Pure Simulation (fa80_lev5_r30)")
    p.add_argument("--start",   default="2023-04-01")
    p.add_argument("--end",     default="2026-04-01")
    p.add_argument("--balance", type=float, default=STARTING_BAL)
    p.add_argument("--output",  default=None, help="JSON output path")
    # Hyperparameter overrides
    p.add_argument("--min-funding",  type=float, default=MIN_FUNDING)
    p.add_argument("--consec",       type=int,   default=CONSEC_NEEDED)
    p.add_argument("--fa-alloc",     type=float, default=FA_ALLOCATION)
    p.add_argument("--leverage",     type=int,   default=LEVERAGE)
    p.add_argument("--max-hold",     type=int,   default=MAX_HOLD_BARS)
    p.add_argument("--exit-rev",     type=int,   default=EXIT_REVERSE)
    p.add_argument("--reinvest",     type=float, default=REINVEST_RATIO)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print(f"\n[FA Simulation] fa80_lev5_r30 | {args.start} → {args.end}")
    print(f"  Balance: ${args.balance:,.0f}  Alloc: {args.fa_alloc:.0%}  Lev: {args.leverage}x  Reinvest: {args.reinvest:.0%}")

    candles      = load_ohlcv(args.start, args.end)
    funding_data = load_funding(args.start, args.end)

    result = run_fa_simulation(
        candles      = candles,
        funding_data = funding_data,
        start        = args.start,
        end          = args.end,
        balance      = args.balance,
        min_funding  = args.min_funding,
        consec       = args.consec,
        fa_alloc     = args.fa_alloc,
        leverage     = args.leverage,
        max_hold     = args.max_hold,
        exit_rev     = args.exit_rev,
        reinvest     = args.reinvest,
    )

    # Print summary
    print(f"\n  ─────────────────────────────────────────────")
    print(f"  CAGR:         {result['cagr']:.2%}")
    print(f"  Sharpe:       {result['sharpe']:.3f}")
    print(f"  MDD:          {result['mdd']:.2%}")
    print(f"  Trades:       {result['num_trades']}")
    print(f"  Win Rate:     {result['win_rate']:.1%}")
    print(f"  Funding Earn: ${result['total_funding_earned']:,.2f}")
    print(f"  Total Fees:   ${result['total_fees']:,.2f}")
    print(f"  Final Equity: ${result['final_equity']:,.2f}")
    print(f"  Spot BTC:     {result['spot_btc_qty']:.4f} BTC")
    print(f"  ─────────────────────────────────────────────")

    # V5 criteria check (FA-specific: CAGR >= 10%, Sharpe >= 1.0, MDD >= -15%)
    ok_cagr   = result['cagr']   >= 0.10
    ok_sharpe = result['sharpe'] >= 1.0
    ok_mdd    = result['mdd']    >= -0.15
    print(f"  V5: CAGR {'✓' if ok_cagr else '✗'}  Sharpe {'✓' if ok_sharpe else '✗'}  MDD {'✓' if ok_mdd else '✗'}")

    # Phase 9 comparison (+13.11% CAGR self-engine baseline)
    delta = result['cagr'] - 0.1311
    print(f"  Phase 9 delta: {delta:+.2%}  ({'within ±2%' if abs(delta) <= 0.02 else 'outside ±2%'})")

    # Save JSON
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n  Saved: {args.output}")
    else:
        print("\n" + json.dumps(result, indent=2))
