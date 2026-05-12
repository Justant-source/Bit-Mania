"""
Jesse 1.x Research API Backtest Runner.

Usage:
    python scripts/run_backtest.py \
        --strategy IntradaySeasonality \
        --start 2023-04-01 --end 2026-04-01 \
        --output storage/results/IntradaySeasonality_main.json

    python scripts/run_backtest.py \
        --strategy BtcBuyAndHold \
        --start 2024-01-01 --end 2024-12-31 \
        --output storage/results/sanity_check.json

Candle format (Jesse): [timestamp_ms, open, CLOSE, high, low, volume]
  - Index 0: timestamp in milliseconds
  - Index 1: open
  - Index 2: close  <- NOTE: close is at index 2, not 4!
  - Index 3: high
  - Index 4: low
  - Index 5: volume
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

# Add strategies dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
EXCHANGE_NAME = "Bybit Perpetual"
SYMBOL = "BTC-USDT"
TIMEFRAME = "1h"


# ─── Parquet → Jesse numpy loader ─────────────────────────────────────────────

def load_candles_np(start: str, end: str, timeframe: str = "1h") -> np.ndarray:
    """
    Load Binance Vision parquet files and convert to Jesse candle format.

    Jesse candle format: [timestamp_ms, open, close, high, low, volume]
    Parquet columns:     open_time(datetime), open, high, low, close, volume, ...

    Args:
        start: ISO date string, e.g. '2023-04-01'
        end:   ISO date string, e.g. '2026-04-01'
        timeframe: '1h' or '1d'

    Returns:
        numpy array of shape (N, 6), dtype float64
    """
    try:
        import polars as pl
    except ImportError:
        raise ImportError("polars required: pip install polars")

    base = DATA_DIR / "binance_vision" / "klines" / "BTCUSDT" / timeframe

    if not base.exists():
        raise FileNotFoundError(
            f"Kline data directory not found: {base}\n"
            "Expected layout: /data/binance_vision/klines/BTCUSDT/1h/<year>/<month>.parquet"
        )

    # Parse date range
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    frames = []
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue
        if year < start_dt.year - 1 or year > end_dt.year + 1:
            continue
        for f in sorted(year_dir.glob("*.parquet")):
            frames.append(pl.scan_parquet(f))

    if not frames:
        raise FileNotFoundError(f"No parquet files found in {base}")

    df = pl.concat(frames).collect()

    # Timestamp conversion: open_time is datetime[ms, UTC]
    if "open_time" in df.columns:
        ts_col = "open_time"
    elif "timestamp" in df.columns:
        ts_col = "timestamp"
    else:
        raise ValueError(f"No timestamp column found. Columns: {df.columns}")

    # Convert datetime to epoch milliseconds
    df = df.with_columns(
        pl.col(ts_col).dt.epoch("ms").alias("ts_ms")
    )

    # Filter by date range
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)
    df = df.filter(
        (pl.col("ts_ms") >= start_ms) & (pl.col("ts_ms") < end_ms)
    )

    if len(df) == 0:
        raise ValueError(
            f"No candles found for range {start} → {end}. "
            "Check that data files cover this period."
        )

    # Select columns in Jesse format: [timestamp_ms, open, close, high, low, volume]
    df = df.select([
        pl.col("ts_ms").cast(pl.Float64),
        pl.col("open").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ]).sort("ts_ms")

    # Deduplicate
    df = df.unique(subset=["ts_ms"], keep="first").sort("ts_ms")

    arr = df.to_numpy()
    print(f"  [candles] Loaded {len(arr):,} candles ({start} → {end}, {timeframe})")
    return arr


def load_daily_candles_np(start: str, end: str) -> np.ndarray:
    """Load daily candles for trend indicators (50-day SMA etc.)."""
    # Load 1h candles and resample to daily
    # Jesse can use multiple timeframes if we provide data_routes
    # For simplicity, load 1d parquet directly
    try:
        import polars as pl
    except ImportError:
        raise ImportError("polars required")

    base = DATA_DIR / "binance_vision" / "klines" / "BTCUSDT" / "1d"
    if not base.exists():
        # Fall back: resample 1h
        print("  [candles] No 1d data dir, using 1h data resampled to 1d")
        arr_1h = load_candles_np(start, end, "1h")
        return _resample_to_daily(arr_1h)

    frames = []
    for year_dir in sorted(base.iterdir()):
        if year_dir.is_dir():
            for f in sorted(year_dir.glob("*.parquet")):
                frames.append(pl.scan_parquet(f))

    if not frames:
        return _resample_to_daily(load_candles_np(start, end, "1h"))

    df = pl.concat(frames).collect()

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    ts_col = "open_time" if "open_time" in df.columns else "timestamp"
    df = df.with_columns(pl.col(ts_col).dt.epoch("ms").alias("ts_ms"))
    df = df.filter((pl.col("ts_ms") >= start_ms) & (pl.col("ts_ms") < end_ms))
    df = df.select([
        pl.col("ts_ms").cast(pl.Float64),
        pl.col("open").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ]).sort("ts_ms").unique(subset=["ts_ms"], keep="first").sort("ts_ms")

    return df.to_numpy()


def upsample_1h_to_1m(arr_1h: np.ndarray) -> np.ndarray:
    """
    Upsample 1h candles to 1m candles required by Jesse 1.x.

    Jesse internally uses 1-minute as the base timeframe. When you pass
    1h candles, Jesse upsamples them by treating each 1h candle as 60
    identical 1m candles.

    For each 1h candle [ts, open, close, high, low, volume]:
      - Creates 60 1m candles at ts, ts+60s, ts+120s, ..., ts+3540s
      - All 60 share the same open, close, high, low
      - Volume is distributed evenly (vol/60 per minute)
    """
    if len(arr_1h) == 0:
        return arr_1h

    MINUTE_MS = 60_000
    n = len(arr_1h)
    out = np.empty((n * 60, 6), dtype=np.float64)

    for i, row in enumerate(arr_1h):
        ts, op, cl, hi, lo, vol = row[0], row[1], row[2], row[3], row[4], row[5]
        vol_per_min = vol / 60.0
        base = i * 60
        for m in range(60):
            out[base + m, 0] = ts + m * MINUTE_MS  # timestamp_ms
            out[base + m, 1] = op                   # open
            out[base + m, 2] = cl                   # close
            out[base + m, 3] = hi                   # high
            out[base + m, 4] = lo                   # low
            out[base + m, 5] = vol_per_min          # volume

    return out


def _resample_to_daily(arr_1h: np.ndarray) -> np.ndarray:
    """Resample 1h candles to daily by grouping by UTC date."""
    if len(arr_1h) == 0:
        return arr_1h
    # Group by day
    day_ms = 86_400_000
    days: dict[int, list] = {}
    for row in arr_1h:
        day_key = int(row[0] // day_ms) * day_ms
        days.setdefault(day_key, []).append(row)
    result = []
    for day_ts in sorted(days.keys()):
        group = days[day_ts]
        ts  = float(day_ts)
        op  = group[0][1]
        cl  = group[-1][2]
        hi  = max(r[3] for r in group)
        lo  = min(r[4] for r in group)
        vol = sum(r[5] for r in group)
        result.append([ts, op, cl, hi, lo, vol])
    return np.array(result, dtype=np.float64)


# ─── Strategy loader ───────────────────────────────────────────────────────────

BUILT_IN_STRATEGIES = {}  # populated lazily


def load_strategy_class(name: str):
    """
    Load strategy class by name.
    Searches in strategies/ directory and built-ins.
    """
    # Check built-in first
    if name in BUILT_IN_STRATEGIES:
        return BUILT_IN_STRATEGIES[name]

    # Try loading from strategies/<name lower>.py
    strategy_map = {
        "BtcBuyAndHold":                      "strategies.sanity_check",
        "IntradaySeasonality":                "strategies.intraday_seasonality",
        "MacroEvent":                         "strategies.macro_event",
        "ContrarianSentimentStandalone":      "strategies.contrarian_sentiment",
        "SeasonalityWithFGFilter":            "strategies.contrarian_sentiment",
        "FundingArbitrage":                   "strategies.funding_arbitrage",
        "FundingArbitrageWithMacroFilter":    "strategies.funding_arbitrage_v2",
        "FundingArbitrageWithFGSizer":        "strategies.funding_arbitrage_v3",
    }

    module_path = strategy_map.get(name)
    if not module_path:
        raise ValueError(
            f"Unknown strategy: '{name}'. "
            f"Known: {list(strategy_map.keys())}"
        )

    mod = importlib.import_module(module_path)
    cls = getattr(mod, name)
    return cls


# ─── Result formatter ──────────────────────────────────────────────────────────

def format_result(raw: dict, strategy: str, start: str, end: str) -> dict:
    """
    Convert Jesse raw result to V5 report format.

    Jesse result structure:
      result['metrics'] = {
        'total': int,           # total trades
        'win_rate': float,      # 0-1
        'net_profit_percentage': float,
        ... and more
      }
    """
    metrics = raw.get("metrics", {}) or {}

    # Extract key metrics (handle both old and new Jesse metric names)
    num_trades = metrics.get("total", 0) or 0
    win_rate   = metrics.get("win_rate", 0) or 0
    net_profit_pct = metrics.get("net_profit_percentage", 0) or 0

    # Calculate CAGR from net profit
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    years    = (end_dt - start_dt).days / 365.25
    cagr = ((1 + net_profit_pct / 100) ** (1 / years) - 1) if years > 0 and (1 + net_profit_pct/100) > 0 else 0

    sharpe   = metrics.get("sharpe_ratio", metrics.get("sharpe", 0)) or 0
    # Jesse returns max_drawdown as percentage (e.g. -25.3), normalize to decimal (-0.253)
    mdd_pct  = metrics.get("max_drawdown", metrics.get("mdd", 0)) or 0
    mdd      = float(mdd_pct) / 100.0
    fees     = metrics.get("fee", metrics.get("total_fee_in_dollar", metrics.get("total_fees", 0))) or 0
    gross    = metrics.get("gross_profit", metrics.get("gross_pnl", 0)) or 0
    annual_return = metrics.get("annual_return", net_profit_pct) or 0
    # Prefer annual_return for CAGR (Jesse already annualizes it for multi-year backtests)
    cagr = float(annual_return) / 100.0 if abs(float(annual_return)) < 10000 else cagr

    return {
        "strategy":   strategy,
        "start":      start,
        "end":        end,
        "cagr":       round(cagr, 4),
        "sharpe":     round(float(sharpe), 4),
        "mdd":        round(mdd, 4),
        "num_trades": int(num_trades),
        "win_rate":   round(float(win_rate), 4),
        "gross_pnl":  round(float(gross), 2),
        "total_fees": round(float(fees), 2),
        "net_profit_percentage": round(float(net_profit_pct), 4),
        "raw_metrics": metrics,
    }


# ─── Main backtest runner ──────────────────────────────────────────────────────

def run_backtest(
    strategy_name: str,
    start: str,
    end: str,
    starting_balance: float = 10_000,
    fee: float = 0.00055,
    leverage: int = 3,
    timeframe: str = "1h",
) -> dict:
    """
    Run a full Jesse backtest using the research API.
    """
    from jesse import research
    import jesse.helpers as jh

    print(f"\n[run_backtest] {strategy_name} | {start} → {end}")

    # 1. Load 1h candles and upsample to 1m (Jesse 1.x requires 1m base timeframe)
    candles_1h = load_candles_np(start, end, "1h")
    candles_1m = upsample_1h_to_1m(candles_1h)
    print(f"  Upsampled {len(candles_1h):,} 1h → {len(candles_1m):,} 1m candles")

    # Warm-up: 60 days before start (for 50-day SMA etc.)
    from datetime import timedelta
    warmup_start = (
        datetime.fromisoformat(start).replace(tzinfo=timezone.utc) - timedelta(days=60)
    ).strftime("%Y-%m-%d")
    warmup_1h = load_candles_np(warmup_start, start, "1h")
    warmup_candles_arr = upsample_1h_to_1m(warmup_1h)
    print(f"  Warmup: {len(warmup_1h):,} 1h → {len(warmup_candles_arr):,} 1m candles")

    # 2. Configure Jesse
    config = {
        "starting_balance":    starting_balance,
        "fee":                 fee,
        "type":                "futures",
        "futures_leverage":    leverage,
        "futures_leverage_mode": "cross",
        "exchange":            EXCHANGE_NAME,
        "warm_up_candles":     len(warmup_candles_arr),
    }

    # 3. Load strategy class
    strategy_cls = load_strategy_class(strategy_name)

    # 4. Define routes — Jesse uses 1m base, strategy reads 1h via get_candles()
    routes = [{
        "exchange":  EXCHANGE_NAME,
        "strategy":  strategy_cls,
        "symbol":    SYMBOL,
        "timeframe": "1h",   # strategy's primary timeframe (Jesse aggregates from 1m)
    }]

    # 5. Build candle dict (pass 1m candles — Jesse aggregates to 1h internally)
    candle_key = jh.key(EXCHANGE_NAME, SYMBOL)
    candles_dict = {
        candle_key: {
            "exchange": EXCHANGE_NAME,
            "symbol":   SYMBOL,
            "candles":  candles_1m,
        }
    }
    warmup_dict = {
        candle_key: {
            "exchange": EXCHANGE_NAME,
            "symbol":   SYMBOL,
            "candles":  warmup_candles_arr,
        }
    }

    # 6. Run backtest
    print(f"  Starting Jesse research backtest...")
    raw = research.backtest(
        config      = config,
        routes      = routes,
        data_routes = [],
        candles     = candles_dict,
        warmup_candles = warmup_dict,
        generate_json  = True,
    )

    return format_result(raw, strategy_name, start, end)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Jesse 1.x Research API Backtest Runner")
    p.add_argument("--strategy", required=True, help="Strategy class name")
    p.add_argument("--start",    default="2023-04-01")
    p.add_argument("--end",      default="2026-04-01")
    p.add_argument("--output",   default=None, help="JSON output file path")
    p.add_argument("--balance",  type=float, default=10_000)
    p.add_argument("--fee",      type=float, default=0.00055)
    p.add_argument("--leverage", type=int,   default=3)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    result = run_backtest(
        strategy_name    = args.strategy,
        start            = args.start,
        end              = args.end,
        starting_balance = args.balance,
        fee              = args.fee,
        leverage         = args.leverage,
    )

    # Print summary
    print(f"\n  Result: CAGR={result['cagr']:.2%} Sharpe={result['sharpe']:.3f} "
          f"MDD={result['mdd']:.2%} Trades={result['num_trades']}")
    # V5 criteria quick check
    cagr_ok   = result['cagr'] >= 0.10
    sharpe_ok = result['sharpe'] >= 1.0
    mdd_ok    = abs(result['mdd']) <= 0.15
    print(f"  V5 Quick: CAGR {'✓' if cagr_ok else '✗'}  Sharpe {'✓' if sharpe_ok else '✗'}  MDD {'✓' if mdd_ok else '✗'}")

    # Save JSON
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"  Saved: {args.output}")
    else:
        print(json.dumps(result, indent=2))
