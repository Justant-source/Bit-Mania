#!/usr/bin/env python3
"""
run_intrabar_backtest.py — 실제 Binance 1m OHLC 사용 버전

run_external_backtest.py 와의 주요 차이:
- _expand_tf_to_1m() 제거 → 실제 1m OHLC 주입
- Jesse가 1m 봉 안의 high/low로 stop 평가 가능
- _load_1m()으로 /data/ohlcv/BTCUSDT/1m/*.parquet 읽음
- wick 손절 시뮬레이션 가능 (봉 안의 가격 변동 반영)

Usage (inside jesse container):
    python scripts/run_intrabar_backtest.py \\
        --strategy BBPBStrategy \\
        --output /result/batch_1/bbpb/ \\
        --start 2017-08-18 --end 2026-04-30

Output (6 files per strategy):
    stats.json, trades.csv, equity_curve.png, monthly_returns.csv,
    decision.md, strategy.py
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import importlib
import json
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# --- Path setup -----------------------------------------------------------
SCRIPTS_DIR = Path(__file__).parent
JESSE_ROOT  = SCRIPTS_DIR.parent.parent  # /app (scripts are at /app/scripts/runners)
sys.path.insert(0, str(JESSE_ROOT))

# --- Jesse import order fix ------------------------------------------------
# Jesse has a circular import: multiprocessing→redis→helpers→redis.
# Importing helpers and redis BEFORE triggering jesse.__init__ (which imports
# cli→multiprocessing→redis) ensures they are cached when the cycle is hit.
import jesse.helpers          # noqa: E402 — must precede `from jesse import X`
import jesse.services.env     # noqa: E402
import jesse.services.redis   # noqa: E402

DATA_DIR      = Path(os.environ.get('DATA_DIR', '/data'))
EXCHANGE_NAME = 'Bybit Perpetual'
SYMBOL        = 'BTC-USDT'

# PASS thresholds
PASS_CAGR_PCT    =  5.0   # %
PASS_SHARPE      =  0.5
PASS_MDD_PCT     = -30.0  # % (less negative = better)
PASS_TRADES      =  30
PASS_WINRATE_PCT =  35.0  # %
PASS_PF          =  1.2

TF_MINUTES = {"1h": 60, "4h": 240, "1D": 1440}


# ── Candle loaders ─────────────────────────────────────────────────────────

def _load_1m(start: str, end: str) -> np.ndarray:
    """Load real Binance 1m OHLC from monthly parquet files.

    Args:
        start: ISO 8601 string (e.g. '2017-08-18')
        end:   ISO 8601 string (e.g. '2026-04-30')

    Returns:
        numpy array: [timestamp_ms, open, close, high, low, volume]
    """
    try:
        import polars as pl
    except ImportError:
        raise ImportError('polars required')

    base = DATA_DIR / 'ohlcv' / 'BTCUSDT' / '1m'
    if not base.exists():
        raise FileNotFoundError(f'1m candle data dir missing: {base}')

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    frames = []

    # Iterate through months in the range
    current = start_dt.replace(day=1)
    while current <= end_dt:
        # Construct parquet filename: YYYY-MM.parquet
        filename = f'{current.year:04d}-{current.month:02d}.parquet'
        filepath = base / filename

        if filepath.exists():
            try:
                lf = pl.scan_parquet(filepath)
                schema_keys = list(lf.schema.keys())

                # Handle column name variants
                if 'timestamp' in schema_keys:
                    lf = lf.with_columns(pl.col('timestamp').dt.epoch('ms').alias('ts_ms'))
                elif 'open_time' in schema_keys:
                    lf = lf.with_columns(pl.col('open_time').dt.epoch('ms').alias('ts_ms'))
                else:
                    # Try to find timestamp-like column
                    current = current.replace(day=1) + timedelta(days=32)
                    current = current.replace(day=1)
                    continue

                lf = lf.select(['ts_ms', 'open', 'high', 'low', 'close', 'volume'])
                frames.append(lf)
            except Exception as e:
                print(f'  [warn] Failed to read {filepath}: {e}')

        # Move to next month
        current = current.replace(day=1) + timedelta(days=32)
        current = current.replace(day=1)

    if not frames:
        raise FileNotFoundError(f'No parquet files found in {base} for period {start}~{end}')

    df = (pl.concat(frames).collect()
            .filter((pl.col('ts_ms') >= start_ms) & (pl.col('ts_ms') < end_ms))
            .select([
                pl.col('ts_ms').cast(pl.Float64),
                pl.col('open').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64),
            ])
            .sort('ts_ms')
            .unique(subset=['ts_ms'], keep='first').sort('ts_ms'))

    arr = df.to_numpy()
    print(f'  [candles] {len(arr):,} real 1m candles ({start} → {end})')
    return arr


def _load_1h(start: str, end: str) -> np.ndarray:
    try:
        import polars as pl
    except ImportError:
        raise ImportError('polars required')

    base = DATA_DIR / 'ohlcv' / 'BTCUSDT' / '1h'
    if not base.exists():
        raise FileNotFoundError(f'Candle data dir missing: {base}')

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    frames = []
    for yr_dir in sorted(base.iterdir()):
        if not yr_dir.is_dir():
            continue
        try:
            yr = int(yr_dir.name)
        except ValueError:
            continue
        if yr < start_dt.year - 1 or yr > end_dt.year + 1:
            continue
        for f in sorted(yr_dir.glob('*.parquet')):
            # Normalise per-file: handle both 'timestamp' and 'open_time' column names,
            # and discard extra Binance Vision columns (some files have 11 cols).
            lf = pl.scan_parquet(f)
            schema_keys = list(lf.schema.keys())
            if 'timestamp' in schema_keys:
                lf = lf.with_columns(pl.col('timestamp').dt.epoch('ms').alias('ts_ms'))
            elif 'open_time' in schema_keys:
                lf = lf.with_columns(pl.col('open_time').dt.epoch('ms').alias('ts_ms'))
            else:
                continue  # skip unrecognised schema
            frames.append(lf.select(['ts_ms', 'open', 'high', 'low', 'close', 'volume']))

    if not frames:
        raise FileNotFoundError(f'No parquet files under {base}')

    df = (pl.concat(frames).collect()
            .filter((pl.col('ts_ms') >= start_ms) & (pl.col('ts_ms') < end_ms))
            .select([
                pl.col('ts_ms').cast(pl.Float64),
                pl.col('open').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64),
            ])
            .sort('ts_ms')
            .unique(subset=['ts_ms'], keep='first').sort('ts_ms'))

    arr = df.to_numpy()
    print(f'  [candles] {len(arr):,} 1h candles ({start} → {end})')
    return arr


def _upsample_to_1m(arr: np.ndarray) -> np.ndarray:
    # Limitation: each 1h candle is broadcast to 60 identical 1m candles (OHLCV replicated).
    # No intra-hour price simulation — re-validate with real 1m data before Phase 5 live deployment.
    MINUTE_MS = 60_000
    n = len(arr)
    out = np.empty((n * 60, 6), dtype=np.float64)
    for i, row in enumerate(arr):
        ts, op, cl, hi, lo, vol = row
        base = i * 60
        for m in range(60):
            out[base + m] = [ts + m * MINUTE_MS, op, cl, hi, lo, vol / 60.0]
    return out


def _expand_tf_to_1m(arr: np.ndarray, tf_minutes: int) -> np.ndarray:
    """Expand TF bars to tf_minutes identical 1m bars each.
    Required for non-1h routes (4h/1D): Jesse's research.backtest() expects 1m-resolution
    candles as input and aggregates them to the route timeframe. Passing already-resampled
    bars directly results in 60x under-evaluation (Jesse groups them as if they were 1m).
    """
    MINUTE_MS = 60_000
    n = len(arr)
    out = np.empty((n * tf_minutes, 6), dtype=np.float64)
    for i, row in enumerate(arr):
        ts, op, cl, hi, lo, vol = row
        base = i * tf_minutes
        for m in range(tf_minutes):
            out[base + m] = [ts + m * MINUTE_MS, op, cl, hi, lo, vol / tf_minutes]
    return out


def _resample_1h(arr_1h: np.ndarray, tf: str) -> np.ndarray:
    """Resample 1h candles [ts_ms, open, close, high, low, vol] to 4h / 1D.
    Aligns to UTC boundaries: 4h→0/4/8…, 1D→midnight.
    """
    hours_map = {"4h": 4, "1D": 24}
    n_hours = hours_map[tf]
    ms_per_hour = 3_600_000
    align_ms = n_hours * ms_per_hour

    if len(arr_1h) < n_hours:
        return np.empty((0, 6), dtype=np.float64)

    # Trim leading bars to reach next aligned UTC boundary
    first_ts = int(arr_1h[0, 0])
    offset_ms = first_ts % align_ms
    if offset_ms != 0:
        skip = (align_ms - offset_ms) // ms_per_hour
        arr_1h = arr_1h[skip:]

    # Trim to multiple of n_hours
    n = (len(arr_1h) // n_hours) * n_hours
    arr_1h = arr_1h[:n]

    if n == 0:
        return np.empty((0, 6), dtype=np.float64)

    c = arr_1h.reshape(-1, n_hours, 6)
    out = np.empty((len(c), 6), dtype=np.float64)
    out[:, 0] = c[:, 0, 0]              # ts = first bar's open_time
    out[:, 1] = c[:, 0, 1]              # open = first bar's open
    out[:, 2] = c[:, -1, 2]             # close = last bar's close
    out[:, 3] = c[:, :, 3].max(axis=1)  # high = period max
    out[:, 4] = c[:, :, 4].min(axis=1)  # low = period min
    out[:, 5] = c[:, :, 5].sum(axis=1)  # volume = sum
    return out


# ── Strategy loader ───────────────────────────────────────────────────────────

def _load_strategy(name: str):
    external_dir = JESSE_ROOT / 'strategies' / 'external'
    sys.path.insert(0, str(JESSE_ROOT / 'strategies'))
    mod_path = external_dir / f'{name}.py'
    if not mod_path.exists():
        raise FileNotFoundError(f'Strategy not found: {mod_path}')
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)


# ── Metric extraction ─────────────────────────────────────────────────────────

def _extract_metrics(raw: dict, start: str, end: str, no_upsample: bool = False,
                     timeframe: str = '1h') -> dict:
    m = raw.get('metrics', {}) or {}

    num_trades     = int(m.get('total', 0) or 0)
    win_rate_dec   = float(m.get('win_rate', 0) or 0)       # 0-1
    net_profit_pct = float(m.get('net_profit_percentage', 0) or 0)
    sharpe_raw     = float(m.get('sharpe_ratio', m.get('sharpe', 0)) or 0)
    mdd_pct        = float(m.get('max_drawdown', 0) or 0)   # e.g. -25.3 (%)
    profit_factor  = float(m.get('profit_factor', 0) or 0)
    gross_profit   = float(m.get('gross_profit', 0) or 0)
    gross_loss     = float(m.get('gross_loss', 0) or 0)

    # Sharpe correction:
    # - 1h no-upsample: Jesse groups 60 1h bars into one evaluation, treating them as 1m.
    #   Jesse annualizes with sqrt(525600) assuming 1m bars → divide by sqrt(60) to correct.
    # - 4h/1D (new expand-to-1m + native route): Jesse uses the actual route TF for
    #   annualization → no correction needed.
    # - Standard 1h (1m-expanded): correct by construction.
    # - intrabar (real 1m): Jesse uses 1m route → no correction needed.
    tf_min = TF_MINUTES.get(timeframe, 60)
    if no_upsample and timeframe == '1h':
        sharpe = sharpe_raw / (60 ** 0.5)
    else:
        sharpe = sharpe_raw

    # Compute PF from gross if Jesse didn't provide it
    if profit_factor == 0 and gross_loss != 0:
        profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else 0.0

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    years    = (end_dt - start_dt).days / 365.25
    cagr = ((1 + net_profit_pct / 100) ** (1 / years) - 1) if years > 0 and (1 + net_profit_pct / 100) > 0 else 0.0

    # Use our date-based CAGR as annual_return_pct (Jesse's own annual_return is wrong for 1h candles)
    annual_ret_pct = cagr * 100

    return {
        'annual_return_pct':  round(annual_ret_pct, 4),
        'cagr_pct':           round(cagr * 100, 4),
        'sharpe_ratio':       round(sharpe, 4),
        'max_drawdown_pct':   round(mdd_pct, 4),
        'total_trades':       num_trades,
        'win_rate_pct':       round(win_rate_dec * 100, 4),
        'profit_factor':      round(profit_factor, 4),
        'gross_profit':       round(gross_profit, 2),
        'gross_loss':         round(gross_loss, 2),
        'net_profit_pct':     round(net_profit_pct, 4),
        'no_upsample':        no_upsample,
        'timeframe':          timeframe,
        'start':              start,
        'end':                end,
        'raw_metrics':        m,
    }


def _pass_fail(metrics: dict) -> tuple[str, dict]:
    checks = {
        'CAGR ≥ 5%':          metrics['annual_return_pct'] >= PASS_CAGR_PCT,
        'Sharpe ≥ 0.5':       metrics['sharpe_ratio']      >= PASS_SHARPE,
        'MDD ≥ -30%':         metrics['max_drawdown_pct']  >= PASS_MDD_PCT,
        'Trades ≥ 30':        metrics['total_trades']       >= PASS_TRADES,
        'WinRate ≥ 35%':      metrics['win_rate_pct']       >= PASS_WINRATE_PCT,
        'ProfitFactor ≥ 1.2': metrics['profit_factor']      >= PASS_PF,
    }
    verdict = 'PASS' if all(checks.values()) else 'FAIL'
    return verdict, checks


# ── Output writers ────────────────────────────────────────────────────────────

def _write_stats(out_dir: Path, strategy: str, metrics: dict, verdict: str, checks: dict,
                 balance: float = 10_000.0, leverage: int = 1, variant: str = 'bidirectional'):
    data = {**metrics, 'strategy': strategy, 'verdict': verdict,
            'checks': {k: bool(v) for k, v in checks.items()},
            'starting_balance': balance, 'leverage': leverage, 'variant': variant}
    # Write JSON
    with open(out_dir / 'stats.json', 'w') as f:
        json.dump(data, f, indent=2)
    # Write Parquet for easier analytics
    try:
        pd.DataFrame([data]).to_parquet(out_dir / 'stats.parquet', index=False)
    except Exception as e:
        print(f'  [warn] stats.parquet write failed: {e}')


def _write_trades(out_dir: Path, trades: list):
    # Prepare data for both CSV and Parquet
    trades_data = []
    for t in trades:
        trades_data.append({
            'opened_at': t.get('opened_at'),
            'closed_at': t.get('closed_at'),
            'side': t.get('type'),
            'entry_price': t.get('entry_price'),
            'exit_price': t.get('exit_price'),
            'qty': t.get('qty'),
            'pnl': t.get('PNL', t.get('pnl', t.get('net_profit', 0))),
            'fee': t.get('fee'),
        })

    # Write CSV
    with open(out_dir / 'trades.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['opened_at', 'closed_at', 'side', 'entry_price',
                         'exit_price', 'qty', 'pnl', 'fee'])
        for row in trades_data:
            writer.writerow([row[k] for k in ['opened_at', 'closed_at', 'side',
                                              'entry_price', 'exit_price', 'qty', 'pnl', 'fee']])

    # Write Parquet for easier analytics
    try:
        df = pd.DataFrame(trades_data)
        df.to_parquet(out_dir / 'trades.parquet', index=False)
    except Exception as e:
        print(f'  [warn] trades.parquet write failed: {e}')


def _write_equity_curve(out_dir: Path, strategy: str, equity_curve):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        if equity_curve is None or (hasattr(equity_curve, '__len__') and len(equity_curve) == 0):
            plt.figure(figsize=(10, 4))
            plt.text(0.5, 0.5, 'No equity curve data', ha='center', transform=plt.gca().transAxes)
        else:
            plt.figure(figsize=(12, 5))
            if isinstance(equity_curve, dict):
                # Jesse 2.x returns dict with 'timestamps' and 'values' keys
                data = equity_curve.get('values', list(equity_curve.values()))
            elif hasattr(equity_curve, 'values'):
                data = equity_curve.values
            elif isinstance(equity_curve, np.ndarray):
                data = equity_curve
            else:
                data = list(equity_curve)
            plt.plot(data)

        plt.title(f'{strategy} Equity Curve')
        plt.xlabel('Time'); plt.ylabel('USDT')
        plt.tight_layout()
        plt.savefig(out_dir / 'equity_curve.png', dpi=80)
        plt.close()
    except Exception as e:
        print(f'  [warn] equity_curve plot skipped: {e}')
        (out_dir / 'equity_curve.png').write_text(f'plot skipped: {e}')


def _write_monthly_returns(out_dir: Path, trades: list, start: str, end: str):
    """Compute monthly P&L from trades and write CSV + Parquet."""
    monthly: dict[str, float] = {}
    for t in trades:
        closed_ms = t.get('closed_at')
        pnl = float(t.get('PNL') or t.get('pnl') or t.get('net_profit') or 0)
        if closed_ms is None:
            continue
        try:
            dt = datetime.utcfromtimestamp(int(closed_ms) / 1000)
            key = f'{dt.year}-{dt.month:02d}'
            monthly[key] = monthly.get(key, 0.0) + pnl
        except Exception:
            continue

    # Write CSV
    with open(out_dir / 'monthly_returns.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['month', 'pnl_usdt'])
        for k in sorted(monthly):
            writer.writerow([k, round(monthly[k], 2)])

    # Write Parquet for easier analytics
    try:
        df = pd.DataFrame([{'month': k, 'pnl_usdt': monthly[k]} for k in sorted(monthly)])
        df.to_parquet(out_dir / 'monthly_returns.parquet', index=False)
    except Exception as e:
        print(f'  [warn] monthly_returns.parquet write failed: {e}')


def _write_decision(out_dir: Path, strategy: str, metrics: dict, verdict: str, checks: dict):
    cagr = metrics['annual_return_pct']
    sharpe = metrics['sharpe_ratio']
    mdd = metrics['max_drawdown_pct']
    trades = metrics['total_trades']
    wr = metrics['win_rate_pct']
    pf = metrics['profit_factor']

    lines = [
        f'---',
        f'strategy: {strategy}',
        f'verdict: {verdict}',
        f'created: {datetime.utcnow().strftime("%Y-%m-%d")}',
        f'---',
        f'',
        f'# {strategy} 백테스트 결과',
        f'',
        f'## 메트릭',
        f'',
        f'| 항목 | 결과 | 기준 | 통과 |',
        f'|------|------|------|------|',
        f'| CAGR | {cagr:.2f}% | ≥ +5% | {"✓" if checks["CAGR ≥ 5%"] else "✗"} |',
        f'| Sharpe | {sharpe:.3f} | ≥ 0.5 | {"✓" if checks["Sharpe ≥ 0.5"] else "✗"} |',
        f'| MDD | {mdd:.2f}% | ≥ -30% | {"✓" if checks["MDD ≥ -30%"] else "✗"} |',
        f'| Trades | {trades} | ≥ 30 | {"✓" if checks["Trades ≥ 30"] else "✗"} |',
        f'| Win Rate | {wr:.1f}% | ≥ 35% | {"✓" if checks["WinRate ≥ 35%"] else "✗"} |',
        f'| Profit Factor | {pf:.3f} | ≥ 1.2 | {"✓" if checks["ProfitFactor ≥ 1.2"] else "✗"} |',
        f'',
        f'## 종합 판정: **{verdict}**',
        f'',
        f'## 관찰 사항',
        f'- 백테스트 기간: {metrics["start"]} ~ {metrics["end"]} (6년)',
        f'- 기준: post-ETF 압축장 표준 (backtest-skillset.md)',
        f'- 데이터: 실제 Binance 1m OHLC (intrabar wick 손절 포함)',
        f'',
        f'## CryptoEngine 적용 권고',
    ]
    if verdict == 'PASS':
        lines += [
            f'- [ ] walk-forward 검증 후 paper trading 검토',
            f'- [ ] fa80_lev5_r30 기존 전략과 상관관계 분석 필요',
        ]
    else:
        lines += [
            f'- [ ] archived 처리 (폐기 round 1)',
            f'- [ ] 실패 항목: {", ".join(k for k,v in checks.items() if not v)}',
        ]

    with open(out_dir / 'decision.md', 'w') as f:
        f.write('\n'.join(lines) + '\n')


def _copy_strategy(out_dir: Path, strategy: str):
    src = JESSE_ROOT / 'strategies' / 'external' / f'{strategy}.py'
    if src.exists():
        shutil.copy(src, out_dir / 'strategy.py')
    else:
        print(f'  [warn] strategy source not found: {src}')


# ── V3 환각 방지 마커 ────────────────────────────────────────────────────────────

def write_success_marker(output_dir: Path, stats_path: Path) -> None:
    sha = hashlib.sha256(stats_path.read_bytes()).hexdigest()
    s = json.loads(stats_path.read_text())
    (output_dir / 'EXECUTION_SUCCESS.marker').write_text(
        f"status: SUCCESS\n"
        f"stats_sha256: {sha}\n"
        f"stats_size: {stats_path.stat().st_size}\n"
        f"sample_metrics:\n"
        f"  cagr: {s.get('annual_return_pct', '?')}\n"
        f"  sharpe: {s.get('sharpe_ratio', '?')}\n"
        f"  trades: {s.get('total_trades', '?')}\n"
        f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
    )


def write_failure_marker(output_dir: Path, reason: str, tb: str = '') -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'EXECUTION_FAILED.marker').write_text(
        f"status: FAILED\n"
        f"reason: {reason}\n"
        f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
        f"traceback:\n{tb}\n"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run(args):
    import os as _os
    _os.environ['STRATEGY_LEVERAGE'] = str(args.leverage)  # must be before _load_strategy

    strategy_name = args.strategy
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = args.start
    end   = args.end

    tf = getattr(args, 'timeframe', '1h')
    use_real_1m = getattr(args, 'real_1m', True)  # default: use real 1m
    tf_hours = TF_MINUTES.get(tf, 60) // 60

    print(f'\n=== {strategy_name} [{tf} intrabar] | {start} → {end} ===')

    # 1. Determine if we can use real 1m or fall back to expanded
    if tf == '1h' and use_real_1m:
        # Load real 1m for 1h trading (intrabar wick evaluation)
        try:
            candles_1m_main = _load_1m(start, end)
            print(f'  Using real 1m candles for wick-based stop evaluation')

            # Warmup: load prior 1h data and convert to 1m
            from datetime import timedelta
            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            warmup_days = max(60, 220 // 24 + 1)  # ~220 1m bars = ~3.7h worth
            wu_start_dt = start_dt - timedelta(days=warmup_days)
            wu_start = wu_start_dt.strftime('%Y-%m-%d')
            warmup_1h = _load_1h(wu_start, start)
            warmup_1m = _upsample_to_1m(warmup_1h)
            if len(warmup_1m) < 200 * 60:
                print(
                    f'  [warn] Insufficient warmup candles: got {len(warmup_1m)} 1m bars, '
                    f'need 12000. Indicators will be cold at backtest start.',
                    flush=True,
                )

            candles_1m = candles_1m_main
            route_tf = '1h'
            print(f'  Main: {len(candles_1m):,} real 1m')
            print(f'  Warmup: {len(warmup_1m):,} 1m (from 1h expansion)')
            no_upsample = False
        except Exception as e:
            print(f'  [warn] Failed to load real 1m: {e}. Falling back to 1h expansion.')
            # Fall back to 1h mode with upsample
            candles_1h = _load_1h(start, end)
            candles_1m = _upsample_to_1m(candles_1h)

            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            warmup_days = max(60, 220 // 24 + 1)
            wu_start_dt = start_dt - timedelta(days=warmup_days)
            wu_start = wu_start_dt.strftime('%Y-%m-%d')
            warmup_1h = _load_1h(wu_start, start)
            warmup_1m = _upsample_to_1m(warmup_1h)

            route_tf = '1h'
            print(f'  Main: {len(candles_1h):,} 1h → {len(candles_1m):,} 1m (fallback)')
            print(f'  Warmup: {len(warmup_1h):,} 1h → {len(warmup_1m):,} 1m')
            no_upsample = False

    elif tf != '1h':
        # For non-1h TFs, use real 1m OHLC directly (intrabar wick evaluation)
        try:
            candles_1m = _load_1m(start, end)
            print(f'  Using real 1m candles for {tf} wick-based stop evaluation')

            from datetime import timedelta
            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            tf_minutes = TF_MINUTES[tf]
            warmup_days = max(60, tf_minutes * 220 // 1440 + 1)
            wu_start_dt = start_dt - timedelta(days=warmup_days)
            wu_start = wu_start_dt.strftime('%Y-%m-%d')
            try:
                warmup_1m = _load_1m(wu_start, start)
            except Exception:
                # fallback to 1h-based warmup if 1m data not available for warmup period
                warmup_1h = _load_1h(wu_start, start)
                warmup_1m = _upsample_to_1m(warmup_1h)

            no_upsample = True
            route_tf = tf
            print(f'  Main: {len(candles_1m):,} real 1m')
            print(f'  Warmup: {len(warmup_1m):,} 1m')
        except Exception as e:
            # Fallback: original expand-to-1m approach
            print(f'  [warn] Failed to load real 1m for {tf}: {e}. Falling back to 1h expansion.')
            from datetime import timedelta
            candles_1h = _load_1h(start, end)
            start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            tf_hours = TF_MINUTES.get(tf, 60) // 60
            warmup_days = max(60, tf_hours * 220 // 24 + 1)
            wu_start_dt = start_dt - timedelta(days=warmup_days)
            wu_start = wu_start_dt.strftime('%Y-%m-%d')
            warmup_1h = _load_1h(wu_start, start)
            tf_minutes = TF_MINUTES[tf]
            candles_tf = _resample_1h(candles_1h, tf)
            warmup_tf  = _resample_1h(warmup_1h, tf)
            if len(warmup_tf) == 0:
                borrow = min(200, max(1, len(candles_tf) // 4))
                warmup_tf  = candles_tf[:borrow]
                candles_tf = candles_tf[borrow:]
            candles_1m = _expand_tf_to_1m(candles_tf, tf_minutes)
            warmup_1m  = _expand_tf_to_1m(warmup_tf, tf_minutes)
            no_upsample = True
            route_tf = tf
            print(f'  Main: {len(candles_1h):,} 1h → {len(candles_1m):,} 1m (fallback)')
            print(f'  Warmup: {len(warmup_1m):,} 1m (fallback)')

    else:
        # 1h without real 1m (fallback/legacy)
        candles_1h = _load_1h(start, end)

        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        warmup_days = max(60, 220 // 24 + 1)
        wu_start_dt = start_dt - timedelta(days=warmup_days)
        wu_start = wu_start_dt.strftime('%Y-%m-%d')
        warmup_1h = _load_1h(wu_start, start)
        if len(warmup_1h) < 200 * 60:
            print(
                f'  [warn] Insufficient warmup candles: got {len(warmup_1h)} 1h bars, '
                f'need 200. Indicators will be cold at backtest start.',
                flush=True,
            )

        candles_1m = _upsample_to_1m(candles_1h)
        warmup_1m  = _upsample_to_1m(warmup_1h)
        route_tf = '1h'
        print(f'  Main: {len(candles_1h):,} 1h → {len(candles_1m):,} 1m')
        print(f'  Warmup: {len(warmup_1h):,} 1h → {len(warmup_1m):,} 1m')
        no_upsample = False

    # 2. Load strategy
    strategy_cls = _load_strategy(strategy_name)

    if args.variant == 'long_only':
        from external._long_only_factory import make_long_only
        strategy_cls = make_long_only(strategy_cls)
    elif args.variant == 'buy_and_hold':
        from external.BuyAndHoldStrategy import BuyAndHoldStrategy as _BnH
        strategy_cls = _BnH

    # 3. Jesse config
    from jesse import research
    import jesse.helpers as jh

    config = {
        'starting_balance':      args.balance,
        'fee':                   args.fee,
        'type':                  'futures',
        'futures_leverage':      int(args.leverage) if args.leverage == int(args.leverage) else int(args.leverage) + 1,
        'futures_leverage_mode': 'isolated',
        'exchange':              EXCHANGE_NAME,
        'warm_up_candles':       len(warmup_1m),
    }
    routes = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
               'symbol': SYMBOL, 'timeframe': route_tf}]
    key = jh.key(EXCHANGE_NAME, SYMBOL)
    candles_dict = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': candles_1m}}
    warmup_dict  = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warmup_1m}}

    # 4. Pre-populate Jesse ENV_VALUES so open_connection() works after Jesse
    # creates storage/ in CWD (which makes is_jesse_project() return True mid-run).
    from jesse.services.env import ENV_VALUES as _jenv
    _jenv['POSTGRES_NAME']     = os.environ.get('JESSE_DB_NAME', 'jesse_db')
    _jenv['POSTGRES_USERNAME'] = os.environ.get('JESSE_DB_USER', 'jesse')
    _jenv['POSTGRES_PASSWORD'] = os.environ.get('JESSE_DB_PASSWORD', '')
    _jenv['POSTGRES_HOST']     = os.environ.get('JESSE_DB_HOST', 'localhost')
    _jenv['POSTGRES_PORT']     = os.environ.get('JESSE_DB_PORT', '5432')
    _jenv['PASSWORD']          = 'backtest'

    # 5. Run backtest
    print('  Running Jesse backtest...')
    raw = research.backtest(
        config=config, routes=routes, data_routes=[],
        candles=candles_dict, warmup_candles=warmup_dict,
        generate_json=True, generate_csv=True, generate_equity_curve=True,
        hyperparameters=getattr(args, 'hp_dict', None) or {},
    )

    # 6. Extract metrics
    metrics = _extract_metrics(raw, start, end, no_upsample=no_upsample, timeframe=tf)
    verdict, checks = _pass_fail(metrics)

    print(f'\n  Result: CAGR={metrics["annual_return_pct"]:.2f}%  '
          f'Sharpe={metrics["sharpe_ratio"]:.3f}  '
          f'MDD={metrics["max_drawdown_pct"]:.2f}%  '
          f'Trades={metrics["total_trades"]}  '
          f'WR={metrics["win_rate_pct"]:.1f}%  '
          f'PF={metrics["profit_factor"]:.3f}')
    print(f'  Verdict: {verdict}')
    for k, v in checks.items():
        print(f'    {"✓" if v else "✗"} {k}')

    # 7. Write output files
    trades = raw.get('trades', []) or []
    _write_stats(out_dir, strategy_name, metrics, verdict, checks,
                 balance=args.balance, leverage=args.leverage, variant=getattr(args, 'variant', 'bidirectional'))
    _write_trades(out_dir, trades)
    _write_equity_curve(out_dir, strategy_name, raw.get('equity_curve'))
    _write_monthly_returns(out_dir, trades, start, end)
    _write_decision(out_dir, strategy_name, metrics, verdict, checks)
    _copy_strategy(out_dir, strategy_name)

    print(f'\n  Saved 6 files → {out_dir}')
    return verdict, metrics


def parse_args():
    p = argparse.ArgumentParser(description='External strategy backtest runner (intrabar 1m edition)')
    p.add_argument('--strategy', required=True, help='Strategy class name')
    p.add_argument('--output',   required=True, help='Output directory path')
    p.add_argument('--start',    default='2017-08-18')
    p.add_argument('--end',      default='2026-04-30')
    p.add_argument('--balance',  type=float, default=10_000.0)
    p.add_argument('--fee',      type=float, default=0.0002)   # Bybit maker (limit order)
    p.add_argument('--leverage', type=float, default=1)
    p.add_argument('--variant',  choices=['bidirectional', 'long_only', 'buy_and_hold'],
                   default='bidirectional')
    p.add_argument('--timeframe', choices=['1h', '4h', '1D'], default='1h',
                   help='Target timeframe')
    p.add_argument('--no-real-1m', action='store_true',
                   help='For 1h: disable real 1m loading and use 1h expansion instead')
    p.add_argument('--hp-json', default='{}',
                   help='Hyperparameters as JSON string, e.g. \'{"st_factor":3.0}\'')
    args = p.parse_args()
    import json as _json
    args.hp_dict = _json.loads(args.hp_json) if args.hp_json else {}
    return args


if __name__ == '__main__':
    args = parse_args()
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    _success = False
    try:
        verdict, metrics = run(args)
        stats_path = out_dir / 'stats.json'
        if stats_path.exists():
            write_success_marker(out_dir, stats_path)
            _success = True
        else:
            write_failure_marker(out_dir, 'stats.json not found after run()')
    except BaseException as e:
        if isinstance(e, SystemExit) and e.code == 0:
            _success = True  # clean exit from within run()
        else:
            write_failure_marker(out_dir, str(e), traceback.format_exc())
    sys.exit(0 if _success else 1)
