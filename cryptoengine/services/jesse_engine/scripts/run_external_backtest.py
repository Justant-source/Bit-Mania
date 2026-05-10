#!/usr/bin/env python3
"""
External strategy backtest runner for 9-strategy cross-batch evaluation.

Usage (inside jesse container):
    python scripts/run_external_backtest.py \\
        --strategy BBPBStrategy \\
        --output /result/batch_1/bbpb/

Output (6 files per strategy):
    stats.json, trades.csv, equity_curve.png, monthly_returns.csv,
    decision.md, strategy.py

PASS criteria (from backtest-skillset.md, post-ETF compression standard):
    CAGR >= 5%, Sharpe >= 0.5, MDD >= -30%, Trades >= 30,
    WinRate >= 35%, ProfitFactor >= 1.2
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# --- Path setup -----------------------------------------------------------
SCRIPTS_DIR = Path(__file__).parent
JESSE_ROOT  = SCRIPTS_DIR.parent
sys.path.insert(0, str(JESSE_ROOT))

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

TF_MINUTES = {"1h": 60, "2h": 120, "4h": 240, "1D": 1440}


# ── Candle loader (reuse logic from run_backtest.py) ──────────────────────────

def _load_1h(start: str, end: str) -> np.ndarray:
    try:
        import polars as pl
    except ImportError:
        raise ImportError('polars required')

    base = DATA_DIR / 'binance_vision' / 'klines' / 'BTCUSDT' / '1h'
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
            frames.append(pl.scan_parquet(f))

    if not frames:
        raise FileNotFoundError(f'No parquet files under {base}')

    ts_col = 'open_time'
    df = (pl.concat(frames).collect()
            .with_columns(pl.col(ts_col).dt.epoch('ms').alias('ts_ms'))
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
    Required for non-1h routes (2h/4h/1D): Jesse's research.backtest() expects 1m-resolution
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
    """Resample 1h candles [ts_ms, open, close, high, low, vol] to 2h / 4h / 1D.
    Aligns to UTC boundaries: 2h→even-hours, 4h→0/4/8…, 1D→midnight.
    """
    hours_map = {"2h": 2, "4h": 4, "1D": 24}
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
    # - 2h/4h/1D (new expand-to-1m + native route): Jesse uses the actual route TF for
    #   annualization → no correction needed.
    # - Standard 1h (1m-expanded): correct by construction.
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
    with open(out_dir / 'stats.json', 'w') as f:
        json.dump(data, f, indent=2)


def _write_trades(out_dir: Path, trades: list):
    with open(out_dir / 'trades.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['opened_at', 'closed_at', 'side', 'entry_price',
                         'exit_price', 'qty', 'pnl', 'fee'])
        for t in trades:
            writer.writerow([
                t.get('opened_at'), t.get('closed_at'), t.get('type'),
                t.get('entry_price'), t.get('exit_price'),
                t.get('qty'),
                t.get('PNL', t.get('pnl', t.get('net_profit', 0))),
                t.get('fee'),
            ])


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
    """Compute monthly P&L from trades and write CSV."""
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

    with open(out_dir / 'monthly_returns.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['month', 'pnl_usdt'])
        for k in sorted(monthly):
            writer.writerow([k, round(monthly[k], 2)])


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
    strategy_name = args.strategy
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = args.start
    end   = args.end

    tf = getattr(args, 'timeframe', '1h')
    no_upsample = getattr(args, 'no_upsample', False)
    tf_hours = TF_MINUTES.get(tf, 60) // 60

    print(f'\n=== {strategy_name} [{tf}] | {start} → {end} ===')

    # 1. Load 1h candles then resample to target TF
    candles_1h = _load_1h(start, end)

    from datetime import timedelta
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    # Warmup: need 220 bars in target TF units (enough for SMA200 + margin)
    warmup_days = max(60, tf_hours * 220 // 24 + 1)
    wu_start_dt = start_dt - timedelta(days=warmup_days)
    # Warmup must come from pre-period data on disk (2019 parquet backfilled).
    # Never slice warmup from the main window — that silently pushes the trading
    # start date forward and invalidates BnH and regime-sensitive comparisons.
    wu_start = wu_start_dt.strftime('%Y-%m-%d')
    warmup_1h = _load_1h(wu_start, start)
    if len(warmup_1h) < tf_hours * 200:
        raise RuntimeError(
            f'Insufficient warmup candles for {tf}: got {len(warmup_1h)} 1h bars, '
            f'need {tf_hours * 200}. Backfill {wu_start} → {start} first.'
        )

    if tf != '1h':
        # Resample 1h → target TF, then expand to 1m for Jesse's base format.
        # Jesse's research.backtest() always expects 1m-resolution input and aggregates to
        # the route timeframe. Passing pre-resampled TF bars directly causes 60x under-evaluation
        # (Jesse treats each bar as 1m and groups 60 into one TF bar). Fix: expand each TF bar
        # into tf_minutes identical 1m bars so Jesse aggregates them back to one TF bar.
        tf_minutes = TF_MINUTES[tf]
        candles_tf = _resample_1h(candles_1h, tf)
        warmup_tf  = _resample_1h(warmup_1h, tf)
        candles_1m = _expand_tf_to_1m(candles_tf, tf_minutes)
        warmup_1m  = _expand_tf_to_1m(warmup_tf, tf_minutes)
        no_upsample = True
        route_tf = tf
        print(f'  Main: {len(candles_1h):,} 1h → {len(candles_tf):,} {tf} → {len(candles_1m):,} 1m')
        print(f'  Warmup: {len(warmup_tf):,} {tf} → {len(warmup_1m):,} 1m')
    elif no_upsample:
        candles_1m = candles_1h
        warmup_1m  = warmup_1h
        route_tf = '1h'
        print(f'  Main: {len(candles_1h):,} 1h (no-upsample, V3 mode)')
        print(f'  Warmup: {len(warmup_1h):,} 1h (no-upsample)')
    else:
        candles_1m = _upsample_to_1m(candles_1h)
        warmup_1m  = _upsample_to_1m(warmup_1h)
        route_tf = '1h'
        print(f'  Main: {len(candles_1h):,} 1h → {len(candles_1m):,} 1m')
        print(f'  Warmup: {len(warmup_1h):,} 1h → {len(warmup_1m):,} 1m')

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
        'futures_leverage':      args.leverage,
        'futures_leverage_mode': 'isolated',  # Changed from 'cross' to allow proper margin calculation per position
        'exchange':              EXCHANGE_NAME,
        'warm_up_candles':       len(warmup_1m),
    }
    # For non-1h TFs: use the actual timeframe + 1m-expanded candles so Jesse aggregates correctly.
    # For 1h: use '1h' route with 1h candles (no-upsample) or 1m candles (upsample mode).
    routes = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
               'symbol': SYMBOL, 'timeframe': route_tf}]
    key = jh.key(EXCHANGE_NAME, SYMBOL)
    candles_dict = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': candles_1m}}
    warmup_dict  = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warmup_1m}}

    # 4. Run backtest
    print('  Running Jesse backtest...')
    raw = research.backtest(
        config=config, routes=routes, data_routes=[],
        candles=candles_dict, warmup_candles=warmup_dict,
        generate_json=True, generate_csv=True, generate_equity_curve=True,
    )

    # 5. Extract metrics
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

    # 6. Write output files
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
    p = argparse.ArgumentParser(description='External strategy backtest runner')
    p.add_argument('--strategy', required=True, help='Strategy class name')
    p.add_argument('--output',   required=True, help='Output directory path')
    p.add_argument('--start',    default='2020-01-01')
    p.add_argument('--end',      default='2025-12-31')
    p.add_argument('--balance',  type=float, default=10_000.0)
    p.add_argument('--fee',      type=float, default=0.00055)
    p.add_argument('--leverage', type=int,   default=1)
    p.add_argument('--variant',  choices=['bidirectional', 'long_only', 'buy_and_hold'],
                   default='bidirectional')
    p.add_argument('--no-upsample', action='store_true',
                   help='V3: use 1h candles directly, skip 60x upsample (faster)')
    p.add_argument('--timeframe', choices=['1h', '2h', '4h', '1D'], default='1h',
                   help='V4: target timeframe (non-1h implies --no-upsample)')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        verdict, metrics = run(args)
        stats_path = out_dir / 'stats.json'
        if stats_path.exists():
            write_success_marker(out_dir, stats_path)
        else:
            write_failure_marker(out_dir, 'stats.json not found after run()')
            sys.exit(1)
        sys.exit(0)
    except Exception as e:
        write_failure_marker(out_dir, str(e), traceback.format_exc())
        sys.exit(1)
