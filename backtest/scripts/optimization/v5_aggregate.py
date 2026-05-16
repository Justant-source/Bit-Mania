#!/usr/bin/env python3
"""
v5_aggregate.py — 324 combo × 8 독립 윈도우 집계 + Plateau + v4∩v5 Cross-Validation + v6 입력 추출

v4_aggregate.py를 기반으로 아래를 추가:
  1. Baseline: combo_18이 v5 grid 밖 → v4_all_combos.csv에서 참조값 주입 (144.99/149.17)
  2. Plateau analysis: 6 파라미터 각 ±1 step 이웃의 성과 일관성 → PLATEAU/ISLAND/MIXED/EDGE
  3. v4∩v5 Cross-Validation: 12개 공통 combo의 v4 결과와 drift 비교
  4. Sweet spot score: (safety×0.4 + plateau×0.4 + cagr×0.2) → v6 입력 top15

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v5_aggregate.py \
        --db /result/v5_optimization/queue.sqlite3 \
        --output-dir /result/v5_optimization
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev

WINDOWS_8 = [
    ('W1', '2017-08-18', '2018-09-19'),
    ('W2', '2018-09-19', '2019-10-22'),
    ('W3', '2019-10-22', '2020-11-23'),
    ('W4', '2020-11-23', '2021-12-26'),
    ('W5', '2021-12-26', '2023-01-28'),
    ('W6', '2023-01-28', '2024-03-01'),
    ('W7', '2024-03-01', '2025-04-03'),
    ('W8', '2025-04-03', '2026-04-30'),
]

FUNDING_PATH_PRIMARY   = '/data/funding/BTCUSDT_8h.parquet'
FUNDING_PATH_SECONDARY = '/data/funding/binance_api/BTCUSDT_2019.parquet'
FUNDING_REAL_START_MS  = 1568073600000  # 2019-09-10 08:00 UTC

LEVERAGE = 3.0

# v5 grid 정의 (plateau neighbor lookup에 사용)
GRID = {
    'st_factor':          [2.3, 2.5, 2.7],
    'st_period':          [6, 7, 8],
    'fast_ema_len':       [9, 10, 11],
    'slow_ema_len':       [25, 30],
    'direction_ema_len':  [230, 250],
    'atr_mult':           [3.0, 3.2, 3.5],
}
PARAMS = list(GRID.keys())
INT_PARAMS = {'st_period', 'fast_ema_len', 'slow_ema_len', 'direction_ema_len'}

# v4 결과 경로 (컨테이너 내 world-readable)
V4_CSV = Path('/result/v4_optimization/v4_all_combos.csv')

# combo_18 HP (v4 baseline 식별용)
COMBO_18_HP = {
    'st_factor': 2.5, 'st_period': 6, 'fast_ema_len': 7,
    'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0,
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _duration_years(start_str: str, end_str: str) -> float:
    s = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
    return (e - s).total_seconds() / (86400 * 365.25)


def _annualize(simple_ret: float, years: float) -> float:
    if years <= 0:
        return simple_ret * 100
    if 1 + simple_ret <= 0:
        return -100.0
    return ((1 + simple_ret) ** (1 / years) - 1) * 100


def _param_key(row: dict) -> tuple:
    parts = []
    for p in PARAMS:
        v = row[p]
        if p in INT_PARAMS:
            parts.append(int(float(v)))
        else:
            parts.append(round(float(v), 2))
    return tuple(parts)


def _grid_level_key(param: str, val) -> int | float:
    if param in INT_PARAMS:
        return int(float(val))
    return round(float(val), 2)


# ---------------------------------------------------------------------------
# Funding
# ---------------------------------------------------------------------------

def load_funding_data() -> dict[int, float]:
    data: dict[int, float] = {}
    try:
        import polars as pl
        dfs = []
        for path in (FUNDING_PATH_PRIMARY, FUNDING_PATH_SECONDARY):
            p = Path(path)
            if not p.exists():
                continue
            df = pl.read_parquet(p)
            rename = {}
            for col in df.columns:
                low = col.lower()
                if low in ('timestamp', 'timestamp_ms', 'time', 'ts'):
                    rename[col] = 'ts'
                elif low in ('rate', 'funding_rate', 'funding', 'value'):
                    rename[col] = 'rate'
            if rename:
                df = df.rename(rename)
            if 'ts' in df.columns and 'rate' in df.columns:
                dfs.append(df.select(['ts', 'rate']))
        if dfs:
            combined = pl.concat(dfs).unique(subset=['ts']).sort('ts')
            for row in combined.iter_rows(named=True):
                ts, rate = int(row['ts']), float(row['rate'])
                if rate != 0.0:
                    data[ts] = rate
    except Exception as e:
        print(f"  [warn] funding load failed: {e}")
    print(f"  [funding] {len(data):,} nonzero settlement records loaded")
    return data


def funding_for_trade(open_ms: int, close_ms: int, notional: float,
                      funding_data: dict[int, float]) -> float:
    if open_ms >= close_ms or notional <= 0:
        return 0.0
    if close_ms <= FUNDING_REAL_START_MS:
        return 0.0

    EIGHT_HOURS_MS = 8 * 3600 * 1000
    SETTLEMENT_HOURS = {0, 8, 16}
    total = 0.0

    open_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
    day_start = open_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_ms = int(day_start.timestamp() * 1000)

    ts = day_start_ms
    while ts < close_ms:
        for h in SETTLEMENT_HOURS:
            settlement_ms = ts + h * 3600 * 1000
            if open_ms <= settlement_ms < close_ms:
                rate = funding_data.get(settlement_ms)
                if rate is None:
                    for lag in range(1, 4):
                        rate = funding_data.get(settlement_ms - lag * EIGHT_HOURS_MS)
                        if rate is not None:
                            break
                if rate is not None:
                    total += notional * rate
        ts += 24 * 3600 * 1000

    return total


# ---------------------------------------------------------------------------
# Trades loader
# ---------------------------------------------------------------------------

def load_trades(trades_path: Path) -> list[dict]:
    trades = []
    if not trades_path.exists():
        return trades
    try:
        with open(trades_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    opened_at   = int(float(row.get('opened_at', 0)))
                    closed_at   = int(float(row.get('closed_at', 0)))
                    qty         = float(row.get('qty', 0) or 0)
                    entry_price = float(row.get('entry_price', 0) or 0)
                    exit_price  = float(row.get('exit_price', 0) or 0)
                    pnl         = float(row.get('pnl', 0) or 0)
                    if closed_at > opened_at and qty > 0:
                        trades.append({
                            'opened_at':   opened_at,
                            'closed_at':   closed_at,
                            'qty':         qty,
                            'entry_price': entry_price,
                            'exit_price':  exit_price,
                            'pnl':         pnl,
                        })
                except (ValueError, KeyError):
                    continue
    except Exception:
        pass
    return sorted(trades, key=lambda t: t['closed_at'])


# ---------------------------------------------------------------------------
# Per-window analysis
# ---------------------------------------------------------------------------

def analyze_window(combo_id: int, w_name: str, w_start: str, w_end: str,
                   output_dir: Path, funding_data: dict) -> dict:
    result_dir = output_dir / f"combo_{combo_id}_{w_name}"
    stats_path  = result_dir / 'stats.json'
    trades_path = result_dir / 'trades.csv'
    duration_years = _duration_years(w_start, w_end)

    if not stats_path.exists():
        return {
            'window': w_name, 'complete': False,
            'cagr_raw': None, 'mdd_raw': None, 'cagr_adj': None, 'mdd_adj': None,
            'sharpe': None, 'trades_count': 0, 'liquidated': False,
            'finishing_balance': None,
        }

    try:
        with open(stats_path) as f:
            stats = json.load(f)
    except Exception:
        return {
            'window': w_name, 'complete': False,
            'cagr_raw': None, 'mdd_raw': None, 'cagr_adj': None, 'mdd_adj': None,
            'sharpe': None, 'trades_count': 0, 'liquidated': False,
            'finishing_balance': None,
        }

    cagr_raw = stats.get('cagr_pct')
    mdd_raw  = stats.get('max_drawdown_pct')
    sharpe   = stats.get('sharpe_ratio')
    n_trades = stats.get('total_trades', 0)
    raw      = stats.get('raw_metrics', {})
    finishing_balance = raw.get('finishing_balance')

    liq_from_stats = (finishing_balance is not None and finishing_balance < 10.0)

    trades = load_trades(trades_path)

    liq_from_trades = False
    cagr_adj = cagr_raw
    mdd_adj  = mdd_raw

    if trades:
        equity = 10000.0
        peak   = equity
        mdd_a  = 0.0

        for t in trades:
            notional = t['qty'] * t['entry_price']
            fc       = funding_for_trade(t['opened_at'], t['closed_at'], notional, funding_data)
            pnl_adj  = t['pnl'] - fc
            equity  += pnl_adj
            peak     = max(peak, equity)
            dd       = (equity - peak) / peak * 100 if peak > 0 else 0.0
            if dd < mdd_a:
                mdd_a = dd

            if t['entry_price'] > 0:
                margin_ret = (t['exit_price'] - t['entry_price']) / t['entry_price'] * LEVERAGE
                if margin_ret <= -1.0:
                    liq_from_trades = True

        ret_adj = (equity - 10000.0) / 10000.0
        cagr_adj = _annualize(ret_adj, duration_years)
        mdd_adj  = mdd_a

        if equity < 10.0:
            liq_from_trades = True

    liquidated = liq_from_stats or liq_from_trades

    return {
        'window':             w_name,
        'complete':           True,
        'cagr_raw':           cagr_raw,
        'mdd_raw':            mdd_raw,
        'cagr_adj':           cagr_adj,
        'mdd_adj':            mdd_adj,
        'sharpe':             sharpe,
        'trades_count':       n_trades,
        'liquidated':         liquidated,
        'finishing_balance':  finishing_balance,
    }


# ---------------------------------------------------------------------------
# Combo aggregation
# ---------------------------------------------------------------------------

def aggregate_combo(combo_id: int, hp: dict, output_dir: Path,
                    funding_data: dict) -> dict:
    window_results = []
    for w_name, w_start, w_end in WINDOWS_8:
        wr = analyze_window(combo_id, w_name, w_start, w_end, output_dir, funding_data)
        window_results.append(wr)

    complete_windows = [wr for wr in window_results if wr['complete']]
    n_complete = len(complete_windows)

    if n_complete == 0:
        return {
            'combo_id': combo_id, **hp,
            'n_complete': 0, 'n_positive': 0,
            'mean_cagr': None, 'std_cagr': None, 'worst_window': None, 'worst_mdd': None,
            'mean_mdd': None, 'total_trades': 0, 'liquidated': False,
            'tier1': False, 'tier2': False, 'tier3': False, 'tier4': False,
            'tier_pass': False, 'cagr_rank': 9999, 'safe_rank': 9999,
            'safety_score': None, 'window_stats_json': json.dumps(window_results),
            'plateau_quality': 'EDGE', 'plateau_score': 0.0,
            'sweet_spot_score': None,
            'cross_validated': False, 'v4_combo_id': None,
            'v4_mean_cagr': None, 'v4_v5_drift': None,
        }

    cagrs      = [wr['cagr_adj'] for wr in complete_windows if wr['cagr_adj'] is not None]
    mdds       = [wr['mdd_adj']  for wr in complete_windows if wr['mdd_adj']  is not None]
    trade_cnts = [wr['trades_count'] for wr in complete_windows]
    liquidated = any(wr['liquidated'] for wr in complete_windows)

    mean_cagr    = mean(cagrs) if cagrs else None
    std_cagr     = stdev(cagrs) if len(cagrs) > 1 else 0.0
    n_positive   = sum(1 for c in cagrs if c > 0)
    worst_window = min(cagrs) if cagrs else None
    worst_mdd    = min(mdds)  if mdds  else None
    mean_mdd     = mean(mdds) if mdds  else None
    total_trades = sum(trade_cnts)

    return {
        'combo_id':   combo_id,
        **hp,
        'n_complete':    n_complete,
        'n_positive':    n_positive,
        'mean_cagr':     mean_cagr,
        'std_cagr':      std_cagr,
        'worst_window':  worst_window,
        'worst_mdd':     worst_mdd,
        'mean_mdd':      mean_mdd,
        'total_trades':  total_trades,
        'liquidated':    liquidated,
        'tier1': not liquidated,
        'tier2': n_positive == 8 and n_complete == 8,
        'tier3': False,
        'tier4': False,
        'tier_pass': False,
        'cagr_rank': 9999,
        'safe_rank':  9999,
        'safety_score': None,
        'window_stats_json': json.dumps(window_results, default=str),
        # v5 추가 (plateau/cross-val은 후처리에서 채움)
        'plateau_quality': 'EDGE',
        'plateau_score': 0.0,
        'sweet_spot_score': None,
        'cross_validated': False,
        'v4_combo_id': None,
        'v4_mean_cagr': None,
        'v4_v5_drift': None,
    }


# ---------------------------------------------------------------------------
# Baseline (v4 reference — combo_18 not in v5 grid)
# ---------------------------------------------------------------------------

def load_v4_baseline() -> dict:
    """v4_all_combos.csv에서 combo_18(st=2.5/p=6/fe=7/se=20/de=200/atr=3.0) 참조값 로드."""
    if V4_CSV.exists():
        try:
            with open(V4_CSV) as f:
                for row in csv.DictReader(f):
                    if (abs(float(row['st_factor']) - 2.5) < 0.001
                            and int(float(row['st_period'])) == 6
                            and int(float(row['fast_ema_len'])) == 7
                            and int(float(row['slow_ema_len'])) == 20
                            and int(float(row['direction_ema_len'])) == 200
                            and abs(float(row['atr_mult']) - 3.0) < 0.001):
                        mc = float(row['mean_cagr'])
                        sc = float(row['std_cagr'])
                        print(f"  [baseline] loaded from v4 CSV: mean={mc:.2f}% std={sc:.2f}%")
                        return {'mean_cagr': mc, 'std_cagr': sc}
        except Exception as e:
            print(f"  [warn] v4 baseline load failed: {e}")
    print("  [baseline] fallback to literals: mean=144.99% std=149.17%")
    return {'mean_cagr': 144.99, 'std_cagr': 149.17}


# ---------------------------------------------------------------------------
# Tier gates + ranking
# ---------------------------------------------------------------------------

def apply_tiers_and_ranking(results: list[dict], baseline: dict) -> None:
    baseline_mean_cagr = baseline['mean_cagr']
    baseline_std_cagr  = baseline['std_cagr']
    tier4_threshold    = baseline_std_cagr * 1.2

    for r in results:
        mc = r['mean_cagr']
        sc = r['std_cagr']
        t3 = (mc is not None and mc > baseline_mean_cagr)
        t4 = (sc is not None and sc < tier4_threshold)
        r['tier3'] = t3
        r['tier4'] = t4
        r['tier_pass'] = r['tier1'] and r['tier2'] and t3 and t4

    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    for rank, r in enumerate(cagr_sorted, 1):
        r['cagr_rank'] = rank

    for r in valid:
        mc = r['mean_cagr'] or 0.0
        sc = r['std_cagr']  or 0.0
        np_ = r['n_positive']
        ww  = r['worst_window'] or 0.0
        consistency = mc / max(sc, 0.01) if sc is not None else mc * 10.0
        r['safety_score'] = np_ * 10.0 + consistency + ww * 0.5

    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    for rank, r in enumerate(safe_sorted, 1):
        r['safe_rank'] = rank


# ---------------------------------------------------------------------------
# Plateau analysis
# ---------------------------------------------------------------------------

def apply_plateau(results: list[dict]) -> None:
    lookup = {_param_key(r): r for r in results if r['mean_cagr'] is not None}

    for r in results:
        if r['mean_cagr'] is None:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        target_cagr = r['mean_cagr']
        neighbors = []

        for param in PARAMS:
            levels = GRID[param]
            target_val = _grid_level_key(param, r[param])
            try:
                idx = [_grid_level_key(param, v) for v in levels].index(target_val)
            except ValueError:
                continue

            for offset in [-1, 1]:
                new_idx = idx + offset
                if 0 <= new_idx < len(levels):
                    nb_key = tuple(
                        _grid_level_key(p, levels[new_idx]) if p == param
                        else _grid_level_key(p, r[p])
                        for p in PARAMS
                    )
                    if nb_key in lookup:
                        neighbors.append(lookup[nb_key])

        if len(neighbors) < 4:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        neighbor_cagrs = [n['mean_cagr'] for n in neighbors if n['mean_cagr'] is not None]
        neighbor_pass  = [n['tier_pass'] for n in neighbors if n['mean_cagr'] is not None]

        if not neighbor_cagrs:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        pass_ratio = sum(1 for p in neighbor_pass if p) / len(neighbor_pass)
        cagr_ratio = (sum(neighbor_cagrs) / len(neighbor_cagrs)) / target_cagr if target_cagr > 0 else 0.0
        score = round(pass_ratio * cagr_ratio * 100, 2)

        if pass_ratio >= 0.8 and cagr_ratio >= 0.7:
            quality = 'PLATEAU'
        elif cagr_ratio < 0.5:
            quality = 'ISLAND'
        else:
            quality = 'MIXED'

        r['plateau_quality'] = quality
        r['plateau_score'] = score


# ---------------------------------------------------------------------------
# v4∩v5 Cross-Validation
# ---------------------------------------------------------------------------

def apply_cross_validation(results: list[dict]) -> int:
    """v4_all_combos.csv와 HP 매칭, drift/cross_validated 부여. overlap count 반환."""
    if not V4_CSV.exists():
        print("  [cross-val] v4 CSV not found — skipping")
        return 0

    v4_rows = []
    try:
        with open(V4_CSV) as f:
            v4_rows = list(csv.DictReader(f))
    except Exception as e:
        print(f"  [cross-val] read error: {e}")
        return 0

    overlap = 0
    drift_warn = []

    for r in results:
        if r['mean_cagr'] is None:
            continue
        for v4 in v4_rows:
            try:
                if (abs(float(v4['st_factor']) - float(r['st_factor'])) < 0.001
                        and int(float(v4['st_period'])) == int(r['st_period'])
                        and int(float(v4['fast_ema_len'])) == int(r['fast_ema_len'])
                        and int(float(v4['slow_ema_len'])) == int(r['slow_ema_len'])
                        and int(float(v4['direction_ema_len'])) == int(r['direction_ema_len'])
                        and abs(float(v4['atr_mult']) - float(r['atr_mult'])) < 0.001):
                    v4_mc = float(v4['mean_cagr'])
                    v4_tp = v4['tier_pass'] == 'True'
                    drift = abs(v4_mc - r['mean_cagr']) / max(abs(v4_mc), 1.0)
                    r['v4_combo_id'] = int(v4['combo_id'])
                    r['v4_mean_cagr'] = round(v4_mc, 2)
                    r['v4_v5_drift'] = round(drift * 100, 2)
                    stable = drift < 0.10 and v4_tp == r['tier_pass']
                    r['cross_validated'] = stable
                    overlap += 1
                    if drift > 0.15:
                        drift_warn.append((r['combo_id'], v4_mc, r['mean_cagr'], drift * 100))
                    break
            except (ValueError, KeyError):
                continue

    print(f"  [cross-val] v4∩v5 overlap: {overlap} combos, "
          f"STABLE: {sum(1 for r in results if r['cross_validated'])}")
    if drift_warn:
        print(f"  [cross-val] ⚠ DRIFT >15% ({len(drift_warn)} combos):")
        for cid, v4mc, v5mc, d in sorted(drift_warn, key=lambda x: -x[3]):
            print(f"    combo={cid} v4={v4mc:.1f}% v5={v5mc:.1f}% drift={d:.1f}%")
        if any(d > 20 for _, _, _, d in drift_warn):
            print("  [cross-val] ⚠⚠ DRIFT >20% DETECTED — environment may have changed!")

    return overlap


# ---------------------------------------------------------------------------
# Sweet spot score + v6 extraction
# ---------------------------------------------------------------------------

def apply_sweet_spot_score(results: list[dict]) -> None:
    for r in results:
        if r['safety_score'] is None or r['mean_cagr'] is None:
            r['sweet_spot_score'] = None
            continue
        r['sweet_spot_score'] = round(
            (r['safety_score'] or 0.0) * 0.4
            + (r['plateau_score'] or 0.0) * 0.4
            + (r['mean_cagr'] / 200.0 * 100.0) * 0.2,
            2
        )


def write_v6_input(results: list[dict], output_dir: Path) -> Path:
    candidates = [
        r for r in results
        if r['tier_pass']
        and r['plateau_quality'] in ('PLATEAU', 'MIXED')
        and r['worst_window'] is not None and r['worst_window'] > 0
        and r['sweet_spot_score'] is not None
    ]
    candidates_sorted = sorted(candidates, key=lambda r: r['sweet_spot_score'], reverse=True)

    # v6 tier 분류
    for r in candidates_sorted:
        in_v4 = (r['v4_combo_id'] is not None
                 and r['v4_mean_cagr'] is not None
                 and r['v4_mean_cagr'] > 144.99)
        if in_v4 and r['cross_validated'] and r['plateau_quality'] == 'PLATEAU':
            r['v6_tier'] = 'A'
        elif r['plateau_quality'] == 'PLATEAU':
            r['v6_tier'] = 'B'
        else:
            r['v6_tier'] = 'C'

    top15 = sorted(candidates_sorted[:15], key=lambda r: (r['v6_tier'], -r['sweet_spot_score']))

    fieldnames = [
        'v6_tier', 'combo_id', 'st_factor', 'st_period', 'fast_ema_len',
        'slow_ema_len', 'direction_ema_len', 'atr_mult',
        'mean_cagr', 'std_cagr', 'n_positive', 'worst_window', 'worst_mdd',
        'plateau_quality', 'plateau_score', 'safety_score', 'sweet_spot_score',
        'cross_validated', 'v4_combo_id', 'v4_mean_cagr', 'v4_v5_drift',
    ]
    out_path = output_dir / 'v6_input_combos.csv'
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(top15)
    print(f"v6 input: {out_path} ({len(top15)} combos)")
    return out_path


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_csv(results: list[dict], output_dir: Path) -> Path:
    fieldnames = [
        'combo_id', 'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
        'direction_ema_len', 'atr_mult', 'sl_margin_pct',
        'n_complete', 'n_positive', 'mean_cagr', 'std_cagr',
        'worst_window', 'worst_mdd', 'mean_mdd', 'total_trades', 'liquidated',
        'tier1', 'tier2', 'tier3', 'tier4', 'tier_pass',
        'cagr_rank', 'safe_rank', 'safety_score',
        'plateau_quality', 'plateau_score', 'sweet_spot_score',
        'cross_validated', 'v4_combo_id', 'v4_mean_cagr', 'v4_v5_drift',
        'window_stats_json',
    ]
    csv_path = output_dir / 'v5_all_combos.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"Written: {csv_path} ({len(results)} rows)")
    return csv_path


def write_summary(results: list[dict], baseline: dict, output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    tier_pass   = [r for r in valid if r['tier_pass']]
    tier1_only  = [r for r in valid if r['tier1']]
    tier12      = [r for r in valid if r['tier1'] and r['tier2']]
    all_8_pos   = [r for r in valid if r['n_positive'] == 8]

    lines = [
        '# v5 Dense Grid + Plateau 집계 결과',
        '',
        f'총 {len(results)}개 combo | 분석 완료(≥6윈도우): {len(valid)}개',
        f'Tier1(무청산): {len(tier1_only)} | Tier1+2(무청산+전체양수): {len(tier12)} | Tier1-4전체: {len(tier_pass)}',
        '',
        f'> Baseline: v4 reference (combo_18, st=2.5/p=6/fe=7/se=20/de=200/atr=3.0) — **v5 grid 밖**',
        f'> Tier3 기준: mean_cagr > {baseline["mean_cagr"]:.2f}% | Tier4: std_cagr < {baseline["std_cagr"]*1.2:.2f}%',
        '> SL 없음(sl_margin_pct=0.0) — 3x 레버리지, 가격 -33.3% = 마진 청산 위험',
        '> 펀딩비: 2019-09-10 이전 funding=0 처리 (W1·W2 CAGR 과대평가 한계)',
        '> 독립 윈도우: 각 window balance=10,000 시작',
        '',
    ]

    lines += [
        '## CAGR Top-20 (mean_cagr_adj DESC)',
        '',
        '| CAGR# | id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | plateau | T1 | T2 | T3 | T4 | pass |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    for r in cagr_sorted[:20]:
        t1 = '✅' if r['tier1'] else '❌'
        t2 = '✅' if r['tier2'] else '❌'
        t3 = '✅' if r['tier3'] else '❌'
        t4 = '✅' if r['tier4'] else '❌'
        tp = '✅' if r['tier_pass'] else '❌'
        lines.append(
            f"| {r['cagr_rank']} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
            f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | {r['atr_mult']} | "
            f"{r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | {r['n_positive']}/8 | "
            f"{r['worst_window']:.1f} | {r['plateau_quality']} | {t1} | {t2} | {t3} | {t4} | {tp} |"
        )

    lines += ['', '## Safe Top-20 (safety_score DESC)', '',
              '| Safe# | id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | safety | plateau | T1 | T2 | T3 | T4 | pass |',
              '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for r in safe_sorted[:20]:
        t1 = '✅' if r['tier1'] else '❌'
        t2 = '✅' if r['tier2'] else '❌'
        t3 = '✅' if r['tier3'] else '❌'
        t4 = '✅' if r['tier4'] else '❌'
        tp = '✅' if r['tier_pass'] else '❌'
        lines.append(
            f"| {r['safe_rank']} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
            f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | {r['atr_mult']} | "
            f"{r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | {r['n_positive']}/8 | "
            f"{r['worst_window']:.1f} | {r['safety_score']:.1f} | {r['plateau_quality']} | {t1} | {t2} | {t3} | {t4} | {tp} |"
        )

    # Plateau 분포
    plateau_counts: dict[str, int] = {}
    for r in valid:
        k = r['plateau_quality']
        plateau_counts[k] = plateau_counts.get(k, 0) + 1
    lines += ['', '## Plateau 분포', '']
    for k in ['PLATEAU', 'MIXED', 'ISLAND', 'EDGE']:
        lines.append(f'- {k}: {plateau_counts.get(k, 0)}/{len(valid)}')

    # Parameter summary
    lines += ['', '## 파라미터별 mean_cagr 평균', '']
    for pk in PARAMS:
        groups: dict = {}
        for r in valid:
            v = r.get(pk)
            if v is not None:
                groups.setdefault(str(v), []).append(r['mean_cagr'])
        if groups:
            lines.append(f'**{pk}**')
            for val in sorted(groups.keys(), key=lambda x: float(x)):
                cagrs = groups[val]
                lines.append(f'  - {val}: mean_cagr={mean(cagrs):.1f}% (n={len(cagrs)})')
        lines.append('')

    all_mc = [r['mean_cagr'] for r in valid]
    lines += [
        '## 통계',
        '',
        f'- mean_cagr 최대: {max(all_mc):.2f}% / 최소: {min(all_mc):.2f}% / 평균: {mean(all_mc):.2f}%',
        f'- 청산 발생 combo: {sum(1 for r in valid if r["liquidated"])}/{len(valid)}',
        f'- n_positive=8: {len(all_8_pos)}/{len(valid)}',
        f'- Tier1-4 전부(tier_pass): {len(tier_pass)}/{len(valid)}',
    ]

    md_path = output_dir / 'v5_summary.md'
    md_path.write_text('\n'.join(lines) + '\n')
    print(f"Summary: {md_path}")
    return md_path


def write_verdict(results: list[dict], baseline: dict, output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    cagr_top10_ids = {r['combo_id'] for r in cagr_sorted[:10]}
    safe_top10_ids = {r['combo_id'] for r in safe_sorted[:10]}
    intersection   = cagr_top10_ids & safe_top10_ids
    all_8_pos   = [r for r in valid if r['n_positive'] == 8]
    tier_pass   = [r for r in valid if r['tier_pass']]
    bmc = baseline['mean_cagr']
    bsc = baseline['std_cagr']

    q1_combos = [r for r in valid
                 if r['mean_cagr'] >= bmc and r['std_cagr'] < bsc]

    lines = [
        '# 19 — Supertrend 4h Long 3x v5 Dense Grid Plateau Verdict',
        '',
        f'*Generated: {datetime.now(timezone.utc).isoformat()}*',
        '',
        '## 요약',
        '',
        f'- 그리드: 324 combos (st_factor×3, st_period×3, fast_ema×3, slow_ema×2, dir_ema×2, atr×3)',
        f'- 윈도우: 8 독립 (2017-08-18~2026-04-30 동등 분할, balance=10,000 각)',
        f'- SL: 없음 (sl_margin_pct=0.0) — v4와 동일 조건',
        f'- 분석 combo: {len(valid)}/{len(results)} (≥6 윈도우 완료)',
        f'- Baseline: **v4 reference** (combo_18 = st=2.5/p=6/fe=7/se=20/de=200/atr=3.0, **v5 grid 밖**)',
        f'  mean_cagr={bmc:.2f}% std={bsc:.2f}% (v4_all_combos.csv 실측값)',
        '',
        '---',
        '',
        '## 4대 핵심 질문',
        '',
    ]

    # Q1
    lines += ['### Q1. mean_cagr ≥ baseline AND std_cagr < baseline인 combo 존재?', '']
    if q1_combos:
        lines.append(f'**YES — {len(q1_combos)}개 combo 발견**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | plateau | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(q1_combos, key=lambda x: x['mean_cagr'], reverse=True)[:10]:
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | "
                f"{r['plateau_quality']} | #{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append(f'**NO — baseline(mean={bmc:.1f}%, std={bsc:.1f}%) 동시 개선 combo 없음**')
    lines.append('')

    # Q2
    lines += ['### Q2. 8 윈도우 전부 양수인 combo 존재?', '']
    if all_8_pos:
        lines.append(f'**YES — {len(all_8_pos)}개**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | worst_W% | plateau | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(all_8_pos, key=lambda x: x['mean_cagr'], reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['worst_window']:.1f} | "
                f"{r['plateau_quality']} | #{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append('**NO — 8 윈도우 전부 양수인 combo 없음**')
    lines.append('')

    # Q3
    lines += ['### Q3. CAGR-top10 ∩ Safe-top10 교집합 combo 존재?', '']
    if intersection:
        lines.append(f'**YES — {len(intersection)}개**: combo_id {sorted(intersection)}')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | plateau | CAGR# | Safe# | tier_pass |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|']
        for cid in sorted(intersection):
            r = next(x for x in valid if x['combo_id'] == cid)
            tp = '✅' if r['tier_pass'] else '❌'
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['plateau_quality']} | "
                f"#{r['cagr_rank']} | #{r['safe_rank']} | {tp} |"
            )
    else:
        lines.append('**NO — CAGR-top10과 Safe-top10 교집합 없음**')
    lines.append('')

    # Q4
    lines += [
        '### Q4. Tier 1-4 전부 통과(ROBUST winner)는?',
        '',
        f'- Tier1(무청산): {sum(1 for r in valid if r["tier1"])}/{len(valid)}',
        f'- Tier2(전구간양수): {sum(1 for r in valid if r["tier2"])}/{len(valid)}',
        f'- Tier3(mean_cagr>{bmc:.2f}%): {sum(1 for r in valid if r["tier3"])}/{len(valid)}',
        f'- Tier4(std_cagr<{bsc*1.2:.2f}%): {sum(1 for r in valid if r["tier4"])}/{len(valid)}',
        f'- **Tier1-4 전부**: {len(tier_pass)}/{len(valid)}',
        '',
    ]
    if tier_pass:
        lines.append(f'**ROBUST winner {len(tier_pass)}개:**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | plateau | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(tier_pass, key=lambda x: x['mean_cagr'], reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | "
                f"{r['n_positive']}/8 | {r['worst_window']:.1f} | "
                f"{r['plateau_quality']} | #{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append('**0개 — ROBUST winner 없음.**')
    lines.append('')

    # Plateau analysis section
    lines += ['---', '', '## Plateau 분석', '']
    plateau_counts: dict[str, int] = {}
    for r in valid:
        k = r['plateau_quality']
        plateau_counts[k] = plateau_counts.get(k, 0) + 1
    for k in ['PLATEAU', 'MIXED', 'ISLAND', 'EDGE']:
        lines.append(f'- {k}: {plateau_counts.get(k, 0)}/{len(valid)}')
    lines.append('')

    plateau_robust = [r for r in tier_pass if r['plateau_quality'] == 'PLATEAU']
    if plateau_robust:
        lines.append(f'**PLATEAU + ROBUST 동시 통과: {len(plateau_robust)}개** (v6 Tier A 후보)')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | plateau_score | cross_val | v4_id |',
                  '|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(plateau_robust, key=lambda x: x['plateau_score'], reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['plateau_score']:.1f} | "
                f"{'✅' if r['cross_validated'] else '—'} | {r['v4_combo_id'] or '—'} |"
            )
        lines.append('')
    else:
        lines.append('> PLATEAU + ROBUST 동시 통과 combo 없음 — MIXED + ROBUST로 v6 입력 검토 필요')
        lines.append('')

    # v4∩v5 cross-validation section
    lines += ['---', '', '## v4∩v5 Cross-Validation', '']
    overlap_rows = [r for r in valid if r['v4_combo_id'] is not None]
    stable_rows  = [r for r in overlap_rows if r['cross_validated']]
    lines += [
        f'- v4∩v5 공통 combo: {len(overlap_rows)} (예상 ≈12)',
        f'- STABLE (drift<10% AND tier_pass 일치): {len(stable_rows)}/{len(overlap_rows)}',
    ]
    if overlap_rows:
        drifts = [r['v4_v5_drift'] for r in overlap_rows if r['v4_v5_drift'] is not None]
        if drifts:
            lines.append(f'- drift 평균: {mean(drifts):.1f}% / 최대: {max(drifts):.1f}%')
        lines += ['',
                  '| v5_id | v4_id | st_f | st_p | fe | se | de | atr | v5_cagr% | v4_cagr% | drift% | stable | v5_plateau |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(overlap_rows, key=lambda x: x['v4_v5_drift'] or 0, reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['v4_combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['v4_mean_cagr']:.1f} | "
                f"{r['v4_v5_drift']:.1f} | {'✅' if r['cross_validated'] else '❌'} | {r['plateau_quality']} |"
            )
        lines.append('')
        max_drift = max(drifts) if drifts else 0
        if max_drift > 20:
            lines.append('> ⚠⚠ drift >20% 발생 — backtest 환경 변동 의심. v6 진행 전 검토 필요.')
        elif max_drift > 15:
            lines.append('> ⚠ drift >15% 발생 — 환경 검토 권고.')
        else:
            lines.append('> ✅ drift <15% — 환경 안정, v6 결과 신뢰 가능.')
        lines.append('')

    # v6 input candidates section
    lines += ['---', '', '## v6 TP/SL Sweep 입력 군집 (Sweet Spot Top-15)', '']
    sweet_candidates = [
        r for r in valid
        if r['tier_pass'] and r['plateau_quality'] in ('PLATEAU', 'MIXED')
        and r['worst_window'] is not None and r['worst_window'] > 0
        and r['sweet_spot_score'] is not None
    ]
    sweet_sorted = sorted(sweet_candidates, key=lambda r: r['sweet_spot_score'], reverse=True)
    if sweet_sorted:
        lines.append(f'**{min(len(sweet_sorted), 15)}개 선정** (tier_pass & plateau∈{{PLATEAU,MIXED}} & worst_window>0)')
        lines += ['',
                  '| rank | id | st_f | st_p | fe | se | de | atr | mean_cagr% | worst_W% | plateau | sweet_score | v6_tier |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
        for rank, r in enumerate(sweet_sorted[:15], 1):
            in_v4 = r['v4_combo_id'] is not None and r['v4_mean_cagr'] is not None and r['v4_mean_cagr'] > 144.99
            if in_v4 and r['cross_validated'] and r['plateau_quality'] == 'PLATEAU':
                v6t = 'A'
            elif r['plateau_quality'] == 'PLATEAU':
                v6t = 'B'
            else:
                v6t = 'C'
            lines.append(
                f"| {rank} | {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['worst_window']:.1f} | "
                f"{r['plateau_quality']} | {r['sweet_spot_score']:.1f} | {v6t} |"
            )
        lines += ['', '> sweet_spot_score = safety×0.4 + plateau×0.4 + (mean_cagr/200×100)×0.2',
                  '> v6_tier A: v4∩v5 ROBUST+PLATEAU+STABLE | B: ROBUST+PLATEAU | C: ROBUST+MIXED',
                  '> → v6_input_combos.csv 로 저장됨']
    else:
        lines.append('> ⚠ sweet spot 후보 없음 — v6 입력 불충분. grid 재검토 필요.')
    lines.append('')

    # Decision
    lines += ['---', '', '## 결정 분기', '']
    if sweet_sorted:
        winner = sweet_sorted[0]
        lines += [
            f'**→ v6 sweep 진행**: `v6_input_combos.csv` top 15 combo를 carrier로 TP×SL grid(~12조합) 적용',
            '',
            f'v5 대표 sweet spot: combo_id={winner["combo_id"]} '
            f'(st={winner["st_factor"]}/p={winner["st_period"]}/fe={winner["fast_ema_len"]}/'
            f'se={winner["slow_ema_len"]}/de={winner["direction_ema_len"]}/atr={winner["atr_mult"]})',
            '',
            f'mean_cagr={winner["mean_cagr"]:.1f}% / worst_W={winner["worst_window"]:.1f}% / '
            f'plateau={winner["plateau_quality"]} / sweet_score={winner["sweet_spot_score"]:.1f}',
        ]
    elif tier_pass:
        lines += [
            '**→ v6 진행 가능하나 plateau 미확인**: tier_pass combo를 v6 입력으로 사용.',
            '단, ISLAND 라벨 combo 우세 시 overfitting 의심 — v6 결과 해석 주의.',
        ]
    else:
        lines += [
            '**→ v6 보류**: ROBUST winner 없음. v5 grid 재설계 또는 v4 winner(combo_70) 유지.',
        ]

    lines += [
        '',
        '---',
        '',
        '## 한계 및 주의',
        '',
        '- Baseline이 v5 grid 밖 — Tier3/4는 v4 실측값(144.99%/149.17%) 기준 (공정한 절대 비교)',
        '- W1(2017-08~2018-09), W2(2018-09~2019-10 일부): 펀딩비 데이터 없음 → CAGR 과대평가',
        '- SL 없음 조건: 단일 트레이드 -33.3% 가격 하락 시 마진 전액 손실 가능',
        '- Plateau 분석: 이웃<4인 grid 경계 combo는 EDGE 처리 (plateau 판단 불가)',
        '- v4∩v5 drift>10%: 환경 변동 또는 데이터 변경 의심 — 절대 수치보다 상대 순위 신뢰',
    ]

    verdict_path = output_dir / '19_SUPERTREND_V5_PLATEAU_CROSSVAL_VERDICT.md'
    verdict_path.write_text('\n'.join(lines) + '\n')
    print(f"Verdict: {verdict_path}")
    return verdict_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v5_optimization/queue.sqlite3')
    p.add_argument('--output-dir', type=str, default='/result/v5_optimization')
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT DISTINCT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct FROM jobs ORDER BY combo_id"
    ).fetchall()
    conn.close()

    combos = [dict(r) for r in rows]
    print(f"Processing {len(combos)} combos × 8 windows...")

    # v4 baseline (combo_18, not in v5 grid)
    baseline = load_v4_baseline()
    print(f"Baseline (v4 ref): mean_cagr={baseline['mean_cagr']:.2f}% std={baseline['std_cagr']:.2f}%")
    print(f"  Tier3 threshold: >{baseline['mean_cagr']:.2f}%  "
          f"Tier4 threshold: <{baseline['std_cagr']*1.2:.2f}%")

    funding_data = load_funding_data()

    results = []
    for i, combo in enumerate(combos):
        hp = {k: combo[k] for k in ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
                                      'direction_ema_len', 'atr_mult', 'sl_margin_pct']}
        r = aggregate_combo(combo['combo_id'], hp, output_dir, funding_data)
        results.append(r)
        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{len(combos)}", flush=True)

    print(f"\nAggregation done: {len(results)} combos")

    apply_tiers_and_ranking(results, baseline)

    print("\nRunning plateau analysis...")
    apply_plateau(results)
    plateau_counts: dict[str, int] = {}
    for r in results:
        k = r['plateau_quality']
        plateau_counts[k] = plateau_counts.get(k, 0) + 1
    for k in ['PLATEAU', 'MIXED', 'ISLAND', 'EDGE']:
        print(f"  {k}: {plateau_counts.get(k, 0)}")

    print("\nRunning v4∩v5 cross-validation...")
    apply_cross_validation(results)

    apply_sweet_spot_score(results)

    write_csv(results, output_dir)
    write_summary(results, baseline, output_dir)
    write_verdict(results, baseline, output_dir)
    write_v6_input(results, output_dir)

    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    tier_pass = [r for r in valid if r['tier_pass']]
    sweet_candidates = [r for r in valid if r['tier_pass'] and r['plateau_quality'] in ('PLATEAU', 'MIXED') and r.get('worst_window', 0) is not None and (r.get('worst_window', 0) or 0) > 0]
    print(f"\n=== v5 결과 요약 ===")
    print(f"유효 combo: {len(valid)}/{len(results)}")
    print(f"청산 발생: {sum(1 for r in valid if r['liquidated'])}")
    print(f"n_positive=8: {sum(1 for r in valid if r['n_positive']==8)}")
    print(f"Tier1-4 ROBUST: {len(tier_pass)}")
    print(f"v6 sweet spot 후보: {len(sweet_candidates)}")
    if tier_pass:
        for r in sorted(tier_pass, key=lambda x: x['mean_cagr'], reverse=True)[:5]:
            print(f"  combo={r['combo_id']} mean={r['mean_cagr']:.1f}% "
                  f"n_pos={r['n_positive']}/8 plateau={r['plateau_quality']}")

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
