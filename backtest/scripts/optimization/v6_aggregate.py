#!/usr/bin/env python3
"""
v6_aggregate.py — 225 combo × 8 독립 윈도우 집계 + Tier A/B/C + sanity check

v5_aggregate.py 기반. v6 변경:
  - plateau/cross-val/v4-baseline 제거
  - v6 Tier A/B/C (worst_mdd 개선 기준) + v6_score 추가
  - Sanity: carrier v5_atr=3.0 × TP=3.0/SL=3.0 → v5 결과 ±15% 검증

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v6_aggregate.py \
        --db /result/v6_optimization/queue.sqlite3 \
        --output-dir /result/v6_optimization
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

# Tier 기준
TIER_A_MDD_THRESHOLD = -50.0  # worst_mdd ≥ -50% AND mean_cagr > 0
TIER_B_MDD_THRESHOLD = -70.0  # worst_mdd ≥ -70% AND mean_cagr ≥ v5_baseline × 0.5
TIER_C_MDD_IMPROVEMENT = 10.0 # worst_mdd > v5_worst_mdd + 10 (any improvement)


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
# Per-window analysis (v5와 동일 — 변경 없음)
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

    empty = {
        'combo_id': combo_id, **hp,
        'n_complete': 0, 'n_positive': 0,
        'mean_cagr': None, 'std_cagr': None, 'worst_window': None, 'worst_mdd': None,
        'mean_mdd': None, 'total_trades': 0, 'liquidated': False,
        'safety_score': None,
        'mdd_improvement': None, 'cagr_drop': None,
        'tier_a': False, 'tier_b': False, 'tier_c': False,
        'final_tier': 'FAIL', 'v6_score': None, 'rank_in_tier': 9999,
        'is_sanity_case': False, 'v5_v6_drift': None,
        'window_stats_json': json.dumps(window_results),
    }

    if n_complete == 0:
        return empty

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
        'safety_score':  None,
        'mdd_improvement': None,
        'cagr_drop':     None,
        'tier_a':        False,
        'tier_b':        False,
        'tier_c':        False,
        'final_tier':    'FAIL',
        'v6_score':      None,
        'rank_in_tier':  9999,
        'is_sanity_case': False,
        'v5_v6_drift':   None,
        'window_stats_json': json.dumps(window_results, default=str),
    }


# ---------------------------------------------------------------------------
# v6 Tier + score
# ---------------------------------------------------------------------------

def apply_v6_tiers(results: list[dict]) -> None:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]

    for r in valid:
        mc  = r['mean_cagr']
        wm  = r['worst_mdd']
        ww  = r['worst_window'] or 0.0
        v5m = r['v5_mean_cagr']
        v5d = r['v5_worst_mdd']
        sc  = r['std_cagr'] or 0.0
        np_ = r['n_positive']

        # Safety score (ranking within tier)
        consistency = mc / max(sc, 0.01) if sc > 0 else mc * 10.0
        r['safety_score'] = np_ * 10.0 + consistency + ww * 0.5

        # v6 MDD metrics
        mdd_improvement = v5d - wm if (wm is not None and v5d is not None) else None
        cagr_drop       = v5m - mc if (v5m is not None) else None
        r['mdd_improvement'] = round(mdd_improvement, 2) if mdd_improvement is not None else None
        r['cagr_drop']       = round(cagr_drop, 2) if cagr_drop is not None else None

        # Tiers
        tier_a = (wm is not None and wm >= TIER_A_MDD_THRESHOLD and mc > 0)
        tier_b = (wm is not None and wm >= TIER_B_MDD_THRESHOLD
                  and v5m is not None and mc >= v5m * 0.5)
        tier_c = (wm is not None and v5d is not None and wm > v5d + TIER_C_MDD_IMPROVEMENT)
        r['tier_a'] = tier_a
        r['tier_b'] = tier_b
        r['tier_c'] = tier_c

        if tier_a:    r['final_tier'] = 'A'
        elif tier_b:  r['final_tier'] = 'B'
        elif tier_c:  r['final_tier'] = 'C'
        else:         r['final_tier'] = 'FAIL'

        # v6 score: MDD 개선 핵심 + worst_window 보존 + CAGR drop 페널티
        if mdd_improvement is not None and cagr_drop is not None:
            r['v6_score'] = round(
                mdd_improvement * 1.0
                + ww * 2.0
                - cagr_drop * 0.3,
                2
            )

        # Sanity case: TP=3.0, SL=3.0, carrier v5_atr=3.0 → v5 atr_mult=3.0 재현
        tp = r.get('tp_atr_mult', 0.0)
        sl = r.get('sl_atr_mult', 0.0)
        v5a = r.get('v5_atr_mult', 0.0)
        is_sanity = (abs(tp - 3.0) < 0.01 and abs(sl - 3.0) < 0.01
                     and abs(v5a - 3.0) < 0.01)
        r['is_sanity_case'] = is_sanity
        if is_sanity and v5m is not None and abs(v5m) > 0.01:
            r['v5_v6_drift'] = round(abs(mc - v5m) / abs(v5m) * 100, 2)

    # Rank within tier
    for tier in ['A', 'B', 'C', 'FAIL']:
        tier_rows = [r for r in valid if r['final_tier'] == tier and r['v6_score'] is not None]
        for rank, r in enumerate(sorted(tier_rows, key=lambda x: x['v6_score'], reverse=True), 1):
            r['rank_in_tier'] = rank


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_csv(results: list[dict], output_dir: Path) -> Path:
    fieldnames = [
        'combo_id', 'carrier_id', 'v5_combo_id',
        'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
        'direction_ema_len', 'tp_atr_mult', 'sl_atr_mult', 'sl_margin_pct',
        'v5_atr_mult', 'v5_mean_cagr', 'v5_worst_mdd',
        'n_complete', 'n_positive', 'mean_cagr', 'std_cagr',
        'worst_window', 'worst_mdd', 'mean_mdd', 'total_trades', 'liquidated',
        'safety_score', 'mdd_improvement', 'cagr_drop',
        'tier_a', 'tier_b', 'tier_c', 'final_tier',
        'v6_score', 'rank_in_tier',
        'is_sanity_case', 'v5_v6_drift',
        'window_stats_json',
    ]
    csv_path = output_dir / 'v6_all_combos.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"Written: {csv_path} ({len(results)} rows)")
    return csv_path


def write_tier_csvs(results: list[dict], output_dir: Path) -> None:
    fieldnames = [
        'combo_id', 'carrier_id', 'v5_combo_id',
        'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
        'direction_ema_len', 'tp_atr_mult', 'sl_atr_mult',
        'v5_atr_mult', 'v5_mean_cagr', 'v5_worst_mdd',
        'mean_cagr', 'std_cagr', 'worst_window', 'worst_mdd',
        'mdd_improvement', 'cagr_drop', 'final_tier', 'v6_score', 'rank_in_tier',
    ]
    for tier in ['A', 'B', 'C']:
        tier_rows = sorted(
            [r for r in results if r['final_tier'] == tier],
            key=lambda r: r['v6_score'] or -9999, reverse=True
        )
        path = output_dir / f'v6_tier_{tier}.csv'
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(tier_rows)
        print(f"Tier {tier}: {path} ({len(tier_rows)} rows)")


def write_summary(results: list[dict], output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    tier_counts = {}
    for r in valid:
        k = r['final_tier']
        tier_counts[k] = tier_counts.get(k, 0) + 1

    lines = [
        '# v6 비대칭 TP/SL Sweep 집계 결과',
        '',
        f'총 {len(results)}개 combo | 분석 완료(≥6윈도우): {len(valid)}개',
        f'Carriers: 15 × TP×SL: 15 (TP∈[2.5,3.0,3.5] × SL∈[1.5,1.8,2.0,2.5,3.0])',
        '',
        '## Tier 분포',
        '',
        f'- Tier A (worst_mdd ≥ -50%, mean_cagr > 0): **{tier_counts.get("A", 0)}개**',
        f'- Tier B (worst_mdd ≥ -70%, cagr ≥ v5×0.5): **{tier_counts.get("B", 0)}개**',
        f'- Tier C (any mdd improvement > 10%p): **{tier_counts.get("C", 0)}개**',
        f'- FAIL: {tier_counts.get("FAIL", 0)}개',
        '',
        '## Top-20 by v6_score',
        '',
        '| rank | combo | carrier | tp | sl | final_tier | mean_cagr% | worst_mdd% | mdd_improv%p | cagr_drop%p | v6_score |',
        '|---|---|---|---|---|---|---|---|---|---|---|',
    ]

    scored = sorted([r for r in valid if r['v6_score'] is not None],
                    key=lambda r: r['v6_score'], reverse=True)
    for rank, r in enumerate(scored[:20], 1):
        lines.append(
            f"| {rank} | {r['combo_id']} | {r['carrier_id']} | "
            f"{r['tp_atr_mult']} | {r['sl_atr_mult']} | {r['final_tier']} | "
            f"{r['mean_cagr']:.1f} | {r['worst_mdd']:.1f} | "
            f"{r['mdd_improvement']:+.1f} | {r['cagr_drop']:+.1f} | {r['v6_score']:.1f} |"
        )

    # Sanity check
    sanity_rows = [r for r in valid if r['is_sanity_case']]
    lines += ['', '## Sanity Check (TP=3.0, SL=3.0, v5_atr=3.0)', '']
    if sanity_rows:
        lines += [
            '| carrier | combo | v5_cagr% | v6_cagr% | drift% | v5_mdd% | v6_mdd% | status |',
            '|---|---|---|---|---|---|---|---|',
        ]
        for r in sorted(sanity_rows, key=lambda x: x['carrier_id']):
            drift = r['v5_v6_drift'] or 0.0
            status = '✅' if drift < 15.0 else '⚠'
            lines.append(
                f"| {r['carrier_id']} | {r['combo_id']} | {r['v5_mean_cagr']:.1f} | "
                f"{r['mean_cagr']:.1f} | {drift:.1f} | {r['v5_worst_mdd']:.1f} | "
                f"{r['worst_mdd']:.1f} | {status} |"
            )
        drifts = [r['v5_v6_drift'] for r in sanity_rows if r['v5_v6_drift'] is not None]
        if drifts:
            lines.append(f'\n최대 drift: {max(drifts):.1f}%  평균: {mean(drifts):.1f}%')
            if max(drifts) > 15:
                lines.append('\n⚠ HALT 조건: sanity drift > 15% — 환경 변동 의심')
    else:
        lines.append('sanity case 없음 (DB에 TP=3.0/SL=3.0/v5_atr=3.0 조합 없음)')

    # Carrier best
    lines += ['', '## Carrier별 최적 TP/SL (v6_score 기준)', '']
    lines += [
        '| carrier_id | v5_combo | v5_cagr% | v5_mdd% | best_tp | best_sl | v6_cagr% | v6_mdd% | mdd_improv%p | tier |',
        '|---|---|---|---|---|---|---|---|---|---|',
    ]
    for cid in sorted(set(r['carrier_id'] for r in valid)):
        cr = [r for r in valid if r['carrier_id'] == cid and r['v6_score'] is not None]
        if not cr:
            continue
        best = max(cr, key=lambda r: r['v6_score'])
        lines.append(
            f"| {cid} | {best['v5_combo_id']} | {best['v5_mean_cagr']:.1f} | "
            f"{best['v5_worst_mdd']:.1f} | {best['tp_atr_mult']} | {best['sl_atr_mult']} | "
            f"{best['mean_cagr']:.1f} | {best['worst_mdd']:.1f} | "
            f"{best['mdd_improvement']:+.1f} | {best['final_tier']} |"
        )

    # TP/SL pattern analysis
    lines += ['', '## TP/SL 조합별 평균 mdd_improvement', '']
    lines += ['| tp | sl | n_combo | avg_mdd_improv%p | avg_cagr_drop%p | Tier A | Tier B |', '|---|---|---|---|---|---|---|']
    from itertools import product as iproduct
    for tp, sl in iproduct([2.5, 3.0, 3.5], [1.5, 1.8, 2.0, 2.5, 3.0]):
        rows = [r for r in valid
                if abs(r['tp_atr_mult'] - tp) < 0.01 and abs(r['sl_atr_mult'] - sl) < 0.01
                and r['mdd_improvement'] is not None]
        if not rows:
            continue
        avg_mdd = mean(r['mdd_improvement'] for r in rows)
        avg_cd  = mean(r['cagr_drop'] for r in rows)
        ta = sum(1 for r in rows if r['tier_a'])
        tb = sum(1 for r in rows if r['tier_b'])
        lines.append(f"| {tp} | {sl} | {len(rows)} | {avg_mdd:+.1f} | {avg_cd:+.1f} | {ta} | {tb} |")

    md_path = output_dir / 'v6_summary.md'
    md_path.write_text('\n'.join(lines) + '\n')
    print(f"Summary: {md_path}")
    return md_path


def write_verdict(results: list[dict], output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    tier_a_rows = sorted([r for r in valid if r['tier_a']], key=lambda r: r['v6_score'] or -9999, reverse=True)
    tier_b_rows = sorted([r for r in valid if r['tier_b'] and not r['tier_a']], key=lambda r: r['v6_score'] or -9999, reverse=True)
    tier_c_rows = sorted([r for r in valid if r['tier_c'] and not r['tier_a'] and not r['tier_b']], key=lambda r: r['v6_score'] or -9999, reverse=True)
    sanity_rows = [r for r in valid if r['is_sanity_case'] and r['v5_v6_drift'] is not None]

    lines = [
        '# 20 — Supertrend 4h Long 3x v6 비대칭 TP/SL Verdict',
        '',
        f'*Generated: {datetime.now(timezone.utc).isoformat()}*',
        '',
        '## 요약',
        '',
        f'- Carriers: 15개 (v5 sweet-spot top-15, sweet_score 순)',
        f'- TP/SL grid: TP∈[2.5,3.0,3.5] × SL∈[1.5,1.8,2.0,2.5,3.0] = 15 조합',
        f'- 총 combos: 225 × 8 windows = 1,800 backtests',
        f'- 분석 combo: {len(valid)}/225 (≥6 윈도우 완료)',
        f'- SL: sl_margin_pct=0.0 (ATR 비대칭만 사용)',
        f'- Leverage: 3x | Timeframe: 4h | Strategy: SupertrendStrategyAsymmetric',
        '',
        '---',
        '',
        '## 4대 핵심 질문',
        '',
    ]

    # Q1: Tier A
    lines += ['### Q1. worst_mdd ≥ -50% AND mean_cagr > 0 (Tier A — 실거래 가능)?', '']
    if tier_a_rows:
        lines.append(f'**YES — {len(tier_a_rows)}개 Tier A 발견 → 🟢 실거래 후보 존재**')
        lines += ['',
                  '| rank | combo | carrier | tp | sl | mean_cagr% | worst_mdd% | mdd_improv%p | cagr_drop%p | v6_score |',
                  '|---|---|---|---|---|---|---|---|---|---|']
        for r in tier_a_rows[:15]:
            lines.append(
                f"| {r['rank_in_tier']} | {r['combo_id']} | {r['carrier_id']} | "
                f"{r['tp_atr_mult']} | {r['sl_atr_mult']} | {r['mean_cagr']:.1f} | "
                f"{r['worst_mdd']:.1f} | {r['mdd_improvement']:+.1f} | "
                f"{r['cagr_drop']:+.1f} | {r['v6_score']:.1f} |"
            )
    else:
        lines.append('**NO — Tier A 없음. 3x 레버리지로 worst_mdd -50% 달성 불가.**')
    lines.append('')

    # Q2: Tier B
    lines += ['### Q2. worst_mdd ≥ -70% AND cagr ≥ v5 × 50% (Tier B — 의미 있는 개선)?', '']
    if tier_b_rows:
        lines.append(f'**YES — {len(tier_b_rows)}개 Tier B (Tier A 제외)**')
        lines += ['',
                  '| rank | combo | carrier | tp | sl | mean_cagr% | worst_mdd% | mdd_improv%p | cagr_drop%p | v6_score |',
                  '|---|---|---|---|---|---|---|---|---|---|']
        for r in tier_b_rows[:10]:
            lines.append(
                f"| {r['rank_in_tier']} | {r['combo_id']} | {r['carrier_id']} | "
                f"{r['tp_atr_mult']} | {r['sl_atr_mult']} | {r['mean_cagr']:.1f} | "
                f"{r['worst_mdd']:.1f} | {r['mdd_improvement']:+.1f} | "
                f"{r['cagr_drop']:+.1f} | {r['v6_score']:.1f} |"
            )
    else:
        lines.append('**NO — Tier B 없음.**')
    lines.append('')

    # Q3: Sanity check
    lines += ['### Q3. Sanity check (TP=3.0/SL=3.0/v5_atr=3.0 → v5 결과 ±15% 이내)?', '']
    if sanity_rows:
        drifts = [r['v5_v6_drift'] for r in sanity_rows]
        max_drift = max(drifts)
        avg_drift = mean(drifts)
        if max_drift < 15.0:
            lines.append(f'**✅ PASS — max drift {max_drift:.1f}% < 15% (avg {avg_drift:.1f}%) — 환경 안정**')
        else:
            lines.append(f'**⚠ FAIL — max drift {max_drift:.1f}% > 15% — 환경 변동 의심, 결과 신뢰도 검토 필요**')
        lines += ['',
                  '| carrier | combo | v5_cagr% | v6_cagr% | drift% | v5_mdd% | v6_mdd% |',
                  '|---|---|---|---|---|---|---|']
        for r in sorted(sanity_rows, key=lambda x: x['v5_v6_drift'] or 0, reverse=True):
            lines.append(
                f"| {r['carrier_id']} | {r['combo_id']} | {r['v5_mean_cagr']:.1f} | "
                f"{r['mean_cagr']:.1f} | {r['v5_v6_drift']:.1f} | "
                f"{r['v5_worst_mdd']:.1f} | {r['worst_mdd']:.1f} |"
            )
    else:
        lines.append('**N/A — sanity case 없음 (v5_atr=3.0 carrier 미해당)**')
    lines.append('')

    # Q4: Carrier별 최적 TP/SL
    lines += ['### Q4. 각 Carrier의 최적 (TP, SL) 조합은?', '', '']
    lines += [
        '| carrier | v5_combo | best_tp | best_sl | mdd_improv%p | cagr_drop%p | final_tier | v6_score |',
        '|---|---|---|---|---|---|---|---|',
    ]
    for cid in sorted(set(r['carrier_id'] for r in valid)):
        cr = [r for r in valid if r['carrier_id'] == cid and r['v6_score'] is not None]
        if not cr:
            continue
        best = max(cr, key=lambda r: r['v6_score'])
        lines.append(
            f"| {cid} | {best['v5_combo_id']} | {best['tp_atr_mult']} | {best['sl_atr_mult']} | "
            f"{best['mdd_improvement']:+.1f} | {best['cagr_drop']:+.1f} | "
            f"{best['final_tier']} | {best['v6_score']:.1f} |"
        )
    lines.append('')

    # Tier 분포
    tier_counts = {}
    for r in valid:
        k = r['final_tier']
        tier_counts[k] = tier_counts.get(k, 0) + 1

    lines += [
        '---',
        '',
        '## Tier 분포',
        '',
        f'- **Tier A** (worst_mdd ≥ -50%): **{tier_counts.get("A", 0)}/{len(valid)}**',
        f'- **Tier B** (worst_mdd ≥ -70%): **{tier_counts.get("B", 0)}/{len(valid)}**',
        f'- **Tier C** (any improvement ≥ 10%p): **{tier_counts.get("C", 0)}/{len(valid)}**',
        f'- FAIL: {tier_counts.get("FAIL", 0)}/{len(valid)}',
        '',
    ]

    # TP/SL pattern
    lines += ['## TP/SL 조합별 평균 mdd_improvement', '']
    lines += ['| tp | sl | avg_mdd_improv%p | avg_cagr_drop%p | Tier A | Tier B | n |', '|---|---|---|---|---|---|---|']
    from itertools import product as iproduct
    pattern_rows = []
    for tp, sl in iproduct([2.5, 3.0, 3.5], [1.5, 1.8, 2.0, 2.5, 3.0]):
        rows = [r for r in valid
                if abs(r['tp_atr_mult'] - tp) < 0.01 and abs(r['sl_atr_mult'] - sl) < 0.01
                and r['mdd_improvement'] is not None]
        if not rows:
            continue
        avg_mdd = mean(r['mdd_improvement'] for r in rows)
        avg_cd  = mean(r['cagr_drop'] for r in rows)
        ta = sum(1 for r in rows if r['tier_a'])
        tb = sum(1 for r in rows if r['tier_b'])
        pattern_rows.append((avg_mdd, tp, sl, avg_mdd, avg_cd, ta, tb, len(rows)))
    for _, tp, sl, avg_mdd, avg_cd, ta, tb, n in sorted(pattern_rows, reverse=True):
        lines.append(f"| {tp} | {sl} | {avg_mdd:+.1f} | {avg_cd:+.1f} | {ta} | {tb} | {n} |")
    lines.append('')

    # Decision
    lines += ['---', '', '## 결정 분기', '']
    if tier_a_rows:
        best_a = tier_a_rows[0]
        lines += [
            f'🟢 **Tier A 발견 → 실거래 후보**',
            '',
            f'v6 winner: combo={best_a["combo_id"]} carrier={best_a["carrier_id"]} '
            f'TP={best_a["tp_atr_mult"]}/SL={best_a["sl_atr_mult"]}',
            f'mean_cagr={best_a["mean_cagr"]:.1f}% / worst_mdd={best_a["worst_mdd"]:.1f}% / '
            f'mdd_improvement={best_a["mdd_improvement"]:+.1f}%p / v6_score={best_a["v6_score"]:.1f}',
            '',
            '→ Phase 5 게이트 (14_VERDICT) 재실행 권장. combo_18 → v6 winner upgrade 검토.',
        ]
    elif tier_b_rows:
        best_b = tier_b_rows[0]
        lines += [
            f'🟡 **Tier A 없음, Tier B {len(tier_b_rows)}개 → 1x/2x 레버리지 검토**',
            '',
            f'v6 Tier B top: combo={best_b["combo_id"]} carrier={best_b["carrier_id"]} '
            f'TP={best_b["tp_atr_mult"]}/SL={best_b["sl_atr_mult"]}',
            f'mean_cagr={best_b["mean_cagr"]:.1f}% / worst_mdd={best_b["worst_mdd"]:.1f}%',
            '',
            '→ v7 작업: 동일 HP로 1x backtest (worst_mdd 기대치 -25~-35%) / '
            '2x backtest (기대치 -45~-65%). BnH(+43%) 대비 우위 확인 후 레버리지 결정.',
        ]
    elif tier_c_rows:
        lines += [
            f'🟠 **Tier A/B 없음, Tier C {len(tier_c_rows)}개 (약한 개선)**',
            '',
            '→ 비대칭 TP/SL의 MDD 보호 효과 제한적. combo_18 운영 유지 + 시장가 SL 별도 검토.',
        ]
    else:
        lines += [
            '🔴 **모든 combo FAIL — 비대칭 TP/SL 도움 안 됨**',
            '',
            '→ 3x 전략 archive 결정. worst_mdd -90%대 구조적 한계 확정.',
            '→ 실거래: fa80 funding-arb 단독 유지.',
        ]

    lines += [
        '',
        '---',
        '',
        '## 한계 및 주의',
        '',
        '- EMA 데드크로스 청산이 ATR SL 도달 전 먼저 청산되는 경우: SL 효과 과소추정 가능',
        '  (v5와 동일 조건이므로 baseline 비교는 유효)',
        '- W1(2017-08~2018-09), W2(2018-09~2019-10 일부): 펀딩비 데이터 없음 → CAGR 과대평가',
        '- 독립 윈도우: 각 window balance=10,000 시작 (복리 효과 미반영)',
        '- worst_mdd는 funding-adjusted equity 기준 (v5 동일 방법론)',
    ]

    verdict_path = output_dir / '20_SUPERTREND_V6_ASYMMETRIC_TPSL_VERDICT.md'
    verdict_path.write_text('\n'.join(lines) + '\n')
    print(f"Verdict: {verdict_path}")
    return verdict_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v6_optimization/queue.sqlite3')
    p.add_argument('--output-dir', type=str, default='/result/v6_optimization')
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT DISTINCT combo_id, carrier_id, v5_combo_id, "
        "st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, tp_atr_mult, sl_atr_mult, sl_margin_pct, "
        "v5_atr_mult, v5_mean_cagr, v5_worst_mdd "
        "FROM jobs ORDER BY combo_id"
    ).fetchall()
    conn.close()

    combos = [dict(r) for r in rows]
    print(f"Processing {len(combos)} combos × 8 windows...")

    funding_data = load_funding_data()

    results = []
    for i, combo in enumerate(combos):
        hp = {k: combo[k] for k in [
            'carrier_id', 'v5_combo_id',
            'st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len',
            'direction_ema_len', 'tp_atr_mult', 'sl_atr_mult', 'sl_margin_pct',
            'v5_atr_mult', 'v5_mean_cagr', 'v5_worst_mdd',
        ]}
        r = aggregate_combo(combo['combo_id'], hp, output_dir, funding_data)
        results.append(r)
        if (i + 1) % 25 == 0:
            print(f"  Processed {i+1}/{len(combos)}", flush=True)

    print(f"\nAggregation done: {len(results)} combos")

    print("\nApplying v6 Tier A/B/C + scores...")
    apply_v6_tiers(results)

    # Sanity check summary
    sanity_rows = [r for r in results if r.get('is_sanity_case') and r.get('v5_v6_drift') is not None]
    if sanity_rows:
        max_drift = max(r['v5_v6_drift'] for r in sanity_rows)
        avg_drift = mean(r['v5_v6_drift'] for r in sanity_rows)
        print(f"  [sanity] {len(sanity_rows)} sanity cases, max_drift={max_drift:.1f}%, avg={avg_drift:.1f}%")
        if max_drift > 15:
            print(f"  ⚠ HALT: sanity drift {max_drift:.1f}% > 15% — environment shift suspected!")

    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    tier_a = [r for r in valid if r['tier_a']]
    tier_b = [r for r in valid if r['tier_b'] and not r['tier_a']]
    tier_c = [r for r in valid if r['tier_c'] and not r['tier_a'] and not r['tier_b']]
    print(f"Tier A: {len(tier_a)} / Tier B: {len(tier_b)} / Tier C: {len(tier_c)} / "
          f"FAIL: {len(valid) - len(tier_a) - len(tier_b) - len(tier_c)}")

    write_csv(results, output_dir)
    write_tier_csvs(results, output_dir)
    write_summary(results, output_dir)
    write_verdict(results, output_dir)

    if tier_a:
        best = tier_a[0]
        print(f"\n=== 🟢 Tier A winner ===")
        print(f"combo={best['combo_id']} carrier={best['carrier_id']} "
              f"TP={best['tp_atr_mult']}/SL={best['sl_atr_mult']}")
        print(f"mean_cagr={best['mean_cagr']:.1f}% worst_mdd={best['worst_mdd']:.1f}% "
              f"mdd_improvement={best['mdd_improvement']:+.1f}%p")
    elif tier_b:
        best = tier_b[0]
        print(f"\n=== 🟡 Tier B top (no Tier A) ===")
        print(f"combo={best['combo_id']} carrier={best['carrier_id']} "
              f"TP={best['tp_atr_mult']}/SL={best['sl_atr_mult']}")
        print(f"mean_cagr={best['mean_cagr']:.1f}% worst_mdd={best['worst_mdd']:.1f}%")
    else:
        print(f"\n=== 🔴 No Tier A/B — 3x MDD 보호 한계 확정 ===")

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
