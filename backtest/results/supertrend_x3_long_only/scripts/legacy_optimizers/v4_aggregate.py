#!/usr/bin/env python3
"""
v4_aggregate.py — 216 combo × 8 독립 윈도우 집계 + Tier 1-4 + 이중 랭킹 + Verdict

각 (combo, window) backtest 결과를 수집하여:
  - 펀딩비 차감 → window_cagr_adj, window_mdd_adj
  - 청산 감지 (Tier 1)
  - 8 윈도우 집계: mean_cagr, std_cagr, n_positive, worst_window, worst_mdd, liquidated
  - Tier 1-4 게이트 (combo_18 in-grid baseline 기준)
  - CAGR rank + Safe rank 이중 랭킹
  - v4_all_combos.csv, v4_summary.md, 18_SUPERTREND_V4_SWEET_SPOT_VERDICT.md

Usage (inside Jesse container):
    python3 /app/scripts/optimization/v4_aggregate.py \
        --db /result/v4_optimization/queue.sqlite3 \
        --output-dir /result/v4_optimization
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

# combo_18 HP (baseline 기준): 이 파라미터와 일치하는 in-grid combo가 baseline
COMBO_18_HP = {
    'st_factor': 2.5, 'st_period': 6, 'fast_ema_len': 7,
    'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0,
}

LEVERAGE = 3.0


def _ts_ms(date_str: str) -> int:
    return int(datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


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
# Per-window analysis
# ---------------------------------------------------------------------------

def analyze_window(combo_id: int, w_name: str, w_start: str, w_end: str,
                   output_dir: Path, funding_data: dict) -> dict:
    """Analyze one (combo, window) result. Returns per-window stats dict."""
    result_dir = output_dir / f"combo_{combo_id}_{w_name}"
    stats_path  = result_dir / 'stats.json'
    trades_path = result_dir / 'trades.csv'
    duration_years = _duration_years(w_start, w_end)

    # Load stats.json
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

    # Liquidation from stats: balance essentially wiped
    liq_from_stats = (finishing_balance is not None and finishing_balance < 10.0)

    # Load trades for funding adjustment and liquidation check
    trades = load_trades(trades_path)

    liq_from_trades = False
    cagr_adj = cagr_raw  # fallback if no trades
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

            # Liquidation: single trade with full margin wipeout
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
        # Tier gates filled in later after baseline is known
        'tier1': not liquidated,
        'tier2': n_positive == 8 and n_complete == 8,
        'tier3': False,  # filled after baseline
        'tier4': False,  # filled after baseline
        'tier_pass': False,
        'cagr_rank': 9999,
        'safe_rank':  9999,
        'safety_score': None,
        'window_stats_json': json.dumps(window_results, default=str),
    }


# ---------------------------------------------------------------------------
# Baseline identification
# ---------------------------------------------------------------------------

def find_baseline(results: list[dict]) -> dict | None:
    for r in results:
        if all(abs(float(r.get(k, 0)) - float(v)) < 0.001
               for k, v in COMBO_18_HP.items()):
            return r
    return None


# ---------------------------------------------------------------------------
# Tier gates + ranking
# ---------------------------------------------------------------------------

def apply_tiers_and_ranking(results: list[dict], baseline: dict | None) -> None:
    baseline_mean_cagr = baseline['mean_cagr'] if baseline and baseline['mean_cagr'] is not None else 0.0
    baseline_std_cagr  = baseline['std_cagr']  if baseline and baseline['std_cagr']  is not None else float('inf')

    for r in results:
        mc = r['mean_cagr']
        sc = r['std_cagr']
        t3 = (mc is not None and baseline_mean_cagr is not None and mc > baseline_mean_cagr)
        t4 = (sc is not None and baseline_std_cagr  is not None and
              baseline_std_cagr > 0 and sc < baseline_std_cagr * 1.2)
        r['tier3'] = t3
        r['tier4'] = t4
        r['tier_pass'] = r['tier1'] and r['tier2'] and t3 and t4

    # CAGR rank (mean_cagr DESC, None last)
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    for rank, r in enumerate(cagr_sorted, 1):
        r['cagr_rank'] = rank

    # Safety score + Safe rank
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
        'window_stats_json',
    ]
    csv_path = output_dir / 'v4_all_combos.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"Written: {csv_path} ({len(results)} rows)")
    return csv_path


def write_summary(results: list[dict], baseline: dict | None, output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    tier_pass   = [r for r in valid if r['tier_pass']]
    tier1_only  = [r for r in valid if r['tier1']]
    tier12      = [r for r in valid if r['tier1'] and r['tier2']]
    all_8_pos   = [r for r in valid if r['n_positive'] == 8]

    lines = [
        '# v4 Sweet Spot 최적화 집계 결과',
        '',
        f'총 {len(results)}개 combo | 분석 완료(≥6윈도우): {len(valid)}개',
        f'Tier1(무청산): {len(tier1_only)} | Tier1+2(무청산+전체양수): {len(tier12)} | Tier1-4전체: {len(tier_pass)}',
        '',
        '> SL 없음(sl_margin_pct=0.0) — 3x 레버리지, 가격 -33.3% = 마진 청산 위험',
        '> 펀딩비: 2019-09-10 이전(W1·W2 일부) funding=0 처리 (과대평가 한계)',
        '> 독립 윈도우: 각 window balance=10,000 시작 (복리 경로 독립, 진짜 OOS)',
        '',
    ]

    # Baseline
    if baseline:
        mc = baseline.get('mean_cagr')
        sc = baseline.get('std_cagr')
        np_ = baseline.get('n_positive')
        ww  = baseline.get('worst_window')
        liq = baseline.get('liquidated')
        lines += [
            '## Baseline — combo_18 (st=2.5/p=6/fe=7/se=20/de=200/atr=3.0)',
            '',
            f'| mean_cagr_adj% | std_cagr% | n_positive | worst_window% | worst_mdd% | liquidated | Tier1 | Tier2 |',
            f'|---|---|---|---|---|---|---|---|',
            f'| {mc:.2f} | {sc:.2f} | {np_}/8 | {ww:.1f} | {baseline.get("worst_mdd", 0):.1f} | {liq} | {baseline["tier1"]} | {baseline["tier2"]} |',
            '',
        ]
    else:
        lines.append('> ⚠️ Baseline (combo_18) not found in results — HP mismatch or backtest failed.\n')

    # CAGR Top-20
    lines += [
        '## CAGR Top-20 (mean_cagr_adj DESC)',
        '',
        '| CAGR# | id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | T1 | T2 | T3 | T4 | pass |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|',
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
            f"{r['worst_window']:.1f} | {t1} | {t2} | {t3} | {t4} | {tp} |"
        )

    lines += ['', '## Safe Top-20 (safety_score DESC)', '',
              '| Safe# | id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | safety | T1 | T2 | T3 | T4 | pass |',
              '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
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
            f"{r['worst_window']:.1f} | {r['safety_score']:.1f} | {t1} | {t2} | {t3} | {t4} | {tp} |"
        )

    # Parameter summary
    lines += ['', '## 파라미터별 mean_cagr 평균 (valid combos)', '']
    param_keys = ['st_factor', 'st_period', 'fast_ema_len', 'slow_ema_len', 'direction_ema_len', 'atr_mult']
    for pk in param_keys:
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

    # Statistics
    all_mc = [r['mean_cagr'] for r in valid]
    lines += [
        '## 통계',
        '',
        f'- mean_cagr 최대: {max(all_mc):.2f}% / 최소: {min(all_mc):.2f}% / 평균: {mean(all_mc):.2f}%',
        f'- 청산 발생 combo: {sum(1 for r in valid if r["liquidated"])}/{len(valid)}',
        f'- n_positive=8 (전구간 양수): {len(all_8_pos)}/{len(valid)}',
        f'- Tier1(무청산): {len(tier1_only)}/{len(valid)}',
        f'- Tier1+2: {len(tier12)}/{len(valid)}',
        f'- Tier1-4 전부(tier_pass): {len(tier_pass)}/{len(valid)}',
    ]

    md_path = output_dir / 'v4_summary.md'
    md_path.write_text('\n'.join(lines) + '\n')
    print(f"Summary: {md_path}")
    return md_path


def write_verdict(results: list[dict], baseline: dict | None, output_dir: Path) -> Path:
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    cagr_sorted = sorted(valid, key=lambda r: r['mean_cagr'], reverse=True)
    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    cagr_top10_ids = {r['combo_id'] for r in cagr_sorted[:10]}
    safe_top10_ids = {r['combo_id'] for r in safe_sorted[:10]}
    intersection   = cagr_top10_ids & safe_top10_ids
    all_8_pos   = [r for r in valid if r['n_positive'] == 8]
    tier_pass   = [r for r in valid if r['tier_pass']]
    bmc = baseline['mean_cagr'] if baseline and baseline['mean_cagr'] is not None else None
    bsc = baseline['std_cagr']  if baseline and baseline['std_cagr']  is not None else None

    # Q1: similar/higher mean_cagr + lower std
    q1_combos = []
    if bmc is not None and bsc is not None:
        q1_combos = [r for r in valid
                     if r['mean_cagr'] >= bmc and r['std_cagr'] < bsc
                     and r['combo_id'] != (baseline['combo_id'] if baseline else -1)]

    lines = [
        '# 18 — Supertrend 4h Long 3x v4 Sweet Spot Verdict',
        '',
        f'*Generated: {datetime.now(timezone.utc).isoformat()}*',
        '',
        '## 요약',
        '',
        f'- 그리드: 216 combos (st_factor×3, st_period×3, fast_ema×2, slow_ema×2, dir_ema×2, atr×3)',
        f'- 윈도우: 8 독립 (2017-08-18~2026-04-30 동등 분할, balance=10,000 각)',
        f'- SL: 없음 (sl_margin_pct=0.0) — 실제 운영 combo_18과 동일 조건',
        f'- 분석 combo: {len(valid)}/{len(results)} (≥6 윈도우 완료)',
        '',
        '---',
        '',
        '## Baseline — combo_18 (in-grid)',
        '',
    ]

    if baseline:
        lines += [
            f'| 항목 | 값 |',
            f'|---|---|',
            f'| HP | st={baseline["st_factor"]}/p={baseline["st_period"]}/fe={baseline["fast_ema_len"]}/se={baseline["slow_ema_len"]}/de={baseline["direction_ema_len"]}/atr={baseline["atr_mult"]} |',
            f'| combo_id | {baseline["combo_id"]} |',
            f'| mean_cagr_adj | {baseline["mean_cagr"]:.2f}% |',
            f'| std_cagr | {baseline["std_cagr"]:.2f}% |',
            f'| n_positive | {baseline["n_positive"]}/8 |',
            f'| worst_window | {baseline["worst_window"]:.1f}% |',
            f'| worst_mdd | {baseline["worst_mdd"]:.1f}% |',
            f'| liquidated | {baseline["liquidated"]} |',
            f'| Tier1 | {baseline["tier1"]} |',
            f'| Tier2 | {baseline["tier2"]} |',
            f'| CAGR rank | #{baseline["cagr_rank"]} |',
            f'| Safe rank | #{baseline["safe_rank"]} |',
        ]
    else:
        lines.append('> ⚠️ combo_18 백테스트 실패 또는 HP 불일치 — baseline 비교 불가\n')

    lines += ['', '---', '', '## 4대 핵심 질문', '']

    # Q1
    lines += [
        '### Q1. combo_18보다 mean_cagr ≥ baseline AND std_cagr < baseline인 combo 존재?',
        '',
    ]
    if bmc is None:
        lines.append('> baseline 없음 — 비교 불가\n')
    elif q1_combos:
        lines.append(f'**YES — {len(q1_combos)}개 combo 발견**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(q1_combos, key=lambda x: x['mean_cagr'], reverse=True)[:10]:
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | "
                f"#{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append(f'**NO — baseline(mean={bmc:.1f}%, std={bsc:.1f}%) 동시 개선 combo 없음**')
    lines.append('')

    # Q2
    lines += [
        '### Q2. 8 윈도우 전부 양수인 combo 존재?',
        '',
    ]
    if all_8_pos:
        lines.append(f'**YES — {len(all_8_pos)}개**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | worst_W% | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(all_8_pos, key=lambda x: x['mean_cagr'], reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['worst_window']:.1f} | "
                f"#{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append('**NO — 8 윈도우 전부 양수인 combo 없음**')
    lines.append('')

    # Q3
    lines += [
        '### Q3. CAGR-top10 ∩ Safe-top10 교집합 combo 존재?',
        '',
    ]
    if intersection:
        lines.append(f'**YES — {len(intersection)}개**: combo_id {sorted(intersection)}')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | CAGR# | Safe# | tier_pass |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|']
        for cid in sorted(intersection):
            r = next(x for x in valid if x['combo_id'] == cid)
            tp = '✅' if r['tier_pass'] else '❌'
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | "
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
        f'- Tier3(mean_cagr>baseline): {sum(1 for r in valid if r["tier3"])}/{len(valid)}',
        f'- Tier4(std_cagr<baseline×1.2): {sum(1 for r in valid if r["tier4"])}/{len(valid)}',
        f'- **Tier1-4 전부**: {len(tier_pass)}/{len(valid)}',
        '',
    ]
    if tier_pass:
        lines.append(f'**ROBUST winner {len(tier_pass)}개:**')
        lines += ['',
                  '| combo_id | st_f | st_p | fe | se | de | atr | mean_cagr% | std% | n_pos | worst_W% | CAGR# | Safe# |',
                  '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
        for r in sorted(tier_pass, key=lambda x: x['mean_cagr'], reverse=True):
            lines.append(
                f"| {r['combo_id']} | {r['st_factor']} | {r['st_period']} | "
                f"{r['fast_ema_len']} | {r['slow_ema_len']} | {r['direction_ema_len']} | "
                f"{r['atr_mult']} | {r['mean_cagr']:.1f} | {r['std_cagr']:.1f} | "
                f"{r['n_positive']}/8 | {r['worst_window']:.1f} | "
                f"#{r['cagr_rank']} | #{r['safe_rank']} |"
            )
    else:
        lines.append('**0개 — ROBUST winner 없음. combo_18 유지 권고.**')
    lines.append('')

    # Decision
    lines += ['---', '', '## 결정 분기', '']
    if tier_pass:
        winner = sorted(tier_pass, key=lambda x: x['mean_cagr'], reverse=True)[0]
        lines += [
            f'**→ WINNER 채택**: combo_id={winner["combo_id"]} '
            f'(st={winner["st_factor"]}/p={winner["st_period"]}/fe={winner["fast_ema_len"]}/'
            f'se={winner["slow_ema_len"]}/de={winner["direction_ema_len"]}/atr={winner["atr_mult"]})',
            '',
            f'mean_cagr={winner["mean_cagr"]:.1f}% / std={winner["std_cagr"]:.1f}% / '
            f'n_pos={winner["n_positive"]}/8 / worst_W={winner["worst_window"]:.1f}%',
            '',
            '**Phase 5 게이트**: worst_mdd 및 운영 안정성 검토 후 Phase 5 재판정 필요.',
        ]
    elif intersection:
        cid = sorted(intersection)[0]
        r   = next(x for x in valid if x['combo_id'] == cid)
        lines += [
            f'**→ 차선: CAGR∩Safe 교집합 combo_id={cid}** '
            f'(Tier 일부 미충족이나 양쪽 랭킹 상위)',
            '',
            '**combo_18 유지 권고** (ROBUST 조건 미충족). 추가 검토 후 결정.',
        ]
    else:
        lines += [
            '**→ combo_18 유지**: 그리드 내 combo_18 대비 우월한 robust winner 없음.',
            '',
            '**Phase 5**: combo_18 기준 재판정. Safe-top10 검토 후 참고 운영 가능.',
        ]

    lines += [
        '',
        '---',
        '',
        '## 한계 및 주의',
        '',
        '- W1(2017-08~2018-09), W2(2018-09~2019-10 일부) 구간: 펀딩비 데이터 없음 → CAGR 과대평가',
        '- SL 없음 조건: 단일 트레이드 -33.3% 가격 하락 시 마진 전액 손실 가능',
        '- 독립 윈도우: 복리 누적 효과 미반영 (보수적 추정)',
        '- 백테스트 시뮬레이션 vs 실제 슬리피지/유동성 차이 존재',
    ]

    verdict_path = output_dir / '18_SUPERTREND_V4_SWEET_SPOT_VERDICT.md'
    verdict_path.write_text('\n'.join(lines) + '\n')
    print(f"Verdict: {verdict_path}")
    return verdict_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--db', type=str, default='/result/v4_optimization/queue.sqlite3')
    p.add_argument('--output-dir', type=str, default='/result/v4_optimization')
    args = p.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load all distinct combos from DB
    conn = sqlite3.connect(args.db, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT DISTINCT combo_id, st_factor, st_period, fast_ema_len, slow_ema_len, "
        "direction_ema_len, atr_mult, sl_margin_pct "
        "FROM jobs ORDER BY combo_id"
    ).fetchall()
    conn.close()

    combos = [dict(r) for r in rows]
    print(f"Processing {len(combos)} combos × 8 windows...")

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

    baseline = find_baseline(results)
    if baseline:
        print(f"Baseline (combo_18) = combo_id={baseline['combo_id']} "
              f"mean_cagr={baseline['mean_cagr']:.2f}% std={baseline['std_cagr']:.2f}% "
              f"n_pos={baseline['n_positive']}/8")
    else:
        print("[WARN] combo_18 not found in results")

    apply_tiers_and_ranking(results, baseline)

    write_csv(results, output_dir)
    write_summary(results, baseline, output_dir)
    write_verdict(results, baseline, output_dir)

    # Quick console summary
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]
    tier_pass = [r for r in valid if r['tier_pass']]
    print(f"\n=== v4 결과 요약 ===")
    print(f"유효 combo: {len(valid)}/{len(results)}")
    print(f"청산 발생: {sum(1 for r in valid if r['liquidated'])}")
    print(f"n_positive=8: {sum(1 for r in valid if r['n_positive']==8)}")
    print(f"Tier1-4 ROBUST: {len(tier_pass)}")
    if tier_pass:
        for r in sorted(tier_pass, key=lambda x: x['mean_cagr'], reverse=True):
            print(f"  combo={r['combo_id']} mean_cagr={r['mean_cagr']:.1f}% "
                  f"std={r['std_cagr']:.1f}% n_pos={r['n_positive']}/8")

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
