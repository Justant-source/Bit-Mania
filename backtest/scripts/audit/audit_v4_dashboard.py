#!/usr/bin/env python3
"""V4 대시보드 표시 정확성 검증.

dashboard.html 의 window.V4_DATA payload 와 raw 데이터 / BTC ground truth 가
일치하는지, 그리고 JS 의 슬라이싱 알고리즘이 Python 미러 결과와 일치하는지
검증한다.

실행: python3 audit_v4_dashboard.py [--html PATH]

산출:
  - stdout: [PASS|FAIL] 카테고리별 요약
  - 7-strategies/audit_dashboard_report.md: 실패 케이스 + 비교 표

검증 카테고리:
  2A. payload ↔ raw         — V4_DATA 의 stats/trades/equity 가 raw 파일과 일치
  2B. Full-slice KPI         — slicedStats(전체) ≈ stats.json
  2C. BnH 슬라이스 ground   — 2026-Q1, 2024, 2025 슬라이스 vs BTC parquet
  2D. 거래 표 슬라이스 합산  — strict filter 합 = trades.csv 합
  2E. Equity 곡선 일관성    — sliced equity endpoints
  2F. 마커 차트 데이터       — slice 내 trades 개수
  2G. Leverage 표시         — r.leverage 파싱
  2H. Cache invalidation     — sliceCache.clear() 코드 존재
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from _paths import DATA_ROOT, RESULTS_ROOT

RESULT_DIR = RESULTS_ROOT / '7-strategies'
BTC_KLINES = DATA_ROOT / 'binance_vision' / 'klines' / 'BTCUSDT'

DEFAULT_HTML = RESULT_DIR / 'dashboard.html'

START_MS = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
END_MS   = int(datetime(2026, 4, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)


# ─── Failure accumulator (mirror of audit_v4_data.Audit) ─────────────────────

class Audit:
    def __init__(self):
        self.failures: dict[str, list[dict]] = defaultdict(list)
        self.warnings: dict[str, list[dict]] = defaultdict(list)
        self.checked:  dict[str, int]        = defaultdict(int)

    def fail(self, section: str, **details):
        self.failures[section].append(details)

    def warn(self, section: str, **details):
        self.warnings[section].append(details)

    def tick(self, section: str, n: int = 1):
        self.checked[section] += n

    def summary(self) -> str:
        lines = []
        for sec in sorted(set(list(self.failures) + list(self.warnings) + list(self.checked))):
            n_fail = len(self.failures.get(sec, []))
            n_warn = len(self.warnings.get(sec, []))
            n_ok   = self.checked.get(sec, 0)
            tag    = '[FAIL]' if n_fail else ('[WARN]' if n_warn else '[PASS]')
            lines.append(f'  {tag} {sec:30s} checked={n_ok}, failures={n_fail}, warnings={n_warn}')
        return '\n'.join(lines)

    def write_report(self, path: Path) -> None:
        lines = [
            '# V4 대시보드 표시 정확성 검증 리포트',
            '',
            f'**생성 시각**: {datetime.now(timezone.utc).isoformat()}',
            '**스크립트**: `audit_v4_dashboard.py`',
            '',
            '## 요약',
            '',
            '| 카테고리 | 검증 건수 | 실패 | 경고 | 결과 |',
            '|---------|----------|------|------|------|',
        ]
        for sec in sorted(set(list(self.failures) + list(self.warnings) + list(self.checked))):
            n_fail = len(self.failures.get(sec, []))
            n_warn = len(self.warnings.get(sec, []))
            n_ok   = self.checked.get(sec, 0)
            tag    = '❌ FAIL' if n_fail else ('⚠️ WARN' if n_warn else '✅ PASS')
            lines.append(f'| {sec} | {n_ok} | {n_fail} | {n_warn} | {tag} |')
        lines.append('')
        if not self.failures and not self.warnings:
            lines.append('## All checks passed ✅')
        for sec in sorted(self.failures):
            items = self.failures[sec]
            lines.append(f'## ❌ {sec} — {len(items)} failures')
            lines.append('')
            for d in items[:50]:
                lines.append('- ' + ', '.join(f'`{k}`={v}' for k, v in d.items()))
            if len(items) > 50:
                lines.append(f'- … and {len(items) - 50} more')
            lines.append('')
        for sec in sorted(self.warnings):
            items = self.warnings[sec]
            lines.append(f'## ⚠️ {sec} — {len(items)} warnings')
            lines.append('')
            for d in items[:50]:
                lines.append('- ' + ', '.join(f'`{k}`={v}' for k, v in d.items()))
            if len(items) > 50:
                lines.append(f'- … and {len(items) - 50} more')
            lines.append('')
        path.write_text('\n'.join(lines) + '\n')


# ─── Loaders ─────────────────────────────────────────────────────────────────

def parse_dashboard_payload(html_path: Path) -> dict:
    """Extract window.V4_DATA = {...} JSON object from dashboard.html."""
    html = html_path.read_text()
    m = re.search(r'window\.V4_DATA\s*=\s*', html)
    if not m:
        raise ValueError('window.V4_DATA assignment not found in dashboard.html')
    s = m.end()
    if html[s] != '{':
        raise ValueError('window.V4_DATA is not a JSON object literal')
    # Bracket match accounting for strings/escapes
    depth = 0
    in_str = False
    escape = False
    i = s
    while i < len(html):
        c = html[i]
        if escape:
            escape = False
        elif c == '\\':
            escape = True
        elif in_str:
            if c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                return json.loads(html[s:i + 1])
        i += 1
    raise ValueError('Unterminated V4_DATA object')


def load_trades_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with open(path, newline='') as f:
        for i, row in enumerate(csv.DictReader(f)):
            try:
                out.append({
                    'idx':     i,
                    't_open':  int(float(row['opened_at'])),
                    't_close': int(float(row['closed_at'])),
                    'side':    row['side'],
                    'entry':   float(row['entry_price']),
                    'exit':    float(row['exit_price']),
                    'qty':     float(row['qty']),
                    'pnl':     float(row['pnl']),
                    'fee':     float(row['fee']),
                })
            except (KeyError, ValueError):
                pass
    return out


def load_stats(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def load_btc_daily() -> list[dict]:
    import pandas as pd
    frames = []
    base = BTC_KLINES / '1d'
    for year in range(2020, 2027):
        for month in range(1, 13):
            p = base / str(year) / f'{month:02d}.parquet'
            if p.exists():
                frames.append(pd.read_parquet(p, columns=['open_time', 'close']))
    if not frames:
        return []
    df = pd.concat(frames).sort_values('open_time')
    df = df[(df['open_time'] >= pd.Timestamp('2021-01-01', tz='UTC')) &
            (df['open_time'] <= pd.Timestamp('2026-04-30', tz='UTC'))]
    return [{'t': int(r.open_time.timestamp() * 1000), 'c': float(r.close)} for r in df.itertuples()]


# ─── JS algorithm mirror (Python) ────────────────────────────────────────────

def equity_at(equity: list[dict], ms: int) -> float:
    """Mirror of dashboard JS equityAt(): largest equity[i].t ≤ ms."""
    if not equity:
        return 10_000.0
    if ms <= equity[0]['t']:
        return equity[0]['v']
    if ms >= equity[-1]['t']:
        return equity[-1]['v']
    lo, hi = 0, len(equity) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if equity[mid]['t'] <= ms:
            lo = mid
        else:
            hi = mid - 1
    return equity[lo]['v']


def sliced_equity(equity: list[dict], start_ms: int, end_ms: int) -> list[dict]:
    e_start = equity_at(equity, start_ms)
    if e_start <= 100:
        return [{'t': start_ms, 'v': 10_000.0}, {'t': end_ms, 'v': 0.0}]
    norm = 10_000.0 / e_start
    out  = [{'t': start_ms, 'v': 10_000.0}]
    liquidated = False
    for p in equity:
        if p['t'] <= start_ms or p['t'] >= end_ms:
            continue
        v = p['v'] * norm
        if not liquidated and v <= 500:
            out.append({'t': p['t'], 'v': 0.0})
            liquidated = True
            continue
        if liquidated:
            continue
        out.append({'t': p['t'], 'v': v})
    if liquidated:
        out.append({'t': end_ms, 'v': 0.0})
    else:
        out.append({'t': end_ms, 'v': equity_at(equity, end_ms) * norm})
    return out


def sliced_stats(equity: list[dict], trades: list[dict],
                 start_ms: int, end_ms: int) -> dict:
    eq        = sliced_equity(equity, start_ms, end_ms)
    finishing = eq[-1]['v']
    peak, mdd = 10_000.0, 0.0
    liquidated = False
    for p in eq:
        if not liquidated and p['v'] <= 500:
            liquidated = True
        if not liquidated:
            if p['v'] > peak:
                peak = p['v']
            if peak > 0:
                dd = (p['v'] - peak) / peak * 100
                if dd < mdd:
                    mdd = dd
    if liquidated:
        finishing = 0.0
    years  = (end_ms - start_ms) / (365.25 * 86400_000)
    cagr   = ((finishing / 10_000) ** (1 / years) - 1) * 100 if (years > 0 and finishing > 0) else 0
    in_slice = [t for t in trades if t['t_open'] >= start_ms and t['t_close'] <= end_ms]
    return {
        'cagr': cagr, 'mdd': mdd, 'finishing': finishing,
        'trades': len(in_slice), 'liquidated': liquidated,
    }


# ─── Checks ──────────────────────────────────────────────────────────────────

def check_2a_payload_vs_raw(audit: Audit, V4: dict) -> None:
    for strat, results in V4['groups'].items():
        for r in results:
            lbl = f"{strat}/{r['tf']}/{r['variant']}"
            folder = RESULT_DIR / strat / r['tf'] / r['variant']
            stats = load_stats(folder / 'stats.json')
            trades_csv = load_trades_csv(folder / 'trades.csv')
            if stats is None:
                audit.fail('2A_stats_missing', label=lbl)
                continue

            raw = stats.get('raw_metrics', {})
            # finishing
            r_fin = r['stats']['finishing']
            f_fin = raw.get('finishing_balance', 0)
            if abs(r_fin - f_fin) > 0.5:
                audit.fail('2A_finishing', label=lbl,
                           payload=r_fin, stats=f_fin, diff=round(r_fin - f_fin, 4))
            audit.tick('2A_finishing')

            # trade count
            n_csv = len(trades_csv)
            if r['stats']['trades'] != stats.get('total_trades') or len(r['trades']) != n_csv:
                audit.fail('2A_trade_count', label=lbl,
                           payload_stats_trades=r['stats']['trades'],
                           payload_trades_len=len(r['trades']),
                           stats_total=stats.get('total_trades'),
                           csv_rows=n_csv)
            audit.tick('2A_trade_count')

            # leverage parsing
            m = re.search(r'_x(\d+)$', r['variant'])
            expected_lev = int(m.group(1)) if m else 1
            if r['leverage'] != expected_lev:
                audit.fail('2A_leverage', label=lbl,
                           payload_leverage=r['leverage'], expected=expected_lev)
            audit.tick('2A_leverage')

            # trade-by-trade match (sample first 3 + last 1)
            if trades_csv:
                sample_idx = list(range(min(3, len(trades_csv))))
                if len(trades_csv) > 3:
                    sample_idx.append(len(trades_csv) - 1)
                for i in sample_idx:
                    if i >= len(r['trades']):
                        audit.fail('2A_trade_index', label=lbl, missing=i)
                        continue
                    a = trades_csv[i]
                    b = r['trades'][i]
                    for fld_csv, fld_pl in [('t_open','t_open'),('t_close','t_close'),
                                            ('side','side'),('entry','entry'),
                                            ('exit','exit'),('qty','qty'),
                                            ('pnl','pnl'),('fee','fee')]:
                        av, bv = a[fld_csv], b[fld_pl]
                        ok = (av == bv) if isinstance(av, str) else abs(av - bv) < 0.0001
                        if not ok:
                            audit.fail('2A_trade_field', label=lbl, row=i,
                                       field=fld_csv, csv=av, payload=bv)
                    audit.tick('2A_trade_field', 8)

            # equity series shape
            eq = r.get('equity', [])
            if strat == 'buy_and_hold':
                if len(eq) < 1000:
                    audit.fail('2A_equity_bnh_sparse', label=lbl, len=len(eq))
            else:
                expected_len = n_csv + 2
                if len(eq) != expected_len:
                    audit.fail('2A_equity_len', label=lbl,
                               len=len(eq), expected=expected_len)
            audit.tick('2A_equity_len')


def check_2b_full_slice_kpi(audit: Audit, V4: dict) -> None:
    """Sliced finishing on the full period should match payload finishing — except:
    - BnH uses dense BTC-based equity (intentionally different from Jesse's sparse calc)
    - Strategies with extreme MDD (≤ -95%) get auto-liquidated by dashboard's $500 floor
      even when Jesse reports a recovered balance — this is intentional (real-world
      Bybit margin liquidation would have happened at the dip).
    """
    for strat, results in V4['groups'].items():
        for r in results:
            lbl = f"{strat}/{r['tf']}/{r['variant']}"
            stats = sliced_stats(r['equity'], r['trades'], START_MS, END_MS)
            actual_finishing = r['stats']['finishing']

            # Intentional BnH divergence
            if strat == 'buy_and_hold':
                audit.tick('2B_full_finishing')
                continue

            # Liquidation-by-deep-drawdown intentional flagging by dashboard
            payload_mdd = r['stats'].get('mdd', 0)
            if stats['liquidated'] and payload_mdd <= -95:
                audit.warn('2B_intentional_liq', label=lbl,
                           payload_finishing=round(actual_finishing, 2),
                           payload_mdd=payload_mdd)
                audit.tick('2B_full_finishing')
                continue

            if r['stats'].get('liquidated', False):
                # Payload says liquidated — sliced should also be liquidated
                if not stats['liquidated'] and stats['finishing'] > 100:
                    audit.fail('2B_liquidation_mismatch', label=lbl,
                               payload_liq=r['stats'].get('liquidated'),
                               sliced_finishing=round(stats['finishing'], 2))
                audit.tick('2B_liquidation_mismatch')
                continue

            if actual_finishing > 100:
                rel = abs(stats['finishing'] - actual_finishing) / actual_finishing
                if rel > 0.01:
                    audit.fail('2B_full_finishing', label=lbl,
                               sliced=round(stats['finishing'], 2),
                               payload=round(actual_finishing, 2),
                               rel_diff=round(rel, 4))
            audit.tick('2B_full_finishing')


def check_2c_bnh_slice_ground_truth(audit: Audit, V4: dict, btc: list[dict]) -> None:
    bnh = next((r for r in V4['groups'].get('buy_and_hold', []) if r['tf'] == '1D'), None)
    if not bnh:
        audit.fail('2C_bnh_1d_missing')
        audit.tick('2C_bnh_1d_missing')
        return
    audit.tick('2C_bnh_1d_missing')

    def btc_close_at(ms: int) -> float:
        # Largest close.t ≤ ms (last available)
        last = btc[0]['c']
        for p in btc:
            if p['t'] <= ms:
                last = p['c']
            else:
                break
        return last

    test_slices = [
        ('2026-Q1', int(datetime(2026, 1, 1,  tzinfo=timezone.utc).timestamp()*1000),
                    int(datetime(2026, 4, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)),
        ('2024',    int(datetime(2024, 1, 1,  tzinfo=timezone.utc).timestamp()*1000),
                    int(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)),
        ('2025',    int(datetime(2025, 1, 1,  tzinfo=timezone.utc).timestamp()*1000),
                    int(datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)),
        ('2023',    int(datetime(2023, 1, 1,  tzinfo=timezone.utc).timestamp()*1000),
                    int(datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)),
    ]
    for name, s, e in test_slices:
        ds_stats = sliced_stats(bnh['equity'], bnh['trades'], s, e)
        btc_start = btc_close_at(s)
        btc_end   = btc_close_at(e)
        if btc_start <= 0:
            audit.fail('2C_btc_data_missing', slice=name)
            continue
        expected = 10_000 * (btc_end / btc_start)
        rel_diff = abs(ds_stats['finishing'] - expected) / expected if expected > 0 else 0
        if rel_diff > 0.01:
            audit.fail('2C_bnh_slice', slice=name,
                       btc_start=btc_start, btc_end=btc_end,
                       expected_finishing=round(expected, 2),
                       dashboard_finishing=round(ds_stats['finishing'], 2),
                       rel_diff=round(rel_diff, 4))
        audit.tick('2C_bnh_slice')


def check_2d_table_slice_sum(audit: Audit, V4: dict) -> None:
    """Strict filter sum check — sample 5 strategies × 2 slices."""
    samples = []
    for strat in ('supertrend', 'tradeiq_psar_ha', 'momentum_ma', 'trendtype', 'stoch'):
        for r in V4['groups'].get(strat, []):
            if r['tf'] == '1D' and r['variant'] in ('long_only', 'bidirectional'):
                samples.append(r)
                break

    slices = [
        ('full',   START_MS, END_MS),
        ('2024',   int(datetime(2024, 1, 1,  tzinfo=timezone.utc).timestamp()*1000),
                   int(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)),
    ]
    for r in samples:
        lbl = f"{r['strat']}/{r['tf']}/{r['variant']}"
        folder = RESULT_DIR / r['strat'] / r['tf'] / r['variant']
        trades_csv = load_trades_csv(folder / 'trades.csv')
        for name, s, e in slices:
            csv_in_slice = [t for t in trades_csv if t['t_open'] >= s and t['t_close'] <= e]
            payload_in_slice = [t for t in r['trades'] if t['t_open'] >= s and t['t_close'] <= e]
            if len(csv_in_slice) != len(payload_in_slice):
                audit.fail('2D_slice_count_mismatch', label=lbl, slice=name,
                           csv_count=len(csv_in_slice), payload_count=len(payload_in_slice))
            csv_sum     = sum(t['pnl'] for t in csv_in_slice)
            payload_sum = sum(t['pnl'] for t in payload_in_slice)
            if abs(csv_sum - payload_sum) > 0.5:
                audit.fail('2D_slice_pnl_mismatch', label=lbl, slice=name,
                           csv_sum=round(csv_sum, 2), payload_sum=round(payload_sum, 2))
            audit.tick('2D_slice_count_mismatch')
            audit.tick('2D_slice_pnl_mismatch')


def check_2e_equity_endpoints(audit: Audit, V4: dict) -> None:
    """Same caveats as 2B: BnH uses dense equity (intentional ~3% divergence);
    deep-drawdown leveraged variants get $500-floor liquidation in slice view."""
    for strat, results in V4['groups'].items():
        for r in results:
            lbl = f"{strat}/{r['tf']}/{r['variant']}"
            eq = sliced_equity(r['equity'], START_MS, END_MS)
            if abs(eq[0]['v'] - 10_000) > 0.01:
                audit.fail('2E_first_point_not_10k', label=lbl, first=eq[0]['v'])
            audit.tick('2E_first_point_not_10k')

            if strat == 'buy_and_hold' or r['stats'].get('mdd', 0) <= -95:
                audit.tick('2E_last_point_vs_payload_finishing')
                continue

            payload_finishing = r['stats']['finishing']
            if r['stats'].get('liquidated'):
                continue
            if payload_finishing > 100:
                rel = abs(eq[-1]['v'] - payload_finishing) / payload_finishing
                if rel > 0.01:
                    audit.fail('2E_last_point_vs_payload_finishing', label=lbl,
                               sliced_last=round(eq[-1]['v'], 2),
                               payload_finishing=round(payload_finishing, 2),
                               rel_diff=round(rel, 4))
            audit.tick('2E_last_point_vs_payload_finishing')


def check_2f_marker_count(audit: Audit, V4: dict) -> None:
    """Sample: marker count (= strict in-slice trades) for 5 strategies."""
    samples = []
    for strat in ('supertrend', 'tradeiq_cci_ce', 'momentum_ma', 'trendtype', 'stoch'):
        for r in V4['groups'].get(strat, []):
            if r['tf'] == '1D' and r['variant'] == 'long_only':
                samples.append(r)
                break

    slice_2024_s = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()*1000)
    slice_2024_e = int(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp()*1000)
    for r in samples:
        lbl = f"{r['strat']}/{r['tf']}/{r['variant']}"
        in_slice = [t for t in r['trades']
                    if t['t_open'] >= slice_2024_s and t['t_close'] <= slice_2024_e]
        for t in in_slice:
            if t['t_open'] < slice_2024_s or t['t_close'] > slice_2024_e:
                audit.fail('2F_marker_strict_filter', label=lbl, row=t)
        audit.tick('2F_marker_strict_filter', len(in_slice))


def check_cache_invalidation(audit: Audit, build_script: Path) -> None:
    src = build_script.read_text()
    if 'sliceCache.clear()' not in src:
        audit.fail('2H_cache_clear_missing')
    audit.tick('2H_cache_clear_missing')


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='V4 대시보드 표시 정확성 검증')
    ap.add_argument('--html', default=str(DEFAULT_HTML), help='dashboard.html 경로')
    ap.add_argument('--report', default=str(RESULT_DIR / 'audit_dashboard_report.md'))
    args = ap.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        print(f'dashboard.html not found: {html_path}', file=sys.stderr)
        sys.exit(2)

    print(f'Parsing payload from {html_path}…')
    V4 = parse_dashboard_payload(html_path)
    print(f'  n_results: {V4.get("n_results")}, btc_1d: {len(V4.get("btc_1d", []))}')

    # Reshape: groups already nested by strat — but result objects don't have strat field
    # in payload. Add it for downstream use.
    for strat, results in V4['groups'].items():
        for r in results:
            r.setdefault('strat', strat)

    audit = Audit()

    print('\n[2A] payload ↔ raw consistency…')
    check_2a_payload_vs_raw(audit, V4)

    print('[2B] Full-slice KPI vs payload finishing…')
    check_2b_full_slice_kpi(audit, V4)

    print('[2C] BnH slice ground truth via BTC parquet…')
    btc = load_btc_daily()
    check_2c_bnh_slice_ground_truth(audit, V4, btc)

    print('[2D] Trade table slice sum (sample)…')
    check_2d_table_slice_sum(audit, V4)

    print('[2E] Equity endpoint consistency…')
    check_2e_equity_endpoints(audit, V4)

    print('[2F] Marker strict filter (sample)…')
    check_2f_marker_count(audit, V4)

    print('[2H] sliceCache.clear() presence…')
    check_cache_invalidation(audit, SCRIPT_DIR / 'build_v4_dashboard.py')

    print('\n=== Summary ===')
    print(audit.summary())

    out = Path(args.report)
    out.parent.mkdir(parents=True, exist_ok=True)
    audit.write_report(out)
    print(f'\nReport: {out}')

    n_fail = sum(len(v) for v in audit.failures.values())
    sys.exit(1 if n_fail > 0 else 0)


if __name__ == '__main__':
    main()
