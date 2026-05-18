#!/usr/bin/env python3
"""
Worker 결과를 집계해 equity_stop_results.json + equity_stop_report.md 생성.

실행:
  python3 _aggregate_results.py
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
WORKERS_DIR = SCRIPT_DIR / 'workers'

LEVERAGE     = 3
MAINT_MARGIN = 0.005
LIQ_RATIO    = 1 - (1 / LEVERAGE - MAINT_MARGIN)

BACKTEST_START = '2019-01-01'
BACKTEST_END   = '2026-04-30'


def load_workers() -> dict:
    """workers/*.json 읽어서 {combo_id: {variant: data}} 구조로 반환."""
    combined: dict[str, dict] = {}
    files = sorted(WORKERS_DIR.glob('w*.json'))
    print(f'[집계] worker 파일 {len(files)}개 발견')
    for f in files:
        data = json.loads(f.read_text())
        cid  = data['combo_id']
        if cid not in combined:
            combined[cid] = {}
        for vname, vdata in data['results'].items():
            combined[cid][vname] = vdata
        print(f'  {f.name}: {cid} / {list(data["results"].keys())}')
    return combined


def write_report(combined: dict, path: Path):
    LEVERAGE_VAL = LEVERAGE
    stop_labels = {
        'no_stop':    '원본 (no stop)',
        'eq_stop_70': 'Equity Stop −70%',
        'eq_stop_75': 'Equity Stop −75%',
        'eq_stop_80': 'Equity Stop −80%',
    }
    stop_prices = {
        'eq_stop_70': f'entry × {1 - 0.70/LEVERAGE_VAL:.4f}',
        'eq_stop_75': f'entry × {1 - 0.75/LEVERAGE_VAL:.4f}',
        'eq_stop_80': f'entry × {1 - 0.80/LEVERAGE_VAL:.4f}',
    }

    lines = [
        '# Equity Stop Backtest Report — SupertrendStrategy 4h 3x Long-Only',
        '',
        f'**분석 기간**: {BACKTEST_START} ~ {BACKTEST_END}',
        f'**레버리지**: {LEVERAGE_VAL}x isolated',
        f'**liq 임계**: entry × {LIQ_RATIO:.4f} (진입가 대비 −{LIQ_RATIO*100:.2f}%)',
        '',
        '| Variant | stop_price | equity 손실 |',
        '|---------|-----------|------------|',
        '| no_stop    | — | 기존 ATR stop + EMA exit |',
        f'| eq_stop_70 | {stop_prices["eq_stop_70"]} | −70% |',
        f'| eq_stop_75 | {stop_prices["eq_stop_75"]} | −75% |',
        f'| eq_stop_80 | {stop_prices["eq_stop_80"]} | −80% |',
        '',
        '> ※ Jesse 시뮬레이션은 bar-close 기준. intrabar 실거래와 차이 있음.',
        '',
        '---',
        '',
    ]

    for cid in sorted(combined.keys()):
        cdata = combined[cid]
        lines += [f'## {cid}', '', '### 지표 비교', '',
                  '| Variant | CAGR | MDD | Sharpe | Trades | Wins | PF | Stop발동 |',
                  '|---------|------|-----|--------|--------|------|-----|---------|']

        for vname in ['no_stop', 'eq_stop_70', 'eq_stop_75', 'eq_stop_80']:
            vdata = cdata.get(vname, {})
            m     = vdata.get('metrics', {})
            n_ev  = len(vdata.get('stop_events', []))
            stops = f'{n_ev}건' if vname != 'no_stop' else '—'
            lines.append(
                f'| {stop_labels.get(vname, vname)} | {m.get("cagr_pct", "?"):+.1f}% | '
                f'{m.get("mdd_pct", "?"):.1f}% | {m.get("sharpe", "?"):.3f} | '
                f'{m.get("trades", "?")} | {m.get("win_rate", "?"):.1f}% | '
                f'{m.get("pf", "?"):.3f} | {stops} |'
            )

        lines += ['']

        for vname in ['eq_stop_70', 'eq_stop_75', 'eq_stop_80']:
            events = cdata.get(vname, {}).get('stop_events', [])
            if not events:
                lines += [f'### {stop_labels.get(vname, vname)}: stop 발동 없음', '']
                continue

            helped = sum(1 for e in events if e['outcome'] in ('HELPED', 'PREVENTED_LIQ'))
            hurt   = sum(1 for e in events if e['outcome'] == 'HURT')
            prev   = sum(1 for e in events if e['outcome'] == 'PREVENTED_LIQ')

            lines += [
                f'### {stop_labels.get(vname, vname)}: 총 {len(events)}건 발동',
                f'- HELPED (stop이 더 큰 손실 방지): **{helped}건**',
                f'- HURT (stop 이후 반등 → 성과 저하): **{hurt}건**',
                f'- PREVENTED_LIQ (liq 도달 방지): **{prev}건**',
                '',
                '| # | 진입일 | stop 발동일 | stop_exit | no_stop_exit | liq_price | 결과 |',
                '|---|--------|------------|----------|-------------|----------|------|',
            ]
            for i, e in enumerate(events, 1):
                lines.append(
                    f'| {i} | {e["trade_entry_dt"][:10]} | {e["stop_triggered_dt"][:10]} | '
                    f'${e["stop_exit_price"]:,.0f} | ${e["no_stop_exit_price"]:,.0f} | '
                    f'${e["liq_price"]:,.0f} | **{e["outcome"]}** |'
                )
            lines += ['']

        lines += ['']

    # 권장
    lines += [
        '---', '', '## 종합 권장 임계값', '',
        '| 임계값 | MDD 개선 효과 | CAGR 영향 | 판정 |',
        '|--------|------------|----------|------|',
        '| −70% | 가장 큰 MDD 개선 | CAGR 손실 가능 | 공격적 보호 |',
        '| −75% | 중간 MDD 개선 | CAGR 소폭 영향 | **균형** |',
        '| −80% | 소폭 MDD 개선 | CAGR 거의 무영향 | 최소 보호 |',
        '',
        '> 위 지표 비교 테이블의 실제 결과로 판단하세요.',
        '',
        f'*생성: {datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}*',
    ]

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'  → {path.name}')


def main():
    combined = load_workers()

    output = {
        'leverage':        LEVERAGE,
        'maint_margin':    MAINT_MARGIN,
        'liq_ratio':       LIQ_RATIO,
        'analysis_period': f'{BACKTEST_START} ~ {BACKTEST_END}',
        'generated_at':    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'results':         combined,
    }

    out_json = SCRIPT_DIR / 'equity_stop_results.json'
    out_md   = SCRIPT_DIR / 'equity_stop_report.md'

    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f'  → {out_json.name}')

    write_report(combined, out_md)
    print('\n집계 완료.')


if __name__ == '__main__':
    main()
