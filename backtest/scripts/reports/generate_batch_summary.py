#!/usr/bin/env python3
"""
Generate SUMMARY.md for a completed batch.
Usage: python scripts/generate_batch_summary.py --batch 1 --result-dir /result/batch_1/
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


BATCH_META = {
    1: {
        'title': 'Batch 1 — Mean Reversion / Volatility 종합',
        'strategies': ['bbpb', 'bbwp', 'stoch'],
        'names': ['BBPB', 'BBWP', 'Stoch'],
        'category': 'Mean Reversion / Volatility',
        'prior_note': 'CryptoEngine 003 mean reversion은 post-ETF 구조 변화로 폐기됨',
    },
    2: {
        'title': 'Batch 2 — Trend Following 종합',
        'strategies': ['supertrend', 'tradeiq_psar_ha'],
        'names': ['Supertrend', 'TradeIQ PSAR-HA'],
        'category': 'Trend Following',
        'prior_note': 'CryptoEngine 004 trend following은 -1.84% (수수료 > alpha)로 폐기됨',
    },
    3: {
        'title': 'Batch 3 — Hybrid / Regime 종합',
        'strategies': ['trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce'],
        'names': ['TrendType', 'SupertrendTrendType', 'TradeIQ CCI-CE'],
        'category': 'Hybrid / Regime',
        'prior_note': 'regime classifier 단독은 후행성 한계, 5-factor는 진입 빈도 부족 예상',
    },
}


def load_stats(result_dir: Path, strategy_dir: str) -> dict | None:
    stats_file = result_dir / strategy_dir / 'stats.json'
    if not stats_file.exists():
        return None
    with open(stats_file) as f:
        return json.load(f)


def generate_summary(batch: int, result_dir: Path):
    meta = BATCH_META[batch]
    rows = []
    pass_count = 0

    for strat_dir, name in zip(meta['strategies'], meta['names']):
        stats = load_stats(result_dir, strat_dir)
        if stats is None:
            rows.append((name, 'N/A', 'N/A', 'N/A', 'N/A', '⚠ 미완료'))
            continue
        cagr   = f"{stats.get('annual_return_pct', 0):.2f}%"
        sharpe = f"{stats.get('sharpe_ratio', 0):.3f}"
        mdd    = f"{stats.get('max_drawdown_pct', 0):.2f}%"
        trades = str(stats.get('total_trades', 0))
        verdict = stats.get('verdict', 'N/A')
        rows.append((name, cagr, sharpe, mdd, trades, verdict))
        if verdict == 'PASS':
            pass_count += 1

    lines = [
        f'# {meta["title"]}',
        f'',
        f'**생성일**: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}',
        f'**백테스트 기간**: 2020-01-01 ~ 2025-12-31 (6년)',
        f'**자본**: 10,000 USDT | **레버리지**: 1x | **수수료**: 0.055%',
        f'',
        f'## 결과 요약',
        f'',
        f'| 전략 | CAGR | Sharpe | MDD | Trades | 판정 |',
        f'|------|------|--------|-----|--------|------|',
    ]
    for row in rows:
        lines.append(f'| {" | ".join(row)} |')

    lines += [
        f'',
        f'## 카테고리 결론',
        f'',
        f'- **{meta["category"]}** 카테고리에서 **{pass_count}/{len(meta["strategies"])}**개 통과',
        f'- 선례: {meta["prior_note"]}',
        f'',
        f'## PASS/FAIL 기준 (post-ETF 압축장 표준)',
        f'',
        f'| 메트릭 | PASS |',
        f'|--------|------|',
        f'| CAGR | ≥ +5% |',
        f'| Sharpe | ≥ 0.5 |',
        f'| MDD | ≥ -30% |',
        f'| Trades | ≥ 30 |',
        f'| WinRate | ≥ 35% |',
        f'| ProfitFactor | ≥ 1.2 |',
        f'',
        f'## 후속 결정',
        f'',
    ]
    if pass_count == 0:
        lines.append(f'- [ ] 모두 FAIL → {meta["category"]} 카테고리 영구 폐기 확정')
    else:
        for row in rows:
            if row[-1] == 'PASS':
                lines.append(f'- [ ] **{row[0]}** PASS → walk-forward 검증 진행 (별도 작업지시서)')
        lines.append(f'- [ ] FAIL 전략 → archived 처리')

    summary_path = result_dir / 'SUMMARY.md'
    with open(summary_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'Generated: {summary_path}')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--batch',      type=int,  required=True, choices=[1, 2, 3])
    p.add_argument('--result-dir', type=Path, required=True)
    args = p.parse_args()
    generate_summary(args.batch, args.result_dir)
