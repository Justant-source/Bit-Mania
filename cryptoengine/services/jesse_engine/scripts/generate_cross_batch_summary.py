#!/usr/bin/env python3
"""
Generate CROSS_BATCH_SUMMARY.md comparing all 9 strategies across 3 batches.
Usage: python scripts/generate_cross_batch_summary.py --result-dir /result/
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


ALL_STRATEGIES = [
    (1, 'bbpb',               'BBPB',                'Mean Reversion'),
    (1, 'bbwp',               'BBWP',                'Mean Reversion'),
    (1, 'stoch',              'Stoch',               'Mean Reversion'),
    (2, 'momentum_ma',        'MomentumMA',          'Trend Following'),
    (2, 'supertrend',         'Supertrend',          'Trend Following'),
    (2, 'tradeiq_220320',     'TradeIQ220320',        'Trend Following'),
    (3, 'trendtype',          'TrendType',           'Hybrid/Regime'),
    (3, 'supertrend_trendtype','SupertrendTrendType', 'Hybrid/Regime'),
    (3, 'tradeiq_220323',     'TradeIQ220323',        'Hybrid/Regime'),
]


def load_stats(result_dir: Path, batch: int, strategy_dir: str) -> dict | None:
    f = result_dir / f'batch_{batch}' / strategy_dir / 'stats.json'
    if not f.exists():
        return None
    with open(f) as fh:
        return json.load(fh)


def _get_common_settings(result_dir: Path) -> tuple[float, int]:
    """Extract balance and leverage from the first available stats.json."""
    for batch, strat_dir, _, _ in ALL_STRATEGIES:
        for variant in ('bidirectional', 'long_only', ''):
            path = result_dir / f'batch_{batch}' / strat_dir
            f = (path / variant / 'stats.json') if variant else (path / 'stats.json')
            if f.exists():
                with open(f) as fh:
                    s = json.load(fh)
                return float(s.get('starting_balance', 10_000)), int(s.get('leverage', 1))
    return 10_000.0, 1


def generate(result_dir: Path):
    rows = []
    pass_strategies = []
    category_stats: dict[str, list] = {}

    for batch, strat_dir, name, category in ALL_STRATEGIES:
        stats = load_stats(result_dir, batch, strat_dir)
        if stats is None:
            rows.append((f'B{batch}', name, category, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', '⚠'))
            continue
        cagr   = stats.get('annual_return_pct', 0)
        sharpe = stats.get('sharpe_ratio', 0)
        mdd    = stats.get('max_drawdown_pct', 0)
        trades = stats.get('total_trades', 0)
        wr     = stats.get('win_rate_pct', 0)
        pf     = stats.get('profit_factor', 0)
        verdict = stats.get('verdict', 'N/A')
        rows.append((f'B{batch}', name, category,
                     f'{cagr:.2f}%', f'{sharpe:.3f}', f'{mdd:.2f}%',
                     str(trades), f'{wr:.1f}%', verdict))
        category_stats.setdefault(category, []).append(verdict)
        if verdict == 'PASS':
            pass_strategies.append(name)

    total_pass = len(pass_strategies)
    total = len(ALL_STRATEGIES)

    balance, leverage = _get_common_settings(result_dir)
    lines = [
        '# Cross-Batch Summary — 9개 전략 종합 비교',
        '',
        f'**생성일**: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}',
        f'**백테스트 기간**: 2020-01-01 ~ 2025-12-31 (6년, BTC-USDT 1h)',
        f'**공통 설정**: {balance:,.0f} USDT | {leverage}x leverage | 0.055% fee | 0.03% slippage',
        '',
        f'## 전체 결과: {total_pass}/{total}개 PASS',
        '',
        '| Batch | 전략 | 카테고리 | CAGR | Sharpe | MDD | Trades | WinRate | 판정 |',
        '|-------|------|----------|------|--------|-----|--------|---------|------|',
    ]
    for row in rows:
        lines.append('| ' + ' | '.join(row) + ' |')

    lines += [
        '',
        '## 카테고리별 결론',
        '',
    ]
    for cat, verdicts in category_stats.items():
        pc = sum(1 for v in verdicts if v == 'PASS')
        lines.append(f'- **{cat}**: {pc}/{len(verdicts)}개 PASS')

    lines += [
        '',
        '## PASS 전략 (walk-forward 후보)',
        '',
    ]
    if pass_strategies:
        for name in pass_strategies:
            lines.append(f'- **{name}**: walk-forward 검증 진행 필요')
    else:
        lines.append('- 모든 전략 FAIL — 9개 외부 전략 통합 폐기 권고')

    lines += [
        '',
        '## 분석 요약',
        '',
        '### 시장 구조 맥락',
        '- 2020-2021: BTC 강세장 (+1000%) → 대부분 전략에 유리',
        '- 2022: 약세장 (-75%) → 양방향 전략 유리, 단방향 long 전략 불리',
        '- 2023-2024: ETF 승인 전후 회복 (+150%)',
        '- 2024-2025: post-ETF 압축장 → 레탈 전략의 alpha 희석',
        '',
        '### CryptoEngine 적용 결론',
    ]
    if pass_strategies:
        lines += [
            f'- {total_pass}개 전략이 초기 PASS → walk-forward + paper trading 단계 진행',
            '- 기존 fa80_lev5_r30 (FA 전략)과 상관관계 분석 필요',
        ]
    else:
        lines += [
            '- 9개 전략 모두 FAIL → 외부 retail 전략의 CryptoEngine 적용 불가',
            '- 현행 fa80_lev5_r30 단독 운영 유지 권고',
            '- 다음 단계: walk-forward 최적화 / Phase 5 메인넷 소액 전환 진행',
        ]

    out_path = result_dir / 'CROSS_BATCH_SUMMARY.md'
    with open(out_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'Generated: {out_path}')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--result-dir', type=Path, default=Path('/result/'))
    args = p.parse_args()
    generate(args.result_dir)
