#!/usr/bin/env python3
"""
7-strategies/README.md 와 summary.csv 를 현재 stats.json 기준으로 동기화

Usage:
    python3 backtest/scripts/reports/sync_7strategies_docs.py
"""
import json
import csv
from pathlib import Path
from datetime import datetime


def main():
    SEVEN_DIR = Path('/home/justant/Data/Bit-Mania/backtest/results/7-strategies')

    # Collect all stats
    rows = []
    for stats_path in sorted(SEVEN_DIR.glob('*/*/*/stats.json')):
        parts = stats_path.parts
        # 경로: .../7-strategies/{strat}/{tf}/{variant}/stats.json
        variant = parts[-2]
        tf = parts[-3]
        strat = parts[-4]

        try:
            stats = json.loads(stats_path.read_text())
        except Exception as e:
            print(f"Warning: Failed to read {stats_path}: {e}")
            continue

        # Extract metrics from stats.json
        cagr = stats.get('cagr_pct', 0)
        mdd = stats.get('max_drawdown_pct', 0)
        sharpe = stats.get('sharpe_ratio', 0)
        pf = stats.get('profit_factor', 0)
        trades = stats.get('total_trades', 0)

        rows.append({
            'strat': strat,
            'tf': tf,
            'variant': variant,
            'cagr': round(cagr, 2),
            'mdd': round(mdd, 2),
            'sharpe': round(sharpe, 3),
            'pf': round(pf, 3),
            'trades': int(trades),
        })

    # Save summary.csv
    csv_path = SEVEN_DIR / 'summary.csv'
    fieldnames = ['strat', 'tf', 'variant', 'cagr', 'mdd', 'sharpe', 'pf', 'trades']
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # Build README.md
    lines = [
        '# 7-Strategies 백테스트 결과',
        '',
        '**기간**: 2021-01-01 ~ 2026-04-30',
        '**초기자본**: $10,000 | **레버리지**: 1× | **심볼**: BTC-USDT',
        '',
        f'> 자동 생성 — `sync_7strategies_docs.py` (동기화: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")})',
        f'> 수집된 결과: {len(rows)}건',
        '',
        '## Champion 조합 (TF별 최고 성과)',
        '',
        '| 전략 | TF | 변형 | CAGR(%) | MDD(%) | Sharpe | PF | 거래수 |',
        '|---|---|---|---|---|---|---|---|',
    ]

    # Group by strategy and TF to find champions
    champions_by_tf = {}
    for r in rows:
        key = (r['strat'], r['tf'])
        if key not in champions_by_tf:
            champions_by_tf[key] = r
        else:
            # Compare by Sharpe ratio (primary metric)
            if r['sharpe'] > champions_by_tf[key]['sharpe']:
                champions_by_tf[key] = r

    # Sort by CAGR descending
    champions = sorted(champions_by_tf.values(), key=lambda x: x['cagr'], reverse=True)
    for r in champions:
        lines.append(
            f"| {r['strat']} | {r['tf']} | {r['variant']} "
            f"| {r['cagr']:+.2f} | {r['mdd']:.2f} "
            f"| {r['sharpe']:.3f} | {r['pf']:.3f} | {r['trades']} |"
        )

    lines += [
        '',
        '## 전체 결과 ({} 건)'.format(len(rows)),
        '',
        '| 전략 | TF | 변형 | CAGR(%) | MDD(%) | Sharpe | PF | 거래수 |',
        '|---|---|---|---|---|---|---|---|',
    ]

    # Sort all results by CAGR descending
    sorted_rows = sorted(rows, key=lambda x: x['cagr'], reverse=True)
    for r in sorted_rows:
        lines.append(
            f"| {r['strat']} | {r['tf']} | {r['variant']} "
            f"| {r['cagr']:+.2f} | {r['mdd']:.2f} "
            f"| {r['sharpe']:.3f} | {r['pf']:.3f} | {r['trades']} |"
        )

    lines += [
        '',
        '## 참고',
        '',
        '- 본 표는 `backtest/results/7-strategies/{strat}/{tf}/{variant}/stats.json` 기준으로 자동 생성됨',
        '- 수정 시: `python3 backtest/scripts/reports/sync_7strategies_docs.py` 실행',
    ]

    readme_path = SEVEN_DIR / 'README.md'
    readme_path.write_text('\n'.join(lines) + '\n')

    print(f"✓ {csv_path} 저장 ({len(rows)}행)")
    print(f"✓ {readme_path} 저장 (champions: {len(champions)}개)")


if __name__ == '__main__':
    main()
