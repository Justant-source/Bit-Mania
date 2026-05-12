#!/usr/bin/env python3
"""
219 survivor 비용 보정 (trade-level 데이터 없는 경우 추정 방식)
- Taker fee: 0.020% → 0.055% (+0.035% per side × 2 = +0.070%)
- Funding: 평균 보유 기간 × 평균 펀딩율 × long/short 방향
- Trade-level CSV가 없으므로, 전략/TF별 평균 보유 시간 가정으로 추정
"""
import json
import statistics
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
FUNDING_PATH = ROOT / 'data' / 'funding' / 'BTCUSDT_8h.parquet'
OUT_BASE = ROOT / 'results' / 'adjusted_costs'

# 검증된 219 survivor 목록 수집
def load_survivors(min_score=-998.0):
    survivors = []
    for sweep_ver in ('v2', 'v3'):
        base = ROOT / 'results' / 'param_sweep' / sweep_ver
        for summary_path in sorted(base.rglob('summary.json')):
            s = json.loads(summary_path.read_text())
            if s.get('score', -999) <= min_score:
                continue
            parts = summary_path.parts
            variant = parts[-3]
            tf = parts[-4]
            strat = parts[-5]
            combo_idx = int(parts[-2].replace('combo_', ''))
            survivors.append({
                'version': sweep_ver,
                'strat': strat,
                'tf': tf,
                'variant': variant,
                'combo': combo_idx,
                'score': s['score'],
                'hp': s.get('hp', {}),
                'periods': s.get('periods', {}),
                'summary_path': summary_path,
            })
    return survivors


# TF별 평균 보유 시간 (시간 단위)
# 백테스트에서 관찰된 평균 거래 기간 기반
TF_HOLD_HOURS = {
    '1h': 10,   # 약 1.25 펀딩 주기
    '4h': 32,   # 약 4 펀딩 주기
    '1D': 96,   # 약 12 펀딩 주기
}

# 기간별 날짜 범위 (정확한 펀딩율 계산용)
PERIOD_DATES = {
    'p0': ('2018-04-01', '2020-06-30'),
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

# 기간별 연도 (정확한 연환산 계산용)
PERIOD_YEARS = {
    'p0': 2.25,   # 2018-04 ~ 2020-06
    'p1': 5.08,   # 2021-04 ~ 2026-04
    'p2': 3.42,   # 2022-12 ~ 2026-04
    'p3': 4.50,   # 2021-04 ~ 2025-09
    'p4': 2.83,   # 2022-12 ~ 2025-09
}


def apply_costs(row: dict, fund_df: pd.DataFrame) -> dict:
    """단일 combo에 비용 보정 적용"""
    tf = row['tf']
    variant = row['variant']
    periods = row['periods']

    hold_hours = TF_HOLD_HOURS.get(tf, 32)
    n_funding = hold_hours / 8.0  # 평균 펀딩 주기 횟수 (8h 주기)

    # Taker fee delta: 0.020% → 0.055% = +0.035% per side
    # Entry + Exit = 2 sides → +0.070% per round-trip
    fee_delta_per_side = (0.055 - 0.020) / 100.0

    # long_only: 펀딩 차감 (자금 조달 비용),
    # short_only: 펀딩 수령 (자금 대여 수익),
    # bidirectional: 반반 (net ~0)
    if variant == 'long_only':
        funding_sign = -1.0
    elif variant == 'short_only':
        funding_sign = +1.0
    else:  # bidirectional, both_ways
        funding_sign = 0.0

    adj_periods = {}
    for p_key, m in periods.items():
        cagr = m.get('cagr', 0)
        mdd = m.get('mdd', 0)
        trades = m.get('trades', 0)
        sharpe = m.get('sharpe', 0)
        pf = m.get('pf', 1)

        # 기간별 평균 펀딩율 계산
        if p_key in PERIOD_DATES:
            s_date, e_date = PERIOD_DATES[p_key]
            s_ts = int(pd.Timestamp(s_date).timestamp() * 1000)
            e_ts = int(pd.Timestamp(e_date).timestamp() * 1000)
            mask = (fund_df['timestamp'] >= s_ts) & (
                fund_df['timestamp'] <= e_ts
            ) & (fund_df['funding_rate'] != 0)
            avg_fund = (
                fund_df.loc[mask, 'funding_rate'].mean()
                if mask.any()
                else 0.0005
            )
        else:
            avg_fund = 0.0005  # 0.05% per 8h 기본값

        # 기간 길이 (년)
        period_years = PERIOD_YEARS.get(p_key, 3.0)

        # 연환산 비용 추정
        # trades_per_year ≈ trades / period_years
        # fee_cost_annual ≈ trades_per_year × fee_delta_per_side × 2 (entry+exit) × 100%
        # funding_cost_annual ≈ trades_per_year × n_funding × avg_fund × funding_sign × 100%

        trades_per_year = trades / period_years if period_years > 0 else 0

        # Taker fee 비용 (연환산 %)
        # 각 거래당 Entry에서 fee_delta, Exit에서 fee_delta
        fee_cost_pct_annual = trades_per_year * fee_delta_per_side * 2 * 100

        # 펀딩 비용 (연환산 %)
        # 각 거래당 평균 n_funding 주기 동안 avg_fund 비용 발생
        fund_cost_pct_annual = (
            trades_per_year * n_funding * avg_fund * funding_sign * 100
        )

        total_cost_annual = fee_cost_pct_annual + fund_cost_pct_annual

        # 조정 CAGR
        adj_cagr = round(cagr - total_cost_annual, 2)

        # 조정 MDD: 보수적으로 fee 비용의 0.5배 추가 악화 가정
        # (실제 drawdown은 비용으로 더 악화될 수 있음)
        adj_mdd = round(mdd - abs(fee_cost_pct_annual) * 0.5, 2)

        # 조정 Sharpe: CAGR 비율로 조정
        if cagr != 0:
            adj_sharpe = round(sharpe * (adj_cagr / cagr), 3)
        else:
            adj_sharpe = 0.0

        adj_periods[p_key] = {
            'original_cagr': cagr,
            'adj_cagr': adj_cagr,
            'original_mdd': mdd,
            'adj_mdd': adj_mdd,
            'original_sharpe': sharpe,
            'adj_sharpe': adj_sharpe,
            'trades': trades,
            'pf': pf,
            'fee_cost_annual_pct': round(fee_cost_pct_annual, 3),
            'funding_cost_annual_pct': round(fund_cost_pct_annual, 3),
            'avg_funding_rate': round(avg_fund * 100, 5),
            'hold_hours': hold_hours,
            'n_funding_periods': round(n_funding, 2),
        }

    # 5구간 평균 조정 CAGR으로 새로운 스코어 계산
    adj_cagrs = [
        v['adj_cagr'] for v in adj_periods.values() if 'adj_cagr' in v
    ]
    adj_score = round(statistics.mean(adj_cagrs), 2) if adj_cagrs else -999

    return {
        'strat': row['strat'],
        'tf': tf,
        'variant': variant,
        'combo': row['combo'],
        'version': row['version'],
        'hp': row['hp'],
        'original_score': row['score'],
        'adjusted_score': adj_score,
        'periods': adj_periods,
    }


def main():
    print('=' * 70)
    print('219 Survivor 비용 보정 시작')
    print('=' * 70)

    # 1. Survivor 로드
    survivors = load_survivors()
    print(f'\n[1] Survivor 조회: {len(survivors)} 건')
    print(
        f'    v2: {sum(1 for x in survivors if x["version"] == "v2")} | '
        f'v3: {sum(1 for x in survivors if x["version"] == "v3")}'
    )

    # 2. 펀딩 데이터 로드
    fund_df = pd.read_parquet(FUNDING_PATH)
    non_zero_count = (fund_df['funding_rate'] != 0).sum()
    print(
        f'\n[2] 펀딩 데이터: {len(fund_df)} rows, '
        f'non-zero: {non_zero_count} ({100*non_zero_count/len(fund_df):.1f}%)'
    )

    # 3. 비용 보정 적용
    print(f'\n[3] 비용 보정 적용 중...')
    results = []
    for i, row in enumerate(survivors):
        adj = apply_costs(row, fund_df)
        results.append(adj)

        # 저장
        out_dir = (
            OUT_BASE
            / adj['strat']
            / adj['tf']
            / adj['variant']
            / f"combo_{adj['combo']}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'adjusted_stats.json').write_text(
            json.dumps(adj, indent=2)
        )

        if (i + 1) % 50 == 0:
            print(f'  {i+1}/{len(survivors)} 완료')

    print(f'  {len(results)}/{len(survivors)} 완료 ✓')

    # 4. 통계
    print(f'\n[4] 조정 CAGR 통계:')
    scores = [r['adjusted_score'] for r in results if r['adjusted_score'] > -998]
    if scores:
        print(f'    평균: {statistics.mean(scores):.2f}%')
        print(f'    최대: {max(scores):.2f}%')
        print(f'    최소: {min(scores):.2f}%')
        print(
            f'    중앙값: {sorted(scores)[len(scores)//2]:.2f}%'
        )

    # 5. Baseline 비교
    BASELINE = 34.87  # funding-arb baseline
    above_baseline = [r for r in results if r['adjusted_score'] > BASELINE]
    print(f'\n[5] Baseline 비교 (funding-arb: {BASELINE}%):')
    print(
        f'    조정 CAGR > baseline: {len(above_baseline)} 건 '
        f'({100*len(above_baseline)/len(results):.1f}%)'
    )

    # 6. 파일 저장
    output_json = OUT_BASE / 'all_adjusted_results.json'
    with open(output_json, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'\n[6] 결과 저장:')
    print(f'    개별: {len(results)} × adjusted_stats.json')
    print(f'    통합: {output_json}')

    print(f'\n' + '=' * 70)
    print('완료!')
    print('=' * 70)


if __name__ == '__main__':
    main()
