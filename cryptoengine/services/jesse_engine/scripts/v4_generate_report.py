#!/usr/bin/env python3
"""
V4 자동 보고서 생성 (LLM 직접 작성 금지 — stats.json + monthly_returns.csv 전용).

Output:
    /result/v4/MATRIX_REPORT.md   — 연도별 수익률 + $1,000 누적 잔고 매트릭스
    /result/v4/SUMMARY.md         — Tier 분포 + Top 10 + 결론
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

RESULT_DIR = Path('/result/9-strategies')
TIMEFRAMES = ['1h', '2h', '4h', '1D']
YEARS      = [2020, 2021, 2022, 2023, 2024, 2025]
START_BAL  = 1_000.0

STRATEGIES = [
    ('BBPBStrategy',                'bbpb'),
    ('BBWPStrategy',                'bbwp'),
    ('StochStrategy',               'stoch'),
    ('MomentumMAStrategy',          'momentum_ma'),
    ('SupertrendStrategy',          'supertrend'),
    ('TradeIQ220320Strategy',       'tradeiq_220320'),
    ('TrendTypeStrategy',           'trendtype'),
    ('SupertrendTrendTypeStrategy', 'supertrend_trendtype'),
    ('TradeIQ220323Strategy',       'tradeiq_220323'),
]
VARIANTS = ['bidirectional', 'long_only']


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_stats(path: Path) -> dict | None:
    stats_p = path / 'stats.json'
    marker  = path / 'EXECUTION_SUCCESS.marker'
    if stats_p.exists() and marker.exists():
        return json.loads(stats_p.read_text())
    return None


def load_monthly(path: Path) -> dict[str, float]:
    """Return {YYYY-MM: pnl_usdt}. Empty if file missing."""
    f = path / 'monthly_returns.csv'
    if not f.exists():
        return {}
    result: dict[str, float] = {}
    with open(f, newline='') as fh:
        for row in csv.DictReader(fh):
            result[row['month']] = float(row['pnl_usdt'])
    return result


# ── Balance simulation ────────────────────────────────────────────────────────

def simulate(monthly: dict[str, float], start: float = START_BAL) -> tuple[dict[int, dict], float]:
    """Simulate $start compounding through years.
    Returns (yearly_data, final_balance).
    yearly_data[yr] = {'ret_pct': float, 'end_bal': float, 'liquidated': bool}
    """
    balance = start
    liquidated = False
    yearly: dict[int, dict] = {}
    for yr in YEARS:
        yr_start = balance
        yr_pnl = sum(monthly.get(f'{yr}-{m:02d}', 0.0) for m in range(1, 13))
        ret_pct = (yr_pnl / yr_start * 100) if (yr_start > 0 and not liquidated) else 0.0
        balance = yr_start + yr_pnl
        if balance <= 0 and not liquidated:
            liquidated = True
            balance = 0.0
        yearly[yr] = {'ret_pct': ret_pct, 'end_bal': balance, 'liquidated': liquidated}
    return yearly, balance


# ── Tier classifier ───────────────────────────────────────────────────────────

def tier(stats: dict | None, bnh_sharpe: float) -> str:
    if stats is None:
        return 'FAILED'
    sharpe  = stats.get('sharpe_ratio', 0)
    cagr    = stats.get('annual_return_pct', 0)
    mdd     = stats.get('max_drawdown_pct', 0)
    trades  = stats.get('total_trades', 0)
    if sharpe >= bnh_sharpe * 0.7 and cagr >= 5 and mdd >= -30 and trades >= 30:
        return 'A'
    if sharpe >= 0.3 and cagr >= 0 and mdd >= -40:
        return 'B'
    return 'C'


# ── Collect all results ───────────────────────────────────────────────────────

def collect() -> list[dict]:
    rows = []
    for tf in TIMEFRAMES:
        # BnH
        bnh_path = RESULT_DIR / 'buy_and_hold' / tf / 'buy_and_hold'
        bnh_stats = load_stats(bnh_path)
        bnh_sharpe = bnh_stats.get('sharpe_ratio', 0.0) if bnh_stats else 0.0
        bnh_monthly = load_monthly(bnh_path)
        bnh_yearly, bnh_final = simulate(bnh_monthly)
        rows.append({
            'tf': tf, 'strat': 'buy_and_hold', 'variant': '-',
            'stats': bnh_stats, 'yearly': bnh_yearly, 'final_bal': bnh_final,
            'bnh_sharpe': bnh_sharpe, 'tier': 'BNH',
        })
        # Strategies
        for cls, d in STRATEGIES:
            for var in VARIANTS:
                path = RESULT_DIR / d / tf / var
                stats = load_stats(path)
                monthly = load_monthly(path)
                yearly, final = simulate(monthly)
                rows.append({
                    'tf': tf, 'strat': d, 'variant': var,
                    'stats': stats, 'yearly': yearly, 'final_bal': final,
                    'bnh_sharpe': bnh_sharpe,
                    'tier': tier(stats, bnh_sharpe),
                })
    return rows


# ── Formatters ────────────────────────────────────────────────────────────────

def fmt_ret(yd: dict, yr: int) -> str:
    if yr not in yd:
        return 'N/A'
    y = yd[yr]
    if y.get('liquidated') and y['ret_pct'] == 0 and yr > YEARS[0]:
        return '💀'
    return f"{y['ret_pct']:+.1f}%"


def fmt_bal(final: float, yearly: dict) -> str:
    for yr in YEARS:
        if yearly.get(yr, {}).get('liquidated') and yearly[yr]['end_bal'] == 0:
            return f'💀${0:.0f}'
    return f'${final:,.0f}'


def fmt_stats(stats: dict | None) -> tuple[str, str]:
    if stats is None:
        return 'N/A', 'N/A'
    sharpe = f"{stats.get('sharpe_ratio', 0):.3f}"
    mdd    = f"{stats.get('max_drawdown_pct', 0):.1f}%"
    return sharpe, mdd


# ── Report writers ────────────────────────────────────────────────────────────

def write_matrix(rows: list[dict]) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    yr_hdrs = ' | '.join(str(y) for y in YEARS)
    lines = [
        '# V4 멀티타임프레임 백테스트 매트릭스 (자동 생성)',
        '',
        f'**생성 시각**: {ts}',
        f'**생성 방식**: `v4_generate_report.py` (LLM 직접 작성 금지)',
        f'**초기 자금**: ${START_BAL:,.0f}',
        '',
        f'| 전략 | 변형 | TF | {yr_hdrs} | $1,000→ | Sharpe | MDD | Tier |',
        f'|------|-----|----|{"|----|" * len(YEARS)}---------|--------|-----|------|',
    ]
    for r in rows:
        yr_cells = ' | '.join(fmt_ret(r['yearly'], yr) for yr in YEARS)
        bal = fmt_bal(r['final_bal'], r['yearly'])
        sharpe, mdd = fmt_stats(r['stats'])
        lines.append(
            f"| {r['strat']} | {r['variant']} | {r['tf']} | {yr_cells} "
            f"| {bal} | {sharpe} | {mdd} | {r['tier']} |"
        )
    out = RESULT_DIR / 'MATRIX_REPORT.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'MATRIX_REPORT written: {out}')


def write_summary(rows: list[dict]) -> None:
    ts = datetime.now(timezone.utc).isoformat()

    strat_rows = [r for r in rows if r['variant'] != '-']
    tier_counts = {t: 0 for t in ['A', 'B', 'C', 'FAILED']}
    for r in strat_rows:
        tier_counts[r['tier']] = tier_counts.get(r['tier'], 0) + 1

    top10 = sorted(
        [r for r in strat_rows if r['stats'] is not None],
        key=lambda r: r['final_bal'], reverse=True
    )[:10]

    lines = [
        '# V4 종합 요약 (자동 생성)',
        '',
        f'**생성 시각**: {ts}',
        f'**생성 방식**: `v4_generate_report.py` (LLM 직접 작성 금지)',
        '',
        '## Tier 분포',
        '',
        f'| Tier | 건수 | 기준 |',
        f'|------|------|------|',
        f'| A | {tier_counts["A"]} | Sharpe≥BnH×0.7, CAGR≥5%, MDD≥-30%, Trades≥30 |',
        f'| B | {tier_counts["B"]} | Sharpe≥0.3, CAGR≥0%, MDD≥-40% |',
        f'| C | {tier_counts["C"]} | B 미달 |',
        f'| FAILED | {tier_counts["FAILED"]} | 백테스트 실패 |',
        '',
        '## BnH 벤치마크 (TF별)',
        '',
        '| TF | Sharpe | CAGR | $1,000→ |',
        '|----|--------|------|---------|',
    ]
    for r in rows:
        if r['variant'] == '-':
            sh, mdd = fmt_stats(r['stats'])
            cagr = f"{r['stats'].get('annual_return_pct', 0):+.2f}%" if r['stats'] else 'N/A'
            lines.append(f"| {r['tf']} | {sh} | {cagr} | {fmt_bal(r['final_bal'], r['yearly'])} |")

    lines += [
        '',
        '## Top 10 — 최고 누적 잔고 (전략, 변형, TF)',
        '',
        '| 순위 | 전략 | 변형 | TF | $1,000→ | Sharpe | MDD | Tier |',
        '|------|------|-----|----|---------|--------|-----|------|',
    ]
    for i, r in enumerate(top10, 1):
        sh, mdd = fmt_stats(r['stats'])
        lines.append(
            f"| {i} | {r['strat']} | {r['variant']} | {r['tf']} "
            f"| {fmt_bal(r['final_bal'], r['yearly'])} | {sh} | {mdd} | {r['tier']} |"
        )

    # Tier A list
    tier_a = [r for r in strat_rows if r['tier'] == 'A']
    lines += [
        '',
        f'## Tier A 전략 목록 ({len(tier_a)}건)',
        '',
        '| 전략 | 변형 | TF | CAGR | Sharpe | MDD | Trades | $1,000→ |',
        '|------|-----|----|------|--------|-----|--------|---------|',
    ]
    for r in sorted(tier_a, key=lambda r: (r['strat'], r['variant'], r['tf'])):
        s = r['stats'] or {}
        lines.append(
            f"| {r['strat']} | {r['variant']} | {r['tf']} "
            f"| {s.get('annual_return_pct', 0):+.2f}% "
            f"| {s.get('sharpe_ratio', 0):.3f} "
            f"| {s.get('max_drawdown_pct', 0):.1f}% "
            f"| {s.get('total_trades', 0)} "
            f"| {fmt_bal(r['final_bal'], r['yearly'])} |"
        )

    out = RESULT_DIR / 'SUMMARY.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'SUMMARY written: {out}')


def main() -> None:
    rows = collect()
    write_matrix(rows)
    write_summary(rows)
    print('V4 report generation complete.')


if __name__ == '__main__':
    main()
