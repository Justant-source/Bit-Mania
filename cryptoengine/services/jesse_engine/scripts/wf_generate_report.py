#!/usr/bin/env python3
"""
TrendType Walk-Forward 보고서 자동 생성.
v3/walk_forward/trendtype/ 의 stats.json에서 연도별 / IS-OOS 집계.

Output: /result/v3/walk_forward/WF_REPORT.md
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

BASE    = Path('/result/v3')
WF_BASE = BASE / 'walk_forward' / 'trendtype'
VARIANTS = ['bidirectional', 'long_only']
YEARS    = ['2021', '2022', '2023', '2024', '2025']

YEAR_RANGES = {
    '2021': ('2021-04-01', '2021-12-31'),
    '2022': ('2022-01-01', '2022-12-31'),
    '2023': ('2023-01-01', '2023-12-31'),
    '2024': ('2024-01-01', '2024-12-31'),
    '2025': ('2025-01-01', '2025-12-31'),
}


def load(path: Path) -> dict | None:
    stats_p = path / 'stats.json'
    marker  = path / 'EXECUTION_SUCCESS.marker'
    if stats_p.exists() and marker.exists():
        return json.loads(stats_p.read_text())
    return None


def fmt_row(year: str, s: dict | None) -> str:
    if s is None:
        return f'| {year} | FAILED | - | - | - | - | - | - |'
    return (
        f"| {year} "
        f"| {s.get('annual_return_pct', 0):+.2f}% "
        f"| {s.get('sharpe_ratio', 0):.3f} "
        f"| {s.get('max_drawdown_pct', 0):.2f}% "
        f"| {s.get('total_trades', 0)} "
        f"| {s.get('win_rate_pct', 0):.1f}% "
        f"| {s.get('profit_factor', 0):.2f} |"
    )


def stability_score(year_stats: list[dict | None]) -> str:
    valid = [s for s in year_stats if s is not None]
    if len(valid) < 3:
        return '데이터 부족'
    positive_years = sum(1 for s in valid if s.get('annual_return_pct', 0) > 0)
    sharpes = [s.get('sharpe_ratio', 0) for s in valid]
    avg_sharpe = sum(sharpes) / len(sharpes)
    if positive_years >= 4 and avg_sharpe >= 0.3:
        return f'✓ 안정 ({positive_years}/{len(valid)}년 양수, avg Sharpe {avg_sharpe:.3f})'
    elif positive_years >= 3:
        return f'△ 보통 ({positive_years}/{len(valid)}년 양수, avg Sharpe {avg_sharpe:.3f})'
    else:
        return f'✗ 불안정 ({positive_years}/{len(valid)}년 양수, avg Sharpe {avg_sharpe:.3f})'


def oos_verdict(is_s: dict | None, oos_s: dict | None, bnh_sharpe: float) -> str:
    if is_s is None or oos_s is None:
        return '검증 불가 (데이터 없음)'
    is_sharpe  = is_s.get('sharpe_ratio', 0)
    oos_sharpe = oos_s.get('sharpe_ratio', 0)
    oos_cagr   = oos_s.get('annual_return_pct', 0)
    oos_mdd    = oos_s.get('max_drawdown_pct', 0)
    degradation = (is_sharpe - oos_sharpe) / max(abs(is_sharpe), 0.001)

    checks = {
        'OOS Sharpe ≥ 0.5':       oos_sharpe >= 0.5,
        'OOS CAGR > 0%':          oos_cagr > 0,
        'OOS MDD ≥ -50%':         oos_mdd >= -50,
        'Sharpe 열화 < 30%':      degradation < 0.3,
    }
    passed = sum(checks.values())
    if passed == 4:
        return f'✅ OOS 통과 (4/4) — Phase 5 후보 적합'
    elif passed >= 3:
        return f'⚠️ OOS 부분 통과 ({passed}/4) — 추가 검토 권고'
    else:
        return f'❌ OOS 미통과 ({passed}/4) — 미배포 권고'


def main() -> None:
    bnh_s = load(BASE / 'buy_and_hold')
    bnh_sharpe = bnh_s.get('sharpe_ratio', 0) if bnh_s else 0
    bnh_cagr   = bnh_s.get('annual_return_pct', 0) if bnh_s else 0

    v3_bidi  = load(BASE / 'batch_3' / 'trendtype' / 'bidirectional')
    v3_long  = load(BASE / 'batch_3' / 'trendtype' / 'long_only')

    lines = [
        '# TrendType Walk-Forward 검증 보고서 (자동 생성)',
        '',
        f'**생성 시각**: {datetime.now(timezone.utc).isoformat()}',
        f'**생성 방식**: `wf_generate_report.py` (LLM 직접 작성 금지)',
        '',
        '## 기준선',
        f'| 항목 | 값 |',
        f'|------|-----|',
        f'| BnH CAGR | {bnh_cagr:.2f}% |',
        f'| BnH Sharpe | {bnh_sharpe:.4f} |',
        f'| V3 Full (bidi) CAGR | {v3_bidi.get("annual_return_pct", "N/A") if v3_bidi else "N/A"}% |',
        f'| V3 Full (bidi) Sharpe | {v3_bidi.get("sharpe_ratio", "N/A") if v3_bidi else "N/A"} |',
        f'| V3 Full (long) CAGR | {v3_long.get("annual_return_pct", "N/A") if v3_long else "N/A"}% |',
        f'| V3 Full (long) Sharpe | {v3_long.get("sharpe_ratio", "N/A") if v3_long else "N/A"} |',
        '',
    ]

    for variant in VARIANTS:
        vd = WF_BASE / variant
        year_data = {yr: load(vd / yr) for yr in YEARS}
        oos_s = load(vd / 'oos_2023_2025')
        is_s  = load(vd / 'is_2020_2022') if (vd / 'is_2020_2022').exists() else None
        # is_2020_2022 may not exist separately — approximate from year data
        if is_s is None:
            # Use OOS complement: full-period stats (V3) is a proxy
            is_s = v3_bidi if variant == 'bidirectional' else v3_long

        annual = [year_data[yr] for yr in YEARS]

        lines += [
            f'## {variant.upper()}',
            '',
            '### 연도별 성과',
            '| 연도 | CAGR | Sharpe | MDD | 거래 | WR | PF |',
            '|------|------|--------|-----|------|-----|-----|',
        ]
        for yr in YEARS:
            lines.append(fmt_row(yr, year_data[yr]))

        lines += [
            '',
            '### IS / OOS 비교',
            '| 구간 | CAGR | Sharpe | MDD | 거래 |',
            '|------|------|--------|-----|------|',
        ]
        def _ioos_row(label: str, s: dict | None) -> str:
            if s is None:
                return f'| {label} | N/A | N/A | N/A | N/A |'
            return (f"| {label} | {s.get('annual_return_pct', 0):+.2f}% "
                    f"| {s.get('sharpe_ratio', 0):.3f} "
                    f"| {s.get('max_drawdown_pct', 0):.2f}% "
                    f"| {s.get('total_trades', 0)} |")

        lines.append(_ioos_row('IS (2020-2025 전체)', is_s))
        lines.append(_ioos_row('OOS (2023-2025 단독)', oos_s))
        lines += [
            '',
            f'**OOS 판정**: {oos_verdict(is_s, oos_s, bnh_sharpe)}',
            f'**연도별 안정성**: {stability_score(annual)}',
            '',
        ]

    lines += [
        '## 종합 판단',
        '',
        '| Variant | OOS Sharpe | OOS CAGR | 연도별 안정성 |',
        '|---------|-----------|---------|-------------|',
    ]
    for variant in VARIANTS:
        vd = WF_BASE / variant
        oos_s = load(vd / 'oos_2023_2025')
        annual = [load(vd / yr) for yr in YEARS]
        oos_sh = f'{oos_s.get("sharpe_ratio", 0):.3f}' if oos_s else 'N/A'
        oos_ca = f'{oos_s.get("annual_return_pct", 0):+.2f}%' if oos_s else 'N/A'
        lines.append(f'| {variant} | {oos_sh} | {oos_ca} | {stability_score(annual)} |')

    out = WF_BASE.parent / 'WF_REPORT.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'Walk-forward report written: {out}')


if __name__ == '__main__':
    main()
