#!/usr/bin/env python3
"""
V3 보고서 자동 생성.
v3_verify_markers.py 통과 후에만 실행.
LLM/사람이 숫자를 직접 입력하지 않음 — stats.json에서만 추출.
실패한 백테스트는 "FAILED — no data" 행으로 표시.

Usage:
  python v3_generate_report.py [batch_1|batch_2|batch_3]
  (인자 없으면 전체 batch summary + cross-batch 보고서 생성)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path('/result/v3')

EXPECTED = {
    'batch_1': ['bbpb', 'bbwp', 'stoch'],
    'batch_2': ['supertrend', 'tradeiq_psar_ha'],
    'batch_3': ['trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce'],
}
VARIANTS = ['bidirectional', 'long_only']


def load_or_failed(variant_dir: Path) -> dict | None:
    success = variant_dir / 'EXECUTION_SUCCESS.marker'
    if not success.exists():
        return None
    stats_path = variant_dir / 'stats.json'
    if not stats_path.exists():
        return None
    return json.loads(stats_path.read_text())


def determine_tier(stats: dict | None, bnh_sharpe: float) -> tuple[str, int]:
    if stats is None:
        return 'FAILED', 0

    pass_checks = {
        'cagr':   stats.get('annual_return_pct', -999) >= 5,
        'sharpe': stats.get('sharpe_ratio', -999)      >= 0.5,
        'mdd':    stats.get('max_drawdown_pct', -999)  >= -30,
        'trades': stats.get('total_trades', 0)          >= 30,
        'wr':     stats.get('win_rate_pct', 0)          >= 35,
        'pf':     stats.get('profit_factor', 0)         >= 1.2,
    }
    pass_count = sum(pass_checks.values())
    is_pass = pass_count == 6

    sharpe = stats.get('sharpe_ratio', -999)
    if is_pass and bnh_sharpe > 0 and sharpe / bnh_sharpe >= 0.7:
        return 'A', pass_count
    if (pass_count >= 4 and
            stats.get('profit_factor', 0) >= 1.0 and
            stats.get('total_trades', 0) >= 100):
        return 'B', pass_count
    return 'C', pass_count


def _stats_row(stats: dict | None) -> str:
    if stats is None:
        return '- | - | - | - | - | -'
    return (
        f"{stats.get('annual_return_pct', 0):.2f}% | "
        f"{stats.get('sharpe_ratio', 0):.3f} | "
        f"{stats.get('max_drawdown_pct', 0):.2f}% | "
        f"{stats.get('total_trades', 0)} | "
        f"{stats.get('win_rate_pct', 0):.1f}% | "
        f"{stats.get('profit_factor', 0):.2f}"
    )


def generate_batch_summary(batch: str) -> None:
    bnh = load_or_failed(BASE / 'buy_and_hold')
    bnh_sharpe = bnh.get('sharpe_ratio', 0) if bnh else 0

    lines = [
        f'# {batch} V3 Summary (자동 생성)',
        '',
        f'**생성 시각**: {datetime.now(timezone.utc).isoformat()}',
        f'**생성 방식**: `v3_generate_report.py` (LLM 직접 작성 금지)',
        f'**BnH Sharpe**: {bnh_sharpe:.4f}  Tier A 기준: ≥ {bnh_sharpe * 0.7:.4f}',
        '',
        '| 전략 | Variant | Status | CAGR | Sharpe | MDD | Trades | WR | PF | Tier |',
        '|------|---------|--------|------|--------|-----|--------|-----|-----|------|',
    ]

    for strategy in EXPECTED[batch]:
        for variant in VARIANTS:
            vd = BASE / batch / strategy / variant
            stats = load_or_failed(vd)
            tier, _ = determine_tier(stats, bnh_sharpe)
            status = 'SUCCESS' if stats is not None else 'FAILED'
            lines.append(
                f'| {strategy} | {variant} | {status} | {_stats_row(stats)} | **{tier}** |'
            )

    out = BASE / batch / 'SUMMARY.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'Batch summary written: {out}')


def generate_cross_batch_report() -> None:
    bnh = load_or_failed(BASE / 'buy_and_hold')
    if bnh is None:
        raise RuntimeError('BnH 결과 없음 — cross-batch 보고서 생성 불가')

    bnh_cagr   = bnh.get('annual_return_pct', 0)
    bnh_sharpe = bnh.get('sharpe_ratio', 0)
    bnh_mdd    = bnh.get('max_drawdown_pct', 0)

    lines = [
        '# V3 Cross-Batch Selection Report (자동 생성)',
        '',
        f'**생성 시각**: {datetime.now(timezone.utc).isoformat()}',
        f'**생성 방식**: `v3_generate_report.py` (LLM 직접 작성 금지)',
        f'**입력 검증**: `v3_verify_markers.py` 통과 후에만 생성됨',
        '',
        '## BnH 벤치마크 (실측)',
        f'- CAGR: **{bnh_cagr:.2f}%**',
        f'- Sharpe: **{bnh_sharpe:.4f}**',
        f'- MDD: **{bnh_mdd:.2f}%**',
        f'- Tier A 기준 Sharpe: **≥ {bnh_sharpe * 0.7:.4f}** (BnH × 70%)',
        '',
        '## 18개 결과 종합',
        '',
        '| Batch | 전략 | Variant | Status | CAGR | Sharpe | MDD | Trades | WR | PF | Tier |',
        '|-------|------|---------|--------|------|--------|-----|--------|-----|-----|------|',
    ]

    summary: dict[str, int] = {'A': 0, 'B': 0, 'C': 0, 'FAILED': 0}

    for batch, strategies in EXPECTED.items():
        for strategy in strategies:
            for variant in VARIANTS:
                vd = BASE / batch / strategy / variant
                stats = load_or_failed(vd)
                tier, pass_count = determine_tier(stats, bnh_sharpe)
                summary[tier] = summary.get(tier, 0) + 1
                status = 'SUCCESS' if stats is not None else '**FAILED**'
                lines.append(
                    f'| {batch} | {strategy} | {variant} | {status} | '
                    f'{_stats_row(stats)} | **{tier}** |'
                )

    lines += [
        '',
        '## Tier 분포',
        f'- Tier A: **{summary.get("A", 0)}**',
        f'- Tier B: **{summary.get("B", 0)}**',
        f'- Tier C: **{summary.get("C", 0)}**',
        f'- Failed: **{summary.get("FAILED", 0)}**',
        '',
        '## 종합',
    ]

    if summary.get('A', 0) > 0:
        lines.append(f'⭐ **{summary["A"]}개 전략이 Tier A 도달** — walk-forward 진행 권고')
    elif summary.get('B', 0) > 0:
        lines.append(f'🔍 Tier A 0개. **{summary["B"]}개 전략 Tier B** — 6개월 후 재시도 후보로 보존')
    elif summary.get('FAILED', 0) > 0:
        lines.append(f'⚠️ **{summary["FAILED"]}개 백테스트 실패** — 재실행 필요')
    else:
        lines.append('🗑️ 모든 전략 Tier C — 외부 retail 전략 영구 폐기 결정 데이터 누적')

    out = BASE / 'CROSS_BATCH_V3_SELECTION.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'Cross-batch report written: {out}')


if __name__ == '__main__':
    batch_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if batch_arg and batch_arg in EXPECTED:
        generate_batch_summary(batch_arg)
    else:
        for batch in EXPECTED:
            generate_batch_summary(batch)
        generate_cross_batch_report()
