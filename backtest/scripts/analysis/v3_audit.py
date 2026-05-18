#!/usr/bin/env python3
"""
V3 작업 종료 후 환각 점검표 생성.
사용자에게 보고하기 전 마지막 단계로 본 스크립트 실행 필수.
모든 산출물을 SHA256 재검증 + mtime 분포 분석 + 자동 생성 마커 확인.

Output: /result/v3/HALLUCINATION_AUDIT.md
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

BASE = Path('/result/v3')

EXPECTED = {
    'batch_1': ['bbpb', 'bbwp', 'stoch'],
    'batch_2': ['supertrend', 'tradeiq_psar_ha'],
    'batch_3': ['trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce'],
}
VARIANTS = ['bidirectional', 'long_only']


def main() -> None:
    audit_lines = [
        '# V3 환각 점검표 (자동 생성)',
        '',
        f'**점검 시각**: {datetime.now(timezone.utc).isoformat()}',
        f'**생성 방식**: `v3_audit.py` (LLM 직접 작성 금지)',
        '',
        '## 모든 백테스트 산출물 검증',
        '',
        '| 경로 | stats.json | SUCCESS marker | SHA256 | 크기 | mtime (UTC) |',
        '|------|-----------|---------------|--------|------|------------|',
    ]

    paths: list[Path] = [BASE / 'buy_and_hold']
    for batch, strategies in EXPECTED.items():
        for strategy in strategies:
            for variant in VARIANTS:
                paths.append(BASE / batch / strategy / variant)

    n_total = 0
    n_real  = 0
    mtimes: list[float] = []

    for p in paths:
        n_total += 1
        rel = str(p.relative_to(BASE.parent))
        stats_p = p / 'stats.json'
        marker  = p / 'EXECUTION_SUCCESS.marker'

        has_stats  = stats_p.exists()
        has_marker = marker.exists()
        sha_match  = 'N/A'
        size_str   = '-'
        mtime_str  = '-'

        if has_stats and has_marker:
            actual = hashlib.sha256(stats_p.read_bytes()).hexdigest()
            expected_sha = None
            for line in marker.read_text().splitlines():
                if line.startswith('stats_sha256:'):
                    expected_sha = line.split(':', 1)[1].strip()
                    break
            sha_match = '✓' if (not expected_sha or expected_sha == actual) else '✗ MISMATCH'
            size_str  = f'{stats_p.stat().st_size}B'
            mt = stats_p.stat().st_mtime
            mtime_str = datetime.fromtimestamp(mt, tz=timezone.utc).strftime('%H:%M:%S')
            mtimes.append(mt)
            if sha_match == '✓':
                n_real += 1

        audit_lines.append(
            f'| {rel} | {"✓" if has_stats else "✗"} | {"✓" if has_marker else "✗"} | '
            f'{sha_match} | {size_str} | {mtime_str} |'
        )

    audit_lines += [
        '',
        '## 점검 통계',
        f'- 총 산출물 경로: {n_total} (예상: 19 = 18 variants + 1 BnH)',
        f'- SHA256 검증 통과: **{n_real}**',
        f'- 검증 실패/미완료: **{n_total - n_real}**',
        '',
        '## 환각 의심 점검',
    ]

    if mtimes:
        mt_range = max(mtimes) - min(mtimes)
        audit_lines.append(f'- mtime 분산: {mt_range:.1f}초 (모든 stats.json mtime의 max−min)')
        if mt_range < 5 and len(mtimes) > 3:
            audit_lines.append('  ⚠️ **의심**: 5초 미만 — 환각 가능성 검토 필요')
        else:
            audit_lines.append('  ✓ 정상 (각 백테스트가 다른 시각에 완료됨)')
    else:
        audit_lines.append('- mtime 분산: 측정 불가 (stats.json 없음)')

    report = BASE / 'CROSS_BATCH_V3_SELECTION.md'
    if report.exists():
        content = report.read_text()
        is_auto = '자동 생성' in content[:300]
        audit_lines.append(
            f'- CROSS_BATCH_V3_SELECTION.md: {"자동 생성 ✓" if is_auto else "⚠️ 자동 생성 마커 없음 — 검토 필요"}'
        )
    else:
        audit_lines.append('- CROSS_BATCH_V3_SELECTION.md: ✗ 미생성')

    audit_path = BASE / 'HALLUCINATION_AUDIT.md'
    audit_path.write_text('\n'.join(audit_lines) + '\n')
    print(f'Audit report: {audit_path}')
    print(f'Real backtests (SHA256 OK): {n_real}/{n_total}')


if __name__ == '__main__':
    main()
