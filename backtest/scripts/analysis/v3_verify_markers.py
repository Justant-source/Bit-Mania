#!/usr/bin/env python3
"""
V3 백테스트 결과 검증.
다음 단계(보고서 생성) 진입 전 본 스크립트 통과 필수.
모든 variant 폴더에 EXECUTION_SUCCESS.marker 또는 EXECUTION_FAILED.marker 있어야 함.
SUCCESS 마커면 stats.json SHA256 재검증.

Exit codes:
  0 = 모든 마커 있음, SUCCESS ≥ 1개, BnH 완료 → 다음 단계 진입 가능
  2 = 마커 없는 variant 존재 (미완료)
  3 = 마커 있으나 성공 0개 (전체 실패)
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

BASE = Path('/result/v3')

EXPECTED = {
    'batch_1': ['bbpb', 'bbwp', 'stoch'],
    'batch_2': ['momentum_ma', 'supertrend', 'tradeiq_psar_ha'],
    'batch_3': ['trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce'],
}
VARIANTS = ['bidirectional', 'long_only']


def verify_marker(variant_dir: Path) -> tuple[str, dict | None]:
    """Returns (status, stats_dict_or_None)."""
    success = variant_dir / 'EXECUTION_SUCCESS.marker'
    failed  = variant_dir / 'EXECUTION_FAILED.marker'

    if success.exists() and failed.exists():
        return 'CONFLICT_BOTH_MARKERS', None
    if not success.exists() and not failed.exists():
        return 'NO_MARKER', None
    if failed.exists():
        return 'FAILED', None

    stats_path = variant_dir / 'stats.json'
    if not stats_path.exists():
        return 'SUCCESS_BUT_NO_STATS', None

    expected_sha = None
    for line in success.read_text().splitlines():
        if line.startswith('stats_sha256:'):
            expected_sha = line.split(':', 1)[1].strip()
            break
    actual_sha = hashlib.sha256(stats_path.read_bytes()).hexdigest()
    if expected_sha and expected_sha != actual_sha:
        return 'SHA256_MISMATCH', None

    return 'SUCCESS', json.loads(stats_path.read_text())


def main(batch_filter: str | None = None) -> int:
    print('=' * 60)
    print('V3 백테스트 결과 검증')
    print('=' * 60)

    n_total = n_success = n_failed = n_missing = 0

    for batch, strategies in EXPECTED.items():
        if batch_filter and batch != batch_filter:
            continue
        for strategy in strategies:
            for variant in VARIANTS:
                n_total += 1
                vd = BASE / batch / strategy / variant
                status, _ = verify_marker(vd)
                mark = '✓' if status == 'SUCCESS' else ('✗' if status == 'FAILED' else '?')
                print(f'  {mark} {batch}/{strategy}/{variant}: {status}')
                if status == 'SUCCESS':
                    n_success += 1
                elif status == 'FAILED':
                    n_failed += 1
                else:
                    n_missing += 1

    bnh_dir = BASE / 'buy_and_hold'
    bnh_status, _ = verify_marker(bnh_dir)
    print(f'\n  BnH: {bnh_status}')

    print('=' * 60)
    print(f'Summary: SUCCESS={n_success}, FAILED={n_failed}, MISSING={n_missing}, TOTAL={n_total}')
    print('=' * 60)

    if n_missing > 0:
        print('\n⚠️  다음 단계 진입 불가: 마커 없는 variant 존재')
        return 2

    if not batch_filter and bnh_status != 'SUCCESS':
        print('\n⚠️  BnH 미완료 — Tier 분류 불가')
        return 2

    if n_success == 0:
        print('\n⚠️  성공한 백테스트 0개 — 보고서 의미 없음')
        return 3

    print('\n✓ 다음 단계 진입 가능')
    return 0


if __name__ == '__main__':
    batch_filter = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(main(batch_filter))
