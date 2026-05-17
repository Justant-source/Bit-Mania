#!/usr/bin/env python3
"""
run_sweep.py — Supertrend 4h 3x Long-Only sweet spot search.

완전한 파이프라인을 한 번에 실행:
  1. pg_generate_grid  — PG에 파라미터 그리드 삽입
  2. pg_master         — 다중 워커로 백테스트 병렬 실행 + pg_aggregate 자동 호출
  3. build_dashboard   — dashboard_v2.html 재빌드

Usage (Docker 내부에서 실행):
    DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
    $DC python3 /result/supertrend_x3_long_only/scripts/run_sweep.py \\
        --sweep v9 \\
        --grid-json '{"st_factor":[2.4,2.5],"st_period":[8,9],"fast_ema_len":[7,8],"slow_ema_len":[25,27],"direction_ema_len":[330,360,400,450],"atr_mult":[3.1,3.2]}' \\
        --workers 6

    # 사전 확인만 (백테스트 미실행)
    $DC python3 /result/supertrend_x3_long_only/scripts/run_sweep.py --sweep v9 --grid-json '...' --dry

파라미터 가이드 (Supertrend 4h 3x, 현재 sweet spot 기준):
    st_factor:         2.4~2.6   (Supertrend ATR 배수)
    st_period:         7~10      (Supertrend ATR 기간)
    fast_ema_len:      6~9       (진입 신호 빠른 EMA)
    slow_ema_len:      24~30     (진입 신호 느린 EMA)
    direction_ema_len: 250~500   (방향 필터 EMA — v8 신호: 길수록 성능 향상)
    atr_mult:          3.0~3.3   (ATR stop-loss 배수)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

GENERATE_GRID = '/app/scripts/optimization/pg_generate_grid.py'
MASTER = '/app/scripts/optimization/pg_master.py'
BUILD_DASHBOARD = '/app/scripts/reports/build_dashboard.py'
DASHBOARD_OUT = '/result/supertrend_x3_long_only/dashboard_v2.html'


def run(cmd: list[str], label: str) -> int:
    print(f'\n[{label}] 실행: {" ".join(cmd)}', flush=True)
    t = time.time()
    result = subprocess.run(cmd)
    elapsed = time.time() - t
    status = '완료' if result.returncode == 0 else f'실패 (code={result.returncode})'
    print(f'[{label}] {status} ({elapsed:.0f}s)', flush=True)
    return result.returncode


def count_combos(grid: dict) -> int:
    total = 1
    for v in grid.values():
        total *= len(v)
    return total


def main():
    parser = argparse.ArgumentParser(description='Supertrend 3x sweet spot 파라미터 스윕')
    parser.add_argument('--sweep', required=True, help='스윕 ID (예: v9)')
    parser.add_argument('--grid-json', required=True,
                        help='파라미터 그리드 JSON (예: {"st_factor":[2.4,2.5],...})')
    parser.add_argument('--description', default='',
                        help='스윕 설명 (선택)')
    parser.add_argument('--workers', type=int, default=6,
                        help='병렬 워커 수 (기본 6)')
    parser.add_argument('--dry', action='store_true',
                        help='그리드 조합 수만 출력하고 종료')
    parser.add_argument('--skip-dashboard', action='store_true',
                        help='대시보드 재빌드 건너뜀')
    args = parser.parse_args()

    # 그리드 JSON 파싱
    try:
        grid = json.loads(args.grid_json)
    except json.JSONDecodeError as e:
        print(f'ERROR: --grid-json 파싱 실패: {e}', file=sys.stderr)
        sys.exit(1)

    n_combos = count_combos(grid)
    n_backtests = n_combos * 8
    est_minutes = n_backtests / 32  # ~32 bt/min @ 6 workers

    print(f'=== Supertrend 3x Sweet Spot Sweep: {args.sweep} ===')
    print(f'파라미터: {json.dumps(grid, ensure_ascii=False)}')
    print(f'조합: {n_combos} combos × 8 windows = {n_backtests} backtests')
    print(f'예상 시간: ~{est_minutes:.0f}분 ({args.workers} workers @ 32 bt/min)')

    if args.dry:
        print('\n[DRY] --dry 플래그: 그리드 확인만 완료. 실행하지 않습니다.')
        return 0

    t_total = time.time()

    # Step 1: 그리드 생성
    gen_cmd = [
        'python3', GENERATE_GRID,
        '--sweep', args.sweep,
        '--grid-json', args.grid_json,
    ]
    if args.description:
        gen_cmd += ['--description', args.description]

    rc = run(gen_cmd, 'Step 1: generate_grid')
    if rc != 0:
        print(f'ERROR: 그리드 생성 실패. 이미 존재하는 sweep이면 --sweep 변경 후 재시도.', file=sys.stderr)
        sys.exit(rc)

    # Step 2: 백테스트 실행 (pg_master → pg_aggregate 자동 호출)
    master_cmd = [
        'python3', MASTER,
        '--sweep', args.sweep,
        '--workers', str(args.workers),
    ]
    rc = run(master_cmd, 'Step 2: run backtests + aggregate')
    if rc != 0:
        print(f'ERROR: 백테스트 실패. PG에서 잔존 행 확인 후 재시도 가능.', file=sys.stderr)
        sys.exit(rc)

    # Step 3: 대시보드 재빌드
    if not args.skip_dashboard:
        dash_cmd = [
            'python3', BUILD_DASHBOARD,
            '--out', DASHBOARD_OUT,
        ]
        run(dash_cmd, 'Step 3: build dashboard')

    total_elapsed = time.time() - t_total
    print(f'\n=== 완료: {args.sweep} sweep ({total_elapsed:.0f}s = {total_elapsed/60:.1f}분) ===')
    print(f'대시보드: {DASHBOARD_OUT}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
