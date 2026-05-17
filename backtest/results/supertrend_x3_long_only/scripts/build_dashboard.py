#!/usr/bin/env python3
"""
build_dashboard.py — dashboard_v2.html 재빌드 스크립트.

PostgreSQL의 st_combos/st_window_results에서 전체 sweep 데이터를 읽어
dashboard_v2.html을 생성합니다.

Usage (Docker 컨테이너 내부):
    DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
    $DC python3 /result/supertrend_x3_long_only/scripts/build_dashboard.py

    # 출력 경로 지정
    $DC python3 /result/supertrend_x3_long_only/scripts/build_dashboard.py --out /result/supertrend_x3_long_only/dashboard_v2.html
"""
import subprocess
import sys
import time
from pathlib import Path

BUILD_SCRIPT = '/app/scripts/reports/build_dashboard.py'
DEFAULT_OUT = '/result/supertrend_x3_long_only/dashboard_v2.html'
TEMPLATE = '/result/supertrend_x3_long_only/dashboard_template.html'


def main():
    import argparse
    parser = argparse.ArgumentParser(description='dashboard_v2.html 재빌드')
    parser.add_argument('--out', default=DEFAULT_OUT, help='출력 HTML 경로')
    args = parser.parse_args()

    if not Path(BUILD_SCRIPT).exists():
        print(f'ERROR: {BUILD_SCRIPT} not found. Docker 컨테이너 내부에서 실행하세요.', file=sys.stderr)
        sys.exit(1)

    cmd = ['python3', BUILD_SCRIPT, '--out', args.out]
    t = time.time()
    print(f'[build_dashboard] 실행: {" ".join(cmd)}')
    result = subprocess.run(cmd)
    elapsed = time.time() - t

    if result.returncode == 0:
        print(f'[build_dashboard] 완료 ({elapsed:.0f}s) → {args.out}')
    else:
        print(f'[build_dashboard] 실패 (code={result.returncode}, {elapsed:.0f}s)', file=sys.stderr)
        sys.exit(result.returncode)


if __name__ == '__main__':
    main()
