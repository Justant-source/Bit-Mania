#!/usr/bin/env python3
"""
run_supertrend_dashboard.py — supertrend_sweep_dashboard.html 재빌드 래퍼.

PostgreSQL의 st_combos/st_window_results에서 전체 sweep 데이터를 읽어
supertrend_sweep_dashboard.html을 생성합니다.

Usage (Docker 컨테이너 내부):
    DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
    $DC python3 /dashboards/script/run_supertrend_dashboard.py

    # 출력 경로 지정
    $DC python3 /dashboards/script/run_supertrend_dashboard.py --out /dashboards/supertrend_sweep_dashboard.html
"""
import subprocess
import sys
import time
from pathlib import Path

BUILD_SCRIPT = '/dashboards/script/build_supertrend_dashboard.py'
DEFAULT_OUT = '/dashboards/supertrend_sweep_dashboard.html'
TEMPLATE = '/dashboards/script/supertrend_dashboard_template.html'


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
