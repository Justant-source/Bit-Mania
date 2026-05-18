#!/usr/bin/env python3
"""
pg_supervisor.py — 시간대별 worker 수 자동 조절 supervisor.

KST 00:00~07:00 : 7 workers
KST 07:00~24:00 : 4 workers

worker 개별 프로세스를 직접 관리하며, stop file로 graceful scale-down 수행.
예기치 않게 종료된 worker는 자동 재시작.
모든 작업 완료 시 pg_aggregate.py 실행 후 종료.

Usage (Docker 컨테이너 내부):
    python3 /result/supertrend_x3_long_only/scripts/pg_supervisor.py --sweep v10
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, '/app/scripts/db')
from _common import connect

WORKER_SCRIPT   = '/app/scripts/optimization/pg_worker.py'
AGGREGATE_SCRIPT = '/app/scripts/optimization/pg_aggregate.py'

KST = timezone(timedelta(hours=9))
NIGHT_WORKERS = 7   # 00:00~07:00 KST
DAY_WORKERS   = 4   # 07:00~24:00 KST
POLL_INTERVAL = 30  # seconds


def kst_now() -> datetime:
    return datetime.now(KST)


def target_worker_count() -> int:
    h = kst_now().hour
    return NIGHT_WORKERS if 0 <= h < 7 else DAY_WORKERS


def stop_file(worker_id: int) -> Path:
    return Path(f'/tmp/pg_stop_worker_{worker_id}')


def start_worker(worker_id: int, sweep_id: str) -> subprocess.Popen:
    stop_file(worker_id).unlink(missing_ok=True)
    return subprocess.Popen(
        ['python3', WORKER_SCRIPT, '--sweep', sweep_id, '--worker-id', str(worker_id)],
        cwd='/app',
    )


def get_progress(sweep_id: str) -> tuple[int, int]:
    try:
        conn = connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) * 8 FROM st_combos WHERE sweep_id = %s", (sweep_id,)
            )
            total = cur.fetchone()[0]
            cur.execute(
                """SELECT COUNT(*) FROM st_window_results wr
                   JOIN st_combos c ON c.pk = wr.combo_pk
                   WHERE c.sweep_id = %s AND wr.complete = TRUE""",
                (sweep_id,),
            )
            done = cur.fetchone()[0]
        conn.close()
        return done, total - done
    except Exception as e:
        print(f'[SUPERVISOR] progress error: {e}', flush=True)
        return 0, 1  # assume still pending


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--sweep', required=True)
    args = p.parse_args()
    sweep_id = args.sweep

    # 기존 stop file 초기화
    for i in range(NIGHT_WORKERS):
        stop_file(i).unlink(missing_ok=True)

    n_start = target_worker_count()
    workers: dict[int, subprocess.Popen] = {}
    for i in range(n_start):
        workers[i] = start_worker(i, sweep_id)

    initial_done, _ = get_progress(sweep_id)
    print(f'[SUPERVISOR] sweep={sweep_id} 시작 | '
          f'{kst_now().strftime("%H:%M KST")} | 초기 workers={n_start} | '
          f'이미 완료={initial_done:,}', flush=True)

    t_start = time.time()

    while True:
        time.sleep(POLL_INTERVAL)

        # 종료된 worker 감지 후 상태 정리
        exited = [wid for wid, p in workers.items() if p.poll() is not None]
        for wid in exited:
            del workers[wid]
            stop_file(wid).unlink(missing_ok=True)

        # 모든 job 완료 여부 확인
        done, pending = get_progress(sweep_id)
        elapsed_m = (time.time() - t_start) / 60
        new_done = done - initial_done  # supervisor 시작 이후 새로 완료된 것만

        if pending == 0 and len(workers) == 0:
            print(f'[SUPERVISOR] 모든 작업 완료 ({done:,} backtests, {elapsed_m:.1f}분)', flush=True)
            break

        target = target_worker_count()

        # Scale UP
        for wid in range(target):
            if wid not in workers:
                workers[wid] = start_worker(wid, sweep_id)
                print(f'[SUPERVISOR] worker {wid} 시작 (scale up to {target})', flush=True)

        # Scale DOWN — stop file로 graceful 종료 요청
        for wid in sorted(workers.keys()):
            if wid >= target:
                if not stop_file(wid).exists():
                    stop_file(wid).touch()
                    print(f'[SUPERVISOR] worker {wid} stop 신호 전송 (scale down to {target})', flush=True)

        # 진행 로그 (rate/ETA는 supervisor 시작 이후 신규 완료 기준)
        alive = len(workers)
        rate = new_done / elapsed_m if elapsed_m > 0 else 0
        eta_m = pending / rate if rate > 0 else float('inf')
        print(
            f'[SUPERVISOR] {kst_now().strftime("%H:%M KST")} | '
            f'workers={alive} (target={target}) | '
            f'{done:,}/{done+pending:,} | +{new_done} new | rate={rate:.1f}/min | ETA={eta_m:.0f}m',
            flush=True,
        )

    # pg_aggregate 실행
    print('[SUPERVISOR] pg_aggregate.py 실행 중...', flush=True)
    result = subprocess.run(
        ['python3', AGGREGATE_SCRIPT, '--sweep', sweep_id], cwd='/app'
    )
    if result.returncode == 0:
        print('[SUPERVISOR] aggregate 완료.', flush=True)
    else:
        print(f'[SUPERVISOR] aggregate 실패 (code={result.returncode})', flush=True)

    return 0


if __name__ == '__main__':
    sys.exit(main())
