#!/usr/bin/env python3
"""
sweep_scheduler.py — 시간대별 worker 수 자동 조정 스케줄러.

00:00~07:00 KST : 7 workers
07:00~24:00 KST : 2 workers

pg_master.py를 docker compose run으로 실행. 시간 전환 시 중단 후 재시작.
강제 종료로 인한 complete=NULL 플레이스홀더를 정리해 jobs를 재실행 가능 상태로 복원.

Usage (host에서):
    python3 backtest/scripts/optimization/sweep_scheduler.py --sweep v7_st
"""
from __future__ import annotations

import argparse
import datetime
import subprocess
import sys
import time

COMPOSE_FILE = 'backtest/docker/docker-compose.yml'
PG_MASTER    = '/app/scripts/optimization/pg_master.py'
PG_AGGREGATE = '/app/scripts/optimization/pg_aggregate.py'
POLL_INTERVAL = 30  # seconds


def get_workers(night_workers: int, day_workers: int) -> int:
    h = datetime.datetime.now().hour
    return night_workers if 0 <= h < 7 else day_workers


def seconds_to_next_transition() -> float:
    now = datetime.datetime.now()
    h = now.hour
    if 0 <= h < 7:
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
    else:
        target = (now + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
    return max(0.0, (target - now).total_seconds())


def cleanup_stale(sweep_id: str) -> int:
    """강제 종료 후 complete=NULL 플레이스홀더 제거. 반환값: 삭제된 행 수."""
    sql = (
        f"DELETE FROM st_window_results "
        f"WHERE complete IS NULL "
        f"AND combo_pk IN (SELECT pk FROM st_combos WHERE sweep_id='{sweep_id}');"
    )
    result = subprocess.run(
        ['docker', 'exec', 'cryptoengine-backtest-postgres',
         'psql', '-U', 'jesse', '-d', 'jesse_db', '-c', sql],
        capture_output=True, text=True
    )
    for line in result.stdout.splitlines():
        if line.startswith('DELETE'):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                pass
    return 0


def get_progress(sweep_id: str) -> tuple[int, int]:
    """(done_windows, total_windows) 반환."""
    sql = (
        f"SELECT "
        f"  (SELECT COUNT(*) FROM st_window_results wr "
        f"   JOIN st_combos c ON wr.combo_pk=c.pk "
        f"   WHERE c.sweep_id='{sweep_id}' AND wr.complete=TRUE), "
        f"  (SELECT COUNT(*) FROM st_combos WHERE sweep_id='{sweep_id}') * 8;"
    )
    result = subprocess.run(
        ['docker', 'exec', 'cryptoengine-backtest-postgres',
         'psql', '-U', 'jesse', '-d', 'jesse_db', '-t', '-c', sql],
        capture_output=True, text=True
    )
    try:
        parts = result.stdout.strip().split('|')
        return int(parts[0].strip()), int(parts[1].strip())
    except Exception:
        return 0, 0


def start_master(sweep_id: str, workers: int) -> subprocess.Popen:
    cmd = [
        'docker', 'compose', '-f', COMPOSE_FILE, '--profile', 'backtest',
        'run', '--rm', 'backtester',
        'python3', PG_MASTER,
        '--sweep', sweep_id,
        '--workers', str(workers),
        '--no-aggregate',
    ]
    return subprocess.Popen(cmd)


def run_aggregate(sweep_id: str) -> None:
    print(f'[scheduler] Running pg_aggregate for {sweep_id}...', flush=True)
    subprocess.run([
        'docker', 'compose', '-f', COMPOSE_FILE, '--profile', 'backtest',
        'run', '--rm', 'backtester',
        'python3', PG_AGGREGATE, '--sweep', sweep_id,
    ], check=False)


def main() -> int:
    p = argparse.ArgumentParser(description='Time-aware sweep scheduler')
    p.add_argument('--sweep', required=True, help='Sweep ID (e.g. v7_st)')
    p.add_argument('--night-workers', type=int, default=7,
                   help='Workers from 00:00-07:00 (default: 7)')
    p.add_argument('--day-workers', type=int, default=2,
                   help='Workers from 07:00-24:00 (default: 2)')
    args = p.parse_args()

    sweep_id = args.sweep
    night_w  = args.night_workers
    day_w    = args.day_workers

    print(f'[scheduler] sweep={sweep_id}  night={night_w}w(00-07)  day={day_w}w(07-24)',
          flush=True)

    iteration = 0
    while True:
        iteration += 1
        workers  = get_workers(night_w, day_w)
        wait_s   = seconds_to_next_transition()
        next_t   = datetime.datetime.now() + datetime.timedelta(seconds=wait_s)

        done, total = get_progress(sweep_id)
        print(f'\n[scheduler] iter={iteration}  workers={workers}  '
              f'done={done}/{total} ({done*100//total if total else 0}%)  '
              f'next_transition={next_t.strftime("%m/%d %H:%M")}',
              flush=True)

        if done >= total > 0:
            print('[scheduler] All jobs complete. Running aggregate...', flush=True)
            run_aggregate(sweep_id)
            print('[scheduler] Done.', flush=True)
            return 0

        proc = start_master(sweep_id, workers)

        elapsed   = 0.0
        completed = False
        while elapsed < wait_s:
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            if proc.poll() is not None:
                completed = True
                break

        if completed:
            print('[scheduler] pg_master exited naturally. Running aggregate...',
                  flush=True)
            run_aggregate(sweep_id)
            print('[scheduler] Done.', flush=True)
            return 0

        now_str = datetime.datetime.now().strftime('%H:%M')
        new_w   = get_workers(night_w, day_w)
        print(f'[scheduler] Time transition at {now_str}. '
              f'Switching {workers}w → {new_w}w. Stopping pg_master...',
              flush=True)

        proc.terminate()
        try:
            proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        deleted = cleanup_stale(sweep_id)
        if deleted:
            print(f'[scheduler] Cleaned {deleted} stale placeholders.', flush=True)

        time.sleep(3)


if __name__ == '__main__':
    sys.exit(main())
