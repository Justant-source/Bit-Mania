#!/usr/bin/env python3
"""
sweep_scheduler.py — 시간대별 worker 수 자동 조정 스케줄러.

00:00~06:00 KST : night workers (기본 6), cores 3–7
06:00~24:00 KST : day workers (기본 2), cores 6–7

운영 컨테이너와 CPU를 나누기 위해 compose overlay + nice 19 + 낮은 cpu_shares를 쓴다.
pg_master.py를 docker compose run --no-deps로 실행. 시간 전환 시 중단 후 재시작.
강제 종료로 인한 complete=NULL 플레이스홀더를 정리해 jobs를 재실행 가능 상태로 복원.

Usage (host, repo root or any cwd):
    python3 backtest/scripts/optimization/sweep_scheduler.py --sweep v10_notp \
        --night-workers 6 --day-workers 2 --night-end-hour 6
"""
from __future__ import annotations

import argparse
import datetime
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO_ROOT / 'backtest/docker/docker-compose.yml'
OVERLAY_DAY = REPO_ROOT / 'backtest/docker/compose.sweep-day.yml'
OVERLAY_NIGHT = REPO_ROOT / 'backtest/docker/compose.sweep-night.yml'
PG_MASTER = '/app/scripts/optimization/pg_master.py'
PG_AGGREGATE = '/app/scripts/optimization/pg_aggregate.py'
BUILD_DASHBOARD = '/dashboards/script/build_supertrend_dashboard.py'
POLL_INTERVAL = 30  # seconds
BACKTESTER_FILTER = 'cryptoengine-backtest-backtester-run-'


def is_night(now: datetime.datetime, night_end_hour: int) -> bool:
    return 0 <= now.hour < night_end_hour


def get_workers(night_workers: int, day_workers: int, night_end_hour: int) -> int:
    return night_workers if is_night(datetime.datetime.now(), night_end_hour) else day_workers


def overlay_for_now(night_end_hour: int) -> Path:
    return OVERLAY_NIGHT if is_night(datetime.datetime.now(), night_end_hour) else OVERLAY_DAY


def seconds_to_next_transition(night_end_hour: int) -> float:
    now = datetime.datetime.now()
    if is_night(now, night_end_hour):
        target = now.replace(hour=night_end_hour, minute=0, second=0, microsecond=0)
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


def stop_backtester_containers() -> list[str]:
    """실행 중인 backtester-run 컨테이너만 stop. 운영 cryptoengine 이름은 매칭하지 않음."""
    result = subprocess.run(
        ['docker', 'ps', '--filter', f'name={BACKTESTER_FILTER}',
         '--format', '{{.Names}}'],
        capture_output=True, text=True
    )
    names = [n.strip() for n in result.stdout.splitlines() if n.strip()]
    if names:
        subprocess.run(['docker', 'stop', '-t', '30'] + names,
                       capture_output=True, timeout=60)
    return names


def start_master(sweep_id: str, workers: int, overlay: Path) -> subprocess.Popen:
    cmd = [
        'nice', '-n', '19',
        'docker', 'compose',
        '-f', str(COMPOSE_FILE),
        '-f', str(overlay),
        '--profile', 'backtest',
        'run', '--rm', '--no-deps', '-T',
        'backtester',
        'nice', '-n', '19',
        'python3', PG_MASTER,
        '--sweep', sweep_id,
        '--workers', str(workers),
        '--no-aggregate',
    ]
    return subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT / 'backtest/docker'),
        stdout=None,
        stderr=None,
        env={**os.environ, 'COMPOSE_IGNORE_ORPHANS': '1'},
    )


def run_aggregate(sweep_id: str) -> None:
    print(f'[scheduler] Running pg_aggregate for {sweep_id}...', flush=True)
    overlay = overlay_for_now(6)
    subprocess.run([
        'nice', '-n', '19',
        'docker', 'compose',
        '-f', str(COMPOSE_FILE),
        '-f', str(overlay),
        '--profile', 'backtest',
        'run', '--rm', '--no-deps', '-T',
        'backtester',
        'nice', '-n', '19',
        'python3', PG_AGGREGATE, '--sweep', sweep_id,
    ], cwd=str(REPO_ROOT / 'backtest/docker'), check=False)


def run_dashboard(sweep_id: str) -> None:
    print(f'[scheduler] Rebuilding sweep dashboard for {sweep_id}...', flush=True)
    overlay = overlay_for_now(6)
    subprocess.run([
        'nice', '-n', '19',
        'docker', 'compose',
        '-f', str(COMPOSE_FILE),
        '-f', str(overlay),
        '--profile', 'backtest',
        'run', '--rm', '--no-deps', '-T',
        'backtester',
        'nice', '-n', '19',
        'python3', BUILD_DASHBOARD,
        '--sweeps', sweep_id,
    ], cwd=str(REPO_ROOT / 'backtest/docker'), check=False)


def main() -> int:
    p = argparse.ArgumentParser(description='Time-aware low-priority sweep scheduler')
    p.add_argument('--sweep', required=True, help='Sweep ID (e.g. v10_notp)')
    p.add_argument('--night-workers', type=int, default=6,
                   help='Workers from 00:00 until --night-end-hour (default: 6)')
    p.add_argument('--day-workers', type=int, default=2,
                   help='Workers after --night-end-hour (default: 2)')
    p.add_argument('--night-end-hour', type=int, default=6,
                   help='KST hour when night window ends (default: 6 → 00-06)')
    args = p.parse_args()

    sweep_id = args.sweep
    night_w = args.night_workers
    day_w = args.day_workers
    night_end = args.night_end_hour

    print(
        f'[scheduler] sweep={sweep_id}  night={night_w}w(00-{night_end:02d})  '
        f'day={day_w}w({night_end:02d}-24)  '
        f'overlays=day:{OVERLAY_DAY.name} night:{OVERLAY_NIGHT.name}',
        flush=True,
    )

    iteration = 0
    while True:
        iteration += 1
        workers = get_workers(night_w, day_w, night_end)
        overlay = overlay_for_now(night_end)
        wait_s = seconds_to_next_transition(night_end)
        next_t = datetime.datetime.now() + datetime.timedelta(seconds=wait_s)

        done, total = get_progress(sweep_id)
        print(f'\n[scheduler] iter={iteration}  workers={workers}  overlay={overlay.name}  '
              f'done={done}/{total} ({done * 100 // total if total else 0}%)  '
              f'next_transition={next_t.strftime("%m/%d %H:%M")}',
              flush=True)

        if done >= total > 0:
            print('[scheduler] All jobs complete. Running aggregate...', flush=True)
            run_aggregate(sweep_id)
            run_dashboard(sweep_id)
            print('[scheduler] Done.', flush=True)
            return 0

        leftovers = stop_backtester_containers()
        if leftovers:
            print(f'[scheduler] Stopped leftover containers: {leftovers}', flush=True)

        proc = start_master(sweep_id, workers, overlay)

        elapsed = 0.0
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
            run_dashboard(sweep_id)
            print('[scheduler] Done.', flush=True)
            return 0

        now_str = datetime.datetime.now().strftime('%H:%M')
        new_w = get_workers(night_w, day_w, night_end)
        print(f'[scheduler] Time transition at {now_str}. '
              f'Switching {workers}w → {new_w}w. Stopping pg_master...',
              flush=True)

        proc.terminate()
        try:
            proc.wait(timeout=45)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        stopped = stop_backtester_containers()
        if stopped:
            print(f'[scheduler] Stopped containers: {stopped}', flush=True)

        deleted = cleanup_stale(sweep_id)
        if deleted:
            print(f'[scheduler] Cleaned {deleted} stale placeholders.', flush=True)

        time.sleep(3)


if __name__ == '__main__':
    sys.exit(main())
