#!/usr/bin/env python3
"""
SupertrendStrategy 4h 3x Long-Only 재실행 스크립트.

실행할 변형:
  - long_only_x3      : 기본 파라미터 (st_factor=3.0 기본값)
  - long_only_x3_v2   : v2 파라미터
  - long_only_x3_164  : v6_st 검증된 top combo #164
  - long_only_x3_173  : v6_st 검증된 top combo #173
  - long_only_x3_176  : v6_st 검증된 top combo #176

기존 v6 TP/SL 테스트 콤보(566/875~1286)를 삭제하고 올바른 콤보로 교체.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

START       = '2017-08-18'
END         = '2026-04-30'
BALANCE     = '10000'
FEE         = '0.00055'
PARALLELISM = 5
TIMEOUT_S   = 1800

BASE = Path('/result/7-strategies/supertrend/4h')

HP_DEFAULT = json.dumps({
    'st_factor': 3.0, 'st_period': 7, 'fast_ema_len': 7,
    'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0,
})
HP_V2 = json.dumps({
    'st_factor': 2.3, 'st_period': 8, 'fast_ema_len': 10,
    'slow_ema_len': 20, 'direction_ema_len': 250, 'atr_mult': 3.0,
})
# v6_st 검증된 top-3 (covid_crash_analysis에서 ZERO_RISK 확인)
HP_164 = json.dumps({
    'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
    'slow_ema_len': 25, 'direction_ema_len': 230, 'atr_mult': 3.2,
})
HP_173 = json.dumps({
    'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
    'slow_ema_len': 27, 'direction_ema_len': 230, 'atr_mult': 3.2,
})
HP_176 = json.dumps({
    'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
    'slow_ema_len': 27, 'direction_ema_len': 250, 'atr_mult': 3.2,
})

JOBS = [
    ('long_only_x3',     HP_DEFAULT),
    ('long_only_x3_v2',  HP_V2),
    ('long_only_x3_164', HP_164),
    ('long_only_x3_173', HP_173),
    ('long_only_x3_176', HP_176),
]

# 기존 잘못된 v6 TP/SL 콤보 디렉토리 목록 (삭제 대상)
STALE_DIRS = [
    'long_only_x3_566',  'long_only_x3_875',  'long_only_x3_878',
    'long_only_x3_881',  'long_only_x3_956',  'long_only_x3_959',
    'long_only_x3_962',  'long_only_x3_1280', 'long_only_x3_1283',
    'long_only_x3_1286',
    # 대응하는 1x 버전도 삭제
    'long_only_566',     'long_only_875',      'long_only_878',
    'long_only_881',     'long_only_956',      'long_only_959',
    'long_only_962',     'long_only_1280',     'long_only_1283',
    'long_only_1286',
]


def purge_stale():
    removed = 0
    for name in STALE_DIRS:
        d = BASE / name
        if d.exists():
            shutil.rmtree(d)
            print(f'[PURGE] {name}', flush=True)
            removed += 1
    if removed == 0:
        print('[PURGE] 삭제할 stale 디렉토리 없음', flush=True)
    return removed


def run_one(variant_name: str, hp_json: str) -> dict:
    out_dir = BASE / variant_name
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    cmd = [
        'python3', '/app/scripts/runners/run_intrabar_backtest.py',
        '--strategy', 'SupertrendStrategy',
        '--timeframe', '4h',
        '--variant', 'long_only',
        '--leverage', '3',
        '--start', START,
        '--end', END,
        '--balance', BALANCE,
        '--fee', FEE,
        '--hp-json', hp_json,
        '--output', str(out_dir),
    ]

    print(f'[START] {variant_name}', flush=True)
    try:
        subprocess.run(cmd, check=False, timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        print(f'[TIMEOUT] {variant_name}  {elapsed:.0f}s', flush=True)
        return {'tag': variant_name, 'ok': False, 'elapsed': elapsed}
    except Exception as e:
        elapsed = time.time() - t0
        print(f'[ERROR] {variant_name}: {e}', flush=True)
        return {'tag': variant_name, 'ok': False, 'elapsed': elapsed}

    elapsed = time.time() - t0
    stats_file = out_dir / 'stats.json'
    ok = stats_file.exists()
    if ok:
        try:
            s = json.loads(stats_file.read_text())
            print(f'[OK] {variant_name}  CAGR={s.get("cagr_pct",0):.1f}%  MDD={s.get("max_drawdown_pct",0):.1f}%  Sharpe={s.get("sharpe_ratio",0):.3f}  {elapsed:.0f}s', flush=True)
        except Exception:
            print(f'[OK] {variant_name}  {elapsed:.0f}s', flush=True)
    else:
        print(f'[FAIL] {variant_name}  {elapsed:.0f}s', flush=True)
    return {'tag': variant_name, 'ok': ok, 'elapsed': elapsed}


def main():
    print('=== SupertrendStrategy 4h 3x Long-Only 재실행 ===')
    print(f'기간: {START} ~ {END} | 레버리지: 3x | 수수료: {FEE}')
    print()

    # 1. 기존 잘못된 콤보 삭제
    print('--- Step 1: 잘못된 v6 TP/SL 콤보 삭제 ---')
    purge_stale()
    print()

    # 2. 새 백테스트 실행
    print(f'--- Step 2: {len(JOBS)}개 변형 백테스트 ({PARALLELISM} 병렬) ---')
    t_total = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=PARALLELISM) as exe:
        futs = {exe.submit(run_one, name, hp): name for name, hp in JOBS}
        for fut in as_completed(futs):
            results.append(fut.result())

    elapsed_total = time.time() - t_total
    ok_count = sum(1 for r in results if r['ok'])
    print(f'\n=== 완료: {ok_count}/{len(JOBS)} 성공  총 {elapsed_total:.0f}s ===')

    if ok_count < len(JOBS):
        failed = [r['tag'] for r in results if not r['ok']]
        print(f'실패: {failed}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
