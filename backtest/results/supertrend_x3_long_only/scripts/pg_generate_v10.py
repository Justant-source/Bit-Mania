#!/usr/bin/env python3
"""
pg_generate_v10.py — v10 sweep: dir_ema 200-260 하단 미완료 조합 삽입.

전체 파라미터 공간 18,750 조합 중 이미 8윈도우 전부 완료(complete=TRUE)된
조합을 skip하고, 나머지만 v10 sweep에 삽입합니다.

Usage (Docker 컨테이너 내부):
    python3 /result/supertrend_x3_long_only/scripts/pg_generate_v10.py [--dry]
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / 'scripts' / 'db'))
sys.path.insert(0, '/app/scripts/db')
from _common import connect

SWEEP_ID    = 'v10'
DESCRIPTION = 'dir_ema 200-260 하단 탐색 — 전체 파라미터 공간 미완료 조합'
LEVERAGE    = 3.0

ST_FACTORS = [2.3, 2.4, 2.5, 2.6, 2.7]
ST_PERIODS = [5, 6, 7, 8, 9]
FAST_EMAS  = [7, 8, 9, 10, 11]
SLOW_EMAS  = [20, 25, 26, 27, 28, 30]
DIR_EMAS   = [200, 230, 240, 250, 260]
ATR_MULTS  = [2.5, 3.0, 3.1, 3.2, 3.5]


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--dry', action='store_true', help='조합 수만 출력, DB 변경 없음')
    args = p.parse_args()

    total = (len(ST_FACTORS) * len(ST_PERIODS) * len(FAST_EMAS)
             * len(SLOW_EMAS) * len(DIR_EMAS) * len(ATR_MULTS))
    print(f'전체 목표 조합: {total:,}')

    conn = connect()
    conn.autocommit = False

    # 이미 8윈도우 모두 complete=TRUE인 파라미터 조합 조회
    print('DB에서 완료된 조합 확인 중...')
    with conn.cursor() as cur:
        cur.execute('''
            SELECT c.st_factor, c.st_period, c.fast_ema_len, c.slow_ema_len,
                   c.direction_ema_len, c.atr_mult
            FROM st_combos c
            WHERE c.st_factor = ANY(%s)
              AND c.st_period = ANY(%s)
              AND c.fast_ema_len = ANY(%s)
              AND c.slow_ema_len = ANY(%s)
              AND c.direction_ema_len = ANY(%s)
              AND c.atr_mult = ANY(%s)
              AND (
                  SELECT COUNT(*) FROM st_window_results wr
                  WHERE wr.combo_pk = c.pk AND wr.complete = TRUE
              ) = 8
        ''', (
            [float(x) for x in ST_FACTORS],
            ST_PERIODS, FAST_EMAS, SLOW_EMAS, DIR_EMAS,
            [float(x) for x in ATR_MULTS],
        ))
        done = set()
        for row in cur.fetchall():
            done.add((float(row[0]), int(row[1]), int(row[2]),
                      int(row[3]), int(row[4]), float(row[5])))

    print(f'이미 완료된 조합 (skip): {len(done):,}')

    # 미완료 조합 추출
    missing = []
    for sf, sp, fe, se, de, am in itertools.product(
            ST_FACTORS, ST_PERIODS, FAST_EMAS, SLOW_EMAS, DIR_EMAS, ATR_MULTS):
        key = (float(sf), int(sp), int(fe), int(se), int(de), float(am))
        if key not in done:
            missing.append((sf, sp, fe, se, de, am))

    print(f'v10에 삽입할 조합: {len(missing):,}')
    print(f'총 backtests: {len(missing) * 8:,}')

    if args.dry:
        print('[DRY] DB 변경 없음.')
        conn.close()
        return 0

    # sweep 메타데이터 upsert
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO st_sweeps(sweep_id, description, leverage, variant, grid_json, n_combos)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(sweep_id) DO UPDATE SET
              description = EXCLUDED.description,
              n_combos    = EXCLUDED.n_combos
        ''', (
            SWEEP_ID, DESCRIPTION, LEVERAGE, 'long_only',
            json.dumps({'st_factor': ST_FACTORS, 'st_period': ST_PERIODS,
                        'fast_ema_len': FAST_EMAS, 'slow_ema_len': SLOW_EMAS,
                        'direction_ema_len': DIR_EMAS, 'atr_mult': ATR_MULTS}),
            len(missing),
        ))
    conn.commit()
    print(f'✓ sweep {SWEEP_ID} 메타데이터 저장')

    # 조합 삽입
    inserted = 0
    for combo_id, (sf, sp, fe, se, de, am) in enumerate(missing):
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO st_combos(
                  sweep_id, combo_id, st_factor, st_period, fast_ema_len,
                  slow_ema_len, direction_ema_len, atr_mult, sl_margin_pct,
                  n_complete, n_positive, mean_cagr, std_cagr, worst_window,
                  worst_mdd, mean_mdd, total_trades, liquidated,
                  worst_mdd_recent, mean_cagr_recent,
                  tier1, tier2, tier3, tier4, tier_pass,
                  tier_a, tier_b, tier_c, final_tier,
                  safety_score, plateau_quality, plateau_score, sweet_spot_score,
                  cross_val_status, xref_json, raw_json
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, 0.0,
                  NULL, NULL, NULL, NULL, NULL,
                  NULL, NULL, NULL, FALSE,
                  NULL, NULL,
                  FALSE, FALSE, FALSE, FALSE, FALSE,
                  FALSE, FALSE, FALSE, NULL,
                  NULL, NULL, 0, NULL,
                  'NEW', NULL, NULL
                )
                ON CONFLICT(sweep_id, combo_id) DO NOTHING
            ''', (SWEEP_ID, combo_id,
                  float(sf), int(sp), int(fe), int(se), int(de), float(am)))
            inserted += cur.rowcount
        conn.commit()

        if (combo_id + 1) % 1000 == 0:
            print(f'  ... {combo_id + 1:,} / {len(missing):,} 삽입 중')

    conn.close()
    print(f'✓ 총 {inserted:,} combos 삽입 완료 → sweep {SWEEP_ID}')
    print(f'✓ 실행 준비 완료: pg_master.py --sweep {SWEEP_ID} --workers 4')
    return 0


if __name__ == '__main__':
    sys.exit(main())
