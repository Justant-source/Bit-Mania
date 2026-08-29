#!/usr/bin/env python3
"""
Copy combo parameters from an existing sweep into a new sweep_id.

Does not copy st_window_results (those must be re-run under current strategy code).
Does not modify the source sweep.

Usage (inside backtester, or via docker exec postgres + this script in-container):
    python3 pg_clone_sweep.py --from v7_st --to v10_notp \
        --description "ATR SL-only re-run of v7_st"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'db'))
from _common import connect


def main() -> int:
    p = argparse.ArgumentParser(description='Clone combo params to a new sweep_id')
    p.add_argument('--from', dest='src', required=True)
    p.add_argument('--to', dest='dst', required=True)
    p.add_argument('--description', default='')
    args = p.parse_args()
    src, dst = args.src, args.dst
    if src == dst:
        print('ERROR: --from and --to must differ', file=sys.stderr)
        return 1

    conn = connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT description, leverage, variant, grid_json, n_combos '
                'FROM st_sweeps WHERE sweep_id=%s',
                (src,),
            )
            row = cur.fetchone()
            if row is None:
                print(f'ERROR: source sweep {src} not found', file=sys.stderr)
                return 1
            src_desc, leverage, variant, grid_json, n_combos = row
            desc = args.description or (
                f'{src} clone under current SupertrendStrategy ({src_desc})'
            )

            cur.execute(
                '''
                INSERT INTO st_sweeps(sweep_id, description, leverage, variant, grid_json, n_combos)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sweep_id) DO UPDATE SET
                  description=EXCLUDED.description,
                  n_combos=EXCLUDED.n_combos
                ''',
                (dst, desc, leverage, variant, json.dumps(grid_json, default=str)
                 if grid_json is not None else None, n_combos),
            )

            cur.execute(
                '''
                INSERT INTO st_combos(
                  sweep_id, combo_id, st_factor, st_period, fast_ema_len,
                  slow_ema_len, direction_ema_len, atr_mult, sl_margin_pct,
                  tp_atr_mult, sl_atr_mult, liquidated, cross_val_status, xref_json
                )
                SELECT
                  %s, combo_id, st_factor, st_period, fast_ema_len,
                  slow_ema_len, direction_ema_len, atr_mult, sl_margin_pct,
                  tp_atr_mult, sl_atr_mult, FALSE, 'NEW',
                  jsonb_build_object('source_sweep', sweep_id, 'source_combo_id', combo_id,
                                     'policy', 'atr_sl_only')
                FROM st_combos
                WHERE sweep_id=%s
                ON CONFLICT (sweep_id, combo_id) DO NOTHING
                ''',
                (dst, src),
            )
            inserted = cur.rowcount
            cur.execute('SELECT COUNT(*) FROM st_combos WHERE sweep_id=%s', (dst,))
            total = cur.fetchone()[0]
            cur.execute(
                'SELECT COUNT(*) FROM st_window_results wr '
                'JOIN st_combos c ON wr.combo_pk=c.pk WHERE c.sweep_id=%s',
                (dst,),
            )
            windows = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f'ERROR: {e}', file=sys.stderr)
        return 1
    finally:
        conn.close()

    print(f'Cloned {src} → {dst}')
    print(f'  inserted_this_run={inserted}  combos_in_dst={total}  window_rows={windows}')
    print('  window results were not copied; pg_worker will fill them.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
