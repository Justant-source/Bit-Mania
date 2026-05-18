#!/usr/bin/env python3
"""
pg_generate_grid.py — Generate sweep grid into PostgreSQL st_sweeps + st_combos.

Window results are tracked via a 'pending' approach: for each combo×window pair
that doesn't have a row in st_window_results yet, the worker will claim and run it.

Usage:
    python3 pg_generate_grid.py --sweep my_sweep [--description "desc"] [--leverage 3] \
        [--grid-json '{"st_factor":[2.4,2.5],...}'] [--dry]
"""
from __future__ import annotations

import argparse
import json
import itertools
import sys
from pathlib import Path

# Add db module to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'db'))
from _common import connect


WINDOWS_8 = [
    ('W1', '2017-08-18', '2018-12-15'),
    ('W2', '2018-12-15', '2019-10-22'),
    ('W3', '2019-10-22', '2021-02-21'),
    ('W4', '2021-02-21', '2021-11-10'),
    ('W5', '2021-11-10', '2023-01-01'),
    ('W6', '2023-01-01', '2024-03-01'),
    ('W7', '2024-03-01', '2025-04-03'),
    ('W8', '2025-04-03', '2026-04-30'),
]

# Default grid (v5_2 dense grid)
DEFAULT_GRID = {
    'st_factor':         [2.4, 2.5, 2.6, 2.7],
    'st_period':         [6, 7, 8, 9],
    'fast_ema_len':      [7, 8, 9],
    'slow_ema_len':      [25, 27, 30],
    'direction_ema_len': [230, 250, 270],
    'atr_mult':          [3.0, 3.1, 3.2],
}


def main():
    p = argparse.ArgumentParser(description='Generate sweep grid to PostgreSQL')
    p.add_argument('--sweep', type=str, required=True, help='Sweep ID')
    p.add_argument('--description', type=str, default='', help='Optional description')
    p.add_argument('--leverage', type=float, default=3.0, help='Default leverage')
    p.add_argument('--grid-json', type=str, default=None,
                   help='Grid JSON (default: v5_2 dense grid)')
    p.add_argument('--dry', action='store_true', help='Show count only, no insert')
    args = p.parse_args()

    sweep_id = args.sweep
    description = args.description or f'Sweep {sweep_id}'
    leverage = args.leverage

    # Parse grid
    if args.grid_json:
        try:
            grid = json.loads(args.grid_json)
        except json.JSONDecodeError as e:
            print(f'ERROR: Invalid grid JSON: {e}', file=sys.stderr)
            return 1
    else:
        grid = DEFAULT_GRID

    # Validate grid keys
    param_keys = list(grid.keys())
    levels = [grid[k] for k in param_keys]

    # Calculate total combos
    n_combos = 1
    for lv in levels:
        n_combos *= len(lv)
    total_pairs = n_combos * len(WINDOWS_8)

    print(f'Sweep: {sweep_id}')
    print(f'Description: {description}')
    print(f'Leverage: {leverage}')
    print(f'Grid: {" × ".join(f"{k}({len(v)})" for k, v in grid.items())}')
    print(f'Total combos: {n_combos}')
    print(f'Total windows: {len(WINDOWS_8)}')
    print(f'Total (combo,window) pairs: {total_pairs}')

    if args.dry:
        print('[DRY] No changes made.')
        return 0

    # Connect and insert
    try:
        conn = connect()
        conn.autocommit = False

        # Upsert sweep metadata
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO st_sweeps(sweep_id, description, leverage, variant, grid_json, n_combos)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(sweep_id) DO UPDATE SET
                  description=EXCLUDED.description,
                  n_combos=EXCLUDED.n_combos
            """, (
                sweep_id,
                description,
                leverage,
                'long_only',  # variant
                json.dumps(grid),
                n_combos
            ))
        conn.commit()

        # Insert combos
        inserted = 0
        for combo_id, values in enumerate(itertools.product(*levels)):
            params = dict(zip(param_keys, values))

            with conn.cursor() as cur:
                cur.execute("""
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
                      %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      NULL, NULL, NULL, NULL, NULL,
                      NULL, NULL, NULL, FALSE,
                      NULL, NULL,
                      FALSE, FALSE, FALSE, FALSE, FALSE,
                      FALSE, FALSE, FALSE, NULL,
                      NULL, NULL, 0, NULL,
                      'NEW', NULL, NULL
                    )
                    ON CONFLICT(sweep_id, combo_id) DO NOTHING
                """, (
                    sweep_id, combo_id,
                    params['st_factor'], params['st_period'], params['fast_ema_len'],
                    params['slow_ema_len'], params['direction_ema_len'], params['atr_mult'], 0.0
                ))
                inserted += cur.rowcount
            conn.commit()

        # Count final combos
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM st_combos WHERE sweep_id=%s", (sweep_id,))
            final_count = cur.fetchone()[0]

        conn.close()

        print(f'✓ Inserted {inserted} new combos')
        print(f'✓ Total combos in sweep: {final_count}')
        print('Grid generation complete.')
        return 0

    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
