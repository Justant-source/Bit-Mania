#!/usr/bin/env python3
"""
pg_generate_grid.py — Generate sweep grid into PostgreSQL st_sweeps + st_combos.

Usage:
    python3 pg_generate_grid.py --sweep v11a --grid-file /app/configs/v11a_blocks.json \\
        --exclude-sweeps v10_notp [--dry] [--append]
"""
from __future__ import annotations

import argparse
import json
import itertools
import sys
from pathlib import Path

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

AXIS_ORDER = [
    'st_factor',
    'st_period',
    'fast_ema_len',
    'slow_ema_len',
    'direction_ema_len',
    'atr_mult',
]
INT_AXIS = {'st_period', 'fast_ema_len', 'slow_ema_len', 'direction_ema_len'}

DEFAULT_GRID = {
    'st_factor':         [2.4, 2.5, 2.6, 2.7],
    'st_period':         [6, 7, 8, 9],
    'fast_ema_len':      [7, 8, 9],
    'slow_ema_len':      [25, 27, 30],
    'direction_ema_len': [230, 250, 270],
    'atr_mult':          [3.0, 3.1, 3.2],
}


def _canon5(sf, sp, fe, se, de) -> tuple:
    return (round(float(sf), 4), int(sp), int(fe), int(se), int(de))


def _canon6(vals: tuple) -> tuple:
    sf, sp, fe, se, de, at = vals
    return (
        round(float(sf), 4),
        int(sp),
        int(fe),
        int(se),
        int(de),
        round(float(at), 4),
    )


def _strip_block(raw: dict) -> dict:
    out = {}
    for k, v in raw.items():
        if k.startswith('_'):
            continue
        if k not in AXIS_ORDER:
            continue
        out[k] = v
    missing = [k for k in AXIS_ORDER if k not in out]
    if missing:
        raise ValueError(f'block missing keys {missing}')
    return out


def _blocks_from_grid(grid) -> list[dict]:
    if isinstance(grid, dict):
        if 'blocks' in grid and isinstance(grid['blocks'], list):
            return [_strip_block(b) for b in grid['blocks']]
        return [_strip_block(grid)]
    if isinstance(grid, list):
        return [_strip_block(b) for b in grid]
    raise ValueError('grid must be dict or list of dicts')


def _union_tuples(blocks: list[dict]) -> list[tuple]:
    seen: set[tuple] = set()
    out: list[tuple] = []
    for b in blocks:
        levels = [b[k] for k in AXIS_ORDER]
        for vals in itertools.product(*levels):
            t = _canon6(vals)
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


def _load_excluded_5(conn, sweep_ids: list[str]) -> set[tuple]:
    if not sweep_ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT st_factor, st_period, fast_ema_len, slow_ema_len, direction_ema_len
            FROM st_combos
            WHERE sweep_id = ANY(%s)
            """,
            (sweep_ids,),
        )
        return {_canon5(*row) for row in cur.fetchall()}


def main() -> int:
    p = argparse.ArgumentParser(description='Generate sweep grid to PostgreSQL')
    p.add_argument('--sweep', type=str, required=True)
    p.add_argument('--description', type=str, default='')
    p.add_argument('--leverage', type=float, default=3.0)
    p.add_argument('--grid-json', type=str, default=None)
    p.add_argument('--grid-file', type=str, default=None)
    p.add_argument('--exclude-sweeps', type=str, default='',
                   help='Comma-separated sweep_ids whose 5-tuples are skipped')
    p.add_argument('--append', action='store_true',
                   help='Continue combo_id after MAX(existing)')
    p.add_argument('--dry', action='store_true')
    args = p.parse_args()

    if args.grid_file:
        grid = json.loads(Path(args.grid_file).read_text())
    elif args.grid_json:
        try:
            grid = json.loads(args.grid_json)
        except json.JSONDecodeError as e:
            print(f'ERROR: Invalid grid JSON: {e}', file=sys.stderr)
            return 1
    else:
        grid = DEFAULT_GRID

    try:
        blocks = _blocks_from_grid(grid)
    except ValueError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1

    tuples = _union_tuples(blocks)
    exclude_ids = [s.strip() for s in args.exclude_sweeps.split(',') if s.strip()]

    print(f'Sweep: {args.sweep}')
    print(f'Blocks: {len(blocks)}  union tuples: {len(tuples)}')
    print(f'Exclude sweeps: {exclude_ids or "(none)"}')

    conn = connect()
    conn.autocommit = False
    try:
        excluded = _load_excluded_5(conn, exclude_ids)
        kept = [t for t in tuples if _canon5(*t[:5]) not in excluded]
        skipped_excluded = len(tuples) - len(kept)

        start_id = 0
        skipped_existing = 0
        if args.append:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT COALESCE(MAX(combo_id), -1) FROM st_combos WHERE sweep_id=%s',
                    (args.sweep,),
                )
                start_id = cur.fetchone()[0] + 1
                cur.execute(
                    """
                    SELECT st_factor, st_period, fast_ema_len, slow_ema_len,
                           direction_ema_len, atr_mult
                    FROM st_combos WHERE sweep_id=%s
                    """,
                    (args.sweep,),
                )
                existing6 = {_canon6(tuple(row)) for row in cur.fetchall()}
            before = len(kept)
            kept = [t for t in kept if t not in existing6]
            skipped_existing = before - len(kept)

        print(f'Kept: {len(kept)}  skipped_excluded={skipped_excluded}  '
              f'skipped_existing={skipped_existing}  start_combo_id={start_id}')
        print(f'Total windows: 8  pairs: {len(kept) * 8}')

        if args.dry:
            print('[DRY] No changes made.')
            return 0

        levels: dict[str, list] = {k: [] for k in AXIS_ORDER}
        for t in kept:
            for i, k in enumerate(AXIS_ORDER):
                levels[k].append(t[i])
        for k in AXIS_ORDER:
            levels[k] = sorted(set(levels[k]))

        meta = {
            'blocks': blocks,
            'levels': levels,
            'atr_fixed': 3.3 if set(levels['atr_mult']) == {3.3} else None,
        }
        desc = args.description or f'Sweep {args.sweep}'
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO st_sweeps(sweep_id, description, leverage, variant, grid_json, n_combos)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(sweep_id) DO UPDATE SET
                  description=EXCLUDED.description,
                  n_combos=EXCLUDED.n_combos,
                  grid_json=EXCLUDED.grid_json
                """,
                (args.sweep, desc, args.leverage, 'long_only',
                 json.dumps(meta), len(kept)),
            )
        conn.commit()

        inserted = 0
        for combo_id, t in enumerate(kept, start=start_id):
            sf, sp, fe, se, de, at = t
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """,
                    (args.sweep, combo_id, sf, sp, fe, se, de, at, 0.0),
                )
                inserted += cur.rowcount
            if combo_id % 200 == 0:
                conn.commit()
        conn.commit()

        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM st_combos WHERE sweep_id=%s', (args.sweep,))
            final_count = cur.fetchone()[0]
        print(f'Inserted {inserted} new combos')
        print(f'Total combos in sweep: {final_count}')
        return 0
    except Exception as e:
        conn.rollback()
        print(f'ERROR: {e}', file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
