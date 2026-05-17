#!/usr/bin/env python3
"""
pg_aggregate.py — Compute aggregate stats from st_window_results and update st_combos.

Implements tier system, plateau analysis, and sweet spot scoring based on v5_2 logic.

Usage:
    python3 pg_aggregate.py --sweep <sweep_id>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from statistics import mean, stdev

# Add db module to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'db'))
from _common import connect


GRID = {
    'st_factor':         [2.4, 2.5, 2.6, 2.7],
    'st_period':         [6, 7, 8, 9],
    'fast_ema_len':      [7, 8, 9],
    'slow_ema_len':      [25, 27, 30],
    'direction_ema_len': [230, 250, 270],
    'atr_mult':          [3.0, 3.1, 3.2],
}
PARAMS = list(GRID.keys())
INT_PARAMS = {'st_period', 'fast_ema_len', 'slow_ema_len', 'direction_ema_len'}
RECENT_WINDOWS = {'W7', 'W8'}

# Baseline from v4 (v5_2_aggregate.py fallback)
BASELINE_MEAN_CAGR = 144.99
BASELINE_STD_CAGR = 149.17
TIER4_THRESHOLD = BASELINE_STD_CAGR * 1.2  # 179.004


def _grid_level_key(param: str, val):
    """Convert param value to grid-comparable key."""
    if param in INT_PARAMS:
        return int(round(float(val)))
    return round(float(val), 2)


def _param_key(row: dict) -> tuple:
    """Get canonical (st_factor, st_period, ..., atr_mult) tuple for lookup."""
    parts = []
    for p in PARAMS:
        v = row[p]
        if p in INT_PARAMS:
            parts.append(int(round(float(v))))
        else:
            parts.append(round(float(v), 2))
    return tuple(parts)


def load_combo_window_results(conn, sweep_id: str) -> dict[int, list[dict]]:
    """Load all window_results grouped by combo_pk."""
    results: dict[int, list[dict]] = {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT wr.combo_pk, wr."window", wr.complete, wr.cagr_adj, wr.mdd_adj,
                   wr.sharpe, wr.trades_count, wr.liquidated, wr.finishing_balance
            FROM st_window_results wr
            JOIN st_combos c ON c.pk = wr.combo_pk
            WHERE c.sweep_id = %s
            ORDER BY wr.combo_pk, wr."window"
        """, (sweep_id,))

        for row in cur.fetchall():
            combo_pk, window, complete, cagr_adj, mdd_adj, sharpe, trades_count, \
                liquidated, finishing_balance = row

            if combo_pk not in results:
                results[combo_pk] = []

            results[combo_pk].append({
                'window': window,
                'complete': complete,
                'cagr_adj': float(cagr_adj) if cagr_adj is not None else None,
                'mdd_adj': float(mdd_adj) if mdd_adj is not None else None,
                'sharpe': float(sharpe) if sharpe is not None else None,
                'trades_count': int(trades_count) if trades_count is not None else 0,
                'liquidated': bool(liquidated) if liquidated is not None else False,
                'finishing_balance': float(finishing_balance) if finishing_balance is not None else None,
            })

    return results


def compute_combo_stats(combo_pk: int, window_results: list[dict]) -> dict:
    """Compute aggregate stats for a combo from its window results."""
    complete_windows = [wr for wr in window_results if wr['complete']]
    n_complete = len(complete_windows)

    if n_complete == 0:
        return {
            'combo_pk': combo_pk,
            'n_complete': 0,
            'n_positive': 0,
            'mean_cagr': None,
            'std_cagr': None,
            'worst_window': None,
            'worst_mdd': None,
            'mean_mdd': None,
            'total_trades': 0,
            'liquidated': False,
            'worst_mdd_recent': None,
            'mean_cagr_recent': None,
            'tier1': False,
            'tier2': False,
            'tier3': False,
            'tier4': False,
            'tier_pass': False,
        }

    cagrs = [wr['cagr_adj'] for wr in complete_windows if wr['cagr_adj'] is not None]
    mdds = [wr['mdd_adj'] for wr in complete_windows if wr['mdd_adj'] is not None]
    trade_cnts = [wr['trades_count'] for wr in complete_windows]
    liquidated = any(wr['liquidated'] for wr in complete_windows)

    mean_cagr = mean(cagrs) if cagrs else None
    std_cagr = stdev(cagrs) if len(cagrs) > 1 else (0.0 if cagrs else None)
    n_positive = sum(1 for c in cagrs if c > 0)
    worst_window = min(cagrs) if cagrs else None
    worst_mdd = min(mdds) if mdds else None
    mean_mdd = mean(mdds) if mdds else None
    total_trades = sum(trade_cnts)

    # Recent windows (W7+W8)
    recent_windows = [wr for wr in complete_windows if wr['window'] in RECENT_WINDOWS]
    recent_mdds = [wr['mdd_adj'] for wr in recent_windows if wr['mdd_adj'] is not None]
    recent_cagrs = [wr['cagr_adj'] for wr in recent_windows if wr['cagr_adj'] is not None]
    worst_mdd_recent = min(recent_mdds) if recent_mdds else None
    mean_cagr_recent = mean(recent_cagrs) if recent_cagrs else None

    # Tier gates
    tier1 = not liquidated
    tier2 = n_positive == 8 and n_complete == 8
    tier3 = mean_cagr is not None and mean_cagr > BASELINE_MEAN_CAGR
    tier4 = std_cagr is not None and std_cagr < TIER4_THRESHOLD
    tier_pass = tier1 and tier2 and tier3 and tier4

    return {
        'combo_pk': combo_pk,
        'n_complete': n_complete,
        'n_positive': n_positive,
        'mean_cagr': mean_cagr,
        'std_cagr': std_cagr,
        'worst_window': worst_window,
        'worst_mdd': worst_mdd,
        'mean_mdd': mean_mdd,
        'total_trades': total_trades,
        'liquidated': liquidated,
        'worst_mdd_recent': worst_mdd_recent,
        'mean_cagr_recent': mean_cagr_recent,
        'tier1': tier1,
        'tier2': tier2,
        'tier3': tier3,
        'tier4': tier4,
        'tier_pass': tier_pass,
    }


def apply_safety_score(results: list[dict]) -> None:
    """Compute safety_score for each combo."""
    valid = [r for r in results if r['mean_cagr'] is not None and r['n_complete'] >= 6]

    for r in valid:
        mc = r['mean_cagr'] or 0.0
        sc = r['std_cagr'] or 0.0
        np_ = r['n_positive']
        ww = r['worst_window'] or 0.0

        consistency = mc / max(sc, 0.01) if sc is not None else mc * 10.0
        r['safety_score'] = np_ * 10.0 + consistency + ww * 0.5

    # Safety rank
    safe_sorted = sorted(valid, key=lambda r: (r['safety_score'] or -9999), reverse=True)
    for rank, r in enumerate(safe_sorted, 1):
        r['safe_rank'] = rank


def find_grid_neighbors(combo_row: dict, lookup: dict) -> list[dict]:
    """Find combos differing by exactly one grid step."""
    neighbors = []
    target_cagr = combo_row['mean_cagr']

    for param in PARAMS:
        levels = GRID[param]
        target_val = _grid_level_key(param, combo_row[param])

        try:
            idx = [_grid_level_key(param, v) for v in levels].index(target_val)
        except ValueError:
            continue

        for offset in [-1, 1]:
            new_idx = idx + offset
            if 0 <= new_idx < len(levels):
                nb_key = tuple(
                    _grid_level_key(p, levels[new_idx]) if p == param
                    else _grid_level_key(p, combo_row[p])
                    for p in PARAMS
                )
                if nb_key in lookup:
                    neighbors.append(lookup[nb_key])

    return neighbors


def apply_plateau(conn, results: list[dict], sweep_id: str) -> None:
    """Compute plateau_quality and plateau_score for each combo."""
    # Build lookup for fast neighbor queries
    lookup = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.pk, c.st_factor, c.st_period, c.fast_ema_len, c.slow_ema_len,
                   c.direction_ema_len, c.atr_mult
            FROM st_combos c
            WHERE c.sweep_id = %s
        """, (sweep_id,))

        for row in cur.fetchall():
            pk, st_factor, st_period, fast_ema_len, slow_ema_len, \
                direction_ema_len, atr_mult = row

            key = (
                round(float(st_factor), 2),
                int(round(float(st_period))),
                int(round(float(fast_ema_len))),
                int(round(float(slow_ema_len))),
                int(round(float(direction_ema_len))),
                round(float(atr_mult), 2),
            )
            # Find corresponding result
            for r in results:
                if r['combo_pk'] == pk:
                    lookup[key] = r
                    break

    for r in results:
        if r['mean_cagr'] is None:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        # Build combo_row for neighbor lookup
        with conn.cursor() as cur:
            cur.execute("""
                SELECT st_factor, st_period, fast_ema_len, slow_ema_len,
                       direction_ema_len, atr_mult
                FROM st_combos WHERE pk = %s
            """, (r['combo_pk'],))
            row = cur.fetchone()
            if row is None:
                r['plateau_quality'] = 'EDGE'
                r['plateau_score'] = 0.0
                continue

            combo_row = {
                'st_factor': float(row[0]),
                'st_period': int(row[1]),
                'fast_ema_len': int(row[2]),
                'slow_ema_len': int(row[3]),
                'direction_ema_len': int(row[4]),
                'atr_mult': float(row[5]),
                'mean_cagr': r['mean_cagr'],
            }

        neighbors = find_grid_neighbors(combo_row, lookup)

        if len(neighbors) < 4:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        neighbor_cagrs = [n['mean_cagr'] for n in neighbors if n['mean_cagr'] is not None]
        neighbor_pass = [n['tier_pass'] for n in neighbors if n['mean_cagr'] is not None]

        if not neighbor_cagrs:
            r['plateau_quality'] = 'EDGE'
            r['plateau_score'] = 0.0
            continue

        pass_ratio = sum(1 for p in neighbor_pass if p) / len(neighbor_pass)
        cagr_ratio = (sum(neighbor_cagrs) / len(neighbor_cagrs)) / r['mean_cagr'] \
            if r['mean_cagr'] > 0 else 0.0
        score = round(pass_ratio * cagr_ratio * 100, 2)

        if pass_ratio >= 0.8 and cagr_ratio >= 0.7:
            quality = 'PLATEAU'
        elif cagr_ratio < 0.5:
            quality = 'ISLAND'
        else:
            quality = 'MIXED'

        r['plateau_quality'] = quality
        r['plateau_score'] = score


def apply_sweet_spot_score(results: list[dict]) -> None:
    """Compute sweet_spot_score = 0.4*safety + 0.4*plateau + 0.2*cagr."""
    for r in results:
        if r['safety_score'] is None or r['mean_cagr'] is None:
            r['sweet_spot_score'] = None
            continue

        r['sweet_spot_score'] = round(
            (r['safety_score'] or 0.0) * 0.4
            + (r['plateau_score'] or 0.0) * 0.4
            + (r['mean_cagr'] / 200.0 * 100.0) * 0.2,
            2
        )


def update_st_combos(conn, results: list[dict]) -> None:
    """Update st_combos with computed stats."""
    with conn.cursor() as cur:
        for r in results:
            cur.execute("""
                UPDATE st_combos SET
                  n_complete = %s,
                  n_positive = %s,
                  mean_cagr = %s,
                  std_cagr = %s,
                  worst_window = %s,
                  worst_mdd = %s,
                  mean_mdd = %s,
                  total_trades = %s,
                  liquidated = %s,
                  worst_mdd_recent = %s,
                  mean_cagr_recent = %s,
                  tier1 = %s,
                  tier2 = %s,
                  tier3 = %s,
                  tier4 = %s,
                  tier_pass = %s,
                  safety_score = %s,
                  plateau_quality = %s,
                  plateau_score = %s,
                  sweet_spot_score = %s
                WHERE pk = %s
            """, (
                r['n_complete'],
                r['n_positive'],
                r['mean_cagr'],
                r['std_cagr'],
                r['worst_window'],
                r['worst_mdd'],
                r['mean_mdd'],
                r['total_trades'],
                r['liquidated'],
                r['worst_mdd_recent'],
                r['mean_cagr_recent'],
                r['tier1'],
                r['tier2'],
                r['tier3'],
                r['tier4'],
                r['tier_pass'],
                r['safety_score'],
                r['plateau_quality'],
                r['plateau_score'],
                r['sweet_spot_score'],
                r['combo_pk'],
            ))
    conn.commit()


def print_summary(results: list[dict]) -> None:
    """Print aggregation summary."""
    tier_pass_count = sum(1 for r in results if r['tier_pass'])
    plateau_count = sum(1 for r in results if r['plateau_quality'] == 'PLATEAU')
    island_count = sum(1 for r in results if r['plateau_quality'] == 'ISLAND')
    mixed_count = sum(1 for r in results if r['plateau_quality'] == 'MIXED')
    edge_count = sum(1 for r in results if r['plateau_quality'] == 'EDGE')

    print(f'Aggregation Summary:')
    print(f'  Total combos: {len(results)}')
    print(f'  Tier pass: {tier_pass_count}')
    print(f'  Plateau quality: PLATEAU={plateau_count} ISLAND={island_count} '
          f'MIXED={mixed_count} EDGE={edge_count}')

    # Top 5 by sweet spot score
    valid = [r for r in results if r['sweet_spot_score'] is not None]
    if valid:
        top_5 = sorted(valid, key=lambda r: r['sweet_spot_score'], reverse=True)[:5]
        print(f'\n  Top 5 by sweet_spot_score:')
        for i, r in enumerate(top_5, 1):
            print(f'    {i}. combo_pk={r["combo_pk"]} score={r["sweet_spot_score"]} '
                  f'mean_cagr={r["mean_cagr"]:.1f}% plateau={r["plateau_quality"]}')


def main():
    p = argparse.ArgumentParser(description='Aggregate window results and compute scores')
    p.add_argument('--sweep', type=str, required=True, help='Sweep ID')
    args = p.parse_args()

    sweep_id = args.sweep

    try:
        conn = connect()
        conn.autocommit = False

        print(f'Loading window results for sweep {sweep_id}...')
        combo_window_results = load_combo_window_results(conn, sweep_id)

        print(f'Computing combo stats ({len(combo_window_results)} combos)...')
        results = []
        for combo_pk, window_results in combo_window_results.items():
            stats = compute_combo_stats(combo_pk, window_results)
            results.append(stats)

        print('Computing safety scores...')
        apply_safety_score(results)

        print('Computing plateau quality...')
        apply_plateau(conn, results, sweep_id)

        print('Computing sweet spot scores...')
        apply_sweet_spot_score(results)

        print('Updating st_combos...')
        update_st_combos(conn, results)

        print_summary(results)

        conn.close()
        print('\n✓ Aggregation complete.')
        return 0

    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
