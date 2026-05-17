"""Migrate v7 leverage test results to database."""
import csv
import json
from _common import connect, upsert_sweep, upsert_combo, insert_windows, _f, _b, _i


def main():
    """Load v7 leverage test CSV and populate database."""
    csv_path = '/result/v7_leverage_test/v7_results.csv'

    conn = connect()

    # Register sweep
    upsert_sweep(
        conn,
        sweep_id='v7',
        description='v7 leverage validation (3x only, top 3 v5_2 carriers)',
        leverage=3,
        variant='long_only',
        grid_json=None,
        n_combos=3,
        source_csv=csv_path
    )

    # Group rows by combo_id, filtering for leverage == 3.0
    combos = {}

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only process rows with leverage == 3.0
            if _f(row, 'leverage') != 3.0:
                continue

            combo_id = _i(row, 'combo_id')
            period = row.get('period', '').strip()

            if combo_id not in combos:
                combos[combo_id] = {'full': None, 'recent': None}

            if period in ('full', 'recent'):
                combos[combo_id][period] = dict(row)

    row_count = 0
    for combo_id in sorted(combos.keys()):
        periods = combos[combo_id]
        full_row = periods.get('full')

        if not full_row:
            print(f'  Warning: combo_id {combo_id} has no full period data')
            continue

        row_count += 1

        # Use full row as primary (combo-level aggregation)
        combo_data = {
            'sweep_id': 'v7',
            'combo_id': combo_id,
            'st_factor': _f(full_row, 'st_factor'),
            'st_period': _i(full_row, 'st_period'),
            'fast_ema_len': _i(full_row, 'fast_ema_len'),
            'slow_ema_len': _i(full_row, 'slow_ema_len'),
            'direction_ema_len': _i(full_row, 'dir_ema_len'),  # Note: CSV column is 'dir_ema_len'
            'atr_mult': _f(full_row, 'atr_mult'),
            'sl_margin_pct': None,
            'tp_atr_mult': None,
            'sl_atr_mult': None,
            'n_complete': None,
            'n_positive': None,
            'mean_cagr': _f(full_row, 'cagr'),  # Combo-level is full period
            'std_cagr': None,
            'worst_window': None,
            'worst_mdd': _f(full_row, 'mdd'),
            'mean_mdd': None,
            'total_trades': _i(full_row, 'trades'),
            'liquidated': None,
            'worst_mdd_recent': None,
            'mean_cagr_recent': None,
            'tier1': None,
            'tier2': None,
            'tier3': None,
            'tier4': None,
            'tier_pass': None,
            'tier_a': None,
            'tier_b': None,
            'tier_c': None,
            'final_tier': None,
            'safety_score': None,
            'plateau_quality': None,
            'plateau_score': None,
            'sweet_spot_score': None,
            'cross_val_status': None,
            'xref_json': json.dumps({
                'multiplier': _f(full_row, 'multiplier'),
                'win_rate': _f(full_row, 'win_rate')
            }),
            'raw_json': json.dumps({
                'full': dict(full_row),
                'recent': dict(periods.get('recent', {}))
            })
        }

        pk = upsert_combo(conn, 'v7', combo_data)

        # Insert window results for both periods
        windows = []

        # Full period window
        windows.append({
            'window': 'full',
            'complete': True,
            'cagr_raw': _f(full_row, 'cagr'),
            'mdd_raw': _f(full_row, 'mdd'),
            'cagr_adj': _f(full_row, 'cagr'),
            'mdd_adj': _f(full_row, 'mdd'),
            'sharpe': _f(full_row, 'sharpe'),
            'trades_count': _i(full_row, 'trades'),
            'liquidated': False,
            'finishing_balance': None
        })

        # Recent period window (if available)
        recent_row = periods.get('recent')
        if recent_row:
            windows.append({
                'window': 'recent',
                'complete': True,
                'cagr_raw': _f(recent_row, 'cagr'),
                'mdd_raw': _f(recent_row, 'mdd'),
                'cagr_adj': _f(recent_row, 'cagr'),
                'mdd_adj': _f(recent_row, 'mdd'),
                'sharpe': _f(recent_row, 'sharpe'),
                'trades_count': _i(recent_row, 'trades'),
                'liquidated': False,
                'finishing_balance': None
            })

        insert_windows(conn, pk, windows)

        if row_count % 100 == 0:
            print(f'  {row_count} combos processed')

    conn.close()
    print(f'v7 migration complete: {row_count} combos loaded')


if __name__ == '__main__':
    main()
