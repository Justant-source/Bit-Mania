"""Migrate v6 optimization results to database."""
import csv
import json
from _common import connect, upsert_sweep, upsert_combo, insert_windows, _f, _b, _i


def parse_windows_json(json_str):
    """Parse window_stats_json and return list of window result dicts."""
    if not json_str or json_str == 'None':
        return []

    try:
        # Handle double-quoted JSON strings
        if isinstance(json_str, str) and json_str.startswith('"'):
            json_str = json.loads(json_str)
        windows = json.loads(json_str) if isinstance(json_str, str) else json_str

        result = []
        for w in windows:
            result.append({
                'window': w.get('window'),
                'complete': w.get('complete'),
                'cagr_raw': _f(w, 'cagr_raw'),
                'mdd_raw': _f(w, 'mdd_raw'),
                'cagr_adj': _f(w, 'cagr_adj'),
                'mdd_adj': _f(w, 'mdd_adj'),
                'sharpe': _f(w, 'sharpe'),
                'trades_count': _i(w, 'trades_count'),
                'liquidated': w.get('liquidated'),
                'finishing_balance': _f(w, 'finishing_balance')
            })
        return result
    except (json.JSONDecodeError, TypeError, AttributeError):
        return []


def main():
    """Load v6 optimization CSV and populate database."""
    csv_path = '/result/v6_optimization/v6_all_combos.csv'

    conn = connect()

    # Register sweep
    upsert_sweep(
        conn,
        sweep_id='v6',
        description='v6 asymmetric tp/sl sweep (225 combos)',
        leverage=3,
        variant='long_only',
        grid_json=None,
        n_combos=225,
        source_csv=csv_path
    )

    row_count = 0
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1

            # Parse windows
            windows = parse_windows_json(row.get('window_stats_json', ''))

            # Build xref with v5 and v6-specific fields
            xref = {
                'carrier_id': _i(row, 'carrier_id'),
                'v5_combo_id': _i(row, 'v5_combo_id'),
                'v5_atr_mult': _f(row, 'v5_atr_mult'),
                'v5_mean_cagr': _f(row, 'v5_mean_cagr'),
                'v5_worst_mdd': _f(row, 'v5_worst_mdd'),
                'mdd_improvement': _f(row, 'mdd_improvement'),
                'cagr_drop': _f(row, 'cagr_drop'),
                'v6_score': _f(row, 'v6_score'),
                'rank_in_tier': _i(row, 'rank_in_tier'),
                'is_sanity_case': _b(row, 'is_sanity_case'),
                'v5_v6_drift': _f(row, 'v5_v6_drift')
            }

            combo_data = {
                'sweep_id': 'v6',
                'combo_id': _i(row, 'combo_id'),
                'st_factor': _f(row, 'st_factor'),
                'st_period': _i(row, 'st_period'),
                'fast_ema_len': _i(row, 'fast_ema_len'),
                'slow_ema_len': _i(row, 'slow_ema_len'),
                'direction_ema_len': _i(row, 'direction_ema_len'),
                'atr_mult': None,  # v6 uses tp_atr_mult and sl_atr_mult instead
                'sl_margin_pct': _f(row, 'sl_margin_pct'),
                'tp_atr_mult': _f(row, 'tp_atr_mult'),
                'sl_atr_mult': _f(row, 'sl_atr_mult'),
                'n_complete': _i(row, 'n_complete'),
                'n_positive': _i(row, 'n_positive'),
                'mean_cagr': _f(row, 'mean_cagr'),
                'std_cagr': _f(row, 'std_cagr'),
                'worst_window': _f(row, 'worst_window'),
                'worst_mdd': _f(row, 'worst_mdd'),
                'mean_mdd': _f(row, 'mean_mdd'),
                'total_trades': _i(row, 'total_trades'),
                'liquidated': _b(row, 'liquidated'),
                'worst_mdd_recent': None,
                'mean_cagr_recent': None,
                'tier1': None,
                'tier2': None,
                'tier3': None,
                'tier4': None,
                'tier_pass': None,
                'tier_a': _b(row, 'tier_a'),
                'tier_b': _b(row, 'tier_b'),
                'tier_c': _b(row, 'tier_c'),
                'final_tier': row.get('final_tier'),
                'safety_score': _f(row, 'safety_score'),
                'plateau_quality': None,
                'plateau_score': None,
                'sweet_spot_score': None,
                'cross_val_status': None,
                'xref_json': json.dumps(xref),
                'raw_json': json.dumps(dict(row))
            }

            pk = upsert_combo(conn, 'v6', combo_data)
            insert_windows(conn, pk, windows)

            if row_count % 100 == 0:
                print(f'  {row_count} rows processed')

    conn.close()
    print(f'v6 migration complete: {row_count} combos loaded')


if __name__ == '__main__':
    main()
