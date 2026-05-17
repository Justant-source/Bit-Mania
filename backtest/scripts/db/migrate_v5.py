"""Migrate v5 optimization results to database."""
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
    """Load v5 optimization CSV and populate database."""
    csv_path = '/result/v5_optimization/v5_all_combos.csv'

    conn = connect()

    # Register sweep
    upsert_sweep(
        conn,
        sweep_id='v5',
        description='v5 grid (324 combos, 4×3×3×3×3×3)',
        leverage=3,
        variant='long_only',
        grid_json=None,
        n_combos=324,
        source_csv=csv_path
    )

    row_count = 0
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1

            # Parse windows
            windows = parse_windows_json(row.get('window_stats_json', ''))

            # Build xref with v4 cross-reference
            xref = {
                'v4_combo_id': _i(row, 'v4_combo_id'),
                'v4_mean_cagr': _f(row, 'v4_mean_cagr'),
                'v4_v5_drift': _f(row, 'v4_v5_drift')
            }

            combo_data = {
                'sweep_id': 'v5',
                'combo_id': _i(row, 'combo_id'),
                'st_factor': _f(row, 'st_factor'),
                'st_period': _i(row, 'st_period'),
                'fast_ema_len': _i(row, 'fast_ema_len'),
                'slow_ema_len': _i(row, 'slow_ema_len'),
                'direction_ema_len': _i(row, 'direction_ema_len'),
                'atr_mult': _f(row, 'atr_mult'),
                'sl_margin_pct': _f(row, 'sl_margin_pct'),
                'tp_atr_mult': None,
                'sl_atr_mult': None,
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
                'tier1': _b(row, 'tier1'),
                'tier2': _b(row, 'tier2'),
                'tier3': _b(row, 'tier3'),
                'tier4': _b(row, 'tier4'),
                'tier_pass': _b(row, 'tier_pass'),
                'tier_a': None,
                'tier_b': None,
                'tier_c': None,
                'final_tier': None,
                'safety_score': _f(row, 'safety_score'),
                'plateau_quality': row.get('plateau_quality'),
                'plateau_score': _f(row, 'plateau_score'),
                'sweet_spot_score': _f(row, 'sweet_spot_score'),
                'cross_val_status': None,
                'xref_json': json.dumps(xref),
                'raw_json': json.dumps(dict(row))
            }

            pk = upsert_combo(conn, 'v5', combo_data)
            insert_windows(conn, pk, windows)

            if row_count % 100 == 0:
                print(f'  {row_count} rows processed')

    conn.close()
    print(f'v5 migration complete: {row_count} combos loaded')


if __name__ == '__main__':
    main()
