"""Verify database migration results."""
import csv
import json
import random
from _common import connect, _f


def main():
    """Run verification checks on migrated data."""
    conn = connect()

    print('=' * 70)
    print('DATABASE MIGRATION VERIFICATION')
    print('=' * 70)

    # 1. Check st_sweeps
    print('\n1. Sweep metadata:')
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sweep_id, n_combos, description
            FROM st_sweeps
            ORDER BY sweep_id
        """)
        sweeps = cur.fetchall()
        for sweep_id, n_combos, desc in sweeps:
            print(f'  {sweep_id:8s}: {n_combos:5d} combos - {desc[:50]}')

    # 2. Verify combo counts
    print('\n2. Combo counts:')
    expected_combos = {'v4': 216, 'v5': 324, 'v5_2': 1296, 'v6': 225, 'v7': 3}
    with conn.cursor() as cur:
        for sweep_id, expected_count in expected_combos.items():
            cur.execute(
                'SELECT COUNT(*) FROM st_combos WHERE sweep_id = %s',
                (sweep_id,)
            )
            actual_count = cur.fetchone()[0]
            status = 'OK' if actual_count == expected_count else f'FAIL (expected {expected_count})'
            print(f'  {sweep_id:8s}: {actual_count:5d} {status}')

    # 3. Verify window results counts
    print('\n3. Window results counts:')
    with conn.cursor() as cur:
        # v4/v5/v6 should have n_combos * 8 windows
        for sweep_id in ['v4', 'v5', 'v6']:
            cur.execute("""
                SELECT COUNT(*)
                FROM st_window_results wr
                JOIN st_combos c ON wr.combo_pk = c.pk
                WHERE c.sweep_id = %s
            """, (sweep_id,))
            count = cur.fetchone()[0]
            exp_win = expected_combos[sweep_id] * 8
            status = 'OK' if count == exp_win else f'FAIL (expected {exp_win})'
            print(f'  {sweep_id:8s}: {count:6d} {status}')

        # v5_2 should have 1296 * 8 windows
        cur.execute("""
            SELECT COUNT(*)
            FROM st_window_results wr
            JOIN st_combos c ON wr.combo_pk = c.pk
            WHERE c.sweep_id = 'v5_2'
        """)
        count = cur.fetchone()[0]
        expected = 1296 * 8
        status = 'OK' if count == expected else f'FAIL (expected {expected})'
        print(f'  v5_2    : {count:6d} {status}')

        # v7 should have 3 * 2 windows (full + recent)
        cur.execute("""
            SELECT COUNT(*)
            FROM st_window_results wr
            JOIN st_combos c ON wr.combo_pk = c.pk
            WHERE c.sweep_id = 'v7'
        """)
        count = cur.fetchone()[0]
        expected = 3 * 2
        status = 'OK' if count == expected else f'FAIL (expected {expected})'
        print(f'  v7      : {count:6d} {status}')

    # 4. Verify v5_2 sweet_spot_score ordering (top-10 match with CSV)
    print('\n4. v5_2 sweet_spot_score ordering (top-10):')
    csv_path = '/result/v5_2_optimization/v5_2_all_combos.csv'

    # Load CSV and sort by sweet_spot_score
    csv_combos = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            score = _f(row, 'sweet_spot_score')
            combo_id = int(row['combo_id'])
            csv_combos.append((combo_id, score))

    csv_combos.sort(key=lambda x: (-x[1], x[0]))  # Sort by score desc, then combo_id
    csv_top10 = [cid for cid, _ in csv_combos[:10]]

    # Load from database
    with conn.cursor() as cur:
        cur.execute("""
            SELECT combo_id
            FROM st_combos
            WHERE sweep_id = 'v5_2'
            ORDER BY sweet_spot_score DESC, combo_id
            LIMIT 10
        """)
        db_top10 = [row[0] for row in cur.fetchall()]

    if csv_top10 == db_top10:
        print('  Top-10 ordering: OK')
    else:
        print('  Top-10 ordering: FAIL')
        print(f'    CSV top-10: {csv_top10}')
        print(f'    DB  top-10: {db_top10}')

    # 5. Verify 5 random v5_2 combos raw_json fields
    print('\n5. v5_2 raw_json field validation (5 random combos):')
    with conn.cursor() as cur:
        cur.execute("""
            SELECT combo_id, raw_json
            FROM st_combos
            WHERE sweep_id = 'v5_2'
            ORDER BY RANDOM()
            LIMIT 5
        """)
        db_rows = cur.fetchall()

    # Load CSV for lookup
    csv_data = {}
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data[int(row['combo_id'])] = row

    all_ok = True
    for combo_id, raw_json in db_rows:
        if combo_id not in csv_data:
            print(f'  combo_id {combo_id}: NOT FOUND in CSV')
            all_ok = False
            continue

        csv_row = csv_data[combo_id]
        # psycopg2 auto-converts JSONB → dict; handle both str and dict
        db_obj = raw_json if isinstance(raw_json, dict) else json.loads(raw_json)

        # Check key fields
        fields_to_check = ['mean_cagr', 'worst_mdd', 'sweet_spot_score']
        for field in fields_to_check:
            csv_val = _f(csv_row, field)
            db_val = _f(db_obj, field)
            if csv_val != db_val:
                print(f'  combo_id {combo_id}: {field} mismatch (CSV={csv_val}, DB={db_val})')
                all_ok = False

    if all_ok:
        print('  All checked fields: OK')

    # Final result
    print('\n' + '=' * 70)
    print('VERIFICATION COMPLETE')
    print('=' * 70)

    conn.close()


if __name__ == '__main__':
    main()
