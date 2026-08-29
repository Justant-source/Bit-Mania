"""Common utilities for database migrations."""
import os
import json
import psycopg2
from psycopg2.extras import Json as PgJson


def _to_pg_json(v):
    """Wrap a value for a JSONB column: accepts dict, list, JSON string, or None."""
    if v is None:
        return None
    if isinstance(v, str):
        v = json.loads(v)
    return PgJson(v)


def connect():
    """Connect to backtest PostgreSQL database."""
    password = os.environ.get('JESSE_DB_PASSWORD')
    if not password:
        raise RuntimeError('JESSE_DB_PASSWORD is required (fail-closed)')
    return psycopg2.connect(
        host=os.environ.get('JESSE_DB_HOST', 'backtest-postgres'),
        port=int(os.environ.get('JESSE_DB_PORT', 5432)),
        dbname='jesse_db',
        user='jesse',
        password=password,
    )


def apply_schema(conn, schema_path):
    """Apply SQL schema from file."""
    sql = open(schema_path).read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _f(row, key, default=None):
    """Get float or None from csv row."""
    v = row.get(key)
    if v is None or v == '' or v == 'None':
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _b(row, key):
    """Get bool from csv row (True/False/1/0)."""
    v = row.get(key, '')
    return str(v).strip().lower() in ('true', '1', 'yes')


def _i(row, key, default=None):
    """Get int or None from csv row."""
    v = row.get(key)
    if v is None or v == '' or v == 'None':
        return default
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def upsert_sweep(conn, sweep_id, description, leverage, variant, grid_json, n_combos, source_csv):
    """Insert or update sweep metadata."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO st_sweeps(sweep_id, description, leverage, variant, grid_json, n_combos, source_csv)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(sweep_id) DO UPDATE SET
              description=EXCLUDED.description,
              n_combos=EXCLUDED.n_combos,
              source_csv=EXCLUDED.source_csv
        """, (
            sweep_id,
            description,
            leverage,
            variant,
            _to_pg_json(grid_json),
            n_combos,
            source_csv
        ))
    conn.commit()


def upsert_combo(conn, sweep_id, row_data):
    """Insert or update combo parameters and results.

    Args:
        conn: Database connection
        sweep_id: Sweep identifier
        row_data: Dict with all st_combos columns except pk
                  Must include raw_json = json.dumps(original csv row)

    Returns:
        pk: Primary key of inserted/updated combo
    """
    # Wrap JSONB fields for proper psycopg2 type handling
    row_data = dict(row_data)
    row_data['xref_json'] = _to_pg_json(row_data.get('xref_json'))
    row_data['raw_json'] = _to_pg_json(row_data.get('raw_json'))

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO st_combos(
              sweep_id, combo_id, st_factor, st_period, fast_ema_len, slow_ema_len,
              direction_ema_len, atr_mult, sl_margin_pct, tp_atr_mult, sl_atr_mult,
              n_complete, n_positive, mean_cagr, std_cagr, worst_window, worst_mdd, mean_mdd,
              total_trades, liquidated, worst_mdd_recent, mean_cagr_recent,
              tier1, tier2, tier3, tier4, tier_pass,
              tier_a, tier_b, tier_c, final_tier,
              safety_score, plateau_quality, plateau_score, sweet_spot_score,
              cross_val_status, xref_json, raw_json
            ) VALUES (
              %(sweep_id)s, %(combo_id)s, %(st_factor)s, %(st_period)s, %(fast_ema_len)s, %(slow_ema_len)s,
              %(direction_ema_len)s, %(atr_mult)s, %(sl_margin_pct)s, %(tp_atr_mult)s, %(sl_atr_mult)s,
              %(n_complete)s, %(n_positive)s, %(mean_cagr)s, %(std_cagr)s, %(worst_window)s, %(worst_mdd)s, %(mean_mdd)s,
              %(total_trades)s, %(liquidated)s, %(worst_mdd_recent)s, %(mean_cagr_recent)s,
              %(tier1)s, %(tier2)s, %(tier3)s, %(tier4)s, %(tier_pass)s,
              %(tier_a)s, %(tier_b)s, %(tier_c)s, %(final_tier)s,
              %(safety_score)s, %(plateau_quality)s, %(plateau_score)s, %(sweet_spot_score)s,
              %(cross_val_status)s, %(xref_json)s, %(raw_json)s
            )
            ON CONFLICT(sweep_id, combo_id) DO UPDATE SET
              mean_cagr=EXCLUDED.mean_cagr,
              sweet_spot_score=EXCLUDED.sweet_spot_score,
              raw_json=EXCLUDED.raw_json
            RETURNING pk
        """, row_data)
        pk = cur.fetchone()[0]
    conn.commit()
    return pk


def insert_windows(conn, combo_pk, windows):
    """Insert window results for a combo.

    Args:
        conn: Database connection
        combo_pk: Foreign key to st_combos
        windows: List of dicts with window_results fields
    """
    if not windows:
        return

    with conn.cursor() as cur:
        for w in windows:
            cur.execute("""
                INSERT INTO st_window_results(
                  combo_pk, "window", complete, cagr_raw, mdd_raw, cagr_adj, mdd_adj,
                  sharpe, trades_count, liquidated, finishing_balance
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(combo_pk, "window") DO NOTHING
            """, (
                combo_pk,
                w['window'],
                w.get('complete'),
                w.get('cagr_raw'),
                w.get('mdd_raw'),
                w.get('cagr_adj'),
                w.get('mdd_adj'),
                w.get('sharpe'),
                w.get('trades_count'),
                w.get('liquidated'),
                w.get('finishing_balance')
            ))
    conn.commit()
