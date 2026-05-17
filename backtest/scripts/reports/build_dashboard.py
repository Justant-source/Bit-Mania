#!/usr/bin/env python3
"""
build_dashboard.py — Build dashboard.html from PostgreSQL unified data.

Queries st_combos table, transforms to compact columnar JSON format, and injects
into dashboard_v2.html template. Adds sweep filter multiselect control.

Usage:
    python3 build_dashboard.py [--out /result/supertrend_x3_long_only/dashboard.html]

Environment:
    JESSE_DB_HOST, JESSE_DB_PORT (optional; defaults: backtest-postgres, 5432)
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

import psycopg2

# Database connection
def connect():
    """Connect to backtest PostgreSQL database."""
    return psycopg2.connect(
        host=os.environ.get('JESSE_DB_HOST', 'backtest-postgres'),
        port=int(os.environ.get('JESSE_DB_PORT', 5432)),
        dbname='jesse_db',
        user='jesse',
        password='***REMOVED***'
    )


def load_data(conn):
    """
    Query all st_combos, sorted by sweet_spot_score DESC then mean_cagr DESC.
    Returns: (list of row dicts, dict of sweep metadata)
    """
    with conn.cursor() as cur:
        # Get all combos with required columns
        cur.execute("""
            SELECT
              sweep_id, combo_id, st_factor, st_period, fast_ema_len, slow_ema_len,
              direction_ema_len, atr_mult, mean_cagr, std_cagr, worst_window, worst_mdd,
              worst_mdd_recent, mean_cagr_recent, tier_pass, plateau_quality, plateau_score,
              sweet_spot_score
            FROM st_combos
            ORDER BY sweet_spot_score DESC NULLS LAST, mean_cagr DESC NULLS LAST
        """)
        rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # Get sweep metadata
        cur.execute("""
            SELECT sweep_id, n_combos, description
            FROM st_sweeps
            ORDER BY sweep_id
        """)
        sweeps = {}
        for sweep_id, n_combos, description in cur.fetchall():
            sweeps[str(sweep_id)] = {
                'n': n_combos,
                'desc': description if description else f'Sweep {sweep_id}'
            }

    return rows, sweeps


def round_float(v, decimals=4):
    """Round float to N decimals, or return None for None."""
    if v is None:
        return None
    return round(float(v), decimals)


def plateau_abbrev(pq):
    """Convert plateau_quality string to 1-char abbreviation."""
    if pq is None:
        return None
    pq_upper = str(pq).upper()
    mapping = {
        'PLATEAU': 'P',
        'MIXED': 'M',
        'ISLAND': 'I',
        'EDGE': 'E'
    }
    return mapping.get(pq_upper, pq_upper[0] if pq_upper else None)


def to_compact_json(rows, sweeps):
    """
    Transform rows to compact columnar JSON format.

    Headers: ["sw","i","sf","sp","fe","se","de","at","mc","sc","ww","wm","wmr","mcr","tp","pq","ps","ss"]
    - sw = sweep_id (str)
    - i = combo_id (int)
    - sf = st_factor (float)
    - sp = st_period (int)
    - fe = fast_ema_len (int)
    - se = slow_ema_len (int)
    - de = direction_ema_len (int)
    - at = atr_mult (float)
    - mc = mean_cagr (float|null)
    - sc = std_cagr (float|null)
    - ww = worst_window (float|null)
    - wm = worst_mdd (float|null)
    - wmr = worst_mdd_recent (float|null)
    - mcr = mean_cagr_recent (float|null)
    - tp = tier_pass (1|0|null)
    - pq = plateau_quality abbrev (str|null)
    - ps = plateau_score (float|null)
    - ss = sweet_spot_score (float|null)

    Returns: {"h": [...], "r": [[...], ...], "sweeps": {...}}
    """
    headers = ["sw", "i", "sf", "sp", "fe", "se", "de", "at", "mc", "sc", "ww", "wm", "wmr", "mcr", "tp", "pq", "ps", "ss"]

    data_rows = []
    for row in rows:
        # Map sweep_id to string for JSON key
        sweep_id_str = str(row['sweep_id'])

        # Transform tier_pass: True → 1, False → 0, None → None
        tier_pass = row['tier_pass']
        if tier_pass is not None:
            tier_pass = 1 if tier_pass else 0

        data_row = [
            sweep_id_str,
            row['combo_id'],
            round_float(row['st_factor']),
            row['st_period'],
            row['fast_ema_len'],
            row['slow_ema_len'],
            row['direction_ema_len'],
            round_float(row['atr_mult']),
            round_float(row['mean_cagr']),
            round_float(row['std_cagr']),
            round_float(row['worst_window']),
            round_float(row['worst_mdd']),
            round_float(row['worst_mdd_recent']),
            round_float(row['mean_cagr_recent']),
            tier_pass,
            plateau_abbrev(row['plateau_quality']),
            round_float(row['plateau_score']),
            round_float(row['sweet_spot_score']),
        ]
        data_rows.append(data_row)

    return {
        "h": headers,
        "r": data_rows,
        "sweeps": sweeps
    }


def read_template(template_path):
    """Read template HTML file."""
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def build_html(html_template, data_json, total_rows, sweep_counts):
    """
    Inject unified data into template and build final HTML.

    Returns: modified HTML string
    """
    # Serialize data to compact JSON
    data_json_str = json.dumps(data_json, separators=(',', ':'))

    # Build new DATA block
    new_data_block = f"""const DATA = {data_json_str};
const HEADER = DATA.h;
const ROWS = DATA.r.map(r => Object.fromEntries(HEADER.map((k,i) => [k, r[i]])));
const SWEEPS_META = DATA.sweeps;"""

    # Replace the old V5_2_DATA line — use lambda to avoid re.sub interpreting \u escapes in JSON
    _new_data_block = new_data_block  # capture for lambda
    html = re.sub(
        r'const V5_2_DATA = \{.*?\};',
        lambda m: _new_data_block,
        html_template,
        count=1,
        flags=re.DOTALL
    )

    # Remove the old HEADER and ROWS lines if they still exist
    html = re.sub(r'\nconst HEADER = V5_2_DATA\.h;', '', html)
    html = re.sub(r'\nconst ROWS = V5_2_DATA\.r\.map\(r => Object\.fromEntries\(HEADER\.map\(\(k,i\) => \[k, r\[i\]\]\)\)\);', '', html)

    # Update subtitle with total combo count and sweep info
    sweep_list_str = ', '.join(sorted(set(data_json['sweeps'].keys())))
    total_sweeps = len(data_json['sweeps'])
    _subtitle_new = f'<div class="subtitle">전체 {total_rows:,} combos ({total_sweeps} sweeps: {sweep_list_str}). <strong>3x leverage 고정</strong>. MDD 게이트 -80%.</div>'
    html = re.sub(
        r'<div class="subtitle">v5_2 결과 1,296 combos × 8 windows.*?</div>',
        lambda m: _subtitle_new,
        html, count=1, flags=re.DOTALL
    )

    # Add sweep filter multiselect to controls panel
    # Insert after the Tier filter control-group
    sweep_filter_html = '''    <div class="control-group" id="sweep-filter-wrap">
      <label>Sweep</label>
      <!-- populated by JS -->
    </div>'''

    # Find the closing </div> of the Tier control-group (filter-tier)
    # and insert the sweep filter right after it
    pattern = r'(<select id="filter-tier">.*?</select>\s*</div>)'
    def insert_after_tier(match):
        return match.group(1) + '\n' + sweep_filter_html

    html = re.sub(pattern, insert_after_tier, html, count=1, flags=re.DOTALL)

    # Find the location after "const SWEEPS_META = DATA.sweeps;" and add sweep selection code
    # Look for the line after "const TOP15_IDS = ..." to insert the sweep initialization
    sweep_init_code = '''

// ==================== SWEEP FILTER ====================
const SWEEP_VALUES = [...new Set(ROWS.map(r => r.sw))].sort();
let sweepSelects = new Set(SWEEP_VALUES);
createMultiSelect('sweep-filter-wrap', 'Sweep', SWEEP_VALUES, vals => { sweepSelects = new Set(vals); render(); });'''

    # Insert after TOP15_IDS definition — use lambda to avoid re.sub escape interpretation
    _sweep_init = sweep_init_code
    html = re.sub(
        r'(const TOP15_IDS = \[.*?\];)',
        lambda m: m.group(1) + _sweep_init,
        html,
        count=1,
        flags=re.DOTALL
    )

    # Add sweep filter check to applyFilter()
    html = re.sub(
        r'(function applyFilter\(useParamFilters = true\) \{\s+return ROWS\.filter\(r => \{)',
        lambda m: m.group(1) + '\n    if (!sweepSelects.has(r.sw)) return false;',
        html, count=1, flags=re.DOTALL
    )

    return html


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Build unified dashboard from PostgreSQL data')
    parser.add_argument('--out', type=str, default='/result/supertrend_x3_long_only/dashboard.html',
                        help='Output HTML file path')
    args = parser.parse_args()

    out_path = Path(args.out)
    template_path = Path('/result/supertrend_x3_long_only/dashboard_v2.html')

    # Validate paths
    if not template_path.exists():
        print(f"ERROR: Template not found: {template_path}", file=sys.stderr)
        sys.exit(1)

    # Create output directory if needed
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database
    try:
        conn = connect()
        print(f"[✓] Connected to database", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    # Load data from database
    try:
        rows, sweeps = load_data(conn)
        conn.close()
        print(f"[✓] Loaded {len(rows)} combos from {len(sweeps)} sweeps", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to load data: {e}", file=sys.stderr)
        sys.exit(1)

    # Transform to compact JSON
    try:
        data_json = to_compact_json(rows, sweeps)
        print(f"[✓] Transformed to compact JSON format", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to transform data: {e}", file=sys.stderr)
        sys.exit(1)

    # Read template
    try:
        html_template = read_template(template_path)
        print(f"[✓] Read template ({len(html_template):,} chars)", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to read template: {e}", file=sys.stderr)
        sys.exit(1)

    # Build HTML
    try:
        html_output = build_html(html_template, data_json, len(rows), sweeps)
        print(f"[✓] Injected data and built HTML", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to build HTML: {e}", file=sys.stderr)
        sys.exit(1)

    # Write output
    try:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(html_output)
        file_size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"[✓] Wrote dashboard to {out_path} ({file_size_mb:.2f} MB)", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: Failed to write output: {e}", file=sys.stderr)
        sys.exit(1)

    print("Dashboard build complete!", file=sys.stderr)


if __name__ == '__main__':
    main()
