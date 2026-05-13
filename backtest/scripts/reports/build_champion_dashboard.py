#!/usr/bin/env python3
"""
Champion Dashboard Builder — param_sweep v2+v3 + cost-adjusted + intrabar MDD
Generates self-contained HTML dashboard with Plotly charts.
Output: backtest/results/champion_dashboard.html
"""
import json
import math
from pathlib import Path
from datetime import datetime
from collections import defaultdict

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
ADJ_RESULTS = ROOT / 'results' / 'adjusted_costs' / 'all_adjusted_results.json'
INTRABAR_BASE = ROOT / 'results' / 'intrabar'
SWEEP_BASE = ROOT / 'results' / 'param_sweep'
OUT = ROOT / 'results' / 'champion_dashboard.html'

BASELINE = {'cagr': 34.87, 'mdd': -4.52, 'sharpe': 3.583, 'name': 'fa80_lev5_r30'}
PERIODS = ['p0', 'p1', 'p2', 'p3', 'p4']
PERIOD_LABELS = {
    'p0': 'P0 2018-04~2020-06',
    'p1': 'P1 2021-04~2026-04',
    'p2': 'P2 2022-12~2026-04',
    'p3': 'P3 2021-04~2025-09',
    'p4': 'P4 2022-12~2025-09'
}

PRE21_PERIODS = [
    'pre21_full', 'pre21_bear', 'pre21_range',
    'pre21_recovery', 'pre21_covid', 'pre21_bull'
]
PRE21_PERIOD_LABELS = {
    'pre21_full':     'Full 2017-08~2020-12',
    'pre21_bear':     'Bear 2017-12~2018-12',
    'pre21_range':    'Range 2018-12~2019-04',
    'pre21_recovery': 'Recovery 2019-04~2020-02',
    'pre21_covid':    'COVID 2020-03~2020-04',
    'pre21_bull':     'Bull 2020-05~2020-12',
}

FUNDING_BADGE = {
    'bybit_live':    '🟢',
    'binance_proxy': '🟡',
    'mixed':         '🟠',
    'fee_only':      '🔴',
}

PRE21_ADJ_RESULTS = ROOT / 'results' / 'adjusted_costs_pre2021' / 'all_adjusted_results_pre21.json'

# 5 champion identifiers (strat, tf, variant, combo, version)
CHAMPIONS = [
    ('supertrend',           '4h', 'long_only',     18, 'v4'),
    ('supertrend_trendtype', '4h', 'long_only',      6, 'v3'),
    ('trendtype',            '1D', 'long_only',      6, 'v3'),
    ('tradeiq_psar_ha',      '1D', 'long_only',      8, 'v3'),
    ('tradeiq_cci_ce',       '4h', 'bidirectional',  2, 'v3'),
]

# Phase 5 gates
PHASE5_GATES = {
    'adj_score': 34.87,
    'intrabar_mdd': -35.0,
    'min_trades': 30,
    'min_sharpe': 0.5
}


def load_adjusted_results():
    """Load and deduplicate adjusted costs results."""
    with open(ADJ_RESULTS) as f:
        data = json.load(f)

    # Deduplicate by (strat, tf, variant, combo): keep highest adjusted_score
    dedup = {}
    for entry in data:
        key = (entry['strat'], entry['tf'], entry['variant'], entry['combo'])
        if key not in dedup or entry['adjusted_score'] > dedup[key]['adjusted_score']:
            dedup[key] = entry

    print(f"Loaded {len(data)} entries, deduplicated to {len(dedup)} unique combos")
    return list(dedup.values())


def load_pre21_results():
    """Load pre-2021 adjusted results. Returns dict keyed by (strat,tf,variant,combo)."""
    if not PRE21_ADJ_RESULTS.exists():
        print(f"[pre21] {PRE21_ADJ_RESULTS} not found, skipping")
        return {}
    data = json.loads(PRE21_ADJ_RESULTS.read_text())
    lookup = {}
    for entry in data:
        key = (entry['strat'], entry['tf'], entry['variant'], entry['combo'])
        lookup[key] = entry
    print(f"[pre21] Loaded {len(data)} champion pre-21 entries")
    return lookup


def load_intrabar_stats(strat, tf, variant, combo, period):
    """Load intrabar stats for a specific combo/period."""
    path = INTRABAR_BASE / strat / tf / variant / f'combo_{combo}' / period / 'stats.json'
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            print(f"  Error reading {path}: {e}")
    return None


def load_sweep_summary(version, strat, tf, variant, combo):
    """Load param_sweep summary for period breakdowns."""
    path = SWEEP_BASE / version / strat / tf / variant / f'combo_{combo}' / 'summary.json'
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            print(f"  Error reading {path}: {e}")
    return None


def enrich_with_intrabar_and_sweep(entries, pre21_lookup=None):
    """Enrich entries with intrabar MDD and sweep summary sharpe data."""
    enriched = []
    if pre21_lookup is None:
        pre21_lookup = {}

    for entry in entries:
        strat = entry['strat']
        tf = entry['tf']
        variant = entry['variant']
        combo = entry['combo']
        version = entry['version']

        # Load intrabar stats for each period
        intrabar_mdds = {}
        intrabar_trades = {}
        for period in PERIODS:
            stats = load_intrabar_stats(strat, tf, variant, combo, period)
            if stats:
                intrabar_mdds[period] = stats.get('max_drawdown_pct', None)
                intrabar_trades[period] = stats.get('total_trades', None)

        # Load sweep summary for original sharpe data
        sweep_summary = load_sweep_summary(version, strat, tf, variant, combo)
        sweep_sharpe = {}
        if sweep_summary and 'periods' in sweep_summary:
            for period in PERIODS:
                if period in sweep_summary['periods']:
                    sweep_sharpe[period] = sweep_summary['periods'][period].get('sharpe', None)

        # Compute averages across periods
        adj_cagrs = [entry['periods'][p]['adj_cagr'] for p in PERIODS if p in entry['periods']]
        avg_adj_cagr = sum(adj_cagrs) / len(adj_cagrs) if adj_cagrs else 0

        intrabar_mdd_values = [v for v in intrabar_mdds.values() if v is not None]
        avg_intrabar_mdd = sum(intrabar_mdd_values) / len(intrabar_mdd_values) if intrabar_mdd_values else None

        avg_sharpe_values = [entry['periods'][p].get('adj_sharpe', 0) for p in PERIODS if p in entry['periods']]
        avg_adj_sharpe = sum(avg_sharpe_values) / len(avg_sharpe_values) if avg_sharpe_values else 0

        trades_per_period = [entry['periods'][p].get('trades', 0) for p in PERIODS if p in entry['periods']]
        avg_trades = sum(trades_per_period) / len(trades_per_period) if trades_per_period else 0

        # Phase 5 gate check
        phase5_pass = True
        phase5_fails = []

        if entry['adjusted_score'] < PHASE5_GATES['adj_score']:
            phase5_pass = False
            phase5_fails.append(f"adj_score={entry['adjusted_score']:.2f}<{PHASE5_GATES['adj_score']}")

        if avg_intrabar_mdd is None or avg_intrabar_mdd < PHASE5_GATES['intrabar_mdd']:
            phase5_pass = False
            mdd_val = avg_intrabar_mdd if avg_intrabar_mdd is not None else 'N/A'
            phase5_fails.append(f"intrabar_mdd={mdd_val}<{PHASE5_GATES['intrabar_mdd']}")

        # All 5 periods must have adj_cagr > 0
        if not all(entry['periods'].get(p, {}).get('adj_cagr', -999) > 0 for p in PERIODS):
            phase5_pass = False
            phase5_fails.append("not all periods have adj_cagr>0")

        if avg_trades < PHASE5_GATES['min_trades']:
            phase5_pass = False
            phase5_fails.append(f"avg_trades={avg_trades:.1f}<{PHASE5_GATES['min_trades']}")

        if avg_adj_sharpe < PHASE5_GATES['min_sharpe']:
            phase5_pass = False
            phase5_fails.append(f"avg_sharpe={avg_adj_sharpe:.3f}<{PHASE5_GATES['min_sharpe']}")

        entry['avg_adj_cagr'] = avg_adj_cagr
        entry['avg_intrabar_mdd'] = avg_intrabar_mdd
        entry['avg_adj_sharpe'] = avg_adj_sharpe
        entry['avg_trades'] = avg_trades
        entry['intrabar_mdds'] = intrabar_mdds
        entry['phase5_pass'] = phase5_pass
        entry['phase5_fails'] = phase5_fails

        # Merge pre21 period data if available
        key = (entry['strat'], entry['tf'], entry['variant'], entry['combo'])
        if pre21_lookup and key in pre21_lookup:
            pre21_entry = pre21_lookup[key]
            entry['pre21_periods'] = pre21_entry.get('periods', {})
            entry['pre21_adj_score'] = pre21_entry.get('adjusted_score', None)
        else:
            entry['pre21_periods'] = {}
            entry['pre21_adj_score'] = None

        enriched.append(entry)

    return enriched


def compute_summary_stats(entries):
    """Compute overall summary statistics."""
    phase5_count = sum(1 for e in entries if e['phase5_pass'])
    cost_adj_exceed = sum(1 for e in entries if e['adjusted_score'] > BASELINE['cagr'])
    basic_gate_pass = sum(1 for e in entries if e['adjusted_score'] >= 30)

    best_adj_cagr = max((e['avg_adj_cagr'] for e in entries), default=0)
    best_intrabar_mdd = max((e['avg_intrabar_mdd'] for e in entries if e['avg_intrabar_mdd'] is not None), default=None)

    return {
        'total_tested': len(entries),
        'phase5_go': phase5_count,
        'cost_adj_exceed': cost_adj_exceed,
        'basic_gate_pass': basic_gate_pass,
        'best_adj_cagr': best_adj_cagr,
        'best_intrabar_mdd': best_intrabar_mdd
    }


def build_strategy_group_summary(entries):
    """Build strategy/TF/variant group averages."""
    groups = defaultdict(list)
    for e in entries:
        key = f"{e['strat']}/{e['tf']}/{e['variant']}"
        groups[key].append(e)

    summary = []
    for key, combos in groups.items():
        avg_cagr = sum(c['avg_adj_cagr'] for c in combos) / len(combos)
        phase5_count = sum(1 for c in combos if c['phase5_pass'])
        summary.append({
            'group': key,
            'avg_cagr': avg_cagr,
            'phase5_count': phase5_count,
            'total': len(combos)
        })

    summary.sort(key=lambda x: x['avg_cagr'], reverse=True)
    return summary


def build_scatter_data(entries):
    """Build scatter plot data: adj_cagr vs intrabar_mdd."""
    data = []
    strategies = set()
    for e in entries:
        if e['avg_intrabar_mdd'] is not None:
            strategies.add(e['strat'])
            data.append({
                'strat': e['strat'],
                'tf': e['tf'],
                'variant': e['variant'],
                'combo': e['combo'],
                'x': e['avg_adj_cagr'],
                'y': e['avg_intrabar_mdd'],
                'size': max(5, min(50, e['avg_trades'])),
                'phase5': e['phase5_pass'],
                'label': f"{e['strat']} combo_{e['combo']}"
            })
    return data, list(strategies)


def build_top_30_heatmap(entries):
    """Build heatmap for top 30 combos by adj_score."""
    top = sorted(entries, key=lambda x: x['adjusted_score'], reverse=True)[:30]
    heatmap_data = []
    labels = []

    for e in top:
        row = []
        for period in PERIODS:
            adj_cagr = e['periods'].get(period, {}).get('adj_cagr', None)
            if adj_cagr is not None:
                row.append(adj_cagr)
            else:
                row.append(None)
        heatmap_data.append(row)
        labels.append(f"{e['strat']} combo_{e['combo']}")

    return heatmap_data, labels


def build_pre21_heatmap(entries):
    """Build heatmap for 5 champions across pre-2021 periods."""
    champion_keys = {(s, t, v, c): (s, t, v, c, ver) for s, t, v, c, ver in CHAMPIONS}
    result_rows = []
    coverage_rows = []
    labels = []
    for e in entries:
        key = (e['strat'], e['tf'], e['variant'], e['combo'])
        if key not in champion_keys:
            continue
        champ = champion_keys[key]
        label = f"{champ[0]}/{champ[1]} c{champ[3]}"
        labels.append(label)
        row = []
        cov_row = []
        for p in PRE21_PERIODS:
            pdata = e.get('pre21_periods', {}).get(p, {})
            row.append(pdata.get('adj_cagr', None))
            cov = pdata.get('funding_coverage', None)
            cov_row.append(FUNDING_BADGE.get(cov, '—') if cov else '—')
        result_rows.append(row)
        coverage_rows.append(cov_row)
    return result_rows, labels, coverage_rows


def build_cost_breakdown(entries):
    """Build cost breakdown for top 20 combos."""
    top = sorted(entries, key=lambda x: x['adjusted_score'], reverse=True)[:20]

    data = []
    for e in top:
        # Average cost per period
        fee_costs = [e['periods'].get(p, {}).get('fee_cost_annual_pct', 0) for p in PERIODS]
        funding_costs = [e['periods'].get(p, {}).get('funding_cost_annual_pct', 0) for p in PERIODS]

        avg_fee = sum(fee_costs) / len(fee_costs) if fee_costs else 0
        avg_funding = sum(funding_costs) / len(funding_costs) if funding_costs else 0

        data.append({
            'label': f"{e['strat']} combo_{e['combo']}",
            'original_cagr': e['original_score'],
            'adjusted_cagr': e['adjusted_score'],
            'fee_cost': avg_fee,
            'funding_cost': avg_funding
        })

    return data


def generate_html(entries, summary_stats, group_summary, scatter_data, strategies, heatmap_data, heatmap_labels, cost_data, heatmap_data_pre21=None, labels_pre21=None, coverage_pre21=None):
    """Generate complete HTML dashboard."""

    # Prepare JSON for embedding
    entries_json = json.dumps(entries, default=str)

    # Build table rows
    sorted_entries = sorted(entries, key=lambda x: x['adjusted_score'], reverse=True)
    table_rows_html = ""
    for idx, e in enumerate(sorted_entries, 1):
        status_class = "pass" if e['phase5_pass'] else "fail"
        status_text = "PASS" if e['phase5_pass'] else "FAIL"
        fails_text = " | ".join(e['phase5_fails']) if e['phase5_fails'] else "-"
        mdd_str = f"{e['avg_intrabar_mdd']:.2f}" if e['avg_intrabar_mdd'] is not None else 'N/A'

        row_html = f"""
        <tr class="result-row {status_class}">
            <td>{idx}</td>
            <td>{e['strat']}/{e['tf']}/{e['variant']}</td>
            <td>{e['combo']}</td>
            <td>{e['original_score']:.2f}</td>
            <td>{e['adjusted_score']:.2f}</td>
            <td>{mdd_str}</td>
            <td>{e['avg_adj_sharpe']:.3f}</td>
            <td>{e['avg_trades']:.0f}</td>
            <td class="{status_class}">{status_text}</td>
            <td>{fails_text}</td>
        </tr>
        """
        table_rows_html += row_html

    # Build Phase 5 candidates section
    phase5_candidates = [e for e in sorted_entries if e['phase5_pass']]
    phase5_html = ""
    if phase5_candidates:
        for e in phase5_candidates[:10]:  # Top 10 Phase 5 passes
            phase5_html += f"""
            <div class="phase5-card">
                <h4>{e['strat']} Combo {e['combo']}</h4>
                <div class="phase5-params">
                    <p><strong>Strategy:</strong> {e['strat']}/{e['tf']}/{e['variant']}</p>
                    <p><strong>Adjusted CAGR:</strong> {e['adjusted_score']:.2f}% (vs baseline {BASELINE['cagr']:.2f}%)</p>
                    <p><strong>Intrabar MDD:</strong> {e['avg_intrabar_mdd']:.2f}% (vs baseline {BASELINE['mdd']:.2f}%)</p>
                    <p><strong>Sharpe:</strong> {e['avg_adj_sharpe']:.3f} (vs baseline {BASELINE['sharpe']:.3f})</p>
                    <p><strong>Avg Trades:</strong> {e['avg_trades']:.0f}</p>
                </div>
            </div>
            """
    else:
        phase5_html = "<p style='color: #d29922;'>No Phase 5 GO candidates yet.</p>"

    html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BTC Champion Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            background-color: #0d1117;
            color: #e6edf3;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            padding: 20px;
            line-height: 1.6;
        }}

        .container {{ max-width: 1400px; margin: 0 auto; }}

        header {{
            margin-bottom: 40px;
            border-bottom: 1px solid #30363d;
            padding-bottom: 20px;
        }}

        h1 {{
            font-size: 28px;
            color: #1f6feb;
            margin-bottom: 5px;
        }}

        .subtitle {{
            color: #8b949e;
            font-size: 14px;
            margin-top: 10px;
        }}

        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 30px 0;
        }}

        .kpi-card {{
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
            text-align: center;
        }}

        .kpi-value {{
            font-size: 28px;
            font-weight: bold;
            color: #1f6feb;
            margin: 10px 0;
        }}

        .kpi-label {{
            font-size: 12px;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .section {{
            background-color: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 30px;
            margin: 30px 0;
        }}

        h2 {{
            color: #1f6feb;
            margin-bottom: 20px;
            font-size: 20px;
            border-bottom: 2px solid #30363d;
            padding-bottom: 10px;
        }}

        h3 {{
            color: #79c0ff;
            margin-top: 15px;
            margin-bottom: 10px;
            font-size: 16px;
        }}

        h4 {{
            color: #79c0ff;
            margin-bottom: 10px;
        }}

        .phase5-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }}

        .phase5-card {{
            background-color: #0d1117;
            border: 2px solid #3fb950;
            border-radius: 6px;
            padding: 20px;
        }}

        .phase5-params p {{
            margin: 8px 0;
            font-size: 13px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            font-size: 13px;
        }}

        th {{
            background-color: #0d1117;
            color: #79c0ff;
            padding: 12px;
            text-align: left;
            border-bottom: 2px solid #30363d;
            font-weight: 600;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #30363d;
        }}

        tr:hover {{
            background-color: #161b22;
        }}

        .result-row.pass {{
            border-left: 3px solid #3fb950;
        }}

        .result-row.fail {{
            border-left: 3px solid #f85149;
        }}

        .pass {{
            color: #3fb950;
        }}

        .fail {{
            color: #f85149;
        }}

        .chart-container {{
            margin: 30px 0;
            background-color: #0d1117;
            border-radius: 6px;
            padding: 20px;
            border: 1px solid #30363d;
        }}

        .baseline-info {{
            background-color: #0d1117;
            border-left: 3px solid #d29922;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
            font-size: 13px;
        }}

        .baseline-info strong {{
            color: #d29922;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>BTC 파라미터 스윕 Champion 대시보드</h1>
            <div class="subtitle">
                최종 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                param_sweep v2+v3 통합 분석 (비용 조정 + 매분봉 MDD)
            </div>
        </header>

        <div class="baseline-info">
            <strong>Baseline Reference (fa80_lev5_r30):</strong> CAGR +34.87%, MDD -4.52%, Sharpe 3.583
        </div>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Total Tested</div>
                <div class="kpi-value">{summary_stats['total_tested']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Basic Gate Pass</div>
                <div class="kpi-value">{summary_stats['basic_gate_pass']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Cost-Adj Exceed</div>
                <div class="kpi-value">{summary_stats['cost_adj_exceed']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Phase 5 GO</div>
                <div class="kpi-value phase5" style="color: #3fb950;">{summary_stats['phase5_go']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Best Adj CAGR</div>
                <div class="kpi-value">{summary_stats['best_adj_cagr']:.2f}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Best Intrabar MDD</div>
                <div class="kpi-value">{summary_stats['best_intrabar_mdd']:.2f}%</div>
            </div>
        </div>

        <div class="section">
            <h2>Phase 5 GO Candidates</h2>
            {phase5_html}
        </div>

        <div class="section">
            <h2>Strategy Group Summary</h2>
            <div class="chart-container">
                <div id="chart-group-summary"></div>
            </div>
        </div>

        <div class="section">
            <h2>CAGR vs Intrabar MDD Scatter</h2>
            <p>X축: 조정된 CAGR (기준선 34.87%), Y축: 매분봉 MDD (기준선 -35%)</p>
            <div class="chart-container">
                <div id="chart-scatter"></div>
            </div>
        </div>

        <div class="section">
            <h2>Top 30 Combos: Period Consistency Heatmap</h2>
            <div class="chart-container">
                <div id="chart-heatmap"></div>
            </div>
        </div>

        <div class="section">
            <h2>Cost Breakdown: Top 20 Combos</h2>
            <div class="chart-container">
                <div id="chart-costs"></div>
            </div>
        </div>

        <div class="section">
            <h2>Pre-2021 Period Analysis (5 Champions)</h2>
            <p style="color:#8b949e; font-size:13px; margin-bottom:15px;">
                Funding coverage: 🟢 Bybit live &nbsp;|&nbsp; 🟡 Binance proxy &nbsp;|&nbsp; 🟠 Mixed &nbsp;|&nbsp; 🔴 Fee-only (no funding data)
            </p>
            <div class="chart-container">
                <div id="chart-pre21-heatmap"></div>
            </div>
        </div>

        <div class="section">
            <h2>Interactive Results Table</h2>
            <input type="text" id="filter-input" placeholder="Filter by strategy/TF/variant..."
                   style="padding: 10px; background-color: #0d1117; color: #e6edf3; border: 1px solid #30363d; border-radius: 4px; margin-bottom: 15px; width: 100%;">
            <table>
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Strategy/TF/Variant</th>
                        <th>Combo</th>
                        <th>Orig CAGR</th>
                        <th>Adj CAGR</th>
                        <th>Intrabar MDD</th>
                        <th>Sharpe</th>
                        <th>Avg Trades</th>
                        <th>Phase5 Status</th>
                        <th>Gate Fails</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows_html}
                </tbody>
            </table>
        </div>
    </div>

    <script>
        // Embed data
        const allEntries = {entries_json};
        const groupSummary = {json.dumps(group_summary)};
        const scatterData = {json.dumps(scatter_data)};
        const strategies = {json.dumps(strategies)};
        const heatmapData = {json.dumps(heatmap_data)};
        const heatmapLabels = {json.dumps(heatmap_labels)};
        const costData = {json.dumps(cost_data)};
        const pre21HeatmapData = {json.dumps(heatmap_data_pre21 or [])};
        const pre21Labels = {json.dumps(labels_pre21 or [])};
        const pre21Coverage = {json.dumps(coverage_pre21 or [])};

        // Chart: Pre-2021 Heatmap
        function plotPre21Heatmap() {{
            if (!pre21HeatmapData || !pre21HeatmapData.length) {{
                document.getElementById('chart-pre21-heatmap').innerHTML =
                    '<p style="color:#8b949e; padding:20px;">Pre-2021 data not available yet. Run apply_realistic_costs_pre21.py first.</p>';
                return;
            }}
            const xLabels = {json.dumps([PRE21_PERIOD_LABELS[p] for p in PRE21_PERIODS])};
            const hoverTexts = pre21HeatmapData.map((row, ri) =>
                row.map((v, ci) => {{
                    const cov = pre21Coverage[ri] ? pre21Coverage[ri][ci] : '—';
                    return `${{pre21Labels[ri]}}<br>${{xLabels[ci]}}<br>Adj CAGR: ${{v !== null ? v.toFixed(2) + '%' : 'N/A'}}<br>Funding: ${{cov}}`;
                }})
            );
            const zData = pre21HeatmapData.map(row => row.map(v => v !== null ? parseFloat(v.toFixed(2)) : 0));
            const trace = {{
                z: zData,
                x: xLabels,
                y: pre21Labels,
                type: 'heatmap',
                text: hoverTexts,
                hovertemplate: '%{{text}}<extra></extra>',
                colorscale: [[0, '#f85149'], [0.5, '#0d1117'], [1, '#3fb950']],
            }};
            const layout = {{
                title: '5 Champions: Pre-2021 Adjusted CAGR',
                xaxis: {{ title: 'Pre-2021 Period', tickangle: -30 }},
                yaxis: {{ title: 'Champion' }},
                plot_bgcolor: '#0d1117',
                paper_bgcolor: '#161b22',
                font: {{ color: '#e6edf3', family: 'Arial, sans-serif' }},
                margin: {{ l: 200, r: 40, b: 100, t: 60 }},
                height: 350
            }};
            Plotly.newPlot('chart-pre21-heatmap', [trace], layout, {{ responsive: true }});
        }}

        // Chart: Group Summary
        function plotGroupSummary() {{
            const labels = groupSummary.map(g => g.group);
            const values = groupSummary.map(g => g.avg_cagr);
            const colors = groupSummary.map(g => g.phase5_count > 0 ? '#3fb950' : '#f85149');

            const trace = {{
                x: labels,
                y: values,
                type: 'bar',
                marker: {{ color: colors }},
                hovertemplate: '<b>%{{x}}</b><br>Avg CAGR: %{{y:.2f}}%<extra></extra>'
            }};

            const layout = {{
                title: 'Strategy Group: Avg Adjusted CAGR',
                xaxis: {{ title: 'Strategy/TF/Variant', tickangle: -45 }},
                yaxis: {{ title: 'Avg Adjusted CAGR (%)' }},
                plot_bgcolor: '#0d1117',
                paper_bgcolor: '#161b22',
                font: {{ color: '#e6edf3', family: 'Arial, sans-serif' }},
                margin: {{ b: 150, l: 60, r: 40, t: 60 }},
                showlegend: false
            }};

            Plotly.newPlot('chart-group-summary', [trace], layout, {{ responsive: true }});
        }}

        // Chart: Scatter
        function plotScatter() {{
            const colorMap = {{}};
            strategies.forEach((s, i) => {{
                const colors = ['#1f6feb', '#79c0ff', '#b1baf8', '#58a6ff'];
                colorMap[s] = colors[i % colors.length];
            }});

            const traces = strategies.map(strategy => {{
                const data = scatterData.filter(d => d.strat === strategy);
                return {{
                    x: data.map(d => d.x),
                    y: data.map(d => d.y),
                    mode: 'markers',
                    type: 'scatter',
                    name: strategy,
                    marker: {{
                        size: data.map(d => d.size),
                        color: data.map(d => d.phase5 ? '#3fb950' : colorMap[strategy]),
                        opacity: data.map(d => d.phase5 ? 1.0 : 0.7),
                        line: {{ color: data.map(d => d.phase5 ? '#3fb950' : '#30363d'), width: 1 }}
                    }},
                    text: data.map(d => `${{d.label}}<br>CAGR: ${{d.x.toFixed(2)}}%<br>MDD: ${{d.y.toFixed(2)}}%<br>Trades: ${{d.size.toFixed(0)}}`),
                    hovertemplate: '%{{text}}<extra></extra>'
                }};
            }});

            const layout = {{
                title: 'Adjusted CAGR vs Intrabar MDD',
                xaxis: {{ title: 'Adjusted CAGR (%)', zeroline: false }},
                yaxis: {{ title: 'Intrabar Max Drawdown (%)' }},
                plot_bgcolor: '#0d1117',
                paper_bgcolor: '#161b22',
                font: {{ color: '#e6edf3', family: 'Arial, sans-serif' }},
                margin: {{ l: 70, r: 40, b: 70, t: 60 }},
                hovermode: 'closest',
                shapes: [
                    {{ type: 'line', x0: 34.87, x1: 34.87, y0: Math.min(...scatterData.map(d => d.y)), y1: Math.max(...scatterData.map(d => d.y)), line: {{ color: '#d29922', width: 2, dash: 'dash' }} }},
                    {{ type: 'line', x0: Math.min(...scatterData.map(d => d.x)), x1: Math.max(...scatterData.map(d => d.x)), y0: -35, y1: -35, line: {{ color: '#f85149', width: 2, dash: 'dash' }} }}
                ]
            }};

            Plotly.newPlot('chart-scatter', traces, layout, {{ responsive: true }});
        }}

        // Chart: Heatmap
        function plotHeatmap() {{
            const zData = heatmapData.map(row => row.map(v => v !== null ? parseFloat(v.toFixed(2)) : 0));

            const trace = {{
                z: zData,
                x: ['P0', 'P1', 'P2', 'P3', 'P4'],
                y: heatmapLabels,
                type: 'heatmap',
                colorscale: [
                    [0, '#f85149'],
                    [0.5, '#0d1117'],
                    [1, '#3fb950']
                ],
                hovertemplate: '%{{y}}<br>%{{x}}<br>CAGR: %{{z:.2f}}%<extra></extra>'
            }};

            const layout = {{
                title: 'Top 30 Combos: Period Consistency (Adj CAGR)',
                xaxis: {{ title: 'Period' }},
                yaxis: {{ title: 'Combo', autosize: true }},
                plot_bgcolor: '#0d1117',
                paper_bgcolor: '#161b22',
                font: {{ color: '#e6edf3', family: 'Arial, sans-serif' }},
                margin: {{ l: 200, r: 40, b: 50, t: 60 }},
                height: 800
            }};

            Plotly.newPlot('chart-heatmap', [trace], layout, {{ responsive: true }});
        }}

        // Chart: Cost Breakdown
        function plotCosts() {{
            const labels = costData.map(d => d.label);
            const originalCagr = costData.map(d => d.original_cagr);
            const adjustedCagr = costData.map(d => d.adjusted_cagr);
            const feeCosts = costData.map(d => d.fee_cost);
            const fundingCosts = costData.map(d => d.funding_cost);

            const traceFee = {{
                x: labels,
                y: feeCosts,
                name: 'Fee Cost',
                type: 'bar',
                marker: {{ color: '#f85149' }}
            }};

            const traceRouding = {{
                x: labels,
                y: fundingCosts,
                name: 'Funding Cost',
                type: 'bar',
                marker: {{ color: '#d29922' }}
            }};

            const layout = {{
                title: 'Cost Breakdown: Top 20 Combos (Avg Annual %)',
                xaxis: {{ title: 'Combo', tickangle: -45 }},
                yaxis: {{ title: 'Annual Cost (%)' }},
                barmode: 'stack',
                plot_bgcolor: '#0d1117',
                paper_bgcolor: '#161b22',
                font: {{ color: '#e6edf3', family: 'Arial, sans-serif' }},
                margin: {{ b: 150, l: 60, r: 40, t: 60 }},
                height: 400
            }};

            Plotly.newPlot('chart-costs', [traceFee, traceRouding], layout, {{ responsive: true }});
        }}

        // Table filter
        document.getElementById('filter-input').addEventListener('keyup', function() {{
            const filter = this.value.toLowerCase();
            const rows = document.querySelectorAll('.result-row');
            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            }});
        }});

        // Initialize all charts
        plotGroupSummary();
        plotScatter();
        plotHeatmap();
        plotCosts();
        plotPre21Heatmap();
    </script>
</body>
</html>
"""

    return html_content


def main():
    print("Loading adjusted results...")
    entries = load_adjusted_results()

    print("Loading pre-2021 results...")
    pre21_lookup = load_pre21_results()

    print("Enriching with intrabar stats and sweep summaries...")
    entries = enrich_with_intrabar_and_sweep(entries, pre21_lookup)

    print("Computing summary statistics...")
    summary_stats = compute_summary_stats(entries)

    print(f"Summary: {summary_stats}")
    print(f"  Phase 5 GO candidates: {summary_stats['phase5_go']}")

    print("Building visualizations...")
    group_summary = build_strategy_group_summary(entries)
    scatter_data, strategies = build_scatter_data(entries)
    heatmap_data, heatmap_labels = build_top_30_heatmap(entries)
    cost_data = build_cost_breakdown(entries)
    heatmap_data_pre21, labels_pre21, coverage_pre21 = build_pre21_heatmap(entries)

    print("Generating HTML...")
    html_content = generate_html(
        entries, summary_stats, group_summary, scatter_data, strategies,
        heatmap_data, heatmap_labels, cost_data,
        heatmap_data_pre21=heatmap_data_pre21,
        labels_pre21=labels_pre21,
        coverage_pre21=coverage_pre21,
    )

    print(f"Writing to {OUT}...")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, 'w') as f:
        f.write(html_content)

    size_kb = OUT.stat().st_size / 1024
    print(f"✓ Dashboard generated: {OUT} ({size_kb:.1f} KB)")


if __name__ == '__main__':
    main()
