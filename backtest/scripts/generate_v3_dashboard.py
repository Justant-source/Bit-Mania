#!/usr/bin/env python3
"""
Generate interactive HTML dashboard for v3 fine-grid optimization results.
Uses Plotly.js via CDN with embedded JSON data.
"""

import csv
import json
import statistics
from pathlib import Path

# File paths
CSV_PATH = Path("/home/justant/Data/Bit-Mania/backtest/results/v3_optimization/v3_all_combos.csv")
OUTPUT_PATH = Path("/home/justant/Data/Bit-Mania/backtest/results/v3_optimization/v3_dashboard.html")

# Ensure output directory exists
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def read_csv_data(csv_path):
    """Read CSV and return list of dicts."""
    data = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row['combo_id'] = int(row['combo_id'])
            row['st_factor'] = float(row['st_factor'])
            row['st_period'] = int(row['st_period'])
            row['fast_ema_len'] = int(row['fast_ema_len'])
            row['slow_ema_len'] = int(row['slow_ema_len'])
            row['direction_ema_len'] = int(row['direction_ema_len'])
            row['atr_mult'] = float(row['atr_mult'])
            row['n_positive'] = int(row['n_positive'])
            row['mean_cagr_adj'] = float(row['mean_cagr_adj'])
            row['cagr_std'] = float(row['cagr_std'])
            row['worst_mdd'] = float(row['worst_mdd'])
            row['mean_mdd'] = float(row['mean_mdd'])
            row['total_trades'] = int(row['total_trades'])
            row['sharpe_full'] = float(row['sharpe_full'])
            row['cagr_raw_full'] = float(row['cagr_raw_full'])
            row['mdd_full'] = float(row['mdd_full'])

            data.append(row)
    return data

def clamp_cagr(value, min_val=-1000, max_val=5000):
    """Clamp CAGR values for display."""
    return max(min_val, min(max_val, value))

def get_top_combos(data, n=50):
    """Get top N combos by mean_cagr_adj."""
    sorted_data = sorted(data, key=lambda x: x['mean_cagr_adj'], reverse=True)
    return sorted_data[:n]

def main():
    print("Reading CSV data...")
    data = read_csv_data(CSV_PATH)
    print(f"Loaded {len(data)} combos")

    # Calculate summary stats
    n_positive_values = [d['n_positive'] for d in data]
    mean_cagr_values = [d['mean_cagr_adj'] for d in data]

    best_n_positive = max(n_positive_values)
    best_cagr = max(mean_cagr_values)
    best_combo = max(data, key=lambda x: x['mean_cagr_adj'])

    print(f"Best n_positive: {best_n_positive}")
    print(f"Best mean_cagr_adj: {best_cagr:.2f}")
    print(f"Best combo ID: {best_combo['combo_id']}")

    # Clamp display values
    data_display = data.copy()
    for d in data_display:
        d['mean_cagr_adj_display'] = clamp_cagr(d['mean_cagr_adj'])
        d['cagr_raw_full_display'] = clamp_cagr(d['cagr_raw_full'])

    # Get top 50
    top_50 = get_top_combos(data_display, 50)

    # Calculate distributions
    n_positive_dist = {}
    for i in range(11):
        n_positive_dist[i] = len([d for d in data if d['n_positive'] == i])

    # Calculate heatmaps
    # st_factor x st_period heatmap
    st_factor_sorted = sorted(set(d['st_factor'] for d in data))
    st_period_sorted = sorted(set(d['st_period'] for d in data))

    heatmap_st = {}
    for st_f in st_factor_sorted:
        for st_p in st_period_sorted:
            matching = [d for d in data if d['st_factor'] == st_f and d['st_period'] == st_p]
            if matching:
                heatmap_st[f"{st_f}_{st_p}"] = statistics.mean([m['n_positive'] for m in matching])

    # fast_ema x slow_ema heatmap
    fast_ema_sorted = sorted(set(d['fast_ema_len'] for d in data))
    slow_ema_sorted = sorted(set(d['slow_ema_len'] for d in data))

    heatmap_ema = {}
    for fe in fast_ema_sorted:
        for se in slow_ema_sorted:
            matching = [d for d in data if d['fast_ema_len'] == fe and d['slow_ema_len'] == se]
            if matching:
                heatmap_ema[f"{fe}_{se}"] = statistics.mean([m['n_positive'] for m in matching])

    # direction_ema x atr_mult heatmap
    dir_ema_sorted = sorted(set(d['direction_ema_len'] for d in data))
    atr_mult_sorted = sorted(set(d['atr_mult'] for d in data))

    heatmap_dir_atr = {}
    for de in dir_ema_sorted:
        for am in atr_mult_sorted:
            matching = [d for d in data if d['direction_ema_len'] == de and d['atr_mult'] == am]
            if matching:
                heatmap_dir_atr[f"{de}_{am}"] = statistics.mean([m['n_positive'] for m in matching])

    print("Generating HTML...")

    html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>v3 Fine-Grid 최적화 대시보드</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        :root {{
            --dark-bg: #0f0f1e;
            --card-bg: #1a1a2e;
            --text-primary: #e0e0e0;
            --text-secondary: #a0a0a0;
            --accent: #00d4ff;
        }}

        body {{
            background-color: var(--dark-bg);
            color: var(--text-primary);
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }}

        .navbar {{
            background-color: var(--card-bg) !important;
            border-bottom: 1px solid var(--accent);
        }}

        .card {{
            background-color: var(--card-bg);
            border: 1px solid #2d2d3d;
            color: var(--text-primary);
            margin-bottom: 20px;
        }}

        .card-title {{
            color: var(--accent);
            font-weight: 600;
            margin-bottom: 15px;
        }}

        .stat-box {{
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 15px;
            border-left: 4px solid var(--accent);
        }}

        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: var(--accent);
        }}

        .stat-label {{
            color: var(--text-secondary);
            font-size: 0.9em;
            margin-top: 5px;
        }}

        .table {{
            color: var(--text-primary);
            border-color: #2d2d3d;
        }}

        .table thead {{
            background-color: #252535;
            border-color: #2d2d3d;
        }}

        .table tbody tr {{
            border-color: #2d2d3d;
        }}

        .table tbody tr:hover {{
            background-color: #232333 !important;
        }}

        .table tbody tr:nth-child(odd) {{
            background-color: #1d1d2d;
        }}

        .table tbody tr:nth-child(even) {{
            background-color: #15151f;
        }}

        .table th {{
            color: var(--accent);
            font-weight: 600;
            cursor: pointer;
            user-select: none;
        }}

        .table th:hover {{
            background-color: #2d2d3d;
        }}

        .plotly-chart {{
            background-color: var(--card-bg);
            border-radius: 8px;
            padding: 15px;
        }}

        .nav-tabs .nav-link {{
            color: var(--text-secondary);
            border-color: #2d2d3d;
        }}

        .nav-tabs .nav-link.active {{
            color: var(--accent);
            background-color: var(--card-bg);
            border-color: var(--accent);
        }}

        .container-fluid {{
            padding: 20px;
        }}

        h1, h2 {{
            color: var(--accent);
        }}

        .subtitle {{
            color: var(--text-secondary);
            margin-bottom: 30px;
        }}

        .table-sortable {{
            max-height: 800px;
            overflow-y: auto;
        }}
    </style>
</head>
<body>
    <!-- Navigation -->
    <nav class="navbar navbar-dark">
        <div class="container-fluid">
            <span class="navbar-brand mb-0 h1">
                <span style="color: var(--accent);">v3 Fine-Grid 최적화 대시보드</span>
            </span>
            <span class="text-secondary">1,620 Combos | BTC 4h Long 3x</span>
        </div>
    </nav>

    <div class="container-fluid">
        <!-- Summary Stats -->
        <section class="mt-4 mb-4">
            <h2>요약 통계</h2>
            <div class="row">
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value">{len(data)}</div>
                        <div class="stat-label">Total Combos</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value">{best_n_positive}</div>
                        <div class="stat-label">Best n_positive (10 windows)</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-value">{best_cagr:.2f}%</div>
                        <div class="stat-label">Best mean_cagr_adj</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="stat-box">
                        <div class="stat-label">Best Combo Params</div>
                        <div class="stat-value" style="font-size: 1.2em; margin-top: 10px;">
                            ID: {best_combo['combo_id']}<br>
                            <span style="font-size: 0.8em; color: var(--text-secondary);">
                                st_f={best_combo['st_factor']}, st_p={best_combo['st_period']}<br>
                                f_ema={best_combo['fast_ema_len']}, s_ema={best_combo['slow_ema_len']}<br>
                                d_ema={best_combo['direction_ema_len']}, atr_m={best_combo['atr_mult']}
                            </span>
                        </div>
                    </div>
                </div>
            </div>
        </section>

        <!-- Tabs for different sections -->
        <ul class="nav nav-tabs" role="tablist">
            <li class="nav-item">
                <a class="nav-link active" href="#parcoords" data-bs-toggle="tab">파라미터 공간</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="#distributions" data-bs-toggle="tab">분포</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="#boxplots" data-bs-toggle="tab">Box Plots</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="#heatmaps" data-bs-toggle="tab">Heatmaps</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="#scatter" data-bs-toggle="tab">Scatter</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="#table" data-bs-toggle="tab">Top-50 Table</a>
            </li>
        </ul>

        <div class="tab-content">
            <!-- Parallel Coordinates Tab -->
            <div id="parcoords" class="tab-pane fade show active" role="tabpanel">
                <div class="card mt-3">
                    <div class="card-body">
                        <h5 class="card-title">파라미터 공간 탐색 — 10구간 양수 횟수로 색상</h5>
                        <div id="plotParcoords" class="plotly-chart" style="height: 700px;"></div>
                    </div>
                </div>
            </div>

            <!-- Distributions Tab -->
            <div id="distributions" class="tab-pane fade" role="tabpanel">
                <div class="card mt-3">
                    <div class="card-body">
                        <h5 class="card-title">n_positive 분포 히스토그램</h5>
                        <div id="plotHistogram" class="plotly-chart" style="height: 500px;"></div>
                    </div>
                </div>
            </div>

            <!-- Box Plots Tab -->
            <div id="boxplots" class="tab-pane fade" role="tabpanel">
                <div class="row mt-3">
                    <div class="col-lg-6">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">st_factor</h5>
                                <div id="plotBox1" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">st_period</h5>
                                <div id="plotBox2" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 mt-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">fast_ema_len</h5>
                                <div id="plotBox3" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 mt-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">slow_ema_len</h5>
                                <div id="plotBox4" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 mt-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">direction_ema_len</h5>
                                <div id="plotBox5" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 mt-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">atr_mult</h5>
                                <div id="plotBox6" class="plotly-chart" style="height: 400px;"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Heatmaps Tab -->
            <div id="heatmaps" class="tab-pane fade" role="tabpanel">
                <div class="row mt-3">
                    <div class="col-lg-6">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">st_factor × st_period</h5>
                                <div id="plotHeatmap1" class="plotly-chart" style="height: 500px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">fast_ema_len × slow_ema_len</h5>
                                <div id="plotHeatmap2" class="plotly-chart" style="height: 500px;"></div>
                            </div>
                        </div>
                    </div>
                    <div class="col-lg-6 mt-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">direction_ema_len × atr_mult</h5>
                                <div id="plotHeatmap3" class="plotly-chart" style="height: 500px;"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Scatter Tab -->
            <div id="scatter" class="tab-pane fade" role="tabpanel">
                <div class="card mt-3">
                    <div class="card-body">
                        <h5 class="card-title">mean_CAGR vs n_positive (colored by st_factor)</h5>
                        <div id="plotScatter" class="plotly-chart" style="height: 700px;"></div>
                    </div>
                </div>
            </div>

            <!-- Table Tab -->
            <div id="table" class="tab-pane fade" role="tabpanel">
                <div class="card mt-3">
                    <div class="card-body">
                        <h5 class="card-title">Top-50 Combos (Sortable)</h5>
                        <div class="table-sortable">
                            <table class="table table-striped table-sm" id="topTable">
                                <thead>
                                    <tr>
                                        <th style="cursor: pointer;" onclick="sortTable(0)">Rank</th>
                                        <th style="cursor: pointer;" onclick="sortTable(1)">ID</th>
                                        <th style="cursor: pointer;" onclick="sortTable(2)">st_factor</th>
                                        <th style="cursor: pointer;" onclick="sortTable(3)">st_period</th>
                                        <th style="cursor: pointer;" onclick="sortTable(4)">fast_ema</th>
                                        <th style="cursor: pointer;" onclick="sortTable(5)">slow_ema</th>
                                        <th style="cursor: pointer;" onclick="sortTable(6)">dir_ema</th>
                                        <th style="cursor: pointer;" onclick="sortTable(7)">atr_mult</th>
                                        <th style="cursor: pointer;" onclick="sortTable(8)">n_pos</th>
                                        <th style="cursor: pointer;" onclick="sortTable(9)">mean_CAGR%</th>
                                        <th style="cursor: pointer;" onclick="sortTable(10)">worst_MDD%</th>
                                        <th style="cursor: pointer;" onclick="sortTable(11)">full_CAGR%</th>
                                        <th style="cursor: pointer;" onclick="sortTable(12)">Sharpe</th>
                                    </tr>
                                </thead>
                                <tbody id="tableBody">
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Embedded data
        const DATA = {json.dumps(data_display)};
        const TOP_50 = {json.dumps(top_50)};
        const N_POSITIVE_DIST = {json.dumps(n_positive_dist)};
        const ST_FACTOR_SORTED = {json.dumps(st_factor_sorted)};
        const ST_PERIOD_SORTED = {json.dumps(st_period_sorted)};
        const FAST_EMA_SORTED = {json.dumps(fast_ema_sorted)};
        const SLOW_EMA_SORTED = {json.dumps(slow_ema_sorted)};
        const DIR_EMA_SORTED = {json.dumps(dir_ema_sorted)};
        const ATR_MULT_SORTED = {json.dumps(atr_mult_sorted)};

        // Parallel Coordinates
        function plotParallelCoordinates() {{
            const colors = DATA.map(d => d.n_positive);
            const trace = {{
                type: 'parcoords',
                line: {{
                    color: colors,
                    colorscale: 'RdYlGn',
                    showscale: true,
                    cmin: 0,
                    cmax: 10,
                    colorbar: {{
                        title: 'n_positive'
                    }}
                }},
                dimensions: [
                    {{label: 'st_factor', values: DATA.map(d => d.st_factor)}},
                    {{label: 'st_period', values: DATA.map(d => d.st_period)}},
                    {{label: 'fast_ema_len', values: DATA.map(d => d.fast_ema_len)}},
                    {{label: 'slow_ema_len', values: DATA.map(d => d.slow_ema_len)}},
                    {{label: 'direction_ema_len', values: DATA.map(d => d.direction_ema_len)}},
                    {{label: 'atr_mult', values: DATA.map(d => d.atr_mult)}},
                    {{label: 'n_positive', values: DATA.map(d => d.n_positive)}},
                    {{label: 'mean_cagr_adj', values: DATA.map(d => d.mean_cagr_adj_display)}}
                ]
            }};
            const layout = {{
                title: 'Parameter Space Exploration',
                font: {{color: '#e0e0e0'}},
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e',
                margin: {{l: 150, r: 50, t: 50, b: 50}}
            }};
            Plotly.newPlot('plotParcoords', [trace], layout, {{responsive: true}});
        }}

        // n_positive Histogram
        function plotHistogram() {{
            const x_vals = [];
            const y_vals = [];
            for (let i = 0; i <= 10; i++) {{
                x_vals.push(i.toString());
                y_vals.push(N_POSITIVE_DIST[i] || 0);
            }}
            const trace = {{
                x: x_vals,
                y: y_vals,
                type: 'bar',
                marker: {{
                    color: y_vals,
                    colorscale: 'Blues',
                    showscale: false
                }},
                text: y_vals,
                textposition: 'outside'
            }};
            const layout = {{
                title: 'Distribution of n_positive (10 windows)',
                xaxis: {{title: 'n_positive'}},
                yaxis: {{title: 'Count of Combos'}},
                font: {{color: '#e0e0e0'}},
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e',
                xaxis: {{
                    title: 'n_positive',
                    gridcolor: '#2d2d3d'
                }},
                yaxis: {{
                    title: 'Count',
                    gridcolor: '#2d2d3d'
                }}
            }};
            Plotly.newPlot('plotHistogram', [trace], layout, {{responsive: true}});
        }}

        // Box Plots for each parameter
        function createBoxPlot(divId, paramName, paramKey) {{
            const uniqueVals = [...new Set(DATA.map(d => d[paramKey]))].sort((a, b) => a - b);
            const traces = [];

            for (const val of uniqueVals) {{
                const cagrVals = DATA.filter(d => d[paramKey] === val).map(d => d.mean_cagr_adj_display);
                traces.push({{
                    y: cagrVals,
                    name: val.toString(),
                    type: 'box',
                    boxmean: 'sd'
                }});
            }}

            const layout = {{
                title: paramName,
                yaxis: {{title: 'mean_cagr_adj (%)', gridcolor: '#2d2d3d'}},
                font: {{color: '#e0e0e0'}},
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e',
                showlegend: false
            }};
            Plotly.newPlot(divId, traces, layout, {{responsive: true}});
        }}

        // Heatmaps
        function createHeatmap(divId, xVals, yVals, zData, xLabel, yLabel) {{
            const trace = {{
                z: zData,
                x: xVals,
                y: yVals,
                type: 'heatmap',
                colorscale: 'RdYlGn',
                colorbar: {{title: 'Avg n_positive'}}
            }};
            const layout = {{
                title: '{{xLabel}} × {{yLabel}}',
                xaxis: {{title: xLabel}},
                yaxis: {{title: yLabel}},
                font: {{color: '#e0e0e0'}},
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e'
            }};
            Plotly.newPlot(divId, [trace], layout, {{responsive: true}});
        }}

        function plotHeatmaps() {{
            // st_factor x st_period
            const hm1_z = [];
            for (const st_p of ST_PERIOD_SORTED) {{
                const row = [];
                for (const st_f of ST_FACTOR_SORTED) {{
                    const key = st_f + '_' + st_p;
                    row.push({json.dumps(heatmap_st)}[key] || 0);
                }}
                hm1_z.push(row);
            }}
            createHeatmap('plotHeatmap1', ST_FACTOR_SORTED, ST_PERIOD_SORTED, hm1_z, 'st_factor', 'st_period');

            // fast_ema x slow_ema
            const hm2_z = [];
            for (const se of SLOW_EMA_SORTED) {{
                const row = [];
                for (const fe of FAST_EMA_SORTED) {{
                    const key = fe + '_' + se;
                    row.push({json.dumps(heatmap_ema)}[key] || 0);
                }}
                hm2_z.push(row);
            }}
            createHeatmap('plotHeatmap2', FAST_EMA_SORTED, SLOW_EMA_SORTED, hm2_z, 'fast_ema_len', 'slow_ema_len');

            // direction_ema x atr_mult
            const hm3_z = [];
            for (const am of ATR_MULT_SORTED) {{
                const row = [];
                for (const de of DIR_EMA_SORTED) {{
                    const key = de + '_' + am;
                    row.push({json.dumps(heatmap_dir_atr)}[key] || 0);
                }}
                hm3_z.push(row);
            }}
            createHeatmap('plotHeatmap3', DIR_EMA_SORTED, ATR_MULT_SORTED, hm3_z, 'direction_ema_len', 'atr_mult');
        }}

        // Scatter plot
        function plotScatter() {{
            const stFactors = [...new Set(DATA.map(d => d.st_factor))].sort((a, b) => a - b);
            const traces = [];
            const colorMap = {{}};
            const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E2'];

            stFactors.forEach((sf, idx) => {{
                colorMap[sf] = colors[idx % colors.length];
            }});

            stFactors.forEach(sf => {{
                const points = DATA.filter(d => d.st_factor === sf);
                const trace = {{
                    x: points.map(p => p.n_positive + (Math.random() - 0.5) * 0.3),
                    y: points.map(p => p.mean_cagr_adj_display),
                    mode: 'markers',
                    type: 'scatter',
                    name: 'st_f=' + sf,
                    marker: {{
                        size: points.map(p => Math.min(p.total_trades / 10, 20)),
                        color: colorMap[sf],
                        opacity: 0.7
                    }},
                    text: points.map(p =>
                        'ID: ' + p.combo_id + '<br>' +
                        'st_f: ' + p.st_factor + '<br>' +
                        'st_p: ' + p.st_period + '<br>' +
                        'f_ema: ' + p.fast_ema_len + '<br>' +
                        's_ema: ' + p.slow_ema_len + '<br>' +
                        'd_ema: ' + p.direction_ema_len + '<br>' +
                        'atr_m: ' + p.atr_mult + '<br>' +
                        'n_pos: ' + p.n_positive + '<br>' +
                        'mean_CAGR: ' + p.mean_cagr_adj.toFixed(2) + '%<br>' +
                        'total_trades: ' + p.total_trades
                    ),
                    hoverinfo: 'text'
                }};
                traces.push(trace);
            }});

            const layout = {{
                title: 'mean_CAGR vs n_positive',
                xaxis: {{title: 'n_positive', gridcolor: '#2d2d3d'}},
                yaxis: {{title: 'mean_cagr_adj (%)', gridcolor: '#2d2d3d'}},
                font: {{color: '#e0e0e0'}},
                plot_bgcolor: '#1a1a2e',
                paper_bgcolor: '#1a1a2e',
                hovermode: 'closest',
                showlegend: true,
                legend: {{x: 1.05, y: 1}}
            }};
            Plotly.newPlot('plotScatter', traces, layout, {{responsive: true}});
        }}

        // Populate Table
        function populateTable() {{
            const tbody = document.getElementById('tableBody');
            TOP_50.forEach((row, idx) => {{
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${{idx + 1}}</td>
                    <td>${{row.combo_id}}</td>
                    <td>${{row.st_factor.toFixed(1)}}</td>
                    <td>${{row.st_period}}</td>
                    <td>${{row.fast_ema_len}}</td>
                    <td>${{row.slow_ema_len}}</td>
                    <td>${{row.direction_ema_len}}</td>
                    <td>${{row.atr_mult.toFixed(1)}}</td>
                    <td><strong>${{row.n_positive}}/10</strong></td>
                    <td>${{row.mean_cagr_adj.toFixed(2)}}</td>
                    <td>${{row.worst_mdd.toFixed(2)}}</td>
                    <td>${{row.cagr_raw_full.toFixed(2)}}</td>
                    <td>${{row.sharpe_full.toFixed(3)}}</td>
                `;
                tbody.appendChild(tr);
            }});
        }}

        // Simple table sorting
        function sortTable(n) {{
            const table = document.getElementById('topTable');
            let rows = Array.from(table.querySelectorAll('tbody tr'));
            let ascending = table.dataset.sortCol === n ? table.dataset.ascending !== 'true' : true;

            rows.sort((a, b) => {{
                const aVal = a.cells[n].textContent.trim();
                const bVal = b.cells[n].textContent.trim();
                const aNum = parseFloat(aVal) || aVal;
                const bNum = parseFloat(bVal) || bVal;

                if (ascending) {{
                    return aNum > bNum ? 1 : -1;
                }} else {{
                    return aNum < bNum ? 1 : -1;
                }}
            }});

            const tbody = document.getElementById('tableBody');
            rows.forEach(row => tbody.appendChild(row));
            table.dataset.sortCol = n;
            table.dataset.ascending = ascending;
        }}

        // Initialize all plots
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('Initializing plots...');
            plotParallelCoordinates();
            plotHistogram();
            createBoxPlot('plotBox1', 'st_factor', 'st_factor');
            createBoxPlot('plotBox2', 'st_period', 'st_period');
            createBoxPlot('plotBox3', 'fast_ema_len', 'fast_ema_len');
            createBoxPlot('plotBox4', 'slow_ema_len', 'slow_ema_len');
            createBoxPlot('plotBox5', 'direction_ema_len', 'direction_ema_len');
            createBoxPlot('plotBox6', 'atr_mult', 'atr_mult');
            plotHeatmaps();
            plotScatter();
            populateTable();
            console.log('Plots initialized successfully');
        }});
    </script>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.0/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

    # Write HTML file
    with open(OUTPUT_PATH, 'w') as f:
        f.write(html_content)

    print(f"HTML file written to: {OUTPUT_PATH}")
    print(f"File size: {OUTPUT_PATH.stat().st_size / (1024*1024):.2f} MB")

if __name__ == '__main__':
    main()
