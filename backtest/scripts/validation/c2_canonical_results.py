import json
import pandas as pd
from pathlib import Path

ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
OUT = ROOT / 'results/validation_phase5'

PERIODS = ['p0', 'p1', 'p2', 'p3', 'p4']

rows = []

# Helper to make hp_json string
def hp_str(hp): return json.dumps(hp, sort_keys=True)

# ──────────────── SOURCE 1: Sweep ────────────────
# Load v3 first, then v2 (v3 takes priority)
sweep_loaded = set()  # track (strat, tf, variant, combo)

for ver in ('v3', 'v2'):
    base = ROOT / 'results/param_sweep' / ver
    if not base.exists():
        continue
    for summary_path in sorted(base.rglob('summary.json')):
        try:
            s = json.loads(summary_path.read_text())
            if s.get('score', -999) <= -998:
                continue
            parts = summary_path.parts
            variant = parts[-3]
            tf = parts[-4]
            strat = parts[-5]
            combo = int(parts[-2].replace('combo_', ''))
            key = (strat, tf, variant, combo)
            if key in sweep_loaded:
                continue  # v3 already loaded
            sweep_loaded.add(key)

            hp = s.get('hp', {})
            for p_key, m in s.get('periods', {}).items():
                rows.append({
                    'strat': strat, 'tf': tf, 'variant': variant,
                    'combo': combo, 'version': ver, 'period': p_key,
                    'source': 'sweep',
                    'cagr': m.get('cagr'),
                    'mdd': m.get('mdd'),
                    'sharpe': m.get('sharpe'),
                    'trades': m.get('trades'),
                    'pf': m.get('pf'),
                    'hp_json': hp_str(hp),
                })
        except Exception as e:
            pass

# ──────────────── SOURCE 2: Intrabar ────────────────
intrabar_base = ROOT / 'results/intrabar'
if intrabar_base.exists():
    for stats_path in sorted(intrabar_base.rglob('stats.json')):
        try:
            s = json.loads(stats_path.read_text())
            parts = stats_path.parts
            period = parts[-2]
            combo = int(parts[-3].replace('combo_', ''))
            variant = parts[-4]
            tf = parts[-5]
            strat = parts[-6]
            if period not in PERIODS:
                continue
            rows.append({
                'strat': strat, 'tf': tf, 'variant': variant,
                'combo': combo, 'version': 'intrabar', 'period': period,
                'source': 'intrabar',
                'cagr': s.get('cagr_pct') or s.get('annual_return_pct'),
                'mdd': s.get('max_drawdown_pct'),
                'sharpe': s.get('sharpe_ratio'),
                'trades': s.get('total_trades'),
                'pf': s.get('profit_factor') or (s.get('raw_metrics', {}).get('win_rate')),
                'hp_json': '{}',  # not stored in stats.json
            })
        except Exception:
            pass

# ──────────────── SOURCE 3: Adjusted costs ────────────────
adj_base = ROOT / 'results/adjusted_costs'
if adj_base.exists():
    for adj_path in sorted(adj_base.rglob('adjusted_stats.json')):
        try:
            s = json.loads(adj_path.read_text())
            strat = s['strat']
            tf = s['tf']
            variant = s['variant']
            combo = s['combo']
            hp = s.get('hp', {})
            for p_key, m in s.get('periods', {}).items():
                rows.append({
                    'strat': strat, 'tf': tf, 'variant': variant,
                    'combo': combo, 'version': s.get('version', 'v3'), 'period': p_key,
                    'source': 'adjusted',
                    'cagr': m.get('adj_cagr'),
                    'mdd': m.get('adj_mdd'),
                    'sharpe': m.get('adj_sharpe'),
                    'trades': m.get('trades'),
                    'pf': m.get('pf'),
                    'hp_json': hp_str(hp),
                })
        except Exception:
            pass

# ──────────────── Build DataFrame ────────────────
df = pd.DataFrame(rows)
df['cagr'] = df['cagr'].round(4)
df['mdd'] = df['mdd'].round(4)
df['sharpe'] = df['sharpe'].round(4)

OUT.mkdir(parents=True, exist_ok=True)
csv_path = OUT / 'canonical_results.csv'
df.to_csv(csv_path, index=False)
print(f"Written: {csv_path} — {len(df)} rows")

# ──────────────── combo_18 spotlight table ────────────────
c18 = df[(df['strat']=='supertrend') & (df['tf']=='4h') & (df['variant']=='long_only') & (df['combo']==18)]
print("\ncombo_18 (supertrend/4h/long_only) — all 3 sources:")
print(c18[['period','source','cagr','mdd','sharpe','trades']].sort_values(['period','source']).to_string(index=False))

# Write summary markdown
c18_md = c18[['period','source','cagr','mdd','sharpe','trades']].sort_values(['period','source'])
md = "# C-2 Canonical Results — combo_18 Spotlight\n\n"
md += f"Total rows: {len(df)} | Sources: {df['source'].unique().tolist()} | Combos: {df[['strat','tf','variant','combo']].drop_duplicates().shape[0]}\n\n"
md += "## combo_18 (supertrend/4h/long_only) — 3-source comparison\n\n"
md += "| Period | Source | CAGR | MDD | Sharpe | Trades |\n"
md += "|---|---|---|---|---|---|\n"
for _, row in c18_md.iterrows():
    md += f"| {row['period']} | {row['source']} | {row['cagr']:.2f}% | {row['mdd']:.2f}% | {row['sharpe']:.3f} | {int(row['trades']) if pd.notna(row['trades']) else '—'} |\n"
(OUT / 'c2_summary.md').write_text(md)
print("\nC-2 complete")
