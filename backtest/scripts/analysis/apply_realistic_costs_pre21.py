#!/usr/bin/env python3
"""
apply_realistic_costs_pre21.py

Read 30 raw backtest result files from pre2021_backfill (5 strategies × 6 periods),
classify funding data coverage per period, apply realistic costs (fee delta + funding),
and write all_adjusted_results_pre21.json.

Input schema (result.json from pre21_backfill):
  {
    "strat": "supertrend", "tf": "4h", "variant": "long_only",
    "period": "pre21_bear",
    "champ_src": ["v4", 18],
    "hp": {...},
    "metrics": {
      "annual_return_pct": ..., "cagr_pct": ..., "sharpe_ratio": ...,
      "max_drawdown_pct": ..., "total_trades": ..., "profit_factor": ...,
      "net_profit_pct": ...
    },
    "period_meta": {"start": "2017-12-17", "end": "2018-12-15"}
  }

Funding sources (priority: Bybit live > Binance vision > Binance API 2019 > fee-only):
  1. Bybit live (2020-03-25+): BTCUSDT_8h.parquet → timestamp (ms), funding_rate
  2. Binance vision (2020-01 ~ 2020-03-24): BTCUSDT/2020-0*.parquet → calc_time (UTC), last_funding_rate
  3. Binance API 2019 (2019-09-08 ~ 2019-12-31): BTCUSDT_2019.parquet → timestamp (ms), funding_rate
  4. fee-only: no funding data coverage

Output:
  - all_adjusted_results_pre21.json: 5 champions across all 6 periods
  - Per-job: adjusted_costs_pre2021/<strat>/<tf>/<variant>/<period>/adjusted_stats.json
"""
import json
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np

# ─── Config ───────────────────────────────────────────────────────────────────
_HOST_ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
if Path('/data').exists() and Path('/result').exists():
    # Running inside Docker container
    _DATA_ROOT   = Path('/data')
    _RESULT_ROOT = Path('/result')
else:
    _DATA_ROOT   = _HOST_ROOT / 'data'
    _RESULT_ROOT = _HOST_ROOT / 'results'

BACKFILL_DIR = _RESULT_ROOT / 'pre2021_backfill'
OUT_BASE     = _RESULT_ROOT / 'adjusted_costs_pre2021'

BYBIT_FUNDING     = _DATA_ROOT / 'funding' / 'BTCUSDT_8h.parquet'
BINANCE_VISION_DIR = _DATA_ROOT / 'funding' / 'binance_vision' / 'BTCUSDT'
BINANCE_API_2019  = _DATA_ROOT / 'funding' / 'binance_api' / 'BTCUSDT_2019.parquet'
BINANCE_API_FAIL  = _DATA_ROOT / 'funding' / 'binance_api' / 'FETCH_FAILED.marker'

# Period definitions (all before 2021-01-01)
PRE21_PERIODS = {
    'pre21_full':     ('2017-08-18', '2020-12-31'),
    'pre21_bear':     ('2017-12-17', '2018-12-15'),
    'pre21_range':    ('2018-12-16', '2019-04-01'),
    'pre21_recovery': ('2019-04-02', '2020-02-29'),
    'pre21_covid':    ('2020-03-01', '2020-04-30'),
    'pre21_bull':     ('2020-05-01', '2020-12-31'),
}

PRE21_PERIOD_YEARS = {
    'pre21_full':     3.37,
    'pre21_bear':     0.99,
    'pre21_range':    0.29,
    'pre21_recovery': 0.91,
    'pre21_covid':    0.17,
    'pre21_bull':     0.67,
}

# Timeframe → hold hours (average position duration)
TF_HOLD_HOURS = {
    '1h': 10,
    '4h': 32,
    '1D': 96,
}

# Cost model
FEE_DELTA_PER_SIDE = (0.055 - 0.020) / 100.0  # 0.00035


# ─── Funding data loading ──────────────────────────────────────────────────────
def load_funding_sources():
    """
    Load all 3 funding sources (gracefully handle missing).
    Returns: (bybit_df, vision_df, api2019_df)
    """
    bybit_df = None
    vision_df = None
    api2019_df = None

    # 1. Bybit live (2020-03-25+)
    if BYBIT_FUNDING.exists():
        try:
            bybit_df = pd.read_parquet(BYBIT_FUNDING)
            print(
                f'[funding] Bybit: {len(bybit_df)} rows '
                f'({bybit_df["funding_rate"].ne(0).sum()} non-zero)'
            )
        except Exception as e:
            print(f'[warning] Could not load Bybit funding: {e}')

    # 2. Binance vision (2020-01 ~ 2020-03-24)
    try:
        vision_files = sorted(BINANCE_VISION_DIR.glob('2020-0*.parquet'))
        if vision_files:
            dfs = []
            for fpath in vision_files:
                df = pd.read_parquet(fpath)
                # Rename columns: calc_time → timestamp (ms), last_funding_rate → funding_rate
                df = df.rename(columns={
                    'calc_time': 'timestamp',
                    'last_funding_rate': 'funding_rate'
                })
                # Convert timestamp to ms int64 (datetime64[ms] → int64 gives ms epoch directly in pandas 2+)
                df['timestamp'] = df['timestamp'].astype('int64')
                dfs.append(df[['timestamp', 'funding_rate']])
            vision_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
            print(
                f'[funding] Binance vision: {len(vision_df)} rows '
                f'({vision_df["funding_rate"].ne(0).sum()} non-zero)'
            )
    except Exception as e:
        print(f'[warning] Could not load Binance vision funding: {e}')

    # 3. Binance API 2019 (2019-09-08 ~ 2019-12-31)
    if BINANCE_API_2019.exists() and not BINANCE_API_FAIL.exists():
        try:
            api2019_df = pd.read_parquet(BINANCE_API_2019)
            print(
                f'[funding] Binance API 2019: {len(api2019_df)} rows '
                f'({api2019_df["funding_rate"].ne(0).sum()} non-zero)'
            )
        except Exception as e:
            print(f'[warning] Could not load Binance API 2019: {e}')
    elif BINANCE_API_FAIL.exists():
        print('[funding] Binance API 2019: fetch failed (FETCH_FAILED.marker exists)')

    return bybit_df, vision_df, api2019_df


def _str_to_ms(date_str: str) -> int:
    """Convert 'YYYY-MM-DD' to ms since epoch (UTC)."""
    dt = pd.Timestamp(date_str, tz='UTC')
    return int(dt.timestamp() * 1000)


def _date_str_to_ms(date_str: str) -> int:
    """Same as _str_to_ms."""
    return _str_to_ms(date_str)


def classify_funding_coverage(period_start_str: str, period_end_str: str,
                              bybit_ms: np.ndarray, proxy_ms: np.ndarray,
                              api2019_ms: np.ndarray):
    """
    Classify funding coverage for a period.

    Args:
      period_start_str, period_end_str: 'YYYY-MM-DD'
      bybit_ms, proxy_ms, api2019_ms: sorted arrays of timestamps (ms int64)

    Returns:
      (coverage_type, breakdown_dict)
      where coverage_type ∈ ['bybit_live', 'binance_proxy', 'binance_api', 'fee_only']
      and breakdown_dict = {bybit_live_days, binance_proxy_days, binance_api_days, fee_only_days}
    """
    period_start_ms = _date_str_to_ms(period_start_str)
    period_end_ms = _date_str_to_ms(period_end_str)

    # Generate daily timestamps
    daily_ms = []
    current_ms = period_start_ms
    while current_ms < period_end_ms:
        daily_ms.append(current_ms)
        current_ms += 86400000  # 24h in ms

    # For each day, find which source covers it
    bybit_live_count = 0
    proxy_count = 0
    api2019_count = 0
    fee_only_count = 0

    for day_ms in daily_ms:
        day_end_ms = day_ms + 86400000

        # Check if covered by each source (at least 1 row in that day)
        has_bybit = np.any((bybit_ms >= day_ms) & (bybit_ms < day_end_ms))
        has_proxy = np.any((proxy_ms >= day_ms) & (proxy_ms < day_end_ms))
        has_api = np.any((api2019_ms >= day_ms) & (api2019_ms < day_end_ms))

        if has_bybit:
            bybit_live_count += 1
        elif has_proxy:
            proxy_count += 1
        elif has_api:
            api2019_count += 1
        else:
            fee_only_count += 1

    total_days = len(daily_ms)

    # Determine primary coverage type (>= 95% of days)
    if bybit_live_count >= 0.95 * total_days:
        coverage_type = 'bybit_live'
    elif proxy_count >= 0.95 * total_days:
        coverage_type = 'binance_proxy'
    elif api2019_count >= 0.95 * total_days:
        coverage_type = 'binance_api'
    else:
        coverage_type = 'mixed' if fee_only_count < total_days else 'fee_only'

    breakdown = {
        'bybit_live_days': bybit_live_count,
        'binance_proxy_days': proxy_count,
        'binance_api_days': api2019_count,
        'fee_only_days': fee_only_count,
    }

    return coverage_type, breakdown


def compute_avg_funding(combined_df: pd.DataFrame, period_start_str: str,
                        period_end_str: str) -> float:
    """
    Compute average funding rate for a period.

    combined_df must have: timestamp (ms int64), funding_rate (float64)
    Returns: average funding rate (0.0 if no data or all-zero)
    """
    if combined_df is None or len(combined_df) == 0:
        return 0.0

    start_ms = _date_str_to_ms(period_start_str)
    end_ms = _date_str_to_ms(period_end_str)

    mask = (combined_df['timestamp'] >= start_ms) & \
           (combined_df['timestamp'] < end_ms) & \
           (combined_df['funding_rate'] != 0)

    if not mask.any():
        return 0.0

    avg = combined_df.loc[mask, 'funding_rate'].mean()
    return float(avg) if not np.isnan(avg) else 0.0


# ─── Cost application ──────────────────────────────────────────────────────────
def apply_costs_to_period(metrics: dict, tf: str, variant: str, period_years: float,
                          avg_funding_rate: float) -> dict:
    """
    Apply realistic costs to a single period's metrics.

    Args:
      metrics: {'annual_return_pct', 'cagr_pct', 'sharpe_ratio', 'max_drawdown_pct',
                'total_trades', 'profit_factor', ...}
      tf: '1h', '4h', '1D'
      variant: 'long_only', 'short_only', 'bidirectional', 'both_ways'
      period_years: years (float)
      avg_funding_rate: average funding rate (0.0 if fee-only)

    Returns:
      dict with adjusted metrics + cost breakdown
    """
    cagr = metrics.get('cagr_pct', 0)
    mdd = metrics.get('max_drawdown_pct', 0)
    sharpe = metrics.get('sharpe_ratio', 0)
    trades = metrics.get('total_trades', 0)
    pf = metrics.get('profit_factor', 1)

    hold_hours = TF_HOLD_HOURS.get(tf, 32)
    n_funding = hold_hours / 8.0  # number of 8h funding periods

    # Strip leverage suffix to get base variant for funding direction
    base_variant = re.sub(r'_x\d+$', '', variant)
    lev_match = re.search(r'_x(\d+)$', variant)
    leverage_mult = int(lev_match.group(1)) if lev_match else 1

    # Funding sign based on base variant (long_only_x2/x3 → base = long_only → +1)
    if base_variant == 'long_only':
        funding_sign = +1.0
    elif base_variant == 'short_only':
        funding_sign = -1.0
    else:  # bidirectional, both_ways
        funding_sign = 0.0

    # Annualized cost calculation
    trades_per_year = trades / period_years if period_years > 0 else 0

    # Fee cost: scaled by leverage (leveraged positions pay proportionally more)
    fee_cost_pct_annual = trades_per_year * FEE_DELTA_PER_SIDE * 2 * 100 * leverage_mult

    # Funding cost: scaled by leverage
    fund_cost_pct_annual = (
        trades_per_year * n_funding * avg_funding_rate * funding_sign * 100 * leverage_mult
    )

    total_cost = fee_cost_pct_annual + fund_cost_pct_annual

    # Adjusted metrics
    adj_cagr = round(cagr - total_cost, 2)
    adj_mdd = round(mdd - abs(fee_cost_pct_annual) * 0.5, 2)

    if cagr != 0:
        adj_sharpe = round(sharpe * (adj_cagr / cagr), 3)
    else:
        adj_sharpe = 0.0

    return {
        'original_cagr': cagr,
        'adj_cagr': adj_cagr,
        'original_mdd': mdd,
        'adj_mdd': adj_mdd,
        'original_sharpe': sharpe,
        'adj_sharpe': adj_sharpe,
        'trades': trades,
        'pf': pf,
        'fee_cost_annual_pct': round(fee_cost_pct_annual, 3),
        'funding_cost_annual_pct': round(fund_cost_pct_annual, 3),
        'avg_funding_rate': round(avg_funding_rate * 100, 5),
    }


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    print('=' * 80)
    print('Pre-2021 Backtest Cost Adjustment')
    print('=' * 80)

    # 1. Load funding sources
    print('\n[1] Loading funding sources...')
    bybit_df, vision_df, api2019_df = load_funding_sources()

    # Build combined funding dataframe (priority: bybit > vision > api2019)
    combined_df = pd.DataFrame({'timestamp': [], 'funding_rate': []})

    if bybit_df is not None:
        combined_df = pd.concat(
            [combined_df, bybit_df[['timestamp', 'funding_rate']]],
            ignore_index=True
        )
    if vision_df is not None:
        combined_df = pd.concat(
            [combined_df, vision_df[['timestamp', 'funding_rate']]],
            ignore_index=True
        )
    if api2019_df is not None:
        combined_df = pd.concat(
            [combined_df, api2019_df[['timestamp', 'funding_rate']]],
            ignore_index=True
        )

    # Remove duplicates, keep first (highest priority)
    if len(combined_df) > 0:
        combined_df = combined_df.sort_values('timestamp')
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='first')
        combined_df = combined_df.reset_index(drop=True)

    print(f'  Combined: {len(combined_df)} rows')

    # Extract timestamp arrays for classification (non-zero rows only — zeros mean no live data)
    if bybit_df is not None:
        bybit_ms = bybit_df.loc[bybit_df['funding_rate'] != 0, 'timestamp'].values
    else:
        bybit_ms = np.array([], dtype='int64')

    if vision_df is not None:
        vision_ms = vision_df.loc[vision_df['funding_rate'] != 0, 'timestamp'].values
    else:
        vision_ms = np.array([], dtype='int64')

    if api2019_df is not None:
        api2019_ms = api2019_df.loc[api2019_df['funding_rate'] != 0, 'timestamp'].values
    else:
        api2019_ms = np.array([], dtype='int64')

    # 2. Scan backfill results
    print('\n[2] Scanning pre2021_backfill results...')
    if not BACKFILL_DIR.exists():
        print(f'  WARNING: {BACKFILL_DIR} does not exist. Exiting.')
        return

    result_files = list(BACKFILL_DIR.rglob('result.json'))
    if not result_files:
        print(f'  WARNING: No result.json files found in {BACKFILL_DIR}. Exiting.')
        return

    print(f'  Found {len(result_files)} result.json files')

    # 3. Group by (strat, tf, variant) to identify champions
    print('\n[3] Grouping results by (strat, tf, variant)...')
    grouped = defaultdict(lambda: {})  # {(strat, tf, variant): {period: result}}

    for result_path in result_files:
        result_data = json.loads(result_path.read_text())

        strat = result_data.get('strat')
        tf = result_data.get('tf')
        variant = result_data.get('variant')
        period = result_data.get('period')

        if not all([strat, tf, variant, period]):
            print(f'  WARNING: Malformed result.json: {result_path}')
            continue

        key = (strat, tf, variant)
        grouped[key][period] = result_data

    print(f'  Identified {len(grouped)} unique champion combinations')

    # 4. Apply costs to each champion
    print('\n[4] Applying costs...')
    results = []

    for combo_idx, (key, periods_dict) in enumerate(grouped.items(), 1):
        strat, tf, variant = key

        # Pick first result to get champ_src
        first_result = next(iter(periods_dict.values()))
        champ_src = first_result.get('champ_src', ['unknown', 0])
        if isinstance(champ_src, (list, tuple)):
            version, combo = (champ_src[0], champ_src[1]) if len(champ_src) >= 2 else ('unknown', 0)
        else:
            parts = str(champ_src).split('/')
            version = parts[0]
            combo = int(parts[1].replace('combo_', '')) if len(parts) > 1 else 0
        hp = first_result.get('hp', {})

        adj_periods = {}
        period_scores = []

        for period_key in sorted(PRE21_PERIODS.keys()):
            if period_key not in periods_dict:
                continue

            result_data = periods_dict[period_key]
            metrics = result_data.get('metrics', {})
            period_meta = result_data.get('period_meta', {})

            period_start = period_meta.get('start') or PRE21_PERIODS[period_key][0]
            period_end = period_meta.get('end') or PRE21_PERIODS[period_key][1]
            period_years = PRE21_PERIOD_YEARS[period_key]

            # Classify funding coverage
            coverage_type, breakdown = classify_funding_coverage(
                period_start, period_end, bybit_ms, vision_ms, api2019_ms
            )

            # Compute average funding rate
            avg_funding = compute_avg_funding(combined_df, period_start, period_end)

            # Apply costs
            adj_metrics = apply_costs_to_period(
                metrics, tf, variant, period_years, avg_funding
            )
            adj_metrics['funding_coverage'] = coverage_type
            adj_metrics['funding_coverage_breakdown'] = breakdown

            adj_periods[period_key] = adj_metrics
            period_scores.append(adj_metrics['adj_cagr'])

        # Compute adjusted score (mean of adj_cagr across available periods)
        adjusted_score = round(
            statistics.mean(period_scores), 2
        ) if period_scores else -999.0

        result_entry = {
            'strat': strat,
            'tf': tf,
            'variant': variant,
            'combo': int(combo),
            'version': version,
            'hp': hp,
            'adjusted_score': adjusted_score,
            'periods': adj_periods,
        }
        results.append(result_entry)

        # Write per-job adjusted_stats.json
        for period_key, adj_data in adj_periods.items():
            out_dir = OUT_BASE / strat / tf / variant / period_key
            try:
                out_dir.mkdir(parents=True, exist_ok=True)

                job_stats = {
                    'strat': strat,
                    'tf': tf,
                    'variant': variant,
                    'period': period_key,
                    'combo': int(combo),
                    'version': version,
                    'hp': hp,
                    'adjusted_metrics': adj_data,
                }
                (out_dir / 'adjusted_stats.json').write_text(
                    json.dumps(job_stats, indent=2)
                )
            except (PermissionError, OSError):
                pass  # Skip per-job file if owned by another user (e.g. Docker wrote it first)

        if combo_idx % 5 == 0:
            print(f'  {combo_idx}/{len(grouped)} processed')

    print(f'  {len(results)}/{len(grouped)} champions processed ✓')

    # 5. Write aggregate output
    print('\n[5] Writing output files...')
    output_json = OUT_BASE / 'all_adjusted_results_pre21.json'
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(results, indent=2))
    print(f'  {output_json}')

    # 6. Summary statistics
    print('\n[6] Summary:')
    scores = [r['adjusted_score'] for r in results if r['adjusted_score'] > -998]
    if scores:
        print(f'  Adjusted CAGR (mean): {statistics.mean(scores):.2f}%')
        print(f'  Adjusted CAGR (max):  {max(scores):.2f}%')
        print(f'  Adjusted CAGR (min):  {min(scores):.2f}%')
        print(f'  Adjusted CAGR (median): {sorted(scores)[len(scores)//2]:.2f}%')

    print(f'\n  Funding coverage by period:')
    coverage_stats = defaultdict(int)
    for result in results:
        for period_key, period_data in result['periods'].items():
            cov = period_data.get('funding_coverage', 'unknown')
            coverage_stats[(period_key, cov)] += 1

    for period_key in sorted(PRE21_PERIODS.keys()):
        counts = {}
        for (p, cov), cnt in coverage_stats.items():
            if p == period_key:
                counts[cov] = cnt
        if counts:
            cov_str = ', '.join(f'{cov}:{cnt}' for cov, cnt in sorted(counts.items()))
            print(f'    {period_key}: {cov_str}')

    print('\n' + '=' * 80)
    print('Completed!')
    print('=' * 80)


if __name__ == '__main__':
    main()
