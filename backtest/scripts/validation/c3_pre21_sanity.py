#!/usr/bin/env python3
"""
c3_pre21_sanity.py — Sanity-check pre-2021 backfill results against existing sweep data.

Runs AFTER pre21_backfill.py and apply_realistic_costs_pre21.py complete.
Validates:
  - C1: All 30 jobs completed (5 strategies × 6 periods)
  - C2: adj_results_pre21 exists and has 5 champions
  - C3: supertrend p0 overlap (sign consistency check)
  - C4: pre21_bear has fee_only funding coverage for all 5
  - C5: pre21_bull has bybit_live funding coverage
  - C6: Trade count sanity checks

Output: backtest/results/validation_pre21_sanity.json
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# ── Constants ──────────────────────────────────────────────────────────────────
ROOT = Path('/home/justant/Data/Bit-Mania/backtest')
BACKFILL = ROOT / 'results' / 'pre2021_backfill'
ADJ_PRE21 = ROOT / 'results' / 'adjusted_costs_pre2021' / 'all_adjusted_results_pre21.json'
V4_SWEEP = ROOT / 'results' / 'param_sweep_v4'
OUT = ROOT / 'results' / 'validation_pre21_sanity.json'

STRATEGIES_MANIFEST = [
    {
        'strat': 'supertrend', 'tf': '4h', 'variant': 'long_only',
        'cls_name': 'SupertrendStrategy',
        'champ_src': ('v4', 18),
    },
    {
        'strat': 'supertrend_trendtype', 'tf': '4h', 'variant': 'long_only',
        'cls_name': 'SupertrendTrendTypeStrategy',
        'champ_src': ('v3', 6),
    },
    {
        'strat': 'trendtype', 'tf': '1D', 'variant': 'long_only',
        'cls_name': 'TrendTypeStrategy',
        'champ_src': ('v3', 6),
    },
    {
        'strat': 'tradeiq_psar_ha', 'tf': '1D', 'variant': 'long_only',
        'cls_name': 'TradeIQPsarHaStrategy',
        'champ_src': ('v3', 8),
    },
    {
        'strat': 'tradeiq_cci_ce', 'tf': '4h', 'variant': 'bidirectional',
        'cls_name': 'TradeIQCciCeStrategy',
        'champ_src': ('v3', 2),
    },
]

PRE21_PERIODS = {
    'pre21_full': ('2017-08-18', '2020-12-31'),
    'pre21_bear': ('2017-12-17', '2018-12-15'),
    'pre21_range': ('2018-12-16', '2019-04-01'),
    'pre21_recovery': ('2019-04-02', '2020-02-29'),
    'pre21_covid': ('2020-03-01', '2020-04-30'),
    'pre21_bull': ('2020-05-01', '2020-12-31'),
}

PRE21_PERIOD_YEARS = {
    'pre21_full': 3.37,
    'pre21_bear': 0.99,
    'pre21_range': 0.29,
    'pre21_recovery': 0.91,
    'pre21_covid': 0.17,
    'pre21_bull': 0.67,
}


# ── Check 1: All 30 jobs completed ─────────────────────────────────────────────
def check_all_jobs_done() -> Tuple[str, Dict]:
    """Verify all 30 combination result.json files exist."""
    missing = []
    found = 0
    expected = 30

    for strat_entry in STRATEGIES_MANIFEST:
        strat = strat_entry['strat']
        tf = strat_entry['tf']
        variant = strat_entry['variant']

        for period_key in PRE21_PERIODS.keys():
            result_path = BACKFILL / strat / tf / variant / period_key / 'result.json'
            if result_path.exists():
                found += 1
            else:
                missing.append(f'{strat}/{tf}/{variant}/{period_key}')

    status = 'PASS' if len(missing) == 0 else 'FAIL'
    return status, {
        'status': status,
        'found': found,
        'expected': expected,
        'missing': missing,
    }


# ── Check 2: adj_results_pre21 exists and has 5 champions ──────────────────────
def check_adj_results() -> Tuple[str, Dict]:
    """Verify all_adjusted_results_pre21.json exists with 5 entries."""
    if not ADJ_PRE21.exists():
        return 'FAIL', {
            'status': 'FAIL',
            'reason': f'{ADJ_PRE21} does not exist',
            'missing_champions': [],
        }

    try:
        data = json.loads(ADJ_PRE21.read_text())
        if not isinstance(data, list):
            return 'FAIL', {
                'status': 'FAIL',
                'reason': 'Data is not a list',
                'found': 0,
                'expected': 5,
            }

        found = len(data)
        expected = 5

        # Identify which champions are present
        present_keys = set()
        for entry in data:
            strat = entry.get('strat')
            tf = entry.get('tf')
            variant = entry.get('variant')
            if all([strat, tf, variant]):
                present_keys.add((strat, tf, variant))

        # Identify which are missing
        all_expected = {
            (e['strat'], e['tf'], e['variant'])
            for e in STRATEGIES_MANIFEST
        }
        missing_keys = all_expected - present_keys

        status = 'PASS' if len(missing_keys) == 0 else 'FAIL'
        return status, {
            'status': status,
            'found': found,
            'expected': expected,
            'present_champions': sorted([f'{s}/{t}/{v}' for s, t, v in present_keys]),
            'missing_champions': sorted([f'{s}/{t}/{v}' for s, t, v in missing_keys]),
        }
    except Exception as e:
        return 'FAIL', {
            'status': 'FAIL',
            'reason': f'Error reading JSON: {str(e)}',
        }


# ── Check 3: supertrend p0 overlap (sign consistency) ───────────────────────────
def check_p0_overlap() -> Tuple[str, Dict]:
    """
    Load supertrend/4h/long_only p0 from v4 sweep (2018-04 ~ 2020-06).
    Load pre21_recovery and pre21_covid from pre21_backfill.
    Verify signs are consistent: if p0 CAGR > 0, at least one recovery/bull should be > 0.
    """
    try:
        # Load v4 p0 summary
        p0_path = V4_SWEEP / 'supertrend' / '4h' / 'long_only' / 'combo_18' / 'summary.json'
        if not p0_path.exists():
            return 'SKIP', {
                'status': 'SKIP',
                'reason': f'{p0_path} not found',
            }

        p0_summary = json.loads(p0_path.read_text())
        p0_cagr = p0_summary.get('periods', {}).get('p0', {}).get('cagr')

        if p0_cagr is None:
            return 'SKIP', {
                'status': 'SKIP',
                'reason': 'p0 CAGR not found in summary',
            }

        # Load pre21 periods
        recovery_path = BACKFILL / 'supertrend' / '4h' / 'long_only' / 'pre21_recovery' / 'result.json'
        covid_path = BACKFILL / 'supertrend' / '4h' / 'long_only' / 'pre21_covid' / 'result.json'
        bull_path = BACKFILL / 'supertrend' / '4h' / 'long_only' / 'pre21_bull' / 'result.json'

        recovery_cagr = None
        covid_cagr = None
        bull_cagr = None

        if recovery_path.exists():
            recovery_data = json.loads(recovery_path.read_text())
            recovery_cagr = recovery_data.get('metrics', {}).get('cagr_pct')

        if covid_path.exists():
            covid_data = json.loads(covid_path.read_text())
            covid_cagr = covid_data.get('metrics', {}).get('cagr_pct')

        if bull_path.exists():
            bull_data = json.loads(bull_path.read_text())
            bull_cagr = bull_data.get('metrics', {}).get('cagr_pct')

        # Compute blended CAGR for 2019-04 ~ 2020-04 window
        blended = None
        if recovery_cagr is not None and covid_cagr is not None:
            # Weight by period length
            w_recovery = PRE21_PERIOD_YEARS['pre21_recovery']
            w_covid = PRE21_PERIOD_YEARS['pre21_covid']
            total_w = w_recovery + w_covid
            blended = (recovery_cagr * w_recovery + covid_cagr * w_covid) / total_w

        # Sign consistency check
        # If p0 CAGR > 0, at least one of recovery/bull should be > 0
        # If p0 CAGR < 0, it's plausible if early bear (2018) was terrible
        is_consistent = True
        reason = ''

        if p0_cagr > 0:
            has_positive = False
            if recovery_cagr is not None and recovery_cagr > 0:
                has_positive = True
            if bull_cagr is not None and bull_cagr > 0:
                has_positive = True
            if not has_positive and recovery_cagr is not None and covid_cagr is not None:
                is_consistent = False
                reason = 'p0 CAGR > 0 but recovery & covid both negative'

        status = 'PASS' if is_consistent else 'FAIL'
        return status, {
            'status': status,
            'p0_cagr_pct': round(p0_cagr, 2) if p0_cagr else None,
            'pre21_recovery_cagr_pct': round(recovery_cagr, 2) if recovery_cagr else None,
            'pre21_covid_cagr_pct': round(covid_cagr, 2) if covid_cagr else None,
            'pre21_bull_cagr_pct': round(bull_cagr, 2) if bull_cagr else None,
            'blended_2019_2020_cagr_pct': round(blended, 2) if blended else None,
            'consistent': is_consistent,
            'reason': reason,
        }
    except Exception as e:
        return 'FAIL', {
            'status': 'FAIL',
            'reason': f'Error during check: {str(e)}',
        }


# ── Check 4: pre21_bear should have fee_only funding ──────────────────────────
def check_bear_fee_only() -> Tuple[str, Dict]:
    """
    pre21_bear (2017-12-17 ~ 2018-12-15): no Bybit or Binance funding data available.
    All 5 champions should have funding_coverage == 'fee_only'.
    """
    if not ADJ_PRE21.exists():
        return 'SKIP', {
            'status': 'SKIP',
            'reason': f'{ADJ_PRE21} does not exist',
        }

    try:
        data = json.loads(ADJ_PRE21.read_text())
        all_pass = True
        details = {}

        for entry in data:
            strat = entry.get('strat')
            tf = entry.get('tf')
            variant = entry.get('variant')
            key = f'{strat}/{tf}/{variant}'

            period_data = entry.get('periods', {}).get('pre21_bear')
            if not period_data:
                details[key] = 'pre21_bear not found'
                all_pass = False
                continue

            coverage = period_data.get('funding_coverage', 'unknown')
            is_ok = coverage == 'fee_only'
            details[key] = {
                'funding_coverage': coverage,
                'ok': is_ok,
            }
            if not is_ok:
                all_pass = False

        status = 'PASS' if all_pass else 'FAIL'
        return status, {
            'status': status,
            'details': details,
        }
    except Exception as e:
        return 'FAIL', {
            'status': 'FAIL',
            'reason': f'Error reading JSON: {str(e)}',
        }


# ── Check 5: pre21_bull should have bybit_live funding ───────────────────────
def check_bull_bybit() -> Tuple[str, Dict]:
    """
    pre21_bull (2020-05-01 ~ 2020-12-31): Bybit funding started 2020-03-25.
    All 5 champions should have funding_coverage == 'bybit_live'.
    """
    if not ADJ_PRE21.exists():
        return 'SKIP', {
            'status': 'SKIP',
            'reason': f'{ADJ_PRE21} does not exist',
        }

    try:
        data = json.loads(ADJ_PRE21.read_text())
        all_pass = True
        details = {}

        for entry in data:
            strat = entry.get('strat')
            tf = entry.get('tf')
            variant = entry.get('variant')
            key = f'{strat}/{tf}/{variant}'

            period_data = entry.get('periods', {}).get('pre21_bull')
            if not period_data:
                details[key] = 'pre21_bull not found'
                all_pass = False
                continue

            coverage = period_data.get('funding_coverage', 'unknown')
            is_ok = coverage == 'bybit_live'
            details[key] = {
                'funding_coverage': coverage,
                'ok': is_ok,
            }
            if not is_ok:
                all_pass = False

        status = 'PASS' if all_pass else 'FAIL'
        return status, {
            'status': status,
            'details': details,
        }
    except Exception as e:
        return 'FAIL', {
            'status': 'FAIL',
            'reason': f'Error reading JSON: {str(e)}',
        }


# ── Check 6: Trade count sanity ────────────────────────────────────────────────
def check_trade_sanity() -> Tuple[str, Dict]:
    """
    For each pre21 period result, verify:
      - total_trades >= 0 (no negative)
      - total_trades < 10000 (no overflow)
      - annual_return_pct between -99.9 and 500
    """
    issues = []

    for strat_entry in STRATEGIES_MANIFEST:
        strat = strat_entry['strat']
        tf = strat_entry['tf']
        variant = strat_entry['variant']

        for period_key in PRE21_PERIODS.keys():
            result_path = BACKFILL / strat / tf / variant / period_key / 'result.json'
            if not result_path.exists():
                continue

            try:
                result_data = json.loads(result_path.read_text())
                metrics = result_data.get('metrics', {})

                trades = metrics.get('total_trades')
                annual_return = metrics.get('annual_return_pct')

                label = f'{strat}/{tf}/{variant}/{period_key}'

                if trades is not None:
                    if trades < 0:
                        issues.append(f'{label}: negative trades ({trades})')
                    elif trades >= 10000:
                        issues.append(f'{label}: trades overflow ({trades})')

                if annual_return is not None:
                    if annual_return < -99.9:
                        issues.append(f'{label}: annual_return below -99.9% ({annual_return:.2f}%)')
                    elif annual_return > 500:
                        issues.append(f'{label}: annual_return above 500% ({annual_return:.2f}%)')

            except Exception as e:
                issues.append(f'{label}: error reading result.json: {str(e)}')

    status = 'PASS' if len(issues) == 0 else 'FAIL'
    return status, {
        'status': status,
        'issues': issues,
        'checked_combinations': len(STRATEGIES_MANIFEST) * len(PRE21_PERIODS),
    }


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> int:
    """Run all checks and write output."""
    print('=' * 80)
    print('c3_pre21_sanity — Pre-2021 Backfill Validation')
    print('=' * 80)
    print()

    # Run all checks
    checks = {}

    print('[C1] All 30 jobs completed...')
    status, result = check_all_jobs_done()
    checks['c1_all_jobs_done'] = result
    print(f'  {status}: {result["found"]}/{result["expected"]} found')
    if result['missing']:
        print(f'  Missing: {result["missing"][:3]}{"..." if len(result["missing"]) > 3 else ""}')
    print()

    print('[C2] adj_results_pre21 exists and has 5 champions...')
    status, result = check_adj_results()
    checks['c2_adj_results'] = result
    print(f'  {status}: {result.get("found", "?")} champions found')
    if result.get('missing_champions'):
        print(f'  Missing: {result["missing_champions"]}')
    print()

    print('[C3] supertrend p0 overlap (sign consistency)...')
    status, result = check_p0_overlap()
    checks['c3_p0_overlap'] = result
    print(f'  {status}: p0_cagr={result.get("p0_cagr_pct")}%, ' +
          f'recovery={result.get("pre21_recovery_cagr_pct")}%, ' +
          f'covid={result.get("pre21_covid_cagr_pct")}%')
    if result.get('reason'):
        print(f'  Note: {result["reason"]}')
    print()

    print('[C4] pre21_bear has fee_only funding...')
    status, result = check_bear_fee_only()
    checks['c4_bear_fee_only'] = result
    passed = sum(1 for d in result.get('details', {}).values()
                 if isinstance(d, dict) and d.get('ok'))
    total = len(result.get('details', {}))
    print(f'  {status}: {passed}/{total} champions have fee_only')
    print()

    print('[C5] pre21_bull has bybit_live funding...')
    status, result = check_bull_bybit()
    checks['c5_bull_bybit'] = result
    passed = sum(1 for d in result.get('details', {}).values()
                 if isinstance(d, dict) and d.get('ok'))
    total = len(result.get('details', {}))
    print(f'  {status}: {passed}/{total} champions have bybit_live')
    print()

    print('[C6] Trade count sanity...')
    status, result = check_trade_sanity()
    checks['c6_trade_sanity'] = result
    print(f'  {status}: {len(result["issues"])} issues found')
    if result['issues']:
        for issue in result['issues'][:3]:
            print(f'    - {issue}')
        if len(result['issues']) > 3:
            print(f'    ... and {len(result["issues"]) - 3} more')
    print()

    # Determine overall status
    critical_pass = (
        checks['c1_all_jobs_done'].get('status') == 'PASS' and
        checks['c2_adj_results'].get('status') == 'PASS'
    )
    overall = 'PASS' if critical_pass else 'FAIL'

    # Write output
    output = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'overall': overall,
        'checks': checks,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(output, indent=2, default=str))

    print('=' * 80)
    print(f'Overall: {overall}')
    print(f'Output: {OUT}')
    print('=' * 80)

    return 0 if critical_pass else 1


if __name__ == '__main__':
    sys.exit(main())
