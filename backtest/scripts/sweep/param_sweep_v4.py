#!/usr/bin/env python3
"""
param_sweep_v4.py — Focused parameter sweep for supertrend/4h/long_only.

Key improvements from v3:
  - Single strategy only: supertrend (long_only variant)
  - Finer HP grid for st_factor (1.3-4.5) and st_period (3-16)
  - EMA parameter exploration (fast_ema_len, slow_ema_len, atr_mult)
  - 5-period evaluation (includes P0: 2018-04~2020-06)
  - Realistic cost scoring: taker 0.055% + funding estimate per trade
  - Total combos: 248 (168 Phase 1 + 80 Phase 2)

Periods (all 5):
  P0: 2018-04-01 ~ 2020-06-30  (bear-bull transition, ~2.25 yrs)
  P1: 2021-04-01 ~ 2026-04-30  (high entry, ~5 yrs)
  P2: 2022-12-01 ~ 2026-04-30  (low entry, ~3.5 yrs)
  P3: 2021-04-01 ~ 2025-09-30  (high → next high)
  P4: 2022-12-01 ~ 2025-09-30  (low → high, bull run)

Scoring:
  score = mean(adj_cagr_p0, adj_cagr_p1, ...) if ALL periods: adj_mdd >= -35% AND trades >= 5
  else -999
  where adj_cagr = cagr - fee_cost - funding_cost

Usage (inside Jesse/backtester container):
    python /app/scripts/sweep/param_sweep_v4.py --workers 6
    python /app/scripts/sweep/param_sweep_v4.py --dry-run
    python /app/scripts/sweep/param_sweep_v4.py --combos 0-100  # partial run

Idempotent: summary.json exists → SKIP.
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# ── Path setup ─────────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
JESSE_ROOT  = SCRIPTS_DIR.parent
sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(JESSE_ROOT / 'strategies'))

from run_external_backtest import (
    _load_1h, _resample_1h, _expand_tf_to_1m, _upsample_to_1m,
    _extract_metrics, _pass_fail,
    EXCHANGE_NAME, SYMBOL, TF_MINUTES,
)

# ── Constants ──────────────────────────────────────────────────────────────────
BALANCE       = 10_000.0
FEE           = 0.0002
LEVERAGE      = 1
RESULT_DIR    = Path('/result/param_sweep_v4')
STRATEGY      = 'supertrend'
TIMEFRAME     = '4h'
VARIANT       = 'long_only'

FULL_START    = '2017-09-01'  # needs 60d warmup before P0 (2018-04-01)
FULL_END      = '2026-04-30'

PERIODS = {
    'p0': ('2018-04-01', '2020-06-30'),
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

# ── HP Grid for v4 (248 combos total) ──────────────────────────────────────────

def _build_v4_combos() -> list[dict]:
    """Build 248 combos: Phase 1 (st_factor×st_period fine grid) + Phase 2 (EMA exploration)"""
    combos = []

    # Phase 1: st_factor x st_period fine grid (EMA params fixed at v3 best)
    # 14 st_factor values × 12 st_period values = 168 combos
    for sf in [1.3, 1.5, 1.8, 2.0, 2.2, 2.3, 2.5, 2.7, 2.8, 3.0, 3.2, 3.5, 4.0, 4.5]:
        for sp in [3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16]:
            combos.append({
                'st_factor': sf,
                'st_period': sp,
                'fast_ema_len': 7,
                'slow_ema_len': 20,
                'direction_ema_len': 200,
                'atr_mult': 3.0,
            })

    # Phase 2: EMA exploration (fix st_factor=2.5, st_period=9 as baseline winner)
    # 4 fast_ema × 4 slow_ema × 5 atr_mult = 80 combos
    for fl in [5, 7, 9, 11]:
        for sl in [15, 20, 25, 30]:
            for am in [2.0, 2.5, 3.0, 3.5, 4.0]:
                combos.append({
                    'st_factor': 2.5,
                    'st_period': 9,
                    'fast_ema_len': fl,
                    'slow_ema_len': sl,
                    'direction_ema_len': 200,
                    'atr_mult': am,
                })

    return combos

COMBOS_V4 = _build_v4_combos()
print(f'[init] Built {len(COMBOS_V4)} combos (P1: 168 + P2: 80)', flush=True)

# ── Candle cache (per-process) ─────────────────────────────────────────────────
_FULL_1H_CACHE: dict = {}


def _get_full_1h() -> np.ndarray:
    if 'data' not in _FULL_1H_CACHE:
        print(f'  [cache] Loading full 1h candles {FULL_START} → {FULL_END}...', flush=True)
        _FULL_1H_CACHE['data'] = _load_1h(FULL_START, FULL_END)
        print(f'  [cache] {len(_FULL_1H_CACHE["data"]):,} 1h candles loaded', flush=True)
    return _FULL_1H_CACHE['data']


def _dt_ms(date_str: str) -> int:
    return int(datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _period_years(period_key: str) -> float:
    """Compute fractional years in a period (for trades/year annualization)"""
    if period_key not in PERIODS:
        return 1.0
    start_str, end_str = PERIODS[period_key]
    start = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
    end   = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
    delta = (end - start).days
    return delta / 365.25


def _build_period_candles(full_1h: np.ndarray, tf: str, p_start: str, p_end: str):
    tf_hours    = TF_MINUTES[tf] // 60
    warmup_days = max(60, tf_hours * 220 // 24 + 1)

    start_ms = _dt_ms(p_start)
    end_ms   = _dt_ms(p_end)
    wu_ms    = int((datetime.fromisoformat(p_start).replace(tzinfo=timezone.utc)
                    - timedelta(days=warmup_days)).timestamp() * 1000)

    period_1h = full_1h[(full_1h[:, 0] >= start_ms) & (full_1h[:, 0] < end_ms)]
    warmup_1h = full_1h[(full_1h[:, 0] >= wu_ms)    & (full_1h[:, 0] < start_ms)]

    if tf == '1h':
        candles  = _upsample_to_1m(period_1h)
        warmup   = _upsample_to_1m(warmup_1h)
        route_tf = '1h'
    else:
        tf_min   = TF_MINUTES[tf]
        candles  = _expand_tf_to_1m(_resample_1h(period_1h, tf), tf_min)
        warmup   = _expand_tf_to_1m(_resample_1h(warmup_1h, tf), tf_min)
        route_tf = tf

    return candles, warmup, route_tf


# ── Strategy / backtest helpers ────────────────────────────────────────────────

def _load_strategy_cls(variant: str):
    cls_name = 'SupertrendStrategy'
    ext_dir  = JESSE_ROOT / 'strategies' / 'external'
    mod_path = ext_dir / f'{cls_name}.py'
    spec_mod = importlib.util.spec_from_file_location(cls_name, mod_path)
    mod      = importlib.util.module_from_spec(spec_mod)
    spec_mod.loader.exec_module(mod)
    strategy_cls = getattr(mod, cls_name)
    if variant == 'long_only':
        from external._long_only_factory import make_long_only
        strategy_cls = make_long_only(strategy_cls)
    return strategy_cls


def _jesse_run(strategy_cls, route_tf: str, hp: dict, candles, warmup):
    from jesse import research
    import jesse.helpers as jh
    import os
    os.environ['STRATEGY_LEVERAGE'] = str(LEVERAGE)

    key = jh.key(EXCHANGE_NAME, SYMBOL)
    config = {
        'starting_balance':      BALANCE,
        'fee':                   FEE,
        'type':                  'futures',
        'futures_leverage':      LEVERAGE,
        'futures_leverage_mode': 'isolated',
        'exchange':              EXCHANGE_NAME,
        'warm_up_candles':       len(warmup),
    }
    routes       = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
                     'symbol': SYMBOL, 'timeframe': route_tf}]
    candles_dict = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': candles}}
    warmup_dict  = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warmup}}

    return research.backtest(
        config=config, routes=routes, data_routes=[],
        candles=candles_dict, warmup_candles=warmup_dict,
        hyperparameters=hp,
    )


def _realistic_score(period_results: dict, tf: str) -> tuple[float, dict]:
    """
    Score = mean(adj_cagr) if ALL periods: adj_mdd >= -35% AND trades >= 5 else -999
    Returns: (score, adj_results_dict)

    Realistic cost adjustment:
      - FEE_DELTA_RT = (0.055 - 0.020) / 100 * 2 = 0.0007 (taker - maker, 2x round trip)
      - FUNDING_PER_TRADE = 0.000048 * 4.0 = 0.000192 per trade (0.015%/8h × 4 periods/trade)
      - MDD adjustment: intrabar correction ~8% worse on average
    """
    FEE_DELTA_RT = 0.000700
    FUNDING_PER_TRADE = {'4h': 0.000048 * 4.0}

    adj_cagrs = []
    adj_results = {}

    for pk, res in period_results.items():
        trades = res['total_trades']
        cagr = res['annual_return_pct']
        mdd = res['max_drawdown_pct']

        # Annualize costs: estimate trades/year from period length
        years = _period_years(pk)
        trades_per_year = (trades / years) if years > 0 else 0

        # Fee and funding cost as % CAGR
        fee_cost = trades_per_year * FEE_DELTA_RT * 100
        funding_cost = trades_per_year * FUNDING_PER_TRADE.get(tf, 0) * 100

        # Adjusted values
        adj_cagr = cagr - fee_cost - funding_cost
        adj_mdd = mdd * 1.08  # intrabar correction: ~8% worse on average

        # Store for output
        adj_results[pk] = {
            'cagr': round(cagr, 4),
            'adj_cagr': round(adj_cagr, 4),
            'mdd': round(mdd, 4),
            'adj_mdd': round(adj_mdd, 4),
            'trades': trades,
            'fee_cost': round(fee_cost, 4),
            'funding_cost': round(funding_cost, 4),
        }

        # Fail if MDD or trades don't meet threshold
        if adj_mdd < -35.0 or trades < 5:
            return -999.0, adj_results

        adj_cagrs.append(adj_cagr)

    score = statistics.mean(adj_cagrs) if adj_cagrs else -999.0
    return score, adj_results


# ── Job output / idempotency ───────────────────────────────────────────────────

def _job_output_dir(combo_idx: int) -> Path:
    return RESULT_DIR / STRATEGY / TIMEFRAME / VARIANT / f'combo_{combo_idx}'


def _already_done(combo_idx: int) -> bool:
    return (_job_output_dir(combo_idx) / 'summary.json').exists()


def _make_label(combo_idx: int, hp: dict) -> str:
    parts = [
        f'st_f={hp["st_factor"]:.1f}',
        f'st_p={hp["st_period"]}',
        f'f_ema={hp["fast_ema_len"]}',
        f's_ema={hp["slow_ema_len"]}',
        f'atr={hp["atr_mult"]:.1f}',
    ]
    return f'[{TIMEFRAME}] {STRATEGY}/{VARIANT}/combo_{combo_idx} ({",".join(parts)})'


# ── Job runner ─────────────────────────────────────────────────────────────────

def run_job(job: dict) -> dict:
    combo_idx = job['combo_idx']
    hp = job['hp']
    label = job['label']
    out_dir = _job_output_dir(combo_idx)

    if _already_done(combo_idx):
        return {'label': label, 'status': 'SKIP', 'elapsed': 0, 'summary': None}

    t0 = time.monotonic()
    try:
        strategy_cls = _load_strategy_cls(VARIANT)
        full_1h = _get_full_1h()

        period_results: dict = {}
        adj_results: dict = {}

        for pk, (p_start, p_end) in PERIODS.items():
            candles, warmup, route_tf = _build_period_candles(full_1h, TIMEFRAME, p_start, p_end)
            raw = _jesse_run(strategy_cls, route_tf, hp, candles, warmup)
            metrics = _extract_metrics(raw, p_start, p_end, no_upsample=True, timeframe=TIMEFRAME)
            period_results[pk] = metrics

        # Calculate realistic score and adjusted results
        score, adj_results = _realistic_score(period_results, TIMEFRAME)

        # Build summary with both raw and adjusted metrics
        summary = {
            'strategy': STRATEGY,
            'tf': TIMEFRAME,
            'variant': VARIANT,
            'combo_idx': combo_idx,
            'hp': hp,
            'score': round(score, 4),
            'adj_score': round(score, 4),  # Already adjusted in _realistic_score
            'sweep_version': 'v4',
            'periods': adj_results,
        }

        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, default=str))

        elapsed = time.monotonic() - t0
        return {'label': label, 'status': 'OK', 'elapsed': elapsed, 'summary': summary}

    except Exception as e:
        import traceback
        elapsed = time.monotonic() - t0
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'SWEEP_FAILED.marker').write_text(
            f"status: FAILED\nreason: {e}\ntb: {traceback.format_exc()}\n"
            f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
        )
        return {'label': label, 'status': 'FAIL', 'elapsed': elapsed, 'summary': None}


# ── Job list ───────────────────────────────────────────────────────────────────

def _build_jobs(combos_filter: list | None = None) -> list[dict]:
    jobs = []
    combo_indices = combos_filter if combos_filter is not None else list(range(len(COMBOS_V4)))

    for ci in combo_indices:
        if ci >= len(COMBOS_V4):
            continue
        hp = COMBOS_V4[ci]
        label = _make_label(ci, hp)
        jobs.append({'combo_idx': ci, 'hp': hp, 'label': label})

    return jobs


# ── Report generator ───────────────────────────────────────────────────────────

def generate_report() -> None:
    """Generate summary report of all completed combos"""
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    rows = []
    for ci in range(len(COMBOS_V4)):
        p = _job_output_dir(ci) / 'summary.json'
        if p.exists():
            try:
                rows.append(json.loads(p.read_text()))
            except Exception:
                pass

    if not rows:
        print('  [warn] No summary.json found.')
        return

    # Sort by score (descending)
    rows = sorted(rows, key=lambda r: r.get('adj_score', -999), reverse=True)

    lines = [
        '# param_sweep_v4 결과 리포트 (자동 생성)',
        '',
        f'**생성**: {ts}',
        f'**전략**: {STRATEGY}/{TIMEFRAME}/{VARIANT}',
        '**평가 기간**: P0=2018-04~2020-06 / P1=2021-04~2026-04 / P2=2022-12~2026-04 / P3=2021-04~2025-09 / P4=2022-12~2025-09',
        '**점수식**: mean(P0~P4 adj_cagr)  if ALL adj_mdd≥-35% AND trades≥5  else -999',
        '**조정**: 실제 거래 비용 포함 (taker 0.055% + funding 0.000192/trade)',
        '',
        '---',
        '',
        '## 상위 50 파라미터 조합',
        '',
        '| # | st_factor | st_period | f_ema | s_ema | atr | score | P0 | P1 | P2 | P3 | P4 |',
        '|----|-----------|-----------|-------|-------|-----|-------|----|----|----|----|----|',
    ]

    for i, r in enumerate(rows[:50], 1):
        hp = r['hp']
        periods = r['periods']
        line = (
            f"| {i} | {hp['st_factor']:.1f} | {hp['st_period']} | "
            f"{hp['fast_ema_len']} | {hp['slow_ema_len']} | {hp['atr_mult']:.1f} | "
            f"{r['adj_score']:+.2f} | "
            f"{periods['p0']['adj_cagr']:+.1f}% | "
            f"{periods['p1']['adj_cagr']:+.1f}% | "
            f"{periods['p2']['adj_cagr']:+.1f}% | "
            f"{periods['p3']['adj_cagr']:+.1f}% | "
            f"{periods['p4']['adj_cagr']:+.1f}% |"
        )
        lines.append(line)

    lines += [
        '',
        '---',
        '',
        '## 요약 통계',
        '',
    ]

    if rows:
        scores = [r.get('adj_score', -999) for r in rows if r.get('adj_score', -999) > -999]
        if scores:
            lines.append(f'- 성공 조합: {len(scores)} / {len(rows)}')
            lines.append(f'- 평균 점수: {statistics.mean(scores):+.2f}%')
            lines.append(f'- 최고 점수: {max(scores):+.2f}%')
            lines.append(f'- 최저 점수: {min(scores):+.2f}%')

    out = RESULT_DIR / 'param_sweep_v4_report.md'
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(lines) + '\n')
    print(f'\nReport written: {out}')


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_combos_arg(arg_str: str) -> list[int]:
    """Parse --combos argument: '0-50' or '0,5,10' or space-separated"""
    indices = []
    parts = arg_str.replace(',', ' ').split()
    for part in parts:
        if '-' in part:
            start, end = part.split('-')
            indices.extend(range(int(start), int(end) + 1))
        else:
            indices.append(int(part))
    return sorted(set(indices))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f'Parameter sweep v4: {STRATEGY}/{TIMEFRAME}/{VARIANT} fine grid (248 combos)'
    )
    parser.add_argument('--combos', type=str, default=None,
                        help='Combo indices: "0-50" or "0,5,10,15" (default: all 0-247)')
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    combos_filter = None
    if args.combos:
        combos_filter = parse_combos_arg(args.combos)

    jobs = _build_jobs(combos_filter)
    total = len(jobs)
    done_cnt = sum(1 for j in jobs if _already_done(j['combo_idx']))

    print(f'param_sweep_v4: {total} jobs ({done_cnt} done, {total-done_cnt} pending)  '
          f'workers={args.workers}')
    print(f'Strategy   : {STRATEGY}/{TIMEFRAME}/{VARIANT}')
    print(f'Total combos: {len(COMBOS_V4)} (Phase 1: 168 + Phase 2: 80)')
    print(f'Combos filter: {args.combos or "all 0-247"}')
    print(f'Output     : {RESULT_DIR}')
    print(f'Start      : {datetime.now(timezone.utc).isoformat()}')
    print()

    if args.dry_run:
        for j in jobs:
            done = 'SKIP' if _already_done(j['combo_idx']) else 'PEND'
            print(f'  {done} {j["label"]}')
        print(f'\nTotal: {total}  Done: {done_cnt}  Pending: {total - done_cnt}')
        return

    t_global = time.monotonic()
    results_list: list[dict] = []

    pending = [j for j in jobs if not _already_done(j['combo_idx'])]
    for j in jobs:
        if _already_done(j['combo_idx']):
            results_list.append({'label': j['label'], 'status': 'SKIP', 'elapsed': 0})

    if args.workers <= 1:
        for i, job in enumerate(pending, 1):
            print(f'  [{i}/{len(pending)}] START {job["label"]}', flush=True)
            r = run_job(job)
            elapsed_total = time.monotonic() - t_global
            print(f'  [{i}/{len(pending)}] {r["status"]:6s} {job["label"]}  '
                  f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)
            if r.get('summary'):
                s = r['summary']
                p = s['periods']
                print(f'    score={s["adj_score"]:+.2f}%  '
                      f'P0={p["p0"]["adj_cagr"]:+.1f}%  '
                      f'P1={p["p1"]["adj_cagr"]:+.1f}%  '
                      f'P2={p["p2"]["adj_cagr"]:+.1f}%  '
                      f'P3={p["p3"]["adj_cagr"]:+.1f}%  '
                      f'P4={p["p4"]["adj_cagr"]:+.1f}%', flush=True)
            results_list.append(r)
    else:
        n_done = len(results_list)
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_job, j): j for j in pending}
            for fut in as_completed(futures):
                r = fut.result()
                n_done += 1
                elapsed_total = time.monotonic() - t_global
                print(f'  [{n_done}/{total}] {r["status"]:6s} {r["label"]}  '
                      f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)
                results_list.append(r)

    counts: dict[str, int] = {}
    for r in results_list:
        counts[r['status']] = counts.get(r['status'], 0) + 1

    total_elapsed = time.monotonic() - t_global
    print(f'\n--- Sweep summary ---')
    for k, v in sorted(counts.items()):
        if v:
            print(f'  {k}: {v}')
    print(f'  Total wall time: {total_elapsed/60:.1f}m')

    failures = [r for r in results_list if r['status'] == 'FAIL']
    if failures:
        print('\nFailed jobs:')
        for r in failures:
            print(f'  FAIL {r["label"]}')

    print('\nGenerating report...', flush=True)
    generate_report()
    print(f'End: {datetime.now(timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
