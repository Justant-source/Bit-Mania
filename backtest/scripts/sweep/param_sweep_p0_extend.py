#!/usr/bin/env python3
"""
param_sweep_p0_extend.py — P0 (2018-04-01 ~ 2020-06-30) 구간 추가 + 5-period 재점수화

기존 v2/v3 파라미터 sweep 결과에 횡보/약세장 구간(P0)을 추가해
score = mean(P0~P4 CAGR), 5구간 모두 MDD≥-35% AND trades≥5 조건으로
최적해를 재산정한다.

동작:
  RUN_P0_ONLY  — 기존 active combo (score>-999): P0만 추가 백테스트 → summary 재작성
  RUN_FULL     — v3 미실행 combo: P0~P4 전체 신규 실행 → summary 신규 생성
  SKIP_DISQ    — -999 combo: P0 추가해도 -999 불변 → 스킵
  SKIP_DONE    — P0 이미 완료 → idempotent 스킵

Usage (backtester container 내부):
    python /app/scripts/sweep/param_sweep_p0_extend.py --dry-run
    python /app/scripts/sweep/param_sweep_p0_extend.py --workers 6
    python /app/scripts/sweep/param_sweep_p0_extend.py --strategies supertrend --workers 4

Options:
    --strategies STRAT [...]   특정 전략만 (기본: 전체 7개)
    --versions   v2 v3         버전 선택 (기본: v2 v3 모두)
    --workers    N             병렬 워커 수 (기본: 6)
    --dry-run                  실행 없이 분류 결과만 출력
"""
from __future__ import annotations

import argparse
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
SCRIPTS_DIR   = Path(__file__).parent                      # /app/scripts/sweep/
RUNNERS_DIR   = SCRIPTS_DIR.parent / 'runners'            # /app/scripts/runners/
APP_DIR       = SCRIPTS_DIR.parent.parent                  # /app/
EXT_STRAT_DIR = APP_DIR / 'strategies' / 'external'       # /app/strategies/external/

# Insert runners/ first so param_sweep_v2/v3 (which import run_external_backtest) also work
sys.path.insert(0, str(APP_DIR / 'strategies'))            # 'external' subpackage
sys.path.insert(0, str(APP_DIR))                           # jesse package root
sys.path.insert(0, str(SCRIPTS_DIR))                       # param_sweep_v2/v3 modules
sys.path.insert(0, str(RUNNERS_DIR))                       # run_external_backtest module

from run_external_backtest import (
    _load_1h, _resample_1h, _expand_tf_to_1m, _upsample_to_1m,
    _extract_metrics,
    EXCHANGE_NAME, SYMBOL, TF_MINUTES,
)

# Import sweep specs only (static data; module-level side effects are harmless)
import param_sweep_v2 as _v2mod
import param_sweep_v3 as _v3mod
V2_SPECS  = _v2mod.SWEEP_SPECS       # 16 combos/strat, TFs: 1h+4h+1D
V3_SPECS  = _v3mod.SWEEP_SPECS_V3    # 24 combos/strat, TFs: 4h+1D
V2_N      = _v2mod.N_COMBOS          # 16
V3_N      = _v3mod.N_COMBOS          # 24

# ── Constants ──────────────────────────────────────────────────────────────────
BALANCE    = 10_000.0
FEE        = 0.0002
LEVERAGE   = 1

# Extended from v2/v3's '2020-05-01' to cover P0 warmup (1D needs ~220 days prior)
FULL_START = '2017-08-18'
FULL_END   = '2026-04-30'

V2_DIR = Path('/result/param_sweep_v2')
V3_DIR = Path('/result/param_sweep_v3')

PERIODS_FULL: dict[str, tuple[str, str]] = {
    'p0': ('2018-04-01', '2020-06-30'),   # NEW: sideways/bear (2018 crypto winter + recovery)
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

STRATEGIES_ALL = [
    'stoch', 'momentum_ma', 'supertrend', 'tradeiq_psar_ha',
    'trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce',
]
VARIANTS        = ['bidirectional', 'long_only']
V2_TIMEFRAMES   = ['1h', '4h', '1D']
V3_TIMEFRAMES   = ['4h', '1D']

# ── Classification labels ──────────────────────────────────────────────────────
ACT_P0    = 'RUN_P0_ONLY'   # active combo, P0 missing
ACT_FULL  = 'RUN_FULL'      # no summary.json (v3 missing combo)
ACT_DONE  = 'SKIP_DONE'     # P0 already exists
ACT_DISQ  = 'SKIP_DISQ'     # -999 disqualified

# ── Candle cache (per-process) ─────────────────────────────────────────────────
_FULL_1H_CACHE: dict = {}


def _get_full_1h() -> np.ndarray:
    if 'data' not in _FULL_1H_CACHE:
        print(f'  [cache] Loading 1h candles {FULL_START} → {FULL_END}...', flush=True)
        _FULL_1H_CACHE['data'] = _load_1h(FULL_START, FULL_END)
        print(f'  [cache] {len(_FULL_1H_CACHE["data"]):,} 1h candles loaded', flush=True)
    return _FULL_1H_CACHE['data']


def _dt_ms(date_str: str) -> int:
    return int(datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc).timestamp() * 1000)


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


def _build_hp(spec: dict, combo_idx: int, strat: str) -> dict:
    hp    = dict(spec['base_hp'])
    combo = spec['combos'][combo_idx]
    for key, val in zip(spec['param_keys'], combo):
        hp[key] = val
    if strat == 'trendtype' and 'di_len' in hp:
        hp['adx_len'] = hp['di_len']
    return hp


def _load_strategy_cls(cls_name: str, variant: str):
    mod_path = EXT_STRAT_DIR / f'{cls_name}.py'
    spec_    = importlib.util.spec_from_file_location(cls_name, mod_path)
    mod      = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(mod)
    strategy_cls = getattr(mod, cls_name)
    if variant == 'long_only':
        from external._long_only_factory import make_long_only
        strategy_cls = make_long_only(strategy_cls)
    return strategy_cls


def _jesse_run(strategy_cls, route_tf: str, hp: dict,
               candles: np.ndarray, warmup: np.ndarray):
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


def _mini(metrics: dict) -> dict:
    """Extract compact period stats from _extract_metrics output."""
    return {
        'cagr':     round(metrics['annual_return_pct'], 4),
        'sharpe':   round(metrics['sharpe_ratio'], 4),
        'mdd':      round(metrics['max_drawdown_pct'], 4),
        'trades':   metrics['total_trades'],
        'win_rate': round(metrics['win_rate_pct'], 4),
        'pf':       round(metrics['profit_factor'], 4),
    }


def _score_5p(periods: dict) -> float:
    """5-period score: -999 if any fails MDD≥-35% or trades≥5, else mean CAGR."""
    for m in periods.values():
        if m['mdd'] < -35.0 or m['trades'] < 5:
            return -999.0
    return statistics.mean(m['cagr'] for m in periods.values())


# ── Directory helpers ──────────────────────────────────────────────────────────

def _combo_dir(version: str, strat: str, tf: str, variant: str, combo_idx: int) -> Path:
    base = V2_DIR if version == 'v2' else V3_DIR
    return base / strat / tf / variant / f'combo_{combo_idx + 1}'


# ── Classification ─────────────────────────────────────────────────────────────

def classify(version: str, strat: str, tf: str, variant: str, combo_idx: int) -> str:
    d         = _combo_dir(version, strat, tf, variant, combo_idx)
    summary_p = d / 'summary.json'
    if not summary_p.exists():
        return ACT_FULL
    try:
        s = json.loads(summary_p.read_text())
    except Exception:
        return ACT_FULL
    if s.get('score', -999) <= -998:
        return ACT_DISQ
    if (d / 'p0' / 'mini_stats.json').exists():
        return ACT_DONE
    return ACT_P0


# ── Job builder ────────────────────────────────────────────────────────────────

def _build_jobs(strats: list[str], versions: list[str]) -> list[dict]:
    jobs = []
    for ver in versions:
        specs    = V2_SPECS if ver == 'v2' else V3_SPECS
        tfs      = V2_TIMEFRAMES if ver == 'v2' else V3_TIMEFRAMES
        n_combos = V2_N if ver == 'v2' else V3_N
        for strat in strats:
            if strat not in specs:
                continue
            spec = specs[strat]
            for tf in tfs:
                for variant in VARIANTS:
                    for ci in range(n_combos):
                        if ci >= len(spec['combos']):
                            continue
                        action = classify(ver, strat, tf, variant, ci)
                        label  = (f'[{ver}/{tf}] {strat}/{variant}/combo_{ci + 1}')
                        jobs.append({
                            'version':   ver,
                            'strat':     strat,
                            'tf':        tf,
                            'variant':   variant,
                            'combo_idx': ci,
                            'action':    action,
                            'label':     label,
                        })
    return jobs


# ── Worker ─────────────────────────────────────────────────────────────────────

def run_job(job: dict) -> dict:
    version   = job['version']
    strat     = job['strat']
    tf        = job['tf']
    variant   = job['variant']
    combo_idx = job['combo_idx']
    action    = job['action']
    label     = job['label']

    out_dir = _combo_dir(version, strat, tf, variant, combo_idx)
    t0 = time.monotonic()

    try:
        specs        = V2_SPECS if version == 'v2' else V3_SPECS
        spec         = specs[strat]
        hp           = _build_hp(spec, combo_idx, strat)
        strategy_cls = _load_strategy_cls(spec['cls'], variant)
        full_1h      = _get_full_1h()
        no_up        = tf != '1h'

        if action == ACT_P0:
            # ─── Run only P0, then rebuild summary ───────────────────────────
            p_start, p_end = PERIODS_FULL['p0']
            candles, warmup, route_tf = _build_period_candles(full_1h, tf, p_start, p_end)
            raw     = _jesse_run(strategy_cls, route_tf, hp, candles, warmup)
            metrics = _extract_metrics(raw, p_start, p_end,
                                       no_upsample=no_up, timeframe=tf)

            p0_dir = out_dir / 'p0'
            p0_dir.mkdir(parents=True, exist_ok=True)
            p0_mini = _mini(metrics)
            (p0_dir / 'mini_stats.json').write_text(json.dumps(p0_mini, indent=2))

            # Rebuild summary with all 5 periods
            old = json.loads((out_dir / 'summary.json').read_text())
            periods5 = {'p0': p0_mini}
            for pk in ('p1', 'p2', 'p3', 'p4'):
                periods5[pk] = old['periods'][pk]

            new_summary = dict(old)
            new_summary['score']         = round(_score_5p(periods5), 4)
            new_summary['score_version'] = 'p0-p4'
            new_summary['periods']       = {k: periods5[k]
                                            for k in ('p0', 'p1', 'p2', 'p3', 'p4')}
            (out_dir / 'summary.json').write_text(
                json.dumps(new_summary, indent=2, default=str)
            )

        elif action == ACT_FULL:
            # ─── Run all 5 periods from scratch ──────────────────────────────
            out_dir.mkdir(parents=True, exist_ok=True)
            period_results: dict = {}
            for pk, (p_start, p_end) in PERIODS_FULL.items():
                candles, warmup, route_tf = _build_period_candles(
                    full_1h, tf, p_start, p_end)
                raw     = _jesse_run(strategy_cls, route_tf, hp, candles, warmup)
                metrics = _extract_metrics(raw, p_start, p_end,
                                           no_upsample=no_up, timeframe=tf)
                pk_dir  = out_dir / pk
                pk_dir.mkdir(parents=True, exist_ok=True)
                mini = _mini(metrics)
                (pk_dir / 'mini_stats.json').write_text(json.dumps(mini, indent=2))
                period_results[pk] = mini

            summary = {
                'strategy':      strat,
                'tf':            tf,
                'variant':       variant,
                'combo_idx':     combo_idx + 1,
                'hp':            hp,
                'score':         round(_score_5p(period_results), 4),
                'score_version': 'p0-p4',
                'sweep_version': version,
                'periods':       period_results,
            }
            (out_dir / 'summary.json').write_text(
                json.dumps(summary, indent=2, default=str)
            )

        elapsed = time.monotonic() - t0
        return {'label': label, 'action': action, 'status': 'OK', 'elapsed': elapsed}

    except Exception as exc:
        import traceback
        elapsed = time.monotonic() - t0
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'P0_FAILED.marker').write_text(
            f"reason: {exc}\ntb: {traceback.format_exc()}\n"
            f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
        )
        return {'label': label, 'action': action, 'status': 'FAIL', 'elapsed': elapsed}


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='P0 extension for param sweep v2+v3 — adds sideways market period')
    parser.add_argument('--strategies', nargs='+', default=STRATEGIES_ALL,
                        choices=STRATEGIES_ALL,
                        help='Strategies to process (default: all 7)')
    parser.add_argument('--versions',   nargs='+', default=['v2', 'v3'],
                        choices=['v2', 'v3'],
                        help='Sweep versions to process (default: v2 v3)')
    parser.add_argument('--workers',    type=int, default=6,
                        help='Parallel workers (default: 6)')
    parser.add_argument('--dry-run',    action='store_true',
                        help='Print classification only, no execution')
    args = parser.parse_args()

    print(f'param_sweep_p0_extend')
    print(f'  P0: 2018-04-01 ~ 2020-06-30 (sideways/bear market)')
    print(f'  Score: mean(P0~P4 CAGR), all 5 periods MDD≥-35% AND trades≥5')
    print(f'  Versions: {args.versions}  Strategies: {args.strategies}')
    print(f'  Workers: {args.workers}')
    print(f'  Start: {datetime.now(timezone.utc).isoformat()}')
    print()

    jobs = _build_jobs(args.strategies, args.versions)

    counts = {ACT_P0: 0, ACT_FULL: 0, ACT_DONE: 0, ACT_DISQ: 0}
    for j in jobs:
        counts[j['action']] += 1

    print(f'  Total combos  : {len(jobs)}')
    print(f'  {ACT_P0:14s}: {counts[ACT_P0]}   (active, add P0 + re-score)')
    print(f'  {ACT_FULL:14s}: {counts[ACT_FULL]}   (v3 missing, run P0~P4 fresh)')
    print(f'  {ACT_DONE:14s}: {counts[ACT_DONE]}    (P0 already done — idempotent skip)')
    print(f'  {ACT_DISQ:14s}: {counts[ACT_DISQ]}   (-999 disqualified — skip)')
    print()

    pending = [j for j in jobs if j['action'] in (ACT_P0, ACT_FULL)]

    if args.dry_run:
        for j in pending:
            print(f'  {j["action"]:14s} {j["label"]}')
        print(f'\nDry-run: {len(pending)} jobs pending')
        return

    if not pending:
        print('Nothing to run.')
        return

    t_global = time.monotonic()
    results: list[dict] = []
    done_cnt = 0
    total    = len(pending)

    if args.workers <= 1:
        for job in pending:
            done_cnt += 1
            print(f'  [{done_cnt}/{total}] START {job["label"]}', flush=True)
            r = run_job(job)
            wall = time.monotonic() - t_global
            print(f'  [{done_cnt}/{total}] {r["status"]:6s} {job["label"]}  '
                  f'({r["elapsed"]:.0f}s, total {wall/60:.1f}m)', flush=True)
            results.append(r)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_job, j): j for j in pending}
            for fut in as_completed(futures):
                r = fut.result()
                done_cnt += 1
                wall = time.monotonic() - t_global
                print(f'  [{done_cnt}/{total}] {r["status"]:6s} {r["label"]}  '
                      f'({r["elapsed"]:.0f}s, total {wall/60:.1f}m)', flush=True)
                results.append(r)

    ok   = sum(1 for r in results if r['status'] == 'OK')
    fail = sum(1 for r in results if r['status'] == 'FAIL')
    wall = time.monotonic() - t_global

    print(f'\n--- P0 Extend Summary ---')
    print(f'  OK: {ok}  FAIL: {fail}  Wall time: {wall/60:.1f}m')
    if fail:
        print('\nFailed jobs:')
        for r in results:
            if r['status'] == 'FAIL':
                print(f'  FAIL [{r["action"]}] {r["label"]}')

    # Active combos that survived P0 gating
    active_after = sum(
        1 for j in jobs
        if j['action'] in (ACT_P0, ACT_DONE)  # was active, got P0
    )
    print(f'\n  Active combos before P0: ~{counts[ACT_P0] + counts[ACT_DONE]}')
    print(f'  (Run --dry-run then check summary.json scores to see P0 survival rate)')

    print(f'\nNext steps:')
    print(f'  1. Champion re-run (inside container):')
    print(f'     python /app/scripts/sweep/param_sweep_v3.py --champion-run')
    print(f'  2. Dashboard rebuild (on host):')
    print(f'     python3 backtest/scripts/reports/build_v4_dashboard.py')
    print(f'End: {datetime.now(timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
