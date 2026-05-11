#!/usr/bin/env python3
"""
Multi-strategy parameter sweep — 7 strategies × 2 TF × 2 variants × 6 combos = 168 backtests.

핵심 파라미터 2개를 6점(OAT + 교차)으로 테스트하여 최우수 조합을 찾는다.
결과는 /result/param_sweep/ 에 저장되고, param_sweep_report.md 가 자동 생성된다.

Usage (inside Jesse container):
    python /jesse-project/scripts/param_sweep.py --tfs 4h 1D --workers 2
    python /jesse-project/scripts/param_sweep.py --dry-run
    python /jesse-project/scripts/param_sweep.py --strategies supertrend --tfs 4h --workers 1

Idempotent: EXECUTION_SUCCESS.marker 있으면 SKIP.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
JESSE_ROOT  = SCRIPTS_DIR.parent
sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(JESSE_ROOT / 'strategies'))

from run_external_backtest import (
    _load_1h, _resample_1h, _expand_tf_to_1m, _upsample_to_1m,
    _extract_metrics, _pass_fail, EXCHANGE_NAME, SYMBOL, TF_MINUTES,
)

# ── Constants ─────────────────────────────────────────────────────────────────
START       = '2021-01-01'
END         = '2026-04-30'
BALANCE     = 10_000.0
FEE         = 0.0002
LEVERAGE    = 1
RESULT_DIR  = Path('/result/param_sweep')
TIMEFRAMES  = ['4h', '1D']
VARIANTS    = ['bidirectional', 'long_only']

# ── Sweep specs ───────────────────────────────────────────────────────────────
# base_hp: 모든 파라미터 기본값 (완전한 hp dict)
# param_keys: 변경할 2개 파라미터 이름
# combos: [(A, B), ...] 6가지 조합
SWEEP_SPECS: dict[str, dict] = {
    'stoch': {
        'cls': 'StochStrategy',
        'base_hp': {
            'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200,
            'stoch_k_period': 14, 'stoch_smooth': 3,
            'ob_level': 80.0, 'os_level': 20.0,
            'atr_mult': 3.0, 'use_direction_ema': False,
        },
        'param_keys': ['stoch_k_period', 'atr_mult'],
        'combos': [
            (10, 3.0),   # 1: k_period 낮춤
            (14, 3.0),   # 2: baseline
            (18, 3.0),   # 3: k_period 높임
            (14, 2.0),   # 4: atr_mult 낮춤
            (14, 4.0),   # 5: atr_mult 높임
            (10, 2.0),   # 6: 둘 다 낮춤
        ],
    },
    'momentum_ma': {
        'cls': 'MomentumMAStrategy',
        'base_hp': {
            'lin_len': 20, 'val_ma_len': 100, 'atr_mult': 3.0,
        },
        'param_keys': ['lin_len', 'atr_mult'],
        'combos': [
            (14, 3.0),   # 1: lin_len 낮춤
            (20, 3.0),   # 2: baseline
            (30, 3.0),   # 3: lin_len 높임
            (20, 2.0),   # 4: atr_mult 낮춤
            (20, 4.0),   # 5: atr_mult 높임
            (14, 2.0),   # 6: 둘 다 낮춤
        ],
    },
    'supertrend': {
        'cls': 'SupertrendStrategy',
        'base_hp': {
            'st_factor': 3.0, 'st_period': 7,
            'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0,
        },
        'param_keys': ['st_factor', 'st_period'],
        'combos': [
            (2.0, 7),    # 1: st_factor 낮춤
            (3.0, 7),    # 2: baseline
            (4.0, 7),    # 3: st_factor 높임
            (3.0, 5),    # 4: st_period 낮춤
            (3.0, 10),   # 5: st_period 높임
            (2.0, 5),    # 6: 둘 다 낮춤
        ],
    },
    'tradeiq_220320': {
        'cls': 'TradeIQ220320Strategy',
        'base_hp': {
            'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 14, 'atr_mult': 3.0,
        },
        'param_keys': ['rsi_len', 'atr_mult'],
        'combos': [
            (10, 3.0),   # 1: rsi_len 낮춤
            (14, 3.0),   # 2: baseline
            (18, 3.0),   # 3: rsi_len 높임
            (14, 2.0),   # 4: atr_mult 낮춤
            (14, 4.0),   # 5: atr_mult 높임
            (10, 2.0),   # 6: 둘 다 낮춤
        ],
    },
    'trendtype': {
        'cls': 'TrendTypeStrategy',
        'base_hp': {
            'atr_len': 14, 'atr_ma_len': 20,
            'di_len': 14, 'adx_len': 14,   # adx_len = di_len (wf_optimize_skopt.py 패턴)
            'smooth': 1, 'atr_mult': 3.0,
        },
        'param_keys': ['atr_len', 'di_len'],
        'combos': [
            (10, 14),    # 1: atr_len 낮춤
            (14, 14),    # 2: baseline
            (18, 14),    # 3: atr_len 높임
            (14, 10),    # 4: di_len 낮춤
            (14, 18),    # 5: di_len 높임
            (10, 10),    # 6: 둘 다 낮춤
        ],
    },
    'supertrend_trendtype': {
        'cls': 'SupertrendTrendTypeStrategy',
        'base_hp': {
            'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1,
            'st_factor': 3.0, 'st_period': 7,
            'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0,
        },
        'param_keys': ['st_factor', 'atr_len'],
        'combos': [
            (2.0, 14),   # 1: st_factor 낮춤
            (3.0, 14),   # 2: baseline
            (4.0, 14),   # 3: st_factor 높임
            (3.0, 10),   # 4: atr_len 낮춤
            (3.0, 18),   # 5: atr_len 높임
            (2.0, 10),   # 6: 둘 다 낮춤
        ],
    },
    'tradeiq_220323': {
        'cls': 'TradeIQ220323Strategy',
        'base_hp': {
            'cci_period': 20, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 3.0, 'atr_mult': 3.0,
        },
        'param_keys': ['cci_period', 'ce_mult'],
        'combos': [
            (14, 3.0),   # 1: cci_period 낮춤
            (20, 3.0),   # 2: baseline
            (26, 3.0),   # 3: cci_period 높임
            (20, 2.5),   # 4: ce_mult 낮춤
            (20, 3.5),   # 5: ce_mult 높임
            (14, 2.5),   # 6: 둘 다 낮춤
        ],
    },
}


# ── Candle loader ─────────────────────────────────────────────────────────────

def _build_candles(tf: str, start: str, end: str):
    """Load and preprocess candles for given TF. Returns (candles, warmup, route_tf)."""
    tf_hours    = TF_MINUTES[tf] // 60
    warmup_days = max(60, tf_hours * 220 // 24 + 1)
    wu_start    = (datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
                   - timedelta(days=warmup_days)).strftime('%Y-%m-%d')

    candles_1h = _load_1h(start, end)
    warmup_1h  = _load_1h(wu_start, start)

    if tf != '1h':
        tf_min     = TF_MINUTES[tf]
        candles_tf = _resample_1h(candles_1h, tf)
        warmup_tf  = _resample_1h(warmup_1h, tf)
        candles    = _expand_tf_to_1m(candles_tf, tf_min)
        warmup     = _expand_tf_to_1m(warmup_tf, tf_min)
        route_tf   = tf
    else:
        candles    = _upsample_to_1m(candles_1h)
        warmup     = _upsample_to_1m(warmup_1h)
        route_tf   = '1h'

    return candles, warmup, route_tf


# ── Backtest runner ───────────────────────────────────────────────────────────

def _run_one_backtest(strat: str, tf: str, variant: str, combo_idx: int,
                      candles, warmup, route_tf: str) -> dict:
    """Run a single backtest with explicit hp. Returns metrics dict."""
    from jesse import research
    import jesse.helpers as jh
    import importlib

    spec   = SWEEP_SPECS[strat]
    combo  = spec['combos'][combo_idx]
    hp     = dict(spec['base_hp'])
    for key, val in zip(spec['param_keys'], combo):
        hp[key] = val

    # For TrendTypeStrategy: keep adx_len = di_len
    if strat == 'trendtype' and 'di_len' in hp:
        hp['adx_len'] = hp['di_len']

    # Load strategy class
    ext_dir  = JESSE_ROOT / 'strategies' / 'external'
    cls_name = spec['cls']
    mod_path = ext_dir / f'{cls_name}.py'
    spec_mod = importlib.util.spec_from_file_location(cls_name, mod_path)
    mod      = importlib.util.module_from_spec(spec_mod)
    spec_mod.loader.exec_module(mod)
    strategy_cls = getattr(mod, cls_name)

    if variant == 'long_only':
        from external._long_only_factory import make_long_only
        strategy_cls = make_long_only(strategy_cls)

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

    raw     = research.backtest(
        config=config, routes=routes, data_routes=[],
        candles=candles_dict, warmup_candles=warmup_dict,
        hyperparameters=hp,
    )
    no_up   = tf != '1h'
    metrics = _extract_metrics(raw, START, END, no_upsample=no_up, timeframe=tf)
    verdict, checks = _pass_fail(metrics)
    return {**metrics, 'hp': hp, 'verdict': verdict, 'checks': checks,
            'strategy': strat, 'tf': tf, 'variant': variant, 'combo_idx': combo_idx + 1}


# ── Job runner ────────────────────────────────────────────────────────────────

def _job_output_dir(strat: str, tf: str, variant: str, combo_idx: int) -> Path:
    return RESULT_DIR / strat / tf / variant / f'combo_{combo_idx + 1}'


def _already_done(strat: str, tf: str, variant: str, combo_idx: int) -> bool:
    return (_job_output_dir(strat, tf, variant, combo_idx) / 'EXECUTION_SUCCESS.marker').exists()


def _write_result(out_dir: Path, result: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # mini_stats.json (주요 지표만)
    mini = {k: result[k] for k in (
        'strategy', 'tf', 'variant', 'combo_idx',
        'annual_return_pct', 'sharpe_ratio', 'max_drawdown_pct',
        'total_trades', 'win_rate_pct', 'profit_factor',
        'verdict', 'checks', 'hp',
    )}
    (out_dir / 'mini_stats.json').write_text(json.dumps(mini, indent=2, default=str))
    (out_dir / 'EXECUTION_SUCCESS.marker').write_text(
        f"status: SUCCESS\n"
        f"cagr: {result['annual_return_pct']:.4f}\n"
        f"sharpe: {result['sharpe_ratio']:.4f}\n"
        f"trades: {result['total_trades']}\n"
        f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
    )


def run_job(job: dict) -> dict:
    """Top-level job entry (safe for ProcessPoolExecutor)."""
    strat, tf, variant, combo_idx = job['strat'], job['tf'], job['variant'], job['combo_idx']
    label = job['label']

    if _already_done(strat, tf, variant, combo_idx):
        return {'label': label, 'status': 'SKIP', 'elapsed': 0, 'result': None}

    t0 = time.monotonic()
    try:
        candles, warmup, route_tf = _build_candles(tf, START, END)
        result = _run_one_backtest(strat, tf, variant, combo_idx, candles, warmup, route_tf)
        out_dir = _job_output_dir(strat, tf, variant, combo_idx)
        _write_result(out_dir, result)
        elapsed = time.monotonic() - t0
        return {'label': label, 'status': 'OK', 'elapsed': elapsed, 'result': result}
    except Exception as e:
        import traceback
        elapsed = time.monotonic() - t0
        out_dir = _job_output_dir(strat, tf, variant, combo_idx)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'EXECUTION_FAILED.marker').write_text(
            f"status: FAILED\nreason: {e}\ntb: {traceback.format_exc()}\n"
            f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
        )
        return {'label': label, 'status': 'FAIL', 'elapsed': elapsed, 'result': None}


# ── Job list ──────────────────────────────────────────────────────────────────

def _build_jobs(strats: list[str], tfs: list[str], variants: list[str]) -> list[dict]:
    jobs = []
    for strat in strats:
        spec = SWEEP_SPECS[strat]
        for tf in tfs:
            for variant in variants:
                for ci in range(len(spec['combos'])):
                    combo = spec['combos'][ci]
                    k1, k2 = spec['param_keys']
                    v1, v2 = combo
                    label = f'[{tf}] {strat}/{variant}/combo_{ci+1} ({k1}={v1},{k2}={v2})'
                    jobs.append({'strat': strat, 'tf': tf, 'variant': variant,
                                 'combo_idx': ci, 'label': label})
    return jobs


# ── Report generator ──────────────────────────────────────────────────────────

def _load_results(strats: list[str], tfs: list[str], variants: list[str]) -> list[dict]:
    rows = []
    for strat in strats:
        spec = SWEEP_SPECS[strat]
        for tf in tfs:
            for variant in variants:
                for ci in range(len(spec['combos'])):
                    p = _job_output_dir(strat, tf, variant, ci) / 'mini_stats.json'
                    if p.exists():
                        rows.append(json.loads(p.read_text()))
    return rows


def _combo_label(strat: str, ci: int) -> str:
    spec    = SWEEP_SPECS[strat]
    combo   = spec['combos'][ci]
    k1, k2  = spec['param_keys']
    v1, v2  = combo
    return f'{k1}={v1}, {k2}={v2}'


def generate_report(strats: list[str], tfs: list[str], variants: list[str]) -> None:
    ts   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    rows = _load_results(strats, tfs, variants)
    if not rows:
        print('  [warn] No results to report.')
        return

    lines = [
        '# 파라미터 스윕 결과 리포트 (자동 생성)',
        '',
        f'**생성 시각**: {ts}',
        '**생성 방식**: `param_sweep.py` (LLM 직접 작성 금지)',
        f'**분석 기간**: {START} ~ {END}',
        f'**초기 자금**: ${BALANCE:,.0f}',
        '',
        '---',
        '',
        '## 1. 전략별 최우수 파라미터 조합 (전 TF/variant 통합 기준)',
        '',
        '> 점수식: CAGR if (MDD ≥ -35% and trades ≥ 20) else -999',
        '',
        '| 전략 | TF | variant | combo | CAGR | Sharpe | MDD | Trades | verdict |',
        '|------|----|---------|----|------|--------|-----|--------|---------|',
    ]

    # Best combo per strategy (global best)
    best_by_strat: dict[str, dict] = {}
    for strat in strats:
        strat_rows = [r for r in rows if r['strategy'] == strat]
        if not strat_rows:
            continue
        def score(r: dict) -> float:
            mdd    = r.get('max_drawdown_pct', -999)
            trades = r.get('total_trades', 0)
            cagr   = r.get('annual_return_pct', -999)
            return cagr if (mdd >= -35.0 and trades >= 20) else -999.0
        best = max(strat_rows, key=score)
        best_by_strat[strat] = best
        ci    = best['combo_idx'] - 1
        clbl  = _combo_label(strat, ci)
        lines.append(
            f"| `{strat}` | {best['tf']} | {best['variant']} | #{best['combo_idx']} ({clbl}) "
            f"| {best['annual_return_pct']:+.2f}% "
            f"| {best['sharpe_ratio']:.3f} "
            f"| {best['max_drawdown_pct']:.2f}% "
            f"| {best['total_trades']} "
            f"| {best['verdict']} |"
        )

    lines += ['', '---', '', '## 2. 전략 × TF × variant 상세 결과', '']

    for strat in strats:
        spec = SWEEP_SPECS[strat]
        k1, k2 = spec['param_keys']
        lines += [
            f'### `{strat}` ({spec["cls"]})',
            f'**최적화 파라미터**: `{k1}` × `{k2}`',
            '',
        ]
        for tf in tfs:
            for variant in variants:
                combo_rows = sorted(
                    [r for r in rows if r['strategy'] == strat and r['tf'] == tf and r['variant'] == variant],
                    key=lambda r: r['combo_idx']
                )
                if not combo_rows:
                    continue
                lines += [
                    f'#### {tf} / {variant}',
                    '',
                    f'| combo | {k1} | {k2} | CAGR | Sharpe | MDD | Trades | verdict |',
                    f'|-------|-----|-----|------|--------|-----|--------|---------|',
                ]
                for r in combo_rows:
                    ci   = r['combo_idx'] - 1
                    hp   = r.get('hp', {})
                    v1   = hp.get(k1, '?')
                    v2   = hp.get(k2, '?')
                    flag = ' ← best' if (
                        strat in best_by_strat and
                        best_by_strat[strat]['tf'] == tf and
                        best_by_strat[strat]['variant'] == variant and
                        best_by_strat[strat]['combo_idx'] == r['combo_idx']
                    ) else ''
                    lines.append(
                        f"| #{r['combo_idx']}{flag} | {v1} | {v2} "
                        f"| {r['annual_return_pct']:+.2f}% "
                        f"| {r['sharpe_ratio']:.3f} "
                        f"| {r['max_drawdown_pct']:.2f}% "
                        f"| {r['total_trades']} "
                        f"| {r['verdict']} |"
                    )
                lines.append('')

    # Improvement summary vs baseline (combo 2 = baseline)
    lines += ['---', '', '## 3. baseline 대비 개선율', '',
              '| 전략 | TF | variant | best combo | CAGR 개선 | Sharpe 개선 | MDD 개선 |',
              '|------|----|---------|---------|---------|---------|----|',
    ]
    for strat in strats:
        spec = SWEEP_SPECS[strat]
        for tf in tfs:
            for variant in variants:
                base_rows = [r for r in rows
                             if r['strategy'] == strat and r['tf'] == tf
                             and r['variant'] == variant and r['combo_idx'] == 2]
                best_rows = [r for r in rows
                             if r['strategy'] == strat and r['tf'] == tf
                             and r['variant'] == variant]
                if not base_rows or not best_rows:
                    continue
                base = base_rows[0]
                def score(r: dict) -> float:
                    mdd    = r.get('max_drawdown_pct', -999)
                    trades = r.get('total_trades', 0)
                    cagr   = r.get('annual_return_pct', -999)
                    return cagr if (mdd >= -35.0 and trades >= 20) else -999.0
                best = max(best_rows, key=score)
                if best['combo_idx'] == 2:
                    continue  # no improvement
                d_cagr   = best['annual_return_pct']  - base['annual_return_pct']
                d_sharpe = best['sharpe_ratio']        - base['sharpe_ratio']
                d_mdd    = best['max_drawdown_pct']    - base['max_drawdown_pct']
                ci       = best['combo_idx'] - 1
                clbl     = _combo_label(strat, ci)
                lines.append(
                    f"| `{strat}` | {tf} | {variant} | #{best['combo_idx']} ({clbl}) "
                    f"| {d_cagr:+.2f}% | {d_sharpe:+.3f} | {d_mdd:+.2f}% |"
                )

    out = RESULT_DIR / 'param_sweep_report.md'
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(lines) + '\n')
    print(f'\nReport written: {out}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description='Multi-strategy parameter sweep')
    p.add_argument('--strategies', nargs='+', default=list(SWEEP_SPECS.keys()),
                   choices=list(SWEEP_SPECS.keys()), help='Strategies to sweep')
    p.add_argument('--tfs',      nargs='+', default=TIMEFRAMES,
                   choices=['1h', '2h', '4h', '1D'])
    p.add_argument('--variants', nargs='+', default=VARIANTS,
                   choices=['bidirectional', 'long_only'])
    p.add_argument('--workers',  type=int, default=1)
    p.add_argument('--dry-run',  action='store_true')
    args = p.parse_args()

    jobs  = _build_jobs(args.strategies, args.tfs, args.variants)
    total = len(jobs)
    done_cnt = sum(1 for j in jobs if _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx']))

    print(f'Param sweep: {total} jobs ({done_cnt} already done), workers={args.workers}')
    print(f'Strategies : {args.strategies}')
    print(f'TFs        : {args.tfs}   Variants: {args.variants}')
    print(f'Output     : {RESULT_DIR}')
    print(f'Start      : {datetime.now(timezone.utc).isoformat()}')
    print()

    if args.dry_run:
        for j in jobs:
            done = '✓' if _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx']) else ' '
            print(f'  {done} {j["label"]}')
        print(f'\nTotal: {total}  Done: {done_cnt}  Pending: {total - done_cnt}')
        return

    t_global = time.monotonic()
    results_list: list[dict] = []

    pending = [j for j in jobs if not _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx'])]
    # Pre-collect SKIPs
    for j in jobs:
        if _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx']):
            results_list.append({'label': j['label'], 'status': 'SKIP', 'elapsed': 0})

    if args.workers <= 1:
        for i, job in enumerate(pending, 1):
            print(f'  [{i}/{len(pending)}] START {job["label"]}', flush=True)
            r = run_job(job)
            elapsed_total = time.monotonic() - t_global
            print(f'  [{i}/{len(pending)}] {r["status"]:6s} {job["label"]}  '
                  f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)
            if r.get('result'):
                m = r['result']
                print(f'    CAGR={m["annual_return_pct"]:+.2f}%  '
                      f'Sharpe={m["sharpe_ratio"]:.3f}  '
                      f'MDD={m["max_drawdown_pct"]:.2f}%  '
                      f'Trades={m["total_trades"]}  '
                      f'{m["verdict"]}', flush=True)
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

    # Summary
    counts: dict[str, int] = {}
    for r in results_list:
        counts[r['status']] = counts.get(r['status'], 0) + 1
    total_elapsed = time.monotonic() - t_global
    print(f'\n--- Sweep summary ---')
    for k, v in counts.items():
        if v:
            print(f'  {k}: {v}')
    print(f'  Total wall time: {total_elapsed/60:.1f}m')

    failures = [r for r in results_list if r['status'] == 'FAIL']
    if failures:
        print('\nFailed jobs:')
        for r in failures:
            print(f'  FAIL {r["label"]}')

    print('\nGenerating report...', flush=True)
    generate_report(args.strategies, args.tfs, args.variants)
    print(f'End: {datetime.now(timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
