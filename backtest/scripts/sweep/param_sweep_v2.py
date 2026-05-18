#!/usr/bin/env python3
"""
param_sweep_v2.py — 7 strategies × 16 combos × 3 TF × 2 variants × 4 periods = 2,688 backtests.

4-period evaluation (BTC market cycle coverage):
  P1: 2021-04-01 ~ 2026-04-30  고점 매수 → 현재
  P2: 2022-12-01 ~ 2026-04-30  저점 매수 → 현재
  P3: 2021-04-01 ~ 2025-09-30  고점 → 다음 고점 (전체 사이클)
  P4: 2022-12-01 ~ 2025-09-30  저점 → 고점 (Bull run)

score = mean(P1~P4 CAGR)  if ALL periods: MDD >= -35% AND trades >= 5  else -999

Usage (inside Jesse container):
    python /jesse-project/scripts/param_sweep_v2.py --strategies supertrend --tfs 4h --workers 1
    python /jesse-project/scripts/param_sweep_v2.py --dry-run
    python /jesse-project/scripts/param_sweep_v2.py --strategies stoch --workers 1
    python /jesse-project/scripts/param_sweep_v2.py --champion-run  # best params → 7-strategies dir

Idempotent: summary.json 있으면 SKIP.
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
    _write_stats, _write_trades, _write_equity_curve,
    _write_monthly_returns, _write_decision, _copy_strategy,
    write_success_marker,
    EXCHANGE_NAME, SYMBOL, TF_MINUTES,
)

# ── Constants ──────────────────────────────────────────────────────────────────
BALANCE       = 10_000.0
FEE           = 0.0002
LEVERAGE      = 1
RESULT_DIR    = Path('/result/param_sweep_v2')
CHAMPION_DIR  = Path('/result/7-strategies')
TIMEFRAMES    = ['1h', '4h', '1D']
VARIANTS      = ['bidirectional', 'long_only']
N_COMBOS      = 16

FULL_START    = '2020-05-01'   # 전체 캔들 로딩 범위 (warmup 포함)
FULL_END      = '2026-04-30'

CHAMPION_START = '2021-01-01'
CHAMPION_END   = '2026-04-30'

PERIODS = {
    'p1': ('2021-04-01', '2026-04-30'),
    'p2': ('2022-12-01', '2026-04-30'),
    'p3': ('2021-04-01', '2025-09-30'),
    'p4': ('2022-12-01', '2025-09-30'),
}

# ── Sweep specs (16 combos per strategy) ──────────────────────────────────────
# combos[0..5]  = param_sweep.py v1 와 동일 (기존 결과 재활용)
# combos[6..15] = 신규 추가
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
            (10, 3.0), (14, 3.0), (18, 3.0),   # 1-3: OAT k_period
            (14, 2.0), (14, 4.0), (10, 2.0),   # 4-6: OAT atr + v1 cross
            (18, 2.0), (18, 4.0), (10, 4.0),   # 7-9: remaining crosses
            (7,  3.0), (21, 3.0),               # 10-11: extreme k
            (7,  2.0), (21, 2.0),               # 12-13: extreme k + tight
            (7,  4.0), (21, 4.0),               # 14-15: extreme k + wide
            (14, 1.5),                           # 16: ultra-tight stop
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
            (2.0, 7), (3.0, 7), (4.0, 7),
            (3.0, 5), (3.0, 10), (2.0, 5),
            (4.0, 5), (4.0, 10), (2.0, 10),
            (1.5, 7), (5.0, 7),
            (1.5, 5), (5.0, 5),
            (1.5, 10), (5.0, 10),
            (2.5, 7),
        ],
    },
    'tradeiq_psar_ha': {
        'cls': 'TradeIQPsarHaStrategy',
        'base_hp': {
            'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 14, 'atr_mult': 3.0,
        },
        'param_keys': ['rsi_len', 'atr_mult'],
        'combos': [
            (10, 3.0), (14, 3.0), (18, 3.0),
            (14, 2.0), (14, 4.0), (10, 2.0),
            (18, 2.0), (18, 4.0), (10, 4.0),
            (7,  3.0), (21, 3.0),
            (7,  2.0), (21, 2.0),
            (7,  4.0), (21, 4.0),
            (14, 1.5),
        ],
    },
    'trendtype': {
        'cls': 'TrendTypeStrategy',
        'base_hp': {
            'atr_len': 14, 'atr_ma_len': 20,
            'di_len': 14, 'adx_len': 14,   # adx_len 항상 di_len 과 동기화
            'smooth': 1, 'atr_mult': 3.0,
        },
        'param_keys': ['atr_len', 'di_len'],
        'combos': [
            (10, 14), (14, 14), (18, 14),
            (14, 10), (14, 18), (10, 10),
            (10, 18), (18, 10), (18, 18),
            (7,  14), (21, 14),
            (14,  7), (14, 21),
            (7,   7), (21, 21),
            (10,  7),
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
            (2.0, 14), (3.0, 14), (4.0, 14),
            (3.0, 10), (3.0, 18), (2.0, 10),
            (4.0, 10), (4.0, 18), (2.0, 18),
            (1.5, 14), (5.0, 14),
            (1.5, 10), (5.0, 10),
            (1.5, 18), (5.0, 18),
            (2.5, 12),
        ],
    },
    'tradeiq_cci_ce': {
        'cls': 'TradeIQCciCeStrategy',
        'base_hp': {
            'cci_period': 20, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 3.0, 'atr_mult': 3.0,
        },
        'param_keys': ['cci_period', 'ce_mult'],
        'combos': [
            (14, 3.0), (20, 3.0), (26, 3.0),
            (20, 2.5), (20, 3.5), (14, 2.5),
            (26, 2.5), (26, 3.5), (14, 3.5),
            (10, 3.0), (30, 3.0),
            (10, 2.5), (30, 2.5),
            (10, 3.5), (30, 3.5),
            (20, 2.0),
        ],
    },
}

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


def _build_period_candles(full_1h: np.ndarray, tf: str, p_start: str, p_end: str):
    """Slice period + warmup from pre-loaded full 1h array, then resample/expand for TF."""
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

def _build_hp(strat: str, combo_idx: int) -> dict:
    spec  = SWEEP_SPECS[strat]
    hp    = dict(spec['base_hp'])
    combo = spec['combos'][combo_idx]
    for key, val in zip(spec['param_keys'], combo):
        hp[key] = val
    if strat == 'trendtype' and 'di_len' in hp:
        hp['adx_len'] = hp['di_len']
    return hp


def _load_strategy_cls(strat: str, variant: str):
    spec     = SWEEP_SPECS[strat]
    cls_name = spec['cls']
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


def _jesse_run(strategy_cls, route_tf: str, hp: dict, candles, warmup,
               full_output: bool = False):
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

    kwargs: dict = {}
    if full_output:
        kwargs = {'generate_json': True, 'generate_csv': True, 'generate_equity_curve': True}

    return research.backtest(
        config=config, routes=routes, data_routes=[],
        candles=candles_dict, warmup_candles=warmup_dict,
        hyperparameters=hp, **kwargs,
    )


def _score_combo(period_results: dict) -> float:
    for m in period_results.values():
        if m['max_drawdown_pct'] < -35.0 or m['total_trades'] < 5:
            return -999.0
    return statistics.mean(m['annual_return_pct'] for m in period_results.values())


# ── Job output / idempotency ───────────────────────────────────────────────────

def _job_output_dir(strat: str, tf: str, variant: str, combo_idx: int) -> Path:
    return RESULT_DIR / strat / tf / variant / f'combo_{combo_idx + 1}'


def _already_done(strat: str, tf: str, variant: str, combo_idx: int) -> bool:
    return (_job_output_dir(strat, tf, variant, combo_idx) / 'summary.json').exists()


# ── Job runner ─────────────────────────────────────────────────────────────────

def run_job(job: dict) -> dict:
    strat, tf, variant, combo_idx = job['strat'], job['tf'], job['variant'], job['combo_idx']
    label   = job['label']
    out_dir = _job_output_dir(strat, tf, variant, combo_idx)

    if _already_done(strat, tf, variant, combo_idx):
        return {'label': label, 'status': 'SKIP', 'elapsed': 0, 'summary': None}

    t0 = time.monotonic()
    try:
        hp           = _build_hp(strat, combo_idx)
        strategy_cls = _load_strategy_cls(strat, variant)
        full_1h      = _get_full_1h()
        no_up        = tf != '1h'

        period_results: dict = {}
        for pk, (p_start, p_end) in PERIODS.items():
            candles, warmup, route_tf = _build_period_candles(full_1h, tf, p_start, p_end)
            raw     = _jesse_run(strategy_cls, route_tf, hp, candles, warmup)
            metrics = _extract_metrics(raw, p_start, p_end, no_upsample=no_up, timeframe=tf)

            p_dir = out_dir / pk
            p_dir.mkdir(parents=True, exist_ok=True)
            mini = {k: metrics[k] for k in (
                'annual_return_pct', 'sharpe_ratio', 'max_drawdown_pct',
                'total_trades', 'win_rate_pct', 'profit_factor',
            )}
            (p_dir / 'mini_stats.json').write_text(json.dumps(mini, indent=2))
            period_results[pk] = metrics

        score   = _score_combo(period_results)
        summary = {
            'strategy':  strat,
            'tf':        tf,
            'variant':   variant,
            'combo_idx': combo_idx + 1,
            'hp':        hp,
            'score':     round(score, 4),
            'periods': {
                pk: {
                    'cagr':     round(period_results[pk]['annual_return_pct'], 4),
                    'sharpe':   round(period_results[pk]['sharpe_ratio'], 4),
                    'mdd':      round(period_results[pk]['max_drawdown_pct'], 4),
                    'trades':   period_results[pk]['total_trades'],
                    'win_rate': round(period_results[pk]['win_rate_pct'], 4),
                    'pf':       round(period_results[pk]['profit_factor'], 4),
                }
                for pk in PERIODS
            },
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

def _build_jobs(strats: list, tfs: list, variants: list,
                combos_filter: list | None = None) -> list[dict]:
    jobs = []
    combo_indices = combos_filter if combos_filter is not None else list(range(N_COMBOS))
    for strat in strats:
        spec = SWEEP_SPECS[strat]
        for tf in tfs:
            for variant in variants:
                for ci in combo_indices:
                    if ci >= N_COMBOS or ci >= len(spec['combos']):
                        continue
                    combo = spec['combos'][ci]
                    k1, k2 = spec['param_keys']
                    v1, v2 = combo
                    label = f'[{tf}] {strat}/{variant}/combo_{ci+1} ({k1}={v1},{k2}={v2})'
                    jobs.append({'strat': strat, 'tf': tf, 'variant': variant,
                                 'combo_idx': ci, 'label': label})
    return jobs


# ── Summary loader ─────────────────────────────────────────────────────────────

def _load_all_summaries(strats: list, tfs: list, variants: list) -> list[dict]:
    rows = []
    for strat in strats:
        for tf in tfs:
            for variant in variants:
                for ci in range(N_COMBOS):
                    p = _job_output_dir(strat, tf, variant, ci) / 'summary.json'
                    if p.exists():
                        rows.append(json.loads(p.read_text()))
    return rows


# ── Report generator ───────────────────────────────────────────────────────────

def generate_report(strats: list, tfs: list, variants: list) -> None:
    ts   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    rows = _load_all_summaries(strats, tfs, variants)
    if not rows:
        print('  [warn] No summary.json found.')
        return

    lines = [
        '# param_sweep_v2 결과 리포트 (자동 생성)',
        '',
        f'**생성**: {ts}',
        '**평가 기간**: P1=2021-04~2026-04 / P2=2022-12~2026-04 / P3=2021-04~2025-09 / P4=2022-12~2025-09',
        '**점수식**: mean(P1~P4 CAGR)  if ALL MDD≥-35% AND trades≥5  else -999',
        '',
        '---',
        '',
        '## 1. TF별 최우수 파라미터 (long_only 기준)',
        '',
        '| 전략 | TF | param_A | param_B | score | P1 | P2 | P3 | P4 |',
        '|------|----|---------|---------|-------|----|----|----|-----|',
    ]

    for strat in strats:
        spec = SWEEP_SPECS[strat]
        k1, k2 = spec['param_keys']
        for tf in tfs:
            subset = [r for r in rows if r['strategy'] == strat
                      and r['tf'] == tf and r['variant'] == 'long_only']
            if not subset:
                continue
            best = max(subset, key=lambda r: r.get('score', -999))
            hp   = best['hp']
            p    = best['periods']
            lines.append(
                f"| `{strat}` | {tf} | {hp.get(k1,'?')} | {hp.get(k2,'?')} "
                f"| {best['score']:+.2f} "
                f"| {p['p1']['cagr']:+.1f}% "
                f"| {p['p2']['cagr']:+.1f}% "
                f"| {p['p3']['cagr']:+.1f}% "
                f"| {p['p4']['cagr']:+.1f}% |"
            )

    lines += ['', '---', '', '## 2. 전략별 상세 결과', '']

    for strat in strats:
        spec = SWEEP_SPECS[strat]
        k1, k2 = spec['param_keys']
        lines += [f'### `{strat}` ({spec["cls"]})', '']
        for tf in tfs:
            for variant in variants:
                combo_rows = sorted(
                    [r for r in rows if r['strategy'] == strat
                     and r['tf'] == tf and r['variant'] == variant],
                    key=lambda r: r['combo_idx'],
                )
                if not combo_rows:
                    continue
                best_ci = max(combo_rows, key=lambda r: r.get('score', -999))['combo_idx']
                lines += [
                    f'#### {tf} / {variant}',
                    '',
                    f'| # | {k1} | {k2} | score | P1 CAGR | P1 MDD | P2 CAGR | P2 MDD | '
                    f'P3 CAGR | P3 MDD | P4 CAGR | P4 MDD |',
                    f'|---|-----|-----|-------|---------|--------|---------|--------|'
                    f'---------|--------|---------|--------|',
                ]
                for r in combo_rows:
                    hp  = r['hp']
                    p   = r['periods']
                    flg = ' ←' if r['combo_idx'] == best_ci else ''
                    lines.append(
                        f"| #{r['combo_idx']}{flg} "
                        f"| {hp.get(k1,'?')} | {hp.get(k2,'?')} "
                        f"| {r['score']:+.2f} "
                        f"| {p['p1']['cagr']:+.1f}% | {p['p1']['mdd']:.1f}% "
                        f"| {p['p2']['cagr']:+.1f}% | {p['p2']['mdd']:.1f}% "
                        f"| {p['p3']['cagr']:+.1f}% | {p['p3']['mdd']:.1f}% "
                        f"| {p['p4']['cagr']:+.1f}% | {p['p4']['mdd']:.1f}% |"
                    )
                lines.append('')

    out = RESULT_DIR / 'param_sweep_v2_report.md'
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(lines) + '\n')
    print(f'\nReport written: {out}')


# ── Champion run ───────────────────────────────────────────────────────────────

def run_champion_backtests(strats: list, tfs: list, variants: list) -> None:
    """Find best combo per (strat,tf,variant) and run full-period backtest.
    Writes stats.json + supporting files to CHAMPION_DIR for dashboard rebuild.
    """
    summaries = _load_all_summaries(strats, tfs, variants)
    if not summaries:
        print('[champion] No summary.json found. Run sweep first.')
        return

    best_map: dict = {}
    for s in summaries:
        key = (s['strategy'], s['tf'], s['variant'])
        if key not in best_map or s.get('score', -999) > best_map[key].get('score', -999):
            best_map[key] = s

    full_1h = _get_full_1h()
    print(f'\n[champion] {len(best_map)} backtests to run')
    print(f'[champion] Output: {CHAMPION_DIR}\n')

    for (strat, tf, variant), best in sorted(best_map.items()):
        hp   = best['hp']
        spec = SWEEP_SPECS[strat]
        k1, k2 = spec['param_keys']
        out_dir = CHAMPION_DIR / strat / tf / variant
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f'  {strat}/{tf}/{variant}  combo #{best["combo_idx"]} '
              f'({k1}={hp.get(k1,"?")}, {k2}={hp.get(k2,"?")})  score={best["score"]:.2f}',
              flush=True)

        try:
            candles, warmup, route_tf = _build_period_candles(
                full_1h, tf, CHAMPION_START, CHAMPION_END)
            strategy_cls = _load_strategy_cls(strat, variant)
            raw = _jesse_run(strategy_cls, route_tf, hp, candles, warmup, full_output=True)

            no_up   = tf != '1h'
            metrics = _extract_metrics(raw, CHAMPION_START, CHAMPION_END,
                                       no_upsample=no_up, timeframe=tf)
            verdict, checks = _pass_fail(metrics)

            trades = raw.get('trades', []) or []
            _write_stats(out_dir, spec['cls'], metrics, verdict, checks,
                         balance=BALANCE, leverage=LEVERAGE, variant=variant)
            _write_trades(out_dir, trades)
            _write_equity_curve(out_dir, spec['cls'], raw.get('equity_curve'))
            _write_monthly_returns(out_dir, trades, CHAMPION_START, CHAMPION_END)
            _write_decision(out_dir, spec['cls'], metrics, verdict, checks)
            _copy_strategy(out_dir, spec['cls'])

            stats_path = out_dir / 'stats.json'
            if stats_path.exists():
                write_success_marker(out_dir, stats_path)

            print(f'    → CAGR={metrics["annual_return_pct"]:+.2f}%  '
                  f'Sharpe={metrics["sharpe_ratio"]:.3f}  '
                  f'MDD={metrics["max_drawdown_pct"]:.2f}%  '
                  f'Trades={metrics["total_trades"]}  {verdict}', flush=True)

        except Exception as e:
            import traceback
            print(f'    [FAIL] {e}', flush=True)
            (out_dir / 'EXECUTION_FAILED.marker').write_text(
                f"status: FAILED\nreason: {e}\ntb: {traceback.format_exc()}\n"
                f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
            )

    print('\n[champion] Done.')
    print('Rebuild dashboard on HOST:')
    print('  python3 backtest/scripts/reports/build_v4_dashboard.py')


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='Multi-strategy parameter sweep v2')
    parser.add_argument('--strategies', nargs='+', default=list(SWEEP_SPECS.keys()),
                        choices=list(SWEEP_SPECS.keys()))
    parser.add_argument('--tfs',      nargs='+', default=TIMEFRAMES,
                        choices=['1h', '4h', '1D'])
    parser.add_argument('--variants', nargs='+', default=VARIANTS,
                        choices=['bidirectional', 'long_only'])
    parser.add_argument('--combos',   nargs='+', type=int, default=None,
                        help='0-based combo indices (default: all 0-15). New only: 6 7 8 9 10 11 12 13 14 15')
    parser.add_argument('--workers',  type=int, default=1)
    parser.add_argument('--dry-run',  action='store_true')
    parser.add_argument('--champion-run', action='store_true',
                        help='Run full-period backtest with best params → write to 7-strategies dir')
    args = parser.parse_args()

    if args.champion_run:
        run_champion_backtests(args.strategies, args.tfs, args.variants)
        return

    jobs     = _build_jobs(args.strategies, args.tfs, args.variants, args.combos)
    total    = len(jobs)
    done_cnt = sum(1 for j in jobs
                   if _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx']))

    print(f'param_sweep_v2: {total} jobs ({done_cnt} done, {total-done_cnt} pending)  '
          f'workers={args.workers}')
    print(f'Strategies : {args.strategies}')
    print(f'TFs        : {args.tfs}   Variants: {args.variants}')
    print(f'Combos     : {args.combos or "all 0-15"}')
    print(f'Output     : {RESULT_DIR}')
    print(f'Start      : {datetime.now(timezone.utc).isoformat()}')
    print()

    if args.dry_run:
        for j in jobs:
            done = '✓' if _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx']) else ' '
            print(f'  {done} {j["label"]}')
        print(f'\nTotal: {total}  Done: {done_cnt}  Pending: {total - done_cnt}')
        return

    t_global     = time.monotonic()
    results_list: list[dict] = []

    pending = [j for j in jobs
               if not _already_done(j['strat'], j['tf'], j['variant'], j['combo_idx'])]
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
            if r.get('summary'):
                s = r['summary']
                p = s['periods']
                print(f'    score={s["score"]:+.2f}  '
                      f'P1={p["p1"]["cagr"]:+.1f}%  P2={p["p2"]["cagr"]:+.1f}%  '
                      f'P3={p["p3"]["cagr"]:+.1f}%  P4={p["p4"]["cagr"]:+.1f}%', flush=True)
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
    generate_report(args.strategies, args.tfs, args.variants)
    print(f'End: {datetime.now(timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
