#!/usr/bin/env python3
"""
TrendType Bayesian Optimization (skopt gp_minimize).

Search space: atr_len(7-21), di_len(7-21), atr_mult(1.5-5.0)
IS  : 2020-01-01 → 2022-12-31  (3yr, diverse: bull+bear)
OOS : 2023-01-01 → 2025-12-31  (3yr, OOS validation)
Q1  : 2026-01-01 → 2026-03-31  (paper trading window)

Objective: -(Sharpe × clamp(trades/30, 0, 1))
  MDD < -30% → heavy penalty (+5) to enforce conservative HP

Fixed HP: atr_ma_len=20, smooth=1, adx_len=di_len

Output:
  /result/v3/walk_forward/optimization/
    best_hp.json           — best HP per variant
    optimization_log.json  — all 40 calls per variant
    OPTIMIZATION_REPORT.md — auto-generated summary
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
from skopt import gp_minimize
from skopt.space import Integer, Real
from skopt.utils import use_named_args

# ── Path setup ────────────────────────────────────────────────────────────────
SCRIPTS_DIR   = Path(__file__).parent
JESSE_ROOT    = SCRIPTS_DIR.parent
STRATEGIES_DIR = JESSE_ROOT / 'strategies'
sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(STRATEGIES_DIR))

from run_external_backtest import _load_1h, _extract_metrics, EXCHANGE_NAME, SYMBOL
from external._long_only_factory import make_long_only

# ── Config ────────────────────────────────────────────────────────────────────
IS_START  = '2021-04-01'
IS_END    = '2022-12-31'
OOS_START = '2023-01-01'
OOS_END   = '2025-12-31'
Q1_START  = '2026-01-01'
Q1_END    = '2026-03-31'
N_CALLS   = 40
BALANCE   = 10_000.0

OUT_BASE = Path('/result/v3/walk_forward/optimization')
OUT_BASE.mkdir(parents=True, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_candles_pair(start: str, end: str):
    """Return (candles_1h, warmup_1h). Warmup = 60-day prior or first slice."""
    candles_1h = _load_1h(start, end)
    start_dt   = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    wu_start_dt = start_dt - timedelta(days=60)
    min_date    = datetime(2020, 1, 1, tzinfo=timezone.utc)
    if wu_start_dt < min_date:
        warmup_1h = candles_1h[:min(1440, len(candles_1h))]
    else:
        wu_start  = wu_start_dt.strftime('%Y-%m-%d')
        warmup_1h = _load_1h(wu_start, start)
    return candles_1h, warmup_1h


def _run_backtest(start: str, end: str, hp: dict, strategy_cls,
                  candles_1h=None, warmup_1h=None) -> dict | None:
    """Single Jesse backtest with explicit HPs. Returns metrics or None."""
    from jesse import research
    import jesse.helpers as jh

    if candles_1h is None:
        candles_1h, warmup_1h = _load_candles_pair(start, end)
    if len(candles_1h) < 50:
        return None

    key          = jh.key(EXCHANGE_NAME, SYMBOL)
    candles_dict = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': candles_1h}}
    warmup_dict  = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warmup_1h}}

    config = {
        'starting_balance':      BALANCE,
        'fee':                   0.00055,
        'type':                  'futures',
        'futures_leverage':      1,
        'futures_leverage_mode': 'isolated',
        'exchange':              EXCHANGE_NAME,
        'warm_up_candles':       len(warmup_1h),
    }
    routes = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
               'symbol': SYMBOL, 'timeframe': '1h'}]
    try:
        raw = research.backtest(
            config=config, routes=routes, data_routes=[],
            candles=candles_dict, warmup_candles=warmup_dict,
            hyperparameters=hp,
        )
        return _extract_metrics(raw, start, end, no_upsample=True)
    except Exception as e:
        print(f'    [err] {e}')
        return None


def _get_strategy(variant: str):
    from external.TrendTypeStrategy import TrendTypeStrategy
    return TrendTypeStrategy if variant == 'bidirectional' else make_long_only(TrendTypeStrategy)


# ── Core optimization ─────────────────────────────────────────────────────────

def optimize_variant(variant: str) -> dict:
    print(f'\n{"="*60}')
    print(f'  TrendType {variant} — skopt Bayesian optimization')
    print(f'  IS: {IS_START} → {IS_END}  n_calls={N_CALLS}')
    print(f'{"="*60}')

    strategy_cls = _get_strategy(variant)

    # Pre-load IS candles once to avoid repeated disk I/O inside objective
    is_candles, is_warmup = _load_candles_pair(IS_START, IS_END)

    space = [
        Integer(7, 21,  name='atr_len'),
        Integer(7, 21,  name='di_len'),
        Real(1.5, 5.0,  name='atr_mult'),
    ]
    call_log: list[dict] = []

    @use_named_args(space)
    def objective(atr_len, di_len, atr_mult):
        hp = {
            'atr_len':    int(atr_len),
            'atr_ma_len': 20,
            'di_len':     int(di_len),
            'adx_len':    int(di_len),
            'smooth':     1,
            'atr_mult':   round(float(atr_mult), 3),
        }
        m = _run_backtest(IS_START, IS_END, hp, strategy_cls,
                          candles_1h=is_candles, warmup_1h=is_warmup)
        if m is None:
            score = 10.0
        else:
            sharpe = m['sharpe_ratio']
            trades = m['total_trades']
            mdd    = m['max_drawdown_pct']
            if mdd < -30.0:
                # MDD 위반: 보수 패널티
                score = 5.0 + abs(mdd + 30) / 20
            else:
                # Penalized Sharpe: sparse trades reduce score
                score = -(sharpe * min(trades / 30.0, 1.0))

        entry = {
            'n':       len(call_log) + 1,
            'atr_len': int(atr_len),
            'di_len':  int(di_len),
            'atr_mult': round(float(atr_mult), 3),
            'score':   round(score, 4),
            'sharpe':  m['sharpe_ratio']          if m else None,
            'trades':  m['total_trades']           if m else None,
            'mdd':     m['max_drawdown_pct']       if m else None,
            'cagr':    m['annual_return_pct']      if m else None,
        }
        call_log.append(entry)
        n = entry['n']
        if n == 1 or n % 10 == 0 or n == N_CALLS:
            sh  = f"{m['sharpe_ratio']:.3f}" if m else 'err'
            tr  = m['total_trades'] if m else '-'
            print(f'  [{n:3d}/{N_CALLS}] atr={int(atr_len):2d} di={int(di_len):2d} '
                  f'mult={float(atr_mult):.2f} → score={score:+.4f}  Sh={sh} T={tr}')
        return score

    result = gp_minimize(objective, space, n_calls=N_CALLS, n_initial_points=12,
                         random_state=42, noise=0.01, acq_func='EI')

    best_hp = {
        'atr_len':    int(result.x[0]),
        'atr_ma_len': 20,
        'di_len':     int(result.x[1]),
        'adx_len':    int(result.x[1]),
        'smooth':     1,
        'atr_mult':   round(float(result.x[2]), 3),
    }
    print(f'\n  Best HP (IS): {best_hp}  score={result.fun:.4f}')

    # IS / OOS / Q1 validation with best HP
    is_m  = _run_backtest(IS_START,  IS_END,  best_hp, strategy_cls,
                          candles_1h=is_candles, warmup_1h=is_warmup)
    oos_m = _run_backtest(OOS_START, OOS_END, best_hp, strategy_cls)
    q1_m  = _run_backtest(Q1_START,  Q1_END,  best_hp, strategy_cls)

    def _fmt(m):
        if m is None: return 'N/A'
        return (f"CAGR={m['annual_return_pct']:.2f}% Sh={m['sharpe_ratio']:.3f} "
                f"MDD={m['max_drawdown_pct']:.2f}% T={m['total_trades']}")
    print(f'  IS  ({IS_START}→{IS_END}): {_fmt(is_m)}')
    print(f'  OOS ({OOS_START}→{OOS_END}): {_fmt(oos_m)}')
    print(f'  Q1  ({Q1_START}→{Q1_END}): {_fmt(q1_m)}')

    return {
        'variant':      variant,
        'best_hp':      best_hp,
        'best_score':   float(result.fun),
        'is_metrics':   is_m,
        'oos_metrics':  oos_m,
        'q1_metrics':   q1_m,
        'call_log':     call_log,
        'convergence':  [float(v) for v in result.func_vals],
    }


# ── Report generator ──────────────────────────────────────────────────────────

def _metrics_row(label: str, m: dict | None) -> str:
    if m is None:
        return f'| {label} | N/A | N/A | N/A | N/A |'
    return (f"| {label} | {m['annual_return_pct']:+.2f}% "
            f"| {m['sharpe_ratio']:.3f} "
            f"| {m['max_drawdown_pct']:.2f}% "
            f"| {m['total_trades']} |")


def generate_report(results: list[dict]) -> None:
    bnh_path = Path('/result/v3/buy_and_hold/stats.json')
    bnh = json.loads(bnh_path.read_text()) if bnh_path.exists() else {}
    bnh_sharpe = bnh.get('sharpe_ratio', 0)
    bnh_cagr   = bnh.get('annual_return_pct', 0)

    v3_bidi = json.loads(Path('/result/v3/batch_3/trendtype/bidirectional/stats.json').read_text())
    v3_long = json.loads(Path('/result/v3/batch_3/trendtype/long_only/stats.json').read_text())
    v3_def_hp = {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'adx_len': 14,
                 'smooth': 1, 'atr_mult': 3.0}

    lines = [
        '# TrendType skopt 최적화 보고서 (자동 생성)',
        '',
        f'**생성 시각**: {datetime.now(timezone.utc).isoformat()}',
        f'**생성 방식**: `wf_optimize_skopt.py` (LLM 직접 작성 금지)',
        '',
        '## 설정',
        f'- IS (in-sample): {IS_START} → {IS_END}',
        f'- OOS (out-of-sample): {OOS_START} → {OOS_END}',
        f'- 2026 Q1 (paper): {Q1_START} → {Q1_END}',
        f'- n_calls: {N_CALLS} per variant',
        f'- 탐색 공간: atr_len∈[7,21], di_len∈[7,21], atr_mult∈[1.5,5.0]',
        f'- 고정 HP: atr_ma_len=20, smooth=1, adx_len=di_len',
        f'- 목적 함수: -(Sharpe × clamp(trades/30, 0, 1)) | MDD<-30% → 패널티',
        '',
        '## 기준선 (Default HP, 전체기간)',
        '| 항목 | 값 |',
        '|------|-----|',
        f'| BnH CAGR | {bnh_cagr:.2f}% |',
        f'| BnH Sharpe | {bnh_sharpe:.4f} |',
        f'| V3 bidi (default HP) CAGR | {v3_bidi["annual_return_pct"]:.2f}% |',
        f'| V3 bidi (default HP) Sharpe | {v3_bidi["sharpe_ratio"]:.4f} |',
        f'| V3 long (default HP) CAGR | {v3_long["annual_return_pct"]:.2f}% |',
        f'| V3 long (default HP) Sharpe | {v3_long["sharpe_ratio"]:.4f} |',
        f'| Default HP | {json.dumps(v3_def_hp)} |',
        '',
    ]

    for r in results:
        variant  = r['variant']
        best_hp  = r['best_hp']
        is_m     = r['is_metrics']
        oos_m    = r['oos_metrics']
        q1_m     = r['q1_metrics']

        # OOS degradation
        is_sh  = is_m['sharpe_ratio']  if is_m  else 0
        oos_sh = oos_m['sharpe_ratio'] if oos_m else 0
        degradation = (is_sh - oos_sh) / max(abs(is_sh), 0.001) * 100

        # OOS checks
        oos_pass = (oos_m is not None and
                    oos_m['sharpe_ratio'] >= 0.5 and
                    oos_m['annual_return_pct'] > 0 and
                    oos_m['max_drawdown_pct'] >= -50 and
                    degradation < 30)

        # Compare optimized vs default HP on IS
        v3_ref = v3_bidi if variant == 'bidirectional' else v3_long
        hp_gain = ((is_m['sharpe_ratio'] - v3_ref['sharpe_ratio'])
                   / max(abs(v3_ref['sharpe_ratio']), 0.001) * 100) if is_m else 0

        lines += [
            f'## {variant.upper()}',
            '',
            f'**최적 HP**: `{json.dumps(best_hp)}`',
            '',
            '### IS / OOS / Q1 성과',
            '| 구간 | CAGR | Sharpe | MDD | 거래 |',
            '|------|------|--------|-----|------|',
            _metrics_row(f'IS  {IS_START}→{IS_END}',  is_m),
            _metrics_row(f'OOS {OOS_START}→{OOS_END}', oos_m),
            _metrics_row(f'Q1  {Q1_START}→{Q1_END}',  q1_m),
            '',
            '### 평가',
            f'| 항목 | 결과 |',
            f'|------|------|',
            f'| OOS 통과 | {"✅ YES" if oos_pass else "❌ NO"} |',
            f'| OOS Sharpe 열화 | {degradation:+.1f}% (IS→OOS) |',
            f'| Default HP 대비 IS Sharpe 개선 | {hp_gain:+.1f}% |',
            '',
        ]

        # Top 5 IS calls
        top5 = sorted(r['call_log'], key=lambda x: x['score'])[:5]
        lines += [
            '### IS 최고 성과 상위 5건',
            '| atr_len | di_len | atr_mult | Sharpe | Trades | MDD | score |',
            '|---------|--------|----------|--------|--------|-----|-------|',
        ]
        for c in top5:
            sh_str  = f"{c['sharpe']:.3f}" if c['sharpe'] is not None else 'N/A'
            mdd_str = f"{c['mdd']:.2f}%"   if c['mdd']    is not None else 'N/A'
            lines.append(
                f"| {c['atr_len']} | {c['di_len']} | {c['atr_mult']} "
                f"| {sh_str} | {c['trades'] or 'N/A'} | {mdd_str} | {c['score']:.4f} |"
            )
        lines.append('')

    # Cross-variant summary
    lines += ['## 종합 결론', '']
    for r in results:
        is_m  = r['is_metrics']
        oos_m = r['oos_metrics']
        q1_m  = r['q1_metrics']
        is_sh  = is_m['sharpe_ratio']  if is_m  else 0
        oos_sh = oos_m['sharpe_ratio'] if oos_m else 0
        deg    = (is_sh - oos_sh) / max(abs(is_sh), 0.001) * 100
        oos_ok = (oos_m is not None and
                  oos_sh >= 0.5 and
                  oos_m['annual_return_pct'] > 0 and
                  oos_m['max_drawdown_pct'] >= -50 and
                  deg < 30)
        if oos_ok:
            verdict = '✅ OOS 통과 — Phase 5 후보 적합'
        else:
            verdict = '⚠️ OOS 미통과 — default HP 유지 권고 (열화 과다)'
        q1_str = (f"CAGR {q1_m['annual_return_pct']:+.2f}%, Sharpe {q1_m['sharpe_ratio']:.3f}, "
                  f"Trades {q1_m['total_trades']}" if q1_m else 'N/A')
        lines += [
            f'**{r["variant"]}**: {verdict}',
            f'- 최적 HP: `{json.dumps(r["best_hp"])}`',
            f'- 2026 Q1 paper: {q1_str}',
            '',
        ]

    out = OUT_BASE / 'OPTIMIZATION_REPORT.md'
    out.write_text('\n'.join(lines) + '\n')
    print(f'\nOptimization report: {out}')


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    all_results = []
    for variant in ['bidirectional', 'long_only']:
        res = optimize_variant(variant)
        all_results.append(res)

        # Save intermediate results
        (OUT_BASE / f'best_hp_{variant}.json').write_text(
            json.dumps({'best_hp': res['best_hp'],
                        'best_score': res['best_score'],
                        'is_metrics': res['is_metrics'],
                        'oos_metrics': res['oos_metrics'],
                        'q1_metrics': res['q1_metrics']}, indent=2)
        )
        (OUT_BASE / f'optimization_log_{variant}.json').write_text(
            json.dumps(res['call_log'], indent=2)
        )

    generate_report(all_results)
    print('\nDone.')


if __name__ == '__main__':
    main()
