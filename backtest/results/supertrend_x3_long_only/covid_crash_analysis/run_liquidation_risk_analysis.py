#!/usr/bin/env python3
"""
Script 1: Liquidation Risk Analysis — SupertrendStrategy 4h 3x Long-Only

목적:
  전체 백테스트 구간(2019-2026)에서 전략이 보유한 모든 롱 포지션에 대해
  "해당 포지션 보유 중 어떤 4h 봉이든 candle.low ≤ liq_price" 조건이
  단 1건이라도 존재하는지 전수 검사한다.

  ※ 단순 -34% 봉 탐지만으로는 누락 케이스 발생:
     - Case B: 이미 -10% 손실 중 → 추가 -25%면 liq 도달
     - Case C: 이미 -25% 손실 중 → 추가 -11%면 liq 도달

3x isolated liq 조건:
  liq_price = entry × (1 − 1/3 + 0.005) = entry × 0.6717

대상 콤보 (v7 top-3):
  combo_173: st_factor=2.4, st_period=8, fast_ema=7, slow_ema=27, dir_ema=230, atr_mult=3.2
  combo_176: st_factor=2.4, st_period=8, fast_ema=7, slow_ema=27, dir_ema=250, atr_mult=3.2  (최고)
  combo_164: st_factor=2.4, st_period=8, fast_ema=7, slow_ema=25, dir_ema=230, atr_mult=3.2

실행 (Docker):
  DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
  $DC python3 /result/supertrend_x3_long_only/covid_crash_analysis/run_liquidation_risk_analysis.py

출력:
  liquidation_risk.json
  liquidation_risk_report.md
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# ── 경로 설정 ────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
JESSE_ROOT = Path(os.environ.get('JESSE_ROOT', '/app'))
DATA_DIR   = Path(os.environ.get('DATA_DIR',  '/data'))
RESULT_DIR = SCRIPT_DIR

sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(JESSE_ROOT / 'strategies'))

EXCHANGE_NAME  = 'Bybit Perpetual'
SYMBOL         = 'BTC-USDT'
LEVERAGE       = 3
BALANCE        = 10_000.0
FEE            = 0.001
BACKTEST_START = '2019-01-01'
BACKTEST_END   = '2026-04-30'
WARMUP_START   = '2018-06-01'    # dir_ema 250 × 4h 충분 (~7개월)

# 3x isolated, Bybit Perp 0.5% maintenance
MAINT_MARGIN = 0.005
LIQ_RATIO    = 1 - (1 / LEVERAGE - MAINT_MARGIN)  # = 0.6717

_4H_MS = 4 * 3_600_000

COMBOS = [
    {'id': 'combo_173', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 27, 'direction_ema_len': 230, 'atr_mult': 3.2},
    {'id': 'combo_176', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 27, 'direction_ema_len': 250, 'atr_mult': 3.2},
    {'id': 'combo_164', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 25, 'direction_ema_len': 230, 'atr_mult': 3.2},
]


# ── 캔들 유틸 (run_external_backtest.py 와 동일 로직) ─────────────────────────

def _load_1h(start: str, end: str) -> np.ndarray:
    import polars as pl
    base     = DATA_DIR / 'ohlcv' / 'BTCUSDT' / '1h'
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)

    frames = []
    for yr_dir in sorted(base.iterdir()):
        if not yr_dir.is_dir():
            continue
        try:
            yr = int(yr_dir.name)
        except ValueError:
            continue
        if yr < start_dt.year - 1 or yr > end_dt.year + 1:
            continue
        for f in sorted(yr_dir.glob('*.parquet')):
            lf   = pl.scan_parquet(f)
            keys = list(lf.schema.keys())
            if 'timestamp' in keys:
                lf = lf.with_columns(pl.col('timestamp').dt.epoch('ms').alias('ts_ms'))
            elif 'open_time' in keys:
                lf = lf.with_columns(pl.col('open_time').dt.epoch('ms').alias('ts_ms'))
            else:
                continue
            frames.append(lf.select(['ts_ms', 'open', 'high', 'low', 'close', 'volume']))

    df = (pl.concat(frames).collect()
            .filter((pl.col('ts_ms') >= start_ms) & (pl.col('ts_ms') < end_ms))
            .select([
                pl.col('ts_ms').cast(pl.Float64),
                pl.col('open').cast(pl.Float64),
                pl.col('close').cast(pl.Float64),
                pl.col('high').cast(pl.Float64),
                pl.col('low').cast(pl.Float64),
                pl.col('volume').cast(pl.Float64),
            ])
            .sort('ts_ms')
            .unique(subset=['ts_ms'], keep='first').sort('ts_ms'))

    arr = df.to_numpy()
    print(f'  [캔들] {len(arr):,} 1h ({start} → {end})')
    return arr


def _resample_1h(arr_1h: np.ndarray, tf: str) -> np.ndarray:
    n_hours  = {'4h': 4, '1D': 24}[tf]
    ms_per_h = 3_600_000
    align    = n_hours * ms_per_h

    first    = int(arr_1h[0, 0])
    offset   = first % align
    if offset:
        arr_1h = arr_1h[(align - offset) // ms_per_h:]

    n      = (len(arr_1h) // n_hours) * n_hours
    arr_1h = arr_1h[:n]
    if n == 0:
        return np.empty((0, 6), dtype=np.float64)

    c   = arr_1h.reshape(-1, n_hours, 6)
    out = np.empty((len(c), 6), dtype=np.float64)
    out[:, 0] = c[:, 0,  0]
    out[:, 1] = c[:, 0,  1]
    out[:, 2] = c[:, -1, 2]
    out[:, 3] = c[:, :,  3].max(axis=1)
    out[:, 4] = c[:, :,  4].min(axis=1)
    out[:, 5] = c[:, :,  5].sum(axis=1)
    return out


def _expand_tf_to_1m(arr: np.ndarray, tf_minutes: int) -> np.ndarray:
    MIN_MS = 60_000
    n      = len(arr)
    out    = np.empty((n * tf_minutes, 6), dtype=np.float64)
    for i, (ts, op, cl, hi, lo, vol) in enumerate(arr):
        base = i * tf_minutes
        for m in range(tf_minutes):
            out[base + m] = [ts + m * MIN_MS, op, cl, hi, lo, vol / tf_minutes]
    return out


# ── 전략 로딩 ──────────────────────────────────────────────────────────────────

def _load_base_strategy():
    mod_path = JESSE_ROOT / 'strategies' / 'external' / 'SupertrendStrategy.py'
    spec     = importlib.util.spec_from_file_location('SupertrendStrategy', mod_path)
    mod      = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, 'SupertrendStrategy')


def _make_fixed_hp(base_cls, hp_values: dict):
    """hyperparameters() default 값을 hp_values로 고정한 서브클래스."""
    class FixedHP(base_cls):
        def hyperparameters(self):
            return [{**h, 'default': hp_values.get(h['name'], h['default'])}
                    for h in super().hyperparameters()]
    FixedHP.__name__     = f'{base_cls.__name__}_Fixed'
    FixedHP.__qualname__ = FixedHP.__name__
    return FixedHP


# ── Jesse 백테스트 실행 ────────────────────────────────────────────────────────

def run_backtest(combo: dict, main_1m: np.ndarray, warm_1m: np.ndarray) -> tuple[list, dict]:
    from jesse import research
    import jesse.helpers as jh

    os.environ['STRATEGY_LEVERAGE'] = str(LEVERAGE)
    base_cls     = _load_base_strategy()
    fixed_cls    = _make_fixed_hp(base_cls, combo)

    from external._long_only_factory import make_long_only
    strategy_cls = make_long_only(fixed_cls)

    config = {
        'starting_balance':      BALANCE,
        'fee':                   FEE,
        'type':                  'futures',
        'futures_leverage':      LEVERAGE,
        'futures_leverage_mode': 'isolated',
        'exchange':              EXCHANGE_NAME,
        'warm_up_candles':       len(warm_1m),
    }
    key          = jh.key(EXCHANGE_NAME, SYMBOL)
    routes       = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
                     'symbol': SYMBOL, 'timeframe': '4h'}]
    candles_dict = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': main_1m}}
    warmup_dict  = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warm_1m}}

    raw  = research.backtest(config=config, routes=routes, data_routes=[],
                             candles=candles_dict, warmup_candles=warmup_dict,
                             generate_json=False, generate_csv=False, generate_equity_curve=False)

    trades  = raw.get('trades', []) or []
    metrics = raw.get('metrics', {}) or {}

    result = [{'opened_at':   int(t.get('opened_at') or 0),
               'closed_at':   int(t.get('closed_at')  or 0),
               'side':        str(t.get('type', 'long')),
               'entry_price': float(t.get('entry_price') or 0),
               'exit_price':  float(t.get('exit_price')  or 0),
               'pnl':         float(t.get('PNL') or t.get('pnl') or t.get('net_profit') or 0)}
              for t in trades]

    print(f'    trades={len(result)}, '
          f'MDD={float(metrics.get("max_drawdown", 0) or 0):.1f}%, '
          f'CAGR≈{_approx_cagr(metrics):.1f}%')
    return result, metrics


def _approx_cagr(m: dict) -> float:
    np_pct = float(m.get('net_profit_percentage', 0) or 0)
    start  = datetime.fromisoformat(BACKTEST_START).replace(tzinfo=timezone.utc)
    end    = datetime.fromisoformat(BACKTEST_END).replace(tzinfo=timezone.utc)
    years  = (end - start).days / 365.25
    if years <= 0 or (1 + np_pct / 100) <= 0:
        return 0.0
    return ((1 + np_pct / 100) ** (1 / years) - 1) * 100


# ── 핵심: 전수 liq 위험 스캔 ──────────────────────────────────────────────────

def scan_all_liq_risks(trades: list, arr_4h: np.ndarray) -> list[dict]:
    """
    모든 long trade의 보유 기간 중 4h 봉 전수 검사.
    candle.low <= liq_price 인 (trade, candle) 쌍을 모두 반환.

    각 이벤트에 포함되는 핵심 정보:
    - equity_at_candle_open: 봉 시작 시점 equity 손실 % (음수 = 이미 손실 중)
    - additional_drop_needed: liq_price까지 도달하려면 봉 시작 가격 대비 추가 낙폭 %
      (0에 가까울수록 위험 → 봉이 이미 liq 영역에서 시작함을 의미)
    """
    ts_col, open_col, close_col, high_col, low_col = 0, 1, 2, 3, 4
    long_sides = {'long', 'Long', 'buy', 'Buy'}
    risks = []

    for trade in trades:
        if trade['side'] not in long_sides:
            continue
        entry   = trade['entry_price']
        if entry <= 0:
            continue
        liq_p   = entry * LIQ_RATIO
        t_start = trade['opened_at']
        t_end   = trade['closed_at'] if trade['closed_at'] > 0 else int(10**15)

        # 보유 기간 4h 봉 필터 (numpy boolean mask)
        mask = (arr_4h[:, ts_col] >= t_start) & (arr_4h[:, ts_col] < t_end)
        held = arr_4h[mask]

        for c in held:
            c_ts    = int(c[ts_col])
            c_open  = float(c[open_col])
            c_low   = float(c[low_col])
            c_close = float(c[close_col])

            if c_low > liq_p:
                continue  # 해당 봉은 안전

            # ── liq 위험 이벤트 ──
            # 봉 시작 시점의 포지션 equity (기준: 진입가 대비)
            eq_at_open    = (c_open - entry) / entry * LEVERAGE * 100
            # 봉 시작 가격(c_open) 기준으로 liq_price 도달에 추가로 필요한 낙폭
            add_drop      = (liq_p - c_open) / c_open * 100  # 음수

            # Case 분류: 봉 시작 가격이 entry 위/아래인지
            if c_open >= entry:
                case = 'ENTRY_LEVEL'    # 진입 직후 급락
            elif eq_at_open > -50:
                case = 'MODERATE_LOSS'  # 손실 중이지만 moderate (-50% equity 이내)
            else:
                case = 'DEEP_LOSS'      # 심각한 손실 중 (-50% equity 이하)

            risks.append({
                'case':                  case,
                'trade_entry_price':     round(entry, 2),
                'trade_entry_dt':        ms_to_dt(t_start),
                'trade_exit_dt':         ms_to_dt(t_end) if t_end < 10**14 else 'open',
                'liq_price':             round(liq_p, 2),
                'candle_datetime':       ms_to_dt(c_ts),
                'candle_open':           round(c_open, 2),
                'candle_low':            round(c_low, 2),
                'candle_close':          round(c_close, 2),
                'intra_drop_pct':        round((c_low - c_open) / c_open * 100, 2),
                'equity_at_candle_open': round(eq_at_open, 2),
                'additional_drop_needed': round(add_drop, 2),
                'trade_pnl':             round(trade['pnl'], 2),
            })

    return risks


def ms_to_dt(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')


# ── 출력 생성 ──────────────────────────────────────────────────────────────────

def write_json(data: dict, path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f'  → {path.name}')


def write_risk_report(results: dict, path: Path):
    lines = [
        '# Liquidation Risk Analysis — SupertrendStrategy 4h 3x Long-Only',
        '',
        f'**분석 기간**: {BACKTEST_START} ~ {BACKTEST_END}',
        f'**Liq 임계**: 진입가 대비 −{LIQ_RATIO*100:.2f}%  '
        f'(liq_price = entry × {1 - LIQ_RATIO:.4f}에서 = entry × {LIQ_RATIO:.4f})',
        f'**레버리지**: {LEVERAGE}x isolated (Bybit Perp, maint 0.5%)',
        '',
        '> **검사 방법**: 각 long 포지션의 보유 기간 중 모든 4h 봉에 대해',
        '> `candle.low ≤ liq_price` 조건을 전수 검사 (단순 -34% 봉 탐지 아님)',
        '',
        '---',
        '',
    ]

    for cid, data in results.items():
        total_trades = data['total_trades']
        risks        = data['risks']
        verdict      = data['verdict']
        emoji        = '✅' if verdict == 'ZERO_RISK' else '❌'

        lines += [
            f'## {cid}  {emoji} `{verdict}`',
            '',
            f'- 전체 long trades: {total_trades}건',
            f'- Liq 위험 이벤트: **{len(risks)}건** (단 1건이라도 있으면 HAS_RISK)',
            '',
        ]

        if not risks:
            lines += [
                '**분석 결과: 전 구간에서 liq_price에 도달한 캔들 없음 → 완전 안전**',
                '',
            ]
        else:
            lines += [
                '| # | 캔들 일시 | 케이스 | 봉 시작 equity | liq까지 추가 낙폭 | 봉 open | 봉 low | liq_price |',
                '|---|---------|--------|----------------|-----------------|---------|--------|-----------|',
            ]
            for i, r in enumerate(risks, 1):
                lines.append(
                    f'| {i} | {r["candle_datetime"]} | {r["case"]} | '
                    f'{r["equity_at_candle_open"]:+.1f}% | '
                    f'{r["additional_drop_needed"]:.1f}% | '
                    f'${r["candle_open"]:,.0f} | ${r["candle_low"]:,.0f} | '
                    f'${r["liq_price"]:,.0f} |'
                )
            lines += [
                '',
                '**케이스 설명**:',
                '- `ENTRY_LEVEL`: 진입 직후 봉에서 liq 수준까지 급락',
                '- `MODERATE_LOSS`: 이미 equity -50% 이내 손실 중 추가 하락으로 liq 도달',
                '- `DEEP_LOSS`: 이미 equity -50% 이상 손실 중 소폭 하락만으로 liq 도달',
                '',
            ]

        lines.append('')

    lines += [
        '---',
        '',
        '## 종합 결론',
        '',
    ]

    has_risk = any(d['verdict'] == 'HAS_RISK' for d in results.values())
    if not has_risk:
        lines += [
            '**모든 콤보에서 ZERO_RISK — 전 구간 어떤 포지션에서도 liq 위험 없음**',
            '',
            '→ equity stop 안전장치 없이도 이론적으로 청산 위험이 없음.',
            '  (단, Jesse 시뮬레이션은 bar-close 기준이므로 실거래 intrabar 위험은 별도 확인 필요)',
        ]
    else:
        lines += [
            '**HAS_RISK 콤보 존재 — equity stop 안전장치 필요**',
            '',
            '→ `run_equity_stop_backtest.py` 를 실행하여 -70/-75/-80% equity stop 효과를 분석하세요.',
        ]

    lines += ['', f'*생성: {datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}*', '']

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'  → {path.name}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print('=' * 65)
    print('Liquidation Risk Analysis — SupertrendStrategy 4h 3x Long-Only')
    print('=' * 65)

    # 1. 4h 캔들 준비
    print(f'\n[1] 캔들 로드')
    main_1h = _load_1h(BACKTEST_START, BACKTEST_END)
    warm_1h = _load_1h(WARMUP_START, BACKTEST_START)
    main_4h = _resample_1h(main_1h, '4h')
    warm_4h = _resample_1h(warm_1h, '4h')
    print(f'  메인: {len(main_4h):,} 4h봉  warmup: {len(warm_4h):,} 4h봉')

    main_1m = _expand_tf_to_1m(main_4h, 240)
    warm_1m = _expand_tf_to_1m(warm_4h, 240)

    # 2. 각 combo 백테스트 + 전수 liq 스캔
    print('\n[2] combo별 백테스트 + Liq 위험 전수 스캔')
    all_results = {}

    for combo in COMBOS:
        cid = combo['id']
        hp  = {k: v for k, v in combo.items() if k != 'id'}
        print(f'\n  [{cid}] params={hp}')
        try:
            trades, metrics = run_backtest(hp, main_1m, warm_1m)
        except Exception as e:
            import traceback
            print(f'  [ERROR] {e}')
            traceback.print_exc()
            all_results[cid] = {'total_trades': 0, 'risks': [], 'verdict': 'ERROR',
                                 'metrics': {}, 'error': str(e)}
            continue

        risks   = scan_all_liq_risks(trades, main_4h)
        verdict = 'ZERO_RISK' if not risks else 'HAS_RISK'

        print(f'  → liq 위험 이벤트: {len(risks)}건  판정: {verdict}')
        for r in risks:
            print(f'    {r["candle_datetime"]}  case={r["case"]}  '
                  f'equity_at_open={r["equity_at_candle_open"]:+.1f}%  '
                  f'add_drop={r["additional_drop_needed"]:.1f}%  '
                  f'low=${r["candle_low"]:,.0f} liq=${r["liq_price"]:,.0f}')

        all_results[cid] = {
            'total_trades': len(trades),
            'risks':        risks,
            'verdict':      verdict,
            'metrics':      {
                'mdd_pct':    float(metrics.get('max_drawdown', 0) or 0),
                'approx_cagr': round(_approx_cagr(metrics), 2),
                'total':      int(metrics.get('total', 0) or 0),
                'sharpe':     float(metrics.get('sharpe_ratio', metrics.get('sharpe', 0)) or 0),
            },
        }

    # 3. 출력 저장
    print('\n[3] 결과 저장')
    output = {
        'liq_ratio':      LIQ_RATIO,
        'leverage':       LEVERAGE,
        'maint_margin':   MAINT_MARGIN,
        'analysis_period': f'{BACKTEST_START} ~ {BACKTEST_END}',
        'generated_at':   datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'results':        all_results,
    }
    write_json(output, RESULT_DIR / 'liquidation_risk.json')
    write_risk_report(all_results, RESULT_DIR / 'liquidation_risk_report.md')

    # 최종 요약
    print('\n[결론]')
    for cid, data in all_results.items():
        verdict = data.get('verdict', 'ERROR')
        n_risks = len(data.get('risks', []))
        print(f'  {cid}: {verdict}  (liq 위험 {n_risks}건)')


if __name__ == '__main__':
    main()
