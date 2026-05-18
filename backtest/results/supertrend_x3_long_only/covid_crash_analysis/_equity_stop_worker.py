#!/usr/bin/env python3
"""
Equity Stop Worker — 특정 combo + variant 서브셋만 실행하는 단일 워커.

LEVERAGE 버그 수정: os.environ은 반드시 Jesse/전략 모듈 import 전에 설정.

사용법:
  python3 _equity_stop_worker.py \
    --combo-id combo_176 \
    --variants no_stop,eq_stop_70 \
    --out /result/.../workers/w3.json
"""
from __future__ import annotations

# ── CRITICAL: STRATEGY_LEVERAGE를 모듈 import 전에 설정 ──────────────────────
import os
os.environ['STRATEGY_LEVERAGE'] = '3'
# ─────────────────────────────────────────────────────────────────────────────

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

JESSE_ROOT = Path(os.environ.get('JESSE_ROOT', '/app'))
DATA_DIR   = Path(os.environ.get('DATA_DIR',  '/data'))

sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(JESSE_ROOT / 'strategies'))

EXCHANGE_NAME  = 'Bybit Perpetual'
SYMBOL         = 'BTC-USDT'
LEVERAGE       = 3
BALANCE        = 10_000.0
FEE            = 0.001
BACKTEST_START = '2019-01-01'
BACKTEST_END   = '2026-04-30'
WARMUP_START   = '2018-06-01'

MAINT_MARGIN = 0.005
LIQ_RATIO    = 1 - (1 / LEVERAGE - MAINT_MARGIN)   # 0.6717

COMBO_PARAMS = {
    'combo_173': {'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
                  'slow_ema_len': 27, 'direction_ema_len': 230, 'atr_mult': 3.2},
    'combo_176': {'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
                  'slow_ema_len': 27, 'direction_ema_len': 250, 'atr_mult': 3.2},
    'combo_164': {'st_factor': 2.4, 'st_period': 8, 'fast_ema_len': 7,
                  'slow_ema_len': 25, 'direction_ema_len': 230, 'atr_mult': 3.2},
}

STOP_PCT_MAP = {
    'eq_stop_70': 0.70,
    'eq_stop_75': 0.75,
    'eq_stop_80': 0.80,
}


# ── 캔들 유틸 ─────────────────────────────────────────────────────────────────

def _load_1h(start: str, end: str) -> np.ndarray:
    import polars as pl
    base     = DATA_DIR / 'ohlcv' / 'BTCUSDT' / '1h'
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp() * 1000)
    frames   = []
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
            .select([pl.col('ts_ms').cast(pl.Float64), pl.col('open').cast(pl.Float64),
                     pl.col('close').cast(pl.Float64), pl.col('high').cast(pl.Float64),
                     pl.col('low').cast(pl.Float64), pl.col('volume').cast(pl.Float64)])
            .sort('ts_ms').unique(subset=['ts_ms'], keep='first').sort('ts_ms'))
    return df.to_numpy()


def _resample_1h(arr: np.ndarray, tf: str) -> np.ndarray:
    n_h   = {'4h': 4, '1D': 24}[tf]
    ms_h  = 3_600_000
    align = n_h * ms_h
    first = int(arr[0, 0])
    off   = first % align
    if off:
        arr = arr[(align - off) // ms_h:]
    n   = (len(arr) // n_h) * n_h
    arr = arr[:n]
    if n == 0:
        return np.empty((0, 6), dtype=np.float64)
    c   = arr.reshape(-1, n_h, 6)
    out = np.empty((len(c), 6), dtype=np.float64)
    out[:, 0] = c[:, 0,  0]; out[:, 1] = c[:, 0,  1]; out[:, 2] = c[:, -1, 2]
    out[:, 3] = c[:, :,  3].max(axis=1); out[:, 4] = c[:, :,  4].min(axis=1)
    out[:, 5] = c[:, :,  5].sum(axis=1)
    return out


def _expand_to_1m(arr: np.ndarray, tf_min: int) -> np.ndarray:
    MIN = 60_000
    out = np.empty((len(arr) * tf_min, 6), dtype=np.float64)
    for i, (ts, op, cl, hi, lo, vol) in enumerate(arr):
        b = i * tf_min
        for m in range(tf_min):
            out[b + m] = [ts + m * MIN, op, cl, hi, lo, vol / tf_min]
    return out


def ms_to_dt(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')


# ── 전략 로딩 ──────────────────────────────────────────────────────────────────

def _load_base():
    path = JESSE_ROOT / 'strategies' / 'external' / 'SupertrendStrategy.py'
    spec = importlib.util.spec_from_file_location('SupertrendStrategy', path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, 'SupertrendStrategy')


def _fixed_hp(base, hp: dict):
    class F(base):
        def hyperparameters(self):
            return [{**h, 'default': hp.get(h['name'], h['default'])}
                    for h in super().hyperparameters()]
    F.__name__ = F.__qualname__ = f'{base.__name__}_Fixed'
    return F


def _equity_stop(base, stop_pct: float):
    _s = stop_pct
    _l = LEVERAGE

    class E(base):
        def update_position(self):
            if self.is_long and self._last_entry > 0:
                if self.price <= self._last_entry * (1 - _s / _l):
                    self.liquidate()
                    return
            super().update_position()

    E.__name__ = E.__qualname__ = f'{base.__name__}_ES{int(stop_pct*100)}'
    return E


# ── Jesse 백테스트 ─────────────────────────────────────────────────────────────

def _run_bt(strategy_cls, main_1m: np.ndarray, warm_1m: np.ndarray) -> tuple[list, dict]:
    from jesse import research
    import jesse.helpers as jh

    config = {'starting_balance': BALANCE, 'fee': FEE, 'type': 'futures',
              'futures_leverage': LEVERAGE, 'futures_leverage_mode': 'isolated',
              'exchange': EXCHANGE_NAME, 'warm_up_candles': len(warm_1m)}
    key    = jh.key(EXCHANGE_NAME, SYMBOL)
    routes = [{'exchange': EXCHANGE_NAME, 'strategy': strategy_cls,
               'symbol': SYMBOL, 'timeframe': '4h'}]
    cd     = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': main_1m}}
    wd     = {key: {'exchange': EXCHANGE_NAME, 'symbol': SYMBOL, 'candles': warm_1m}}

    raw = research.backtest(config=config, routes=routes, data_routes=[],
                            candles=cd, warmup_candles=wd,
                            generate_json=False, generate_csv=False, generate_equity_curve=False)
    trades = [{'opened_at': int(t.get('opened_at') or 0),
               'closed_at': int(t.get('closed_at')  or 0),
               'side':      str(t.get('type', 'long')),
               'entry_price': float(t.get('entry_price') or 0),
               'exit_price':  float(t.get('exit_price')  or 0),
               'pnl':         float(t.get('PNL') or t.get('pnl') or t.get('net_profit') or 0)}
              for t in (raw.get('trades') or [])]
    return trades, raw.get('metrics', {}) or {}


def _metrics(m: dict) -> dict:
    np_pct = float(m.get('net_profit_percentage', 0) or 0)
    start  = datetime.fromisoformat(BACKTEST_START).replace(tzinfo=timezone.utc)
    end    = datetime.fromisoformat(BACKTEST_END).replace(tzinfo=timezone.utc)
    yrs    = (end - start).days / 365.25
    cagr   = ((1 + np_pct / 100) ** (1 / yrs) - 1) * 100 if yrs > 0 and (1 + np_pct / 100) > 0 else 0
    return {
        'cagr_pct':   round(cagr, 2),
        'mdd_pct':    round(float(m.get('max_drawdown', 0) or 0), 2),
        'sharpe':     round(float(m.get('sharpe_ratio', m.get('sharpe', 0)) or 0), 4),
        'trades':     int(m.get('total', 0) or 0),
        'win_rate':   round(float(m.get('win_rate', 0) or 0) * 100, 2),
        'pf':         round(float(m.get('profit_factor', 0) or 0), 4),
        'net_profit': round(np_pct, 2),
    }


# ── 사후 분석: stop 발동 시뮬레이션 ────────────────────────────────────────────

def _sim_stops(trades_no_stop: list, arr_4h: np.ndarray, stop_pct: float) -> list[dict]:
    long_sides = {'long', 'Long', 'buy', 'Buy'}
    events = []
    for t in trades_no_stop:
        if t['side'] not in long_sides or t['entry_price'] <= 0:
            continue
        entry      = t['entry_price']
        stop_p     = entry * (1 - stop_pct / LEVERAGE)
        liq_p      = entry * LIQ_RATIO
        t_start    = t['opened_at']
        t_end      = t['closed_at'] if t['closed_at'] > 0 else int(10**15)
        mask       = (arr_4h[:, 0] >= t_start) & (arr_4h[:, 0] < t_end)
        held       = arr_4h[mask]
        if len(held) == 0:
            continue

        # bar-close ≤ stop_price 인 첫 번째 봉
        breach = None
        for c in held:
            if float(c[2]) <= stop_p:
                breach = c
                break
        if breach is None:
            continue

        stop_exit = float(breach[2])
        liq_reached = any(float(c[4]) <= liq_p for c in held)
        better  = stop_exit > t['exit_price']
        outcome = ('PREVENTED_LIQ' if liq_reached and better
                   else ('HELPED' if better else 'HURT'))

        events.append({
            'trade_entry_dt':    ms_to_dt(t_start),
            'trade_entry_price': round(entry, 2),
            'stop_price':        round(stop_p, 2),
            'liq_price':         round(liq_p, 2),
            'stop_triggered_dt': ms_to_dt(int(breach[0])),
            'stop_exit_price':   round(stop_exit, 2),
            'no_stop_exit_price': round(t['exit_price'], 2),
            'no_stop_pnl':       round(t['pnl'], 2),
            'liq_would_occur':   liq_reached,
            'outcome':           outcome,
            'equity_at_stop':    round((stop_exit - entry) / entry * LEVERAGE * 100, 2),
        })
    return events


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--combo-id',  required=True,
                        choices=list(COMBO_PARAMS.keys()))
    parser.add_argument('--variants',  required=True,
                        help='comma-separated: no_stop,eq_stop_70,eq_stop_75,eq_stop_80')
    parser.add_argument('--out',       required=True, help='output JSON path')
    parser.add_argument('--worker-id', default='?')
    args     = parser.parse_args()

    combo_id = args.combo_id
    variants = [v.strip() for v in args.variants.split(',')]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    hp = COMBO_PARAMS[combo_id]
    print(f'[Worker {args.worker_id}] {combo_id}  variants={variants}')
    print(f'  params={hp}')
    print(f'  STRATEGY_LEVERAGE={os.environ.get("STRATEGY_LEVERAGE")}')

    # 캔들 로드
    print('  [캔들 로드]')
    main_1h = _load_1h(BACKTEST_START, BACKTEST_END)
    warm_1h = _load_1h(WARMUP_START, BACKTEST_START)
    main_4h = _resample_1h(main_1h, '4h')
    warm_4h = _resample_1h(warm_1h, '4h')
    main_1m = _expand_to_1m(main_4h, 240)
    warm_1m = _expand_to_1m(warm_4h, 240)
    print(f'  메인: {len(main_4h):,} 4h봉  warmup: {len(warm_4h):,} 4h봉')

    # 전략 로드 (STRATEGY_LEVERAGE 이미 설정된 후)
    base_cls = _load_base()
    print(f'  SupertrendStrategy.LEVERAGE={getattr(base_cls, "__module__", "?")}')

    from external._long_only_factory import make_long_only

    results      = {}
    no_stop_trades = []

    for vname in variants:
        print(f'  [{vname}] ', end='', flush=True)

        if vname == 'no_stop':
            strategy_cls = make_long_only(_fixed_hp(base_cls, hp))
        else:
            stop_pct     = STOP_PCT_MAP[vname]
            fixed        = _fixed_hp(base_cls, hp)
            es_cls       = _equity_stop(fixed, stop_pct)
            strategy_cls = make_long_only(es_cls)

        try:
            trades, raw_m = _run_bt(strategy_cls, main_1m, warm_1m)
            m             = _metrics(raw_m)
            print(f'CAGR={m["cagr_pct"]:+.1f}%  MDD={m["mdd_pct"]:.1f}%  '
                  f'Sharpe={m["sharpe"]:.3f}  trades={m["trades"]}')
            results[vname] = {'metrics': m, 'trades_count': len(trades)}
            if vname == 'no_stop':
                no_stop_trades = trades
        except Exception as e:
            import traceback
            print(f'ERROR: {e}')
            traceback.print_exc()
            results[vname] = {'metrics': {}, 'error': str(e)}

    # 사후 분석
    print('  [사후 분석]')
    for vname in variants:
        if vname == 'no_stop':
            continue
        stop_pct = STOP_PCT_MAP.get(vname)
        if stop_pct is None:
            continue
        events = _sim_stops(no_stop_trades, main_4h, stop_pct)
        results[vname]['stop_events'] = events
        helped = sum(1 for e in events if e['outcome'] in ('HELPED', 'PREVENTED_LIQ'))
        hurt   = sum(1 for e in events if e['outcome'] == 'HURT')
        prev   = sum(1 for e in events if e['outcome'] == 'PREVENTED_LIQ')
        print(f'  [{vname}] 발동={len(events)}건  '
              f'HELPED={helped}  HURT={hurt}  LIQ방지={prev}')

    # 결과 저장
    output = {
        'worker_id':    args.worker_id,
        'combo_id':     combo_id,
        'variants':     variants,
        'combo_params': hp,
        'results':      results,
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f'  → {out_path}')


if __name__ == '__main__':
    main()
