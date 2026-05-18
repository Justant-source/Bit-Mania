#!/usr/bin/env python3
"""
Script 2: Equity Stop Backtest — SupertrendStrategy 4h 3x Long-Only

목적:
  포지션 진입 즉시 equity 손실이 임계값(-70/-75/-80%)에 도달하면
  강제 시장가 청산하는 로직을 추가했을 때 성과 변화를 측정한다.

  Equity stop 가격 (진입가 대비):
    -70% equity at 3x → stop_price = entry × (1 − 0.70/3) = entry × 0.7667
    -75% equity at 3x → stop_price = entry × (1 − 0.75/3) = entry × 0.7500
    -80% equity at 3x → stop_price = entry × (1 − 0.80/3) = entry × 0.7333
    liq (참고)         → stop_price = entry × (1 − 0.3283) = entry × 0.6717

  ※ Jesse 시뮬레이션은 bar-close 기준이므로:
    - equity stop은 bar-close ≤ stop_price 일 때 발동
    - intrabar spike는 포착 안 됨 (실거래 위험과의 차이점)

사후 분석 (post-analysis):
  no_stop 백테스트의 trade 내역을 기반으로 각 equity stop 임계값별
  "처음으로 bar-close ≤ stop_price 가 되는 캔들"을 찾아
  - 그 이후 가격이 계속 하락 → HELPED (stop이 더 큰 손실을 막음)
  - 그 이후 가격이 반등    → HURT (stop이 없었으면 더 좋은 결과)
  를 판정한다.

대상 콤보: v7 top-3 (173 / 176 / 164)
실험 변형: no_stop / eq_stop_70 / eq_stop_75 / eq_stop_80
총 12회 Jesse 백테스트

실행 (Docker):
  DC="docker compose -f backtest/docker/docker-compose.yml --profile backtest run --rm backtester"
  $DC python3 /result/supertrend_x3_long_only/covid_crash_analysis/run_equity_stop_backtest.py

출력:
  equity_stop_results.json
  equity_stop_report.md
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
WARMUP_START   = '2018-06-01'

MAINT_MARGIN = 0.005
LIQ_RATIO    = 1 - (1 / LEVERAGE - MAINT_MARGIN)   # 0.6717

_4H_MS = 4 * 3_600_000

# equity stop 임계값 (equity 손실 %)
STOP_PCTS = [0.70, 0.75, 0.80]   # -70%, -75%, -80%

COMBOS = [
    {'id': 'combo_173', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 27, 'direction_ema_len': 230, 'atr_mult': 3.2},
    {'id': 'combo_176', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 27, 'direction_ema_len': 250, 'atr_mult': 3.2},
    {'id': 'combo_164', 'st_factor': 2.4, 'st_period': 8,
     'fast_ema_len': 7, 'slow_ema_len': 25, 'direction_ema_len': 230, 'atr_mult': 3.2},
]


# ── 캔들 유틸 ─────────────────────────────────────────────────────────────────

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
            .select([pl.col('ts_ms').cast(pl.Float64), pl.col('open').cast(pl.Float64),
                     pl.col('close').cast(pl.Float64), pl.col('high').cast(pl.Float64),
                     pl.col('low').cast(pl.Float64), pl.col('volume').cast(pl.Float64)])
            .sort('ts_ms')
            .unique(subset=['ts_ms'], keep='first').sort('ts_ms'))

    arr = df.to_numpy()
    print(f'  [캔들] {len(arr):,} 1h ({start} → {end})')
    return arr


def _resample_1h(arr_1h: np.ndarray, tf: str) -> np.ndarray:
    n_hours = {'4h': 4, '1D': 24}[tf]
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
    out[:, 0] = c[:, 0, 0]; out[:, 1] = c[:, 0, 1]; out[:, 2] = c[:, -1, 2]
    out[:, 3] = c[:, :, 3].max(axis=1); out[:, 4] = c[:, :, 4].min(axis=1)
    out[:, 5] = c[:, :, 5].sum(axis=1)
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


def ms_to_dt(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')


# ── 전략 로딩 + Equity Stop 팩토리 ────────────────────────────────────────────

def _load_base_strategy():
    mod_path = JESSE_ROOT / 'strategies' / 'external' / 'SupertrendStrategy.py'
    spec     = importlib.util.spec_from_file_location('SupertrendStrategy', mod_path)
    mod      = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, 'SupertrendStrategy')


def _make_fixed_hp(base_cls, hp_values: dict):
    class FixedHP(base_cls):
        def hyperparameters(self):
            return [{**h, 'default': hp_values.get(h['name'], h['default'])}
                    for h in super().hyperparameters()]
    FixedHP.__name__ = FixedHP.__qualname__ = f'{base_cls.__name__}_Fixed'
    return FixedHP


def make_equity_stop_strategy(base_cls, stop_pct: float, leverage: int = 3):
    """
    stop_pct: equity 손실 임계 (0.70=−70%, 0.75=−75%, 0.80=−80%)
    stop_price_long = entry × (1 − stop_pct / leverage)

    Jesse의 update_position()은 bar-close 기준으로 실행되므로
    bar-close ≤ stop_price 인 경우에만 발동된다.
    intrabar spike는 포착 안 됨 (실거래와의 차이점).
    """
    _stop = stop_pct
    _lev  = leverage

    class EquityStopVariant(base_cls):
        def update_position(self):
            if self.is_long and self._last_entry > 0:
                stop_price = self._last_entry * (1 - _stop / _lev)
                if self.price <= stop_price:
                    self.liquidate()
                    return
            super().update_position()

    tag = f'EqStop{int(stop_pct * 100)}'
    EquityStopVariant.__name__     = f'{base_cls.__name__}_{tag}'
    EquityStopVariant.__qualname__ = EquityStopVariant.__name__
    return EquityStopVariant


# ── Jesse 백테스트 ─────────────────────────────────────────────────────────────

def run_backtest(strategy_cls, main_1m: np.ndarray, warm_1m: np.ndarray) -> tuple[list, dict]:
    from jesse import research
    import jesse.helpers as jh

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

    raw     = research.backtest(config=config, routes=routes, data_routes=[],
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
    return result, metrics


def _extract_metrics(metrics: dict) -> dict:
    mdd      = float(metrics.get('max_drawdown', 0) or 0)
    np_pct   = float(metrics.get('net_profit_percentage', 0) or 0)
    sharpe   = float(metrics.get('sharpe_ratio', metrics.get('sharpe', 0)) or 0)
    total    = int(metrics.get('total', 0) or 0)
    win_rate = float(metrics.get('win_rate', 0) or 0) * 100
    pf       = float(metrics.get('profit_factor', 0) or 0)

    start_dt = datetime.fromisoformat(BACKTEST_START).replace(tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(BACKTEST_END).replace(tzinfo=timezone.utc)
    years    = (end_dt - start_dt).days / 365.25
    cagr     = ((1 + np_pct / 100) ** (1 / years) - 1) * 100 if years > 0 and (1 + np_pct / 100) > 0 else 0.0

    return {
        'cagr_pct':   round(cagr, 2),
        'mdd_pct':    round(mdd, 2),
        'sharpe':     round(sharpe, 4),
        'trades':     total,
        'win_rate':   round(win_rate, 2),
        'pf':         round(pf, 4),
        'net_profit': round(np_pct, 2),
    }


# ── 사후 분석: equity stop 발동 시뮬레이션 ────────────────────────────────────

def simulate_stop_events(trades_no_stop: list, arr_4h: np.ndarray,
                         stop_pct: float, leverage: int = 3) -> list[dict]:
    """
    no_stop 백테스트의 trade 목록을 기반으로,
    만약 equity stop이 있었다면 발동됐을 캔들과 그 결과를 시뮬레이션.

    방법:
      1. 각 long trade의 보유 기간 4h 봉 순회
      2. bar-close ≤ stop_price 인 첫 번째 봉 = stop 발동 봉
      3. 발동 후: 실제 exit 가격(bar-close) vs no_stop 실제 exit 가격 비교
         → HELPED: stop 발동 이후 가격이 더 하락 (stop이 더 큰 손실 막음)
         → HURT:   stop 발동 이후 가격이 반등   (stop이 없었으면 더 좋은 결과)
         → PREVENTED_LIQ: stop 없었다면 liq 도달했을 케이스
    """
    long_sides = {'long', 'Long', 'buy', 'Buy'}
    liq_ratio  = 1 - (1 / leverage - MAINT_MARGIN)
    events = []

    for trade in trades_no_stop:
        if trade['side'] not in long_sides or trade['entry_price'] <= 0:
            continue
        entry      = trade['entry_price']
        stop_price = entry * (1 - stop_pct / leverage)
        liq_price  = entry * liq_ratio
        t_start    = trade['opened_at']
        t_end      = trade['closed_at'] if trade['closed_at'] > 0 else int(10**15)

        mask = (arr_4h[:, 0] >= t_start) & (arr_4h[:, 0] < t_end)
        held = arr_4h[mask]
        if len(held) == 0:
            continue

        # bar-close ≤ stop_price 인 첫 번째 봉 찾기
        breach_idx = None
        for idx, c in enumerate(held):
            if float(c[2]) <= stop_price:   # close ≤ stop_price
                breach_idx = idx
                break

        if breach_idx is None:
            continue  # 이 trade에서는 stop 발동 안 됨

        breach_candle  = held[breach_idx]
        stop_exit_dt   = ms_to_dt(int(breach_candle[0]))
        stop_exit_price = float(breach_candle[2])   # bar-close 가격으로 exit

        # no_stop 실제 exit 가격
        no_stop_exit = trade['exit_price']
        no_stop_pnl  = trade['pnl']

        # 비교: stop exit vs no_stop exit
        stop_pnl_approx = (stop_exit_price - entry) / entry * leverage * BALANCE * 0.95
        better = stop_exit_price > no_stop_exit   # True=stop이 더 비싸게 팜 → HELPED

        # liq 도달 여부 (candle 중 low ≤ liq_price 인 것이 있었는지)
        liq_reached = any(float(c[4]) <= liq_price for c in held)

        outcome = 'PREVENTED_LIQ' if liq_reached and better else ('HELPED' if better else 'HURT')

        events.append({
            'trade_entry_dt':    ms_to_dt(t_start),
            'trade_entry_price': round(entry, 2),
            'stop_price':        round(stop_price, 2),
            'liq_price':         round(liq_price, 2),
            'stop_triggered_dt': stop_exit_dt,
            'stop_exit_price':   round(stop_exit_price, 2),
            'no_stop_exit_price': round(no_stop_exit, 2),
            'no_stop_pnl':       round(no_stop_pnl, 2),
            'liq_would_occur':   liq_reached,
            'outcome':           outcome,
            'equity_at_stop':    round((stop_exit_price - entry) / entry * leverage * 100, 2),
        })

    return events


# ── 출력 ──────────────────────────────────────────────────────────────────────

def write_json(data: dict, path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f'  → {path.name}')


def write_stop_report(results: dict, path: Path):
    stop_labels = {
        'no_stop':    '원본 (no stop)',
        'eq_stop_70': 'Equity Stop −70%',
        'eq_stop_75': 'Equity Stop −75%',
        'eq_stop_80': 'Equity Stop −80%',
    }
    stop_prices = {
        'eq_stop_70': f'entry × {1 - 0.70/LEVERAGE:.4f}',
        'eq_stop_75': f'entry × {1 - 0.75/LEVERAGE:.4f}',
        'eq_stop_80': f'entry × {1 - 0.80/LEVERAGE:.4f}',
    }

    lines = [
        '# Equity Stop Backtest Report — SupertrendStrategy 4h 3x Long-Only',
        '',
        f'**분석 기간**: {BACKTEST_START} ~ {BACKTEST_END}',
        f'**레버리지**: {LEVERAGE}x isolated',
        f'**liq 가격**: entry × {LIQ_RATIO:.4f} (−{LIQ_RATIO*100:.2f}% 진입가 대비)',
        '',
        '| Variant | Stop 가격 기준 | 설명 |',
        '|---------|--------------|------|',
        f'| no_stop    | −          | 기존 전략 (ATR stop + EMA exit) |',
        f'| eq_stop_70 | {stop_prices["eq_stop_70"]} | equity −70% 시 강제청산 |',
        f'| eq_stop_75 | {stop_prices["eq_stop_75"]} | equity −75% 시 강제청산 |',
        f'| eq_stop_80 | {stop_prices["eq_stop_80"]} | equity −80% 시 강제청산 |',
        '',
        '> ※ Jesse 시뮬레이션은 bar-close 기준. intrabar stop 미포착.',
        '',
        '---',
        '',
    ]

    for cid, combo_data in results.items():
        lines += [f'## {cid}', '']

        # 지표 비교 테이블
        lines += [
            '### 지표 비교',
            '',
            '| Variant | CAGR | MDD | Sharpe | Trades | Stops 발동 |',
            '|---------|------|-----|--------|--------|-----------|',
        ]
        for vname in ['no_stop', 'eq_stop_70', 'eq_stop_75', 'eq_stop_80']:
            vdata = combo_data.get(vname, {})
            m     = vdata.get('metrics', {})
            n_ev  = len(vdata.get('stop_events', []))
            stops = f'{n_ev}건' if vname != 'no_stop' else '—'
            lines.append(
                f'| {stop_labels[vname]} | {m.get("cagr_pct", 0):+.1f}% | '
                f'{m.get("mdd_pct", 0):.1f}% | {m.get("sharpe", 0):.3f} | '
                f'{m.get("trades", 0)} | {stops} |'
            )

        lines += ['']

        # Stop 발동 이벤트 상세
        for vname in ['eq_stop_70', 'eq_stop_75', 'eq_stop_80']:
            events = combo_data.get(vname, {}).get('stop_events', [])
            if not events:
                lines += [f'### {stop_labels[vname]}: stop 발동 없음', '']
                continue

            helped = sum(1 for e in events if e['outcome'] in ('HELPED', 'PREVENTED_LIQ'))
            hurt   = sum(1 for e in events if e['outcome'] == 'HURT')
            prev   = sum(1 for e in events if e['outcome'] == 'PREVENTED_LIQ')

            lines += [
                f'### {stop_labels[vname]}: 총 {len(events)}건 발동',
                f'- HELPED (stop이 더 큰 손실 방지): {helped}건',
                f'- HURT (stop 이후 반등, 성과 저하): {hurt}건',
                f'- PREVENTED_LIQ (liq 도달 방지): {prev}건',
                '',
                '| # | 진입일 | stop 발동일 | stop 가격 | no_stop exit | liq_price | 결과 |',
                '|---|--------|------------|----------|-------------|----------|------|',
            ]
            for i, e in enumerate(events, 1):
                lines.append(
                    f'| {i} | {e["trade_entry_dt"][:10]} | {e["stop_triggered_dt"][:10]} | '
                    f'${e["stop_exit_price"]:,.0f} | ${e["no_stop_exit_price"]:,.0f} | '
                    f'${e["liq_price"]:,.0f} | {e["outcome"]} |'
                )
            lines += ['']

        lines += ['']

    # 권장 임계값
    lines += [
        '---',
        '',
        '## 권장 임계값 분석',
        '',
        '| 임계값 | 효과 | 단점 | 판정 |',
        '|--------|------|------|------|',
        f'| −70% | MDD 대폭 개선, liq 위험 제거 | CAGR 손실 가능성 높음, 조기청산 多 | 공격적 보호 |',
        f'| −75% | MDD 개선, 중간 수준 보호 | 일부 조기청산 | **균형** |',
        f'| −80% | MDD 소폭 개선, liq 위험만 제거 | CAGR 영향 최소 | 최소 보호 |',
        '',
        '> 최적 선택은 위 지표 비교 테이블의 실제 결과를 기반으로 판단하세요.',
        '',
        f'*생성: {datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}*',
        '',
    ]

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'  → {path.name}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print('=' * 65)
    print('Equity Stop Backtest — SupertrendStrategy 4h 3x Long-Only')
    print('=' * 65)

    # 1. 공통 캔들 준비
    print('\n[1] 캔들 로드')
    main_1h = _load_1h(BACKTEST_START, BACKTEST_END)
    warm_1h = _load_1h(WARMUP_START, BACKTEST_START)
    main_4h = _resample_1h(main_1h, '4h')
    warm_4h = _resample_1h(warm_1h, '4h')
    print(f'  메인: {len(main_4h):,} 4h봉  warmup: {len(warm_4h):,} 4h봉')

    main_1m = _expand_tf_to_1m(main_4h, 240)
    warm_1m = _expand_tf_to_1m(warm_4h, 240)

    base_cls  = _load_base_strategy()
    all_results = {}

    from external._long_only_factory import make_long_only

    for combo in COMBOS:
        cid = combo['id']
        hp  = {k: v for k, v in combo.items() if k != 'id'}
        print(f'\n[{cid}]')

        fixed_cls = _make_fixed_hp(base_cls, hp)
        combo_out = {}

        # 2. 각 variant 백테스트
        variants = [
            ('no_stop',    None),
            ('eq_stop_70', 0.70),
            ('eq_stop_75', 0.75),
            ('eq_stop_80', 0.80),
        ]

        no_stop_trades = []   # 사후 분석용

        for vname, stop_pct in variants:
            print(f'  [{vname}] ', end='', flush=True)
            os.environ['STRATEGY_LEVERAGE'] = str(LEVERAGE)

            if stop_pct is None:
                strategy_cls = make_long_only(fixed_cls)
            else:
                eq_cls       = make_equity_stop_strategy(fixed_cls, stop_pct, LEVERAGE)
                strategy_cls = make_long_only(eq_cls)

            try:
                trades, metrics_raw = run_backtest(strategy_cls, main_1m, warm_1m)
                m                   = _extract_metrics(metrics_raw)
                print(f'CAGR={m["cagr_pct"]:+.1f}%  MDD={m["mdd_pct"]:.1f}%  '
                      f'Sharpe={m["sharpe"]:.3f}  trades={m["trades"]}')
                combo_out[vname] = {'metrics': m, 'trades': trades}

                if vname == 'no_stop':
                    no_stop_trades = trades
            except Exception as e:
                import traceback
                print(f'ERROR: {e}')
                traceback.print_exc()
                combo_out[vname] = {'metrics': {}, 'trades': [], 'error': str(e)}

        # 3. 사후 분석: stop 발동 시뮬레이션
        print(f'\n  [사후 분석] equity stop 발동 이벤트 시뮬레이션')
        for vname, stop_pct in variants:
            if stop_pct is None:
                continue
            events = simulate_stop_events(no_stop_trades, main_4h, stop_pct, LEVERAGE)
            combo_out[vname]['stop_events'] = events

            helped = sum(1 for e in events if e['outcome'] in ('HELPED', 'PREVENTED_LIQ'))
            hurt   = sum(1 for e in events if e['outcome'] == 'HURT')
            prev   = sum(1 for e in events if e['outcome'] == 'PREVENTED_LIQ')
            print(f'  [{vname}] 발동: {len(events)}건  '
                  f'HELPED={helped}  HURT={hurt}  LIQ방지={prev}')

        all_results[cid] = combo_out

    # 4. 출력
    print('\n[결과 저장]')
    output = {
        'leverage':        LEVERAGE,
        'maint_margin':    MAINT_MARGIN,
        'liq_ratio':       LIQ_RATIO,
        'analysis_period': f'{BACKTEST_START} ~ {BACKTEST_END}',
        'stop_pcts':       STOP_PCTS,
        'stop_prices': {
            f'eq_stop_{int(p*100)}': round(1 - p / LEVERAGE, 4) for p in STOP_PCTS
        },
        'generated_at':   datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'results': {
            cid: {
                vname: {
                    'metrics':      vdata.get('metrics', {}),
                    'stop_events':  vdata.get('stop_events', []),
                }
                for vname, vdata in combo_data.items()
            }
            for cid, combo_data in all_results.items()
        },
    }

    write_json(output, RESULT_DIR / 'equity_stop_results.json')
    write_stop_report(
        {cid: {vn: {'metrics': vd.get('metrics', {}),
                     'stop_events': vd.get('stop_events', [])}
               for vn, vd in cdata.items()}
         for cid, cdata in all_results.items()},
        RESULT_DIR / 'equity_stop_report.md'
    )

    print('\n완료.')
    print(f'  {RESULT_DIR}/equity_stop_results.json')
    print(f'  {RESULT_DIR}/equity_stop_report.md')


if __name__ == '__main__':
    main()
