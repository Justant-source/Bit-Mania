#!/usr/bin/env python3
"""
V4 Backtest Dashboard Builder
Generates a single self-contained HTML dashboard with all 76 backtest results.

Usage:
    python build_v4_dashboard.py [--output PATH]

Output: cryptoengine/.result/v4/dashboard.html (~5-8MB, offline-capable)
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ─── Paths ────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from _paths import DATA_ROOT, RESULTS_ROOT, DASHBOARDS_ROOT

RESULT_DIR          = RESULTS_ROOT / '7-strategies'
PRE21_BACKFILL_DIR  = RESULTS_ROOT / 'pre2021_backfill'
BTC_KLINES          = DATA_ROOT / 'ohlcv' / 'BTCUSDT'
DEFAULT_OUT         = RESULT_DIR / 'dashboard.html'

ADJUSTED_PRE21_JSON = RESULTS_ROOT / 'adjusted_costs_pre2021' / 'all_adjusted_results_pre21.json'
ADJUSTED_POST21_JSON = RESULTS_ROOT / 'adjusted_costs_7strategies' / 'all_adjusted_results_7s.json'
FUNDING_8H_PARQUET   = DATA_ROOT / 'funding' / 'BTCUSDT_8h.parquet'

# ─── Backtest parameters ───────────────────────────────────────────────────────
TIMEFRAMES  = ['1h', '4h', '1D']
STRATEGIES  = ['stoch', 'supertrend',
               'tradeiq_psar_ha', 'trendtype', 'supertrend_trendtype', 'tradeiq_cci_ce']
_VARIANTS_BASE = ['bidirectional', 'long_only',
                  'bidirectional_x2', 'long_only_x2',
                  'bidirectional_x3', 'long_only_x3',
                  'long_only_v2', 'long_only_x3_v2']


def _discover_combo_variants() -> list[str]:
    """Auto-discover long_only_{id} / long_only_x3_{id} from results dir."""
    import re
    found = []
    base = RESULT_DIR / 'supertrend' / '4h'
    if base.exists():
        for d in sorted(base.iterdir()):
            if re.match(r'^long_only(?:_x3)?_\d+$', d.name) and (d / 'stats.json').exists():
                found.append(d.name)
    return found


VARIANTS = _VARIANTS_BASE + _discover_combo_variants()
START_MS    = int(datetime(2017, 8, 18, tzinfo=timezone.utc).timestamp() * 1000)
END_MS      = int(datetime(2026, 4, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
_N_DAYS     = int((END_MS - START_MS) / 86_400_000) + 1  # days inclusive


def _load_bnh_sharpe() -> dict:
    """Read BnH Sharpe from the 1D buy_and_hold results (dynamically)."""
    p = RESULT_DIR / 'buy_and_hold' / '1D' / 'buy_and_hold' / 'stats.json'
    try:
        s = json.loads(p.read_text())
        v = float(s.get('sharpe_ratio', 0) or 0)
    except Exception:
        v = 0.418  # fallback if results not yet available
    return {'1h': v, '4h': v, '1D': v}

# ─── Strategy metadata (Korean / English bilingual) ───────────────────────────
STRATEGY_META: dict[str, dict] = {
    'stoch': {
        'name_ko': '스토캐스틱', 'name_en': 'Stochastic',
        'summary_ko': '스토캐스틱 과매도/과매수 + EMA 방향 + 헤이킨 아시 봉 확인으로 진입',
        'summary_en': 'Stochastic extreme + EMA trend direction + Heikin Ashi candle confirmation',
        'entry_long_ko': [
            'Stochastic %K ≤ 20 (과매도 구간)',
            '헤이킨 아시 양봉 (종가 > 시가)',
            '단기 EMA(7) 상승 중',
            '선택: 가격 > EMA(200)',
        ],
        'entry_long_en': [
            'Stochastic %K ≤ 20 (oversold zone)',
            'Heikin Ashi bullish candle (close > open)',
            'Short EMA(7) trending up',
            'Optional: price above EMA(200)',
        ],
        'entry_short_ko': [
            'Stochastic %K ≥ 80 (과매수 구간)',
            '헤이킨 아시 음봉',
            '단기 EMA(7) 하락 중',
        ],
        'entry_short_en': [
            'Stochastic %K ≥ 80 (overbought zone)',
            'Heikin Ashi bearish candle',
            'Short EMA(7) trending down',
        ],
        'exit_ko': ['ATR(14) × 3.0 손절'],
        'exit_en': ['Stop-loss: ATR(14) × 3.0 from entry'],
        'indicators': ['Stochastic (K=14, smooth=3)', 'EMA (7, 20, 200)', 'Heikin Ashi', 'ATR (14)'],
    },
    'supertrend': {
        'name_ko': '슈퍼트렌드', 'name_en': 'Supertrend',
        'summary_ko': 'Supertrend + EMA 정렬 + 방향 EMA 필터로 추세 추종. v2.0은 파라미터 스윕 최적화 버전 (sweet_spot_score 92.6, PLATEAU)',
        'summary_en': 'Trend-following with Supertrend, EMA alignment, direction EMA filter. v2.0 = sweep-optimized params (sweet_spot_score 92.6, PLATEAU)',
        'entry_long_ko': [
            'Supertrend = 상승 추세 (가격 > Supertrend 라인)',
            'fast EMA > slow EMA (황금 교차)',
            '가격 > direction EMA (장기 상승 추세)',
            '★ v2.0 파라미터: ST(9, 2.5) · EMA(8/25) · dir_EMA(230) · ATR×3.2',
        ],
        'entry_long_en': [
            'Supertrend = uptrend (price above Supertrend line)',
            'fast EMA > slow EMA (bullish EMA cross)',
            'Price above direction EMA (long-term uptrend)',
            '★ v2.0 params: ST(9, 2.5) · EMA(8/25) · dir_EMA(230) · ATR×3.2',
        ],
        'entry_short_ko': [
            'Supertrend = 하락 추세',
            'fast EMA < slow EMA',
            '가격 < direction EMA',
        ],
        'entry_short_en': [
            'Supertrend = downtrend (price below Supertrend line)',
            'fast EMA < slow EMA (bearish EMA cross)',
            'Price below direction EMA',
        ],
        'exit_ko': ['fast/slow EMA 교차 시 청산 (추세 반전)', 'ATR(14) × atr_mult 손절 (v2.0: ×3.2)'],
        'exit_en': ['Exit on fast/slow EMA crossover (trend reversal)', 'Stop-loss: ATR(14) × atr_mult (v2.0: ×3.2)'],
        'indicators': ['Supertrend (period, factor)', 'fast EMA / slow EMA / direction EMA', 'ATR (14)'],
    },
    'trendtype': {
        'name_ko': '트렌드타입', 'name_en': 'TrendType',
        'summary_ko': 'ADX+ATR로 시장 레짐 분류 — 강한 추세(±2)일 때만 진입',
        'summary_en': 'ADX+ATR regime classifier — enters only in strong trends (±2)',
        'entry_long_ko': [
            'TrendType 레짐 = +2.0 (강한 상승)',
            '조건: ATR > ATR 이동평균(20) AND DI+ > DI-',
        ],
        'entry_long_en': [
            'TrendType regime = +2.0 (strong uptrend)',
            'Condition: ATR > ATR_MA(20) AND DI+ > DI-',
        ],
        'entry_short_ko': [
            'TrendType 레짐 = -2.0 (강한 하락)',
            '조건: ATR > ATR 이동평균(20) AND DI- > DI+',
        ],
        'entry_short_en': [
            'TrendType regime = -2.0 (strong downtrend)',
            'Condition: ATR > ATR_MA(20) AND DI- > DI+',
        ],
        'exit_ko': ['레짐이 ±2에서 벗어나면 청산', 'ATR(14) × 3.0 손절'],
        'exit_en': ['Exit when regime leaves ±2.0', 'Stop-loss: ATR(14) × 3.0'],
        'indicators': ['ATR (14, MA=20)', 'DMI/ADX (14)', 'ATR (14)'],
    },
    'supertrend_trendtype': {
        'name_ko': '슈퍼트렌드+트렌드타입', 'name_en': 'Supertrend + TrendType',
        'summary_ko': '5가지 조건이 모두 일치할 때만 진입 — 매우 선별적 (거래 수 적음)',
        'summary_en': 'Enters only when all 5 conditions align — very selective, low trade count',
        'entry_long_ko': [
            'TrendType = +2.0 (강한 상승)',
            'Supertrend = 상승',
            'EMA(7) > EMA(20)',
            '가격 > EMA(200)',
        ],
        'entry_long_en': [
            'TrendType regime = +2.0',
            'Supertrend direction = bullish',
            'EMA(7) > EMA(20)',
            'Price above EMA(200)',
        ],
        'entry_short_ko': [
            'TrendType = -2.0',
            'Supertrend = 하락',
            'EMA(7) < EMA(20)',
            '가격 < EMA(200)',
        ],
        'entry_short_en': [
            'TrendType regime = -2.0',
            'Supertrend direction = bearish',
            'EMA(7) < EMA(20)',
            'Price below EMA(200)',
        ],
        'exit_ko': ['EMA(7) ↔ EMA(20) 교차 시 청산', 'ATR(14) × 3.0 손절'],
        'exit_en': ['Exit on EMA(7)/EMA(20) crossover', 'Stop-loss: ATR(14) × 3.0'],
        'indicators': ['Supertrend (7, 3.0)', 'TrendType (ATR14/MA20, ADX14)', 'EMA (7, 20, 200)', 'ATR (14)'],
    },
    'tradeiq_psar_ha': {
        'name_ko': 'TradeIQ PSAR-HA', 'name_en': 'TradeIQ PSAR-HA',
        'summary_ko': 'PSAR + EMA(200) + RSI(14) + 헤이킨 아시 4가지 모두 일치 시 진입',
        'summary_en': 'Enter only when all 4 agree: Parabolic SAR + EMA(200) + RSI(14) + Heikin Ashi',
        'entry_long_ko': [
            'Parabolic SAR < 현재 가격 (SAR 매수 신호)',
            '가격 > EMA(200)',
            'RSI(14) > 50',
            '헤이킨 아시 양봉',
        ],
        'entry_long_en': [
            'Parabolic SAR < price (bullish SAR)',
            'Price above EMA(200)',
            'RSI(14) > 50 (positive momentum)',
            'Heikin Ashi bullish candle',
        ],
        'entry_short_ko': [
            'Parabolic SAR > 현재 가격',
            '가격 < EMA(200)',
            'RSI(14) < 50',
            '헤이킨 아시 음봉',
        ],
        'entry_short_en': [
            'Parabolic SAR > price (bearish SAR)',
            'Price below EMA(200)',
            'RSI(14) < 50',
            'Heikin Ashi bearish candle',
        ],
        'exit_ko': [
            'PSAR 반전 OR EMA(200) 이탈 OR RSI < 50 중 하나 발생 시 청산',
            'ATR(14) × 3.0 손절',
        ],
        'exit_en': [
            'Exit if any condition flips: PSAR, EMA(200), or RSI<50',
            'Stop-loss: ATR(14) × 3.0',
        ],
        'indicators': ['Parabolic SAR (0.02, 0.2)', 'EMA (200)', 'RSI (14)', 'Heikin Ashi', 'ATR (14)'],
    },
    'tradeiq_cci_ce': {
        'name_ko': 'TradeIQ CCI-CE', 'name_en': 'TradeIQ CCI-CE',
        'summary_ko': 'CCI 과매도/과매수 영역 이탈 + Chandelier Exit 방향 확인으로 진입',
        'summary_en': 'CCI oversold/overbought breakout with Chandelier Exit direction confirmation',
        'entry_long_ko': [
            'CCI(20) ≤ -100에서 > -100으로 상향 돌파 (과매도 이탈)',
            'Chandelier Exit 방향 = +1 (상승 추세 확인)',
        ],
        'entry_long_en': [
            'CCI(20) crosses up from ≤-100 to >-100 (oversold breakout)',
            'Chandelier Exit direction = +1 (uptrend confirmed)',
        ],
        'entry_short_ko': [
            'CCI(20) ≥ +100에서 < +100으로 하향 돌파',
            'Chandelier Exit 방향 = -1',
        ],
        'entry_short_en': [
            'CCI(20) crosses down from ≥+100 to <+100 (overbought breakout)',
            'Chandelier Exit direction = -1 (downtrend)',
        ],
        'exit_ko': ['ATR(14) × 3.0 손절'],
        'exit_en': ['Stop-loss: ATR(14) × 3.0 from entry'],
        'indicators': ['CCI (20, thresholds ±100)', 'Chandelier Exit (22, 3.0)', 'ATR (14)'],
    },
    'buy_and_hold': {
        'name_ko': '바이 앤 홀드 (벤치마크)', 'name_en': 'Buy & Hold (Benchmark)',
        'summary_ko': '시작 시점에 자본의 95%로 BTC 매수 후 보유. 전략 성능 비교 기준선.',
        'summary_en': 'Buy BTC with 95% of capital at start and hold. Performance benchmark.',
        'entry_long_ko': ['백테스트 시작 첫 번째 봉에 자본의 95%로 BTC 매수', '이후 추가 진입 없음'],
        'entry_long_en': ['Buy 95% of capital in BTC on the first candle', 'No further entries'],
        'entry_short_ko': [], 'entry_short_en': [],
        'exit_ko': ['청산 없음 (백테스트 종료까지 보유)'],
        'exit_en': ['Never exits — holds until end of backtest'],
        'indicators': ['None (pure buy-and-hold benchmark)'],
    },
}


# ─── Optimal params (v2+v3 combined best per strategy/TF/variant) ─────────────

def compute_optimal_params() -> dict[str, dict]:
    """Load all param_sweep v2+v3 summary files and return best params per strat/TF."""
    # Only the parameters actually swept per strategy — avoids leaking fixed HP into the table
    _SWEEP_KEYS: dict[str, list[str]] = {
        'supertrend':           ['st_factor', 'st_period'],
        'supertrend_trendtype': ['st_factor', 'atr_len', 'filter_mask'],
        'trendtype':            ['atr_len', 'di_len'],
        'tradeiq_psar_ha':       ['rsi_len', 'atr_mult'],
        'tradeiq_cci_ce':       ['cci_period', 'ce_mult'],
        'stoch':                ['stoch_k_period', 'atr_mult'],
    }

    sweep_bases = [
        (RESULTS_ROOT / 'param_sweep' / 'v2', 'v2'),
        (RESULTS_ROOT / 'param_sweep' / 'v3', 'v3'),
    ]
    # best[(strat, tf)] = {score, variant, params, version, combo, p0_cagr, p1_cagr, p2_cagr, p3_cagr, p4_cagr, p0_mdd, p1_mdd}
    best: dict[tuple, dict] = {}

    for base, version in sweep_bases:
        if not base.exists():
            continue
        for summary_path in sorted(base.glob('*/*/*/combo_*/summary.json')):
            parts = summary_path.parts
            # … / param_sweep_v? / strat / tf / variant / combo_N / summary.json
            strat   = parts[-5]
            tf      = parts[-4]
            variant = parts[-3]
            try:
                d = json.loads(summary_path.read_text())
            except Exception:
                continue
            score = float(d.get('score', -999))
            if score <= -999:
                continue
            key = (strat, tf)
            periods = d.get('periods', {})
            hp = d.get('hp', {})
            sweep_keys = _SWEEP_KEYS.get(strat, list(hp.keys()))
            entry = {
                'score':   round(score, 2),
                'variant': variant,
                'params':  {k: hp[k] for k in sweep_keys if k in hp},
                'version': version,
                'combo':   int(parts[-2].split('_')[1]),
                'p0_cagr': round(periods.get('p0', {}).get('cagr', 0), 1),
                'p1_cagr': round(periods.get('p1', {}).get('cagr', 0), 1),
                'p2_cagr': round(periods.get('p2', {}).get('cagr', 0), 1),
                'p3_cagr': round(periods.get('p3', {}).get('cagr', 0), 1),
                'p4_cagr': round(periods.get('p4', {}).get('cagr', 0), 1),
                'p0_mdd':  round(periods.get('p0', {}).get('mdd', 0), 1),
                'p1_mdd':  round(periods.get('p1', {}).get('mdd', 0), 1),
                'p2_mdd':  round(periods.get('p2', {}).get('mdd', 0), 1),
            }
            if key not in best or score > best[key]['score']:
                best[key] = entry

    # Reshape: result[strat][tf] = entry
    result: dict[str, dict] = {}
    for (strat, tf), entry in best.items():
        result.setdefault(strat, {})[tf] = entry
    return result


# ─── Tier computation ─────────────────────────────────────────────────────────

def compute_tier(stats: dict, tf: str, variant: str) -> str:
    if variant == 'buy_and_hold':
        return 'BNH'
    cagr   = stats.get('cagr_pct', -999)
    sharpe = stats.get('sharpe_ratio', -999)
    mdd    = stats.get('max_drawdown_pct', -999)
    trades = stats.get('total_trades', 0)
    bnh_s  = _load_bnh_sharpe().get(tf, 0.9)
    if sharpe >= bnh_s * 0.7 and cagr >= 5 and mdd >= -30 and trades >= 30:
        return 'A'
    if sharpe >= 0.3 and cagr >= 0 and mdd >= -40:
        return 'B'
    return 'C'


# ─── Data collection ─────────────────────────────────────────────────────────

def _load_csv_trades(path: Path) -> list[dict]:
    # Try Parquet first, then CSV
    parquet_path = path.with_suffix('.parquet')
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            trades = []
            for _, row in df.iterrows():
                try:
                    trades.append({
                        't_open':  int(float(row['opened_at'])),
                        't_close': int(float(row['closed_at'])),
                        'side':    row['side'],
                        'entry':   float(row['entry_price']),
                        'exit':    float(row['exit_price']),
                        'qty':     float(row['qty']),
                        'pnl':     float(row['pnl']),
                        'fee':     float(row['fee']),
                    })
                except (KeyError, ValueError, TypeError):
                    pass
            return sorted(trades, key=lambda t: t['t_close'])
        except Exception:
            pass  # Fall through to CSV

    if not path.exists():
        return []
    trades = []
    with open(path, newline='') as f:
        for row in csv.DictReader(f):
            try:
                trades.append({
                    't_open':  int(float(row['opened_at'])),
                    't_close': int(float(row['closed_at'])),
                    'side':    row['side'],
                    'entry':   float(row['entry_price']),
                    'exit':    float(row['exit_price']),
                    'qty':     float(row['qty']),
                    'pnl':     float(row['pnl']),
                    'fee':     float(row['fee']),
                })
            except (KeyError, ValueError):
                pass
    return sorted(trades, key=lambda t: t['t_close'])


def _load_csv_monthly(path: Path) -> list[dict]:
    # Try Parquet first, then CSV
    parquet_path = path.with_suffix('.parquet')
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            rows = []
            for _, row in df.iterrows():
                try:
                    rows.append({'month': row['month'], 'pnl': float(row['pnl_usdt'])})
                except (KeyError, ValueError, TypeError):
                    pass
            return rows
        except Exception:
            pass  # Fall through to CSV

    if not path.exists():
        return []
    rows = []
    with open(path, newline='') as f:
        for row in csv.DictReader(f):
            try:
                rows.append({'month': row['month'], 'pnl': float(row['pnl_usdt'])})
            except (KeyError, ValueError):
                pass
    return rows


def build_equity_series(trades: list[dict], starting: float, finishing: float) -> list[dict]:
    """Reconstruct equity curve from closed trades."""
    equity = [{'t': START_MS, 'v': round(starting, 2)}]
    balance = starting
    for t in trades:
        balance += t['pnl']
        equity.append({'t': t['t_close'], 'v': round(balance, 2)})
    # Snap endpoint to stats finishing_balance
    equity.append({'t': END_MS, 'v': round(finishing, 2)})
    return equity


def build_bnh_equity_from_btc(starting: float, btc_daily: list[dict]) -> list[dict]:
    """Densify BnH equity from BTC daily closes so that any slice can be re-baselined.
    equity[t] = qty × BTC[t].close, qty = starting / first_close.
    Falls back to a 2-point series if BTC data is missing."""
    if not btc_daily or len(btc_daily) < 2:
        return [{'t': START_MS, 'v': round(starting, 2)},
                {'t': END_MS,   'v': round(starting, 2)}]
    start_px = btc_daily[0]['c']
    if start_px <= 0:
        return [{'t': START_MS, 'v': round(starting, 2)},
                {'t': END_MS,   'v': round(starting, 2)}]
    qty = starting / start_px
    return [{'t': p['t'], 'v': round(qty * p['c'], 2)} for p in btc_daily]


def load_costs_lookup() -> dict:
    """Load pre-2021 and post-2021 adjusted cost data.
    Returns {(strat, tf, variant): {period_key: {fee, fund, coverage, adj_cagr, orig_cagr}}}
    """
    lookup: dict[tuple, dict] = {}

    def _ingest(path: Path):
        if not path.exists():
            print(f'[costs] WARNING: {path} not found — running without cost adjustment', file=sys.stderr)
            return
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            print(f'[costs] WARNING: failed to parse {path}: {e}', file=sys.stderr)
            return
        for entry in data:
            key = (entry.get('strat', ''), entry.get('tf', ''), entry.get('variant', ''))
            if not all(key):
                continue
            periods = lookup.setdefault(key, {})
            for period_name, pdata in entry.get('periods', {}).items():
                periods[period_name] = {
                    'fee':      round(float(pdata.get('fee_cost_annual_pct', 0)), 4),
                    'fund':     round(float(pdata.get('funding_cost_annual_pct', 0)), 4),
                    'coverage': pdata.get('funding_coverage', 'fee_only'),
                    'adj_cagr': round(float(pdata.get('adj_cagr', 0)), 4),
                    'orig_cagr': round(float(pdata.get('original_cagr', 0)), 4),
                    'trades':   int(pdata.get('trades', 0)),
                    'avg_fund_rate': round(float(pdata.get('avg_funding_rate', 0)), 8),
                }

    _ingest(ADJUSTED_PRE21_JSON)
    _ingest(ADJUSTED_POST21_JSON)
    n = sum(len(v) for v in lookup.values())
    print(f'[costs] Loaded {len(lookup)} combos × periods = {n} entries', file=sys.stderr)
    return lookup


def load_8h_funding_series() -> dict:
    """Load 8h funding event series from parquet for per-event dashboard lookup.
    Returns {'ts': [int, ...], 'rates': [float, ...], 'fallback': float}.
    Zero-rate rows kept as-is; JS substitutes with fallback at runtime.
    """
    if not FUNDING_8H_PARQUET.exists():
        print(f'[funding] WARNING: {FUNDING_8H_PARQUET} not found — JS will use 0 for all funding', file=sys.stderr)
        return {'ts': [], 'rates': [], 'fallback': 0.0}
    df = pd.read_parquet(FUNDING_8H_PARQUET).sort_values('timestamp')
    ts = df['timestamp'].astype('int64').tolist()
    rates = [round(float(r), 10) for r in df['funding_rate']]
    nonzero = [r for r in rates if r != 0]
    fallback = round(sum(nonzero) / len(nonzero), 10) if nonzero else 0.0
    print(f'[funding] {len(ts)} 8h events embedded, fallback={fallback:.8f}', file=sys.stderr)
    return {'ts': ts, 'rates': rates, 'fallback': fallback}


_LIQ_RISK_JSON = RESULTS_ROOT / 'supertrend_x3_long_only' / 'covid_crash_analysis' / 'liquidation_risk.json'
_VARIANT_TO_LIQ_COMBO: dict[str, str] = {
    'long_only_x3':     'default',
    'long_only_x3_v2':  'v2',
    'long_only_x3_164': 'combo_164',
    'long_only_x3_173': 'combo_173',
    'long_only_x3_176': 'combo_176',
}


def collect_all_results(btc_daily: list[dict] | None = None,
                         costs_lookup: dict | None = None) -> dict:
    """Returns dict keyed by strategy_dir, each with list of result objects.
    btc_daily is used to build a dense BnH equity curve."""
    groups: dict[str, list[dict]] = {}

    # Load liquidation risk analysis results (2018-2026 full period)
    _liq_risk_results: dict = {}
    if _LIQ_RISK_JSON.exists():
        _lrd = json.loads(_LIQ_RISK_JSON.read_text())
        _liq_risk_results = _lrd.get('results', {})
        print(f'  Liq risk data loaded: {len(_liq_risk_results)} combos')

    def _process(tf: str, strat_dir: str, variant: str, folder: Path):
        stats_path = folder / 'stats.json'
        if not stats_path.exists():
            return
        stats = json.loads(stats_path.read_text())
        trades   = _load_csv_trades(folder / 'trades.csv')
        monthly  = _load_csv_monthly(folder / 'monthly_returns.csv')

        # Merge pre-2021 trades (pre21_full = continuous 2017-08-18 → 2020-12-31 backtest)
        _p21_trades  = PRE21_BACKFILL_DIR / strat_dir / tf / variant / 'pre21_full' / 'trades.csv'
        _p21_monthly = PRE21_BACKFILL_DIR / strat_dir / tf / variant / 'pre21_full' / 'monthly_returns.csv'
        if _p21_trades.exists():
            pre21_trades = _load_csv_trades(_p21_trades)
            trades = sorted(pre21_trades + trades, key=lambda t: t.get('t_open', 0))
        if _p21_monthly.exists():
            pre21_monthly = _load_csv_monthly(_p21_monthly)
            monthly = sorted(pre21_monthly + monthly, key=lambda m: m.get('month', ''))
        starting  = stats.get('starting_balance', 10000.0)
        finishing = stats.get('raw_metrics', {}).get('finishing_balance', starting)
        # Sanitize infinity in stats
        for k in ('sharpe_ratio', 'cagr_pct', 'max_drawdown_pct'):
            v = stats.get(k, 0)
            if not math.isfinite(v):
                stats[k] = 0.0
        tier = compute_tier(stats, tf, variant)
        if strat_dir == 'buy_and_hold' and btc_daily:
            equity = build_bnh_equity_from_btc(starting, btc_daily)
        else:
            equity = build_equity_series(trades, starting, finishing)
        raw = stats.get('raw_metrics', {})
        sortino = raw.get('sortino_ratio', 0)
        calmar  = raw.get('calmar_ratio', 0)
        if not math.isfinite(sortino): sortino = 0.0
        if not math.isfinite(calmar):  calmar  = 0.0
        returns_daily = _compute_daily_returns(equity)
        streaks = _compute_streaks(trades)
        # Leverage from variant suffix (_x2 / _x3 / _x3_v2)
        lev_m = re.search(r'_x(\d+)', variant)
        leverage = int(lev_m.group(1)) if lev_m else 1
        # Liquidation detection (balance ≤ 5% of starting)
        liq_threshold = starting * 0.05
        liquidated = False
        liq_month: str | None = None
        bal = starting
        for m in monthly:
            bal += m['pnl']
            if bal <= liq_threshold and not liquidated:
                liquidated = True
                liq_month = m['month']
                break
        # Safety: liquidation risk verdict from full-period analysis
        liq_risk = None
        combo_id = _VARIANT_TO_LIQ_COMBO.get(variant)
        if combo_id and strat_dir == 'supertrend' and tf == '4h' and combo_id in _liq_risk_results:
            cr = _liq_risk_results[combo_id]
            liq_risk = {
                'verdict': cr.get('verdict', 'UNKNOWN'),
                'trades':  cr.get('total_trades', 0),
                'events':  cr.get('total_liq_risk_events', 0),
                'period':  _lrd.get('analysis_period', '2018-2026'),
            }
        result = {
            'id':       f'{strat_dir}__{variant}__{tf}',
            'strat':    strat_dir,
            'variant':  variant,
            'tf':       tf,
            'tier':     tier,
            'leverage':    leverage,
            'liquidated':  liquidated,
            'liq_month':   liq_month,
            'liq_risk':    liq_risk,
            'stats': {
                'cagr':     round(stats.get('cagr_pct', 0), 4),
                'sharpe':   round(stats.get('sharpe_ratio', 0), 4),
                'mdd':      round(stats.get('max_drawdown_pct', 0), 4),
                'trades':   stats.get('total_trades', 0),
                'win_rate': round(stats.get('win_rate_pct', 0), 2),
                'pf':       round(stats.get('profit_factor', 0), 4),
                'starting': round(starting, 2),
                'finishing': round(finishing, 2),
                'net_pct':  round(stats.get('net_profit_pct', 0), 4),
                'sortino':  round(sortino, 4),
                'calmar':   round(calmar, 4),
            },
            'equity':        equity,
            'trades':        trades,
            'monthly':       monthly,
            'returns_daily': returns_daily,
            'streaks':       streaks,
            'costs':         (costs_lookup or {}).get((strat_dir, tf, variant), {}),
        }
        groups.setdefault(strat_dir, []).append(result)

    for tf in TIMEFRAMES:
        # BuyAndHold
        bnh_folder = RESULT_DIR / 'buy_and_hold' / tf / 'buy_and_hold'
        if bnh_folder.exists():
            _process(tf, 'buy_and_hold', 'buy_and_hold', bnh_folder)
        # Strategies
        for strat in STRATEGIES:
            for var in VARIANTS:
                folder = RESULT_DIR / strat / tf / var
                if folder.exists():
                    _process(tf, strat, var, folder)

    return groups


# ─── BTC Price Data ───────────────────────────────────────────────────────────

def load_btc_1d() -> list[dict]:
    """Load BTC 1D OHLC from parquet files (2017-2026)."""
    frames = []
    base = BTC_KLINES / '1d'
    for year in range(2017, 2027):
        for month in range(1, 13):
            p = base / str(year) / f'{month:02d}.parquet'
            if not p.exists():
                continue
            df_chunk = pd.read_parquet(p)
            # Normalise column name: 'timestamp' → 'open_time'
            if 'timestamp' in df_chunk.columns and 'open_time' not in df_chunk.columns:
                df_chunk = df_chunk.rename(columns={'timestamp': 'open_time'})
            frames.append(df_chunk[['open_time', 'open', 'high', 'low', 'close']])
    if not frames:
        return []
    df = pd.concat(frames).sort_values('open_time')
    # Ensure tz-aware for comparison
    if df['open_time'].dt.tz is None:
        df['open_time'] = df['open_time'].dt.tz_localize('UTC')
    df = df[(df['open_time'] >= pd.Timestamp('2017-08-18', tz='UTC')) &
            (df['open_time'] <= pd.Timestamp('2026-04-30', tz='UTC'))]
    return [
        {
            't': int(row.open_time.timestamp() * 1000),
            'o': round(row.open, 2),
            'h': round(row.high, 2),
            'l': round(row.low, 2),
            'c': round(row.close, 2),
        }
        for row in df.itertuples()
    ]


# ─── Derived metrics helpers ─────────────────────────────────────────────────

def _compute_daily_returns(equity_points: list[dict]) -> list[float]:
    """Trade-based equity → N daily forward-filled pct returns (START_MS+1d … END_MS)."""
    import datetime as dt
    start = dt.date(2017, 8, 18)
    n = _N_DAYS

    # Build (day_idx, value) list from equity points
    updates: list[tuple[int, float]] = []
    for p in equity_points:
        d = dt.datetime.utcfromtimestamp(p['t'] / 1000).date()
        idx = (d - start).days
        if 0 <= idx < n:
            updates.append((idx, p['v']))
    updates.sort()

    # Forward-fill daily equity
    eq = [0.0] * n
    cur = equity_points[0]['v'] if equity_points else 10000.0
    ui = 0
    for i in range(n):
        while ui < len(updates) and updates[ui][0] <= i:
            cur = updates[ui][1]
            ui += 1
        eq[i] = cur

    # Daily pct returns (length 1855)
    rets: list[float] = []
    for i in range(1, n):
        prev = eq[i - 1]
        rets.append(round((eq[i] - prev) / prev, 6) if prev > 0 else 0.0)
    return rets


def _classify_btc_regimes(btc_daily: list[dict]) -> list[str]:
    """Classify each BTC daily bar into BULL_HV/BULL_LV/BEAR_HV/BEAR_LV/SIDE."""
    closes = [p['c'] for p in btc_daily]
    n = len(closes)
    window = 90

    # Rolling 90-day return
    roll_ret = [0.0] * n
    for i in range(window, n):
        base = closes[i - window]
        roll_ret[i] = (closes[i] - base) / base if base > 0 else 0.0

    # Rolling 90-day std of daily returns
    daily_rets = [0.0] + [
        (closes[i] - closes[i - 1]) / closes[i - 1] if closes[i - 1] > 0 else 0.0
        for i in range(1, n)
    ]
    roll_std = [0.0] * n
    for i in range(window, n):
        seg = daily_rets[i - window + 1: i + 1]
        mu = sum(seg) / len(seg)
        roll_std[i] = math.sqrt(sum((x - mu) ** 2 for x in seg) / len(seg))

    valid_stds = sorted(s for s in roll_std[window:] if s > 0)
    med_std = valid_stds[len(valid_stds) // 2] if valid_stds else 0.001

    labels: list[str] = []
    for i in range(n):
        if i < window:
            labels.append('SIDE')
            continue
        ret = roll_ret[i]
        if ret > 0.20:
            trend = 'BULL'
        elif ret < -0.20:
            trend = 'BEAR'
        else:
            labels.append('SIDE')
            continue
        vol_cls = 'HV' if roll_std[i] > med_std else 'LV'
        labels.append(f'{trend}_{vol_cls}')
    return labels


def _compute_streaks(trades: list[dict]) -> dict:
    """Return max win and loss streaks from sorted trade list."""
    win_max = loss_max = win_cur = loss_cur = 0
    for t in trades:
        if t['pnl'] > 0:
            win_cur += 1; loss_cur = 0
            win_max = max(win_max, win_cur)
        elif t['pnl'] < 0:
            loss_cur += 1; win_cur = 0
            loss_max = max(loss_max, loss_cur)
        else:
            win_cur = loss_cur = 0
    return {'win_max': win_max, 'loss_max': loss_max}


# ─── HTML Generator ────────────────────────────────────────────────────────────

def get_plotlyjs() -> str:
    try:
        import plotly.offline as poff
        return poff.get_plotlyjs()
    except Exception:
        print('[warn] plotly not found; chart rendering will fail. pip install plotly', file=sys.stderr)
        return 'console.error("Plotly.js not embedded");'


CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0d1117; color: #e6edf3; font-size: 14px; }
a { color: #58a6ff; }
::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: #161b22; }
::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }

/* Layout */
.app { display: flex; height: 100vh; overflow: hidden; }
.sidebar { width: 300px; min-width: 220px; background: #161b22; border-right: 1px solid #30363d;
           display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; }
.main { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 16px; }

/* Header */
.header { padding: 12px 16px; background: #0d1117; border-bottom: 1px solid #30363d; flex-shrink: 0; }
.header h1 { font-size: 15px; font-weight: 600; color: #f0f6fc; }
.header .subtitle { font-size: 11px; color: #8b949e; margin-top: 2px; }

/* Filters */
.filters { padding: 10px 12px; border-bottom: 1px solid #30363d; flex-shrink: 0; }
.filter-row { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 6px; }
.filter-label { font-size: 11px; color: #8b949e; margin-bottom: 4px; }
.tag { padding: 2px 8px; border-radius: 12px; font-size: 11px; cursor: pointer;
       border: 1px solid #30363d; background: #21262d; color: #8b949e; user-select: none; }
.tag.active { background: #1f6feb; border-color: #388bfd; color: #cae8ff; }

/* Strategy list */
.strat-list { flex: 1; overflow-y: auto; padding: 6px 0; }
.strat-group { border-bottom: 1px solid #21262d; }
.strat-group-header { padding: 8px 12px; cursor: pointer; display: flex; align-items: center;
                       gap: 6px; font-size: 12px; font-weight: 600; color: #c9d1d9;
                       user-select: none; }
.strat-group-header:hover { background: #21262d; }
.strat-group-header .arrow { font-size: 9px; transition: transform 0.15s; }
.strat-group-header.open .arrow { transform: rotate(90deg); }
.strat-group-items { display: none; padding: 0 0 4px 0; }
.strat-group-items.open { display: block; }
.strat-item { padding: 5px 12px 5px 24px; cursor: pointer; display: flex;
               align-items: center; gap: 6px; font-size: 12px; color: #8b949e; }
.strat-item:hover { background: #21262d; color: #c9d1d9; }
.strat-item.selected { background: #1c2d47; color: #79c0ff; }
.strat-item .cb { width: 13px; height: 13px; border: 1px solid #30363d; border-radius: 2px;
                  flex-shrink: 0; display: flex; align-items: center; justify-content: center;
                  font-size: 10px; }
.strat-item.selected .cb { background: #1f6feb; border-color: #388bfd; color: white; }

/* Actions */
.actions { padding: 8px 12px; border-top: 1px solid #30363d; display: flex; gap: 6px; flex-shrink: 0; }
.btn { padding: 5px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; border: none;
       font-weight: 500; }
.btn-primary { background: #1f6feb; color: #fff; } .btn-primary:hover { background: #388bfd; }
.btn-ghost { background: #21262d; color: #8b949e; border: 1px solid #30363d; }
.btn-ghost:hover { background: #30363d; color: #c9d1d9; }
.sort-btn { padding: 3px 10px; border-radius: 6px; font-size: 11px; cursor: pointer; border: 1px solid #30363d;
             background: #21262d; color: #8b949e; user-select: none; }
.sort-btn.active { background: #1f6feb; border-color: #388bfd; color: #cae8ff; }
/* Flat strat list item (sort/top10 mode) */
.flat-item { padding: 5px 12px; cursor: pointer; display: flex; align-items: center;
              gap: 6px; font-size: 12px; color: #8b949e; border-bottom: 1px solid #21262d; }
.flat-item:hover { background: #21262d; color: #c9d1d9; }
.flat-item.selected { background: #1c2d47; color: #79c0ff; }
.flat-item .cb { width: 13px; height: 13px; border: 1px solid #30363d; border-radius: 2px;
                 flex-shrink: 0; display: flex; align-items: center; justify-content: center; font-size: 10px; }
.flat-item.selected .cb { background: #1f6feb; border-color: #388bfd; color: white; }

/* KPI Cards */
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px; }
.kpi-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }
.kpi-label { font-size: 11px; color: #8b949e; margin-bottom: 4px; }
.kpi-value { font-size: 20px; font-weight: 700; color: #f0f6fc; }
.kpi-sub   { font-size: 11px; color: #8b949e; margin-top: 2px; }
.kpi-pos { color: #3fb950; } .kpi-neg { color: #f85149; }

/* Chart containers */
.chart-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }
.chart-title { font-size: 13px; font-weight: 600; color: #c9d1d9; margin-bottom: 8px;
               display: flex; align-items: center; justify-content: space-between; }
.chart-title small { font-size: 11px; color: #8b949e; font-weight: 400; }
.chart-wrap { width: 100%; }

/* Description panel */
.desc-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
.desc-strat-tabs { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
.desc-tab { padding: 4px 12px; border-radius: 6px; font-size: 12px; cursor: pointer;
             border: 1px solid #30363d; background: #21262d; color: #8b949e; }
.desc-tab.active { background: #1c2d47; color: #79c0ff; border-color: #1f6feb; }
.desc-content { display: none; } .desc-content.active { display: block; }
.desc-section { margin-bottom: 14px; }
.desc-section h4 { font-size: 12px; font-weight: 600; color: #c9d1d9; margin-bottom: 6px;
                    padding-bottom: 4px; border-bottom: 1px solid #21262d; }
.desc-bilingual { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 700px) { .desc-bilingual { grid-template-columns: 1fr; } }
.desc-lang-label { font-size: 10px; color: #8b949e; margin-bottom: 4px; font-weight: 600; }
.desc-list { list-style: none; }
.desc-list li { padding: 3px 0; font-size: 12px; color: #8b949e; padding-left: 14px; position: relative; }
.desc-list li::before { content: '▸'; position: absolute; left: 0; color: #388bfd; }
.indicators-row { display: flex; flex-wrap: wrap; gap: 5px; }
.ind-chip { background: #21262d; border: 1px solid #30363d; border-radius: 4px;
             padding: 2px 8px; font-size: 11px; color: #8b949e; }
.empty-state { text-align: center; padding: 40px; color: #8b949e; font-size: 13px; }

/* Trade detail table */
.trade-detail-wrap { margin-top: 14px; border-top: 1px solid #30363d; padding-top: 12px; }
.trade-detail-wrap h5 { font-size: 12px; font-weight: 600; color: #c9d1d9; margin-bottom: 8px; }
.trade-detail-wrap .tbl-wrap { overflow-x: auto; }
.trade-detail-wrap table { width: auto; border-collapse: collapse; font-size: 12px; }
.trade-detail-wrap th { color: #8b949e; padding: 4px 8px; text-align: left; border-bottom: 1px solid #30363d; white-space: nowrap; }
.trade-detail-wrap td { padding: 4px 8px; border-bottom: 1px solid #1a1f2e; color: #c9d1d9; white-space: nowrap; }
.trade-detail-wrap tr:hover td { background: #21262d; }
.trade-detail-wrap tr.focused-row td { background: #1f6feb33; outline: 1px solid #58a6ff; }

/* Responsive */
@media (max-width: 768px) {
  html, body { overflow-x: hidden; }
  .app { flex-direction: column; height: auto; overflow: visible; }
  .sidebar { width: 100%; border-right: none; border-bottom: 1px solid #30363d;
              max-height: none; overflow: visible; flex-shrink: 0; }
  .strat-list { max-height: 240px; overflow-y: auto; }
  .main { padding: 10px; flex: none; overflow: visible; min-width: 0; }
  .kpi-grid { grid-template-columns: repeat(2, 1fr); }
  .chart-card { padding: 8px; }
  .chart-wrap { overflow-x: auto; }
}
@media (max-width: 480px) {
  .kpi-grid { grid-template-columns: 1fr 1fr; }
  .desc-bilingual { grid-template-columns: 1fr; }
  .filter-row { gap: 4px; }
  .tag, .sort-btn { font-size: 10px; padding: 2px 6px; }
}
/* Section headers */
.section-hdr { display: flex; align-items: center; gap: 10px; margin-top: 4px; }
.section-hdr-line { flex: 1; height: 1px; background: #30363d; }
.section-hdr-text { font-size: 11px; font-weight: 600; color: #484f58; letter-spacing: .08em;
                    text-transform: uppercase; white-space: nowrap; }
/* Chart toggle / tab row */
.chart-ctrl { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 8px; }
.chart-tab-btn { padding: 2px 9px; border-radius: 5px; font-size: 11px; cursor: pointer;
                 border: 1px solid #30363d; background: #21262d; color: #8b949e; user-select: none; }
.chart-tab-btn.active { background: #1c2d47; color: #79c0ff; border-color: #1f6feb; }
/* Warning badge */
.warn-badge { display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px;
              border-radius: 4px; font-size: 11px; background: #5a1e02; color: #ffa657; border: 1px solid #d1242f; }
"""


DASHBOARD_JS = r"""
/* ============================================================
   V4 Dashboard — Vanilla JS
   ============================================================ */
const DATA = window.V4_DATA;

// ── Color palette ──────────────────────────────────────────
const PALETTE = [
  '#4C9BE8','#E8834C','#4CE87E','#E84C4C','#B44CE8',
  '#E8D84C','#4CE8D8','#E84CA8','#88E84C','#4C68E8',
];
const BTC_COLOR = '#f7931a';
const DARK_BG   = '#0d1117';
const CARD_BG   = '#161b22';
const GRID_CLR  = '#21262d';
const TEXT_CLR  = '#c9d1d9';

// ── State ──────────────────────────────────────────────────
const state = {
  selected: [],                          // ordered array of IDs (max 6)
  tfFilter:      new Set(['1h','4h','1D']),
  variantFilter: new Set(['bidirectional','long_only','buy_and_hold','bidirectional_x2','long_only_x2','bidirectional_x3','long_only_x3','long_only_v2','long_only_x3_v2']),
  sortMode: 'return',                    // 'alpha' | 'return' | 'top10'
  startMs: Date.UTC(2017, 7, 18),         // 2017-08-18
  endMs:   Date.UTC(2026, 3, 30),        // 2026-04-30
};

// ── Cost period mapping (date range → period key in r.costs) ──────────────
// Maps "YYYY-MM-DD|YYYY-MM-DD" → period key that exists in the adjusted JSON
const COST_PERIOD_MAP = {
  '2017-08-18|2020-12-31': 'pre21_full',
  '2017-12-17|2018-12-15': 'pre21_bear',
  '2018-12-16|2019-04-01': 'pre21_range',
  '2019-04-02|2020-02-29': 'pre21_recovery',
  '2020-03-01|2020-04-30': 'pre21_covid',
  '2020-05-01|2020-12-31': 'pre21_bull',
  '2021-01-01|2021-12-31': '2021',
  '2022-01-01|2022-12-31': '2022',
  '2023-01-01|2023-12-31': '2023',
  '2024-01-01|2024-12-31': '2024',
  '2025-01-01|2026-04-30': '2025',
  '2025-01-01|2025-12-31': '2025',   // year-preset ends Dec 31
  '2021-01-01|2026-04-30': 'post21_full',
};

function msToDateStr(ms) {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth()+1).padStart(2,'0');
  const dy = String(d.getUTCDate()).padStart(2,'0');
  return `${y}-${m}-${dy}`;
}

function currentCostKey() {
  const s = msToDateStr(state.startMs);
  const e = msToDateStr(state.endMs);
  return COST_PERIOD_MAP[`${s}|${e}`] || null;
}

// Per-event 8h funding sum for a trade's hold window [openMs, closeMs).
// Looks up the pre-loaded DATA.funding_ts / DATA.funding_rates arrays via binary search.
// Zero-rate rows (pre-launch fallback) are substituted with DATA.funding_fallback.
// Returns Σ rate — caller multiplies by notional × fundSign.
function fundingRateSumInWindow(openMs, closeMs) {
  const TS    = DATA.funding_ts;
  const RATES = DATA.funding_rates;
  const FB    = DATA.funding_fallback;
  if (!TS || !TS.length) return 0;
  // Binary search: first index >= openMs
  let lo = 0, hi = TS.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (TS[mid] < openMs) lo = mid + 1; else hi = mid;
  }
  let sum = 0;
  for (let i = lo; i < TS.length && TS[i] < closeMs; i++) {
    sum += (RATES[i] === 0 ? FB : RATES[i]);
  }
  return sum;
}

// ── Helpers ────────────────────────────────────────────────
function getAllResults() {
  const out = [];
  for (const strat of Object.keys(DATA.groups)) {
    for (const r of DATA.groups[strat]) out.push(r);
  }
  return out;
}

function getResultById(id) {
  for (const r of getAllResults()) if (r.id === id) return r;
  return null;
}

function filteredResults() {
  return getAllResults().filter(r => {
    if (!state.tfFilter.has(r.tf)) return false;
    if (state.variantFilter.has(r.variant)) return true;
    // Combo x3 variants (long_only_x3_257 etc.) are controlled by the 'long_only_x3' filter.
    // 1x combo variants stay hidden — they exist only as balanceSim base data.
    if (/^long_only_x3_[0-9]+$/.test(r.variant)) return state.variantFilter.has('long_only_x3');
    return false;
  });
}

function fmtDollar(v) {
  if (!isFinite(v)) return 'N/A';
  if (Math.abs(v) >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return '$' + (v/1e3).toFixed(1) + 'k';
  return '$' + v.toFixed(0);
}
function fmtPct(v) { return (v >= 0 ? '+' : '') + v.toFixed(2) + '%'; }
function fmtSharpe(v) { return isFinite(v) ? v.toFixed(3) : 'N/A'; }
function fmtMultiplier(starting, finishing) {
  if (!starting || !isFinite(finishing / starting)) return '?x';
  const x = finishing / starting;
  if (x >= 10000) return x.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',') + 'x';
  if (x >= 1000)  return x.toFixed(0) + 'x';
  if (x >= 10)    return x.toFixed(1) + 'x';
  return x.toFixed(2) + 'x';
}
function fmtTotalPct(starting, finishing) {
  if (!starting) return 'N/A';
  const pct = (finishing - starting) / starting * 100;
  return (pct >= 0 ? '+' : '') + (Math.abs(pct) >= 10000
    ? (pct / 100).toFixed(0) + '배%'
    : pct.toFixed(1) + '%');
}

// Global variant label — used everywhere
function varLabel(r) {
  // Extract numeric combo_id (e.g. long_only_x3_257 → comboId='257').
  // _(?!v) prevents matching version suffix _v2 as a combo id.
  const comboM = r.variant.match(/_(?!v)([0-9]+)$/);
  const comboId = comboM ? comboM[1] : null;
  const comboSfx = comboId ? ' #' + comboId : '';
  const base = r.variant.replace(/(?:_x[0-9]+)?(?:_v[0-9]+)?(?:_[0-9]+)?$/, '');
  // Combo variants are sweep-optimised v2.0 params; version variants use _v2 suffix.
  const version = (r.variant.includes('_v2') || comboId) ? ' v2.0' : '';
  const lev  = r.leverage > 1 ? ' · ' + r.leverage + 'x' : '';
  if (base === 'buy_and_hold') return '매수보유';
  if (base === 'long_only')    return '롱전용' + lev + version + comboSfx;
  return '양방향' + lev + version + comboSfx;
}
function varLabelShort(r) {
  const comboM = r.variant.match(/_(?!v)([0-9]+)$/);
  const comboId = comboM ? comboM[1] : null;
  const comboSfx = comboId ? '#' + comboId : '';
  const base = r.variant.replace(/(?:_x[0-9]+)?(?:_v[0-9]+)?(?:_[0-9]+)?$/, '');
  const version = (r.variant.includes('_v2') || comboId) ? 'v2' : '';
  const lev  = r.leverage > 1 ? 'x' + r.leverage : '';
  if (base === 'buy_and_hold') return 'BnH';
  if (base === 'long_only')    return '롱' + lev + version + comboSfx;
  return '양방' + lev + version + comboSfx;
}
function fmtBalance(r) {
  const s = getStats(r);
  if (s.liquidated) return '<span style="color:#f85149">💀 $0.0k</span>';
  return fmtDollar(s.finishing);
}

// ── Date slicing engine ────────────────────────────────────
const sliceCache = new Map();

function ymOf(ms) {
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}`;
}

// Binary-search r.equity for the value at time `ms` (largest t ≤ ms).
// For sparse equity series (e.g. BnH with only START/END points) this returns the
// most recent known balance; for dense series (BnH built from BTC daily) it
// effectively interpolates with last-observation-carried-forward.
function equityAt(r, ms) {
  if (!r.equity || r.equity.length === 0) return 10_000;
  if (ms <= r.equity[0].t)                  return r.equity[0].v;
  const last = r.equity[r.equity.length - 1];
  if (ms >= last.t)                          return last.v;
  let lo = 0, hi = r.equity.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >>> 1;
    if (r.equity[mid].t <= ms) lo = mid;
    else                       hi = mid - 1;
  }
  return r.equity[lo].v;
}

function ymd(ms) {
  return new Date(ms).toISOString().slice(0,10);
}

function annualisedSharpe(monthlyRets) {
  const n = monthlyRets.length;
  if (n < 2) return 0;
  const mean = monthlyRets.reduce((a,b) => a+b, 0) / n;
  const variance = monthlyRets.reduce((a,v) => a + (v-mean)**2, 0) / (n-1);
  const std = Math.sqrt(variance);
  return std > 0 ? (mean / std) * Math.sqrt(12) : 0;
}

// Balance-bounded simulation parameters ────────────────────────────────────
const MARGIN_CAP_RATIO = 0.95;   // 거래당 사용 가능한 잔고 최대 비율
const LIQ_LEVEL_RATIO  = 0.05;   // 잔고가 starting의 5% 이하로 떨어지면 청산
const TAKER_FEE_PER_SIDE = 0.00055;   // 0.055% Bybit linear-perp taker fee
const FUNDING_8H_MS      = 8 * 3600 * 1000;

// Base-variant lookup: x2/x3 backtests often got wiped out early in the historical
// run, leaving only a handful of trades. The strategy SIGNAL LOGIC is identical
// across leverage variants (same entries/exits), so for sub-period simulation we
// use the base (x1) variant's trades as the signal source and apply leverage
// virtually. This makes "fresh $10k in 2022 with x3" produce the same 165 trades
// as x1 would, just leveraged.
function _baseVariantOf(variant) {
  // Strip the leverage marker _x{n} while preserving any _v{m} or _{combo_id} suffix.
  // long_only_x3        → long_only        long_only_x3_v2  → long_only_v2
  // long_only_x3_257    → long_only_257
  const m = variant.match(/^(.*?)_x[0-9]+(_(?:v[0-9]+|[0-9]+))?$/);
  if (m) return m[1] + (m[2] || '');
  return variant;
}
const _baseResultCache = new Map();
function _baseResultFor(r) {
  if (_baseResultCache.has(r.id)) return _baseResultCache.get(r.id);
  const baseVariant = _baseVariantOf(r.variant);
  let result = r;
  if (baseVariant !== r.variant) {
    const sibling = getAllResults().find(x =>
      x.strat === r.strat && x.tf === r.tf && x.variant === baseVariant);
    if (sibling) result = sibling;
  }
  _baseResultCache.set(r.id, result);
  return result;
}

// Virtual balance simulation: trades in [startMs, endMs] are replayed against
// a fresh $10,000 virtual equity. Each trade uses 95% of current vEq as margin,
// applies the variant's leverage to the base trade's price move to get vPnl.
// Loss is capped at -vMargin (per-trade liquidation when adverse move > 1/lev).
const _balanceSimCache = new Map();
function balanceSim(r, startMs, endMs, cap) {
  if (cap === undefined) cap = MARGIN_CAP_RATIO;
  const key = `${r.id}|${startMs}|${endMs}|${cap}`;
  if (_balanceSimCache.has(key)) return _balanceSimCache.get(key);

  const startBal = 10_000;
  const liqFloor = startBal * LIQ_LEVEL_RATIO;
  const lev      = r.leverage || 1;

  const baseVariant = r.variant.replace(/_x\d+$/, '');
  const variantFundSign = baseVariant === 'long_only' ? 1
                        : baseVariant === 'short_only' ? -1 : null;

  const baseR = _baseResultFor(r);
  const rawTrades = (baseR.trades || [])
    .filter(t => t.t_open >= startMs && t.t_close <= endMs)
    .slice()
    .sort((a, b) => a.t_open - b.t_open);

  let vEq        = startBal;
  let peak       = startBal, mdd = 0;
  let liquidated = false, liqMonth = null;
  const points    = [{ t: startMs, v: vEq }];
  const simTrades = [];

  for (const t of rawTrades) {
    if (liquidated) break;
    if (!(t.entry > 0)) continue;

    const vMargin   = vEq * cap;
    const vNotional = vMargin * lev;
    const priceMove = t.side === 'short'
      ? (t.entry - t.exit) / t.entry
      : (t.exit - t.entry) / t.entry;
    let vPnl = vMargin * lev * priceMove;
    // In leveraged futures, max loss per trade = margin (liquidation).
    if (vPnl < -vMargin) vPnl = -vMargin;

    // Per-trade fee & funding (deducted from equity so they compound)
    const vFee      = vNotional * TAKER_FEE_PER_SIDE * 2;
    const tFundSign = variantFundSign !== null ? variantFundSign : (t.side === 'short' ? -1 : 1);
    const sumRate   = fundingRateSumInWindow(t.t_open, t.t_close);
    const vFunding  = sumRate * vNotional * tFundSign;
    const vNetPnl  = vPnl - vFee - vFunding;

    vEq += vNetPnl;
    if (!liquidated && vEq > peak) peak = vEq;
    if (!liquidated && peak > 0) {
      const dd = (vEq - peak) / peak * 100;
      if (dd < mdd) mdd = dd;
    }
    simTrades.push({
      orig: t,
      vMargin, vNotional, vPnl, vFee, vFunding, vNetPnl,
      capRatio: cap,
      roiPct:   (vNetPnl / vMargin) * 100,
    });
    points.push({ t: t.t_close, v: vEq });

    if (!liquidated && vEq <= liqFloor) {
      liquidated = true;
      liqMonth   = ymOf(t.t_close);
      vEq = 0;
      points[points.length - 1].v = 0;
    }
  }
  points.push({ t: endMs, v: vEq });

  const res = {
    points, finishing: vEq, mdd, peak,
    liquidated, liqMonth,
    trades:           simTrades,
    rawSliceTrades:   rawTrades,
  };
  _balanceSimCache.set(key, res);
  return res;
}

// Fallback equity slice for periods with no trades (e.g. BnH single trade spans whole period).
// BnH: reconstruct from BTC daily price; all others: flat $10,000 line (no positions).
function _rawEquityNormSlice(r, startMs, endMs) {
  if (r.variant === 'buy_and_hold') {
    const btc = DATA.btc_1d;
    const startIdx = btc.findIndex(p => p.t >= startMs);
    if (startIdx < 0) return [{ t: startMs, v: 10_000 }, { t: endMs, v: 10_000 }];
    const startPx = btc[startIdx].c;
    if (!startPx || startPx <= 0) return [{ t: startMs, v: 10_000 }, { t: endMs, v: 10_000 }];
    const qty = 10_000 / startPx;
    const out = [];
    for (const p of btc) {
      if (p.t < startMs || p.t > endMs) continue;
      out.push({ t: p.t, v: qty * p.c });
    }
    if (out.length === 0) return [{ t: startMs, v: 10_000 }, { t: endMs, v: 10_000 }];
    if (out[out.length - 1].t < endMs) out.push({ t: endMs, v: out[out.length - 1].v });
    return out;
  }
  // Non-BnH with no trades in slice: flat $10,000 line
  return [{ t: startMs, v: 10_000 }, { t: endMs, v: 10_000 }];
}

// Equity series for the chart — balance simulation when trades exist,
// otherwise fall back to raw equity norm (BnH and trade-sparse slices).
function slicedEquity(r, startMs, endMs) {
  const sim = balanceSim(r, startMs, endMs);
  if (sim.trades.length === 0) return _rawEquityNormSlice(r, startMs, endMs);
  return sim.points;
}

function slicedStats(r, startMs, endMs) {
  const key = `${r.id}|${startMs}|${endMs}`;
  if (sliceCache.has(key)) return sliceCache.get(key);

  const startBal = 10_000;
  const sim      = balanceSim(r, startMs, endMs);

  // For trade-bearing slices use the simulation; otherwise fall back to
  // raw equity norm (e.g., Buy-and-Hold spans the slice with a single trade).
  let finishing, mdd, liquidated, liq_month, eq, useSim;
  if (sim.trades.length > 0) {
    useSim     = true;
    eq         = sim.points;
    finishing  = sim.finishing;
    mdd        = sim.mdd;
    liquidated = sim.liquidated;
    liq_month  = sim.liqMonth;
  } else {
    useSim     = false;
    eq         = _rawEquityNormSlice(r, startMs, endMs);
    finishing  = eq[eq.length - 1].v;
    let peak   = startBal; mdd = 0;
    liquidated = false; liq_month = null;
    for (const p of eq) {
      if (!liquidated && p.v <= startBal * 0.05) {
        liquidated = true;
        liq_month  = ymOf(p.t);
      }
      if (!liquidated) {
        if (p.v > peak) peak = p.v;
        if (peak > 0) {
          const dd = (p.v - peak) / peak * 100;
          if (dd < mdd) mdd = dd;
        }
      }
    }
    if (liquidated) finishing = 0;
  }

  // CAGR from slice duration
  const years = (endMs - startMs) / (365.25 * 86400_000);
  const cagr  = years > 0 && finishing > 0
    ? ((finishing / startBal) ** (1 / years) - 1) * 100 : 0;

  // Monthly returns from equity points → annualised Sharpe (×√12)
  const monthBoundaries = [startMs];
  {
    const d0 = new Date(startMs);
    let y = d0.getUTCFullYear();
    let m = d0.getUTCMonth();
    if (isFinite(y) && isFinite(m)) {
      let safety = 0;
      while (safety++ < 2000) {
        const next = Date.UTC(y, m, 1);
        if (!isFinite(next) || next >= endMs) break;
        monthBoundaries.push(next);
        m++;
        if (m > 11) { m = 0; y++; }
      }
    }
    monthBoundaries.push(endMs);
  }
  const equityValueAt = (t) => {
    let cur = startBal;
    for (const p of eq) { if (p.t <= t) cur = p.v; else break; }
    return cur;
  };
  const rets = [];
  for (let i = 1; i < monthBoundaries.length; i++) {
    const a = equityValueAt(monthBoundaries[i-1]);
    const b = equityValueAt(monthBoundaries[i]);
    if (a > 0) rets.push((b - a) / a);
  }
  const sharpe = annualisedSharpe(rets);

  // Trade-based stats: use the same simulated PnL the trade table renders.
  let tradeCount, winRate, pf;
  if (useSim) {
    const winning = sim.trades.filter(t => t.vNetPnl > 0);
    const losing  = sim.trades.filter(t => t.vNetPnl <= 0);
    const grossP  = winning.reduce((s, t) => s + t.vNetPnl, 0);
    const grossL  = losing.reduce((s, t)  => s + t.vNetPnl, 0);
    tradeCount = sim.trades.length;
    winRate    = tradeCount ? winning.length / tradeCount * 100 : 0;
    pf         = grossL !== 0 ? grossP / Math.abs(grossL) : (grossP > 0 ? Infinity : 0);
  } else {
    tradeCount = 0; winRate = 0; pf = 0;
  }

  // ── Cost adjustment ──────────────────────────────────────────────────────────
  // Fee cost is always computable from trade count + known FEE_DELTA constant.
  // Funding cost is added from precomputed data when a matching period exists.
  // adj_cagr is always set (never null) so the cost banner is always visible.
  const FEE_DELTA_PER_SIDE = 0.00035;  // maker→taker delta (0.055% - 0.020%)
  const lev = r.leverage || 1;

  // Buy-and-hold has no trading costs
  const isBnH = r.variant === 'buy_and_hold';
  let fee_cost_dyn = 0, fund_cost = 0, fundCoverage = null;

  if (!isBnH && !liquidated && tradeCount > 0 && years > 0) {
    const tradesPerYear = tradeCount / years;
    fee_cost_dyn = parseFloat((tradesPerYear * FEE_DELTA_PER_SIDE * 2 * 100 * lev).toFixed(3));
  }

  // Funding cost from precomputed period data (when an exact preset is active)
  const costKey  = currentCostKey();
  const costData = (costKey && r.costs && r.costs[costKey]) ? r.costs[costKey] : null;
  if (costData) {
    // Use precomputed fee from JSON (more accurate — based on full period trade count)
    // and add funding cost
    fund_cost    = costData.fund || 0;
    fundCoverage = costData.coverage || null;
    // Prefer precomputed fee when available (same formula, avoids floating-point drift)
    fee_cost_dyn = costData.fee || fee_cost_dyn;
  }

  const total_cost = fee_cost_dyn + fund_cost;
  // adj_cagr: raw CAGR minus annual costs; null only for buy-and-hold or 0-trade slices
  const adj_cagr = (!isBnH && tradeCount > 0 && !liquidated)
    ? parseFloat((cagr - total_cost).toFixed(2))
    : null;

  // Effective costData: always synthesise even without precomputed period data
  const effective_cost_data = (!isBnH && tradeCount > 0) ? {
    fee:      fee_cost_dyn,
    fund:     fund_cost,
    coverage: fundCoverage || (costKey ? 'fee_only' : 'dynamic'),
  } : null;

  const stats = {
    cagr, sharpe, mdd,
    trades:    tradeCount,
    win_rate:  winRate,
    pf,
    starting:  startBal,
    finishing,
    net_pct:   (finishing - startBal) / startBal * 100,
    sortino:   sharpe,
    calmar:    mdd !== 0 ? cagr / Math.abs(mdd) : 0,
    liquidated, liq_month,
    adj_cagr,
    cost_data: effective_cost_data,
  };
  sliceCache.set(key, stats);
  return stats;
}

function getStats(r) { return slicedStats(r, state.startMs, state.endMs); }

// ── Sub-period slice helpers ─────────────────────────────────────────────────
// All helpers use the BASE variant data (x1 trades/returns) for x2/x3 results so
// leveraged variants reflect the same signal generation as x1 — not the
// (often wiped-out) leveraged historical backtest.
const _sliceTradesCache = new Map();
function slicedTrades(r, startMs, endMs) {
  const baseR = _baseResultFor(r);
  const key = `${baseR.id}|${startMs}|${endMs}`;
  if (_sliceTradesCache.has(key)) return _sliceTradesCache.get(key);
  const arr = (baseR.trades || []).filter(t => t.t_open >= startMs && t.t_close <= endMs);
  _sliceTradesCache.set(key, arr);
  return arr;
}

const _BASE_MS = Date.UTC(2017, 7, 19);  // returns_daily index origin: 2017-08-19

function slicedReturnsDaily(r, startMs, endMs) {
  const baseR = _baseResultFor(r);
  if (!baseR.returns_daily) return { rets: [], iStart: 0 };
  const iStart = Math.max(0, Math.floor((startMs - _BASE_MS) / 86400000));
  const iEnd   = Math.min(baseR.returns_daily.length, Math.ceil((endMs - _BASE_MS) / 86400000));
  // For leveraged variants, scale daily returns by leverage (approximation).
  const lev = r.leverage || 1;
  const raw = baseR.returns_daily.slice(iStart, iEnd);
  const rets = lev === 1 ? raw : raw.map(v => v * lev);
  return { rets, iStart };
}

// Monthly PnL aggregated from the virtual simulation (correctly reflects leverage
// and fresh $10k starting balance for the slice).
function slicedMonthly(r, startMs, endMs) {
  const sim = balanceSim(r, startMs, endMs);
  const byMonth = new Map();
  for (const st of sim.trades) {
    const ym = ymOf(st.orig.t_close);
    byMonth.set(ym, (byMonth.get(ym) || 0) + st.vNetPnl);
  }
  return Array.from(byMonth, ([month, pnl]) => ({ month, pnl }));
}

function slicedStreaks(r, startMs, endMs) {
  // Use simulated vPnl signs — for x2/x3 a base x1 winning trade may flip to a
  // loss only via the per-trade margin liquidation cap, which slicedTrades can't see.
  const sim = balanceSim(r, startMs, endMs);
  const trades = sim.trades.slice().sort((a, b) => a.orig.t_close - b.orig.t_close);
  let winMax = 0, lossMax = 0, curW = 0, curL = 0;
  for (const t of trades) {
    if (t.vNetPnl > 0)      { curW++; curL = 0; if (curW > winMax)  winMax  = curW; }
    else if (t.vNetPnl < 0) { curL++; curW = 0; if (curL > lossMax) lossMax = curL; }
  }
  return { win_max: winMax, loss_max: lossMax };
}

function updateHeader() {
  const days = Math.round((state.endMs - state.startMs) / 86400_000);
  const el = document.querySelector('.subtitle');
  if (el) el.textContent =
    `초기 자본 $10,000 · ${ymd(state.startMs)} ~ ${ymd(state.endMs)} · ${days}일 · 7가지 전략 · ${DATA.n_results}개 백테스트 · Build ${DATA.build_ts}`;
}

const TF_ORDER   = { '1h': 0, '4h': 1, '1D': 2 };

// ── Sidebar ─────────────────────────────────────────────────
const STRAT_DISPLAY_ORDER = [
  'supertrend','tradeiq_psar_ha','trendtype','supertrend_trendtype',
  'tradeiq_cci_ce','stoch','buy_and_hold',
];

function buildSidebar() {
  const listEl = document.getElementById('strat-list');
  listEl.innerHTML = '';
  const visible = filteredResults();

  if (state.sortMode === 'alpha') {
    // Grouped by strategy name
    const byStrat = DATA.groups;
    const stratOrder = STRAT_DISPLAY_ORDER.filter(s => byStrat[s]);
    const visibleIds = new Set(visible.map(r => r.id));

    for (const stratKey of stratOrder) {
      const items = (byStrat[stratKey] || [])
        .filter(r => visibleIds.has(r.id))
        .sort((a, b) => TF_ORDER[a.tf] - TF_ORDER[b.tf]);
      if (items.length === 0) continue;

      const meta = DATA.meta[stratKey] || {};
      const groupDiv = document.createElement('div');
      groupDiv.className = 'strat-group';

      const header = document.createElement('div');
      header.className = 'strat-group-header';
      const hasSelected = items.some(r => state.selected.includes(r.id));
      if (hasSelected) header.classList.add('open');
      header.innerHTML = `<span class="arrow">▶</span><span>${meta.name_ko || stratKey}</span>`;
      header.addEventListener('click', () => {
        header.classList.toggle('open');
        itemsDiv.classList.toggle('open');
      });

      const itemsDiv = document.createElement('div');
      itemsDiv.className = 'strat-group-items' + (hasSelected ? ' open' : '');

      for (const r of items) {
        const isSelected = state.selected.includes(r.id);
        const item = document.createElement('div');
        item.className = 'strat-item' + (isSelected ? ' selected' : '');
        item.dataset.id = r.id;
        const _balS = getStats(r);
        const balHtml = _balS.liquidated
          ? `<span style="color:#f85149;font-size:11px">💀 $0</span>`
          : `<span style="margin-left:auto;color:#8b949e;font-size:11px">${fmtMultiplier(_balS.starting, _balS.finishing)}</span>`;
        item.innerHTML =
          `<span class="cb">${isSelected ? '✓' : ''}</span>` +
          `<span>${r.tf} · ${varLabel(r)}</span>` +
          balHtml;
        item.addEventListener('click', e => toggleSelect(r.id, e.ctrlKey || e.metaKey || e.shiftKey));
        itemsDiv.appendChild(item);
      }

      groupDiv.appendChild(header);
      groupDiv.appendChild(itemsDiv);
      listEl.appendChild(groupDiv);
    }
  } else {
    // Flat list sorted by return (top10 or return mode); liquidated go to bottom
    // When cost data available, rank by adj_cagr instead of raw finishing balance
    const hasAdj = currentCostKey() !== null;
    let sorted = visible.slice().sort((a, b) => {
      const sa = getStats(a), sb = getStats(b);
      if (sa.liquidated !== sb.liquidated) return sa.liquidated ? 1 : -1;
      if (hasAdj) {
        const ac = sa.adj_cagr ?? sa.cagr, bc = sb.adj_cagr ?? sb.cagr;
        return bc - ac;
      }
      return sb.finishing - sa.finishing;
    });
    if (state.sortMode === 'top10') sorted = sorted.slice(0, 10);

    for (const r of sorted) {
      const meta = DATA.meta[r.strat] || {};
      const isSelected = state.selected.includes(r.id);
      const item = document.createElement('div');
      item.className = 'flat-item' + (isSelected ? ' selected' : '');
      item.dataset.id = r.id;
      const _fs = getStats(r);
      const balHtml = _fs.liquidated
        ? `<span style="flex-shrink:0;color:#f85149;font-size:11px;font-weight:600">💀 $0</span>`
        : `<span style="flex-shrink:0;color:#3fb950;font-size:11px;font-weight:600">${fmtMultiplier(_fs.starting, _fs.finishing)}</span>`;
      item.innerHTML =
        `<span class="cb">${isSelected ? '✓' : ''}</span>` +
        `<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">` +
          `${meta.name_ko||r.strat} · ${r.tf} · ${varLabel(r)}</span>` +
        balHtml;
      item.addEventListener('click', e => toggleSelect(r.id, e.ctrlKey || e.metaKey || e.shiftKey));
      listEl.appendChild(item);
    }
  }

  // Selected count badge
  document.getElementById('selected-count').textContent =
    state.selected.length ? `${state.selected.length}개 선택됨` : '전략을 선택하세요';
}

function toggleSelect(id, multi) {
  if (!multi) {
    if (state.selected.length === 1 && state.selected[0] === id) {
      state.selected = [];
    } else {
      state.selected = [id];
    }
  } else {
    const idx = state.selected.indexOf(id);
    if (idx >= 0) state.selected.splice(idx, 1);
    else if (state.selected.length < 6) state.selected.push(id);
    else { alert('최대 6개까지 비교 가능합니다.'); return; }
  }
  renderAll();
}

// ── Filters ─────────────────────────────────────────────────
function initFilters() {
  document.querySelectorAll('[data-filter]').forEach(el => {
    el.addEventListener('click', () => {
      const key   = el.dataset.filter;
      const value = el.dataset.value;
      const sets = { tf: state.tfFilter, variant: state.variantFilter };
      const s = sets[key];
      if (!s) return;
      if (s.has(value)) {
        if (s.size <= 1) return;
        s.delete(value);
        el.classList.remove('active');
      } else {
        s.add(value);
        el.classList.add('active');
      }
      renderAll();
    });
  });

  document.querySelectorAll('[data-sort]').forEach(el => {
    el.addEventListener('click', () => {
      state.sortMode = el.dataset.sort;
      document.querySelectorAll('[data-sort]').forEach(b => b.classList.remove('active'));
      el.classList.add('active');
      renderAll();
    });
  });
}

// ── KPI Cards ────────────────────────────────────────────────
function renderKPIs() {
  const el = document.getElementById('kpi-grid');
  if (state.selected.length === 0) {
    el.innerHTML = '<div class="empty-state">왼쪽에서 전략을 선택하면 성과 지표가 표시됩니다.<br><small>Ctrl/Shift 클릭으로 최대 6개 비교</small></div>';
    return;
  }
  el.innerHTML = state.selected.map(id => {
    const r = getResultById(id);
    if (!r) return '';
    const s = getStats(r);
    const meta = DATA.meta[r.strat] || {};
    const vl = r.variant === 'buy_and_hold' ? '' : varLabel(r);
    const name = `${meta.name_ko||r.strat} · ${r.tf} · ${vl}`.replace(/\s*·\s*$/, '').trim();
    const hasAdj  = s.adj_cagr !== null && s.adj_cagr !== undefined;
    const dispCagr = hasAdj ? s.adj_cagr : s.cagr;
    const cagrCls = dispCagr >= 0 ? 'kpi-pos' : 'kpi-neg';
    const mddCls  = 'kpi-neg';
    const multStr = s.liquidated ? '💀 0x' : fmtMultiplier(s.starting, s.finishing);
    const liqBanner = s.liquidated
      ? `<div style="background:#3a1c1c;border:1px solid #f85149;border-radius:4px;padding:6px 8px;margin-bottom:8px;font-size:12px">
           💀 <strong style="color:#f85149">청산 발생</strong>
           &nbsp;<span style="color:#c9d1d9">— ${s.liq_month}</span>
           <br><span style="color:#8b949e;font-size:11px">이후 잔고 $0 고정</span>
         </div>` : '';
    // Safety badge: liquidation risk from full-period analysis
    const lr = r.liq_risk;
    const safetyBanner = lr
      ? (lr.verdict === 'ZERO_RISK'
          ? `<div style="background:#0d1f12;border:1px solid #3fb950;border-radius:4px;padding:4px 8px;margin-bottom:6px;font-size:11px;display:flex;align-items:center;gap:6px">
               <span style="color:#3fb950;font-size:13px">🛡</span>
               <span><strong style="color:#3fb950">청산위험 ZERO</strong>
               <span style="color:#6e7681"> · ${lr.period} ${lr.trades}건 전수검사 · 3x liq 미도달</span></span>
             </div>`
          : `<div style="background:#2d1c0e;border:1px solid #e3b341;border-radius:4px;padding:4px 8px;margin-bottom:6px;font-size:11px">
               ⚠️ <strong style="color:#e3b341">청산위험 존재</strong>
               <span style="color:#6e7681"> · ${lr.events}건 위험 이벤트</span>
             </div>`)
      : '';
    // Cost breakdown banner (shown when adj data is available)
    let costBanner = '';
    if (hasAdj && s.cost_data) {
      const cd = s.cost_data;
      const feeTxt  = cd.fee  ? `수수료 -${cd.fee.toFixed(2)}%/yr` : '';
      const fundTxt = cd.fund ? `펀딩 -${cd.fund.toFixed(2)}%/yr`  : '';
      const costParts = [feeTxt, fundTxt].filter(Boolean).join(' · ');
      const isDynamic = cd.coverage === 'dynamic';
      const covColor  = cd.coverage === 'bybit_live' ? '#3fb950'
                      : cd.coverage === 'fee_only'   ? '#8b949e'
                      : isDynamic                    ? '#6e7681'
                      : '#e3b341';
      const covLabel  = cd.coverage === 'bybit_live' ? '실 펀딩 데이터'
                      : cd.coverage === 'fee_only'   ? '수수료만 (펀딩 없음)'
                      : isDynamic                    ? '수수료만 (기간 미매핑)'
                      : '부분 펀딩 데이터';
      costBanner = `<div style="background:#1c2d19;border:1px solid #238636;border-radius:4px;padding:4px 8px;margin-bottom:6px;font-size:11px;color:#8b949e">
        비용 조정: <span style="color:#e06c75">${costParts || '적용 없음'}</span>
        &nbsp;<span style="color:${covColor};font-size:10px">[${covLabel}]</span>
        <br><span style="color:#484f58;font-size:10px">원본 CAGR: ${fmtPct(s.cagr)}/yr → 조정: ${fmtPct(dispCagr)}/yr</span>
      </div>`;
    }
    const cagrLabel = hasAdj ? '조정 CAGR/yr' : 'CAGR/yr';
    return `<div class="kpi-card">
      <div class="kpi-label">${name}</div>
      ${liqBanner}
      ${safetyBanner}
      ${costBanner}
      <div class="kpi-value ${cagrCls}">${multStr}</div>
      <div class="kpi-sub">총수익배수 · ${hasAdj ? '조정 ' : ''}CAGR ${fmtPct(dispCagr)}/yr</div>
      <hr style="border-color:#21262d;margin:8px 0">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px">
        <div><div style="color:#8b949e">Sharpe</div><div style="font-weight:600">${fmtSharpe(s.sharpe)}</div></div>
        <div><div style="color:#8b949e">MDD</div><div class="${mddCls}" style="font-weight:600">${fmtPct(s.mdd)}</div></div>
        <div><div style="color:#8b949e">거래 수</div><div style="font-weight:600">${s.trades}</div></div>
        <div><div style="color:#8b949e">승률</div><div style="font-weight:600">${s.win_rate.toFixed(1)}%</div></div>
        <div><div style="color:#8b949e">${cagrLabel}</div><div style="font-weight:600" class="${cagrCls}">${fmtPct(dispCagr)}</div></div>
        <div><div style="color:#8b949e">최종잔고</div><div style="font-weight:600">${s.liquidated ? '<span style="color:#f85149">💀 $0</span>' : fmtDollar(s.finishing)}</div></div>
      </div>
    </div>`;
  }).join('');
}

// ── Plotly layout helpers ─────────────────────────────────────
function darkLayout(extra) {
  return Object.assign({
    paper_bgcolor: CARD_BG,
    plot_bgcolor:  DARK_BG,
    font:   { color: TEXT_CLR, size: 11 },
    xaxis:  { gridcolor: GRID_CLR, zerolinecolor: GRID_CLR, color: TEXT_CLR },
    yaxis:  { gridcolor: GRID_CLR, zerolinecolor: GRID_CLR, color: TEXT_CLR },
    legend: { bgcolor: 'rgba(0,0,0,0)', bordercolor: GRID_CLR, borderwidth: 1 },
    margin: { l: 60, r: 20, t: 30, b: 40 },
    autosize: true,
  }, extra);
}

// ── Equity Chart ──────────────────────────────────────────────
function renderEquityChart() {
  const div = document.getElementById('chart-equity');
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 수익 곡선이 표시됩니다.</div>';
    return;
  }

  const isAlpha = vState && vState.equityAlpha;
  const isLog   = vState && vState.equityLog;

  // BnH 1D returns for alpha computation
  const bnh = DATA.bnh_returns;
  const bnh_eq = bnh ? cumprod(bnh, 10000) : null;

  const traces = state.selected.map((id, i) => {
    const r = getResultById(id);
    if (!r) return null;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${varLabelShort(r)}/${r.tf}`;
    const eq = slicedEquity(r, state.startMs, state.endMs);
    let xs = eq.map(p => new Date(p.t));
    let ys = eq.map(p => p.v);
    if (isAlpha && bnh_eq) {
      // Map each equity timestamp to nearest BnH daily index; normalise BnH to startMs
      const btcTs = DATA.btc_1d.map(p => p.t);
      let startBtcIdx = 0;
      for (let k=0; k<btcTs.length; k++) { if (btcTs[k] >= state.startMs) { startBtcIdx=k; break; } }
      const bnh_start_v = bnh_eq[Math.min(startBtcIdx, bnh_eq.length-1)];
      ys = ys.map((v, idx) => {
        const ts = eq[idx].t;
        let lo=0, hi=btcTs.length-1;
        while (lo<hi) { const mid=(lo+hi+1)>>1; if (btcTs[mid]<=ts) lo=mid; else hi=mid-1; }
        const bnh_v = bnh_eq[Math.min(lo, bnh_eq.length-1)];
        return (bnh_v > 0 && bnh_start_v > 0) ? v / (bnh_v / bnh_start_v * 10_000) : 1;
      });
    }
    return {
      x: xs, y: ys,
      type: 'scatter', mode: 'lines',
      name: label,
      line: { color: PALETTE[i % PALETTE.length], width: 2 },
      hovertemplate: (isAlpha ? '%{y:.3f}x' : '%{y:$,.0f}') + '<br>%{x|%Y-%m-%d}<extra>' + label + '</extra>',
    };
  }).filter(Boolean);

  // BTC overlay: suppress in alpha mode (alpha curve already normalizes)
  const btc = DATA.btc_1d;
  if (!isAlpha && btc.length > 0) {
    traces.push({
      x: btc.map(p => new Date(p.t)),
      y: btc.map(p => p.c),
      type: 'scatter', mode: 'lines',
      name: 'BTC 가격 (우측축)',
      line: { color: BTC_COLOR, width: 1.5, dash: 'dot' },
      hovertemplate: 'BTC: $%{y:,.0f}<br>%{x|%Y-%m-%d}<extra>BTC 가격</extra>',
      yaxis: 'y2',
    });
  }

  const yAxisCfg = {
    title: isAlpha ? 'BnH 대비 배율 (1.0 = 동일)' : '포트폴리오 잔고 (USD)',
    tickformat: isAlpha ? '.2f' : '$,.0f',
    type: isLog ? 'log' : 'linear',
    gridcolor: GRID_CLR,
    color: TEXT_CLR,
    zerolinecolor: GRID_CLR,
  };
  if (isAlpha) {
    // reference line at 1.0
    yAxisCfg.zeroline = false;
  }

  const layout = darkLayout({
    title: { text: '', font: { size: 12 } },
    xaxis: { type: 'date', gridcolor: GRID_CLR, color: TEXT_CLR, zerolinecolor: GRID_CLR },
    yaxis: yAxisCfg,
    yaxis2: isAlpha ? {} : {
      title: 'BTC 가격 (USD)',
      tickformat: '$,.0f',
      overlaying: 'y',
      side: 'right',
      showgrid: false,
      color: BTC_COLOR,
      tickcolor: BTC_COLOR,
      linecolor: BTC_COLOR,
    },
    hovermode: 'x unified',
    showlegend: true,
    height: 360,
    margin: { l: 70, r: isAlpha ? 20 : 90, t: 30, b: 40 },
    shapes: isAlpha ? [{ type:'line', x0:0,x1:1,y0:1,y1:1, xref:'paper', yref:'y',
                         line:{color:'#f0f6fc',width:1,dash:'dot'} }] : [],
  });

  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive: true, displayModeBar: false });
}

// ── Trade Markers Chart ───────────────────────────────────────
function renderTradeChart() {
  const div = document.getElementById('chart-trades');

  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 거래 마커 차트가 표시됩니다.</div>';
    return;
  }
  if (state.selected.length > 1) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 하나만 선택하면 거래 마커가 표시됩니다.<br><small>현재 ' + state.selected.length + '개 선택됨 — 단일 선택으로 변경하세요</small></div>';
    return;
  }

  const r = getResultById(state.selected[0]);
  const _tcBase = r ? _baseResultFor(r) : null;
  if (!r || !_tcBase || _tcBase.trades.length === 0) {
    div.innerHTML = '<div class="empty-state">이 전략에는 거래 기록이 없습니다.</div>';
    return;
  }
  // Filter BTC candles and trades to selected date range
  const btcFull = DATA.btc_1d;
  const btc = btcFull.filter(p => p.t >= state.startMs && p.t <= state.endMs);
  const candleTrace = {
    type: 'candlestick',
    x:     btc.map(p => new Date(p.t)),
    open:  btc.map(p => p.o), high: btc.map(p => p.h),
    low:   btc.map(p => p.l), close: btc.map(p => p.c),
    name: 'BTC',
    increasing: { line: { color: '#2ea043' }, fillcolor: '#1a7f37' },
    decreasing: { line: { color: '#f85149' }, fillcolor: '#8d2222' },
    showlegend: false,
  };

  // Use base variant trades (same signals as x1) and align with sim.trades
  // (which has leveraged vPnl) by t_open. Index is into sim.trades for row mapping.
  const _tcSim = balanceSim(r, state.startMs, state.endMs);
  const slicedPairs = _tcSim.trades.map((st, i) => [st.orig, i, st.vNetPnl]);
  const longPairs  = slicedPairs.filter(([t]) => t.side === 'long');
  const shortPairs = slicedPairs.filter(([t]) => t.side === 'short');
  const winPairs   = slicedPairs.filter(([_, __, vPnl]) => vPnl > 0);
  const lossPairs  = slicedPairs.filter(([_, __, vPnl]) => vPnl <= 0);

  const mkEntry = (pairs, side, color, symbol) => ({
    type: 'scatter', mode: 'markers',
    x: pairs.map(([t]) => new Date(t.t_open)),
    y: pairs.map(([t]) => t.entry),
    customdata: pairs.map(([_, i]) => [i]),
    name: side === 'long' ? '롱 진입' : '숏 진입',
    marker: { size: 9, color, symbol, line: { color: 'white', width: 1 } },
    hovertemplate: (side === 'long' ? '롱 진입' : '숏 진입') +
      '<br>진입가: $%{y:,.0f}<br>날짜: %{x|%Y-%m-%d}<extra></extra>',
  });

  const mkExit = (pairs, label, color) => ({
    type: 'scatter', mode: 'markers',
    x: pairs.map(([t]) => new Date(t.t_close)),
    y: pairs.map(([t]) => t.exit),
    customdata: pairs.map(([t, i, vPnl]) => [i, vPnl, t.exit]),
    name: label,
    marker: { size: 8, color, symbol: 'square', line: { color: 'white', width: 1 } },
    hovertemplate: label + '<br>종료가: $%{y:,.0f}<br>손익: $%{customdata[1]:+,.0f}<br>날짜: %{x|%Y-%m-%d}<extra></extra>',
  });

  const traces = [
    candleTrace,
    mkEntry(longPairs,  'long',  '#2ea043', 'triangle-up'),
    mkEntry(shortPairs, 'short', '#cf222e', 'triangle-down'),
    mkExit(winPairs,  '수익 종료 ■', '#3fb950'),
    mkExit(lossPairs, '손실 종료 ■', '#f85149'),
  ];

  const meta = DATA.meta[r.strat] || {};
  const title = `${meta.name_ko||r.strat} / ${r.variant} / ${r.tf} — 거래 ${slicedPairs.length}건`;
  const layout = darkLayout({
    title: { text: title, font: { size: 12, color: TEXT_CLR } },
    xaxis: { type: 'date', gridcolor: GRID_CLR, rangeslider: { visible: false } },
    yaxis: { title: 'BTC 가격 (USD)', tickformat: '$,.0f', gridcolor: GRID_CLR },
    height: 420,
    hovermode: 'closest',
    showlegend: true,
    legend: { orientation: 'h', x: 0, y: 1.06, font: { size: 11 } },
  });

  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive: true, displayModeBar: false });

  // Click handler: highlight matching row in trade history table
  div.removeAllListeners && div.removeAllListeners('plotly_click');
  div.on('plotly_click', (ev) => {
    const pt = ev?.points?.[0];
    if (!pt || pt.curveNumber === 0) return;   // curveNumber 0 = candlestick
    const tradeIdx = pt.customdata?.[0];
    if (typeof tradeIdx === 'number') renderTradeTable(tradeIdx);
  });
}

// ── Trade History Table ───────────────────────────────────────
function renderTradeTable(focusIdx = null) {
  const el = document.getElementById('trade-history-table');
  if (!el) return;

  if (state.selected.length === 0) {
    el.innerHTML = '<div class="trade-detail-wrap"><h5 style="color:#8b949e">전략을 선택하면 거래 내역이 표시됩니다.</h5></div>';
    return;
  }
  if (state.selected.length > 1) {
    el.innerHTML = '<div class="trade-detail-wrap"><h5 style="color:#8b949e">단일 전략 선택 시 거래 내역이 표시됩니다. (현재 ' + state.selected.length + '개 선택)</h5></div>';
    return;
  }

  const r = getResultById(state.selected[0]);
  if (!r) { el.innerHTML = ''; return; }

  // Use the balance-bounded simulation as the single source of truth.
  // Each entry in sim.trades has the original trade in .orig plus simulated
  // vMargin / vNotional / vPnl / capRatio / roiPct.
  const sim    = balanceSim(r, state.startMs, state.endMs);
  const trades = sim.trades;

  if (trades.length === 0) {
    const note = r.strat === 'buy_and_hold'
      ? '<br><small>BnH 는 전체 기간을 보유하므로 슬라이스 내 거래가 없습니다 — 손익은 BTC 가격 변화로 KPI 카드에 반영됩니다.</small>'
      : '';
    el.innerHTML = `<div class="trade-detail-wrap"><h5 style="color:#8b949e">선택 기간에 진입+청산이 모두 발생한 거래가 없습니다.${note}</h5></div>`;
    return;
  }

  function fmtMs(ms) {
    const d = new Date(Math.round(ms / 14400000) * 14400000);
    const p = n => String(n).padStart(2, '0');
    const yy = String(d.getUTCFullYear()).slice(2);
    return `${yy}-${p(d.getUTCMonth()+1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
  }
  function fmtDur(ms) {
    const totalMin = Math.round(ms / 60_000);
    const days = Math.floor(totalMin / 1440);
    const hrs  = Math.floor((totalMin % 1440) / 60);
    const mins = totalMin % 60;
    if (days >= 1) return `${days}d ${hrs}h`;
    if (hrs >= 1)  return `${hrs}h ${mins}m`;
    return `${mins}m`;
  }

  const lev = r.leverage;
  const levLabel = lev > 1 ? lev + 'x' : '1x';

  // ── Per-trade fee & funding ─────────────────────────────────────────────────
  // Values are computed inside balanceSim() so equity reflects the cost drag.
  // Here we just read the stored vFee / vFunding / vNetPnl for display.
  const baseVariant = r.variant.replace(/_x\d+$/, '');

  let totalPnl = 0, totalDur = 0, totalFee = 0, totalFunding = 0, wins = 0;
  const rows = trades.map((st, rowN) => {
    const t       = st.orig;
    const origIdx = rowN;
    const isFocus = focusIdx !== null && origIdx === focusIdx;

    const dispPnl = st.vNetPnl;
    const cls    = dispPnl > 0 ? 'kpi-pos' : 'kpi-neg';
    const sideKo = t.side === 'long' ? '🟢 롱' : '🔴 숏';
    const dur    = t.t_close - t.t_open;

    const tradeFee     = st.vFee;
    const tradeFunding = st.vFunding;
    // hasFundingData: true if any 8h event exists in the trade's hold window
    const hasFundingData = DATA.funding_ts && DATA.funding_ts.length > 0;

    totalPnl += dispPnl;
    totalDur += dur;
    totalFee += tradeFee;
    totalFunding += tradeFunding;
    if (dispPnl > 0) wins++;

    const feeCls = 'kpi-neg';
    const fundCls = tradeFunding > 0 ? 'kpi-neg' : tradeFunding < 0 ? 'kpi-pos' : '';
    const feeStr  = tradeFee !== 0
      ? `<span class="${feeCls}">$${(-tradeFee).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</span>`
      : `<span style="color:#484f58">-</span>`;
    const fundStr = tradeFunding !== 0
      ? `<span class="${fundCls}">$${(-tradeFunding).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</span>`
      : `<span style="color:#484f58">${hasFundingData ? '0' : '-'}</span>`;

    const focusCls  = isFocus ? ' class="focused-row"' : '';
    const focusAttr = ` data-trade-idx="${origIdx}"`;
    return `<tr${focusCls}${focusAttr}>
      <td style="color:#8b949e;text-align:right">${rowN + 1}</td>
      <td>${sideKo}</td>
      <td style="text-align:center">${levLabel}</td>
      <td>${fmtMs(t.t_open)}</td>
      <td style="text-align:right">$${t.entry.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td style="text-align:right;line-height:1.3">
        <div>$${st.vMargin.toLocaleString('en-US',{maximumFractionDigits:0})} → $${st.vNotional.toLocaleString('en-US',{maximumFractionDigits:0})}</div>
        <div style="color:#8b949e;font-size:0.85em">margin × ${lev}x · ${(st.capRatio*100).toFixed(0)}% of bal</div>
      </td>
      <td>${fmtMs(t.t_close)}</td>
      <td style="text-align:right">$${t.exit.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td style="text-align:right;color:#8b949e">${fmtDur(dur)}</td>
      <td style="text-align:right">${feeStr}</td>
      <td style="text-align:right">${fundStr}</td>
      <td style="text-align:right" class="${cls}">$${dispPnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</td>
      <td style="text-align:right" class="${cls}">${st.roiPct.toFixed(1)}%</td>
    </tr>`;
  });

  const totalCls     = totalPnl > 0 ? 'kpi-pos' : 'kpi-neg';
  const totalFeeCls  = 'kpi-neg';
  const totalFundCls = totalFunding > 0 ? 'kpi-neg' : totalFunding < 0 ? 'kpi-pos' : '';
  const avgDurMs     = trades.length > 0 ? totalDur / trades.length : 0;
  const losses       = trades.length - wins;
  const meta         = DATA.meta[r.strat] || {};
  const stratLabel   = `${meta.name_ko||r.strat} / ${varLabel(r)} / ${r.tf}`;
  const fundSign     = baseVariant === 'long_only' ? 1 : baseVariant === 'short_only' ? -1 : 0;
  const fundNote     = (fundSign !== 0 && (!DATA.funding_ts || !DATA.funding_ts.length))
    ? `<span style="color:#484f58;font-size:10px"> · 펀딩비: 데이터 없음</span>` : '';

  el.innerHTML = `<div class="trade-detail-wrap">
    <h5>${stratLabel} — 거래 내역 ${trades.length}건 · 승 ${wins} / 패 ${losses} · 누적 손익 <span class="${totalCls}">$${totalPnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</span> · 평균 보유 ${fmtDur(avgDurMs)}${fundNote}</h5>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th style="text-align:right">#</th>
          <th>방향</th>
          <th style="text-align:center">레버리지</th>
          <th>진입일</th>
          <th style="text-align:right">진입가</th>
          <th style="text-align:right">포지션 (증거금/명목)</th>
          <th>종료일</th>
          <th style="text-align:right">종료가</th>
          <th style="text-align:right">보유</th>
          <th style="text-align:right">수수료</th>
          <th style="text-align:right">펀딩비</th>
          <th style="text-align:right">손익 ($)</th>
          <th style="text-align:right">손익 (%)</th>
        </tr></thead>
        <tbody>${rows.join('')}</tbody>
      </table>
    </div>
  </div>`;

  // Scroll focused row into view
  if (focusIdx !== null) {
    const focusedRow = el.querySelector('tr.focused-row');
    if (focusedRow) focusedRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
}

// ── Monthly Heatmap ───────────────────────────────────────────
function renderHeatmap() {
  const div = document.getElementById('chart-heatmap');
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 월별 손익 히트맵이 표시됩니다.</div>';
    return;
  }
  // Build month labels dynamically from the selected date range
  const months = [];
  {
    const d0 = new Date(state.startMs);
    let y = d0.getUTCFullYear(), m = d0.getUTCMonth() + 1;
    const dE = new Date(state.endMs);
    const yE = dE.getUTCFullYear(), mE = dE.getUTCMonth() + 1;
    while (y < yE || (y === yE && m <= mE)) {
      months.push(`${y}-${String(m).padStart(2, '0')}`);
      m++; if (m > 12) { m = 1; y++; }
    }
  }

  const zData = [], yLabels = [];

  for (const id of state.selected) {
    const r = getResultById(id);
    if (!r) continue;
    const map = {};
    for (const row of slicedMonthly(r, state.startMs, state.endMs)) map[row.month] = row.pnl;
    zData.push(months.map(m => map[m] ?? 0));
    const meta = DATA.meta[r.strat] || {};
    yLabels.push(`${meta.name_ko||r.strat}/${r.tf}`);
  }

  const trace = {
    type: 'heatmap',
    z: zData, x: months, y: yLabels,
    colorscale: [
      [0, '#8d1515'], [0.45, '#3a1010'], [0.5, '#1c2128'],
      [0.55, '#0c2a16'], [1, '#1a7f37'],
    ],
    zmid: 0,
    hovertemplate: '%{y}<br>%{x}: $%{z:+,.0f}<extra></extra>',
    showscale: true,
    colorbar: { tickformat: '$,.0f', thickness: 12, len: 0.8, tickfont: { size: 10 } },
  };

  const layout = darkLayout({
    height: Math.max(120, yLabels.length * 48 + 60),
    xaxis: { tickangle: -45, nticks: 24, gridcolor: GRID_CLR },
    yaxis: { automargin: true, gridcolor: GRID_CLR },
    margin: { l: 180, r: 80, t: 20, b: 70 },
  });

  Plotly.purge(div);
  Plotly.newPlot(div, [trace], layout, { responsive: true, displayModeBar: false });

  // Click handler: show trade detail table for clicked cell
  div.removeAllListeners && div.removeAllListeners('plotly_click');
  div.on('plotly_click', function(eventData) {
    if (!eventData.points || !eventData.points[0]) return;
    const pt = eventData.points[0];
    const month = pt.x;
    const rowIdx = Array.isArray(pt.pointNumber) ? pt.pointNumber[0] : pt.pointNumber;
    const id = state.selected[rowIdx];
    if (id) showTradeDetail(id, month);
  });
}

// ── Trade Detail Table (heatmap click) ───────────────────────
function showTradeDetail(id, month) {
  const el = document.getElementById('trade-detail-table');
  const r = getResultById(id);
  if (!r) { el.innerHTML = ''; return; }

  const [yearStr, moStr] = month.split('-');
  const year = parseInt(yearStr), mo = parseInt(moStr);
  const moStartMs = Date.UTC(year, mo - 1, 1);
  const moEndMs   = Date.UTC(year, mo, 1);

  // Filter trades closed in this month from the CURRENT sub-period sim —
  // so PnL values match what the heatmap shows for the same cell.
  const sim = balanceSim(r, state.startMs, state.endMs);
  const trades = sim.trades.filter(st => st.orig.t_close >= moStartMs && st.orig.t_close < moEndMs);

  const meta = DATA.meta[r.strat] || {};
  const stratLabel = `${meta.name_ko||r.strat} / ${varLabel(r)} / ${r.tf}`;

  if (trades.length === 0) {
    el.innerHTML = `<div class="trade-detail-wrap"><h5>${stratLabel} — ${month}: 해당 월에 청산된 거래 없음</h5></div>`;
    el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    return;
  }

  function fmtMs(ms) {
    const d = new Date(Math.round(ms / 14400000) * 14400000);
    const pad = n => String(n).padStart(2,'0');
    const yy = String(d.getUTCFullYear()).slice(2);
    return `${yy}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
  }

  const totalPnl = trades.reduce((s, st) => s + st.vNetPnl, 0);
  const rows = trades.map(st => {
    const t = st.orig;
    const cls = st.vNetPnl > 0 ? 'kpi-pos' : 'kpi-neg';
    const sideKo = t.side === 'long' ? '롱' : '숏';
    return `<tr>
      <td>${sideKo}</td>
      <td>${fmtMs(t.t_open)}</td>
      <td style="text-align:right">$${t.entry.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td>${fmtMs(t.t_close)}</td>
      <td style="text-align:right">$${t.exit.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td style="text-align:right" class="${cls}">$${st.vNetPnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</td>
      <td style="text-align:right;color:#8b949e">$${st.vFee.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
    </tr>`;
  }).join('');

  const totalCls = totalPnl > 0 ? 'kpi-pos' : 'kpi-neg';
  el.innerHTML = `<div class="trade-detail-wrap">
    <h5>${stratLabel} — ${month} 거래 내역 (${trades.length}건 청산 · 합계: <span class="${totalCls}">$${totalPnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</span>)</h5>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>방향</th><th>진입 시각</th><th style="text-align:right">진입가</th>
          <th>종료 시각</th><th style="text-align:right">종료가</th>
          <th style="text-align:right">손익 ($)</th><th style="text-align:right">수수료</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  </div>`;
  el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ── Strategy Description ──────────────────────────────────────
function renderDescription() {
  const el = document.getElementById('desc-panel');
  // Collect unique strategies from selected
  const stratKeys = [...new Set(state.selected.map(id => {
    const r = getResultById(id);
    return r ? r.strat : null;
  }).filter(Boolean))];

  if (stratKeys.length === 0) {
    el.innerHTML = '<div class="empty-state">전략을 선택하면 설명이 표시됩니다.</div>';
    return;
  }

  // Tabs
  const tabsHtml = stratKeys.map((k, i) => {
    const m = DATA.meta[k] || {};
    return `<span class="desc-tab${i===0?' active':''}" data-strat="${k}">${m.name_ko||k}</span>`;
  }).join('');

  const contentsHtml = stratKeys.map((k, i) => {
    const m = DATA.meta[k] || {};
    const mkSection = (koList, title) => {
      if (!koList || koList.length === 0) return '';
      return `<div class="desc-section">
        <h4>${title}</h4>
        <ul class="desc-list">${koList.map(s=>`<li>${s}</li>`).join('')}</ul>
      </div>`;
    };

    const selectedOfStrat = state.selected
      .map(id => getResultById(id))
      .filter(r => r && r.strat === k);
    const hasBidi = selectedOfStrat.some(r => r.variant === 'bidirectional');

    return `<div class="desc-content${i===0?' active':''}" data-strat="${k}">
      <div class="desc-section">
        <h4>전략 개요</h4>
        <p style="font-size:12px;color:#8b949e;line-height:1.6">${m.summary_ko||''}</p>
      </div>
      ${mkSection(m.entry_long_ko, '롱 진입 조건')}
      ${hasBidi ? mkSection(m.entry_short_ko, '숏 진입 조건') : ''}
      ${mkSection(m.exit_ko, '청산 / 손절 조건')}
      <div class="desc-section">
        <h4>사용 지표</h4>
        <div class="indicators-row">${(m.indicators||[]).map(ind=>`<span class="ind-chip">${ind}</span>`).join('')}</div>
      </div>
      <div class="desc-section">
        <h4>변형 비교</h4>
        <div style="font-size:12px;color:#8b949e;line-height:1.7">
          <b>롱전용 (long_only)</b>: 롱 진입 조건만 사용. 하락장에서 현금 보유.<br>
          <b>양방향 (bidirectional)</b>: 롱·숏 모두 진입. 하락장에서도 수익 추구.
        </div>
      </div>
      ${(()=>{
        const op = m.optimal_params || {};
        const tfs = ['1h','4h','1D'].filter(tf => op[tf]);
        if (tfs.length === 0) return '';
        const tfRows = tfs.map(tf => {
          const e = op[tf];
          const pkeys = Object.keys(e.params);
          const pvals = pkeys.map(k => `<b>${k}</b>=${e.params[k]}`).join(', ');
          const varBadge = e.variant === 'long_only'
            ? `<span style="color:#3fb950;font-size:10px">롱전용</span>`
            : `<span style="color:#f78166;font-size:10px">양방향</span>`;
          const verBadge = `<span style="color:#8b949e;font-size:10px">${e.version}</span>`;
          const p0Str   = e.p0_cagr !== undefined ? `P0 ${e.p0_cagr>0?'+':''}${e.p0_cagr}% / ` : '';
          const cagrStr = `${p0Str}P1 ${e.p1_cagr>0?'+':''}${e.p1_cagr}% / P4 ${e.p4_cagr>0?'+':''}${e.p4_cagr}%`;
          const mddStr  = `MDD P0 ${e.p0_mdd??'n/a'}% / P1 ${e.p1_mdd}%`;
          return `<tr>
            <td style="padding:5px 8px;font-weight:700;color:#58a6ff;font-size:12px">${tf}</td>
            <td style="padding:5px 8px;font-size:11px;color:#c9d1d9">${pvals}</td>
            <td style="padding:5px 8px;font-size:10px;color:#8b949e">${varBadge} ${verBadge}</td>
            <td style="padding:5px 8px;font-size:10px;color:#3fb950">score ${e.score>0?'+':''}${e.score}</td>
            <td style="padding:5px 8px;font-size:10px;color:#8b949e">${cagrStr} | ${mddStr}</td>
          </tr>`;
        }).join('');
        const _isSubPeriod = (state.startMs !== DATA.data_start_ms || state.endMs !== DATA.data_end_ms);
        return `<div class="desc-section" style="${_isSubPeriod ? 'opacity:0.5' : ''}">
          <h4>TF별 최적 파라미터 (v2+v3 스윕 결과)</h4>
          ${_isSubPeriod ? '<div style="font-size:10px;color:#8b949e;margin-bottom:6px">전체 기간 옵티마이저 결과 (기간 필터 무관)</div>' : ''}
          <div style="overflow-x:auto">
          <table style="width:100%;border-collapse:collapse;font-size:12px">
            <thead><tr style="border-bottom:1px solid #30363d">
              <th style="padding:5px 8px;color:#8b949e;text-align:left">TF</th>
              <th style="padding:5px 8px;color:#8b949e;text-align:left">최적 파라미터</th>
              <th style="padding:5px 8px;color:#8b949e;text-align:left">Variant</th>
              <th style="padding:5px 8px;color:#8b949e;text-align:left">Score</th>
              <th style="padding:5px 8px;color:#8b949e;text-align:left">성과 요약 (P0/P1/P4)</th>
            </tr></thead>
            <tbody>${tfRows}</tbody>
          </table>
          </div>
          <div style="font-size:10px;color:#8b949e;margin-top:6px">
            Score = 4기간(P1~P4) CAGR 평균 (MDD≥-35% 조건 충족 시). P4 = 2022-12~2025-09 (Bull run)
          </div>
        </div>`;
      })()}
    </div>`;
  }).join('');

  el.innerHTML = `<div class="desc-strat-tabs">${tabsHtml}</div>${contentsHtml}`;

  el.querySelectorAll('.desc-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      el.querySelectorAll('.desc-tab').forEach(t => t.classList.remove('active'));
      el.querySelectorAll('.desc-content').forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      el.querySelector(`.desc-content[data-strat="${tab.dataset.strat}"]`).classList.add('active');
    });
  });
}

// ════════════════════════════════════════════════════════
//  VALIDATION VIEWS
// ════════════════════════════════════════════════════════

// ── JS math helpers ──────────────────────────────────────
const BTC_DATES = DATA.btc_1d.map(p => new Date(p.t));  // parallel to btc_1d
const N_DAYS = __N_DAYS__;  // returns_daily length (2017-08-19..2026-04-30)
// Day-index corresponding to a returns_daily index: i → 2017-08-19 + i days
function dayLabel(i) {
  const d = new Date(Date.UTC(2017, 7, 19) + i * 86400000);
  return d.toISOString().slice(0,10);
}

function cumprod(rets, start) {
  // rets: daily returns array; start: initial equity value
  const out = [start];
  let v = start;
  for (const r of rets) { v *= (1 + r); out.push(v); }
  return out;
}

function rollingSharpe(rets, window) {
  // Annualised Sharpe per rolling window (252 trading days proxy)
  const result = new Array(rets.length).fill(null);
  for (let i = window - 1; i < rets.length; i++) {
    const seg = rets.slice(i - window + 1, i + 1);
    const mu = seg.reduce((a,b) => a+b, 0) / seg.length;
    const variance = seg.reduce((a,b) => a + (b-mu)**2, 0) / seg.length;
    const std = Math.sqrt(variance);
    result[i] = std > 0 ? (mu / std) * Math.sqrt(252) : 0;
  }
  return result;
}

function rollingWinRate(rets, window) {
  const result = new Array(rets.length).fill(null);
  for (let i = window - 1; i < rets.length; i++) {
    const seg = rets.slice(i - window + 1, i + 1);
    result[i] = seg.filter(r => r > 0).length / seg.length * 100;
  }
  return result;
}

function drawdownSeries(rets, start) {
  // Returns array of drawdown % (negative values)
  const eq = cumprod(rets, start);
  const dd = [];
  let peak = eq[0];
  for (const v of eq) {
    if (v > peak) peak = v;
    dd.push(peak > 0 ? (v - peak) / peak * 100 : 0);
  }
  return dd;  // length = rets.length + 1
}

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  let sa=0, sb=0, saa=0, sbb=0, sab=0;
  for (let i=0; i<n; i++) { sa+=a[i]; sb+=b[i]; saa+=a[i]*a[i]; sbb+=b[i]*b[i]; sab+=a[i]*b[i]; }
  const num = n*sab - sa*sb;
  const den = Math.sqrt((n*saa - sa*sa) * (n*sbb - sb*sb));
  return den > 0 ? num/den : 0;
}

function normalPdf(x, mu, sigma) {
  if (sigma <= 0) return 0;
  return Math.exp(-0.5*((x-mu)/sigma)**2) / (sigma * Math.sqrt(2*Math.PI));
}

function skewness(arr) {
  const n = arr.length; if (n < 3) return 0;
  const mu = arr.reduce((a,b)=>a+b,0)/n;
  const s2 = arr.reduce((a,b)=>a+(b-mu)**2,0)/n;
  const s3 = arr.reduce((a,b)=>a+(b-mu)**3,0)/n;
  return s2 > 0 ? s3/s2**1.5 : 0;
}

function kurtosis(arr) {
  const n = arr.length; if (n < 4) return 0;
  const mu = arr.reduce((a,b)=>a+b,0)/n;
  const s2 = arr.reduce((a,b)=>a+(b-mu)**2,0)/n;
  const s4 = arr.reduce((a,b)=>a+(b-mu)**4,0)/n;
  return s2 > 0 ? s4/s2**2 - 3 : 0;  // excess kurtosis
}

function bernoulliMaxStreak(n, p) {
  // Expected max win streak for n trials with probability p
  if (p <= 0 || p >= 1) return 0;
  return Math.log(n) / Math.log(1/p);
}

// Look up BTC regime for a given ms timestamp (binary search on btc_1d)
function getRegimeForMs(ms) {
  const tms = DATA.btc_1d;
  let lo=0, hi=tms.length-1;
  while (lo < hi) {
    const mid = (lo+hi+1)>>1;
    if (tms[mid].t <= ms) lo=mid; else hi=mid-1;
  }
  return DATA.regime_labels[lo] || 'SIDE';
}

// ── Section state ──────────────────────────────────────────
const vState = {
  equityLog:   false,
  equityAlpha: false,
  rollMetric:  'sharpe',  // 'sharpe'|'winrate'
  rollWindow:  60,
  mcRunning:   false,
};

// ── A1: Equity chart toggles ──────────────────────────────
// Patch onto renderEquityChart — override is applied via wrapper
function applyEquityToggles() {
  document.querySelectorAll('.eq-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.toggle;
      vState[key] = !vState[key];
      btn.classList.toggle('active', vState[key]);
      renderEquityChart();
    });
  });
}

// Monkey-patch renderEquityChart to apply log/alpha transforms
const _origRenderEquity = renderEquityChart;
// (Override placed at end of DASHBOARD_JS init section)

// ── A2: Underwater Chart ──────────────────────────────────
function renderUnderwaterChart() {
  const div = document.getElementById('chart-underwater');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 Drawdown 차트가 표시됩니다.</div>';
    return;
  }
  const traces = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const eq = slicedEquity(r, state.startMs, state.endMs);
    let peak = eq[0]?.v || 10000;
    const xs = [], ys = [];
    for (const p of eq) {
      if (p.v > peak) peak = p.v;
      xs.push(new Date(p.t).toISOString().slice(0, 10));
      ys.push(peak > 0 ? (p.v - peak) / peak * 100 : 0);
    }
    traces.push({
      x: xs, y: ys, type: 'scatter', mode: 'lines', name: label, fill: 'tozeroy',
      fillcolor: PALETTE[i % PALETTE.length].replace(')', ',0.15)').replace('rgb','rgba'),
      line: { color: PALETTE[i % PALETTE.length], width: 1.5 },
      hovertemplate: '%{y:.2f}%<br>%{x}<extra>'+label+'</extra>',
    });
  }
  const layout = darkLayout({
    height: 200, margin: { l:60,r:20,t:20,b:40 },
    xaxis: { type:'date', gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { ticksuffix:'%', gridcolor:GRID_CLR, color:TEXT_CLR, zerolinecolor:'#30363d' },
    hovermode: 'x unified', showlegend: false,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── A3: Rolling Metrics Chart ─────────────────────────────
function renderRollingMetricsChart() {
  const div = document.getElementById('chart-rolling');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 롤링 지표가 표시됩니다.</div>';
    return;
  }
  const w = vState.rollWindow;
  const metric = vState.rollMetric;
  const traces = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const { rets, iStart } = slicedReturnsDaily(r, state.startMs, state.endMs);
    if (rets.length < w) {
      traces.push({ x: [], y: [], type: 'scatter', mode: 'lines', name: label,
        line: { color: PALETTE[i % PALETTE.length], width: 2 } });
      continue;
    }
    let ys;
    if (metric === 'sharpe') ys = rollingSharpe(rets, w);
    else                     ys = rollingWinRate(rets, w);
    const xs = [];
    for (let j=0; j<rets.length; j++) xs.push(dayLabel(iStart + j));
    // Replace nulls with undefined for Plotly gap
    traces.push({
      x: xs, y: ys.map(v => v===null ? undefined : v), type: 'scatter', mode: 'lines',
      name: label, connectgaps: false,
      line: { color: PALETTE[i % PALETTE.length], width: 2 },
      hovertemplate: '%{y:.3f}<br>%{x}<extra>'+label+'</extra>',
    });
  }
  const yTitle = metric === 'sharpe' ? `Rolling Sharpe (${w}d)` : `Rolling WinRate % (${w}d)`;
  const layout = darkLayout({
    height: 260, margin: { l:70,r:20,t:20,b:40 },
    xaxis: { type:'date', gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { title:yTitle, ticksuffix: metric==='winrate'?'%':'', gridcolor:GRID_CLR, color:TEXT_CLR, zerolinecolor:'#388bfd' },
    hovermode: 'x unified', showlegend: state.selected.length > 1,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── B1: Trade PnL Histogram ────────────────────────────────
function renderPnlHistogram() {
  const div = document.getElementById('chart-pnl-hist');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 PnL 분포가 표시됩니다.</div>';
    return;
  }
  const traces = [];
  const annotations = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const sim = balanceSim(r, state.startMs, state.endMs);
    if (sim.trades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const pnls = sim.trades.map(t => t.vNetPnl);
    const mu = pnls.reduce((a,b)=>a+b,0) / pnls.length;
    const sigma = Math.sqrt(pnls.reduce((a,b)=>a+(b-mu)**2,0)/pnls.length);
    const sk = skewness(pnls);
    const ku = kurtosis(pnls);
    // Sort pnl and find top-5 contribution
    const sorted = [...pnls].sort((a,b)=>b-a);
    const totalProfit = pnls.filter(p=>p>0).reduce((a,b)=>a+b,0);
    const top5Profit  = sorted.slice(0,5).filter(p=>p>0).reduce((a,b)=>a+b,0);
    const top5Pct = totalProfit > 0 ? top5Profit/totalProfit*100 : 0;
    traces.push({
      x: pnls, type: 'histogram', name: label, opacity: 0.65, nbinsx: 30,
      marker: { color: PALETTE[i % PALETTE.length] },
      hovertemplate: 'PnL: $%{x:.0f}<br>거래수: %{y}<extra>'+label+'</extra>',
    });
    annotations.push(
      `<span style="color:${PALETTE[i%PALETTE.length]}">${label}</span>: ` +
      `왜도 ${sk.toFixed(2)}, 첨도 ${ku.toFixed(2)}, ` +
      `상위 5건 수익 기여 <b>${top5Pct.toFixed(1)}%</b>`
    );
  }
  const layout = darkLayout({
    barmode: 'overlay', height: 260, margin: { l:60,r:20,t:20,b:40 },
    xaxis: { title:'PnL ($)', gridcolor:GRID_CLR, color:TEXT_CLR, tickprefix:'$' },
    yaxis: { title:'거래 수', gridcolor:GRID_CLR, color:TEXT_CLR },
    showlegend: state.selected.length > 1,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
  // Annotation below chart
  const ann = document.getElementById('pnl-hist-annotation');
  if (ann) ann.innerHTML = annotations.join('<br>');
}

// ── B2: Cumulative Trade Count ─────────────────────────────
function renderCumTradesChart() {
  const div = document.getElementById('chart-cum-trades');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 누적 거래 수가 표시됩니다.</div>';
    return;
  }
  const traces = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const trades = slicedTrades(r, state.startMs, state.endMs);
    const lowCount = trades.length < 50;
    const color = lowCount ? '#f85149' : PALETTE[i % PALETTE.length];
    if (trades.length === 0) continue;
    const xs = trades.map(t => new Date(t.t_close));
    const ys = trades.map((_, idx) => idx + 1);
    traces.push({
      x: xs, y: ys, type: 'scatter', mode: 'lines', name: label,
      line: { color, width: 2 },
      hovertemplate: '누적 %{y}건<br>%{x|%Y-%m-%d}<extra>'+label+'</extra>',
    });
  }
  const layout = darkLayout({
    height: 220, margin: { l:60,r:20,t:20,b:40 },
    xaxis: { type:'date', gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { title:'누적 거래 수', gridcolor:GRID_CLR, color:TEXT_CLR },
    hovermode: 'x unified', showlegend: state.selected.length > 1,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── B3: Trade Duration Distribution ───────────────────────
function renderDurationChart() {
  const div = document.getElementById('chart-duration');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 보유 기간 분포가 표시됩니다.</div>';
    return;
  }
  const traces = [];
  const warnings = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const _durTrades = slicedTrades(r, state.startMs, state.endMs);
    if (_durTrades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    // Duration in hours
    const durations = _durTrades.map(t => (t.t_close - t.t_open) / 3600000);
    // Check anomaly: >90% in same hour-bucket
    const bins = {};
    for (const d of durations) {
      const b = Math.floor(d);
      bins[b] = (bins[b]||0)+1;
    }
    const maxBinPct = Math.max(...Object.values(bins)) / durations.length;
    if (maxBinPct > 0.9) warnings.push(`⚠ ${label}: 거래의 ${(maxBinPct*100).toFixed(0)}%가 동일 duration → 버그/룩어헤드 의심`);
    traces.push({
      x: durations, type: 'histogram', name: label, opacity: 0.65, nbinsx: 40,
      marker: { color: PALETTE[i % PALETTE.length] },
      hovertemplate: '%{x:.1f}h<br>%{y}건<extra>'+label+'</extra>',
    });
  }
  const layout = darkLayout({
    barmode: 'overlay', height: 240, margin: { l:60,r:20,t:20,b:40 },
    xaxis: { title:'보유 기간 (시간)', gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { title:'거래 수', gridcolor:GRID_CLR, color:TEXT_CLR },
    showlegend: state.selected.length > 1,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
  const warn = document.getElementById('duration-warnings');
  if (warn) warn.innerHTML = warnings.map(w=>`<div class="warn-badge">${w}</div>`).join('');
}

// ── B4: Win/Loss Streak Chart ─────────────────────────────
function renderStreaksChart() {
  const div = document.getElementById('chart-streaks');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 연속 스트릭이 표시됩니다.</div>';
    return;
  }
  const labels = [], winAct = [], lossAct = [], winExp = [], lossExp = [];
  for (const id of state.selected) {
    const r = getResultById(id);
    if (!r) continue;
    const meta = DATA.meta[r.strat] || {};
    labels.push(`${meta.name_ko||r.strat}/${r.tf}/${varLabelShort(r)}`);
    const sk = slicedStreaks(r, state.startMs, state.endMs);
    winAct.push(sk.win_max);
    lossAct.push(sk.loss_max);
    const sst = slicedStats(r, state.startMs, state.endMs);
    const n = sst.trades;
    const p = sst.win_rate / 100;
    winExp.push(n>0 && p>0 && p<1 ? bernoulliMaxStreak(n, p) : 0);
    lossExp.push(n>0 && p>0 && p<1 ? bernoulliMaxStreak(n, 1-p) : 0);
  }
  const traces = [
    { name:'실측 연승', x:labels, y:winAct, type:'bar', marker:{color:'#3fb950'}, orientation:'v' },
    { name:'기대 연승', x:labels, y:winExp, type:'bar', marker:{color:'#3fb950',opacity:0.35}, orientation:'v' },
    { name:'실측 연패', x:labels, y:lossAct, type:'bar', marker:{color:'#f85149'}, orientation:'v' },
    { name:'기대 연패', x:labels, y:lossExp, type:'bar', marker:{color:'#f85149',opacity:0.35}, orientation:'v' },
  ];
  const layout = darkLayout({
    barmode: 'group', height: 260, margin: { l:60,r:20,t:20,b:120 },
    xaxis: { gridcolor:GRID_CLR, color:TEXT_CLR, tickangle:-25 },
    yaxis: { title:'스트릭 길이', gridcolor:GRID_CLR, color:TEXT_CLR },
    legend: { orientation:'h', x:0, y:1.1 },
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── C3: Regime Breakdown ──────────────────────────────────
const REGIME_ORDER  = ['BULL_LV','BULL_HV','SIDE','BEAR_LV','BEAR_HV'];
const REGIME_LABELS = { BULL_LV:'강세↑안정', BULL_HV:'강세↑고변동', SIDE:'횡보', BEAR_LV:'약세↓안정', BEAR_HV:'약세↓고변동' };
const REGIME_COLORS = { BULL_LV:'#3fb950', BULL_HV:'#4CE87E', SIDE:'#8b949e', BEAR_LV:'#f85149', BEAR_HV:'#E84C4C' };

function renderRegimeBreakdown() {
  const div = document.getElementById('chart-regime');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 Regime 분해가 표시됩니다.</div>';
    return;
  }
  const traces = [];
  for (let i=0; i<state.selected.length; i++) {
    const r = getResultById(state.selected[i]);
    if (!r) continue;
    const _regSim = balanceSim(r, state.startMs, state.endMs);
    if (_regSim.trades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    // Accumulate virtual PnL by regime (leveraged for x2/x3, fresh $10k base)
    const pnlByRegime = {};
    for (const reg of REGIME_ORDER) pnlByRegime[reg] = 0;
    for (const st of _regSim.trades) {
      const reg = getRegimeForMs(st.orig.t_open);
      pnlByRegime[reg] = (pnlByRegime[reg] || 0) + st.vNetPnl;
    }
    traces.push({
      x: REGIME_ORDER.map(k => REGIME_LABELS[k]),
      y: REGIME_ORDER.map(k => Math.round(pnlByRegime[k])),
      type: 'bar', name: label,
      marker: { color: REGIME_ORDER.map(k => REGIME_COLORS[k]) },
      hovertemplate: '%{x}<br>PnL: $%{y:,.0f}<extra>'+label+'</extra>',
    });
  }
  const layout = darkLayout({
    barmode: 'group', height: 280, margin: { l:70,r:20,t:20,b:60 },
    xaxis: { gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { title:'누적 PnL ($)', tickprefix:'$', tickformat:',.0f', gridcolor:GRID_CLR, color:TEXT_CLR, zerolinecolor:'#388bfd' },
    showlegend: state.selected.length > 1,
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── D1: Monte Carlo Trade Shuffle ─────────────────────────
function renderMonteCarloChart() {
  const div = document.getElementById('chart-monte-carlo');
  if (!div) return;
  if (state.selected.length !== 1) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">단일 전략 선택 시 Monte Carlo 분석이 표시됩니다.</div>';
    return;
  }
  const r = getResultById(state.selected[0]);
  const _mcSim = r ? balanceSim(r, state.startMs, state.endMs) : null;
  if (!r || !_mcSim || _mcSim.trades.length < 5) {
    div.innerHTML = '<div class="empty-state">거래 수가 너무 적어 Monte Carlo를 실행할 수 없습니다.</div>';
    return;
  }
  const meta = DATA.meta[r.strat] || {};
  const label = `${meta.name_ko||r.strat}/${r.tf}`;
  const pnls = _mcSim.trades.map(t => t.vNetPnl);
  const N_ITER = 1000;
  const start = 10_000;
  // Run Monte Carlo
  const paths = [];
  for (let it=0; it<N_ITER; it++) {
    const shuffled = [...pnls];
    for (let j=shuffled.length-1; j>0; j--) {
      const k = Math.floor(Math.random()*(j+1));
      [shuffled[j], shuffled[k]] = [shuffled[k], shuffled[j]];
    }
    let bal = start;
    const path = [start];
    for (const p of shuffled) { bal += p; path.push(bal); }
    paths.push(path);
  }
  const n = pnls.length + 1;
  const p5=[],p50=[],p95=[];
  for (let i=0; i<n; i++) {
    const vals = paths.map(p=>p[i]).sort((a,b)=>a-b);
    p5.push(vals[Math.floor(N_ITER*0.05)]);
    p50.push(vals[Math.floor(N_ITER*0.50)]);
    p95.push(vals[Math.floor(N_ITER*0.95)]);
  }
  const xs = Array.from({length:n},(_,i)=>i);
  // Actual equity from virtual simulation
  const actY = _mcSim.points.slice(0, _mcSim.trades.length + 1).map(p => p.v);

  const inBand = actY[actY.length-1] >= p5[p5.length-1] && actY[actY.length-1] <= p95[p95.length-1];
  const bandColor = 'rgba(31,111,235,0.15)';
  const traces = [
    { x:xs, y:p95, type:'scatter', mode:'lines', name:'95%',
      line:{color:'#388bfd',width:1,dash:'dot'}, showlegend:false, hoverinfo:'skip' },
    { x:xs, y:p5, type:'scatter', mode:'lines', name:'5%~95% 밴드', fill:'tonexty',
      fillcolor:bandColor, line:{color:'#388bfd',width:1,dash:'dot'} },
    { x:xs, y:p50, type:'scatter', mode:'lines', name:'중앙값(50%)',
      line:{color:'#8b949e',width:1.5,dash:'dash'} },
    { x:xs, y:actY, type:'scatter', mode:'lines', name:`실제 곡선 (${label})`,
      line:{color: inBand ? '#3fb950' : '#f85149', width:2.5} },
  ];
  const annText = inBand
    ? '실제 곡선이 95% 밴드 내 — 거래 순서 의존성 이상 없음'
    : '⚠ 실제 곡선이 95% 밴드 밖 — 거래 간 의존성(또는 이상치) 의심';
  const layout = darkLayout({
    height: 320, margin: { l:70,r:20,t:30,b:40 },
    xaxis: { title:'거래 순서 (n번째)', gridcolor:GRID_CLR, color:TEXT_CLR },
    yaxis: { title:'잔고 ($)', tickprefix:'$', tickformat:',.0f', gridcolor:GRID_CLR, color:TEXT_CLR },
    showlegend: true,
    annotations: [{
      xref:'paper', yref:'paper', x:0.01, y:0.97, xanchor:'left', yanchor:'top',
      text: annText, showarrow:false, font:{size:11, color: inBand?'#3fb950':'#f85149'},
    }],
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── D2: Risk-Adjusted Scatter (all results) ────────────────
function renderRiskScatter() {
  const div = document.getElementById('chart-scatter');
  if (!div) return;
  const all = getAllResults().filter(r => r.variant !== 'buy_and_hold');
  if (all.length === 0) return;

  const meta = DATA.meta;
  const allStats = all.map(r => getStats(r));
  const maxTrades = Math.max(...allStats.map(s=>s.trades));
  const sharpes = allStats.map(s=>s.sharpe);
  const minS = Math.min(...sharpes), maxS = Math.max(...sharpes);

  const hoverTexts = all.map((r, i) => {
    const m = meta[r.strat]||{};
    const s = allStats[i];
    const adjTxt = s.adj_cagr !== null && s.adj_cagr !== undefined
      ? `<br>조정 CAGR: ${s.adj_cagr.toFixed(2)}%/yr` : '';
    return `${m.name_ko||r.strat} / ${r.tf} / ${varLabel(r)}<br>`+
           `총수익: ${fmtMultiplier(s.starting, s.finishing)}<br>`+
           `CAGR: ${s.cagr.toFixed(2)}%/yr${adjTxt}<br>`+
           `Sharpe: ${s.sharpe.toFixed(3)}<br>`+
           `MDD: ${s.mdd.toFixed(1)}%<br>`+
           `Trades: ${s.trades}`;
  });

  // Color by Sharpe (red→yellow→green)
  const colorscale = [[0,'#f85149'],[0.5,'#d29922'],[1,'#3fb950']];
  // Use adj_cagr for scatter Y-axis when available (falls back to raw cagr)
  const yCagr = allStats.map(s => s.adj_cagr !== null && s.adj_cagr !== undefined ? s.adj_cagr : s.cagr);

  const trace = {
    x: allStats.map(s => s.mdd),
    y: yCagr,
    mode: 'markers',
    type: 'scatter',
    text: hoverTexts,
    hovertemplate: '%{text}<extra></extra>',
    customdata: all.map(r => r.id),
    marker: {
      size: allStats.map(s => 8 + Math.sqrt(s.trades / Math.max(maxTrades,1)) * 24),
      color: sharpes,
      colorscale,
      cmin: minS, cmax: maxS,
      showscale: true,
      colorbar: { title:'Sharpe', thickness:12, len:0.6, tickfont:{size:10,color:TEXT_CLR},
                  titlefont:{size:11,color:TEXT_CLR} },
      line: { color: '#30363d', width: 0.5 },
    },
  };

  // Mark selected
  const selIds = new Set(state.selected);
  const selTrace = {
    x: all.filter(r=>selIds.has(r.id)).map(r=>getStats(r).mdd),
    y: all.filter(r=>selIds.has(r.id)).map(r=>{ const s=getStats(r); return s.adj_cagr??s.cagr; }),
    mode: 'markers', type: 'scatter', name:'선택됨',
    marker: { size: 16, color:'rgba(0,0,0,0)', line:{color:'#f0f6fc', width:2} },
    hoverinfo: 'skip',
  };

  const layout = darkLayout({
    height: 380, margin: { l:70,r:90,t:30,b:60 },
    xaxis: { title:'MDD (%)', ticksuffix:'%', gridcolor:GRID_CLR, color:TEXT_CLR, autorange:'reversed' },
    yaxis: { title: currentCostKey() ? '조정 CAGR (%)' : 'CAGR (%)', ticksuffix:'%', gridcolor:GRID_CLR, color:TEXT_CLR },
    showlegend: false,
    shapes: [
      { type:'line', x0:0,x1:0,y0:-200,y1:200, line:{color:'#388bfd',width:1,dash:'dot'} },
      { type:'line', x0:-100,x1:0, y0:0,y1:0, line:{color:'#388bfd',width:1,dash:'dot'} },
    ],
  });

  Plotly.purge(div);
  Plotly.newPlot(div, [trace, selTrace], layout, { responsive:true, displayModeBar:false });

  // Click handler: select strategy in sidebar
  div.on('plotly_click', data => {
    if (!data.points[0]) return;
    const pt = data.points[0];
    if (pt.data.customdata) {
      const id = pt.data.customdata[pt.pointIndex];
      if (id) { state.selected = [id]; renderAll(); }
    }
  });
}

// ── D3: Correlation Matrix ────────────────────────────────
function renderCorrelationMatrix() {
  const div = document.getElementById('chart-corr');
  if (!div) return;
  if (state.selected.length < 2) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">2개 이상 전략 선택 시 상관관계 매트릭스가 표시됩니다.</div>';
    return;
  }
  const rows = state.selected.map(id => {
    const r = getResultById(id);
    return r ? slicedReturnsDaily(r, state.startMs, state.endMs).rets : null;
  }).filter(Boolean);
  if (rows.length < 2) return;

  const labels = state.selected.map(id => {
    const r = getResultById(id);
    if (!r) return id;
    const m = DATA.meta[r.strat]||{};
    return `${m.name_ko||r.strat}\n${r.tf}`;
  });

  const n = rows.length;
  const z = Array.from({length:n}, (_, i) =>
    Array.from({length:n}, (_, j) => +pearson(rows[i], rows[j]).toFixed(3))
  );
  const textMatrix = z.map(row => row.map(v => v.toFixed(2)));

  const trace = {
    type: 'heatmap', z, x: labels, y: labels,
    text: textMatrix, texttemplate: '%{text}',
    colorscale: [[0,'#f85149'],[0.5,'#21262d'],[1,'#3fb950']],
    zmin: -1, zmax: 1, showscale: true,
    colorbar: { title:'r', thickness:12, len:0.6, tickfont:{size:10,color:TEXT_CLR},
                titlefont:{size:11,color:TEXT_CLR} },
  };
  const h = Math.max(200, n*70+80);
  const layout = darkLayout({
    height: h, margin: { l:120,r:80,t:20,b:120 },
    xaxis: { gridcolor:GRID_CLR, color:TEXT_CLR, tickangle:-30 },
    yaxis: { gridcolor:GRID_CLR, color:TEXT_CLR },
  });
  Plotly.purge(div);
  Plotly.newPlot(div, [trace], layout, { responsive:true, displayModeBar:false });
}

// ── D4: Tornado / Radar Metrics ───────────────────────────
function renderTornadoRadar() {
  const div = document.getElementById('chart-tornado');
  if (!div) return;
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 Radar 차트가 표시됩니다.</div>';
    return;
  }
  const all = getAllResults();
  // 6 metrics: Sharpe, Sortino, |MDD| inverted (=1-mdd/min_mdd), WinRate, PF, Calmar
  const getMetrics = r => {
    const s = getStats(r);
    return {
      sharpe:   s.sharpe,
      sortino:  s.sortino || 0,
      mdd_inv:  -s.mdd,  // less negative = better
      win_rate: s.win_rate,
      pf:       Math.min(s.pf, 5),  // cap at 5
      calmar:   Math.max(Math.min(s.calmar||0, 5), -5),
    };
  };
  const keys = ['sharpe','sortino','mdd_inv','win_rate','pf','calmar'];
  const axisLabels = ['Sharpe','Sortino','MDD 역수(낮을수록 좋음→높게)','승률(%)','Profit Factor','Calmar'];
  // Compute per-metric min/max across all results for normalization
  const ranges = {};
  for (const k of keys) {
    const vals = all.map(r => getMetrics(r)[k]);
    ranges[k] = { min: Math.min(...vals), max: Math.max(...vals) };
  }
  function normalize(val, min, max) {
    return max > min ? (val - min) / (max - min) : 0.5;
  }
  const traces = state.selected.map((id, i) => {
    const r = getResultById(id);
    if (!r) return null;
    const m = DATA.meta[r.strat]||{};
    const label = `${m.name_ko||r.strat}/${r.tf}`;
    const metrics = getMetrics(r);
    const values = keys.map(k => normalize(metrics[k], ranges[k].min, ranges[k].max));
    values.push(values[0]);  // close radar
    const axLabels = [...axisLabels, axisLabels[0]];
    return {
      type: 'scatterpolar', mode: 'lines+markers', name: label,
      r: values, theta: axLabels,
      fill: 'toself', fillcolor: PALETTE[i%PALETTE.length].replace(')', ',0.10)').replace('rgb','rgba'),
      line: { color: PALETTE[i%PALETTE.length], width: 2 },
      marker: { color: PALETTE[i%PALETTE.length], size: 5 },
    };
  }).filter(Boolean);

  const layout = darkLayout({
    height: 360, margin: { l:40,r:40,t:30,b:30 },
    polar: {
      radialaxis: { visible:true, range:[0,1], gridcolor:GRID_CLR, color:TEXT_CLR, tickfont:{size:9} },
      angularaxis: { gridcolor:GRID_CLR, color:TEXT_CLR, tickfont:{size:10} },
      bgcolor: CARD_BG,
    },
    showlegend: true,
    legend: { x:1.05, y:0.5 },
  });
  Plotly.purge(div);
  Plotly.newPlot(div, traces, layout, { responsive:true, displayModeBar:false });
}

// ── Validation render dispatcher ─────────────────────────
function renderValidationViews() {
  renderUnderwaterChart();
  renderRollingMetricsChart();
  renderPnlHistogram();
  renderCumTradesChart();
  renderDurationChart();
  renderStreaksChart();
  renderRegimeBreakdown();
  renderMonteCarloChart();
  renderRiskScatter();
  renderCorrelationMatrix();
  renderTornadoRadar();
}

// ── Main render ───────────────────────────────────────────
function renderAll() {
  buildSidebar();
  renderKPIs();
  renderEquityChart();
  renderTradeChart();
  renderTradeTable();    // focusIdx=null — clears highlight on strategy/slice change
  renderHeatmap();
  renderValidationViews();
  renderDescription();
  updateHeader();
  _checkNoDataBanner();
}

// ── No-data warning banner ────────────────────────────────────
const _TRADES_START_MS = (() => {
  let min = Infinity;
  for (const g of Object.values(DATA.groups)) {
    for (const r of g) {
      for (const t of (r.trades || [])) {
        if (t.t_open < min) min = t.t_open;
      }
    }
  }
  return isFinite(min) ? min : DATA.data_end_ms;
})();
const _TRADES_END_MS = (() => {
  let max = -Infinity;
  for (const g of Object.values(DATA.groups)) {
    for (const r of g) {
      for (const t of (r.trades || [])) {
        if (t.t_close > max) max = t.t_close;
      }
    }
  }
  return isFinite(max) ? max : DATA.data_end_ms;
})();
const _TRADES_START_YEAR = new Date(_TRADES_START_MS).getUTCFullYear();

function _checkNoDataBanner() {
  const banner = document.getElementById('no-data-banner');
  if (!banner) return;
  const noOverlap = state.endMs < _TRADES_START_MS || state.startMs > _TRADES_END_MS;
  if (noOverlap) {
    const selYear = new Date(state.startMs).getUTCFullYear();
    banner.textContent = `⚠ ${selYear}년에는 백테스트 거래 기록이 없습니다. 이 대시보드의 백테스트는 ${_TRADES_START_YEAR}년부터 시작됩니다.`;
    banner.style.display = 'block';
  } else {
    banner.style.display = 'none';
  }
}

// ── Init ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initFilters();
  applyEquityToggles();

  // Date range slicing
  const dateStartEl = document.getElementById('dateStart');
  const dateEndEl   = document.getElementById('dateEnd');
  function onDateChange() {
    let s = Date.parse(dateStartEl.value + 'T00:00:00Z');
    let e = Date.parse(dateEndEl.value   + 'T23:59:59Z');
    const D0 = DATA.data_start_ms, D1 = DATA.data_end_ms;
    if (!isFinite(s)) s = D0;
    if (!isFinite(e)) e = D1;
    s = Math.max(D0, Math.min(s, D1));
    e = Math.max(D0, Math.min(e, D1));
    if (s >= e) return;
    dateStartEl.value = new Date(s).toISOString().slice(0, 10);
    dateEndEl.value   = new Date(e).toISOString().slice(0, 10);
    state.startMs = s;
    state.endMs   = e;
    sliceCache.clear();
    _balanceSimCache.clear();
    _sliceTradesCache.clear();
    renderAll();
  }
  document.getElementById('dateApply').addEventListener('click', onDateChange);

  // Grey-out year presets with no trade data
  document.querySelectorAll('.tag.preset[data-preset]').forEach(el => {
    const p = el.dataset.preset;
    if (/^\d{4}$/.test(p)) {
      const yr = parseInt(p);
      const yrEndMs   = Date.UTC(yr, 11, 31, 23, 59, 59);
      const yrStartMs = Date.UTC(yr, 0, 1);
      if (yrEndMs < _TRADES_START_MS || yrStartMs > _TRADES_END_MS) {
        el.style.opacity = '0.35';
        el.style.cursor  = 'default';
        el.title = `${p}년에는 백테스트 데이터가 없습니다 (데이터: ${_TRADES_START_YEAR}년~)`;
      }
    }
  });

  const PRE21_RANGES = {
    'pre21_full':     ['2017-08-18','2020-12-31'],
    'pre21_bear':     ['2017-12-17','2018-12-15'],
    'pre21_range':    ['2018-12-16','2019-04-01'],
    'pre21_recovery': ['2019-04-02','2020-02-29'],
    'pre21_covid':    ['2020-03-01','2020-04-30'],
    'pre21_bull':     ['2020-05-01','2020-12-31'],
  };

  document.querySelectorAll('.tag.preset').forEach(el => {
    el.addEventListener('click', () => {
      document.querySelectorAll('.tag.preset').forEach(t => t.classList.remove('active'));
      el.classList.add('active');
      const p = el.dataset.preset;
      if (p === 'all') {
        dateStartEl.value = '2017-08-18'; dateEndEl.value = '2026-04-30';
      } else if (/^\d{4}$/.test(p)) {
        const y = p;
        dateStartEl.value = y === '2017' ? '2017-08-18' : `${y}-01-01`;
        dateEndEl.value   = y === '2026' ? '2026-04-30' : `${y}-12-31`;
      } else if (PRE21_RANGES[p]) {
        const [s, e] = PRE21_RANGES[p];
        dateStartEl.value = s; dateEndEl.value = e;
      }
      onDateChange();
    });
  });

  // Sort + roll + window toggle handlers
  document.querySelectorAll('[data-roll-metric]').forEach(btn => {
    btn.addEventListener('click', () => {
      vState.rollMetric = btn.dataset.rollMetric;
      document.querySelectorAll('[data-roll-metric]').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      renderRollingMetricsChart();
    });
  });
  document.querySelectorAll('[data-roll-window]').forEach(btn => {
    btn.addEventListener('click', () => {
      vState.rollWindow = parseInt(btn.dataset.rollWindow);
      document.querySelectorAll('[data-roll-window]').forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      renderRollingMetricsChart();
    });
  });

  renderAll();
  renderRiskScatter(); // render once; re-render on selection change via renderAll

  // Resize handler
  const CHART_IDS = ['chart-equity','chart-underwater','chart-rolling','chart-pnl-hist',
    'chart-cum-trades','chart-duration','chart-streaks','chart-regime','chart-monte-carlo',
    'chart-scatter','chart-corr','chart-tornado','chart-trades','chart-heatmap'];
  let resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      CHART_IDS.forEach(id => {
        const el = document.getElementById(id);
        if (el && el.data) Plotly.relayout(el, { autosize: true });
      });
    }, 200);
  });
});
"""


def generate_html(data_json: str, plotlyjs: str, n_results: int = 57) -> str:
    gen_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js_content = DASHBOARD_JS.replace('__N_DAYS__', str(_N_DAYS - 1))
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>V4 백테스트 대시보드 — BTC 전략 분석</title>
<style>
{CSS}
</style>
</head>
<body>
<div class="app">

  <!-- Sidebar -->
  <aside class="sidebar">
    <div class="header">
      <div class="h1" style="font-size:14px;font-weight:700;color:#f0f6fc">V4 백테스트 대시보드</div>
      <div class="subtitle">초기 자본 $10,000 · 2017-08-18 ~ 2026-04-30 · 7가지 전략 · {n_results}개 백테스트</div>
    </div>

    <!-- Filters -->
    <div class="filters">
      <div class="filter-label">타임프레임</div>
      <div class="filter-row">
        <span class="tag active" data-filter="tf" data-value="1h">1h</span>
        <span class="tag active" data-filter="tf" data-value="4h">4h</span>
        <span class="tag active" data-filter="tf" data-value="1D">1D</span>
      </div>
      <div class="filter-label" style="margin-top:6px">변형</div>
      <div class="filter-row">
        <span class="tag active" data-filter="variant" data-value="long_only">롱전용</span>
        <span class="tag active" data-filter="variant" data-value="bidirectional">양방향</span>
        <span class="tag active" data-filter="variant" data-value="long_only_x2">롱x2</span>
        <span class="tag active" data-filter="variant" data-value="bidirectional_x2">양방x2</span>
        <span class="tag active" data-filter="variant" data-value="long_only_x3">롱x3</span>
        <span class="tag active" data-filter="variant" data-value="bidirectional_x3">양방x3</span>
        <span class="tag active" data-filter="variant" data-value="long_only_v2">롱v2</span>
        <span class="tag active" data-filter="variant" data-value="long_only_x3_v2">롱x3v2</span>
        <span class="tag active" data-filter="variant" data-value="buy_and_hold">매수보유</span>
      </div>
      <div class="filter-label" style="margin-top:6px">정렬 / 보기</div>
      <div class="filter-row">
        <span class="sort-btn" data-sort="alpha">전략별</span>
        <span class="sort-btn active" data-sort="return">수익률순</span>
        <span class="sort-btn" data-sort="top10">Top 10</span>
      </div>
      <div class="filter-label" style="margin-top:8px">기간 선택</div>
      <div style="padding:0 2px;display:flex;flex-direction:column;gap:4px">
        <input type="date" id="dateStart" min="2017-08-18" max="2026-04-30" value="2017-08-18"
          style="background:#0d1117;color:#e6edf3;border:1px solid #30363d;border-radius:4px;padding:3px 6px;font-size:12px;width:100%">
        <input type="date" id="dateEnd" min="2017-08-18" max="2026-04-30" value="2026-04-30"
          style="background:#0d1117;color:#e6edf3;border:1px solid #30363d;border-radius:4px;padding:3px 6px;font-size:12px;width:100%">
        <button id="dateApply" type="button"
          style="background:#1f6feb;color:#fff;border:none;border-radius:4px;padding:5px 8px;font-size:12px;font-weight:600;cursor:pointer;margin-top:2px">
          적용
        </button>
      </div>
      <div class="filter-row" style="margin-top:4px;flex-wrap:wrap;gap:4px">
        <span class="tag preset active" data-preset="all">전체</span>
        <span class="tag preset" data-preset="2018">2018</span>
        <span class="tag preset" data-preset="2019">2019</span>
        <span class="tag preset" data-preset="2020">2020</span>
        <span class="tag preset" data-preset="2021">2021</span>
        <span class="tag preset" data-preset="2022">2022</span>
        <span class="tag preset" data-preset="2023">2023</span>
        <span class="tag preset" data-preset="2024">2024</span>
        <span class="tag preset" data-preset="2025">2025</span>
      </div>
      <div class="filter-row" style="margin-top:4px;flex-wrap:wrap;gap:4px">
        <span class="tag preset" data-preset="pre21_full"     style="background:#332b1f;font-size:11px">Pre-21 전체</span>
        <span class="tag preset" data-preset="pre21_bear"     style="background:#3a2020;font-size:11px">🐻 Bear</span>
        <span class="tag preset" data-preset="pre21_range"    style="background:#2a2a2a;font-size:11px">↔ Range</span>
        <span class="tag preset" data-preset="pre21_recovery" style="background:#1f3320;font-size:11px">↗ Recovery</span>
        <span class="tag preset" data-preset="pre21_covid"    style="background:#332033;font-size:11px">☣ COVID</span>
        <span class="tag preset" data-preset="pre21_bull"     style="background:#1f3338;font-size:11px">🐂 Bull</span>
      </div>
    </div>

    <!-- Strategy list -->
    <div class="strat-list" id="strat-list"></div>

    <div style="padding:8px 12px;font-size:11px;color:#8b949e;border-top:1px solid #30363d" id="selected-count">
      전략을 선택하세요
    </div>
    <div style="padding:4px 12px 8px;font-size:10px;color:#484f58">
      Ctrl/Shift 클릭으로 최대 6개 비교 · 생성: {gen_time}
    </div>
  </aside>

  <!-- Main -->
  <main class="main">

    <!-- No-data warning banner -->
    <div id="no-data-banner" style="display:none;background:#2d1f00;border:1px solid #d29922;border-radius:8px;padding:12px 16px;font-size:13px;color:#d29922;font-weight:500;"></div>

    <!-- KPI Cards -->
    <div>
      <div style="font-size:12px;font-weight:600;color:#8b949e;margin-bottom:8px">성과 지표</div>
      <div class="kpi-grid" id="kpi-grid">
        <div class="empty-state">왼쪽에서 전략을 선택하면 성과 지표가 표시됩니다.<br>
          <small>Ctrl/Shift 클릭으로 최대 6개 비교</small></div>
      </div>
    </div>

    <!-- ── Section A: 시간 분석 ─────────────────── -->
    <div class="section-hdr"><div class="section-hdr-line"></div><div class="section-hdr-text">시간 분석</div><div class="section-hdr-line"></div></div>

    <!-- A1: Equity Curve -->
    <div class="chart-card">
      <div class="chart-title">
        수익 곡선
        <div class="chart-ctrl">
          <button class="chart-tab-btn eq-toggle" data-toggle="equityLog">로그 스케일</button>
          <button class="chart-tab-btn eq-toggle" data-toggle="equityAlpha">HODL 대비 Alpha</button>
          <small style="margin-left:4px;color:#8b949e">BTC 기준선 포함 (우측축)</small>
        </div>
      </div>
      <div class="chart-wrap" id="chart-equity">
        <div class="empty-state">전략을 선택하면 수익 곡선이 표시됩니다.</div>
      </div>
    </div>

    <!-- A1b: Trade Markers + Trade History Table -->
    <div class="chart-card" id="card-trades">
      <div class="chart-title">
        거래 시점 마커
        <small>▲ 진입 · ■ 종료(초록=수익, 빨강=손실) · 마커 클릭 시 해당 거래 행 강조 · 단일 전략 선택 시 활성화</small>
      </div>
      <div class="chart-wrap" id="chart-trades">
        <div class="empty-state">전략을 선택하면 거래 마커 차트가 표시됩니다.</div>
      </div>
      <div id="trade-history-table"></div>
    </div>

    <!-- A2: Underwater -->
    <div class="chart-card">
      <div class="chart-title">Drawdown (Underwater) <small>0 라인 아래 = 최고점 대비 하락 중</small></div>
      <div class="chart-wrap" id="chart-underwater">
        <div class="empty-state">전략을 선택하면 Drawdown이 표시됩니다.</div>
      </div>
    </div>

    <!-- A3: Rolling Metrics -->
    <div class="chart-card">
      <div class="chart-title">
        롤링 지표
        <div class="chart-ctrl">
          <button class="chart-tab-btn active" data-roll-metric="sharpe">Rolling Sharpe</button>
          <button class="chart-tab-btn" data-roll-metric="winrate">Rolling 승률</button>
          <span style="width:1px;background:#30363d;margin:0 4px"></span>
          <button class="chart-tab-btn active" data-roll-window="60">60일</button>
          <button class="chart-tab-btn" data-roll-window="180">180일</button>
        </div>
      </div>
      <div class="chart-wrap" id="chart-rolling">
        <div class="empty-state">전략을 선택하면 롤링 지표가 표시됩니다.</div>
      </div>
    </div>

    <!-- ── Section B: 거래 분포 분석 ─────────────── -->
    <div class="section-hdr"><div class="section-hdr-line"></div><div class="section-hdr-text">거래 분포 분석</div><div class="section-hdr-line"></div></div>

    <!-- B1: PnL Histogram -->
    <div class="chart-card">
      <div class="chart-title">거래 PnL 분포 <small>왜도/첨도 · 상위 5건 기여도</small></div>
      <div class="chart-wrap" id="chart-pnl-hist">
        <div class="empty-state">전략을 선택하면 PnL 분포가 표시됩니다.</div>
      </div>
      <div id="pnl-hist-annotation" style="margin-top:6px;font-size:11px;color:#8b949e;line-height:1.7"></div>
    </div>

    <!-- B2: Cumulative trades -->
    <div class="chart-card">
      <div class="chart-title">누적 거래 수 vs 시간 <small>빨간선 = 6년간 50건 미만 (통계 의미 약함)</small></div>
      <div class="chart-wrap" id="chart-cum-trades">
        <div class="empty-state">전략을 선택하면 누적 거래 수가 표시됩니다.</div>
      </div>
    </div>

    <!-- B3: Duration distribution -->
    <div class="chart-card">
      <div class="chart-title">보유 기간 분포 <small>90% 이상이 동일 구간이면 룩어헤드 의심</small></div>
      <div class="chart-wrap" id="chart-duration">
        <div class="empty-state">전략을 선택하면 보유 기간 분포가 표시됩니다.</div>
      </div>
      <div id="duration-warnings" style="margin-top:6px;display:flex;flex-wrap:wrap;gap:6px"></div>
    </div>

    <!-- B4: Streaks -->
    <div class="chart-card">
      <div class="chart-title">연승/연패 스트릭 <small>진한색=실측, 연한색=베르누이 기댓값</small></div>
      <div class="chart-wrap" id="chart-streaks">
        <div class="empty-state">전략을 선택하면 스트릭 차트가 표시됩니다.</div>
      </div>
    </div>

    <!-- ── Section C: 시장 맥락 ─────────────────── -->
    <div class="section-hdr"><div class="section-hdr-line"></div><div class="section-hdr-text">시장 맥락</div><div class="section-hdr-line"></div></div>

    <!-- C1: Monthly Heatmap -->
    <div class="chart-card">
      <div class="chart-title">월별 손익 히트맵 <small>초록=수익 · 빨강=손실 · 셀 클릭 시 거래 내역</small></div>
      <div class="chart-wrap" id="chart-heatmap">
        <div class="empty-state">전략을 선택하면 월별 손익 히트맵이 표시됩니다.</div>
      </div>
      <div id="trade-detail-table"></div>
    </div>

    <!-- C3: Regime Breakdown -->
    <div class="chart-card">
      <div class="chart-title">Regime별 누적 PnL <small>BTC 90일 추세 × 변동성 5구간 분해</small></div>
      <div class="chart-wrap" id="chart-regime">
        <div class="empty-state">전략을 선택하면 Regime 분해가 표시됩니다.</div>
      </div>
    </div>

    <!-- ── Section D: 견고성 / 비교 ─────────────── -->
    <div class="section-hdr"><div class="section-hdr-line"></div><div class="section-hdr-text">견고성 / 비교</div><div class="section-hdr-line"></div></div>

    <!-- D1: Monte Carlo -->
    <div class="chart-card">
      <div class="chart-title">Monte Carlo 거래 순서 셔플 (1,000회) <small>단일 전략 선택 시 · 실제 곡선이 95% 밴드 밖이면 의존성 의심</small></div>
      <div class="chart-wrap" id="chart-monte-carlo">
        <div class="empty-state">단일 전략 선택 시 Monte Carlo 분석이 표시됩니다.</div>
      </div>
    </div>

    <!-- D2: Risk-Adjusted Scatter (all results) -->
    <div class="chart-card">
      <div class="chart-title">위험조정 산점도 (전체 {n_results}개) <small>x: MDD, y: CAGR, 크기: 거래 수, 색상: Sharpe · 클릭하면 선택</small></div>
      <div class="chart-wrap" id="chart-scatter">
        <div class="empty-state">로딩 중…</div>
      </div>
    </div>

    <!-- D3: Correlation Matrix -->
    <div class="chart-card">
      <div class="chart-title">전략 간 일별 수익률 상관관계 <small>2개 이상 선택 시 · 1에 가까우면 중복</small></div>
      <div class="chart-wrap" id="chart-corr">
        <div class="empty-state">2개 이상 전략을 선택하면 상관관계 매트릭스가 표시됩니다.</div>
      </div>
    </div>

    <!-- D4: Tornado/Radar -->
    <div class="chart-card">
      <div class="chart-title">Radar 지표 비교 <small>Sharpe · Sortino · MDD역수 · 승률 · PF · Calmar — 전체 {n_results}개 기준 정규화</small></div>
      <div class="chart-wrap" id="chart-tornado">
        <div class="empty-state">전략을 선택하면 Radar 차트가 표시됩니다.</div>
      </div>
    </div>

    <!-- Strategy Description -->
    <div class="desc-card">
      <div style="font-size:13px;font-weight:600;color:#c9d1d9;margin-bottom:10px">
        전략 설명
      </div>
      <div id="desc-panel">
        <div class="empty-state">전략을 선택하면 설명이 표시됩니다.</div>
      </div>
    </div>

  </main>
</div>

<script>
{plotlyjs}
</script>
<script>
window.V4_DATA = {data_json};
</script>
<script>
{js_content}
</script>
</body>
</html>"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', default=str(DEFAULT_OUT), help='Output HTML path')
    args = ap.parse_args()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    print('Loading BTC 1D prices…')
    btc_1d = load_btc_1d()
    print(f'  {len(btc_1d)} daily candles')

    print('Loading adjusted cost data…')
    costs_lookup = load_costs_lookup()

    print('Loading 8h funding series…')
    funding_series = load_8h_funding_series()

    print('Collecting backtest results…')
    groups = collect_all_results(btc_daily=btc_1d, costs_lookup=costs_lookup)
    total = sum(len(v) for v in groups.values())
    print(f'  {total} results from {len(groups)} strategies')

    print('Computing regime labels and BnH returns…')
    regime_labels = _classify_btc_regimes(btc_1d)
    # BnH daily returns from BTC closes (more accurate than trade-based forward-fill)
    closes = [p['c'] for p in btc_1d]
    bnh_returns_daily = [
        round((closes[i] - closes[i-1]) / closes[i-1], 6) if closes[i-1] > 0 else 0.0
        for i in range(1, len(closes))
    ]

    # Compute TF-specific optimal params from sweep results
    optimal_params = compute_optimal_params()
    print(f'  Optimal params loaded: {sum(len(v) for v in optimal_params.values())} TF entries across {len(optimal_params)} strategies')

    # Build compact data payload
    data = {
        'generated_at':    datetime.now(timezone.utc).isoformat(),
        'build_ts':        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
        'data_start_ms':   START_MS,
        'data_end_ms':     END_MS,
        'n_results':       total,
        'btc_1d':          btc_1d,
        'regime_labels':   regime_labels,
        'bnh_returns':     bnh_returns_daily,
        'groups':          groups,
        'funding_ts':      funding_series['ts'],
        'funding_rates':   funding_series['rates'],
        'funding_fallback': funding_series['fallback'],
        'meta':         {k: {
            'name_ko':      m['name_ko'],
            'name_en':      m['name_en'],
            'summary_ko':   m['summary_ko'],
            'summary_en':   m['summary_en'],
            'entry_long_ko':  m.get('entry_long_ko', []),
            'entry_long_en':  m.get('entry_long_en', []),
            'entry_short_ko': m.get('entry_short_ko', []),
            'entry_short_en': m.get('entry_short_en', []),
            'exit_ko':      m.get('exit_ko', []),
            'exit_en':      m.get('exit_en', []),
            'indicators':   m.get('indicators', []),
            'optimal_params': optimal_params.get(k, {}),
        } for k, m in STRATEGY_META.items()},
    }
    data_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    print(f'  Data JSON: {len(data_json)/1024:.0f} KB')

    print('Embedding Plotly.js…')
    plotlyjs = get_plotlyjs()
    print(f'  Plotly.js: {len(plotlyjs)/1024:.0f} KB')

    print(f'Generating HTML → {out}')
    html = generate_html(data_json, plotlyjs, total)
    out.write_text(html, encoding='utf-8')
    size_mb = len(html.encode('utf-8')) / 1024 / 1024
    print(f'  Output: {size_mb:.1f} MB')
    print(f'\nDone! Open: firefox "{out}"')


if __name__ == '__main__':
    main()
