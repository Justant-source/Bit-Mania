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
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
JESSE_ROOT = SCRIPT_DIR.parent            # cryptoengine/services/jesse_engine/
CE_ROOT    = JESSE_ROOT.parent.parent     # cryptoengine/
RESULT_DIR = CE_ROOT / 'backtest-results' / 'data' / '9-strategies'
BTC_KLINES = CE_ROOT / 'backtest-results' / 'data' / 'binance_vision' / 'klines' / 'BTCUSDT'
DEFAULT_OUT = CE_ROOT / 'backtest-results' / 'data' / '9-strategies' / 'dashboard.html'

# ─── Backtest parameters ───────────────────────────────────────────────────────
TIMEFRAMES  = ['1h', '2h', '4h', '1D']
STRATEGIES  = ['bbpb', 'bbwp', 'stoch', 'momentum_ma', 'supertrend',
               'tradeiq_220320', 'trendtype', 'supertrend_trendtype', 'tradeiq_220323']
VARIANTS    = ['bidirectional', 'long_only']
START_MS    = int(datetime(2021, 4, 1, tzinfo=timezone.utc).timestamp() * 1000)
END_MS      = int(datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

# BnH Sharpe: 1D only (universal benchmark, 2021-04-01 ~ 2025-12-31)
BNH_SHARPE = {'1h': 0.418, '2h': 0.418, '4h': 0.418, '1D': 0.418}

# ─── Strategy metadata (Korean / English bilingual) ───────────────────────────
STRATEGY_META: dict[str, dict] = {
    'bbpb': {
        'name_ko': 'BB %B', 'name_en': 'Bollinger Band %B',
        'summary_ko': '볼린저 밴드 하단 돌파(%B≤0→>0) + MACD 방향으로 진입, ATR×3 손절',
        'summary_en': 'Enter on BB %B breakout from lower band plus MACD direction confirmation, ATR×3 stop',
        'entry_long_ko': [
            'BB %B ≤ 0에서 > 0으로 상향 돌파 (하단 밴드 진입)',
            'MACD 라인 ≥ MACD 시그널 (상승 모멘텀)',
        ],
        'entry_long_en': [
            '%B crosses up from ≤0 to >0 (price enters lower Bollinger Band)',
            'MACD line ≥ Signal line (bullish momentum)',
        ],
        'entry_short_ko': [
            'BB %B ≥ 1에서 < 1로 하향 돌파 (상단 밴드 이탈)',
            'MACD 라인 ≤ MACD 시그널 (하락 모멘텀)',
        ],
        'entry_short_en': [
            '%B crosses down from ≥1 to <1 (price exits upper band)',
            'MACD line ≤ Signal line (bearish momentum)',
        ],
        'exit_ko': [
            '%B가 반대 경계에 도달하거나 MACD 방향 반전 시 청산',
            'ATR(14) × 3.0 손절',
        ],
        'exit_en': [
            'Exit when %B reaches opposite boundary or MACD reverses',
            'Stop-loss: ATR(14) × 3.0 from entry',
        ],
        'indicators': ['Bollinger Bands (20, 2.0)', 'MACD (12, 26, 9)', 'ATR (14)'],
    },
    'bbwp': {
        'name_ko': 'BB 밴드폭 백분위', 'name_en': 'BB Width Percentile',
        'summary_ko': '밴드 폭이 역사적으로 최하위 10% (매우 좁음)일 때 MACD 방향으로 진입',
        'summary_en': 'Enter when band width is historically narrow (≤10th pct) with MACD direction',
        'entry_long_ko': [
            'BBWP ≤ 10% (252봉 기준 최하위 10%)',
            'MACD 라인 ≥ MACD 시그널',
        ],
        'entry_long_en': [
            'BBWP ≤ 10% (bottom decile of 252-bar lookback)',
            'MACD line ≥ Signal line',
        ],
        'entry_short_ko': [
            'BBWP ≤ 10%',
            'MACD 라인 ≤ MACD 시그널',
        ],
        'entry_short_en': [
            'BBWP ≤ 10%',
            'MACD line ≤ Signal line',
        ],
        'exit_ko': [
            'BBWP ≥ 90% 도달 후 하향 돌파 시 청산 (밴드 확장 완료)',
            'ATR(14) × 3.0 손절',
        ],
        'exit_en': [
            'Exit when BBWP crosses back below 90% after reaching top decile',
            'Stop-loss: ATR(14) × 3.0 from entry',
        ],
        'indicators': ['BB Width Percentile (bb_len=13, lookback=252)', 'MACD (12, 26, 9)', 'ATR (14)'],
    },
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
    'momentum_ma': {
        'name_ko': '모멘텀 MA', 'name_en': 'Momentum MA',
        'summary_ko': 'LazyBear 선형회귀 모멘텀이 자체 EMA(100)를 교차할 때 진입',
        'summary_en': 'LazyBear linreg momentum crossing its own EMA(100)',
        'entry_long_ko': [
            '선형회귀 모멘텀(val)이 EMA(100, val_ma)를 상향 돌파',
            'val = linreg(중선 편차, 20봉); 중선 = (20봉 고저 중간값 + SMA20) / 2',
        ],
        'entry_long_en': [
            'Linreg momentum (val) crosses above its EMA(100)',
            'val = linreg(deviation from midline, 20); midline = (HL-midpoint + SMA20) / 2',
        ],
        'entry_short_ko': ['val이 val_ma를 하향 돌파'],
        'entry_short_en': ['val crosses below val_ma'],
        'exit_ko': ['반대 교차 발생 시 청산', 'ATR(14) × 3.0 손절'],
        'exit_en': ['Exit on reverse crossover', 'Stop-loss: ATR(14) × 3.0'],
        'indicators': ['Linear Regression (20)', 'EMA of momentum (100)', 'ATR (14)'],
    },
    'supertrend': {
        'name_ko': '슈퍼트렌드', 'name_en': 'Supertrend',
        'summary_ko': 'Supertrend(7,3) + EMA(7/20) 정렬 + 200 EMA 방향 필터로 추세 추종',
        'summary_en': 'Trend-following with Supertrend(7,3), EMA(7/20) alignment, 200 EMA filter',
        'entry_long_ko': [
            'Supertrend = 상승 추세 (가격 > Supertrend 라인)',
            'EMA(7) > EMA(20) (황금 교차)',
            '가격 > EMA(200) (장기 상승 추세)',
        ],
        'entry_long_en': [
            'Supertrend = uptrend (price above Supertrend line)',
            'EMA(7) > EMA(20) (bullish EMA cross)',
            'Price above EMA(200) (long-term uptrend)',
        ],
        'entry_short_ko': [
            'Supertrend = 하락 추세',
            'EMA(7) < EMA(20)',
            '가격 < EMA(200)',
        ],
        'entry_short_en': [
            'Supertrend = downtrend (price below Supertrend line)',
            'EMA(7) < EMA(20) (bearish EMA cross)',
            'Price below EMA(200)',
        ],
        'exit_ko': ['EMA(7) ↔ EMA(20) 교차 시 청산 (추세 반전)', 'ATR(14) × 3.0 손절'],
        'exit_en': ['Exit on EMA(7)/EMA(20) crossover (trend reversal)', 'Stop-loss: ATR(14) × 3.0'],
        'indicators': ['Supertrend (7, 3.0)', 'EMA (7, 20, 200)', 'ATR (14)'],
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
    'tradeiq_220320': {
        'name_ko': 'TradeIQ 220320', 'name_en': 'TradeIQ 220320',
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
    'tradeiq_220323': {
        'name_ko': 'TradeIQ 220323', 'name_en': 'TradeIQ 220323',
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


# ─── Tier computation ─────────────────────────────────────────────────────────

def compute_tier(stats: dict, tf: str, variant: str) -> str:
    if variant == 'buy_and_hold':
        return 'BNH'
    cagr   = stats.get('cagr_pct', -999)
    sharpe = stats.get('sharpe_ratio', -999)
    mdd    = stats.get('max_drawdown_pct', -999)
    trades = stats.get('total_trades', 0)
    bnh_s  = BNH_SHARPE.get(tf, 0.9)
    if sharpe >= bnh_s * 0.7 and cagr >= 5 and mdd >= -30 and trades >= 30:
        return 'A'
    if sharpe >= 0.3 and cagr >= 0 and mdd >= -40:
        return 'B'
    return 'C'


# ─── Data collection ─────────────────────────────────────────────────────────

def _load_csv_trades(path: Path) -> list[dict]:
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


def collect_all_results() -> dict:
    """Returns dict keyed by strategy_dir, each with list of result objects."""
    groups: dict[str, list[dict]] = {}

    def _process(tf: str, strat_dir: str, variant: str, folder: Path):
        stats_path = folder / 'stats.json'
        if not stats_path.exists():
            return
        stats = json.loads(stats_path.read_text())
        trades   = _load_csv_trades(folder / 'trades.csv')
        monthly  = _load_csv_monthly(folder / 'monthly_returns.csv')
        starting  = stats.get('starting_balance', 10000.0)
        finishing = stats.get('raw_metrics', {}).get('finishing_balance', starting)
        # Sanitize infinity in stats
        for k in ('sharpe_ratio', 'cagr_pct', 'max_drawdown_pct'):
            v = stats.get(k, 0)
            if not math.isfinite(v):
                stats[k] = 0.0
        tier = compute_tier(stats, tf, variant)
        equity = build_equity_series(trades, starting, finishing)
        raw = stats.get('raw_metrics', {})
        sortino = raw.get('sortino_ratio', 0)
        calmar  = raw.get('calmar_ratio', 0)
        if not math.isfinite(sortino): sortino = 0.0
        if not math.isfinite(calmar):  calmar  = 0.0
        returns_daily = _compute_daily_returns(equity)
        streaks = _compute_streaks(trades)
        result = {
            'id':       f'{strat_dir}__{variant}__{tf}',
            'strat':    strat_dir,
            'variant':  variant,
            'tf':       tf,
            'tier':     tier,
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
    """Load BTC 1D OHLC from parquet files (2020-2025)."""
    frames = []
    base = BTC_KLINES / '1d'
    for year in range(2020, 2026):
        for month in range(1, 13):
            p = base / str(year) / f'{month:02d}.parquet'
            if p.exists():
                frames.append(pd.read_parquet(p, columns=['open_time','open','high','low','close']))
    if not frames:
        return []
    df = pd.concat(frames).sort_values('open_time')
    df = df[(df['open_time'] >= pd.Timestamp('2021-04-01', tz='UTC')) &
            (df['open_time'] <= pd.Timestamp('2025-12-31', tz='UTC'))]
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
    """Trade-based equity → 2191 daily forward-filled pct returns (2020-01-02 … 2025-12-31)."""
    import datetime as dt
    start = dt.date(2020, 1, 1)
    n = 2192  # days inclusive

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

    # Daily pct returns (length 2191)
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
.trade-detail-wrap table { width: 100%; border-collapse: collapse; font-size: 12px; min-width: 560px; }
.trade-detail-wrap th { color: #8b949e; padding: 5px 10px; text-align: left; border-bottom: 1px solid #30363d; white-space: nowrap; }
.trade-detail-wrap td { padding: 5px 10px; border-bottom: 1px solid #1a1f2e; color: #c9d1d9; white-space: nowrap; }
.trade-detail-wrap tr:hover td { background: #21262d; }

/* Responsive */
@media (max-width: 768px) {
  .app { flex-direction: column; height: auto; }
  .sidebar { width: 100%; border-right: none; border-bottom: 1px solid #30363d;
              max-height: 280px; }
  .main { padding: 10px; }
  .kpi-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 480px) {
  .kpi-grid { grid-template-columns: 1fr 1fr; }
  .desc-bilingual { grid-template-columns: 1fr; }
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
  tfFilter:      new Set(['1h','2h','4h','1D']),
  variantFilter: new Set(['bidirectional','long_only','buy_and_hold']),
  sortMode: 'alpha',                     // 'alpha' | 'return' | 'top10'
};

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
  return getAllResults().filter(r =>
    state.tfFilter.has(r.tf) &&
    state.variantFilter.has(r.variant)
  );
}

function fmtDollar(v) {
  if (!isFinite(v)) return 'N/A';
  if (Math.abs(v) >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
  if (Math.abs(v) >= 1e3) return '$' + (v/1e3).toFixed(1) + 'k';
  return '$' + v.toFixed(0);
}
function fmtPct(v) { return (v >= 0 ? '+' : '') + v.toFixed(2) + '%'; }
function fmtSharpe(v) { return isFinite(v) ? v.toFixed(3) : 'N/A'; }

const TF_ORDER   = { '1h': 0, '2h': 1, '4h': 2, '1D': 3 };

// ── Sidebar ─────────────────────────────────────────────────
const STRAT_DISPLAY_ORDER = [
  'supertrend','tradeiq_220320','trendtype','supertrend_trendtype',
  'tradeiq_220323','stoch','momentum_ma','bbwp','bbpb','buy_and_hold',
];

function buildSidebar() {
  const listEl = document.getElementById('strat-list');
  listEl.innerHTML = '';
  const visible = filteredResults();

  const varLabel = r => r.variant === 'buy_and_hold' ? '매수보유' :
                        r.variant === 'long_only' ? '롱전용' : '양방향';

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
        item.innerHTML =
          `<span class="cb">${isSelected ? '✓' : ''}</span>` +
          `<span>${r.tf} · ${varLabel(r)}</span>` +
          `<span style="margin-left:auto;color:#8b949e;font-size:11px">${fmtDollar(r.stats.finishing)}</span>`;
        item.addEventListener('click', e => toggleSelect(r.id, e.ctrlKey || e.metaKey || e.shiftKey));
        itemsDiv.appendChild(item);
      }

      groupDiv.appendChild(header);
      groupDiv.appendChild(itemsDiv);
      listEl.appendChild(groupDiv);
    }
  } else {
    // Flat list sorted by return (top10 or return mode)
    let sorted = visible.slice().sort((a, b) => b.stats.finishing - a.stats.finishing);
    if (state.sortMode === 'top10') sorted = sorted.slice(0, 10);

    for (const r of sorted) {
      const meta = DATA.meta[r.strat] || {};
      const isSelected = state.selected.includes(r.id);
      const item = document.createElement('div');
      item.className = 'flat-item' + (isSelected ? ' selected' : '');
      item.dataset.id = r.id;
      item.innerHTML =
        `<span class="cb">${isSelected ? '✓' : ''}</span>` +
        `<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">` +
          `${meta.name_ko||r.strat} · ${r.tf} · ${varLabel(r)}</span>` +
        `<span style="flex-shrink:0;color:#3fb950;font-size:11px;font-weight:600">${fmtDollar(r.stats.finishing)}</span>`;
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
    const s = r.stats;
    const meta = DATA.meta[r.strat] || {};
    const varLabel = r.variant === 'buy_and_hold' ? '' :
                     r.variant === 'long_only' ? '롱전용' : '양방향';
    const name = `${meta.name_ko||r.strat} ${r.tf} ${varLabel}`.trim();
    const cagrCls = s.cagr >= 0 ? 'kpi-pos' : 'kpi-neg';
    const mddCls  = 'kpi-neg';
    return `<div class="kpi-card">
      <div class="kpi-label">${name}</div>
      <div class="kpi-value ${cagrCls}">${fmtPct(s.cagr)}</div>
      <div class="kpi-sub">CAGR</div>
      <hr style="border-color:#21262d;margin:8px 0">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px">
        <div><div style="color:#8b949e">Sharpe</div><div style="font-weight:600">${fmtSharpe(s.sharpe)}</div></div>
        <div><div style="color:#8b949e">MDD</div><div class="${mddCls}" style="font-weight:600">${fmtPct(s.mdd)}</div></div>
        <div><div style="color:#8b949e">거래 수</div><div style="font-weight:600">${s.trades}</div></div>
        <div><div style="color:#8b949e">승률</div><div style="font-weight:600">${s.win_rate.toFixed(1)}%</div></div>
        <div><div style="color:#8b949e">PF</div><div style="font-weight:600">${s.pf.toFixed(2)}</div></div>
        <div><div style="color:#8b949e">최종잔고</div><div style="font-weight:600">${fmtDollar(s.finishing)}</div></div>
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
    const label = `${meta.name_ko||r.strat}/${r.variant==='long_only'?'롱':r.variant==='buy_and_hold'?'BnH':'양방'}/${r.tf}`;
    let xs = r.equity.map(p => new Date(p.t));
    let ys = r.equity.map(p => p.v);
    if (isAlpha && bnh_eq) {
      // Map each equity timestamp to nearest BnH daily index
      const btcTs = DATA.btc_1d.map(p => p.t);
      ys = ys.map((v, idx) => {
        const ts = r.equity[idx].t;
        let lo=0, hi=btcTs.length-1;
        while (lo<hi) { const mid=(lo+hi+1)>>1; if (btcTs[mid]<=ts) lo=mid; else hi=mid-1; }
        const bnh_v = bnh_eq[Math.min(lo, bnh_eq.length-1)];
        return bnh_v > 0 ? v / bnh_v : 1;
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
  if (!r || r.trades.length === 0) {
    div.innerHTML = '<div class="empty-state">이 전략에는 거래 기록이 없습니다.</div>';
    return;
  }
  const btc = DATA.btc_1d;
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

  const longTrades  = r.trades.filter(t => t.side === 'long');
  const shortTrades = r.trades.filter(t => t.side === 'short');

  const mkEntry = (trades, side, color, symbol) => ({
    type: 'scatter', mode: 'markers',
    x: trades.map(t => new Date(t.t_open)),
    y: trades.map(t => t.entry),
    name: side === 'long' ? '롱 진입' : '숏 진입',
    marker: { size: 9, color, symbol, line: { color: 'white', width: 1 } },
    hovertemplate: (side === 'long' ? '롱 진입' : '숏 진입') +
      '<br>진입가: $%{y:,.0f}<br>날짜: %{x|%Y-%m-%d}<extra></extra>',
  });

  const mkExit = (trades, label, color) => ({
    type: 'scatter', mode: 'markers',
    x: trades.map(t => new Date(t.t_close)),
    y: trades.map(t => t.exit),
    name: label,
    customdata: trades.map(t => [t.pnl, t.exit]),
    marker: { size: 8, color, symbol: 'square', line: { color: 'white', width: 1 } },
    hovertemplate: label + '<br>청산가: $%{y:,.0f}<br>손익: $%{customdata[0]:+,.0f}<br>날짜: %{x|%Y-%m-%d}<extra></extra>',
  });

  const winTrades  = r.trades.filter(t => t.pnl > 0);
  const lossTrades = r.trades.filter(t => t.pnl <= 0);

  const traces = [
    candleTrace,
    mkEntry(longTrades,  'long',  '#2ea043', 'triangle-up'),
    mkEntry(shortTrades, 'short', '#cf222e', 'triangle-down'),
    mkExit(winTrades,  '수익 청산 ■', '#3fb950'),
    mkExit(lossTrades, '손실 청산 ■', '#f85149'),
  ];

  const meta = DATA.meta[r.strat] || {};
  const title = `${meta.name_ko||r.strat} / ${r.variant} / ${r.tf} — 거래 ${r.trades.length}건`;
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
}

// ── Monthly Heatmap ───────────────────────────────────────────
function renderHeatmap() {
  const div = document.getElementById('chart-heatmap');
  if (state.selected.length === 0) {
    Plotly.purge(div);
    div.innerHTML = '<div class="empty-state">전략을 선택하면 월별 손익 히트맵이 표시됩니다.</div>';
    return;
  }
  // Build month labels 2020-01 … 2025-12
  const months = [];
  for (let y = 2020; y <= 2025; y++)
    for (let m = 1; m <= 12; m++)
      months.push(`${y}-${String(m).padStart(2,'0')}`);

  const zData = [], yLabels = [];

  for (const id of state.selected) {
    const r = getResultById(id);
    if (!r) continue;
    const map = {};
    for (const row of r.monthly) map[row.month] = row.pnl;
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
  const startMs = Date.UTC(year, mo - 1, 1);
  const endMs   = Date.UTC(year, mo, 1);

  // Trades that closed in this month (PnL realised on close)
  const trades = r.trades.filter(t => t.t_close >= startMs && t.t_close < endMs);

  const meta = DATA.meta[r.strat] || {};
  const stratLabel = `${meta.name_ko||r.strat} / ${r.variant==='long_only'?'롱전용':r.variant==='buy_and_hold'?'매수보유':'양방향'} / ${r.tf}`;

  if (trades.length === 0) {
    el.innerHTML = `<div class="trade-detail-wrap"><h5>${stratLabel} — ${month}: 해당 월에 청산된 거래 없음</h5></div>`;
    el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    return;
  }

  function fmtMs(ms) {
    const d = new Date(ms);
    const pad = n => String(n).padStart(2,'0');
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
  }

  const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);
  const rows = trades.map(t => {
    const cls = t.pnl > 0 ? 'kpi-pos' : 'kpi-neg';
    const sideKo = t.side === 'long' ? '롱' : '숏';
    return `<tr>
      <td>${sideKo}</td>
      <td>${fmtMs(t.t_open)} UTC</td>
      <td style="text-align:right">$${t.entry.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td>${fmtMs(t.t_close)} UTC</td>
      <td style="text-align:right">$${t.exit.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
      <td style="text-align:right" class="${cls}">$${t.pnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</td>
      <td style="text-align:right;color:#8b949e">$${t.fee.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
    </tr>`;
  }).join('');

  const totalCls = totalPnl > 0 ? 'kpi-pos' : 'kpi-neg';
  el.innerHTML = `<div class="trade-detail-wrap">
    <h5>${stratLabel} — ${month} 거래 내역 (${trades.length}건 청산 · 합계: <span class="${totalCls}">$${totalPnl.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2,signDisplay:'always'})}</span>)</h5>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>방향</th><th>진입 시각 (UTC)</th><th style="text-align:right">진입가</th>
          <th>청산 시각 (UTC)</th><th style="text-align:right">청산가</th>
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
const N_DAYS = 2191;  // returns_daily length (2020-01-02..2025-12-31)
// Day-index corresponding to a returns_daily index: i → 2020-01-02 + i days
function dayLabel(i) {
  const d = new Date(Date.UTC(2020, 0, 2) + i * 86400000);
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
    if (!r || !r.returns_daily) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const dd = drawdownSeries(r.returns_daily, r.stats.starting);
    const xs = [dayLabel(0)];
    for (let j=0; j<r.returns_daily.length; j++) xs.push(dayLabel(j+1));
    traces.push({
      x: xs, y: dd, type: 'scatter', mode: 'lines', name: label, fill: 'tozeroy',
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
    if (!r || !r.returns_daily) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    let ys;
    if (metric === 'sharpe') ys = rollingSharpe(r.returns_daily, w);
    else                     ys = rollingWinRate(r.returns_daily, w);
    const xs = [];
    for (let j=0; j<r.returns_daily.length; j++) xs.push(dayLabel(j));
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
    if (!r || r.trades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    const pnls = r.trades.map(t => t.pnl);
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
    const lowCount = r.trades.length < 50;
    const color = lowCount ? '#f85149' : PALETTE[i % PALETTE.length];
    if (r.trades.length === 0) continue;
    const xs = r.trades.map(t => new Date(t.t_close));
    const ys = r.trades.map((_, idx) => idx + 1);
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
    if (!r || r.trades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    // Duration in hours
    const durations = r.trades.map(t => (t.t_close - t.t_open) / 3600000);
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
    labels.push(`${meta.name_ko||r.strat}/${r.tf}/${r.variant==='long_only'?'롱':'양방'}`);
    winAct.push(r.streaks.win_max);
    lossAct.push(r.streaks.loss_max);
    const n = r.stats.trades;
    const p = r.stats.win_rate / 100;
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
    if (!r || r.trades.length === 0) continue;
    const meta = DATA.meta[r.strat] || {};
    const label = `${meta.name_ko||r.strat}/${r.tf}`;
    // Accumulate PnL by regime
    const pnlByRegime = {};
    for (const reg of REGIME_ORDER) pnlByRegime[reg] = 0;
    for (const t of r.trades) {
      const reg = getRegimeForMs(t.t_open);
      pnlByRegime[reg] = (pnlByRegime[reg] || 0) + t.pnl;
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
  if (!r || r.trades.length < 5) {
    div.innerHTML = '<div class="empty-state">거래 수가 너무 적어 Monte Carlo를 실행할 수 없습니다.</div>';
    return;
  }
  const meta = DATA.meta[r.strat] || {};
  const label = `${meta.name_ko||r.strat}/${r.tf}`;
  const pnls = r.trades.map(t => t.pnl);
  const N_ITER = 1000;
  const start = r.stats.starting;
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
  // Actual equity (ordered by trade close time)
  const actY = [start];
  let bal = start;
  for (const t of r.trades) { bal += t.pnl; actY.push(bal); }

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

// ── D2: Risk-Adjusted Scatter (all 76) ────────────────────
function renderRiskScatter() {
  const div = document.getElementById('chart-scatter');
  if (!div) return;
  const all = getAllResults().filter(r => r.variant !== 'buy_and_hold');
  if (all.length === 0) return;

  const meta = DATA.meta;
  const maxTrades = Math.max(...all.map(r=>r.stats.trades));
  const sharpes = all.map(r=>r.stats.sharpe);
  const minS = Math.min(...sharpes), maxS = Math.max(...sharpes);

  const varLabel = r => r.variant==='long_only'?'롱전용':'양방향';
  const hoverTexts = all.map(r => {
    const m = meta[r.strat]||{};
    return `${m.name_ko||r.strat} / ${r.tf} / ${varLabel(r)}<br>`+
           `CAGR: ${r.stats.cagr.toFixed(2)}%<br>`+
           `Sharpe: ${r.stats.sharpe.toFixed(3)}<br>`+
           `MDD: ${r.stats.mdd.toFixed(1)}%<br>`+
           `Trades: ${r.stats.trades}`;
  });

  // Color by Sharpe (red→yellow→green)
  const colorscale = [[0,'#f85149'],[0.5,'#d29922'],[1,'#3fb950']];

  const trace = {
    x: all.map(r => r.stats.mdd),
    y: all.map(r => r.stats.cagr),
    mode: 'markers',
    type: 'scatter',
    text: hoverTexts,
    hovertemplate: '%{text}<extra></extra>',
    customdata: all.map(r => r.id),
    marker: {
      size: all.map(r => 8 + Math.sqrt(r.stats.trades / Math.max(maxTrades,1)) * 24),
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
    x: all.filter(r=>selIds.has(r.id)).map(r=>r.stats.mdd),
    y: all.filter(r=>selIds.has(r.id)).map(r=>r.stats.cagr),
    mode: 'markers', type: 'scatter', name:'선택됨',
    marker: { size: 16, color:'rgba(0,0,0,0)', line:{color:'#f0f6fc', width:2} },
    hoverinfo: 'skip',
  };

  const layout = darkLayout({
    height: 380, margin: { l:70,r:90,t:30,b:60 },
    xaxis: { title:'MDD (%)', ticksuffix:'%', gridcolor:GRID_CLR, color:TEXT_CLR, autorange:'reversed' },
    yaxis: { title:'CAGR (%)', ticksuffix:'%', gridcolor:GRID_CLR, color:TEXT_CLR },
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
    return r ? r.returns_daily : null;
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
  const getMetrics = r => ({
    sharpe:   r.stats.sharpe,
    sortino:  r.stats.sortino || 0,
    mdd_inv:  -r.stats.mdd,  // less negative = better
    win_rate: r.stats.win_rate,
    pf:       Math.min(r.stats.pf, 5),  // cap at 5
    calmar:   Math.max(Math.min(r.stats.calmar||0, 5), -5),
  });
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
  renderHeatmap();
  renderValidationViews();
  renderDescription();
}

// ── Init ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initFilters();
  applyEquityToggles();

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


def generate_html(data_json: str, plotlyjs: str) -> str:
    gen_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
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
      <div class="subtitle">초기 자본 $10,000 · 2020-01 ~ 2025-12 · 9가지 전략 · 76개 백테스트</div>
    </div>

    <!-- Filters -->
    <div class="filters">
      <div class="filter-label">타임프레임</div>
      <div class="filter-row">
        <span class="tag active" data-filter="tf" data-value="1h">1h</span>
        <span class="tag active" data-filter="tf" data-value="2h">2h</span>
        <span class="tag active" data-filter="tf" data-value="4h">4h</span>
        <span class="tag active" data-filter="tf" data-value="1D">1D</span>
      </div>
      <div class="filter-label" style="margin-top:6px">변형</div>
      <div class="filter-row">
        <span class="tag active" data-filter="variant" data-value="long_only">롱전용</span>
        <span class="tag active" data-filter="variant" data-value="bidirectional">양방향</span>
        <span class="tag active" data-filter="variant" data-value="buy_and_hold">매수보유</span>
      </div>
      <div class="filter-label" style="margin-top:6px">정렬 / 보기</div>
      <div class="filter-row">
        <span class="sort-btn active" data-sort="alpha">전략별</span>
        <span class="sort-btn" data-sort="return">수익률순</span>
        <span class="sort-btn" data-sort="top10">Top 10</span>
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

    <!-- C1: Trade Markers -->
    <div class="chart-card" id="card-trades">
      <div class="chart-title">
        거래 시점 마커
        <small>▲ 진입 · ■ 청산(초록=수익, 빨강=손실) · 단일 전략 선택 시 활성화</small>
      </div>
      <div class="chart-wrap" id="chart-trades">
        <div class="empty-state">전략을 선택하면 거래 마커 차트가 표시됩니다.</div>
      </div>
    </div>

    <!-- C2: Monthly Heatmap -->
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

    <!-- D2: Risk-Adjusted Scatter (all 76) -->
    <div class="chart-card">
      <div class="chart-title">위험조정 산점도 (전체 76개) <small>x: MDD, y: CAGR, 크기: 거래 수, 색상: Sharpe · 클릭하면 선택</small></div>
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
      <div class="chart-title">Radar 지표 비교 <small>Sharpe · Sortino · MDD역수 · 승률 · PF · Calmar — 전체 76개 기준 정규화</small></div>
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
{DASHBOARD_JS}
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

    print('Collecting backtest results…')
    groups = collect_all_results()
    total = sum(len(v) for v in groups.values())
    print(f'  {total} results from {len(groups)} strategies')

    print('Loading BTC 1D prices…')
    btc_1d = load_btc_1d()
    print(f'  {len(btc_1d)} daily candles')

    print('Computing regime labels and BnH returns…')
    regime_labels = _classify_btc_regimes(btc_1d)
    # BnH daily returns from BTC closes (more accurate than trade-based forward-fill)
    closes = [p['c'] for p in btc_1d]
    bnh_returns_daily = [
        round((closes[i] - closes[i-1]) / closes[i-1], 6) if closes[i-1] > 0 else 0.0
        for i in range(1, len(closes))
    ]

    # Build compact data payload
    data = {
        'generated_at':    datetime.now(timezone.utc).isoformat(),
        'btc_1d':          btc_1d,
        'regime_labels':   regime_labels,
        'bnh_returns':     bnh_returns_daily,
        'groups':          groups,
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
        } for k, m in STRATEGY_META.items()},
    }
    data_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    print(f'  Data JSON: {len(data_json)/1024:.0f} KB')

    print('Embedding Plotly.js…')
    plotlyjs = get_plotlyjs()
    print(f'  Plotly.js: {len(plotlyjs)/1024:.0f} KB')

    print(f'Generating HTML → {out}')
    html = generate_html(data_json, plotlyjs)
    out.write_text(html, encoding='utf-8')
    size_mb = len(html.encode('utf-8')) / 1024 / 1024
    print(f'  Output: {size_mb:.1f} MB')
    print(f'\nDone! Open: firefox "{out}"')


if __name__ == '__main__':
    main()
