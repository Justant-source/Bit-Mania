#!/usr/bin/env python3
"""
pre21_backfill.py — Backfill frozen champion strategies across 6 pre-2021 periods.

5 frozen champion strategies × 6 pre-2021 periods = 30 jobs total.

Periods (all before 2021-01-01):
  pre21_full:     2017-08-18 ~ 2020-12-31  (complete period)
  pre21_bear:     2017-12-17 ~ 2018-12-15  (2017-18 bear market)
  pre21_range:    2018-12-16 ~ 2019-04-01  (accumulation range)
  pre21_recovery: 2019-04-02 ~ 2020-02-29  (post-bottom recovery)
  pre21_covid:    2020-03-01 ~ 2020-04-30  (March COVID crash → recovery)
  pre21_bull:     2020-05-01 ~ 2020-12-31  (May-Dec bull run)

Output structure:
  RESULT_DIR / strat / tf / variant / period_key / {
    result.json,        (for apply_realistic_costs_pre21.py)
    trades.csv,
    monthly_returns.csv,
    stats.json,
    parameters.json,
    strategy.py,
  }

Usage (inside Jesse/backtester container):
    python /app/scripts/sweep/pre21_backfill.py --workers 4
    python /app/scripts/sweep/pre21_backfill.py --strategies supertrend --periods pre21_bear
    python /app/scripts/sweep/pre21_backfill.py --dry-run

Idempotent: result.json exists → SKIP.
"""
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import shutil
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# ── Path setup ─────────────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
JESSE_ROOT  = SCRIPTS_DIR.parent
sys.path.insert(0, str(JESSE_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(JESSE_ROOT / 'runners'))
sys.path.insert(0, str(JESSE_ROOT / 'strategies'))

from run_external_backtest import (
    _load_1h, _resample_1h, _expand_tf_to_1m, _upsample_to_1m,
    _extract_metrics, _pass_fail,
    _write_stats, _write_trades, _write_monthly_returns,
    EXCHANGE_NAME, SYMBOL, TF_MINUTES,
)

# ── Constants ──────────────────────────────────────────────────────────────────
BALANCE       = 10_000.0
FEE           = 0.0002
_DEFAULT_LEVERAGE = 1  # per-entry leverage in STRATEGIES_MANIFEST overrides this

# Auto-detect result dir: /result/pre2021_backfill if mounted, else JESSE_ROOT/results
_RESULT_MOUNTED = Path('/result/pre2021_backfill')
if _RESULT_MOUNTED.exists() or Path('/result').exists():
    RESULT_DIR = _RESULT_MOUNTED
else:
    RESULT_DIR = JESSE_ROOT / 'results' / 'pre2021_backfill'

FULL_START    = '2017-06-19'  # 60d warmup before 2017-08-18
FULL_END      = '2020-12-31'

PRE21_PERIODS = {
    'pre21_full':     ('2017-08-18', '2020-12-31'),
    'pre21_bear':     ('2017-12-17', '2018-12-15'),
    'pre21_range':    ('2018-12-16', '2019-04-01'),
    'pre21_recovery': ('2019-04-02', '2020-02-29'),
    'pre21_covid':    ('2020-03-01', '2020-04-30'),
    'pre21_bull':     ('2020-05-01', '2020-12-31'),
}

# ── Strategy manifest (FROZEN — no tuning) ────────────────────────────────────
# 5 original champions (hardcoded HP preserved for idempotency)
_HP_SUPERTREND          = {'st_factor': 1.5, 'st_period': 10, 'fast_ema_len': 7,
                            'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0}
_HP_SUPERTREND_TT       = {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1,
                            'st_factor': 1.8, 'st_period': 7, 'fast_ema_len': 7,
                            'slow_ema_len': 20, 'direction_ema_len': 200, 'atr_mult': 3.0}
_HP_TRENDTYPE           = {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 8, 'adx_len': 8,
                            'smooth': 1, 'atr_mult': 3.0}
_HP_TRADEIQ_PSAR_HA     = {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
                            'direction_ema_len': 200, 'rsi_len': 20, 'atr_mult': 3.0}
_HP_TRADEIQ_CCI_CE      = {'cci_period': 18, 'cci_lower': -100, 'cci_upper': 100,
                            'ce_period': 22, 'ce_mult': 3.0, 'atr_mult': 3.0}

STRATEGIES_MANIFEST = [
    # ── Original 5 champions lev=1 (idempotent skip — already done) ──────────────
    {'strat': 'supertrend',           'tf': '4h', 'variant': 'long_only',
     'cls_name': 'SupertrendStrategy',          'leverage': 1,
     'champ_src': ('v4', 18), 'hp': _HP_SUPERTREND},
    {'strat': 'supertrend_trendtype', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': ('v3', 6),  'hp': _HP_SUPERTREND_TT},
    {'strat': 'trendtype',            'tf': '1D', 'variant': 'long_only',
     'cls_name': 'TrendTypeStrategy',           'leverage': 1,
     'champ_src': ('v3', 6),  'hp': _HP_TRENDTYPE},
    {'strat': 'tradeiq_psar_ha',      'tf': '1D', 'variant': 'long_only',
     'cls_name': 'TradeIQPsarHaStrategy',       'leverage': 1,
     'champ_src': ('v3', 8),  'hp': _HP_TRADEIQ_PSAR_HA},
    {'strat': 'tradeiq_cci_ce',       'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'TradeIQCciCeStrategy',        'leverage': 1,
     'champ_src': ('v3', 2),  'hp': _HP_TRADEIQ_CCI_CE},
    # ── Original 5 champions lev=2 (idempotent skip) ────────────────────────────
    {'strat': 'supertrend',           'tf': '4h', 'variant': 'long_only_x2',
     'cls_name': 'SupertrendStrategy',          'leverage': 2,
     'champ_src': ('v4', 18), 'hp': _HP_SUPERTREND},
    {'strat': 'supertrend_trendtype', 'tf': '4h', 'variant': 'long_only_x2',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 2,
     'champ_src': ('v3', 6),  'hp': _HP_SUPERTREND_TT},
    {'strat': 'trendtype',            'tf': '1D', 'variant': 'long_only_x2',
     'cls_name': 'TrendTypeStrategy',           'leverage': 2,
     'champ_src': ('v3', 6),  'hp': _HP_TRENDTYPE},
    {'strat': 'tradeiq_psar_ha',      'tf': '1D', 'variant': 'long_only_x2',
     'cls_name': 'TradeIQPsarHaStrategy',       'leverage': 2,
     'champ_src': ('v3', 8),  'hp': _HP_TRADEIQ_PSAR_HA},
    {'strat': 'tradeiq_cci_ce',       'tf': '4h', 'variant': 'bidirectional_x2',
     'cls_name': 'TradeIQCciCeStrategy',        'leverage': 2,
     'champ_src': ('v3', 2),  'hp': _HP_TRADEIQ_CCI_CE},
    # ── Original 5 champions lev=3 (idempotent skip) ────────────────────────────
    {'strat': 'supertrend',           'tf': '4h', 'variant': 'long_only_x3',
     'cls_name': 'SupertrendStrategy',          'leverage': 3,
     'champ_src': ('v4', 18), 'hp': _HP_SUPERTREND},
    {'strat': 'supertrend_trendtype', 'tf': '4h', 'variant': 'long_only_x3',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 3,
     'champ_src': ('v3', 6),  'hp': _HP_SUPERTREND_TT},
    {'strat': 'trendtype',            'tf': '1D', 'variant': 'long_only_x3',
     'cls_name': 'TrendTypeStrategy',           'leverage': 3,
     'champ_src': ('v3', 6),  'hp': _HP_TRENDTYPE},
    {'strat': 'tradeiq_psar_ha',      'tf': '1D', 'variant': 'long_only_x3',
     'cls_name': 'TradeIQPsarHaStrategy',       'leverage': 3,
     'champ_src': ('v3', 8),  'hp': _HP_TRADEIQ_PSAR_HA},
    {'strat': 'tradeiq_cci_ce',       'tf': '4h', 'variant': 'bidirectional_x3',
     'cls_name': 'TradeIQCciCeStrategy',        'leverage': 3,
     'champ_src': ('v3', 2),  'hp': _HP_TRADEIQ_CCI_CE},
    # ── 53 additional combos (all remaining 7-strategies leaves) ─────────────────
    {'strat': 'momentum_ma', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '1D', 'variant': 'long_only',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '1h', 'variant': 'bidirectional_x2',
     'cls_name': 'MomentumMAStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '1h', 'variant': 'bidirectional_x3',
     'cls_name': 'MomentumMAStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'momentum_ma', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'MomentumMAStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'lin_len': 14, 'val_ma_len': 100, 'atr_mult': 3.0}},
    {'strat': 'stoch', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '1D', 'variant': 'long_only',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '1h', 'variant': 'bidirectional_x2',
     'cls_name': 'StochStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '1h', 'variant': 'bidirectional_x3',
     'cls_name': 'StochStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'stoch', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'StochStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'fast_n': 7, 'slow_n': 20, 'direction_ema_len': 200, 'stoch_k_period': 10,
            'stoch_smooth': 3, 'ob_level': 80.0, 'os_level': 20.0, 'atr_mult': 3.0,
            'use_direction_ema': False}},
    {'strat': 'supertrend', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'SupertrendStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '1D', 'variant': 'long_only',
     'cls_name': 'SupertrendStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '1D', 'variant': 'long_only_x2',
     'cls_name': 'SupertrendStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '1D', 'variant': 'long_only_x3',
     'cls_name': 'SupertrendStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'SupertrendStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'SupertrendStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_3',
     'hp': {'st_factor': 4.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'SupertrendStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'st_factor': 2.0, 'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.0,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1D', 'variant': 'long_only',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_5',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.5,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1D', 'variant': 'long_only_x2',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 2,
     'champ_src': 'v3/combo_5',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.5,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1D', 'variant': 'long_only_x3',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 3,
     'champ_src': 'v3/combo_5',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.5,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.0,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_5',
     'hp': {'atr_len': 18, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 3.0,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'supertrend_trendtype', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'SupertrendTrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 14, 'smooth': 1, 'st_factor': 2.0,
            'st_period': 7, 'fast_ema_len': 7, 'slow_ema_len': 20,
            'direction_ema_len': 200, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_cci_ce', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'TradeIQCciCeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_9',
     'hp': {'cci_period': 26, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 2.0, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_cci_ce', 'tf': '1D', 'variant': 'long_only',
     'cls_name': 'TradeIQCciCeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_9',
     'hp': {'cci_period': 26, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 2.0, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_cci_ce', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'TradeIQCciCeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_11',
     'hp': {'cci_period': 30, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 3.0, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_cci_ce', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'TradeIQCciCeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_15',
     'hp': {'cci_period': 30, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 3.5, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_cci_ce', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'TradeIQCciCeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_20',
     'hp': {'cci_period': 18, 'cci_lower': -100.0, 'cci_upper': 100.0,
            'ce_period': 22, 'ce_mult': 3.5, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '1h', 'variant': 'long_only_x2',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '1h', 'variant': 'long_only_x3',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 10, 'atr_mult': 3.0}},
    {'strat': 'tradeiq_psar_ha', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'TradeIQPsarHaStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_15',
     'hp': {'psar_start': 0.02, 'psar_inc': 0.02, 'psar_max': 0.2,
            'direction_ema_len': 200, 'rsi_len': 24, 'atr_mult': 3.5}},
    {'strat': 'trendtype', 'tf': '1D', 'variant': 'bidirectional',
     'cls_name': 'TrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_3',
     'hp': {'atr_len': 11, 'atr_ma_len': 20, 'di_len': 11, 'adx_len': 11,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1D', 'variant': 'bidirectional_x2',
     'cls_name': 'TrendTypeStrategy', 'leverage': 2,
     'champ_src': 'v3/combo_3',
     'hp': {'atr_len': 11, 'atr_ma_len': 20, 'di_len': 11, 'adx_len': 11,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1D', 'variant': 'bidirectional_x3',
     'cls_name': 'TrendTypeStrategy', 'leverage': 3,
     'champ_src': 'v3/combo_3',
     'hp': {'atr_len': 11, 'atr_ma_len': 20, 'di_len': 11, 'adx_len': 11,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'bidirectional',
     'cls_name': 'TrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'adx_len': 14,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'bidirectional_x2',
     'cls_name': 'TrendTypeStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'adx_len': 14,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'bidirectional_x3',
     'cls_name': 'TrendTypeStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'adx_len': 14,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'long_only',
     'cls_name': 'TrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_13',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 21, 'adx_len': 21,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'long_only_x2',
     'cls_name': 'TrendTypeStrategy', 'leverage': 2,
     'champ_src': 'v2/combo_13',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 21, 'adx_len': 21,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '1h', 'variant': 'long_only_x3',
     'cls_name': 'TrendTypeStrategy', 'leverage': 3,
     'champ_src': 'v2/combo_13',
     'hp': {'atr_len': 14, 'atr_ma_len': 20, 'di_len': 21, 'adx_len': 21,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '4h', 'variant': 'bidirectional',
     'cls_name': 'TrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v2/combo_1',
     'hp': {'atr_len': 10, 'atr_ma_len': 20, 'di_len': 14, 'adx_len': 14,
            'smooth': 1, 'atr_mult': 3.0}},
    {'strat': 'trendtype', 'tf': '4h', 'variant': 'long_only',
     'cls_name': 'TrendTypeStrategy', 'leverage': 1,
     'champ_src': 'v3/combo_11',
     'hp': {'atr_len': 8, 'atr_ma_len': 20, 'di_len': 12, 'adx_len': 12,
            'smooth': 1, 'atr_mult': 3.0}},
]

print(f'[init] pre21_backfill manifest: {len(STRATEGIES_MANIFEST)} entries × {len(PRE21_PERIODS)} periods = {len(STRATEGIES_MANIFEST) * len(PRE21_PERIODS)} jobs', flush=True)

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
    """Build period + warmup candles for a given TF. Copy pattern from param_sweep_v4.py."""
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
        tf_min     = TF_MINUTES[tf]
        candles_tf = _resample_1h(period_1h, tf)
        warmup_tf  = _resample_1h(warmup_1h, tf)
        if len(warmup_tf) == 0:
            # Not enough prior data for even one TF bar — borrow from main window
            borrow     = min(200, max(1, len(candles_tf) // 4))
            warmup_tf  = candles_tf[:borrow]
            candles_tf = candles_tf[borrow:]  # prevent timestamp overlap in Jesse
        candles  = _expand_tf_to_1m(candles_tf, tf_min)
        warmup   = _expand_tf_to_1m(warmup_tf, tf_min)
        route_tf = tf

    return candles, warmup, route_tf


def _load_strategy_cls(cls_name: str, variant: str):
    """Load strategy class by name. Apply long_only factory if needed."""
    ext_dir  = JESSE_ROOT / 'strategies' / 'external'
    mod_path = ext_dir / f'{cls_name}.py'
    spec_mod = importlib.util.spec_from_file_location(cls_name, mod_path)
    mod      = importlib.util.module_from_spec(spec_mod)
    spec_mod.loader.exec_module(mod)
    strategy_cls = getattr(mod, cls_name)
    if variant.startswith('long_only'):
        from external._long_only_factory import make_long_only
        strategy_cls = make_long_only(strategy_cls)
    return strategy_cls


def _jesse_run(strategy_cls, route_tf: str, tf: str, hp: dict, candles, warmup, leverage: int = 1):
    """Run Jesse backtest for given strategy + hyperparameters."""
    from jesse import research
    import jesse.helpers as jh
    os.environ['STRATEGY_LEVERAGE'] = str(leverage)

    key = jh.key(EXCHANGE_NAME, SYMBOL)
    config = {
        'starting_balance':      BALANCE,
        'fee':                   FEE,
        'type':                  'futures',
        'futures_leverage':      leverage,
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


def _verify_hp_drift(manifest_entry: dict, sweep_base_path: Path):
    """Load HP from champion file and compare with manifest. Non-fatal warning."""
    champ_src = manifest_entry['champ_src']
    # Support both tuple ('v3', 6) and string 'v3/combo_6' formats
    if isinstance(champ_src, (list, tuple)):
        version, combo_idx = champ_src[0], int(champ_src[1])
    else:
        parts = str(champ_src).split('/')
        version = parts[0]
        combo_idx = int(parts[1].replace('combo_', ''))
    manifest_hp = manifest_entry['hp']
    strat = manifest_entry['strat']
    tf = manifest_entry['tf']
    variant = manifest_entry['variant']

    if version == 'v4':
        champ_file = sweep_base_path / 'param_sweep_v4' / strat / tf / variant / f'combo_{combo_idx}' / 'summary.json'
    elif version in ('v2', 'v3'):
        champ_file = sweep_base_path / 'param_sweep' / version / strat / tf / variant / f'combo_{combo_idx}' / 'summary.json'
    else:
        print(f'  [warn] Unknown version {version}', flush=True)
        return

    if not champ_file.exists():
        print(f'  [warn] Champion file not found (OK in Docker): {champ_file}', flush=True)
        return

    try:
        summary = json.loads(champ_file.read_text())
        file_hp = summary.get('hp', {})
        for key, manifest_val in manifest_hp.items():
            file_val = file_hp.get(key)
            if file_val is not None and file_val != manifest_val:
                print(f'  [warn] HP drift in {key}: manifest={manifest_val} vs file={file_val}', flush=True)
    except Exception as e:
        print(f'  [warn] Could not verify HP: {e}', flush=True)


def _job_output_dir(strat: str, tf: str, variant: str, period_key: str) -> Path:
    return RESULT_DIR / strat / tf / variant / period_key


def _make_label(strat: str, tf: str, variant: str, period_key: str) -> str:
    return f'{strat}/{tf}/{variant}/{period_key}'


def run_job(job: dict) -> dict:
    """Run a single backtest job. Returns dict with status/elapsed/etc."""
    strat_entry = job['strat_entry']
    period_key = job['period_key']
    p_start = job['p_start']
    p_end = job['p_end']
    label = job['label']

    strat = strat_entry['strat']
    tf = strat_entry['tf']
    variant = strat_entry['variant']
    cls_name = strat_entry['cls_name']
    hp = strat_entry['hp']
    champ_src = strat_entry['champ_src']
    leverage = strat_entry.get('leverage', _DEFAULT_LEVERAGE)

    out_dir = _job_output_dir(strat, tf, variant, period_key)

    # Idempotency: skip if result.json exists
    if (out_dir / 'result.json').exists():
        return {'label': label, 'status': 'SKIP', 'elapsed': 0}

    t0 = time.monotonic()
    try:
        # Set leverage env var BEFORE loading strategy class — module-level LEVERAGE constant
        # is evaluated at load time; stale env from a prior lev>1 job on the same worker
        # causes oversized order values.
        os.environ['STRATEGY_LEVERAGE'] = str(leverage)
        # Load strategy class
        strategy_cls = _load_strategy_cls(cls_name, variant)

        # Load full 1h candles (cached per-process)
        full_1h = _get_full_1h()

        # Build period + warmup candles
        candles, warmup, route_tf = _build_period_candles(full_1h, tf, p_start, p_end)

        # Run Jesse backtest
        raw = _jesse_run(strategy_cls, route_tf, tf, hp, candles, warmup, leverage=leverage)

        # Extract metrics (no_upsample=True for 4h/1D, False for 1h)
        no_upsample = (tf != '1h')
        metrics = _extract_metrics(raw, p_start, p_end, no_upsample=no_upsample, timeframe=tf)

        # Build result.json (for apply_realistic_costs_pre21.py)
        result_data = {
            'strat': strat,
            'tf': tf,
            'variant': variant,
            'period': period_key,
            'champ_src': champ_src,
            'hp': hp,
            'metrics': {
                'annual_return_pct': metrics['annual_return_pct'],
                'cagr_pct': metrics['cagr_pct'],
                'sharpe_ratio': metrics['sharpe_ratio'],
                'max_drawdown_pct': metrics['max_drawdown_pct'],
                'total_trades': metrics['total_trades'],
                'win_rate_pct': metrics['win_rate_pct'],
                'profit_factor': metrics['profit_factor'],
                'net_profit_pct': metrics['net_profit_pct'],
                'gross_profit': metrics['gross_profit'],
                'gross_loss': metrics['gross_loss'],
            },
            'period_meta': {
                'start': p_start,
                'end': p_end,
                'warmup_days': max(60, TF_MINUTES[tf] // 60 * 220 // 24 + 1),
            },
            'completed_at': datetime.now(timezone.utc).isoformat(),
        }

        # Write output files
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'result.json').write_text(json.dumps(result_data, indent=2, default=str))

        # Write trades, stats, monthly returns (reuse helpers from run_external_backtest)
        trades = raw.get('trades', []) or []
        _write_trades(out_dir, trades)
        _write_monthly_returns(out_dir, trades, p_start, p_end)

        # Write stats.json (for dashboard compatibility)
        verdict, checks = _pass_fail(metrics)
        _write_stats(out_dir, f'{strat}/{tf}/{variant}', metrics, verdict, checks,
                     balance=BALANCE, leverage=leverage, variant=variant)

        # Write parameters.json
        (out_dir / 'parameters.json').write_text(
            json.dumps({
                'strat': strat,
                'tf': tf,
                'variant': variant,
                'champ_src': champ_src,
                'hp': hp,
            }, indent=2)
        )

        # Copy strategy source
        ext_dir = JESSE_ROOT / 'strategies' / 'external'
        shutil.copy2(ext_dir / f'{cls_name}.py', out_dir / 'strategy.py')

        elapsed = time.monotonic() - t0
        return {'label': label, 'status': 'OK', 'elapsed': elapsed}

    except Exception as e:
        elapsed = time.monotonic() - t0
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / 'BACKFILL_FAILED.marker').write_text(
            f"status: FAILED\nreason: {e}\ntb: {traceback.format_exc()}\n"
            f"executed_at: {datetime.now(timezone.utc).isoformat()}\n"
        )
        return {'label': label, 'status': 'FAIL', 'elapsed': elapsed}


def _build_jobs(strats_filter: list[str] | None = None,
                periods_filter: list[str] | None = None) -> list[dict]:
    """Build all jobs (cartesian product of strats × periods)."""
    jobs = []
    sweep_base = JESSE_ROOT / 'results'

    _leveraged_only = strats_filter == ['leveraged']

    for strat_entry in STRATEGIES_MANIFEST:
        strat = strat_entry['strat']

        # Apply strat filter
        if _leveraged_only:
            if strat_entry.get('leverage', 1) <= 1:
                continue
        elif strats_filter is not None and strat not in strats_filter:
            continue

        # Verify HP drift (non-fatal)
        _verify_hp_drift(strat_entry, sweep_base)

        # Cartesian product: strat × periods
        for period_key, (p_start, p_end) in PRE21_PERIODS.items():
            # Apply period filter
            if periods_filter is not None and period_key not in periods_filter:
                continue

            label = _make_label(strat, strat_entry['tf'], strat_entry['variant'], period_key)
            jobs.append({
                'strat_entry': strat_entry,
                'period_key': period_key,
                'p_start': p_start,
                'p_end': p_end,
                'label': label,
            })

    return jobs


def main() -> None:
    parser = argparse.ArgumentParser(
        description='pre21_backfill: 5 champions × 6 pre-2021 periods = 30 jobs'
    )
    parser.add_argument('--workers', type=int, default=1,
                        help='Number of parallel workers (default: 1)')
    parser.add_argument('--strategies', type=str, default='all',
                        help='Strategy filter: "all", "leveraged" (lev>1 only), or comma-separated names (default: all)')
    parser.add_argument('--periods', type=str, default='all',
                        help='Period filter: "all" or comma-separated keys (default: all)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print jobs without running')
    args = parser.parse_args()

    # Parse filters
    strats_filter = None if args.strategies == 'all' else args.strategies.split(',')
    periods_filter = None if args.periods == 'all' else args.periods.split(',')

    jobs = _build_jobs(strats_filter, periods_filter)
    total = len(jobs)
    done = sum(1 for j in jobs if (_job_output_dir(j['strat_entry']['strat'],
                                                     j['strat_entry']['tf'],
                                                     j['strat_entry']['variant'],
                                                     j['period_key']) / 'result.json').exists())

    print(f'pre21_backfill: {total} jobs ({done} done, {total-done} pending)  workers={args.workers}')
    print(f'Output: {RESULT_DIR}')
    print(f'Start: {datetime.now(timezone.utc).isoformat()}')
    print()

    if args.dry_run:
        for j in jobs:
            done_str = 'SKIP' if (_job_output_dir(j['strat_entry']['strat'],
                                                    j['strat_entry']['tf'],
                                                    j['strat_entry']['variant'],
                                                    j['period_key']) / 'result.json').exists() else 'PEND'
            print(f'  {done_str} {j["label"]}')
        print(f'\nTotal: {total}  Done: {done}  Pending: {total - done}')
        return

    t_global = time.monotonic()
    pending = [j for j in jobs if not (_job_output_dir(j['strat_entry']['strat'],
                                                        j['strat_entry']['tf'],
                                                        j['strat_entry']['variant'],
                                                        j['period_key']) / 'result.json').exists()]

    if args.workers <= 1:
        for i, job in enumerate(pending, 1):
            print(f'  [{i}/{len(pending)}] {job["label"]}', flush=True)
            r = run_job(job)
            elapsed_total = time.monotonic() - t_global
            print(f'  [{i}/{len(pending)}] {r["status"]:6s} {job["label"]}  '
                  f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_job, j): j for j in pending}
            for i, fut in enumerate(as_completed(futures), 1):
                r = fut.result()
                elapsed_total = time.monotonic() - t_global
                print(f'  [{i}/{len(pending)}] {r["status"]:6s} {r["label"]}  '
                      f'({r["elapsed"]:.0f}s, total {elapsed_total/60:.1f}m)', flush=True)

    ok = sum(1 for j in jobs if (_job_output_dir(j['strat_entry']['strat'],
                                                   j['strat_entry']['tf'],
                                                   j['strat_entry']['variant'],
                                                   j['period_key']) / 'result.json').exists())
    total_elapsed = time.monotonic() - t_global
    print(f'\n[done] {ok}/{total} jobs completed successfully ({total_elapsed/60:.1f}m)')


if __name__ == '__main__':
    main()
