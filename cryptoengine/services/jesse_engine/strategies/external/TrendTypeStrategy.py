"""
TrendType Strategy (ADX/ATR Regime Classifier) — Jesse 2.x port
Original: Pine Script trendtype-strategy (trading-bot/strategies/trendtype-strategy/)

ADX/ATR regime: 0 (sideways), +2 (uptrend), -2 (downtrend).
Entry when regime changes to directional; exit when regime changes back.
Incremental DMI computation for O(1) per step.
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalTrendType
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class TrendTypeStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'atr_len',    'type': int,   'min': 7,   'max': 21,  'default': 14},
            {'name': 'atr_ma_len', 'type': int,   'min': 10,  'max': 30,  'default': 20},
            {'name': 'di_len',     'type': int,   'min': 7,   'max': 21,  'default': 14},
            {'name': 'adx_len',    'type': int,   'min': 7,   'max': 21,  'default': 14},
            {'name': 'smooth',     'type': int,   'min': 1,   'max': 5,   'default': 1},
            {'name': 'atr_mult',   'type': float, 'min': 1.5, 'max': 5.0, 'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._tt = None  # initialized lazily with hp values
        self._last_entry = 0.0

    def _trend_type(self) -> float:
        if self._tt is None:
            self._tt = IncrementalTrendType(
                atr_len=self.hp['atr_len'],
                atr_ma_len=self.hp['atr_ma_len'],
                di_len=self.hp['di_len'],
                smooth=self.hp['smooth'],
            )
        return self._tt.update(self.candles)

    def should_long(self) -> bool:
        tt = self._trend_type()
        return not np.isnan(tt) and tt == 2.0

    def should_short(self) -> bool:
        tt = self._trend_type()
        return not np.isnan(tt) and tt == -2.0

    def go_long(self):
        qty = (self.balance * 0.95) * LEVERAGE / self.price
        self.buy = qty, self.price
        self._last_entry = self.price

    def go_short(self):
        qty = (self.balance * 0.95) * LEVERAGE / self.price
        self.sell = qty, self.price
        self._last_entry = self.price

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        tt = self._trend_type()
        atr_stop = ta.atr(self.candles, period=self.hp['atr_len'], sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if not np.isnan(tt) and tt != 2.0:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if not np.isnan(tt) and tt != -2.0:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
