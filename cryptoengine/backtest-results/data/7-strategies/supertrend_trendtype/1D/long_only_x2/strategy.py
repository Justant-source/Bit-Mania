"""
Supertrend + TrendType Hybrid Strategy — Jesse 2.x port
Original: Pine Script supertrend-trendtype-strategy

5-factor confluence: TrendType regime + Supertrend direction + EMA cross + 200 EMA.
Very selective entry → expect low trade count. FAIL likely due to < 30 trades.
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalTrendType
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class SupertrendTrendTypeStrategy(Strategy):

    def hyperparameters(self):
        return [
            # TrendType
            {'name': 'atr_len',          'type': int,   'default': 14,  'min': 5,   'max': 30},
            {'name': 'atr_ma_len',       'type': int,   'default': 20,  'min': 10,  'max': 50},
            {'name': 'di_len',           'type': int,   'default': 14,  'min': 5,   'max': 30},
            {'name': 'smooth',           'type': int,   'default': 1,   'min': 1,   'max': 5},
            # Supertrend
            {'name': 'st_factor',        'type': float, 'default': 3.0, 'min': 1.0, 'max': 5.0},
            {'name': 'st_period',        'type': int,   'default': 7,   'min': 3,   'max': 20},
            # EMA
            {'name': 'fast_ema_len',     'type': int,   'default': 7,   'min': 3,   'max': 20},
            {'name': 'slow_ema_len',     'type': int,   'default': 20,  'min': 10,  'max': 50},
            {'name': 'direction_ema_len','type': int,   'default': 200, 'min': 100, 'max': 300},
            {'name': 'atr_mult',         'type': float, 'default': 3.0, 'min': 1.0, 'max': 5.0},
        ]

    def __init__(self):
        super().__init__()
        self._tt = None
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

    @property
    def st_direction(self) -> int:
        """Version-tolerant supertrend direction: +1 uptrend, -1 downtrend."""
        st = ta.supertrend(self.candles,
                           period=self.hp['st_period'],
                           factor=self.hp['st_factor'],
                           sequential=False)
        if hasattr(st, 'trend'):
            t = st.trend
            if isinstance(t, (int, bool)) and t in (1, -1):
                return int(t)
            return 1 if self.price > float(t) else -1
        if hasattr(st, 'supertrend'):
            return 1 if self.price > float(st.supertrend) else -1
        if isinstance(st, (tuple, list)):
            return 1 if self.price > float(st[0]) else -1
        return 1 if self.price > float(st) else -1

    @property
    def _st_uptrend(self) -> bool:
        return self.st_direction == 1

    def should_long(self) -> bool:
        tt = self._trend_type()
        if np.isnan(tt) or tt != 2.0:
            return False
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'],      sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'],      sequential=False)
        dema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
        return self._st_uptrend and fast > slow and self.price > dema

    def should_short(self) -> bool:
        tt = self._trend_type()
        if np.isnan(tt) or tt != -2.0:
            return False
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'],      sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'],      sequential=False)
        dema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
        return not self._st_uptrend and fast < slow and self.price < dema

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
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'], sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'], sequential=False)
        atr_stop = ta.atr(self.candles, period=self.hp['atr_len'], sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if fast < slow:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if fast > slow:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
