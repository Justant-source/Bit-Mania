"""
BBWP (Bollinger Band Width Percentile) Strategy — Jesse 2.x port
Original: Pine Script BBWP Strategy (trading-bot/strategies/bbwp-strategy/)

Volatility compression entry: BBWP <= lower_limit AND MACD direction.
Exit: BBWP crossdown from upper_limit OR ATR stop.
Incremental implementation to avoid O(n^2) cost on 6-year backtests.
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalBBWP
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class BBWPStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'bb_len',      'type': int,   'min': 10, 'max': 30,  'default': 13},
            {'name': 'bbwp_lookback','type': int,   'min': 100,'max': 500, 'default': 252},
            {'name': 'lower_limit', 'type': float, 'min': 5,  'max': 20,  'default': 10},
            {'name': 'upper_limit', 'type': float, 'min': 80, 'max': 95,  'default': 90},
            {'name': 'atr_mult',    'type': float, 'min': 1.5,'max': 5.0, 'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._bbwp = IncrementalBBWP()
        self._last_entry = 0.0

    def _get_bbwp(self) -> float:
        return self._bbwp.update(self.candles, self.hp['bb_len'], self.hp['bbwp_lookback'])

    def should_long(self) -> bool:
        bbwp = self._get_bbwp()
        if np.isnan(bbwp):
            return False
        macd = ta.macd(self.candles, sequential=False)
        return bbwp <= self.hp['lower_limit'] and macd.macd >= macd.signal

    def should_short(self) -> bool:
        bbwp = self._get_bbwp()
        if np.isnan(bbwp):
            return False
        macd = ta.macd(self.candles, sequential=False)
        return bbwp <= self.hp['lower_limit'] and macd.macd <= macd.signal

    def go_long(self):
        qty = self.balance * 0.95 * LEVERAGE / self.price
        self.buy = qty, self.price
        self._last_entry = self.price

    def go_short(self):
        qty = self.balance * 0.95 * LEVERAGE / self.price
        self.sell = qty, self.price
        self._last_entry = self.price

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        bbwp_cur = self._get_bbwp()
        bbwp_prev = self._bbwp.get_prev_bbwp()
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']
        # Exit: BBWP crossed down from upper_limit
        crossed_down = (not np.isnan(bbwp_prev) and not np.isnan(bbwp_cur)
                        and bbwp_prev >= self.hp['upper_limit']
                        and bbwp_cur < self.hp['upper_limit'])

        if self.is_long:
            if crossed_down:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if crossed_down:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
