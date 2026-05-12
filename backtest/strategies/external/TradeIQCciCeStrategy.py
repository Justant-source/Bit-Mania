"""
TradeIQ CCI-CE Strategy (CCI + Chandelier Exit) — Jesse 2.x port
Original: Pine Script tradeiq-220323-strategy (trading-bot/strategies/tradeiq-220323-strategy/)

Hybrid: CCI mean-reversion entry + Chandelier trailing stop direction filter.
CCI cross ±100 triggers entry when Chandelier direction agrees.
Exit: ATR stop only (Pine original's CCI/dir exits are commented out).
Incremental Chandelier for O(ce_period) per step instead of O(n^2).
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalChandelier
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class TradeIQCciCeStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'cci_period', 'type': int,   'min': 14,  'max': 30,  'default': 20},
            {'name': 'cci_lower',  'type': float,                           'default': -100.0},
            {'name': 'cci_upper',  'type': float,                           'default': 100.0},
            {'name': 'ce_period',  'type': int,   'min': 14,  'max': 30,  'default': 22},
            {'name': 'ce_mult',    'type': float, 'min': 2.0, 'max': 4.0, 'default': 3.0},
            {'name': 'atr_mult',   'type': float, 'min': 1.5, 'max': 5.0, 'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._ce = None
        self._cci_prev = float('nan')
        self._last_entry = 0.0

    def _chandelier(self):
        if self._ce is None:
            self._ce = IncrementalChandelier(
                ce_period=self.hp['ce_period'],
                ce_mult=self.hp['ce_mult'],
            )
        return self._ce.update(self.candles)

    def _cci_vals(self):
        cci_seq = ta.cci(self.candles, period=self.hp['cci_period'], sequential=True)
        cur = float(cci_seq[-1]) if len(cci_seq) > 0 else float('nan')
        prev = float(cci_seq[-2]) if len(cci_seq) >= 2 else float('nan')
        return prev, cur

    def should_long(self) -> bool:
        _, _, direction = self._chandelier()
        if np.isnan(direction):
            return False
        cci_prev, cci_cur = self._cci_vals()
        if np.isnan(cci_prev) or np.isnan(cci_cur):
            return False
        cci_cross_up = cci_prev < self.hp['cci_lower'] and cci_cur > self.hp['cci_lower']
        return cci_cross_up and direction == 1.0

    def should_short(self) -> bool:
        _, _, direction = self._chandelier()
        if np.isnan(direction):
            return False
        cci_prev, cci_cur = self._cci_vals()
        if np.isnan(cci_prev) or np.isnan(cci_cur):
            return False
        cci_cross_down = cci_prev > self.hp['cci_upper'] and cci_cur < self.hp['cci_upper']
        return cci_cross_down and direction == -1.0

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
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']
        if self.is_long or self.is_short:
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate()
