"""
BBPB (Bollinger Band Percent B) Strategy — Jesse 2.x port
Original: Pine Script BBPB Strategy (trading-bot/strategies/bbpb-strategy/)

Entry LONG:  %B crossup (≤0 → ≥0) AND MACD_line >= signal_line
Entry SHORT: %B crossdown (≥1 → ≤1) AND MACD_line <= signal_line
Exit:        state reversal / MACD cross / ATR(14)*mult stop
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta


class BBPBStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'bb_len',    'type': int,   'min': 10, 'max': 50,  'default': 20},
            {'name': 'bb_std',    'type': float, 'min': 1.0,'max': 3.0, 'default': 2.0},
            {'name': 'macd_fast', 'type': int,   'min': 8,  'max': 16,  'default': 12},
            {'name': 'macd_slow', 'type': int,   'min': 20, 'max': 40,  'default': 26},
            {'name': 'macd_sig',  'type': int,   'min': 7,  'max': 12,  'default': 9},
            {'name': 'atr_mult',  'type': float, 'min': 1.5,'max': 5.0, 'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        # State machine: 0=NONE, 1=LONG_STATE, -1=SHORT_STATE
        self._pstate = 0
        self._pb_prev = float('nan')
        self._last_entry = 0.0

    def _percentb(self) -> float:
        bb = ta.bollinger_bands(self.candles,
                                period=self.hp['bb_len'],
                                devup=self.hp['bb_std'],
                                devdn=self.hp['bb_std'],
                                sequential=False)
        denom = bb.upperband - bb.lowerband
        if denom == 0 or np.isnan(denom):
            return 0.5
        return (self.price - bb.lowerband) / denom

    def _update_state(self, pb_cur: float):
        pb_prev = self._pb_prev
        if np.isnan(pb_prev):
            self._pb_prev = pb_cur
            return
        # State exits
        if self._pstate == 1 and pb_cur >= 1.0:
            self._pstate = 0
        if self._pstate == -1 and pb_cur <= 0.0:
            self._pstate = 0
        # State entries (crossovers)
        if pb_prev <= 0.0 and pb_cur > 0.0:
            self._pstate = 1
        if pb_prev >= 1.0 and pb_cur < 1.0:
            self._pstate = -1
        self._pb_prev = pb_cur

    def should_long(self) -> bool:
        pb = self._percentb()
        self._update_state(pb)
        macd = ta.macd(self.candles,
                       fast_period=self.hp['macd_fast'],
                       slow_period=self.hp['macd_slow'],
                       signal_period=self.hp['macd_sig'],
                       sequential=False)
        return self._pstate == 1 and macd.macd >= macd.signal

    def should_short(self) -> bool:
        pb = self._percentb()
        self._update_state(pb)
        macd = ta.macd(self.candles,
                       fast_period=self.hp['macd_fast'],
                       slow_period=self.hp['macd_slow'],
                       signal_period=self.hp['macd_sig'],
                       sequential=False)
        return self._pstate == -1 and macd.macd <= macd.signal

    def go_long(self):
        qty = self.balance * 0.95 / self.price
        self.buy = qty, self.price
        self._last_entry = self.price

    def go_short(self):
        qty = self.balance * 0.95 / self.price
        self.sell = qty, self.price
        self._last_entry = self.price

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        macd = ta.macd(self.candles,
                       fast_period=self.hp['macd_fast'],
                       slow_period=self.hp['macd_slow'],
                       signal_period=self.hp['macd_sig'],
                       sequential=False)
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if self._pstate != 1 or macd.macd <= macd.signal:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if self._pstate != -1 or macd.macd >= macd.signal:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
