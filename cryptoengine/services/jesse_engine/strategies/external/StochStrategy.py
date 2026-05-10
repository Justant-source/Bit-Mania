"""
Stoch Strategy (Stochastic + EMA + Heikin Ashi) — Jesse 2.x port
Original: Pine Script stoch-strategy (trading-bot/strategies/stoch-strategy/)

Mean reversion entry via oversold/overbought Stoch, confirmed by EMA trend + HA candle.
Exit: ATR stop only (EMA-based exit is commented out in Pine original).
Incremental HA to avoid O(n^2) on 6-year backtests.
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalHeikinAshi
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class StochStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'fast_n',          'type': int,   'min': 5,   'max': 15,  'default': 7},
            {'name': 'slow_n',          'type': int,   'min': 15,  'max': 30,  'default': 20},
            {'name': 'direction_ema_len','type': int,   'min': 100, 'max': 300, 'default': 200},
            {'name': 'stoch_k_period',  'type': int,   'min': 10,  'max': 20,  'default': 14},
            {'name': 'stoch_smooth',    'type': int,   'min': 1,   'max': 5,   'default': 3},
            {'name': 'ob_level',        'type': float,                          'default': 80.0},
            {'name': 'os_level',        'type': float,                          'default': 20.0},
            {'name': 'atr_mult',        'type': float, 'min': 1.5, 'max': 5.0, 'default': 3.0},
            {'name': 'use_direction_ema','type': bool,                           'default': False},
        ]

    def __init__(self):
        super().__init__()
        self._ob_state = ''       # '' / 'OVER_BOUGHT' / 'OVER_SOLD'
        self._ha = IncrementalHeikinAshi()
        self._last_entry = 0.0
        self._short_ema_prev = float('nan')

    def _stoch_k(self) -> float:
        st = ta.stoch(self.candles,
                      fastk_period=self.hp['stoch_k_period'],
                      slowk_period=self.hp['stoch_smooth'],
                      sequential=False)
        return float(st.k)

    def _update_ob_state(self, k: float):
        if k >= self.hp['ob_level']:
            self._ob_state = 'OVER_BOUGHT'
        elif k <= self.hp['os_level']:
            self._ob_state = 'OVER_SOLD'

    def _trends(self):
        s = ta.ema(self.candles, period=self.hp['fast_n'],  sequential=False)
        l = ta.ema(self.candles, period=self.hp['slow_n'],  sequential=False)
        # Need prev short EMA for trend direction check
        if len(self.candles) >= 2:
            s_seq = ta.ema(self.candles, period=self.hp['fast_n'], sequential=True)
            l_seq = ta.ema(self.candles, period=self.hp['slow_n'], sequential=True)
            s_prev = float(s_seq[-2]) if len(s_seq) >= 2 else s
            l_prev = float(l_seq[-2]) if len(l_seq) >= 2 else l
        else:
            s_prev = s; l_prev = l
        is_up   = s > l and s > s_prev and l > l_prev
        is_down = s < l and s < s_prev and l < l_prev
        return float(s), float(s_prev), is_up, is_down

    def should_long(self) -> bool:
        if len(self.candles) < self.hp['slow_n'] + 2:
            return False
        k = self._stoch_k()
        self._update_ob_state(k)
        s, s_prev, is_up, is_down = self._trends()
        _, _, ha_open, ha_close = self._ha.update(self.candles)

        cond_a = (self._ob_state == 'OVER_SOLD'
                  and ha_close > ha_open
                  and not is_down
                  and s > s_prev)
        if self.hp['use_direction_ema']:
            d_ema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
            cond_a = cond_a and self.price > d_ema
        return cond_a or is_up

    def should_short(self) -> bool:
        if len(self.candles) < self.hp['slow_n'] + 2:
            return False
        k = self._stoch_k()
        self._update_ob_state(k)
        s, s_prev, is_up, is_down = self._trends()
        _, _, ha_open, ha_close = self._ha.update(self.candles)

        cond_a = (self._ob_state == 'OVER_BOUGHT'
                  and ha_close < ha_open
                  and not is_up
                  and s < s_prev)
        if self.hp['use_direction_ema']:
            d_ema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
            cond_a = cond_a and self.price < d_ema
        return cond_a or is_down

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
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']
        if self.is_long or self.is_short:
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate()
