"""
Supertrend Strategy (Supertrend + EMA cross + direction filter) — Jesse 2.x port
Original: Pine Script supertrend-strategy (trading-bot/strategies/supertrend-strategy/)

Triple confirmation: Supertrend direction + EMA cross + 200 EMA filter.
In Jesse 2.x, SuperTrend.trend is a price value (the ST line), not 1/-1.
Direction = uptrend when price > ST line.
"""
from __future__ import annotations

from jesse.strategies import Strategy
import jesse.indicators as ta
import os

LEVERAGE = float(os.environ.get('STRATEGY_LEVERAGE', '1'))


class SupertrendStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'st_factor',        'type': float, 'min': 1.0, 'max': 5.0,  'default': 3.0},
            {'name': 'st_period',        'type': int,   'min': 5,   'max': 14,   'default': 7},
            {'name': 'fast_ema_len',     'type': int,   'min': 5,   'max': 15,   'default': 7},
            {'name': 'slow_ema_len',     'type': int,   'min': 15,  'max': 30,   'default': 20},
            {'name': 'direction_ema_len','type': int,   'min': 100, 'max': 300,  'default': 200},
            {'name': 'atr_mult',         'type': float, 'min': 1.5, 'max': 5.0,  'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._last_entry = 0.0

    @property
    def st_direction(self) -> int:
        """Version-tolerant supertrend direction: +1 uptrend, -1 downtrend."""
        st = ta.supertrend(self.candles,
                           period=self.hp['st_period'],
                           factor=self.hp['st_factor'],
                           sequential=False)
        if hasattr(st, 'trend'):
            # Jesse 1.x: trend is int enum (1=up, -1=down); Jesse 2.x: trend is price line
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
    def _st_is_uptrend(self) -> bool:
        return self.st_direction == 1

    def should_long(self) -> bool:
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'],      sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'],      sequential=False)
        dema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
        return self._st_is_uptrend and fast > slow and self.price > dema

    def should_short(self) -> bool:
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'],      sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'],      sequential=False)
        dema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
        return not self._st_is_uptrend and fast < slow and self.price < dema

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
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'], sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'], sequential=False)
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']

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
