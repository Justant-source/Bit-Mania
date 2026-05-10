"""
Momentum MA Strategy (LazyBear Momentum + EMA) — Jesse 2.x port
Original: Pine Script momentum-ma-strategy (trading-bot/strategies/momentum-ma-strategy/)

val = linreg(close - avg(avg(highest(high,N), lowest(low,N)), sma(close,N)), N, 0)
val_ma = ema(val, 100)
Entry: val crosses val_ma; Exit: reverse cross or ATR stop.

Efficient implementation:
- midline components computed with Jesse indicators (sequential=False, O(n) in C)
- linreg computed on last N deviations using numpy.polyfit (O(N^2) = O(400))
- val_ma updated incrementally (O(1))
"""
from __future__ import annotations

import numpy as np
from jesse.strategies import Strategy
import jesse.indicators as ta
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class MomentumMAStrategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'lin_len',    'type': int,   'min': 10,  'max': 50,  'default': 20},
            {'name': 'val_ma_len', 'type': int,   'min': 50,  'max': 200, 'default': 100},
            {'name': 'atr_mult',   'type': float, 'min': 1.5, 'max': 5.0, 'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._val_ma = float('nan')  # running EMA of val
        self._val_prev = float('nan')
        self._last_entry = 0.0

    def _compute_val_last(self) -> float:
        """
        Compute the last val = linreg(deviation, lin_len, 0) efficiently.
        Uses last lin_len candles only — O(lin_len^2) per call.
        """
        lin = self.hp['lin_len']
        n = len(self.candles)
        if n < lin * 2:
            return float('nan')

        recent = self.candles[-(lin * 2):]  # extra buffer for rolling windows
        m = len(recent)

        high = recent[:, 3]; low = recent[:, 4]; close = recent[:, 2]

        # Compute midline for the last lin points
        deviations = np.empty(lin)
        for k in range(lin):
            i = m - lin + k
            start = max(0, i - lin + 1)
            hh = np.max(high[start:i + 1])
            ll = np.min(low[start:i + 1])
            sm = np.mean(close[start:i + 1])
            mid = ((hh + ll) / 2.0 + sm) / 2.0
            deviations[k] = close[i] - mid

        # linreg: regression over deviations, return value at last point
        x = np.arange(lin, dtype=float)
        coeffs = np.polyfit(x, deviations, 1)  # [slope, intercept]
        return float(coeffs[0] * (lin - 1) + coeffs[1])

    def _update_val_ma(self, val: float) -> float:
        """Update val EMA incrementally — O(1)."""
        if np.isnan(val):
            return self._val_ma
        alpha = 2.0 / (self.hp['val_ma_len'] + 1)
        if np.isnan(self._val_ma):
            self._val_ma = val
        else:
            self._val_ma = alpha * val + (1 - alpha) * self._val_ma
        return self._val_ma

    def should_long(self) -> bool:
        val = self._compute_val_last()
        val_ma = self._update_val_ma(val)
        self._val_prev = val
        if np.isnan(val) or np.isnan(val_ma):
            return False
        return val > val_ma

    def should_short(self) -> bool:
        val = self._compute_val_last()
        val_ma = self._update_val_ma(val)
        if np.isnan(val) or np.isnan(val_ma):
            return False
        return val < val_ma

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
        val = self._compute_val_last()
        val_ma = self._val_ma
        if np.isnan(val) or np.isnan(val_ma):
            return
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if val < val_ma:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if val > val_ma:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
