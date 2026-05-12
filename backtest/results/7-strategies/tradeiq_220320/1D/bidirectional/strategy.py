"""
TradeIQ 220320 Strategy (PSAR + EMA + RSI + Heikin Ashi) — Jesse 2.x port
Original: Pine Script tradeiq-220320-strategy (trading-bot/strategies/tradeiq-220320-strategy/)

4-factor confluence entry: PSAR + 200 EMA + RSI > 50 + HA bullish candle.
Exit: Any of 4 conditions OR ATR stop.
Incremental HA to avoid O(n^2) on long backtests.
"""
from __future__ import annotations

from jesse.strategies import Strategy
import jesse.indicators as ta

from external._helpers import IncrementalHeikinAshi
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class TradeIQ220320Strategy(Strategy):

    def hyperparameters(self):
        return [
            {'name': 'psar_start',      'type': float, 'min': 0.01, 'max': 0.05, 'default': 0.02},
            {'name': 'psar_inc',        'type': float, 'min': 0.01, 'max': 0.05, 'default': 0.02},
            {'name': 'psar_max',        'type': float, 'min': 0.1,  'max': 0.5,  'default': 0.2},
            {'name': 'direction_ema_len','type': int,   'min': 100, 'max': 300,  'default': 200},
            {'name': 'rsi_len',         'type': int,   'min': 7,   'max': 21,   'default': 14},
            {'name': 'atr_mult',        'type': float, 'min': 1.5, 'max': 5.0,  'default': 3.0},
        ]

    def __init__(self):
        super().__init__()
        self._ha = IncrementalHeikinAshi()
        self._last_entry = 0.0

    def _indicators(self):
        try:
            psar = ta.sar(self.candles,
                          acceleration=self.hp['psar_inc'],
                          maximum=self.hp['psar_max'],
                          sequential=False)
        except (AttributeError, TypeError):
            psar = ta.psar(self.candles,
                           acceleration=self.hp['psar_inc'],
                           maximum=self.hp['psar_max'],
                           sequential=False)
        dema = ta.ema(self.candles, period=self.hp['direction_ema_len'], sequential=False)
        rsi  = ta.rsi(self.candles, period=self.hp['rsi_len'], sequential=False)
        return float(psar), float(dema), float(rsi)

    def should_long(self) -> bool:
        psar, dema, rsi = self._indicators()
        _, _, ha_open, ha_close = self._ha.update(self.candles)
        return (psar < self.price and self.price > dema
                and rsi > 50 and ha_close > ha_open)

    def should_short(self) -> bool:
        psar, dema, rsi = self._indicators()
        _, _, ha_open, ha_close = self._ha.update(self.candles)
        return (psar > self.price and self.price < dema
                and rsi < 50 and ha_close < ha_open)

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
        psar, dema, rsi = self._indicators()
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if self.price < psar or self.price < dema or rsi < 50:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return

        if self.is_short:
            if self.price > psar or self.price > dema or rsi > 50:
                self.liquidate(); return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate(); return
