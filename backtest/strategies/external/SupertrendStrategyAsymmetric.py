"""
SupertrendStrategyAsymmetric — SupertrendStrategy + 비대칭 TP/SL (롱 전용 v6 sweep)

tp_atr_mult: take-profit 거리 = ATR(14) × tp_atr_mult (진입가 위)
sl_atr_mult: stop-loss 거리  = ATR(14) × sl_atr_mult (진입가 아래)

v5 SupertrendStrategyWithSL의 symmetric atr_mult를 TP/SL로 분리.
EMA 데드크로스 청산 유지 (v5 동일 조건).
"""
from __future__ import annotations

from external.SupertrendStrategy import SupertrendStrategy
import jesse.indicators as ta


class SupertrendStrategyAsymmetric(SupertrendStrategy):

    def hyperparameters(self):
        return super().hyperparameters() + [
            {
                'name': 'tp_atr_mult',
                'type': float,
                'min': 1.5,
                'max': 6.0,
                'default': 3.0,
            },
            {
                'name': 'sl_atr_mult',
                'type': float,
                'min': 0.5,
                'max': 5.0,
                'default': 2.0,
            },
        ]

    def update_position(self):
        fast = ta.ema(self.candles, period=self.hp['fast_ema_len'], sequential=False)
        slow = ta.ema(self.candles, period=self.hp['slow_ema_len'], sequential=False)
        atr  = ta.atr(self.candles, period=14, sequential=False)

        if self.is_long:
            if fast < slow:
                self.liquidate()
                return
            if self.price <= self._last_entry - atr * self.hp['sl_atr_mult']:
                self.liquidate()
                return
            if self.price >= self._last_entry + atr * self.hp['tp_atr_mult']:
                self.liquidate()
                return
