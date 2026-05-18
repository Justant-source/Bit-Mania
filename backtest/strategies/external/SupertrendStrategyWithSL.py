"""
SupertrendStrategyWithSL — SupertrendStrategy + 마진 기반 명시적 손절 (롱 전용 최적화용)

sl_margin_pct sentinel:
  0.0  = 비활성 (ATR 청산 + EMA 데드크로스만 사용)
  < 0  = 활성 (pnl_pct ≤ sl_margin_pct 이면 즉시 청산)
  예: -15.0 → 레버리지 적용 마진 손실 -15% 도달 시 청산

pnl_pct = (price - entry) / entry × LEVERAGE × 100
"""
from __future__ import annotations

from external.SupertrendStrategy import SupertrendStrategy, LEVERAGE


class SupertrendStrategyWithSL(SupertrendStrategy):

    def hyperparameters(self):
        return super().hyperparameters() + [
            {
                'name': 'sl_margin_pct',
                'type': float,
                'min': -50.0,
                'max': 0.0,
                'default': 0.0,
            },
        ]

    def update_position(self):
        sl = self.hp['sl_margin_pct']
        # sl < 0 sentinel check: 0.0 = disabled
        if sl < 0 and self.position.is_open and self.is_long:
            entry = self.position.entry_price
            pnl_pct = (self.price - entry) / entry * LEVERAGE * 100.0
            if pnl_pct <= sl:
                self._last_liquidation_ts = self.candles[-1][0]
                self.liquidate()
                return
        super().update_position()
