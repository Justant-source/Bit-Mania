"""
Supertrend + TrendType 1d Filter Strategy — Jesse 2.x port
Hybrid: Supertrend (4h) base with 1d TrendType signals as 7-bit filter mask.

Architecture:
- Inherits from SupertrendStrategy (base logic: ST direction + EMA cross + 200 EMA)
- Computes 1d bars inline from 4h candles (aggregates 6 bars)
- Tracks IncrementalTrendType on 1d data
- Enforces 7-bit filter mask (F0–F6) on long entry if mask != 0
- Filter bits: F0=tt==2, F1=tt!=0, F2=ADX>25, F3=+DI>-DI, F4=close>EMA200,
              F5=ATR>ATR_MA, F6=EMA200_slope>0

Strategy effect: Pure supertrend + EMA (mask=0) → progressively stricter filters as mask increases.
"""
from __future__ import annotations

import numpy as np
from collections import deque
from jesse.strategies import Strategy
import jesse.indicators as ta
from external._helpers import IncrementalTrendType
import os

LEVERAGE = int(os.environ.get('STRATEGY_LEVERAGE', '1'))


class SupertrendTrendType1dFilterStrategy(Strategy):

    def hyperparameters(self):
        return [
            # Supertrend (4h)
            {'name': 'st_factor',         'type': float, 'default': 2.5, 'min': 1.0, 'max': 5.0},
            {'name': 'st_period',         'type': int,   'default': 6,   'min': 3,   'max': 20},
            # EMA (4h)
            {'name': 'fast_ema_len',      'type': int,   'default': 7,   'min': 3,   'max': 20},
            {'name': 'slow_ema_len',      'type': int,   'default': 20,  'min': 10,  'max': 50},
            {'name': 'direction_ema_len', 'type': int,   'default': 200, 'min': 100, 'max': 300},
            # 1d TrendType filter
            {'name': 'atr_len',           'type': int,   'default': 9,   'min': 5,   'max': 30},
            {'name': 'atr_ma_len',        'type': int,   'default': 20,  'min': 10,  'max': 50},
            {'name': 'di_len',            'type': int,   'default': 9,   'min': 5,   'max': 30},
            {'name': 'smooth',            'type': int,   'default': 1,   'min': 1,   'max': 5},
            # Filter mask (0–127)
            {'name': 'filter_mask',       'type': int,   'default': 0,   'min': 0,   'max': 127},
            # Position management
            {'name': 'atr_mult',          'type': float, 'default': 3.0, 'min': 1.5, 'max': 5.0},
        ]

    def __init__(self):
        super().__init__()
        self._last_entry = 0.0

        # 1d bar aggregation state
        self._1d_bar_count = 0  # how many 4h bars since last 1d close
        self._1d_o = None  # 1d open
        self._1d_h = None  # 1d high
        self._1d_l = None  # 1d low
        self._1d_c = None  # 1d close
        self._1d_bars: deque = deque()  # buffer of (o, h, l, c) tuples for TrendType

        # TrendType on 1d
        self._tt_1d = None
        self._tt_1d_computer = None

        # 1d DI state (Wilder RMA for +DI / -DI)
        self._plus_di_1d = 0.0
        self._minus_di_1d = 0.0
        self._tr_rma_1d = None
        self._plus_dm_rma_1d = None
        self._minus_dm_rma_1d = None

        # ADX state (DX history + Wilder RMA)
        self._dx_deque: deque = deque()
        self._adx_1d = 0.0
        self._adx_rma = None

        # ATR and ATR MA on 1d
        self._atr_1d = 0.0
        self._atr_ma_1d = 0.0
        self._atr_deque: deque = deque()

        # EMA200 on 1d
        self._ema200_1d = None
        self._ema200_alpha = 2.0 / (200 + 1)
        self._ema200_deque: deque = deque(maxlen=5)  # last 5 EMA200 values for slope
        self._ema200_slope_1d = 0.0

        self._len_1d_processed = 0  # track how many 1d bars we've computed

    def _process_1d_bar(self, o: float, h: float, l: float, c: float):
        """Process a complete 1d bar (o, h, l, c)."""
        self._1d_o = o
        self._1d_h = h
        self._1d_l = l
        self._1d_c = c

        # Build numpy array of current 1d bars for TrendType
        self._1d_bars.append((o, h, l, c))

        # Convert deque to numpy for TrendType update
        if len(self._1d_bars) > 0:
            arr_1d = np.array(list(self._1d_bars), dtype=np.float64)
            # Map to Jesse candle format: [timestamp, open, close, high, low, ...]
            candles_1d = np.zeros((len(arr_1d), 5), dtype=np.float64)
            for idx, (o_val, h_val, l_val, c_val) in enumerate(list(self._1d_bars)):
                candles_1d[idx] = [0, o_val, c_val, h_val, l_val]

            if self._tt_1d_computer is None:
                self._tt_1d_computer = IncrementalTrendType(
                    atr_len=int(self.hp['atr_len']),
                    atr_ma_len=int(self.hp['atr_ma_len']),
                    di_len=int(self.hp['di_len']),
                    smooth=int(self.hp['smooth']),
                )

            self._tt_1d = self._tt_1d_computer.update(candles_1d)

        # Update DI (Wilder RMA) on 1d
        self._update_di_1d(o, h, l, c)

        # Update ADX from DI
        self._update_adx_1d()

        # Update ATR on 1d
        self._update_atr_1d(h, l, c)

        # Update EMA200 on 1d
        self._update_ema200_1d(c)

        self._len_1d_processed += 1

    def _update_di_1d(self, o: float, h: float, l: float, c: float):
        """Incremental Wilder smoothing for +DM, -DM, TR on 1d."""
        di_len = int(self.hp['di_len'])

        # Get previous bar values (if we have at least 2 bars in history)
        if len(self._1d_bars) < 2:
            return

        prev_h = self._1d_bars[-2][1]  # previous high
        prev_l = self._1d_bars[-2][2]  # previous low
        prev_c = self._1d_bars[-2][3]  # previous close

        up_move = h - prev_h
        down_move = prev_l - l
        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))

        # Wilder RMA initialization
        if self._tr_rma_1d is None:
            if self._len_1d_processed < di_len:
                return
            # Use first period bars to initialize
            tr_init = (h - l)  # simplified, would need buffer
            self._tr_rma_1d = tr_init
            self._plus_dm_rma_1d = plus_dm
            self._minus_dm_rma_1d = minus_dm
        else:
            self._tr_rma_1d = (self._tr_rma_1d * (di_len - 1) + tr) / di_len
            self._plus_dm_rma_1d = (self._plus_dm_rma_1d * (di_len - 1) + plus_dm) / di_len
            self._minus_dm_rma_1d = (self._minus_dm_rma_1d * (di_len - 1) + minus_dm) / di_len

        if self._tr_rma_1d > 0:
            self._plus_di_1d = 100 * self._plus_dm_rma_1d / self._tr_rma_1d
            self._minus_di_1d = 100 * self._minus_dm_rma_1d / self._tr_rma_1d
        else:
            self._plus_di_1d = 0.0
            self._minus_di_1d = 0.0

    def _update_adx_1d(self):
        """Compute ADX from +DI and -DI using Wilder smoothing."""
        di_len = int(self.hp['di_len'])

        if self._plus_di_1d + self._minus_di_1d <= 0:
            return

        di_sum = self._plus_di_1d + self._minus_di_1d
        dx = abs(self._plus_di_1d - self._minus_di_1d) / di_sum * 100.0
        self._dx_deque.append(dx)

        if len(self._dx_deque) > di_len:
            self._dx_deque.popleft()

        # Wilder RMA for ADX
        if self._adx_rma is None:
            if len(self._dx_deque) < di_len:
                return
            self._adx_rma = float(np.mean(list(self._dx_deque)))
        else:
            self._adx_rma = (self._adx_rma * (di_len - 1) + dx) / di_len

        self._adx_1d = self._adx_rma

    def _update_atr_1d(self, h: float, l: float, c: float):
        """Update ATR and ATR MA on 1d."""
        atr_len = int(self.hp['atr_len'])
        atr_ma_len = int(self.hp['atr_ma_len'])

        if len(self._1d_bars) < 2:
            return

        prev_c = self._1d_bars[-2][3]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))

        # Simple RMA for ATR (approximate)
        if self._atr_1d == 0:
            self._atr_1d = tr
        else:
            self._atr_1d = (self._atr_1d * (atr_len - 1) + tr) / atr_len

        self._atr_deque.append(self._atr_1d)
        if len(self._atr_deque) > atr_ma_len:
            self._atr_deque.popleft()

        if len(self._atr_deque) >= atr_ma_len:
            self._atr_ma_1d = float(np.mean(list(self._atr_deque)))

    def _update_ema200_1d(self, c: float):
        """Update EMA200 on 1d and track slope."""
        if self._ema200_1d is None:
            self._ema200_1d = c
        else:
            self._ema200_1d = self._ema200_alpha * c + (1 - self._ema200_alpha) * self._ema200_1d

        self._ema200_deque.append(self._ema200_1d)

        # Compute slope (last - first) / 4
        if len(self._ema200_deque) == 5:
            first = self._ema200_deque[0]
            last = self._ema200_deque[-1]
            self._ema200_slope_1d = (last - first) / 4.0
        else:
            self._ema200_slope_1d = 0.0

    def _filter_pass(self, mask: int) -> bool:
        """
        Check 7-bit filter mask (0–127).
        Returns True if (during warmup) or (all enforced filter bits pass).

        Bit values (when ON, enforce condition):
        - F0 (bit 0, value 1):   tt_1d == 2.0 (strong uptrend)
        - F1 (bit 1, value 2):   tt_1d != 0.0 (not sideways)
        - F2 (bit 2, value 4):   adx_1d > 25
        - F3 (bit 3, value 8):   plus_di_1d > minus_di_1d
        - F4 (bit 4, value 16):  close_1d > ema200_1d
        - F5 (bit 5, value 32):  atr_1d > atr_ma_1d (volatility expanding)
        - F6 (bit 6, value 64):  ema200_slope_1d > 0 (EMA200 slope positive)
        """
        # Warmup period: no filters
        if self._len_1d_processed < 60:
            return True

        # No mask set: bypass all filters
        if mask == 0:
            return True

        # Check each bit
        if (mask & 1) and not (np.isnan(self._tt_1d) or self._tt_1d == 2.0):
            # F0: tt_1d == 2.0
            return False

        if (mask & 2) and (np.isnan(self._tt_1d) or self._tt_1d == 0.0):
            # F1: tt_1d != 0.0
            return False

        if (mask & 4) and self._adx_1d <= 25:
            # F2: adx_1d > 25
            return False

        if (mask & 8) and not (self._plus_di_1d > self._minus_di_1d):
            # F3: plus_di_1d > minus_di_1d
            return False

        if (mask & 16) and not (self._1d_c > self._ema200_1d):
            # F4: close_1d > ema200_1d
            return False

        if (mask & 32) and not (self._atr_1d > self._atr_ma_1d):
            # F5: atr_1d > atr_ma_1d
            return False

        if (mask & 64) and not (self._ema200_slope_1d > 0):
            # F6: ema200_slope_1d > 0
            return False

        return True

    def before(self):
        """
        Jesse hook called before every bar decision (every 4h bar).
        Aggregate 1d bars from 4h candles: every 6th bar = 1 complete 1d bar.
        """
        if self.index < 1:
            self._1d_bar_count = 0
            return

        # Track position in 1d aggregation (0–5, then reset)
        self._1d_bar_count = (self._1d_bar_count + 1) % 6

        # Every 6th 4h bar completes a 1d bar
        if self._1d_bar_count == 5:  # 0-indexed: bar 5 is the 6th bar
            # Aggregate previous 6 bars (from index-5 to current)
            start_idx = max(0, self.index - 5)
            bars_window = self.candles[start_idx:self.index + 1]

            if len(bars_window) == 6:
                o_1d = float(bars_window[0, 1])  # first open
                h_1d = float(np.max(bars_window[:, 3]))  # max high
                l_1d = float(np.min(bars_window[:, 4]))  # min low
                c_1d = float(bars_window[-1, 2])  # last close

                self._process_1d_bar(o_1d, h_1d, l_1d, c_1d)

    @property
    def st_direction(self) -> int:
        """Version-tolerant supertrend direction on 4h: +1 uptrend, -1 downtrend."""
        st = ta.supertrend(self.candles,
                           period=int(self.hp['st_period']),
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
    def _st_is_uptrend(self) -> bool:
        return self.st_direction == 1

    def should_long(self) -> bool:
        """
        Base: Supertrend uptrend + EMA fast>slow + price>EMA200 (mirrors SupertrendStrategy.should_long).
        Filter: Apply 1d filter mask if set.
        """
        fast = ta.ema(self.candles, period=int(self.hp['fast_ema_len']),      sequential=False)
        slow = ta.ema(self.candles, period=int(self.hp['slow_ema_len']),      sequential=False)
        dema = ta.ema(self.candles, period=int(self.hp['direction_ema_len']), sequential=False)
        if not (self._st_is_uptrend and fast > slow and self.price > dema):
            return False
        return self._filter_pass(int(self.hp['filter_mask']))

    def should_short(self) -> bool:
        """Short disabled for this strategy (long-only via filter)."""
        return False

    def go_long(self):
        qty = self.balance * 0.95 * LEVERAGE / self.price
        self.buy = qty, self.price
        self._last_entry = self.price

    def go_short(self):
        pass  # Disabled

    def should_cancel_entry(self) -> bool:
        return False

    def update_position(self):
        """Exit on EMA cross or ATR stop."""
        fast = ta.ema(self.candles, period=int(self.hp['fast_ema_len']), sequential=False)
        slow = ta.ema(self.candles, period=int(self.hp['slow_ema_len']), sequential=False)
        atr_stop = ta.atr(self.candles, period=14, sequential=False) * self.hp['atr_mult']

        if self.is_long:
            if fast < slow:
                self.liquidate()
                return
            if (self.price <= self._last_entry - atr_stop
                    or self.price >= self._last_entry + atr_stop):
                self.liquidate()
                return
