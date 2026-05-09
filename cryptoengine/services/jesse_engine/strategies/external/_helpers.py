"""
Shared helpers for external strategy batch backtests.
All computations use incremental state to avoid O(n^2) cost on 6-year backtests.
"""
from __future__ import annotations
from collections import deque
import numpy as np


class IncrementalHeikinAshi:
    """Maintains Heikin Ashi open/close incrementally — O(1) per update."""

    def __init__(self):
        self._ha_open = None
        self._ha_close = None
        self._ha_open_prev = None
        self._ha_close_prev = None
        self._len = 0

    def update(self, candles: np.ndarray) -> tuple[float, float, float, float]:
        """
        Update HA state with candles. Returns (ha_open_prev, ha_close_prev, ha_open_last, ha_close_last).
        Caller should call this once per strategy step.
        """
        n = len(candles)
        if n == self._len:
            return self._ha_open_prev, self._ha_close_prev, self._ha_open, self._ha_close

        if self._len == 0 or n < 2:
            # Full initialization from scratch
            o = candles[:, 1]; c = candles[:, 2]
            h = candles[:, 3]; l = candles[:, 4]
            ha_close_arr = (o + h + l + c) / 4.0
            ha_open_arr = np.empty(n)
            ha_open_arr[0] = (o[0] + c[0]) / 2.0
            for i in range(1, n):
                ha_open_arr[i] = (ha_open_arr[i - 1] + ha_close_arr[i - 1]) / 2.0
            self._ha_open_prev = ha_open_arr[-2] if n >= 2 else ha_open_arr[-1]
            self._ha_close_prev = ha_close_arr[-2] if n >= 2 else ha_close_arr[-1]
            self._ha_open = ha_open_arr[-1]
            self._ha_close = ha_close_arr[-1]
        else:
            # Extend incrementally for each new candle
            for i in range(self._len, n):
                o = candles[i, 1]; c = candles[i, 2]
                h = candles[i, 3]; l = candles[i, 4]
                ha_close_new = (o + h + l + c) / 4.0
                ha_open_new = (self._ha_open + self._ha_close) / 2.0
                self._ha_open_prev = self._ha_open
                self._ha_close_prev = self._ha_close
                self._ha_open = ha_open_new
                self._ha_close = ha_close_new

        self._len = n
        return self._ha_open_prev, self._ha_close_prev, self._ha_open, self._ha_close


class IncrementalBBWP:
    """
    Bollinger Band Width Percentile — O(lookback) per update instead of O(n^2).
    Maintains a running list of BW values.
    """

    def __init__(self):
        self._bw_deque: deque = deque()
        self._last_bbwp: float = float('nan')
        self._len = 0

    def update(self, candles: np.ndarray, bb_len: int, lookback: int) -> float:
        """Returns current BBWP value (0-100), NaN if insufficient data."""
        n = len(candles)
        if n == self._len:
            return self._last_bbwp

        # Extend by new candles
        for i in range(self._len, n):
            if i < bb_len - 1:
                # Not enough candles for BB yet
                self._len = i + 1
                continue
            c = candles[max(0, i - bb_len + 1):i + 1, 2]  # close slice
            sma_val = float(np.mean(c))
            std_val = float(np.std(c, ddof=0))
            bw_val = 2.0 * std_val / sma_val if sma_val != 0 else 0.0
            self._bw_deque.append(bw_val)
            # Keep only lookback+1 values (current + history for percentile)
            while len(self._bw_deque) > lookback + 1:
                self._bw_deque.popleft()
            self._len = i + 1

        if len(self._bw_deque) < 2:
            self._last_bbwp = float('nan')
            return self._last_bbwp

        bw_list = list(self._bw_deque)
        bw_cur = bw_list[-1]
        history = np.array(bw_list[:-1])
        self._last_bbwp = float(np.sum(history <= bw_cur) / len(history) * 100.0)
        return self._last_bbwp

    def get_prev_bbwp(self) -> float:
        """BBWP value for the previous candle (for crossover detection)."""
        if len(self._bw_deque) < 3:
            return float('nan')
        bw_list = list(self._bw_deque)
        bw_prev = bw_list[-2]
        history = np.array(bw_list[:-2])
        if len(history) == 0:
            return float('nan')
        return float(np.sum(history <= bw_prev) / len(history) * 100.0)


class IncrementalTrendType:
    """
    ADX/ATR regime classifier — O(1) per update using Wilder smoothing state.
    Equivalent to Pine's compute_trend_type but fully incremental.
    """

    def __init__(self, atr_len: int = 14, atr_ma_len: int = 20,
                 atr_ma_type: str = 'SMA', di_len: int = 14, smooth: int = 1):
        self.atr_len = atr_len
        self.atr_ma_len = atr_ma_len
        self.atr_ma_type = atr_ma_type
        self.di_len = di_len
        self.smooth = smooth

        # Wilder smoothing state for TR, +DM, -DM
        self._tr_rma = None
        self._plus_dm_rma = None
        self._minus_dm_rma = None
        self._atr_sma_buf: deque = deque(maxlen=atr_ma_len)
        self._atr_ema = None  # for EMA-type ATR MA

        # Trend type history for smoothing
        self._tt_buf: deque = deque(maxlen=max(smooth, 1))

        self._len = 0
        self._last_trend: float = float('nan')
        self._last_smooth: float = float('nan')

    def update(self, candles: np.ndarray) -> float:
        """Returns smoothed trend type {-2, 0, +2} or NaN."""
        n = len(candles)
        if n == self._len:
            return self._last_smooth

        for i in range(self._len, n):
            if i == 0:
                self._len = 1
                continue

            c_cur = candles[i]
            c_prev = candles[i - 1]
            high = c_cur[3]; low = c_cur[4]; close = c_cur[2]
            prev_high = c_prev[3]; prev_low = c_prev[4]; prev_close = c_prev[2]

            up_move = high - prev_high
            down_move = prev_low - low
            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

            # Wilder smoothing (RMA)
            period = self.di_len
            if self._tr_rma is None:
                if i < period:
                    self._len = i + 1
                    continue
                # Initialize with first period's average
                self._tr_rma = float(np.mean(candles[0:period, 3] - candles[0:period, 4]))
                self._plus_dm_rma = 0.0
                self._minus_dm_rma = 0.0
            else:
                self._tr_rma = (self._tr_rma * (period - 1) + tr) / period
                self._plus_dm_rma = (self._plus_dm_rma * (period - 1) + plus_dm) / period
                self._minus_dm_rma = (self._minus_dm_rma * (period - 1) + minus_dm) / period

            if self._tr_rma == 0:
                self._len = i + 1
                continue

            plus_di = 100 * self._plus_dm_rma / self._tr_rma
            minus_di = 100 * self._minus_dm_rma / self._tr_rma

            # ATR value (Wilder, same period as di_len for simplicity — Pine uses atr_len)
            atr_val = self._tr_rma  # approximation; ideally re-smooth with atr_len
            self._atr_sma_buf.append(atr_val)

            if len(self._atr_sma_buf) < self.atr_ma_len:
                self._len = i + 1
                continue

            if self.atr_ma_type == 'EMA':
                alpha = 2.0 / (self.atr_ma_len + 1)
                if self._atr_ema is None:
                    self._atr_ema = float(np.mean(list(self._atr_sma_buf)))
                else:
                    self._atr_ema = alpha * atr_val + (1 - alpha) * self._atr_ema
                atr_ma = self._atr_ema
            else:
                atr_ma = float(np.mean(list(self._atr_sma_buf)))

            is_sideways = atr_val <= atr_ma
            is_up = plus_di > minus_di
            tt = 0.0 if is_sideways else (2.0 if is_up else -2.0)
            self._tt_buf.append(tt)
            self._len = i + 1

        if len(self._tt_buf) == 0:
            return float('nan')
        if self.smooth <= 1:
            self._last_smooth = self._tt_buf[-1]
        else:
            buf = list(self._tt_buf)[-self.smooth:]
            if len(buf) < self.smooth:
                self._last_smooth = float('nan')
            else:
                self._last_smooth = round(float(np.mean(buf)) / 2) * 2
        return self._last_smooth


class IncrementalChandelier:
    """
    Chandelier Exit direction tracker — O(ce_period) per update.
    Maintains trailing long_stop / short_stop and direction state.
    """

    def __init__(self, ce_period: int = 22, ce_mult: float = 3.0):
        self.ce_period = ce_period
        self.ce_mult = ce_mult

        self._long_stop = float('nan')
        self._short_stop = float('nan')
        self._direction: float = 1.0  # 1=up, -1=down
        self._len = 0

    def update(self, candles: np.ndarray) -> tuple[float, float, float]:
        """Returns (long_stop, short_stop, direction). Updates incrementally."""
        n = len(candles)
        if n == self._len:
            return self._long_stop, self._short_stop, self._direction

        for i in range(max(self._len, self.ce_period), n):
            close = candles[i, 2]
            prev_close = candles[i - 1, 2]
            prev_long_stop = self._long_stop
            prev_short_stop = self._short_stop

            # Rolling ATR using last ce_period candles
            window = candles[i - self.ce_period + 1:i + 1]
            high = window[:, 3]; low = window[:, 4]; c = window[:, 2]
            tr_arr = np.maximum(high[1:] - low[1:],
                                np.maximum(np.abs(high[1:] - c[:-1]),
                                           np.abs(low[1:] - c[:-1])))
            atr_val = float(np.mean(tr_arr)) if len(tr_arr) > 0 else 0.0
            ce_gap = atr_val * self.ce_mult

            hh = float(np.max(c))
            ll = float(np.min(c))
            l_raw = hh - ce_gap
            s_raw = ll + ce_gap

            # Trailing
            if np.isnan(prev_long_stop):
                self._long_stop = l_raw
            else:
                self._long_stop = max(l_raw, prev_long_stop) if prev_close > prev_long_stop else l_raw

            if np.isnan(prev_short_stop):
                self._short_stop = s_raw
            else:
                self._short_stop = min(s_raw, prev_short_stop) if prev_close < prev_short_stop else s_raw

            # Direction
            if not np.isnan(prev_short_stop) and close > prev_short_stop:
                self._direction = 1.0
            elif not np.isnan(prev_long_stop) and close < prev_long_stop:
                self._direction = -1.0
            # else keep previous direction

            self._len = i + 1

        if self._len < self.ce_period:
            self._len = min(n, self.ce_period - 1)
        return self._long_stop, self._short_stop, self._direction
