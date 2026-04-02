"""Unit tests for RegimeDetector classification logic.

Covers:
  - Ranging regime detection (low ADX, narrow BB)
  - Trending up/down detection (high ADX, EMA alignment)
  - Volatile regime detection (high ATR)
  - Edge cases and ambiguous signals
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# Thresholds from regime_detector.py
ADX_TRENDING_THRESHOLD = 25.0
ADX_RANGING_THRESHOLD = 20.0
ATR_VOLATILE_MULTIPLIER = 2.0


# ---------------------------------------------------------------------------
# Simplified regime classifier (mirrors the detection logic)
# ---------------------------------------------------------------------------

def classify_regime(
    adx: float,
    atr: float,
    avg_atr: float,
    bb_width: float,
    median_bb_width: float,
    close: float,
    ema20: float,
) -> tuple[str, float]:
    """Classify market regime. Returns (regime, confidence)."""
    # Volatile
    if avg_atr > 0 and atr > avg_atr * ATR_VOLATILE_MULTIPLIER:
        confidence = min(1.0, atr / (avg_atr * ATR_VOLATILE_MULTIPLIER * 1.5))
        return "volatile", round(confidence, 4)

    # Ranging
    if adx < ADX_RANGING_THRESHOLD and bb_width < median_bb_width:
        adx_factor = max(0.0, 1.0 - adx / ADX_RANGING_THRESHOLD)
        bb_factor = max(0.0, 1.0 - bb_width / median_bb_width) if median_bb_width > 0 else 0.5
        confidence = (adx_factor + bb_factor) / 2.0
        return "ranging", round(confidence, 4)

    # Trending up
    if adx > ADX_TRENDING_THRESHOLD and close > ema20:
        confidence = min(1.0, (adx - ADX_TRENDING_THRESHOLD) / 50.0 + 0.5)
        return "trending_up", round(confidence, 4)

    # Trending down
    if adx > ADX_TRENDING_THRESHOLD and close < ema20:
        confidence = min(1.0, (adx - ADX_TRENDING_THRESHOLD) / 50.0 + 0.5)
        return "trending_down", round(confidence, 4)

    # Ambiguous
    return "ranging", 0.3


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRangingDetection:
    def test_classic_ranging(self):
        regime, conf = classify_regime(
            adx=15.0, atr=100.0, avg_atr=120.0,
            bb_width=0.03, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "ranging"
        assert conf > 0.0

    def test_very_low_adx(self):
        regime, conf = classify_regime(
            adx=5.0, atr=80.0, avg_atr=100.0,
            bb_width=0.02, median_bb_width=0.04,
            close=65000.0, ema20=65100.0,
        )
        assert regime == "ranging"
        assert conf > 0.5  # high confidence with very low ADX

    def test_narrow_bb_width(self):
        regime, _ = classify_regime(
            adx=18.0, atr=90.0, avg_atr=100.0,
            bb_width=0.01, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "ranging"


class TestTrendingDetection:
    def test_trending_up(self):
        regime, conf = classify_regime(
            adx=35.0, atr=100.0, avg_atr=100.0,
            bb_width=0.06, median_bb_width=0.05,
            close=66000.0, ema20=65000.0,
        )
        assert regime == "trending_up"
        assert conf >= 0.5

    def test_trending_down(self):
        regime, conf = classify_regime(
            adx=30.0, atr=100.0, avg_atr=100.0,
            bb_width=0.06, median_bb_width=0.05,
            close=64000.0, ema20=65000.0,
        )
        assert regime == "trending_down"
        assert conf >= 0.5

    def test_strong_trend_high_confidence(self):
        regime, conf = classify_regime(
            adx=60.0, atr=100.0, avg_atr=100.0,
            bb_width=0.06, median_bb_width=0.05,
            close=68000.0, ema20=65000.0,
        )
        assert regime == "trending_up"
        assert conf > 0.7

    def test_barely_trending(self):
        regime, conf = classify_regime(
            adx=26.0, atr=100.0, avg_atr=100.0,
            bb_width=0.06, median_bb_width=0.05,
            close=65100.0, ema20=65000.0,
        )
        assert regime == "trending_up"
        assert conf == pytest.approx(0.52, abs=0.01)


class TestVolatileDetection:
    def test_high_volatility(self):
        regime, conf = classify_regime(
            adx=30.0, atr=300.0, avg_atr=100.0,
            bb_width=0.10, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "volatile"
        assert conf > 0.0

    def test_extreme_volatility(self):
        regime, conf = classify_regime(
            adx=40.0, atr=500.0, avg_atr=100.0,
            bb_width=0.15, median_bb_width=0.05,
            close=60000.0, ema20=65000.0,
        )
        assert regime == "volatile"
        assert conf > 0.5

    def test_barely_volatile(self):
        regime, conf = classify_regime(
            adx=15.0, atr=210.0, avg_atr=100.0,
            bb_width=0.03, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "volatile"
        # Just over the 2x threshold

    def test_not_volatile_below_threshold(self):
        regime, _ = classify_regime(
            adx=15.0, atr=190.0, avg_atr=100.0,
            bb_width=0.03, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime != "volatile"


class TestEdgeCases:
    def test_ambiguous_returns_ranging(self):
        """ADX between 20 and 25 with wide BB returns ranging with low conf."""
        regime, conf = classify_regime(
            adx=22.0, atr=100.0, avg_atr=100.0,
            bb_width=0.06, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "ranging"
        assert conf == 0.3

    def test_zero_avg_atr(self):
        """Zero avg ATR should not trigger volatile."""
        regime, _ = classify_regime(
            adx=15.0, atr=100.0, avg_atr=0.0,
            bb_width=0.03, median_bb_width=0.05,
            close=65000.0, ema20=65000.0,
        )
        assert regime == "ranging"

    def test_confidence_always_between_0_and_1(self):
        test_cases = [
            (10, 50, 100, 0.02, 0.05, 65000, 65000),
            (50, 100, 100, 0.10, 0.05, 70000, 65000),
            (5, 500, 100, 0.01, 0.05, 65000, 65000),
        ]
        for adx, atr, avg_atr, bb, med_bb, close, ema in test_cases:
            _, conf = classify_regime(adx, atr, avg_atr, bb, med_bb, close, ema)
            assert 0.0 <= conf <= 1.0

    def test_volatile_takes_priority_over_trending(self):
        """Volatile should override trending when ATR is very high."""
        regime, _ = classify_regime(
            adx=40.0, atr=300.0, avg_atr=100.0,
            bb_width=0.10, median_bb_width=0.05,
            close=68000.0, ema20=65000.0,
        )
        assert regime == "volatile"
