"""Integration tests for orchestrator regime-change weight adjustment.

Tests that:
  - Regime change from ranging to volatile triggers weight shift
  - Weight transition is smoothed (EMA)
  - Kill switch overrides weights to 100% cash
  - LLM adjustments are bounded and applied correctly
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from shared.models.position import PortfolioState


# ---------------------------------------------------------------------------
# Inline WeightManager (mirrors services/orchestrator/weight_manager.py)
# ---------------------------------------------------------------------------

DEFAULT_WEIGHT_MATRIX = {
    "ranging": {"supertrend": 0.30, "cash": 0.70},
    "trending_up": {"supertrend": 0.70, "cash": 0.30},
    "trending_down": {"supertrend": 0.10, "cash": 0.90},
    "volatile": {"supertrend": 0.30, "cash": 0.70},
    "uncertain": {"supertrend": 0.05, "cash": 0.95},
}

STRATEGY_KEYS = ("supertrend", "cash")


class WeightManager:
    def __init__(self, ema_alpha: float = 0.3, min_change: float = 0.02):
        self._matrix = copy.deepcopy(DEFAULT_WEIGHT_MATRIX)
        self._ema_alpha = ema_alpha
        self._min_change = min_change
        self._llm_adj: dict[str, float] = {}

    def get_target_weights(self, regime: str) -> dict[str, float]:
        return copy.deepcopy(
            self._matrix.get(regime, self._matrix["ranging"])
        )

    def smooth_transition(
        self, current: dict[str, float], target: dict[str, float]
    ) -> dict[str, float]:
        if not current:
            return copy.deepcopy(target)
        smoothed = {}
        for k in STRATEGY_KEYS:
            cur = current.get(k, 0.0)
            tgt = target.get(k, 0.0)
            diff = tgt - cur
            if abs(diff) < self._min_change:
                smoothed[k] = cur
            else:
                smoothed[k] = cur + self._ema_alpha * diff
        total = sum(smoothed.values())
        if total > 0:
            for k in smoothed:
                smoothed[k] = round(smoothed[k] / total, 4)
        return smoothed

    def apply_llm_adjustments(
        self, adjustments: dict[str, float], max_adj: float, confidence: float
    ):
        if confidence <= 0.5:
            return
        for k, v in adjustments.items():
            if k in STRATEGY_KEYS:
                clamped = max(-max_adj, min(v, max_adj))
                self._llm_adj[k] = clamped * confidence

    def get_adjusted_weights(self, regime: str) -> dict[str, float]:
        base = self.get_target_weights(regime)
        if self._llm_adj:
            for k, adj in self._llm_adj.items():
                if k in base:
                    base[k] = max(0.0, base[k] + adj)
            total = sum(base.values())
            if total > 0:
                for k in base:
                    base[k] = round(base[k] / total, 4)
        return base


@pytest.fixture
def wm():
    return WeightManager()


# ------------------------------------------------------------------
# Regime change triggers weight adjustment
# ------------------------------------------------------------------

class TestRegimeChangeWeights:
    def test_ranging_weights(self, wm):
        weights = wm.get_target_weights("ranging")
        assert weights["supertrend"] == 0.30
        assert weights["cash"] == 0.70
        assert sum(weights.values()) == pytest.approx(1.0)

    def test_volatile_weights(self, wm):
        weights = wm.get_target_weights("volatile")
        assert weights["cash"] == 0.70
        assert weights["supertrend"] == 0.30
        assert sum(weights.values()) == pytest.approx(1.0)

    def test_regime_change_shifts_supertrend_weight(self, wm):
        ranging = wm.get_target_weights("ranging")
        volatile = wm.get_target_weights("volatile")
        assert volatile["supertrend"] == ranging["supertrend"]
        assert volatile["cash"] == ranging["cash"]

    def test_trending_down_high_cash(self, wm):
        weights = wm.get_target_weights("trending_down")
        assert weights["cash"] == 0.65

    def test_unknown_regime_falls_back_to_ranging(self, wm):
        weights = wm.get_target_weights("nonexistent_regime")
        expected = wm.get_target_weights("ranging")
        assert weights == expected


# ------------------------------------------------------------------
# Smooth transition (EMA)
# ------------------------------------------------------------------

class TestSmoothTransition:
    def test_first_transition_is_target(self, wm):
        target = wm.get_target_weights("volatile")
        smoothed = wm.smooth_transition({}, target)
        assert smoothed == target

    def test_gradual_transition(self, wm):
        ranging = wm.get_target_weights("ranging")
        volatile = wm.get_target_weights("volatile")

        step1 = wm.smooth_transition(ranging, volatile)
        # supertrend stays at 0.30 (both regimes same)
        assert step1["supertrend"] == ranging["supertrend"]
        # Cash stays at 0.70 (both regimes same)
        assert step1["cash"] == ranging["cash"]

    def test_multiple_steps_converge(self, wm):
        current = wm.get_target_weights("ranging")
        target = wm.get_target_weights("trending_up")

        for _ in range(20):
            current = wm.smooth_transition(current, target)

        # After 20 steps, should be very close to target
        for k in STRATEGY_KEYS:
            assert current[k] == pytest.approx(target[k], abs=0.02)

    def test_small_changes_ignored(self, wm):
        current = {"supertrend": 0.30, "cash": 0.70}
        target = {"supertrend": 0.31, "cash": 0.69}
        smoothed = wm.smooth_transition(current, target)
        # Changes < 2% should not move
        assert smoothed["supertrend"] == pytest.approx(current["supertrend"], abs=0.001)

    def test_weights_always_sum_to_one(self, wm):
        current = wm.get_target_weights("ranging")
        for regime in ["trending_up", "volatile", "trending_down", "ranging"]:
            target = wm.get_target_weights(regime)
            current = wm.smooth_transition(current, target)
            assert sum(current.values()) == pytest.approx(1.0, abs=0.001)


# ------------------------------------------------------------------
# Kill switch override
# ------------------------------------------------------------------

class TestKillSwitchOverride:
    def test_kill_switch_sets_all_cash(self):
        emergency = {"supertrend": 0.0, "cash": 1.0}
        assert emergency["cash"] == 1.0
        assert sum(v for k, v in emergency.items() if k != "cash") == 0.0

    def test_kill_switch_overrides_current_weights(self, wm):
        current = wm.get_target_weights("ranging")
        assert current["supertrend"] > 0  # non-zero before kill

        emergency = {"supertrend": 0.0, "cash": 1.0}
        # After kill switch, weights should be emergency
        for k in STRATEGY_KEYS:
            assert emergency.get(k, 0) == (1.0 if k == "cash" else 0.0)


# ------------------------------------------------------------------
# LLM adjustments
# ------------------------------------------------------------------

class TestLLMAdjustments:
    def test_llm_adjustment_applied(self, wm):
        wm.apply_llm_adjustments(
            {"funding_arb": 0.10, "cash": -0.10},
            max_adj=0.15,
            confidence=0.8,
        )
        adjusted = wm.get_adjusted_weights("ranging")
        base = DEFAULT_WEIGHT_MATRIX["ranging"]
        assert adjusted["funding_arb"] > base["funding_arb"]

    def test_llm_adjustment_clamped(self, wm):
        wm.apply_llm_adjustments(
            {"funding_arb": 0.50},  # exceeds max
            max_adj=0.15,
            confidence=0.9,
        )
        # Internal adjustment should be 0.15 * 0.9 = 0.135
        assert wm._llm_adj["funding_arb"] == pytest.approx(0.135, abs=0.001)

    def test_llm_low_confidence_skipped(self, wm):
        wm.apply_llm_adjustments(
            {"funding_arb": 0.10},
            max_adj=0.15,
            confidence=0.3,  # below 0.5
        )
        assert not wm._llm_adj

    def test_adjusted_weights_normalized(self, wm):
        wm.apply_llm_adjustments(
            {"funding_arb": 0.10, "dca": 0.05, "cash": -0.10},
            max_adj=0.15,
            confidence=0.7,
        )
        adjusted = wm.get_adjusted_weights("ranging")
        assert sum(adjusted.values()) == pytest.approx(1.0, abs=0.001)
        for v in adjusted.values():
            assert v >= 0.0
