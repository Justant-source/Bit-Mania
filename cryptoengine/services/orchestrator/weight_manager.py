"""WeightManager — regime-based strategy weight allocation.

Loads the weight matrix from config, applies regime-based weights,
smooths transitions, and integrates LLM advisor adjustments.
"""

from __future__ import annotations

import copy
from typing import Any, Literal

import structlog

log = structlog.get_logger(__name__)

RegimeType = Literal["trending_up", "trending_down", "ranging", "volatile", "uncertain"]

# Default weight matrix used when config is missing or incomplete
DEFAULT_WEIGHT_MATRIX: dict[str, dict[str, float]] = {
    "ranging": {"funding_arb": 0.65, "dca": 0.15, "cash": 0.20},
    "trending_up": {"funding_arb": 0.20, "dca": 0.50, "cash": 0.30},
    "trending_down": {"funding_arb": 0.25, "dca": 0.10, "cash": 0.65},
    "volatile": {"funding_arb": 0.15, "dca": 0.05, "cash": 0.80},
    "uncertain": {"funding_arb": 0.05, "dca": 0.05, "cash": 0.90},
}

STRATEGY_KEYS = ("funding_arb", "dca", "cash")

# Mapping from config YAML keys to internal strategy keys
_CONFIG_KEY_MAP: dict[str, str] = {
    "funding_arb": "funding_arb",
    "adaptive_dca": "dca",
    "dca": "dca",
    "cash_reserve": "cash",
    "cash": "cash",
}


class WeightManager:
    """Manages regime-based strategy weight allocation with smooth transitions."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._weight_matrix = self._load_weight_matrix()
        self._llm_adjustments: dict[str, float] = {}

        # Smoothing parameters
        transition_cfg = config.get("transition", {})
        smoothing_cfg = config.get("smoothing", {})
        self._ema_alpha = transition_cfg.get(
            "ema_alpha", smoothing_cfg.get("ema_alpha", 0.3)
        )
        self._min_weight_change = transition_cfg.get(
            "min_weight_change", smoothing_cfg.get("min_weight_change", 0.02)
        )

        log.info(
            "weight_manager_initialized",
            regimes=list(self._weight_matrix.keys()),
            ema_alpha=self._ema_alpha,
        )

    def _load_weight_matrix(self) -> dict[str, dict[str, float]]:
        """Load weight matrix from config, mapping config keys to internal keys."""
        raw = self._config.get("weights", self._config.get("weight_matrix", {}))
        if not raw:
            log.warning("weight_matrix_not_in_config_using_defaults")
            return copy.deepcopy(DEFAULT_WEIGHT_MATRIX)

        matrix: dict[str, dict[str, float]] = {}
        for regime, strategy_weights in raw.items():
            normalized: dict[str, float] = {}
            for config_key, weight in strategy_weights.items():
                internal_key = _CONFIG_KEY_MAP.get(config_key, config_key)
                if internal_key in STRATEGY_KEYS:
                    normalized[internal_key] = float(weight)

            # Ensure all keys present
            for k in STRATEGY_KEYS:
                normalized.setdefault(k, 0.0)

            # Validate sum
            total = sum(normalized.values())
            if abs(total - 1.0) > 0.01:
                log.warning(
                    "weight_matrix_row_not_normalized",
                    regime=regime,
                    total=total,
                )
                if total > 0:
                    for k in normalized:
                        normalized[k] /= total

            matrix[regime] = normalized

        # Ensure uncertain regime exists
        if "uncertain" not in matrix:
            matrix["uncertain"] = copy.deepcopy(DEFAULT_WEIGHT_MATRIX["uncertain"])

        return matrix

    def get_target_weights(self, regime: RegimeType) -> dict[str, float]:
        """Return target weights for the given market regime."""
        weights = self._weight_matrix.get(regime)
        if weights is None:
            log.warning("unknown_regime_fallback", regime=regime)
            weights = self._weight_matrix.get("ranging", DEFAULT_WEIGHT_MATRIX["ranging"])
        return copy.deepcopy(weights)

    def smooth_transition(
        self,
        current: dict[str, float],
        target: dict[str, float],
    ) -> dict[str, float]:
        """Exponential moving average smoothing for weight transitions.

        Prevents sudden weight jumps by gradually moving toward the target.
        Returns the smoothed weights (always sum to 1.0).
        """
        if not current:
            return copy.deepcopy(target)

        alpha = self._ema_alpha
        smoothed: dict[str, float] = {}

        for key in STRATEGY_KEYS:
            cur = current.get(key, 0.0)
            tgt = target.get(key, 0.0)
            diff = tgt - cur

            # Skip tiny changes
            if abs(diff) < self._min_weight_change:
                smoothed[key] = cur
            else:
                smoothed[key] = cur + alpha * diff

        # Re-normalize to ensure weights sum to 1.0
        total = sum(smoothed.values())
        if total > 0:
            for key in smoothed:
                smoothed[key] = round(smoothed[key] / total, 4)

        return smoothed

    def apply_llm_adjustments(
        self,
        adjustments: dict[str, float],
        max_adjustment: float,
        confidence: float,
    ) -> None:
        """Apply LLM advisor weight adjustments.

        Only applied when confidence > 0.5. Adjustments are clamped to
        ``max_adjustment`` and scaled by confidence.

        Args:
            adjustments: Strategy -> adjustment delta (can be negative).
            max_adjustment: Maximum absolute adjustment per strategy.
            confidence: LLM confidence score (0.0 - 1.0).
        """
        if confidence <= 0.5:
            log.info("llm_adjustment_skipped_low_confidence", confidence=confidence)
            return

        scaled: dict[str, float] = {}
        for key, adj in adjustments.items():
            internal_key = _CONFIG_KEY_MAP.get(key, key)
            if internal_key not in STRATEGY_KEYS:
                continue
            clamped = max(-max_adjustment, min(adj, max_adjustment))
            scaled[internal_key] = clamped * confidence

        self._llm_adjustments = scaled
        log.info("llm_adjustments_stored", adjustments=scaled)

    def get_adjusted_weights(self, regime: RegimeType) -> dict[str, float]:
        """Get regime weights with LLM adjustments applied.

        After applying adjustments, weights are re-normalized and any
        negative weights are floored to zero.
        """
        base = self.get_target_weights(regime)

        if self._llm_adjustments:
            for key, adj in self._llm_adjustments.items():
                if key in base:
                    base[key] = max(0.0, base[key] + adj)

            # Re-normalize
            total = sum(base.values())
            if total > 0:
                for key in base:
                    base[key] = round(base[key] / total, 4)

        return base

    def reset_llm_adjustments(self) -> None:
        """Clear any stored LLM adjustments."""
        self._llm_adjustments = {}

    @property
    def weight_matrix(self) -> dict[str, dict[str, float]]:
        """Return a copy of the current weight matrix."""
        return copy.deepcopy(self._weight_matrix)
