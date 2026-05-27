"""WeightManager — fixed single-strategy allocation.

Single-strategy model: supertrend always receives 100% of available equity.
Regime-based weight allocation was removed 2026-05-25.
"""

from __future__ import annotations

import copy
from typing import Any

import structlog

from shared.log_events import *

log = structlog.get_logger(__name__)

STRATEGY_KEYS = ("supertrend", "cash")

# Single-strategy model: supertrend is always fully deployed (the strategy
# itself uses 95% × 3x leverage internally).
FIXED_WEIGHTS: dict[str, float] = {"supertrend": 1.0, "cash": 0.0}


class WeightManager:
    """Returns the fixed single-strategy allocation."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        log.info(
            SERVICE_STARTED,
            message="weight manager initialized (fixed single-strategy)",
            weights=FIXED_WEIGHTS,
        )

    def get_target_weights(self) -> dict[str, float]:
        """Return the fixed allocation (always 100% supertrend)."""
        return copy.deepcopy(FIXED_WEIGHTS)
