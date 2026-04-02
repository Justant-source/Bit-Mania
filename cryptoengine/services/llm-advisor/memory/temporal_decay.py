"""Temporal Decay Memory — recency-weighted retrieval.

Applies exponential decay so that recent trade memories receive higher
weight during retrieval, with a configurable half-life.
"""

from __future__ import annotations

import math
import time
from typing import Any

import structlog

from services.llm_advisor.memory.trade_memory import TradeMemory

log = structlog.get_logger(__name__)

_SECONDS_PER_DAY = 86400.0


class TemporalDecayMemory:
    """Retrieval layer that applies exponential time decay to results."""

    def __init__(
        self,
        trade_memory: TradeMemory,
        half_life_days: float = 30.0,
    ) -> None:
        self._memory = trade_memory
        self._half_life_days = half_life_days
        # Precompute decay constant: lambda = ln(2) / half_life
        self._decay_lambda = math.log(2) / (half_life_days * _SECONDS_PER_DAY)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def decay_weight(self, timestamp: float) -> float:
        """Compute the exponential decay weight for a given timestamp.

        Parameters
        ----------
        timestamp:
            Unix epoch timestamp of the trade.

        Returns
        -------
        Weight in (0, 1] where 1.0 means "right now" and values
        decrease with age.
        """
        age_seconds = max(time.time() - timestamp, 0.0)
        return math.exp(-self._decay_lambda * age_seconds)

    def retrieve_with_decay(
        self,
        current_features: dict[str, float],
        top_k: int = 10,
    ) -> dict[str, list[dict[str, Any]]]:
        """Query similar trades and re-rank by recency-weighted score.

        The combined score is ``similarity * decay_weight``.

        Returns the same structure as :meth:`TradeMemory.query_similar`
        but with an added ``"decay_score"`` field per result.
        """
        raw = self._memory.query_similar(current_features, top_k=top_k * 2)

        results: dict[str, list[dict[str, Any]]] = {
            "success": [],
            "failure": [],
        }

        for label in ("success", "failure"):
            entries = raw.get(label, [])
            for entry in entries:
                meta = entry.get("metadata", {})
                ts = meta.get("timestamp", 0.0)
                if isinstance(ts, str):
                    try:
                        ts = float(ts)
                    except ValueError:
                        ts = 0.0

                similarity = entry.get("similarity", 0.0)
                decay = self.decay_weight(ts)
                combined = similarity * decay

                entry["decay_weight"] = round(decay, 6)
                entry["decay_score"] = round(combined, 6)
                results[label].append(entry)

            # Sort by combined score descending and trim to top_k
            results[label].sort(
                key=lambda x: x.get("decay_score", 0.0), reverse=True
            )
            results[label] = results[label][:top_k]

        log.debug(
            "temporal_decay_retrieval",
            successes=len(results["success"]),
            failures=len(results["failure"]),
            half_life_days=self._half_life_days,
        )
        return results
