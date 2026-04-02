"""DissimilarityIndex — FreqAI-style data quality guard.

Monitors how far current feature data deviates from the training
distribution. When DI exceeds the threshold, signals "uncertain"
regime to the Orchestrator (increases cash, blocks new entries).
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger(__name__)


class DissimilarityIndex:
    """Detects when live data diverges from training data distribution.

    Stores the mean and standard deviation of each feature from the
    training set. Flags when current data is N standard deviations
    away from the training mean across multiple features.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._threshold = config.get("di_threshold", 2.5)
        self._std_multiplier = config.get("di_std_multiplier", 2.0)
        self._feature_names: list[str] = config.get("features", [])

        # Training distribution statistics
        self._train_means: dict[str, float] = {}
        self._train_stds: dict[str, float] = {}
        self._is_fitted = False

        # Current state
        self._current_di: float = 0.0
        self._feature_deviations: dict[str, float] = {}
        self._uncertain = False

    def fit(self, feature_data: dict[str, list[float]]) -> None:
        """Compute mean and std from training data features.

        Args:
            feature_data: Dict mapping feature name to list of historical values.
        """
        self._train_means = {}
        self._train_stds = {}

        for name, values in feature_data.items():
            if not values:
                continue
            arr = np.array(values, dtype=np.float64)
            self._train_means[name] = float(np.mean(arr))
            self._train_stds[name] = float(np.std(arr, ddof=1))
            # Avoid division by zero
            if self._train_stds[name] < 1e-10:
                self._train_stds[name] = 1e-10

        self._is_fitted = True
        log.info(
            "dissimilarity_index_fitted",
            features=len(self._train_means),
        )

    def update(self, current_features: dict[str, float]) -> float:
        """Calculate DI for current feature vector.

        The DI is the average number of standard deviations each feature
        is away from its training mean.

        Args:
            current_features: Current feature values.

        Returns:
            The dissimilarity index value.
        """
        if not self._is_fitted:
            self._current_di = 0.0
            self._uncertain = False
            return 0.0

        deviations = []
        self._feature_deviations = {}

        for name in self._train_means:
            current_val = current_features.get(name)
            if current_val is None:
                continue

            mean = self._train_means[name]
            std = self._train_stds[name]
            z_score = abs(current_val - mean) / std
            self._feature_deviations[name] = round(z_score, 4)
            deviations.append(z_score)

        if not deviations:
            self._current_di = 0.0
            self._uncertain = False
            return 0.0

        self._current_di = float(np.mean(deviations))
        self._uncertain = self._current_di > self._threshold

        if self._uncertain:
            # Identify which features are outliers
            outlier_features = {
                name: z
                for name, z in self._feature_deviations.items()
                if z > self._std_multiplier
            }
            log.warning(
                "dissimilarity_index_high",
                di=round(self._current_di, 4),
                threshold=self._threshold,
                outlier_features=outlier_features,
            )
        else:
            log.debug(
                "dissimilarity_index_ok",
                di=round(self._current_di, 4),
                threshold=self._threshold,
            )

        return self._current_di

    def is_uncertain(self) -> bool:
        """Return True if DI exceeds threshold (data too different from training)."""
        return self._uncertain

    @property
    def current_di(self) -> float:
        """Current dissimilarity index value."""
        return self._current_di

    @property
    def feature_deviations(self) -> dict[str, float]:
        """Per-feature z-scores from the last update."""
        return self._feature_deviations.copy()

    def to_dict(self) -> dict[str, Any]:
        """Serialize current state for caching/logging."""
        return {
            "di": round(self._current_di, 4),
            "threshold": self._threshold,
            "uncertain": self._uncertain,
            "is_fitted": self._is_fitted,
            "feature_deviations": self._feature_deviations,
        }

    def from_training_dataframe(self, df: Any) -> None:
        """Fit from a pandas DataFrame containing feature columns.

        Args:
            df: DataFrame with columns matching self._feature_names.
        """
        feature_data: dict[str, list[float]] = {}
        for name in self._feature_names:
            if name in df.columns:
                feature_data[name] = df[name].dropna().tolist()
        self.fit(feature_data)
