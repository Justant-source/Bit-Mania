"""RegimeMLModel — FreqAI-style LightGBM classifier for market regime detection.

Runs a background thread that retrains the model every 6 hours using the
last 30 days of feature data. Hot-swaps the model on retrain completion.
Falls back to rule-based regime detection if ML fails.
"""

from __future__ import annotations

import asyncio
import json
import pickle
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import lightgbm as lgb
import numpy as np
import pandas as pd
import redis.asyncio as aioredis
import structlog
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder

log = structlog.get_logger(__name__)

RegimeType = Literal["trending_up", "trending_down", "ranging", "volatile", "uncertain"]

REGIME_LABELS = ["ranging", "trending_up", "trending_down", "volatile"]
REDIS_KEY_MODEL = "ml:regime_model"
REDIS_KEY_FEATURES = "features:latest"
REDIS_KEY_FEATURE_HISTORY = "features:history"


class RegimeMLModel:
    """LightGBM-based market regime classifier with periodic retraining."""

    def __init__(self, redis: aioredis.Redis, config: dict[str, Any]) -> None:
        self._redis = redis
        self._config = config
        self._retrain_interval_hours = config.get("retrain_interval_hours", 6)
        self._lookback_days = config.get("training_lookback_days", 30)
        self._min_samples = config.get("min_training_samples", 500)
        self._feature_names: list[str] = config.get(
            "features",
            [
                "adx_14",
                "rsi_14",
                "bb_width_20",
                "atr_14",
                "volume_sma_ratio",
                "close_sma_ratio_20",
                "close_sma_ratio_50",
                "macd_histogram",
                "obv_slope",
                "funding_rate",
            ],
        )

        self._model: lgb.Booster | None = None
        self._label_encoder = LabelEncoder()
        self._label_encoder.fit(REGIME_LABELS)
        self._model_lock = threading.Lock()
        self._running = False
        self._retrain_thread: threading.Thread | None = None
        self._last_train_time: datetime | None = None
        self._model_accuracy: float = 0.0

    async def start(self) -> None:
        """Load cached model and start background retrain thread."""
        await self._load_cached_model()
        self._running = True
        self._retrain_thread = threading.Thread(
            target=self._retrain_loop, daemon=True, name="regime-retrain"
        )
        self._retrain_thread.start()
        log.info(
            "regime_ml_model_started",
            retrain_interval_hours=self._retrain_interval_hours,
            features=len(self._feature_names),
        )

    async def stop(self) -> None:
        """Stop the retrain thread."""
        self._running = False
        if self._retrain_thread and self._retrain_thread.is_alive():
            self._retrain_thread.join(timeout=10)
        log.info("regime_ml_model_stopped")

    async def predict(self) -> tuple[RegimeType, float]:
        """Predict current market regime from latest features.

        Returns:
            Tuple of (regime, confidence).
        """
        features = await self._get_latest_features()
        if features is None:
            log.warning("no_features_available_for_prediction")
            return await self._rule_based_fallback()

        with self._model_lock:
            if self._model is None:
                log.info("no_ml_model_available_using_fallback")
                return await self._rule_based_fallback()

            try:
                feature_array = np.array(
                    [features.get(f, 0.0) for f in self._feature_names]
                ).reshape(1, -1)
                probabilities = self._model.predict(feature_array)[0]
                predicted_idx = int(np.argmax(probabilities))
                confidence = float(probabilities[predicted_idx])
                regime = REGIME_LABELS[predicted_idx]
                log.info(
                    "ml_regime_predicted",
                    regime=regime,
                    confidence=confidence,
                    probabilities=dict(zip(REGIME_LABELS, probabilities.tolist())),
                )
                return regime, confidence  # type: ignore[return-value]
            except Exception:
                log.exception("ml_prediction_failed")
                return await self._rule_based_fallback()

    async def _rule_based_fallback(self) -> tuple[RegimeType, float]:
        """Rule-based regime detection fallback.

        Uses ADX, BB width, and price vs SMA to classify the regime.
        """
        features = await self._get_latest_features()
        if not features:
            return "ranging", 0.3

        adx = features.get("adx_14", 20.0)
        bb_width = features.get("bb_width_20", 0.05)
        close_sma_20 = features.get("close_sma_ratio_20", 1.0)
        close_sma_50 = features.get("close_sma_ratio_50", 1.0)

        # High volatility
        if bb_width > 0.08:
            return "volatile", 0.6

        # Strong trend
        if adx > 25:
            if close_sma_20 > 1.01 and close_sma_50 > 1.005:
                return "trending_up", 0.55
            elif close_sma_20 < 0.99 and close_sma_50 < 0.995:
                return "trending_down", 0.55
            # ADX high but no clear direction
            return "volatile", 0.45

        # Low ADX, narrow BB
        if bb_width < 0.03:
            return "ranging", 0.6

        return "ranging", 0.4

    async def _get_latest_features(self) -> dict[str, float] | None:
        """Read latest feature vector from Redis."""
        raw = await self._redis.get(REDIS_KEY_FEATURES)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("invalid_feature_data")
            return None

    def _retrain_loop(self) -> None:
        """Background thread: retrain model every N hours."""
        while self._running:
            try:
                self._retrain_model()
            except Exception:
                log.exception("retrain_failed")

            # Sleep until next retrain window
            sleep_seconds = self._retrain_interval_hours * 3600
            elapsed = 0.0
            while self._running and elapsed < sleep_seconds:
                time.sleep(min(30, sleep_seconds - elapsed))
                elapsed += 30

    def _retrain_model(self) -> None:
        """Train a new LightGBM model on historical features."""
        log.info("regime_model_retrain_start")

        # Load training data synchronously via a new event loop
        loop = asyncio.new_event_loop()
        try:
            df = loop.run_until_complete(self._load_training_data())
        finally:
            loop.close()

        if df is None or len(df) < self._min_samples:
            log.warning(
                "insufficient_training_data",
                samples=len(df) if df is not None else 0,
                min_required=self._min_samples,
            )
            return

        # Prepare features and labels
        feature_cols = [c for c in self._feature_names if c in df.columns]
        if not feature_cols:
            log.warning("no_matching_feature_columns", available=list(df.columns))
            return

        X = df[feature_cols].values
        y = self._label_encoder.transform(df["regime"].values)

        # Time series cross-validation
        tscv = TimeSeriesSplit(n_splits=3)
        accuracies = []

        best_model: lgb.Booster | None = None
        best_accuracy = 0.0

        for train_idx, val_idx in tscv.split(X):
            X_train, X_val = X[train_idx], X[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]

            train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_cols)
            val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

            params = {
                "objective": "multiclass",
                "num_class": len(REGIME_LABELS),
                "metric": "multi_logloss",
                "boosting_type": "gbdt",
                "num_leaves": 31,
                "learning_rate": 0.05,
                "feature_fraction": 0.8,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": -1,
                "seed": 42,
            }

            model = lgb.train(
                params,
                train_data,
                num_boost_round=200,
                valid_sets=[val_data],
                callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)],
            )

            preds = model.predict(X_val)
            pred_labels = np.argmax(preds, axis=1)
            accuracy = float(np.mean(pred_labels == y_val))
            accuracies.append(accuracy)

            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_model = model

        if best_model is None:
            log.warning("no_model_produced_during_training")
            return

        avg_accuracy = float(np.mean(accuracies))
        log.info(
            "regime_model_retrain_complete",
            avg_accuracy=round(avg_accuracy, 4),
            best_accuracy=round(best_accuracy, 4),
            samples=len(df),
        )

        # Hot-swap model
        with self._model_lock:
            self._model = best_model
            self._model_accuracy = best_accuracy
            self._last_train_time = datetime.now(timezone.utc)

        # Cache model in Redis
        try:
            loop = asyncio.new_event_loop()
            model_bytes = pickle.dumps(best_model)
            loop.run_until_complete(
                self._redis.set(REDIS_KEY_MODEL, model_bytes, ex=86400)
            )
            loop.close()
        except Exception:
            log.exception("model_cache_failed")

    async def _load_training_data(self) -> pd.DataFrame | None:
        """Load feature history from Redis for training."""
        raw_entries = await self._redis.lrange(REDIS_KEY_FEATURE_HISTORY, 0, -1)
        if not raw_entries:
            return None

        records = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        for entry in raw_entries:
            try:
                data = json.loads(entry)
                ts = datetime.fromisoformat(data.get("timestamp", ""))
                if ts < cutoff:
                    continue
                record = {f: data.get(f, 0.0) for f in self._feature_names}
                record["regime"] = data.get("regime", "ranging")
                record["timestamp"] = ts
                records.append(record)
            except (json.JSONDecodeError, ValueError):
                continue

        if not records:
            return None

        df = pd.DataFrame(records)
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Filter to valid regime labels
        df = df[df["regime"].isin(REGIME_LABELS)]
        return df

    async def _load_cached_model(self) -> None:
        """Try to load a previously trained model from Redis."""
        raw = await self._redis.get(REDIS_KEY_MODEL)
        if raw:
            try:
                model = pickle.loads(raw)  # noqa: S301
                with self._model_lock:
                    self._model = model
                log.info("regime_model_loaded_from_cache")
            except Exception:
                log.warning("cached_model_load_failed")
