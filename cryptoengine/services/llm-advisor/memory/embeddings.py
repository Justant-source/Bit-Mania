"""Embedding Model — sentence-transformers wrapper.

Uses ``all-MiniLM-L6-v2`` for fast, lightweight text embeddings suitable
for trade memory similarity search.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import structlog

log = structlog.get_logger(__name__)

_DEFAULT_MODEL = "all-MiniLM-L6-v2"


class EmbeddingModel:
    """Wrapper around sentence-transformers for text embeddings."""

    def __init__(self, model_name: str = _DEFAULT_MODEL) -> None:
        self._model_name = model_name
        self._model: Any = None

    # ------------------------------------------------------------------
    # Lazy loading
    # ------------------------------------------------------------------

    def _ensure_model(self) -> Any:
        if self._model is not None:
            return self._model

        from sentence_transformers import SentenceTransformer

        log.info("loading_embedding_model", model=self._model_name)
        self._model = SentenceTransformer(self._model_name)
        log.info("embedding_model_loaded", model=self._model_name)
        return self._model

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def encode(self, text: str) -> list[float]:
        """Encode a single text into a vector.

        Returns a list of floats (embedding dimension depends on model;
        384 for all-MiniLM-L6-v2).
        """
        model = self._ensure_model()
        embedding: np.ndarray = model.encode(
            text, convert_to_numpy=True, show_progress_bar=False
        )
        return embedding.tolist()

    def encode_batch(self, texts: list[str], batch_size: int = 32) -> list[list[float]]:
        """Encode a batch of texts into vectors.

        Returns a list of embedding vectors.
        """
        if not texts:
            return []

        model = self._ensure_model()
        embeddings: np.ndarray = model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return embeddings.tolist()

    @property
    def dimension(self) -> int:
        """Return the embedding dimension of the loaded model."""
        model = self._ensure_model()
        return model.get_sentence_embedding_dimension()
