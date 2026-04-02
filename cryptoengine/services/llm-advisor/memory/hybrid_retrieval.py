"""Hybrid Retriever — combined dense + sparse search.

Merges cosine-similarity results from ChromaDB (dense) with BM25
keyword matching (sparse) using a weighted score:

    combined = 0.6 * dense_score + 0.4 * sparse_score
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import structlog
from rank_bm25 import BM25Okapi

from services.llm_advisor.memory.embeddings import EmbeddingModel
from services.llm_advisor.memory.trade_memory import TradeMemory

log = structlog.get_logger(__name__)

_DENSE_WEIGHT = 0.6
_SPARSE_WEIGHT = 0.4


class HybridRetriever:
    """Combined dense (ChromaDB) + sparse (BM25) retrieval."""

    def __init__(
        self,
        trade_memory: TradeMemory,
        embedding_model: EmbeddingModel | None = None,
        dense_weight: float = _DENSE_WEIGHT,
        sparse_weight: float = _SPARSE_WEIGHT,
    ) -> None:
        self._memory = trade_memory
        self._embedding = embedding_model or EmbeddingModel()
        self._dense_weight = dense_weight
        self._sparse_weight = sparse_weight

        # BM25 corpus — rebuilt on each search from ChromaDB results
        self._bm25: BM25Okapi | None = None
        self._corpus_docs: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """Run hybrid dense + sparse search and return ranked results.

        Parameters
        ----------
        query:
            Natural-language query or feature-based text.
        top_k:
            Number of results to return.

        Returns
        -------
        List of result dicts sorted by combined score, each containing
        ``document``, ``metadata``, ``dense_score``, ``sparse_score``,
        and ``combined_score``.
        """
        # --- Dense retrieval via ChromaDB ---
        collection = self._memory._ensure_collection()  # noqa: SLF001
        query_embedding = self._embedding.encode(query)

        # Fetch more candidates than needed so BM25 has material
        n_candidates = max(top_k * 5, 50)

        try:
            dense_results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_candidates,
                include=["documents", "metadatas", "distances"],
            )
        except Exception:
            log.warning("hybrid_dense_query_failed")
            return []

        if not dense_results or not dense_results.get("ids"):
            return []

        ids = dense_results["ids"][0]
        docs = dense_results["documents"][0] if dense_results.get("documents") else []
        metas = dense_results["metadatas"][0] if dense_results.get("metadatas") else []
        dists = dense_results["distances"][0] if dense_results.get("distances") else []

        # Build candidate pool
        candidates: dict[str, dict[str, Any]] = {}
        for doc_id, doc, meta, dist in zip(ids, docs, metas, dists):
            dense_score = max(1.0 - dist, 0.0)  # cosine distance → similarity
            candidates[doc_id] = {
                "id": doc_id,
                "document": doc,
                "metadata": meta,
                "dense_score": dense_score,
                "sparse_score": 0.0,
                "combined_score": 0.0,
            }

        # --- Sparse retrieval via BM25 ---
        if candidates:
            self._build_bm25(candidates)
            sparse_scores = self._bm25_score(query)
            for doc_id, score in sparse_scores.items():
                if doc_id in candidates:
                    candidates[doc_id]["sparse_score"] = score

        # --- Combine scores ---
        for entry in candidates.values():
            entry["combined_score"] = (
                self._dense_weight * entry["dense_score"]
                + self._sparse_weight * entry["sparse_score"]
            )

        # Sort and return top_k
        ranked = sorted(
            candidates.values(),
            key=lambda x: x["combined_score"],
            reverse=True,
        )[:top_k]

        log.debug(
            "hybrid_search_complete",
            query_len=len(query),
            candidates=len(candidates),
            returned=len(ranked),
        )
        return ranked

    # ------------------------------------------------------------------
    # BM25 helpers
    # ------------------------------------------------------------------

    def _build_bm25(self, candidates: dict[str, dict[str, Any]]) -> None:
        """Build a BM25 index from the candidate documents."""
        self._corpus_docs = list(candidates.values())
        tokenized = [
            self._tokenize(doc["document"])
            for doc in self._corpus_docs
        ]
        self._bm25 = BM25Okapi(tokenized)

    def _bm25_score(self, query: str) -> dict[str, float]:
        """Score all corpus documents against the query using BM25.

        Returns a dict mapping doc IDs to normalised BM25 scores in [0, 1].
        """
        if self._bm25 is None or not self._corpus_docs:
            return {}

        query_tokens = self._tokenize(query)
        raw_scores = self._bm25.get_scores(query_tokens)

        # Normalise to [0, 1]
        max_score = float(np.max(raw_scores)) if len(raw_scores) > 0 else 1.0
        if max_score <= 0:
            max_score = 1.0

        result: dict[str, float] = {}
        for doc, score in zip(self._corpus_docs, raw_scores):
            result[doc["id"]] = float(score / max_score)

        return result

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Simple whitespace + lowercase tokenizer."""
        return text.lower().split()
