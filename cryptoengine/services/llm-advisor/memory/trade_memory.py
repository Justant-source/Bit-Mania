"""Trade Memory — ChromaDB-backed storage for trade outcomes.

Stores trade feature vectors alongside metadata (strategy, action, PnL,
regime) and allows similarity-based retrieval with separate success/failure
result sets for few-shot prompting.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import structlog

from services.llm_advisor.memory.embeddings import EmbeddingModel

log = structlog.get_logger(__name__)

_COLLECTION_NAME = "trade_memory"


class TradeMemory:
    """ChromaDB-backed trade outcome memory."""

    def __init__(
        self,
        chroma_path: str = "/data/chromadb",
        embedding_model: EmbeddingModel | None = None,
    ) -> None:
        self._chroma_path = chroma_path
        self._embedding = embedding_model or EmbeddingModel()
        self._collection: Any = None

    # ------------------------------------------------------------------
    # Lazy init
    # ------------------------------------------------------------------

    def _ensure_collection(self) -> Any:
        if self._collection is not None:
            return self._collection

        import chromadb

        client = chromadb.PersistentClient(path=self._chroma_path)
        self._collection = client.get_or_create_collection(
            name=_COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        log.info("trade_memory_collection_ready", name=_COLLECTION_NAME)
        return self._collection

    # ------------------------------------------------------------------
    # Store
    # ------------------------------------------------------------------

    def store_trade(
        self,
        features: dict[str, float],
        strategy: str,
        action: str,
        pnl: float,
        regime: str,
        context_text: str,
    ) -> str:
        """Store a trade outcome in ChromaDB.

        Returns the generated document ID.
        """
        collection = self._ensure_collection()

        doc_id = f"trade_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        label = "success" if pnl > 0 else "failure"

        # Build a human-readable document for embedding
        document = (
            f"Strategy: {strategy} | Action: {action} | "
            f"PnL: {pnl:.4f} | Regime: {regime} | "
            f"Result: {label} | Context: {context_text}"
        )

        # Generate embedding from the feature vector + context
        embedding_text = self._features_to_text(features) + " " + context_text
        embedding = self._embedding.encode(embedding_text)

        metadata = {
            "strategy": strategy,
            "action": action,
            "pnl": float(pnl),
            "regime": regime,
            "label": label,
            "timestamp": time.time(),
            "features_json": json.dumps(features, default=str),
        }

        collection.add(
            ids=[doc_id],
            embeddings=[embedding],
            documents=[document],
            metadatas=[metadata],
        )

        log.debug(
            "trade_stored",
            doc_id=doc_id,
            strategy=strategy,
            pnl=pnl,
            label=label,
        )
        return doc_id

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query_similar(
        self,
        current_features: dict[str, float],
        top_k: int = 10,
    ) -> dict[str, list[dict[str, Any]]]:
        """Find trades similar to the current feature set.

        Returns a dict with ``"success"`` and ``"failure"`` lists,
        each containing trade metadata sorted by relevance.
        """
        collection = self._ensure_collection()

        query_text = self._features_to_text(current_features)
        embedding = self._embedding.encode(query_text)

        results: dict[str, list[dict[str, Any]]] = {
            "success": [],
            "failure": [],
        }

        for label in ("success", "failure"):
            try:
                hits = collection.query(
                    query_embeddings=[embedding],
                    n_results=top_k,
                    where={"label": label},
                    include=["documents", "metadatas", "distances"],
                )
            except Exception:
                log.warning("trade_memory_query_failed", label=label)
                continue

            if not hits or not hits.get("ids"):
                continue

            ids = hits["ids"][0]
            docs = hits["documents"][0] if hits.get("documents") else [""] * len(ids)
            metas = hits["metadatas"][0] if hits.get("metadatas") else [{}] * len(ids)
            dists = hits["distances"][0] if hits.get("distances") else [0.0] * len(ids)

            for doc_id, doc, meta, dist in zip(ids, docs, metas, dists):
                results[label].append({
                    "id": doc_id,
                    "document": doc,
                    "metadata": meta,
                    "similarity": 1.0 - dist,  # cosine distance → similarity
                })

        log.debug(
            "trade_memory_queried",
            successes=len(results["success"]),
            failures=len(results["failure"]),
        )
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _features_to_text(features: dict[str, float]) -> str:
        """Convert a feature dict to a searchable text representation."""
        parts: list[str] = []
        for key, value in sorted(features.items()):
            if isinstance(value, float):
                parts.append(f"{key}={value:.4f}")
            else:
                parts.append(f"{key}={value}")
        return " ".join(parts)
