"""Daily Reflection — compare LLM predictions against outcomes.

Runs at UTC 00:00 to collect all LLM judgments from the past 24 h,
compare them with actual market movements, and store results in
ChromaDB as "success" or "failure" entries for future retrieval.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_JUDGMENT_KEY_PREFIX = "llm:judgment:"
_OUTCOME_KEY_PREFIX = "market:outcome:"
_REFLECTION_KEY_PREFIX = "llm:reflection:"

_CHROMADB_COLLECTION = "llm_reflections"


class DailyReflection:
    """Orchestrate daily comparison of LLM predictions vs market reality."""

    def __init__(self, redis: Any, chroma_path: str = "/data/chromadb") -> None:
        self._redis = redis
        self._chroma_path = chroma_path
        self._collection: Any = None

    # ------------------------------------------------------------------
    # ChromaDB setup
    # ------------------------------------------------------------------

    def _ensure_collection(self) -> Any:
        """Lazily initialise the ChromaDB collection."""
        if self._collection is not None:
            return self._collection

        import chromadb

        client = chromadb.PersistentClient(path=self._chroma_path)
        self._collection = client.get_or_create_collection(
            name=_CHROMADB_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )
        return self._collection

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run_daily_reflection(self) -> None:
        """Full daily review: collect → compare → store."""
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        log.info("daily_reflection_started", date=str(yesterday))

        judgments = await self.collect_judgments(yesterday)
        if not judgments:
            log.info("daily_reflection_no_judgments", date=str(yesterday))
            return

        results = await self.compare_with_outcomes(judgments, yesterday)
        await self.store_results(results, yesterday)

        success_count = sum(1 for r in results if r["label"] == "success")
        log.info(
            "daily_reflection_complete",
            date=str(yesterday),
            total=len(results),
            successes=success_count,
            failures=len(results) - success_count,
        )

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    async def collect_judgments(
        self, date: datetime | Any
    ) -> list[dict[str, Any]]:
        """Retrieve all LLM judgments for *date* from Redis."""
        if hasattr(date, "isoformat"):
            date_str = date.isoformat()
        else:
            date_str = str(date)

        pattern = f"{_JUDGMENT_KEY_PREFIX}{date_str}:*"
        judgments: list[dict[str, Any]] = []

        cursor = "0"
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor, match=pattern, count=200
            )
            for key in keys:
                raw = await self._redis.get(key)
                if raw:
                    try:
                        judgments.append(json.loads(raw))
                    except json.JSONDecodeError:
                        log.warning("invalid_judgment_json", key=key)
            if cursor == "0" or cursor == 0:
                break

        log.debug("judgments_collected", date=date_str, count=len(judgments))
        return judgments

    async def compare_with_outcomes(
        self,
        judgments: list[dict[str, Any]],
        date: Any,
    ) -> list[dict[str, Any]]:
        """Compare each judgment's prediction against actual market outcome."""
        date_str = str(date)
        outcome_key = f"{_OUTCOME_KEY_PREFIX}{date_str}"
        raw_outcome = await self._redis.get(outcome_key)

        if not raw_outcome:
            log.warning("no_market_outcome_data", date=date_str)
            return []

        try:
            outcome = json.loads(raw_outcome)
        except json.JSONDecodeError:
            log.warning("invalid_outcome_json", date=date_str)
            return []

        actual_direction = _determine_direction(outcome)
        results: list[dict[str, Any]] = []

        for judgment in judgments:
            predicted = judgment.get("rating", "hold")
            predicted_dir = _rating_to_direction(predicted)
            is_correct = predicted_dir == actual_direction or predicted_dir == "neutral"

            results.append({
                "judgment": judgment,
                "outcome": outcome,
                "predicted_direction": predicted_dir,
                "actual_direction": actual_direction,
                "correct": is_correct,
                "label": "success" if is_correct else "failure",
                "date": date_str,
                "confidence": judgment.get("confidence", 0.0),
                "reasoning": judgment.get("reasoning", ""),
            })

        return results

    async def store_results(
        self,
        results: list[dict[str, Any]],
        date: Any,
    ) -> None:
        """Persist reflection results in ChromaDB and Redis."""
        if not results:
            return

        collection = self._ensure_collection()
        date_str = str(date)

        ids: list[str] = []
        documents: list[str] = []
        metadatas: list[dict[str, Any]] = []

        for idx, result in enumerate(results):
            doc_id = f"reflection_{date_str}_{idx}"
            ids.append(doc_id)

            doc_text = (
                f"Date: {date_str} | "
                f"Predicted: {result['predicted_direction']} | "
                f"Actual: {result['actual_direction']} | "
                f"Result: {result['label']} | "
                f"Confidence: {result['confidence']:.2f} | "
                f"Reasoning: {result['reasoning']}"
            )
            documents.append(doc_text)

            metadatas.append({
                "date": date_str,
                "label": result["label"],
                "predicted_direction": result["predicted_direction"],
                "actual_direction": result["actual_direction"],
                "confidence": float(result["confidence"]),
            })

        collection.upsert(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
        )

        # Also cache a summary in Redis (7-day TTL)
        summary = {
            "date": date_str,
            "total": len(results),
            "successes": sum(1 for r in results if r["label"] == "success"),
            "failures": sum(1 for r in results if r["label"] == "failure"),
        }
        await self._redis.set(
            f"{_REFLECTION_KEY_PREFIX}{date_str}",
            json.dumps(summary),
            ex=7 * 86400,
        )

        log.info("reflection_results_stored", date=date_str, count=len(results))


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _determine_direction(outcome: dict[str, Any]) -> str:
    """Derive market direction from outcome data."""
    pnl = outcome.get("price_change_pct", 0.0)
    if isinstance(pnl, str):
        try:
            pnl = float(pnl)
        except ValueError:
            return "neutral"

    if pnl > 0.5:
        return "bullish"
    elif pnl < -0.5:
        return "bearish"
    return "neutral"


def _rating_to_direction(rating: str) -> str:
    """Map an LLM rating string to a direction."""
    rating_lower = rating.lower().strip()
    if rating_lower in ("strong_buy", "buy"):
        return "bullish"
    elif rating_lower in ("strong_sell", "sell"):
        return "bearish"
    return "neutral"
