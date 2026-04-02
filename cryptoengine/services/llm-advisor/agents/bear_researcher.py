"""Bear Researcher agent — construct bearish arguments."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.prompt_templates.debate_prompts import BEAR_PROMPT

log = structlog.get_logger(__name__)


class BearResearcher:
    """Construct the strongest possible bearish case for BTC/USDT."""

    def __init__(self, model_manager: ModelManager) -> None:
        self._mm = model_manager

    async def research(self, context: dict[str, Any]) -> dict[str, Any] | None:
        """Build a bearish argument from market data and sentiment analysis.

        Parameters
        ----------
        context:
            Dict with ``market_data`` and ``sentiment_report``.

        Returns a dict with thesis, arguments, price target,
        confidence, and key risk.
        """
        market = context.get("market_data", {})
        sentiment = context.get("sentiment_report", {})

        prompt = BEAR_PROMPT.format(
            market_data=json.dumps(market, indent=2, default=str),
            sentiment_report=json.dumps(sentiment, indent=2, default=str),
        )

        result = await self._mm.invoke(prompt, context)
        if result:
            log.info(
                "bear_research_complete",
                thesis=result.get("thesis", "")[:80],
                confidence=result.get("confidence"),
            )
        else:
            log.warning("bear_research_failed")
        return result
