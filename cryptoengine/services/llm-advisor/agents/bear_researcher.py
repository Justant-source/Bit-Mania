"""Bear Researcher agent — construct bearish arguments."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from shared.log_events import *
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

        from services.llm_advisor.agents.prompt_defaults import get_prompt_vars
        fmt_vars = get_prompt_vars(context.get("market_data", {}))
        fmt_vars.update(
            market_data=json.dumps(market, indent=2, default=str),
            sentiment_report=json.dumps(sentiment, indent=2, default=str),
        )
        prompt = BEAR_PROMPT.format(**fmt_vars)

        result = await self._mm.invoke(prompt, context)
        if result:
            log.info(
                LLM_ANALYSIS_COMPLETE,
                message="약세 연구 완료",
                thesis=result.get("thesis", "")[:80],
                confidence=result.get("confidence"),
            )
        else:
            log.warning(LLM_API_ERROR, message="약세 연구 실패")
        return result
