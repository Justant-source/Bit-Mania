"""Bull Researcher agent — construct bullish arguments."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from shared.log_events import *
from services.llm_advisor.prompt_templates.debate_prompts import BULL_PROMPT

log = structlog.get_logger(__name__)


class BullResearcher:
    """Construct the strongest possible bullish case for BTC/USDT."""

    def __init__(self, model_manager: ModelManager) -> None:
        self._mm = model_manager

    async def research(self, context: dict[str, Any]) -> dict[str, Any] | None:
        """Build a bullish argument from market data and technical analysis.

        Parameters
        ----------
        context:
            Dict with ``market_data`` and ``technical_report``.

        Returns a dict with thesis, arguments, price target,
        confidence, and key risk.
        """
        market = context.get("market_data", {})
        technical = context.get("technical_report", {})

        prompt = BULL_PROMPT.format(
            market_data=json.dumps(market, indent=2, default=str),
            technical_report=json.dumps(technical, indent=2, default=str),
        )

        result = await self._mm.invoke(prompt, context)
        if result:
            log.info(
                LLM_ANALYSIS_COMPLETE,
                message="강세 연구 완료",
                thesis=result.get("thesis", "")[:80],
                confidence=result.get("confidence"),
            )
        else:
            log.warning(LLM_API_ERROR, message="강세 연구 실패")
        return result
