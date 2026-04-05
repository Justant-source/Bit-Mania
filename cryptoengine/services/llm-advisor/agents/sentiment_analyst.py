"""Sentiment Analyst agent — analyze funding rate and long/short ratio."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from shared.log_events import *

log = structlog.get_logger(__name__)

_SENTIMENT_PROMPT = """\
You are a crypto market sentiment analyst specialising in derivatives data. \
Analyze the funding rate, long/short ratio, and open interest to gauge \
market positioning and sentiment.

## Funding Rate Data
{funding_data}

## Long/Short Ratio
{long_short_ratio}

## Open Interest
{open_interest}

## Market Context
- **Price**: ${current_price}
- **Regime**: {regime}

## Instructions
Assess market sentiment from derivatives data. Respond with JSON:
{{
  "sentiment": "extreme_greed|greed|neutral|fear|extreme_fear",
  "funding_bias": "bullish|neutral|bearish",
  "funding_annualized_pct": number,
  "crowding_risk": "low|moderate|high",
  "crowding_direction": "long|short|balanced",
  "contrarian_signal": true|false,
  "contrarian_direction": "bullish|bearish|none",
  "key_observations": ["list of notable findings"],
  "confidence": 0.0-1.0,
  "summary": "Brief 1-2 sentence assessment"
}}
"""


class SentimentAnalyst:
    """Analyze funding rates, long/short ratios, and positioning data."""

    def __init__(self, model_manager: ModelManager) -> None:
        self._mm = model_manager

    async def analyze(self, market_data: dict[str, Any]) -> dict[str, Any] | None:
        """Run sentiment analysis on derivatives data.

        Returns a dict with sentiment classification, funding bias,
        crowding risk, and contrarian signals.
        """
        ticker = market_data.get("btc_price", {})
        funding = market_data.get("funding_rate", {})
        regime = market_data.get("regime", {})
        features = market_data.get("features", {})

        current_price = (
            ticker.get("last_price")
            or ticker.get("price")
            or ticker.get("close", "N/A")
        )

        # Extract long/short ratio from features if available
        long_short = features.get("long_short_ratio", "N/A")
        open_interest = features.get("open_interest", "N/A")

        prompt = _SENTIMENT_PROMPT.format(
            funding_data=json.dumps(funding, default=str) if isinstance(funding, dict) else str(funding),
            long_short_ratio=long_short,
            open_interest=open_interest,
            current_price=current_price,
            regime=regime.get("label", regime) if isinstance(regime, dict) else regime,
        )

        result = await self._mm.invoke(prompt, market_data)
        if result:
            log.info(
                LLM_ANALYSIS_COMPLETE,
                message="감성 분석 완료",
                sentiment=result.get("sentiment"),
                funding_bias=result.get("funding_bias"),
            )
        else:
            log.warning(LLM_API_ERROR, message="감성 분석 실패")
        return result
