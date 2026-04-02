"""Technical Analyst agent — analyze indicators, output regime + signal."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.prompt_templates.market_analysis import (
    MARKET_ANALYSIS_PROMPT,
)

log = structlog.get_logger(__name__)


class TechnicalAnalyst:
    """Analyze technical indicators and produce regime/signal assessment."""

    def __init__(self, model_manager: ModelManager) -> None:
        self._mm = model_manager

    async def analyze(self, market_data: dict[str, Any]) -> dict[str, Any] | None:
        """Run technical analysis on the provided market data.

        Returns a dict with trend, momentum, volatility, key levels,
        signals, regime confirmation, and overall bias.
        """
        features = market_data.get("features", {})
        ticker = market_data.get("btc_price", {})
        regime = market_data.get("regime", {})
        funding = market_data.get("funding_rate", {})
        orderbook = market_data.get("orderbook_depth", {})

        current_price = (
            ticker.get("last_price")
            or ticker.get("price")
            or ticker.get("close", "N/A")
        )

        # Format indicators for the prompt
        indicator_lines = self._format_indicators(features)

        prompt = MARKET_ANALYSIS_PROMPT.format(
            current_price=current_price,
            price_change_24h=ticker.get("price_change_pct_24h", "N/A"),
            funding_rate=funding.get("rate", funding) if isinstance(funding, dict) else funding,
            regime=regime.get("label", regime) if isinstance(regime, dict) else regime,
            indicators=indicator_lines,
            orderbook_summary=json.dumps(orderbook, default=str) if orderbook else "N/A",
        )

        result = await self._mm.invoke(prompt, market_data)
        if result:
            log.info(
                "technical_analysis_complete",
                trend=result.get("trend"),
                bias=result.get("overall_bias"),
            )
        else:
            log.warning("technical_analysis_failed")
        return result

    @staticmethod
    def _format_indicators(features: dict[str, Any]) -> str:
        """Format feature dict into a readable indicator summary."""
        if not features:
            return "No indicator data available."

        lines: list[str] = []
        indicator_keys = [
            ("rsi_14", "RSI(14)"),
            ("macd", "MACD"),
            ("macd_signal", "MACD Signal"),
            ("macd_histogram", "MACD Histogram"),
            ("bb_upper", "Bollinger Upper"),
            ("bb_lower", "Bollinger Lower"),
            ("bb_middle", "Bollinger Middle"),
            ("sma_20", "SMA(20)"),
            ("sma_50", "SMA(50)"),
            ("sma_200", "SMA(200)"),
            ("ema_12", "EMA(12)"),
            ("ema_26", "EMA(26)"),
            ("atr_14", "ATR(14)"),
            ("adx", "ADX"),
            ("obv", "OBV"),
            ("volume_sma_20", "Volume SMA(20)"),
            ("stoch_k", "Stochastic %K"),
            ("stoch_d", "Stochastic %D"),
        ]

        for key, label in indicator_keys:
            value = features.get(key)
            if value is not None:
                if isinstance(value, float):
                    lines.append(f"- {label}: {value:.4f}")
                else:
                    lines.append(f"- {label}: {value}")

        # Include any extra features not in the predefined list
        known = {k for k, _ in indicator_keys}
        for key, value in features.items():
            if key not in known and not key.startswith("_"):
                lines.append(f"- {key}: {value}")

        return "\n".join(lines) if lines else "No indicator data available."
