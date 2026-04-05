"""Risk Manager agent — evaluate portfolio risk and recommend adjustments."""

from __future__ import annotations

import json
from typing import Any

import structlog

from services.llm_advisor.model_manager import ModelManager
from shared.log_events import *
from services.llm_advisor.prompt_templates.risk_evaluation import (
    RISK_EVALUATION_PROMPT,
)

log = structlog.get_logger(__name__)


class RiskManager:
    """Evaluate portfolio risk and recommend weight adjustments."""

    def __init__(self, model_manager: ModelManager) -> None:
        self._mm = model_manager

    async def evaluate(self, context: dict[str, Any]) -> dict[str, Any] | None:
        """Run risk evaluation.

        Parameters
        ----------
        context:
            Dict containing ``market_data``, ``debate_conclusion``,
            and ``technical_report``.

        Returns a dict with risk level, position sizing, leverage,
        stop-loss guidance, and weight adjustments.
        """
        market = context.get("market_data", {})
        debate = context.get("debate_conclusion", {})
        technical = context.get("technical_report", {})

        ticker = market.get("btc_price", {})
        portfolio = market.get("portfolio", {})
        regime = market.get("regime", {})
        funding = market.get("funding_rate", {})
        features = market.get("features", {})

        current_price = (
            ticker.get("last_price")
            or ticker.get("price")
            or ticker.get("close", "N/A")
        )

        prompt = RISK_EVALUATION_PROMPT.format(
            total_equity=portfolio.get("total_equity", "N/A"),
            open_positions=json.dumps(
                portfolio.get("positions", []), default=str
            ),
            unrealised_pnl=portfolio.get("unrealised_pnl", "N/A"),
            exposure_pct=portfolio.get("exposure_pct", "N/A"),
            max_drawdown_pct=portfolio.get("max_drawdown_pct", "N/A"),
            current_price=current_price,
            regime=regime.get("label", regime) if isinstance(regime, dict) else regime,
            funding_rate=funding.get("rate", funding) if isinstance(funding, dict) else funding,
            atr_pct=features.get("atr_pct", features.get("atr_14", "N/A")),
            recent_trades=json.dumps(
                portfolio.get("recent_trades", [])[:10], default=str
            ),
            debate_conclusion=json.dumps(debate, default=str),
        )

        result = await self._mm.invoke(prompt, context)
        if result:
            log.info(
                LLM_ANALYSIS_COMPLETE,
                message="리스크 평가 완료",
                risk_level=result.get("risk_level"),
                reduce_exposure=result.get("reduce_exposure"),
            )
        else:
            log.warning(LLM_API_ERROR, message="리스크 평가 실패")
        return result
