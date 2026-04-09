"""Trading Analysis StateGraph — LangGraph-based multi-agent workflow.

Implements a TradingAgents-style analysis pipeline:

    technical_analysis ──► bull_research ──┐
                                           ├──► debate ──► risk_check ──► decide
    sentiment_analysis ──► bear_research ──┘

Parallel edges run concurrently; sequential edges enforce ordering.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import structlog
from langgraph.graph import END, StateGraph

from shared.log_events import *

from services.llm_advisor.agents.bear_researcher import BearResearcher
from services.llm_advisor.agents.bull_researcher import BullResearcher
from services.llm_advisor.agents.risk_manager import RiskManager
from services.llm_advisor.agents.sentiment_analyst import SentimentAnalyst
from services.llm_advisor.agents.technical_analyst import TechnicalAnalyst
from services.llm_advisor.model_manager import ModelManager
from services.llm_advisor.prompt_templates.debate_prompts import (
    DEBATE_ROUND_1,
    DEBATE_ROUND_2,
    MODERATOR_PROMPT,
)
from services.llm_advisor.vision_chart import ChartAnalyzer

log = structlog.get_logger(__name__)


# ------------------------------------------------------------------
# State definition
# ------------------------------------------------------------------

class TradingAnalysisState(TypedDict, total=False):
    """Shared state flowing through the analysis graph."""

    market_data: dict[str, Any]
    technical_report: dict[str, Any]
    sentiment_report: dict[str, Any]
    bull_argument: dict[str, Any]
    bear_argument: dict[str, Any]
    debate_conclusion: dict[str, Any]
    risk_assessment: dict[str, Any]
    final_decision: dict[str, Any]


# ------------------------------------------------------------------
# Graph builder
# ------------------------------------------------------------------

class TradingAnalysisGraph:
    """Constructs and executes the trading analysis LangGraph."""

    def __init__(
        self,
        model_manager: ModelManager,
        chart_analyzer: ChartAnalyzer,
    ) -> None:
        self._mm = model_manager
        self._chart = chart_analyzer

        # Instantiate agents
        self._technical = TechnicalAnalyst(model_manager)
        self._sentiment = SentimentAnalyst(model_manager)
        self._bull = BullResearcher(model_manager)
        self._bear = BearResearcher(model_manager)
        self._risk = RiskManager(model_manager)

        self._graph = self._build_graph()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    async def run(self, market_data: dict[str, Any]) -> dict[str, Any] | None:
        """Execute the full analysis pipeline.

        Returns the ``final_decision`` dict (enriched with intermediate
        reports) or ``None`` on failure.
        """
        initial_state: TradingAnalysisState = {"market_data": market_data}
        try:
            final_state = await self._graph.ainvoke(initial_state)
            decision = final_state.get("final_decision")
            if decision:
                # Attach intermediate reports so the caller can persist them
                decision["_technical_report"] = final_state.get("technical_report", {})
                decision["_sentiment_report"] = final_state.get("sentiment_report", {})
                decision["_bull_argument"] = final_state.get("bull_argument", {})
                decision["_bear_argument"] = final_state.get("bear_argument", {})
                decision["_debate_conclusion"] = final_state.get("debate_conclusion", {})
                decision["_risk_assessment"] = final_state.get("risk_assessment", {})
                log.info(
                    LLM_ANALYSIS_COMPLETE,
                    message="분석 그래프 완료",
                    rating=decision.get("rating"),
                    confidence=decision.get("confidence"),
                )
            return decision
        except Exception:
            log.exception(LLM_API_ERROR, message="분석 그래프 오류")
            return None

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build_graph(self) -> Any:
        """Build the LangGraph StateGraph with parallel and sequential edges."""
        graph = StateGraph(TradingAnalysisState)

        # Register nodes — two combined parallel nodes reduce sequential LLM calls
        graph.add_node("parallel_init", self._node_parallel_init)
        graph.add_node("parallel_research", self._node_parallel_research)
        graph.add_node("debate", self._node_debate)
        graph.add_node("risk_check", self._node_risk)
        graph.add_node("decide", self._node_decide)

        # parallel_init runs technical + sentiment concurrently (2 LLM calls in parallel)
        graph.set_entry_point("parallel_init")
        # parallel_research runs bull + bear concurrently (2 LLM calls in parallel)
        graph.add_edge("parallel_init", "parallel_research")

        # Sequential debate → risk → decide (3 LLM calls)
        graph.add_edge("parallel_research", "debate")
        graph.add_edge("debate", "risk_check")
        graph.add_edge("risk_check", "decide")
        graph.add_edge("decide", END)

        return graph.compile()

    # ------------------------------------------------------------------
    # Node implementations
    # ------------------------------------------------------------------

    async def _node_parallel_init(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        """Run technical + sentiment analysis concurrently."""
        market = state.get("market_data", {})
        tech_report, sent_report = await asyncio.gather(
            self._technical.analyze(market),
            self._sentiment.analyze(market),
            return_exceptions=True,
        )
        if isinstance(tech_report, BaseException):
            log.warning(LLM_API_ERROR, message="기술분석 오류", error=str(tech_report))
            tech_report = {}
        if isinstance(sent_report, BaseException):
            log.warning(LLM_API_ERROR, message="감성분석 오류", error=str(sent_report))
            sent_report = {}
        return {"technical_report": tech_report or {}, "sentiment_report": sent_report or {}}

    async def _node_parallel_research(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        """Run bull + bear research concurrently."""
        bull_ctx = {
            "market_data": state.get("market_data", {}),
            "technical_report": state.get("technical_report", {}),
        }
        bear_ctx = {
            "market_data": state.get("market_data", {}),
            "sentiment_report": state.get("sentiment_report", {}),
        }
        bull_arg, bear_arg = await asyncio.gather(
            self._bull.research(bull_ctx),
            self._bear.research(bear_ctx),
            return_exceptions=True,
        )
        if isinstance(bull_arg, BaseException):
            log.warning(LLM_API_ERROR, message="강세연구 오류", error=str(bull_arg))
            bull_arg = {}
        if isinstance(bear_arg, BaseException):
            log.warning(LLM_API_ERROR, message="약세연구 오류", error=str(bear_arg))
            bear_arg = {}
        return {"bull_argument": bull_arg or {}, "bear_argument": bear_arg or {}}

    async def _node_debate(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        """Single-call debate: moderator synthesises bull vs bear directly."""
        bull = state.get("bull_argument", {})
        bear = state.get("bear_argument", {})

        from services.llm_advisor.agents.prompt_defaults import get_prompt_vars
        _d = get_prompt_vars(state.get("market_data", {}))

        # Combine bull/bear arguments into a single moderator call (was 3 calls)
        moderator_task = MODERATOR_PROMPT.format(
            bull_argument=bull,
            bear_argument=bear,
            round1_summary="(skipped — single-round debate)",
            round2_summary="(skipped — single-round debate)",
            **_d,
        )
        conclusion = await self._mm.invoke(moderator_task, {"bull": bull, "bear": bear}) or {}

        return {"debate_conclusion": conclusion}

    async def _node_risk(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        context = {
            "market_data": state.get("market_data", {}),
            "debate_conclusion": state.get("debate_conclusion", {}),
            "technical_report": state.get("technical_report", {}),
        }
        assessment = await self._risk.evaluate(context)
        return {"risk_assessment": assessment or {}}

    async def _node_decide(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        """Synthesise all analyses into a final decision."""
        task = (
            "You are the Chief Investment Officer. Based on all analyses, "
            "produce a final trading decision. Respond with JSON containing: "
            "rating (strong_buy|buy|hold|sell|strong_sell), confidence (0-1), "
            "weight_adjustments (dict of strategy→multiplier), reasoning (str), "
            "and regime_assessment (str)."
        )
        context = {
            "technical_report": state.get("technical_report", {}),
            "sentiment_report": state.get("sentiment_report", {}),
            "debate_conclusion": state.get("debate_conclusion", {}),
            "risk_assessment": state.get("risk_assessment", {}),
            "market_data": state.get("market_data", {}),
        }
        decision = await self._mm.invoke(task, context)
        if decision is None:
            decision = {
                "rating": "hold",
                "confidence": 0.0,
                "weight_adjustments": {},
                "reasoning": "LLM unavailable — defaulting to hold.",
                "regime_assessment": "unknown",
            }
        return {"final_decision": decision}
