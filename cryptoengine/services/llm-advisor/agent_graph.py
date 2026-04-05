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

        # Register nodes
        graph.add_node("technical_analysis", self._node_technical)
        graph.add_node("sentiment_analysis", self._node_sentiment)
        graph.add_node("bull_research", self._node_bull)
        graph.add_node("bear_research", self._node_bear)
        graph.add_node("debate", self._node_debate)
        graph.add_node("risk_check", self._node_risk)
        graph.add_node("decide", self._node_decide)

        # Entry: parallel fan-out to technical + sentiment
        graph.set_entry_point("technical_analysis")
        # LangGraph doesn't natively fan-out from START to two nodes,
        # so we run technical first, then sentiment, or we use
        # the parallel helper.  We chain them for compatibility:
        graph.add_edge("technical_analysis", "sentiment_analysis")

        # After both analyses, fan out to bull + bear research
        # (sequential in graph, but the node implementations can
        # run concurrently via the _node_parallel helper).
        graph.add_edge("sentiment_analysis", "bull_research")
        graph.add_edge("bull_research", "bear_research")

        # Sequential debate → risk → decide
        graph.add_edge("bear_research", "debate")
        graph.add_edge("debate", "risk_check")
        graph.add_edge("risk_check", "decide")
        graph.add_edge("decide", END)

        return graph.compile()

    # ------------------------------------------------------------------
    # Node implementations
    # ------------------------------------------------------------------

    async def _node_technical(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        market = state.get("market_data", {})
        report = await self._technical.analyze(market)
        return {"technical_report": report or {}}

    async def _node_sentiment(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        market = state.get("market_data", {})
        report = await self._sentiment.analyze(market)
        return {"sentiment_report": report or {}}

    async def _node_bull(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        context = {
            "market_data": state.get("market_data", {}),
            "technical_report": state.get("technical_report", {}),
        }
        argument = await self._bull.research(context)
        return {"bull_argument": argument or {}}

    async def _node_bear(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        context = {
            "market_data": state.get("market_data", {}),
            "sentiment_report": state.get("sentiment_report", {}),
        }
        argument = await self._bear.research(context)
        return {"bear_argument": argument or {}}

    async def _node_debate(
        self, state: TradingAnalysisState
    ) -> dict[str, Any]:
        bull = state.get("bull_argument", {})
        bear = state.get("bear_argument", {})

        # Round 1
        round1_ctx = {
            "bull_argument": bull,
            "bear_argument": bear,
            "round": 1,
        }
        round1_task = DEBATE_ROUND_1.format(
            bull_argument=bull,
            bear_argument=bear,
        )
        round1 = await self._mm.invoke(round1_task, round1_ctx) or {}

        # Round 2
        round2_task = DEBATE_ROUND_2.format(
            round1_summary=round1,
            bull_argument=bull,
            bear_argument=bear,
        )
        round2 = await self._mm.invoke(round2_task, {"round1": round1}) or {}

        # Moderator conclusion
        moderator_task = MODERATOR_PROMPT.format(
            bull_argument=bull,
            bear_argument=bear,
            round1_summary=round1,
            round2_summary=round2,
        )
        conclusion = await self._mm.invoke(moderator_task) or {}

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
