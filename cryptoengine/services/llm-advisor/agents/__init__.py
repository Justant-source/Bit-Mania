"""Trading analysis agents for the LLM Advisor service."""

from services.llm_advisor.agents.bear_researcher import BearResearcher
from services.llm_advisor.agents.bull_researcher import BullResearcher
from services.llm_advisor.agents.risk_manager import RiskManager
from services.llm_advisor.agents.sentiment_analyst import SentimentAnalyst
from services.llm_advisor.agents.technical_analyst import TechnicalAnalyst

__all__ = [
    "TechnicalAnalyst",
    "SentimentAnalyst",
    "RiskManager",
    "BullResearcher",
    "BearResearcher",
]
