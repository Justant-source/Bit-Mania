"""Prompt templates for the LLM Advisor service."""

from services.llm_advisor.prompt_templates.daily_report import DAILY_REPORT_PROMPT
from services.llm_advisor.prompt_templates.debate_prompts import (
    BEAR_PROMPT,
    BULL_PROMPT,
    DEBATE_ROUND_1,
    DEBATE_ROUND_2,
    MODERATOR_PROMPT,
)
from services.llm_advisor.prompt_templates.market_analysis import (
    MARKET_ANALYSIS_PROMPT,
)
from services.llm_advisor.prompt_templates.regime_assessment import (
    REGIME_ASSESSMENT_PROMPT,
)
from services.llm_advisor.prompt_templates.risk_evaluation import (
    RISK_EVALUATION_PROMPT,
)
from services.llm_advisor.prompt_templates.asset_report import (
    ASSET_REPORT_PROMPT,
)
from services.llm_advisor.prompt_templates.etf_flow_analysis import (
    ETF_FLOW_ANALYSIS_PROMPT,
)
from services.llm_advisor.prompt_templates.macro_liquidity import (
    MACRO_LIQUIDITY_PROMPT,
)

__all__ = [
    "MARKET_ANALYSIS_PROMPT",
    "REGIME_ASSESSMENT_PROMPT",
    "RISK_EVALUATION_PROMPT",
    "DAILY_REPORT_PROMPT",
    "ASSET_REPORT_PROMPT",
    "BULL_PROMPT",
    "BEAR_PROMPT",
    "DEBATE_ROUND_1",
    "DEBATE_ROUND_2",
    "MODERATOR_PROMPT",
    "ETF_FLOW_ANALYSIS_PROMPT",
    "MACRO_LIQUIDITY_PROMPT",
]
