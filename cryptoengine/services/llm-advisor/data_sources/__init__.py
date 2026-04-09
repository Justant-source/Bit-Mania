"""External data source modules for LLM Advisor context enrichment."""

from .base import BaseDataSource, SourceHealth
from .failure_detection import FailureDetector
from .etf_flows import ETFFlowsSource
from .onchain_metrics import OnchainMetricsSource
from .macro_indicators import MacroIndicatorsSource
from .research_reports import ResearchReportsSource
from .derivatives import DerivativesSource

__all__ = [
    "BaseDataSource",
    "SourceHealth",
    "FailureDetector",
    "ETFFlowsSource",
    "OnchainMetricsSource",
    "MacroIndicatorsSource",
    "ResearchReportsSource",
    "DerivativesSource",
]
