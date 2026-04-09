"""Context builder — aggregates all external data sources into a single MarketContext."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from .data_sources import (
    BaseDataSource,
    DerivativesSource,
    ETFFlowsSource,
    MacroIndicatorsSource,
    OnchainMetricsSource,
    ResearchReportsSource,
)
from .data_sources.base import SourceHealth

logger = structlog.get_logger(__name__)

# Priority sources — if these fail, insert warning banner
PRIORITY_SOURCES = {"etf", "macro"}
LOW_CONFIDENCE_THRESHOLD = 0.6


@dataclass
class MarketContext:
    """Unified market context for LLM prompt rendering."""

    # Existing fields (backward compatible)
    btc_price: float = 0.0
    price_change_24h: float = 0.0
    funding_rate: float = 0.0
    regime: str = "unknown"

    # New: 5 external data source summaries
    etf: dict = field(default_factory=dict)
    onchain: dict = field(default_factory=dict)
    macro: dict = field(default_factory=dict)
    research: dict = field(default_factory=dict)
    derivatives: dict = field(default_factory=dict)

    # V2: per-source health status
    health: dict[str, "SourceHealth"] = field(default_factory=dict)

    # Meta
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data_freshness_score: float = 0.0

    @property
    def broken_sources(self) -> list[str]:
        return [k for k, v in self.health.items() if v.status == "BROKEN"]

    @property
    def degraded_sources(self) -> list[str]:
        return [k for k, v in self.health.items() if v.status == "DEGRADED"]

    def to_prompt_vars(self) -> dict:
        """Flatten into dict for prompt template .format() calls.

        All existing variable names are preserved for backward compatibility.
        New variables are appended.
        """
        # Helper to safely get nested dict values
        def _get(source: dict, key: str, default: Any = "N/A") -> Any:
            return source.get(key, default)

        vars_dict = {
            # Existing
            "btc_price": self.btc_price,
            "current_price": self.btc_price,
            "price_change_24h": self.price_change_24h,
            "funding_rate": self.funding_rate,
            "regime": self.regime,

            # ETF
            "etf_narrative": _get(self.etf, "narrative"),
            "etf_daily_net_flow": _get(self.etf, "daily_net_flow_usd_mn", 0),
            "etf_cumulative_flow": _get(self.etf, "cumulative_flow_usd_bn", 0),
            "etf_7d_trend": _get(self.etf, "flow_7d_trend"),
            "etf_flow_streak": _get(self.etf, "flow_streak_days", 0),
            "etf_top_inflow_issuer": _get(self.etf, "top_inflow_issuer", "N/A"),
            "etf_top_outflow_issuer": _get(self.etf, "top_outflow_issuer", "N/A"),
            "etf_issuer_breakdown_table": self._format_issuer_table(),
            "etf_30d_avg_flow": _get(self.etf, "flow_30d_avg_mn", 0),
            "etf_flow_zscore": _get(self.etf, "flow_zscore", 0),
            "etf_max_inflow_90d": _get(self.etf, "max_inflow_90d_mn", 0),
            "etf_max_outflow_90d": _get(self.etf, "max_outflow_90d_mn", 0),

            # On-chain
            "onchain_narrative": _get(self.onchain, "narrative"),
            "mvrv_zscore": _get(self.onchain, "mvrv_zscore", 0),
            "mvrv_percentile": _get(self.onchain, "mvrv_historical_percentile", 0),
            "mvrv_interpretation": _get(self.onchain, "mvrv_interpretation"),
            "exchange_reserve_btc": _get(self.onchain, "exchange_reserve_btc", 0),
            "exchange_reserve_7d_change": _get(self.onchain, "exchange_reserve_7d_change_pct", 0),
            "whale_accumulation_score": _get(self.onchain, "whale_accumulation_score", 0),

            # Macro
            "macro_narrative": _get(self.macro, "narrative"),
            "dxy": _get(self.macro, "dxy", 0),
            "dxy_trend": _get(self.macro, "dxy_trend"),
            "dxy_7d_change": _get(self.macro, "dxy_7d_change_pct", 0),
            "us10y_yield": _get(self.macro, "us10y_yield", 0),
            "us10y_7d_change_bps": _get(self.macro, "us10y_7d_change_bps", 0),
            "real_yield_10y": _get(self.macro, "real_yield_10y", 0),
            "cpi_yoy_last": _get(self.macro, "cpi_yoy_last", 0),
            "cpi_yoy_prev": _get(self.macro, "cpi_yoy_prev", 0),
            "cpi_surprise": _get(self.macro, "cpi_surprise"),
            "next_fomc_date": _get(self.macro, "next_fomc_date"),
            "days_to_fomc": _get(self.macro, "days_to_fomc", 0),
            "fed_rate_current": _get(self.macro, "fed_rate_current", 0),
            "rate_cut_probability": _get(self.macro, "rate_cut_probability_next_meeting", 0),
            "global_m2_trend": _get(self.macro, "global_m2_trend"),

            # Research
            "research_narrative": _get(self.research, "narrative"),
            "research_consensus": _get(self.research, "consensus_view"),

            # Derivatives
            "derivatives_narrative": _get(self.derivatives, "narrative"),
            "funding_percentile": _get(self.derivatives, "funding_rate_percentile_30d", 0),
            "funding_state": _get(self.derivatives, "funding_state"),
            "squeeze_risk": _get(self.derivatives, "squeeze_risk"),
            "open_interest_usd_bn": _get(self.derivatives, "open_interest_usd_bn", 0),
            "oi_24h_change_pct": _get(self.derivatives, "oi_24h_change_pct", 0),
            "long_short_ratio": _get(self.derivatives, "long_short_ratio", 0),
            "nearest_long_liq_cluster": _get(self.derivatives, "nearest_long_liq_cluster", {}),
            "nearest_short_liq_cluster": _get(self.derivatives, "nearest_short_liq_cluster", {}),

            # BTC extras for new prompts
            "btc_7d_change": self.price_change_24h,  # placeholder, will be overridden if available
            "btc_dxy_correlation": _get(self.macro, "btc_dxy_30d_correlation", -0.5),

            # Meta
            "data_freshness_score": round(self.data_freshness_score, 2),

            # Warning banner (empty if confidence is fine)
            "data_warning_banner": self._data_warning_banner(),

            # V2: data source health block for fail-loud prompts
            "data_source_status_block": self._build_health_block(),
        }
        return vars_dict

    def _format_issuer_table(self) -> str:
        """Format ETF issuer breakdown as a text table for prompts."""
        issuers = self.etf.get("issuer_breakdown", [])
        if not issuers:
            return "No issuer data available"
        lines = ["| Ticker | Flow ($M) | AUM ($B) |", "|--------|-----------|----------|"]
        for item in issuers[:10]:
            lines.append(
                f"| {item.get('ticker', '?'):6s} | {item.get('flow_mn', 0):>9.1f} | {item.get('aum_bn', 0):>8.1f} |"
            )
        return "\n".join(lines)

    def _data_warning_banner(self) -> str:
        """Return warning banner if data freshness is low."""
        if self.data_freshness_score < LOW_CONFIDENCE_THRESHOLD:
            return (
                "\n⚠️ **DATA RELIABILITY WARNING**: Overall data freshness score is "
                f"{self.data_freshness_score:.2f} (threshold: {LOW_CONFIDENCE_THRESHOLD}). "
                "Exercise extra caution in all judgments.\n"
            )
        # Check priority sources specifically
        warnings = []
        for src_name in PRIORITY_SOURCES:
            src_data = getattr(self, src_name, {})
            if src_data.get("confidence", 1.0) < 0.3:
                warnings.append(f"  - {src_name.upper()} data unavailable or stale")
        if warnings:
            return "\n⚠️ **PRIORITY DATA WARNING**:\n" + "\n".join(warnings) + "\n"
        return ""

    def _build_health_block(self) -> str:
        """V2: Build data source status block for fail-loud prompts."""
        if not self.health:
            return "No health data available."

        lines = []

        broken = self.broken_sources
        degraded = self.degraded_sources

        if not broken and not degraded:
            lines.append("All data sources healthy.")
        else:
            if broken:
                lines.append(
                    f"UNAVAILABLE SOURCES: {', '.join(broken)}. "
                    "DO NOT make any claims based on these sources. "
                    "Base your analysis ONLY on the remaining healthy sources."
                )
            if degraded:
                for name in degraded:
                    h = self.health[name]
                    missing = ", ".join(h.fields_missing) if h.fields_missing else "some fields"
                    lines.append(
                        f"DEGRADED SOURCES: {name} (missing: {missing}). "
                        "Use remaining fields with caution."
                    )

        lines.append(f"Data freshness score: {self.data_freshness_score:.2f}")
        return "\n".join(lines)


class ContextBuilder:
    """Aggregates all data sources into a MarketContext."""

    def __init__(self, redis_client=None, http_session=None):
        self._redis = redis_client
        self._http_session = http_session
        self._sources: dict[str, BaseDataSource] = {
            "etf": ETFFlowsSource(redis_client=redis_client, http_session=http_session),
            "onchain": OnchainMetricsSource(redis_client=redis_client, http_session=http_session),
            "macro": MacroIndicatorsSource(redis_client=redis_client, http_session=http_session),
            "research": ResearchReportsSource(redis_client=redis_client, http_session=http_session),
            "derivatives": DerivativesSource(redis_client=redis_client, http_session=http_session),
        }

    async def build(
        self,
        btc_price: float = 0.0,
        price_change_24h: float = 0.0,
        funding_rate: float = 0.0,
        regime: str = "unknown",
    ) -> MarketContext:
        """Fetch all sources in parallel and build MarketContext."""
        logger.info("context_build_start", sources=list(self._sources.keys()))

        # Parallel fetch all sources
        tasks = {
            name: asyncio.create_task(source.get_context())
            for name, source in self._sources.items()
        }

        results = {}
        health_map: dict[str, SourceHealth] = {}
        for name, task in tasks.items():
            try:
                result = await asyncio.wait_for(task, timeout=60)
                # V2: get_context() returns tuple[dict, SourceHealth]
                if isinstance(result, tuple) and len(result) == 2:
                    results[name], health_map[name] = result
                else:
                    # V1 fallback: plain dict
                    results[name] = result
                    health_map[name] = SourceHealth(status="HEALTHY")
            except asyncio.TimeoutError:
                logger.error("source_timeout", source=name)
                results[name] = self._sources[name]._fallback_context("timeout after 60s")
                health_map[name] = SourceHealth(
                    status="BROKEN",
                    failure_reason="timeout after 60s",
                    failure_stage="http",
                )
            except Exception as e:
                logger.error("source_error", source=name, error=str(e))
                results[name] = self._sources[name]._fallback_context(str(e))
                health_map[name] = SourceHealth(
                    status="BROKEN",
                    failure_reason=str(e)[:500],
                    failure_stage="unhandled",
                )

        # V2: Compute freshness score from SourceHealth statuses
        health_weights = {"etf": 0.25, "macro": 0.20, "onchain": 0.20, "derivatives": 0.20, "research": 0.15}
        status_scores = {"HEALTHY": 1.0, "DEGRADED": 0.5, "BROKEN": 0.0}
        freshness = sum(
            status_scores.get(health_map[name].status, 0.0) * health_weights.get(name, 0.1)
            for name in health_map
        )

        ctx = MarketContext(
            btc_price=btc_price,
            price_change_24h=price_change_24h,
            funding_rate=funding_rate,
            regime=regime,
            etf=results.get("etf", {}),
            onchain=results.get("onchain", {}),
            macro=results.get("macro", {}),
            research=results.get("research", {}),
            derivatives=results.get("derivatives", {}),
            health=health_map,
            data_freshness_score=round(freshness, 2),
        )

        logger.info(
            "context_build_complete",
            freshness=ctx.data_freshness_score,
            sources_ok=sum(1 for h in health_map.values() if h.status == "HEALTHY"),
            broken=ctx.broken_sources,
            degraded=ctx.degraded_sources,
        )
        return ctx

    async def close(self):
        """Close all data source HTTP sessions."""
        for source in self._sources.values():
            await source.close()
