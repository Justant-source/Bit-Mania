"""Default values for V2 prompt template variables.

When agents call .format() on prompt templates, they must supply
all template variables. V2 added context sections (ETF, on-chain,
macro, research, derivatives) and health/hallucination fields.

Agents that don't yet have access to the full MarketContext should
merge these defaults before calling .format().
"""

V2_PROMPT_DEFAULTS: dict[str, object] = {
    # Data source status
    "data_source_status_block": "N/A (external context not available in agent mode)",

    # ETF
    "etf_narrative": "N/A",
    "etf_daily_net_flow": "N/A",
    "etf_cumulative_flow": "N/A",
    "etf_7d_trend": "N/A",
    "etf_flow_streak": "N/A",
    "etf_top_inflow_issuer": "N/A",
    "etf_top_outflow_issuer": "N/A",
    "etf_issuer_breakdown_table": "N/A",
    "etf_30d_avg_flow": "N/A",
    "etf_flow_zscore": "N/A",
    "etf_max_inflow_90d": "N/A",
    "etf_max_outflow_90d": "N/A",

    # On-chain
    "onchain_narrative": "N/A",
    "mvrv_zscore": "N/A",
    "mvrv_percentile": "N/A",
    "mvrv_interpretation": "N/A",
    "exchange_reserve_btc": "N/A",
    "exchange_reserve_7d_change": "N/A",
    "whale_accumulation_score": "N/A",

    # Macro
    "macro_narrative": "N/A",
    "dxy": "N/A",
    "dxy_trend": "N/A",
    "dxy_7d_change": "N/A",
    "us10y_yield": "N/A",
    "us10y_7d_change_bps": "N/A",
    "real_yield_10y": "N/A",
    "cpi_yoy_last": "N/A",
    "cpi_yoy_prev": "N/A",
    "cpi_surprise": "N/A",
    "next_fomc_date": "N/A",
    "days_to_fomc": "N/A",
    "fed_rate_current": "N/A",
    "rate_cut_probability": "N/A",
    "global_m2_trend": "N/A",

    # Research
    "research_narrative": "N/A",
    "research_consensus": "N/A",

    # Derivatives
    "derivatives_narrative": "N/A",
    "funding_percentile": "N/A",
    "funding_state": "N/A",
    "squeeze_risk": "N/A",
    "open_interest_usd_bn": "N/A",
    "oi_24h_change_pct": "N/A",
    "long_short_ratio": "N/A",
    "nearest_long_liq_cluster": "N/A",
    "nearest_short_liq_cluster": "N/A",

    # BTC extras
    "btc_7d_change": "N/A",
    "btc_dxy_correlation": "N/A",

    # Meta
    "data_freshness_score": "N/A",
    "data_warning_banner": "",
}


def get_prompt_vars(market_data: dict | None = None) -> dict:
    """Get V2 prompt variables from market context, falling back to defaults.

    Parameters
    ----------
    market_data:
        The market context dict that may contain ``_v2_prompt_vars``
        populated by :class:`ContextBuilder`.  When present, those
        real values override the N/A defaults.
    """
    if market_data and market_data.get("_v2_prompt_vars"):
        # Merge: defaults as base, then override with actual data
        result = dict(V2_PROMPT_DEFAULTS)
        result.update(market_data["_v2_prompt_vars"])
        return result
    return dict(V2_PROMPT_DEFAULTS)
