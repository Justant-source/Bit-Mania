"""Unit tests for ContextBuilder and MarketContext."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


class TestMarketContext:
    """Test MarketContext dataclass and to_prompt_vars()."""

    def _make_context(self, **overrides):
        from context_builder import MarketContext
        defaults = {
            "btc_price": 90000.0,
            "price_change_24h": 2.5,
            "funding_rate": 0.01,
            "regime": "trending_up",
            "etf": {
                "narrative": "ETF net inflow of $200M",
                "confidence": 0.95,
                "daily_net_flow_usd_mn": 200.0,
                "cumulative_flow_usd_bn": 18.5,
                "flow_7d_trend": "inflow_strong",
                "flow_streak_days": 5,
                "top_inflow_issuer": "IBIT",
                "top_outflow_issuer": "GBTC",
                "issuer_breakdown": [
                    {"ticker": "IBIT", "flow_mn": 120.0, "aum_bn": 42.0},
                    {"ticker": "FBTC", "flow_mn": 80.0, "aum_bn": 15.0},
                ],
            },
            "onchain": {
                "narrative": "Accumulation phase",
                "confidence": 0.90,
                "mvrv_zscore": 2.34,
                "mvrv_historical_percentile": 62,
                "mvrv_interpretation": "fair_value",
                "exchange_reserve_btc": 2384221,
                "exchange_reserve_7d_change_pct": -0.8,
                "whale_accumulation_score": 0.67,
            },
            "macro": {
                "narrative": "Risk-on macro backdrop",
                "confidence": 0.98,
                "dxy": 104.23,
                "dxy_trend": "weakening",
                "dxy_7d_change_pct": -0.6,
                "us10y_yield": 4.12,
                "us10y_7d_change_bps": -8,
                "real_yield_10y": 1.85,
                "cpi_yoy_last": 3.1,
                "cpi_yoy_prev": 3.2,
                "cpi_surprise": "below_expected",
                "next_fomc_date": "2026-04-29",
                "days_to_fomc": 20,
                "fed_rate_current": 4.50,
                "rate_cut_probability_next_meeting": 0.62,
                "global_m2_trend": "expanding",
            },
            "research": {
                "narrative": "Bullish consensus",
                "confidence": 0.85,
                "consensus_view": "bullish",
            },
            "derivatives": {
                "narrative": "Elevated long positioning",
                "confidence": 0.92,
                "funding_rate_percentile_30d": 78,
                "funding_state": "elevated_long",
                "squeeze_risk": "long_squeeze_elevated",
                "open_interest_usd_bn": 32.1,
                "oi_24h_change_pct": 3.2,
                "long_short_ratio": 1.18,
                "nearest_long_liq_cluster": {"price": 88400, "size_mn": 420},
                "nearest_short_liq_cluster": {"price": 94100, "size_mn": 380},
            },
            "data_freshness_score": 0.93,
        }
        defaults.update(overrides)
        return MarketContext(**defaults)

    def test_to_prompt_vars_has_all_keys(self):
        ctx = self._make_context()
        vars_dict = ctx.to_prompt_vars()

        required_keys = [
            "btc_price", "current_price", "funding_rate", "regime",
            "etf_narrative", "etf_daily_net_flow", "etf_7d_trend",
            "onchain_narrative", "mvrv_zscore", "mvrv_percentile",
            "macro_narrative", "dxy", "dxy_trend", "days_to_fomc",
            "research_narrative", "research_consensus",
            "derivatives_narrative", "funding_percentile", "squeeze_risk",
            "data_freshness_score", "data_warning_banner",
        ]
        for key in required_keys:
            assert key in vars_dict, f"Missing key: {key}"

    def test_warning_banner_low_freshness(self):
        ctx = self._make_context(data_freshness_score=0.4)
        vars_dict = ctx.to_prompt_vars()
        assert "WARNING" in vars_dict["data_warning_banner"]

    def test_no_warning_banner_high_freshness(self):
        ctx = self._make_context(data_freshness_score=0.95)
        vars_dict = ctx.to_prompt_vars()
        assert vars_dict["data_warning_banner"] == ""

    def test_issuer_table_formatting(self):
        ctx = self._make_context()
        vars_dict = ctx.to_prompt_vars()
        table = vars_dict["etf_issuer_breakdown_table"]
        assert "IBIT" in table
        assert "FBTC" in table


class TestContextBuilder:
    """Test ContextBuilder.build() method."""

    @pytest.mark.asyncio
    async def test_build_all_sources_success(self):
        from context_builder import ContextBuilder

        builder = ContextBuilder(redis_client=None, http_session=None)

        # Mock all sources
        for name, source in builder._sources.items():
            source.get_context = AsyncMock(return_value={
                "narrative": f"{name} test data",
                "confidence": 0.9,
                "as_of": "2026-04-09T00:00:00Z",
                "source": name,
            })

        ctx = await builder.build(btc_price=90000, regime="trending_up")
        assert ctx.data_freshness_score >= 0.8
        assert ctx.etf["narrative"] == "etf test data"
        assert ctx.macro["narrative"] == "macro test data"

    @pytest.mark.asyncio
    async def test_build_one_source_fails(self):
        from context_builder import ContextBuilder

        builder = ContextBuilder(redis_client=None, http_session=None)

        for name, source in builder._sources.items():
            if name == "etf":
                source.get_context = AsyncMock(side_effect=Exception("ETF down"))
            else:
                source.get_context = AsyncMock(return_value={
                    "narrative": f"{name} ok",
                    "confidence": 0.9,
                    "as_of": "2026-04-09T00:00:00Z",
                    "source": name,
                })

        ctx = await builder.build(btc_price=90000)
        # ETF failed → lower freshness
        assert ctx.data_freshness_score < 0.9
        # Other sources still present
        assert ctx.macro["narrative"] == "macro ok"

    @pytest.mark.asyncio
    async def test_build_all_sources_fail(self):
        from context_builder import ContextBuilder

        builder = ContextBuilder(redis_client=None, http_session=None)

        for source in builder._sources.values():
            source.get_context = AsyncMock(side_effect=Exception("all down"))

        ctx = await builder.build(btc_price=90000)
        assert ctx.data_freshness_score < 0.1
