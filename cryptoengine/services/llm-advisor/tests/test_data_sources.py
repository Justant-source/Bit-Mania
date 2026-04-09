"""Unit tests for LLM Advisor data sources."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# We need to test each data source's summarize() method with sample raw data
# and verify the output schema.


# ── Schema validation helpers ──

COMMON_REQUIRED_KEYS = {"as_of", "source", "confidence", "narrative"}


def assert_common_fields(result: dict, source_name: str):
    """Verify all data sources include mandatory fields."""
    for key in COMMON_REQUIRED_KEYS:
        assert key in result, f"Missing required key '{key}' in {source_name} output"
    assert isinstance(result["narrative"], str) and len(result["narrative"]) > 0, \
        f"narrative must be non-empty string in {source_name}"
    assert 0.0 <= result["confidence"] <= 1.0, \
        f"confidence must be 0-1 in {source_name}"


# ── ETF Flows ──

class TestETFFlowsSource:
    """Tests for ETFFlowsSource."""

    def _make_source(self):
        from data_sources.etf_flows import ETFFlowsSource
        return ETFFlowsSource(redis_client=None, http_session=None)

    def test_summarize_valid_data(self):
        src = self._make_source()
        raw = {
            "rows": [
                {"date": "2026-04-08", "IBIT": 45.2, "FBTC": -30.1, "ARKB": -15.0,
                 "BITB": 5.0, "BTCO": 0, "EZBC": 0, "BRRR": 0, "HODL": 0,
                 "BTCW": 0, "GBTC": -20.5, "BTC": 0, "Total": -15.4},
                {"date": "2026-04-07", "IBIT": 50.0, "FBTC": -25.0, "ARKB": -10.0,
                 "BITB": 3.0, "BTCO": 0, "EZBC": 0, "BRRR": 0, "HODL": 0,
                 "BTCW": 0, "GBTC": -18.0, "BTC": 0, "Total": 0.0},
            ],
            "source": "farside",
        }
        result = src.summarize(raw)
        assert_common_fields(result, "ETFFlows")
        assert "daily_net_flow_usd_mn" in result
        assert "flow_7d_trend" in result
        assert "flow_streak_days" in result
        assert "issuer_breakdown" in result
        assert isinstance(result["issuer_breakdown"], list)

    def test_summarize_empty_data(self):
        src = self._make_source()
        raw = {"rows": [], "source": "farside"}
        result = src.summarize(raw)
        assert_common_fields(result, "ETFFlows")
        assert result["confidence"] < 0.5

    @pytest.mark.asyncio
    async def test_get_context_failure_returns_fallback(self):
        src = self._make_source()
        # Mock fetch_raw to raise
        src.fetch_raw = AsyncMock(side_effect=Exception("network error"))
        result = await src.get_context()
        assert_common_fields(result, "ETFFlows")
        assert result["confidence"] == 0.0


# ── On-chain Metrics ──

class TestOnchainMetricsSource:

    def _make_source(self):
        from data_sources.onchain_metrics import OnchainMetricsSource
        return OnchainMetricsSource(redis_client=None, http_session=None)

    def test_summarize_valid_data(self):
        src = self._make_source()
        raw = {
            "exchange_reserve": [
                {"t": 1712620800, "v": 2384221},
                {"t": 1712016000, "v": 2403421},
            ],
            "mvrv_zscore": [{"t": 1712620800, "v": 2.34}],
            "puell_multiple": [{"t": 1712620800, "v": 1.18}],
            "sopr": [{"t": 1712620800, "v": 1.02}],
            "lth_supply": [
                {"t": 1712620800, "v": 15420000},
                {"t": 1710028800, "v": 15360000},
            ],
            "source": "glassnode",
        }
        result = src.summarize(raw)
        assert_common_fields(result, "Onchain")
        assert "mvrv_zscore" in result
        assert "mvrv_interpretation" in result
        assert "whale_accumulation_score" in result

    @pytest.mark.asyncio
    async def test_get_context_failure_returns_fallback(self):
        src = self._make_source()
        src.fetch_raw = AsyncMock(side_effect=Exception("API error"))
        result = await src.get_context()
        assert result["confidence"] == 0.0


# ── Macro Indicators ──

class TestMacroIndicatorsSource:

    def _make_source(self):
        from data_sources.macro_indicators import MacroIndicatorsSource
        return MacroIndicatorsSource(redis_client=None, http_session=None)

    def test_summarize_valid_data(self):
        src = self._make_source()
        raw = {
            "dxy": {"current": 104.23, "week_ago": 104.86},
            "us10y": {"current": 4.12, "week_ago": 4.20},
            "real_yield_10y": 1.85,
            "cpi": {"latest": 3.1, "previous": 3.2},
            "fed_rate": 4.50,
            "m2_trend": "expanding",
            "source": "fred_yfinance",
        }
        result = src.summarize(raw)
        assert_common_fields(result, "Macro")
        assert "dxy" in result
        assert "dxy_trend" in result
        assert "days_to_fomc" in result
        assert result["dxy_trend"] in ("strong_up", "up", "flat", "down", "strong_down")

    @pytest.mark.asyncio
    async def test_get_context_failure_returns_fallback(self):
        src = self._make_source()
        src.fetch_raw = AsyncMock(side_effect=Exception("FRED error"))
        result = await src.get_context()
        assert result["confidence"] == 0.0


# ── Research Reports ──

class TestResearchReportsSource:

    def _make_source(self):
        from data_sources.research_reports import ResearchReportsSource
        return ResearchReportsSource(redis_client=None, http_session=None)

    def test_summarize_valid_data(self):
        src = self._make_source()
        raw = {
            "reports": [
                {
                    "title": "Q1 2026 Crypto Review",
                    "publisher": "Messari",
                    "published_at": "2026-04-05",
                    "url": "https://messari.io/report/q1-2026",
                    "content": "Long-term holder supply hits all-time high. ETF inflows exceed mining output.",
                },
            ],
            "source": "messari",
        }
        result = src.summarize(raw)
        assert_common_fields(result, "Research")
        assert "reports" in result
        assert "consensus_view" in result

    @pytest.mark.asyncio
    async def test_get_context_failure_returns_fallback(self):
        src = self._make_source()
        src.fetch_raw = AsyncMock(side_effect=Exception("RSS error"))
        result = await src.get_context()
        assert result["confidence"] == 0.0


# ── Derivatives ──

class TestDerivativesSource:

    def _make_source(self):
        from data_sources.derivatives import DerivativesSource
        return DerivativesSource(redis_client=None, http_session=None)

    def test_summarize_valid_data(self):
        src = self._make_source()
        raw = {
            "funding_rates": [
                {"exchange": "binance", "rate": 0.015, "oi_weight": 0.4},
                {"exchange": "bybit", "rate": 0.012, "oi_weight": 0.3},
                {"exchange": "okx", "rate": 0.018, "oi_weight": 0.3},
            ],
            "open_interest": {"total_usd": 32_100_000_000, "change_24h_pct": 3.2},
            "long_short_ratio": 1.18,
            "top_trader_long_ratio": 0.58,
            "liquidations_24h": {"long_usd": 142_300_000, "short_usd": 38_100_000},
            "liquidation_map": {
                "long_clusters": [{"price": 88400, "size_usd": 420_000_000}],
                "short_clusters": [{"price": 94100, "size_usd": 380_000_000}],
            },
            "source": "coinglass",
        }
        result = src.summarize(raw)
        assert_common_fields(result, "Derivatives")
        assert "funding_rate_weighted_avg" in result
        assert "funding_state" in result
        assert "squeeze_risk" in result
        assert "nearest_long_liq_cluster" in result

    @pytest.mark.asyncio
    async def test_get_context_failure_returns_fallback(self):
        src = self._make_source()
        src.fetch_raw = AsyncMock(side_effect=Exception("Coinglass error"))
        result = await src.get_context()
        assert result["confidence"] == 0.0
