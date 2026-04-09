"""Smoke tests: verify all prompts render without missing variables."""

import pytest


def _make_dummy_vars() -> dict:
    """Create a complete set of prompt variables for rendering tests."""
    return {
        # Existing vars
        "btc_price": 90000.0,
        "current_price": 90000.0,
        "price_change_24h": 2.5,
        "funding_rate": 0.01,
        "regime": "trending_up",
        "indicators": "RSI: 65, MACD: bullish crossover, BB: upper band test",
        "orderbook_summary": "Bids concentrated at 89500, asks thin above 91000",
        "atr": 1200.0,
        "adx": 28.5,
        "recent_price_action": "3 consecutive green candles, higher lows forming",
        "regime_history": "trending_up for 48h, prev: mean_reverting for 72h",
        "total_equity": 10500.0,
        "open_positions": "1 long BTCUSDT 0.01 @ 89000, lev 5x",
        "unrealised_pnl": 100.0,
        "exposure_pct": 42.5,
        "max_drawdown_pct": 2.1,
        "atr_pct": 1.4,
        "recent_trades": "Bought 0.01 BTC @ 89000 (funding arb entry)",
        "debate_conclusion": "Moderate bullish lean, 3-2 source alignment",
        "market_data": "BTC $90000 +2.5%, funding 0.01%, regime trending_up",
        "technical_report": "Uptrend confirmed by RSI 65, MACD bullish",
        "sentiment_report": "Funding elevated, slight long crowding",
        "bull_argument": "Strong ETF inflows + DXY weakness = bullish",
        "bear_argument": "Funding overheated + long liq cluster nearby",
        "round1_summary": "Bull stronger on fundamentals, bear on positioning",
        "round2_summary": "Bull wins 3-2 on source alignment",
        "date": "2026-04-09",
        "starting_equity": 10000.0,
        "ending_equity": 10500.0,
        "daily_pnl": 500.0,
        "daily_pnl_pct": 5.0,
        "trade_count": 3,
        "win_rate": 66.7,
        "strategy_performance": "FA: +$400, DCA: +$100",
        "btc_open": 88000.0,
        "btc_close": 90000.0,
        "btc_change_pct": 2.3,
        "advisory_accuracy": "2/3 correct calls today",
        "rating": "buy",
        "confidence": 0.75,
        "portfolio_state": "1 position, 42% exposure, equity $10500",
        "risk_assessment": "Moderate risk, elevated funding",

        # New ETF vars
        "etf_narrative": "3일 연속 순유출 -$421M",
        "etf_daily_net_flow": -127.4,
        "etf_cumulative_flow": 18.3,
        "etf_7d_trend": "outflow_accelerating",
        "etf_flow_streak": -3,
        "etf_top_inflow_issuer": "IBIT",
        "etf_top_outflow_issuer": "FBTC",
        "etf_issuer_breakdown_table": "| IBIT | 45.2 | 42.1 |",
        "etf_30d_avg_flow": 50.0,
        "etf_flow_zscore": -1.5,
        "etf_max_inflow_90d": 500.0,
        "etf_max_outflow_90d": -300.0,

        # New on-chain vars
        "onchain_narrative": "MVRV 2.34, 거래소 보유량 감소 중",
        "mvrv_zscore": 2.34,
        "mvrv_percentile": 62,
        "mvrv_interpretation": "fair_value",
        "exchange_reserve_btc": 2384221,
        "exchange_reserve_7d_change": -0.8,
        "whale_accumulation_score": 0.67,

        # New macro vars
        "macro_narrative": "DXY 약세, 실질금리 하락, 위험자산 우호적",
        "dxy": 104.23,
        "dxy_trend": "weakening",
        "dxy_7d_change": -0.6,
        "us10y_yield": 4.12,
        "us10y_7d_change_bps": -8,
        "real_yield_10y": 1.85,
        "cpi_yoy_last": 3.1,
        "cpi_yoy_prev": 3.2,
        "cpi_surprise": "below_expected",
        "next_fomc_date": "2026-04-29",
        "days_to_fomc": 20,
        "fed_rate_current": 4.50,
        "rate_cut_probability": 62,
        "global_m2_trend": "expanding",

        # New research vars
        "research_narrative": "기관 리서치 3건 강세 컨센서스",
        "research_consensus": "bullish",

        # New derivatives vars
        "derivatives_narrative": "펀딩 30일 78 percentile, 롱 과열",
        "funding_percentile": 78,
        "funding_state": "elevated_long",
        "squeeze_risk": "long_squeeze_elevated",
        "open_interest_usd_bn": 32.1,
        "oi_24h_change_pct": 3.2,
        "long_short_ratio": 1.18,
        "nearest_long_liq_cluster": {"price": 88400, "size_mn": 420},
        "nearest_short_liq_cluster": {"price": 94100, "size_mn": 380},

        # BTC extras
        "btc_7d_change": 2.5,
        "btc_dxy_correlation": -0.45,

        # Meta
        "data_freshness_score": 0.93,
        "data_warning_banner": "",

        # V2 Fail-Loud vars
        "data_source_status_block": "✅ All data sources healthy.",
        "low_confidence_reason": None,
        "data_sources_used": ["etf", "onchain", "macro", "research", "derivatives"],
        "analysis_completeness": "full",
    }


class TestPromptRendering:
    """Verify each prompt can be .format()-ed without KeyError."""

    @pytest.fixture
    def dummy_vars(self):
        return _make_dummy_vars()

    def test_market_analysis_renders(self, dummy_vars):
        from prompt_templates.market_analysis import MARKET_ANALYSIS_PROMPT
        result = MARKET_ANALYSIS_PROMPT.format(**dummy_vars)
        assert "90000" in result
        assert "etf_narrative" not in result  # variable should be replaced

    def test_regime_assessment_renders(self, dummy_vars):
        from prompt_templates.regime_assessment import REGIME_ASSESSMENT_PROMPT
        result = REGIME_ASSESSMENT_PROMPT.format(**dummy_vars)
        assert "trending_up" in result

    def test_risk_evaluation_renders(self, dummy_vars):
        from prompt_templates.risk_evaluation import RISK_EVALUATION_PROMPT
        result = RISK_EVALUATION_PROMPT.format(**dummy_vars)
        assert "10500" in result

    def test_daily_report_renders(self, dummy_vars):
        from prompt_templates.daily_report import DAILY_REPORT_PROMPT
        result = DAILY_REPORT_PROMPT.format(**dummy_vars)
        assert "2026-04-09" in result

    def test_asset_report_renders(self, dummy_vars):
        from prompt_templates.asset_report import ASSET_REPORT_PROMPT
        result = ASSET_REPORT_PROMPT.format(**dummy_vars)
        assert "90000" in result

    def test_bull_prompt_renders(self, dummy_vars):
        from prompt_templates.debate_prompts import BULL_PROMPT
        result = BULL_PROMPT.format(**dummy_vars)
        assert len(result) > 100

    def test_bear_prompt_renders(self, dummy_vars):
        from prompt_templates.debate_prompts import BEAR_PROMPT
        result = BEAR_PROMPT.format(**dummy_vars)
        assert len(result) > 100

    def test_debate_round1_renders(self, dummy_vars):
        from prompt_templates.debate_prompts import DEBATE_ROUND_1
        result = DEBATE_ROUND_1.format(**dummy_vars)
        assert len(result) > 100

    def test_debate_round2_renders(self, dummy_vars):
        from prompt_templates.debate_prompts import DEBATE_ROUND_2
        result = DEBATE_ROUND_2.format(**dummy_vars)
        assert len(result) > 100

    def test_moderator_prompt_renders(self, dummy_vars):
        from prompt_templates.debate_prompts import MODERATOR_PROMPT
        result = MODERATOR_PROMPT.format(**dummy_vars)
        assert len(result) > 100

    def test_etf_flow_analysis_renders(self, dummy_vars):
        from prompt_templates.etf_flow_analysis import ETF_FLOW_ANALYSIS_PROMPT
        result = ETF_FLOW_ANALYSIS_PROMPT.format(**dummy_vars)
        assert "IBIT" in result

    def test_macro_liquidity_renders(self, dummy_vars):
        from prompt_templates.macro_liquidity import MACRO_LIQUIDITY_PROMPT
        result = MACRO_LIQUIDITY_PROMPT.format(**dummy_vars)
        assert "104.23" in result
