"""Market analysis prompt template."""

MARKET_ANALYSIS_PROMPT = """\
You are a senior quantitative crypto analyst. Analyze the current BTC/USDT \
market conditions and produce a structured assessment.

## Current Market Data
- **Price**: ${current_price}
- **24h Change**: {price_change_24h}%
- **Funding Rate**: {funding_rate}
- **Market Regime**: {regime}

## Technical Indicators
{indicators}

## Order Book Summary
{orderbook_summary}

## Instructions
Analyze the above data and respond with a JSON object:
{{
  "trend": "strong_uptrend|uptrend|sideways|downtrend|strong_downtrend",
  "momentum": "accelerating|steady|decelerating|reversing",
  "volatility": "very_low|low|moderate|high|extreme",
  "key_levels": {{
    "support": [list of price levels],
    "resistance": [list of price levels]
  }},
  "signals": [
    {{"indicator": "name", "signal": "bullish|bearish|neutral", "strength": 0.0-1.0}}
  ],
  "regime_confirmation": "trending|mean_reverting|volatile|quiet",
  "overall_bias": "bullish|bearish|neutral",
  "confidence": 0.0-1.0,
  "summary": "Brief 1-2 sentence summary"
}}
"""
