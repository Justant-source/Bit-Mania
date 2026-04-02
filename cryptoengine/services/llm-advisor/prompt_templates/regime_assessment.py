"""Regime assessment prompt template."""

REGIME_ASSESSMENT_PROMPT = """\
You are a market regime specialist. Determine the current market regime for \
BTC/USDT and recommend strategy weight adjustments.

## Current Market State
- **Price**: ${current_price}
- **Regime (quantitative)**: {regime}
- **Funding Rate**: {funding_rate}
- **Volatility (ATR)**: {atr}
- **ADX**: {adx}

## Recent Price Action
{recent_price_action}

## Historical Regime Context
{regime_history}

## Instructions
Classify the current regime and recommend strategy weights. Respond with JSON:
{{
  "regime": "trending_up|trending_down|mean_reverting|high_volatility|low_volatility|transition",
  "regime_confidence": 0.0-1.0,
  "regime_duration_estimate": "hours|days|weeks",
  "strategy_weights": {{
    "trend_following": 0.0-1.0,
    "mean_reversion": 0.0-1.0,
    "momentum": 0.0-1.0,
    "volatility": 0.0-1.0
  }},
  "risk_multiplier": 0.5-1.5,
  "reasoning": "Brief explanation of regime assessment",
  "watch_for": ["list of signals that would indicate regime change"]
}}
"""
